{.push raises: [].}

import
  std/[hashes, strutils, tables, net],
  chronos,
  chronicles,
  metrics,
  results,
  stew/byteutils,
  eth/keys,
  eth/p2p/discoveryv5/enr,
  libp2p/crypto/crypto,
  libp2p/protocols/ping,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/pubsub/rpc/messages,
  libp2p/stream/connection,
  libp2p/varint,
  libp2p/builders,
  libp2p/transports/tcptransport,
  libp2p/transports/wstransport,
  libp2p_mix

import
  ../waku_node,
  ../../utils/requests,
  ../../waku_core,
  ../../waku_core/topics/sharding,
  ../../waku_lightpush_legacy/client as legacy_lightpush_client,
  ../../waku_lightpush_legacy as legacy_lightpush_protocol,
  ../../waku_lightpush/client as lightpush_client,
  ../../waku_lightpush as lightpush_protocol,
  ../peer_manager,
  ../../common/rate_limit/setting,
  ../../rln

logScope:
  topics = "waku node lightpush api"

const MountWithoutRelayError* = "cannot mount lightpush because relay is not mounted"

const MixReplySurbs* = 1'u8
  ## Number of return paths (SURBs) in a mix-routed lightpush request. One is
  ## sufficient for one reply, and each SURB adds bytes to the packet.

const MixReplyTimeout* = chronos.seconds(5)
  ## Time limit for one mix-routed lightpush request: the write of the request
  ## and the reply of the exit node. A broken path costs one attempt, not the
  ## full send. The limit is short because the send service retries each
  ## round, and an attempt that waits holds a retry slot.

const MixLibraryReplyTimeout* = MixReplyTimeout + MixReplyTimeout
  ## The reply time limit given to the mix entry connection. Twice the limit
  ## of `publishOverMix`, so that the timer of `publishOverMix` fires first and
  ## a lost reply has one outcome. The library's limit is the backstop.

type
  MixSendCertainty* {.pure.} = enum
    ## What one mix attempt lets the caller conclude about the message.
    NotSent
      ## The message is not on the network: no request left this node, or the
      ## exit node answered that it did not publish.
    MaybeSent
      ## The request was written to the mix connection, or may have been, and
      ## no readable answer came back. The exit node may have published the
      ## message.
    Confirmed ## The exit node answered that it published the message.

  MixSendReason* {.pure.} = enum
    ## What happened in the attempt. The certainty and the reason are separate
    ## facts: the certainty is a conclusion, the reason is the event.
    Accepted ## The exit node accepted the request.
    NoExitNode ## No lightpush server with a mix key is known for the shard.
    TooLarge
      ## The request does not fit a sphinx packet next to one return path. A
      ## retry cannot make it fit.
    PreWriteFailure
      ## Nothing left this node: the path could not be built, the first hop
      ## refused the connection, the write failed, or a condition before the
      ## write (the RLN proof, the mix connection) was not met. The text of
      ## the answer says which.
    ReplyTimeout
      ## The time limit ended before a reply. The write may or may not have
      ## completed; either way the request may have left this node.
    ReplyUnreadable
      ## The request was written. The reply was truncated, reset, or could not
      ## be decoded.
    ExitRefused
      ## The request was written. The exit node answered that it did not
      ## publish; its answer is in `answer`.

  MixSendOutcome* = object
    certainty*: MixSendCertainty
    reason*: MixSendReason
    exit*: Opt[PeerId] ## The exit node of the attempt, when one was selected.
    answer*: lightpush_protocol.WakuLightPushResult
      ## The exit node's answer, or a local status for a failure before the
      ## reply. Its text is for the log. A decision reads the typed fields
      ## and, for `ExitRefused`, the status code of the answer.

func requestLeft*(outcome: MixSendOutcome): bool =
  ## True when the request was written to the mix connection, or may have
  ## been: the message may be on the network, or in the hands of an exit node
  ## that refused it. A caller that must not send the message in clear text
  ## after that reads this and not the certainty: an exit node that refused
  ## the request has seen the message all the same.
  outcome.reason in {
    MixSendReason.Accepted, MixSendReason.ReplyTimeout, MixSendReason.ReplyUnreadable,
    MixSendReason.ExitRefused,
  }

func mixNotSent(
    reason: MixSendReason, answer: lightpush_protocol.WakuLightPushResult
): MixSendOutcome =
  MixSendOutcome(certainty: MixSendCertainty.NotSent, reason: reason, answer: answer)

func mixMaybeSent(
    reason: MixSendReason, answer: lightpush_protocol.WakuLightPushResult
): MixSendOutcome =
  MixSendOutcome(certainty: MixSendCertainty.MaybeSent, reason: reason, answer: answer)

proc publishOverMix*(
    node: WakuNode,
    conn: Connection,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    replyTimeout: Duration = MixReplyTimeout,
): Future[MixSendOutcome] {.async.} =
  ## One lightpush request on a mix connection, with the time limit
  ## `replyTimeout` over the write and the reply. The proc makes the exchange
  ## itself and not through the lightpush client, so that it knows on which
  ## side of the write each failure fell: the certainty of the outcome is a
  ## fact of this proc and not a reading of an error text. `replyTimeout` is
  ## a parameter so that tests can use a short limit.
  ##
  ## The size check is exact: the bytes that go to the mix connection (the
  ## length-prefixed request) against the payload a sphinx packet holds next
  ## to `MixReplySurbs` return paths.
  ##
  ## The mix entry connection has its own reply time limit
  ## (`MixParameters.replyTimeout`, applied in `readOnce`). That limit does not
  ## cover a stall in the write: mix dials the first hop with `switch.dial`
  ## and not with the peer manager, so `DefaultDialTimeout` does not apply,
  ## and pool entries do not expire. One timer here covers the write and the
  ## read. `chronos.withTimeout` is not usable: it waits for the cancellation
  ## of the future it cancels, and a stall inside a libp2p dial may not answer
  ## a cancellation. Thus `race` with a timer, a reset of the stream, and no
  ## wait for the cancellation.
  let request = LightpushRequest(
    requestId: generateRequestId(node.rng),
    pubsubTopic: Opt.some(pubsubTopic),
    message: message,
  )
  let encoded = request.encode().buffer
  # The frame as `writeLp` builds it: a varint length and the bytes.
  let prefix = PB.toBytes(encoded.len.uint64)
  var frame = newSeqUninit[byte](prefix.len + encoded.len)
  frame[0 ..< prefix.len] = prefix.toOpenArray()
  frame[prefix.len ..< frame.len] = encoded

  let maxPayload = getMaxMessageSizeForCodec(WakuLightPushCodec, MixReplySurbs).valueOr:
    return mixNotSent(
      MixSendReason.PreWriteFailure,
      lightpushResultServiceUnavailable(
        "mix cannot carry the lightpush codec: " & error
      ),
    )
  if frame.len > maxPayload:
    debug "Message too large for a mix packet",
      pubsubTopic = pubsubTopic, size = frame.len, maxPayload = maxPayload
    return mixNotSent(
      MixSendReason.TooLarge,
      lighpushErrorResult(
        LightPushErrorCode.PAYLOAD_TOO_LARGE,
        "message too large for a mix packet: " & $frame.len & " bytes, maximum " &
          $maxPayload,
      ),
    )

  let deadline = sleepAsync(replyTimeout)
  var pending: FutureBase = nil ## The phase in flight, for a cancellation.
  try:
    let writeFut = conn.write(frame)
    pending = writeFut
    discard await race(FutureBase(writeFut), FutureBase(deadline))
    if not writeFut.finished():
      # A stall in the write, the dial of the first hop in the common case.
      # Whether bytes left this node is not known.
      await conn.reset()
      writeFut.cancelSoon()
      debug "Mix lightpush timed out before the write completed",
        pubsubTopic = pubsubTopic, timeout = replyTimeout
      return mixMaybeSent(
        MixSendReason.ReplyTimeout,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix: no reply within the time limit; the write did not complete"
        ),
      )
    if writeFut.failed():
      # The mix library raises `LPStreamError` before the write for a path it
      # cannot build, a first hop that refuses the connection, or a packet it
      # cannot make. Nothing left this node.
      await deadline.cancelAndWait()
      await conn.close()
      debug "Mix lightpush failed before the write",
        pubsubTopic = pubsubTopic, error = writeFut.error.msg
      return mixNotSent(
        MixSendReason.PreWriteFailure,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix failed before the write: " & writeFut.error.msg
        ),
      )

    let readFut = conn.readLp(DefaultMaxRpcSize.int)
    pending = readFut
    discard await race(FutureBase(readFut), FutureBase(deadline))
    if not readFut.finished():
      await conn.reset()
      readFut.cancelSoon()
      debug "Mix lightpush timed out", pubsubTopic = pubsubTopic, timeout = replyTimeout
      return mixMaybeSent(
        MixSendReason.ReplyTimeout,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix: no reply within the time limit"
        ),
      )
    await deadline.cancelAndWait()
    await conn.close()
    if not readFut.completed():
      # libp2p raises a subclass of `LPStreamError` for a reply it cannot
      # read: truncated, reset, closed, or the library's own time limit.
      let msg =
        if readFut.failed():
          readFut.error.msg
        else:
          "the read was cancelled"
      debug "Mix lightpush reply unreadable", pubsubTopic = pubsubTopic, error = msg
      return mixMaybeSent(
        MixSendReason.ReplyUnreadable,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix: the reply could not be read: " & msg
        ),
      )
    let response = LightPushResponse.decode(readFut.read()).valueOr:
      debug "Mix lightpush reply could not be decoded", pubsubTopic = pubsubTopic
      return mixMaybeSent(
        MixSendReason.ReplyUnreadable,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix: the reply could not be decoded"
        ),
      )
    if response.requestId != request.requestId and
        response.statusCode != LightPushErrorCode.TOO_MANY_REQUESTS:
      debug "Mix lightpush reply names another request",
        requestId = request.requestId, responseRequestId = response.requestId
      return mixMaybeSent(
        MixSendReason.ReplyUnreadable,
        lightpushResultServiceUnavailable(
          "Waku lightpush over mix: the reply names another request"
        ),
      )
    let answer = toPushResult(response)
    if answer.isOk():
      return MixSendOutcome(
        certainty: MixSendCertainty.Confirmed,
        reason: MixSendReason.Accepted,
        answer: answer,
      )
    # The exit node answered and did not publish: no relay peer, too many
    # requests, an RLN rejection, or another refusal.
    return mixNotSent(MixSendReason.ExitRefused, answer)
  except CancelledError as exc:
    # The caller cancels the attempt (the send service stops). `race` does
    # not cancel its inputs: reset the mix stream, cancel the phase in flight
    # and the timer, and let the cancellation go up.
    await conn.reset()
    if not pending.isNil() and not pending.finished():
      pending.cancelSoon()
    deadline.cancelSoon()
    raise exc

## Waku lightpush
proc mountLegacyLightPush*(
    node: WakuNode, rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit
): Future[Result[void, string]] {.async.} =
  info "mounting legacy light push"

  if node.wakuRelay.isNil():
    return err(MountWithoutRelayError)

  info "mounting legacy lightpush with relay"
  let pushHandler = legacy_lightpush_protocol.getRelayPushHandler(node.wakuRelay)

  node.wakuLegacyLightPush = WakuLegacyLightPush.new(
    node.peerManager, node.rng, pushHandler, Opt.some(rateLimit)
  )

  if node.started:
    # Node has started already. Let's start lightpush too.
    await node.wakuLegacyLightPush.start()

  node.switch.mount(node.wakuLegacyLightPush, protocolMatcher(WakuLegacyLightPushCodec))

  debug "legacy lightpush mounted successfully"
  return ok()

proc mountLegacyLightPushClient*(node: WakuNode) =
  info "mounting legacy light push client"

  if node.wakuLegacyLightpushClient.isNil():
    node.wakuLegacyLightpushClient =
      WakuLegacyLightPushClient.new(node.peerManager, node.rng)

proc internalLegacyLightpushPublish(
    node: WakuNode, pubsubTopic: PubsubTopic, message: WakuMessage, peer: RemotePeerInfo
): Future[legacy_lightpush_protocol.WakuLightPushResult[string]] {.async, gcsafe.} =
  ## Dispatches to the legacy lightpush client if mounted, otherwise to the
  ## self-hosted server. Callers guarantee at least one is mounted.
  let msgHash = pubsubTopic.computeMessageHash(message).to0xHex()
  if not node.wakuLegacyLightpushClient.isNil():
    debug "Publishing message with legacy lightpush",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      target_peer_id = peer.peerId,
      msg_hash = msgHash
    return await node.wakuLegacyLightpushClient.publish(pubsubTopic, message, peer)

  debug "Publishing message with self hosted legacy lightpush",
    pubsubTopic = pubsubTopic,
    contentTopic = message.contentTopic,
    target_peer_id = peer.peerId,
    msg_hash = msgHash
  return await node.wakuLegacyLightPush.handleSelfLightPushRequest(pubsubTopic, message)

proc resolveLegacyPubsubTopic(
    node: WakuNode, pubsubTopic: Opt[PubsubTopic], contentTopic: ContentTopic
): Result[PubsubTopic, string] =
  ## Returns the explicit pubsub topic, else derives it from `contentTopic`
  ## via autosharding. The legacy wire format requires a pubsub topic and the
  ## server never derives it, so the client must resolve it here.
  if pubsubTopic.isSome():
    return ok(pubsubTopic.get())
  if node.wakuAutoSharding.isNone():
    return err("Pubsub topic must be specified when static sharding is enabled")
  let parsedTopic = NsContentTopic.parse(contentTopic).valueOr:
    return err("Invalid content-topic: " & $error)
  let shard = node.wakuAutoSharding.get().getShard(parsedTopic).valueOr:
      return err("Autosharding error: " & error)
  return ok($shard)

proc legacyLightpushPublish*(
    node: WakuNode,
    pubsubTopic: Opt[PubsubTopic],
    message: WakuMessage,
    peer: RemotePeerInfo,
): Future[legacy_lightpush_protocol.WakuLightPushResult[string]] {.async, gcsafe.} =
  ## Pushes a `WakuMessage` to a node which relays it further on PubSub topic.
  ## Returns whether relaying was successful or not.
  ## `WakuMessage` should contain a `contentTopic` field for light node
  ## functionality.
  if node.wakuLegacyLightpushClient.isNil() and node.wakuLegacyLightPush.isNil():
    debug "Failed to publish message as legacy lightpush not available"
    return err("Waku lightpush not available")

  # toRLNSignal hashes the timestamp into the proof, so fix it before proof gen;
  # the downstream ensureTimestampSet then becomes a no-op.
  let message = ensureTimestampSet(message)

  let rln =
    if node.rln.isNil():
      Opt.none(Rln)
    else:
      Opt.some(node.rln)
  let msgWithProof = (await checkAndGenerateRLNProof(rln, message)).valueOr:
    return err("failed call checkAndGenerateRLNProof from lightpush: " & error)

  try:
    let pubsubForPublish = resolveLegacyPubsubTopic(
      node, pubsubTopic, message.contentTopic
    ).valueOr:
      return err(error)

    let publishResult =
      await internalLegacyLightpushPublish(node, pubsubForPublish, msgWithProof, peer)

    # Legacy has no status codes, so string-match the RLN error to detect a
    # stale merkle proof path. Schedule the refresh and hand the error back:
    # retrying is the caller's decision, the same way the non-legacy path
    # behaves. A retry regenerates the proof against the refreshed cache.
    if publishResult.isOk() or rln.isNone() or
        not publishResult.error.contains(RlnValidatorErrorMsg):
      return publishResult

    debug "legacy lightpush send rejected as RLN-invalid; scheduling merkle proof refresh"
    rln.get().groupManager.scheduleMerkleProofRefresh()
    return err(RlnProofRefreshScheduledMsg & ": " & publishResult.error)
  except CatchableError:
    return err(getCurrentExceptionMsg())

# TODO: Move to application module (e.g., wakunode2.nim)
proc legacyLightpushPublish*(
    node: WakuNode, pubsubTopic: Opt[PubsubTopic], message: WakuMessage
): Future[legacy_lightpush_protocol.WakuLightPushResult[string]] {.
    async, gcsafe, deprecated: "Use 'node.legacyLightpushPublish()' instead"
.} =
  if node.wakuLegacyLightpushClient.isNil() and node.wakuLegacyLightPush.isNil():
    debug "Failed to publish message as legacy lightpush not available"
    return err("waku legacy lightpush not available")

  var peerOpt: Opt[RemotePeerInfo] = Opt.none(RemotePeerInfo)
  if not node.wakuLegacyLightpushClient.isNil():
    peerOpt = node.peerManager.selectPeer(WakuLegacyLightPushCodec)
    if peerOpt.isNone():
      let msg = "no suitable remote peers"
      debug "Failed to publish message", err = msg
      return err(msg)
  elif not node.wakuLegacyLightPush.isNil():
    peerOpt = Opt.some(RemotePeerInfo.init($node.switch.peerInfo.peerId))

  return await node.legacyLightpushPublish(pubsubTopic, message, peer = peerOpt.get())

proc mountLightPush*(
    node: WakuNode, rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit
): Future[Result[void, string]] {.async.} =
  info "mounting light push"

  if node.wakuRelay.isNil():
    return err(MountWithoutRelayError)

  info "mounting lightpush with relay"
  let pushHandler = lightpush_protocol.getRelayPushHandler(node.wakuRelay)

  node.wakuLightPush = WakuLightPush.new(
    node.peerManager, node.rng, pushHandler, node.wakuAutoSharding, Opt.some(rateLimit)
  )

  if node.started:
    # Node has started already. Let's start lightpush too.
    await node.wakuLightPush.start()

  node.switch.mount(node.wakuLightPush, protocolMatcher(WakuLightPushCodec))

  debug "lightpush mounted successfully"
  return ok()

proc mountLightPushClient*(node: WakuNode) =
  info "mounting light push client"

  if node.wakuLightpushClient.isNil():
    node.wakuLightpushClient = WakuLightPushClient.new(node.peerManager, node.rng)

proc lightpushPublishHandler(
    node: WakuNode,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    peer: RemotePeerInfo | PeerInfo,
    mixify: bool = false,
): Future[lightpush_protocol.WakuLightPushResult] {.async.} =
  let msgHash = pubsubTopic.computeMessageHash(message).to0xHex()
  if not node.wakuLightpushClient.isNil():
    debug "Publishing message with lightpush",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      target_peer_id = peer.peerId,
      msg_hash = msgHash,
      mixify = mixify
    if defined(libp2p_mix_experimental_exit_is_dest) and mixify:
      #indicates we want to use mix to send the message
      when defined(libp2p_mix_experimental_exit_is_dest):
        #TODO: How to handle multiple addresses?
        let conn = node.wakuMix.toConnection(
          MixDestination.exitNode(peer.peerId),
          WakuLightPushCodec,
          MixParameters(expectReply: Opt.some(true), numSurbs: Opt.some(byte(1))),
            # indicating we only want a single path to be used for reply hence numSurbs = 1
        ).valueOr:
          debug "Could not create mix connection"
          return lighpushErrorResult(
            LightPushErrorCode.SERVICE_NOT_AVAILABLE,
            "Waku lightpush with mix not available",
          )

        return
          await node.wakuLightpushClient.publish(Opt.some(pubsubTopic), message, conn)
    else:
      return
        await node.wakuLightpushClient.publish(Opt.some(pubsubTopic), message, peer)

  if not node.wakuLightPush.isNil():
    if mixify:
      debug "Mixify is not supported with self hosted lightpush"
      return lighpushErrorResult(
        LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        "Waku lightpush with mix not available",
      )
    debug "Publishing message with self hosted lightpush",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      target_peer_id = peer.peerId,
      msg_hash = msgHash
    return await node.wakuLightPush.handleSelfLightPushRequest(
      Opt.some(pubsubTopic), message
    )

proc lightpushPublish*(
    node: WakuNode,
    pubsubTopic: Opt[PubsubTopic],
    message: WakuMessage,
    peerOpt: Opt[RemotePeerInfo] = Opt.none(RemotePeerInfo),
    mixify: bool = false,
): Future[lightpush_protocol.WakuLightPushResult] {.async.} =
  if node.wakuLightpushClient.isNil() and node.wakuLightPush.isNil():
    debug "Failed to publish message as lightpush not available"
    return lighpushErrorResult(
      LightPushErrorCode.SERVICE_NOT_AVAILABLE, "Waku lightpush not available"
    )
  if mixify and node.wakuMix.isNil():
    debug "Failed to publish message using mix as mix protocol is not mounted"
    return lighpushErrorResult(
      LightPushErrorCode.SERVICE_NOT_AVAILABLE, "Waku lightpush with mix not available"
    )
  let toPeer: RemotePeerInfo = peerOpt.valueOr:
    if not node.wakuLightPush.isNil():
      RemotePeerInfo.init(node.peerId())
    elif not node.wakuLightpushClient.isNil():
      node.peerManager.selectPeer(WakuLightPushCodec).valueOr:
        let msg = "no suitable remote peers"
        debug "Failed to publish message", msg = msg
        return lighpushErrorResult(LightPushErrorCode.NO_PEERS_TO_RELAY, msg)
    else:
      return lighpushErrorResult(
        LightPushErrorCode.NO_PEERS_TO_RELAY, "no suitable remote peers"
      )

  let pubsubForPublish = pubsubTopic.valueOr:
    if node.wakuAutoSharding.isNone():
      let msg = "Pubsub topic must be specified when static sharding is enabled"
      debug "Lightpush publish error", error = msg
      return lighpushErrorResult(LightPushErrorCode.INVALID_MESSAGE, msg)

    let parsedTopic = NsContentTopic.parse(message.contentTopic).valueOr:
      let msg = "Invalid content-topic:" & $error
      debug "Lightpush request handling error", error = msg
      return lighpushErrorResult(LightPushErrorCode.INVALID_MESSAGE, msg)

    node.wakuAutoSharding.get().getShard(parsedTopic).valueOr:
      let msg = "Autosharding error: " & error
      debug "Lightpush publish error", error = msg
      return lighpushErrorResult(LightPushErrorCode.INTERNAL_SERVER_ERROR, msg)

  # toRLNSignal hashes the timestamp into the proof, so fix it before proof gen;
  # the downstream ensureTimestampSet then becomes a no-op.
  let message = ensureTimestampSet(message)

  let rln =
    if node.rln.isNil():
      Opt.none(Rln)
    else:
      Opt.some(node.rln)
  let msgWithProof = (await checkAndGenerateRLNProof(rln, message)).valueOr:
    return lighpushErrorResult(LightPushErrorCode.OUT_OF_RLN_PROOF, error)

  let firstResult =
    await lightpushPublishHandler(node, pubsubForPublish, msgWithProof, toPeer, mixify)

  # Gate the refresh on unambiguously RLN-related failures: 504
  # (OUT_OF_RLN_PROOF) is always RLN; 420 (INVALID_MESSAGE) also covers non-RLN
  # rejections (e.g. oversized), so additionally require RlnValidatorErrorMsg.
  if firstResult.isOk() or rln.isNone():
    return firstResult
  let isRlnRelatedFailure =
    firstResult.error.code == LightPushErrorCode.OUT_OF_RLN_PROOF or (
      firstResult.error.code == LightPushErrorCode.INVALID_MESSAGE and
      firstResult.error.desc.get("").contains(RlnValidatorErrorMsg)
    )
  if not isRlnRelatedFailure:
    return firstResult

  # Schedule the refresh and return immediately, normalized to 504 with
  # RlnProofRefreshScheduledMsg so callers can tell "stale proof, retry" from a
  # permanent rejection. A retry regenerates against the refreshed cache.
  debug "lightpush send rejected as RLN-invalid; scheduling merkle proof refresh",
    statusCode = $firstResult.error.code
  rln.get().groupManager.scheduleMerkleProofRefresh()
  return lighpushErrorResult(
    LightPushErrorCode.OUT_OF_RLN_PROOF,
    RlnProofRefreshScheduledMsg & ": " &
      firstResult.error.desc.get($firstResult.error.code),
  )

proc lightpushPublishOverMix*(
    node: WakuNode,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    exit: RemotePeerInfo,
): Future[MixSendOutcome] {.async.} =
  ## The mix counterpart of `lightpushPublish`: one request through mix to
  ## `exit`, a lightpush server that is a mix node and the last node of the
  ## sphinx path. Attaches the RLN proof as `lightpushPublish` does, and
  ## schedules a merkle proof refresh when the exit node rejects the proof.
  ## The outcome is typed; see `publishOverMix`.
  when not defined(libp2p_mix_experimental_exit_is_dest):
    return mixNotSent(
      MixSendReason.PreWriteFailure,
      lightpushResultServiceUnavailable(
        "Waku lightpush with mix not available: built without libp2p_mix_experimental_exit_is_dest"
      ),
    )
  else:
    if node.wakuMix.isNil():
      return mixNotSent(
        MixSendReason.PreWriteFailure,
        lightpushResultServiceUnavailable(
          "Waku lightpush with mix not available: mix is not mounted"
        ),
      )

    # toRLNSignal hashes the timestamp into the proof, so fix it before proof
    # generation.
    let message = ensureTimestampSet(message)
    let rln =
      if node.rln.isNil():
        Opt.none(Rln)
      else:
        Opt.some(node.rln)
    let msgWithProof = (await checkAndGenerateRLNProof(rln, message)).valueOr:
      return mixNotSent(
        MixSendReason.PreWriteFailure,
        lighpushErrorResult(LightPushErrorCode.OUT_OF_RLN_PROOF, error),
      )

    let conn = node.wakuMix.toConnection(
      MixDestination.exitNode(exit.peerId),
      WakuLightPushCodec,
      MixParameters(
        expectReply: Opt.some(true),
        numSurbs: Opt.some(MixReplySurbs),
        replyTimeout: Opt.some(MixLibraryReplyTimeout),
      ),
    ).valueOr:
      return mixNotSent(
        MixSendReason.PreWriteFailure,
        lightpushResultServiceUnavailable(
          "Waku lightpush with mix not available: " & error
        ),
      )

    debug "Publishing message with lightpush over mix",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      exit = exit.peerId,
      msg_hash = pubsubTopic.computeMessageHash(message).to0xHex()
    var outcome = await node.publishOverMix(conn, pubsubTopic, msgWithProof)
    outcome.exit = Opt.some(exit.peerId)

    # The same gate as `lightpushPublish`: a proof the exit node rejected is
    # regenerated against a refreshed merkle path, and the answer is
    # normalized so that the caller can tell a stale proof from a refusal.
    if outcome.reason == MixSendReason.ExitRefused and rln.isSome() and
        outcome.answer.isErr():
      let refused = outcome.answer.error
      let isRlnRelatedFailure =
        refused.code == LightPushErrorCode.OUT_OF_RLN_PROOF or (
          refused.code == LightPushErrorCode.INVALID_MESSAGE and
          refused.desc.get("").contains(RlnValidatorErrorMsg)
        )
      if isRlnRelatedFailure:
        debug "lightpush over mix rejected as RLN-invalid; scheduling merkle proof refresh",
          statusCode = $refused.code
        rln.get().groupManager.scheduleMerkleProofRefresh()
        outcome.answer = lighpushErrorResult(
          LightPushErrorCode.OUT_OF_RLN_PROOF,
          RlnProofRefreshScheduledMsg & ": " & refused.desc.get($refused.code),
        )
    return outcome
