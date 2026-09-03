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
  libp2p/builders,
  libp2p/transports/tcptransport,
  libp2p/transports/wstransport,
  libp2p_mix

import
  ../waku_node,
  ../../waku_mix,
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
  ## Time limit for the reply to a mix-routed lightpush request. A broken path
  ## costs one attempt, not the full send. The limit is short so that an
  ## attempt with no reply does not hold its batch of retries for long. The
  ## mix entry connection gets the same value as `MixParameters.replyTimeout`,
  ## so the two layers agree.

const MixLibraryReplyTimeout* = MixReplyTimeout + MixReplyTimeout
  ## The reply time limit given to the mix entry connection. Twice the send
  ## path's limit, so that the timer of `publishOverMix` fires first and a
  ## lost reply has one description (`MixReplyTimeoutDesc`). The library's
  ## limit is the backstop.

const MixReadOnceErrorPrefix = "error in readOnce: "
  ## The mix entry connection wraps an unexpected error of its read in the
  ## exact `LPStreamError` type with this prefix (`entry_connection.nim`,
  ## `readOnce`). The read comes after the write.

const MixReplyTimeoutDesc* = "Waku lightpush over mix timed out"
  ## The description of the result of a mix attempt that got no reply within
  ## `MixReplyTimeout`. The mix processor reads it: such an attempt may have
  ## reached the exit node.

const MixReplyUnreadableDesc* = "Waku lightpush over mix: the reply could not be read"
  ## The start of the description of a mix attempt whose request was written
  ## and whose reply could not be read (truncated, reset, closed). The mix
  ## processor reads it: such an attempt may have reached the exit node.

proc publishOverMix*(
    node: WakuNode,
    conn: Connection,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    replyTimeout: Duration = MixReplyTimeout,
): Future[lightpush_protocol.WakuLightPushResult] {.async.} =
  ## Publishes one lightpush request on a mix connection with the time limit
  ## `replyTimeout`. The proc returns when the limit ends, in each failure mode
  ## of the mix path. `replyTimeout` is a parameter so that tests can use a
  ## short limit.
  ##
  ## The mix entry connection has its own reply time limit
  ## (`MixParameters.replyTimeout`, applied in `readOnce`). That limit covers a
  ## reply that does not arrive. It does not cover a stall in the send: mix
  ## dials the first hop with `switch.dial` and not with the peer manager, so
  ## `DefaultDialTimeout` does not apply, and pool entries do not expire. This
  ## proc covers the two cases with one timer. Without it, one dead mix node
  ## stops the send-service loop for all messages of the node.
  ##
  ## `chronos.withTimeout` is not usable here. It cancels the future and then
  ## waits for the cancellation to complete. A stall in the send is inside a
  ## libp2p dial, and this proc must return without a dependence on the
  ## cancellation of that dial. Thus this proc uses `race` with a timer,
  ## resets the stream, and does not wait for the cancellation.
  let publishFut =
    node.wakuLightpushClient.publish(Opt.some(pubsubTopic), message, conn)
  let deadline = sleepAsync(replyTimeout)
  try:
    discard await race(FutureBase(publishFut), FutureBase(deadline))
  except CancelledError as exc:
    # The caller cancels the publish (the send service stops). `race` does not
    # cancel its input futures. Reset the mix stream and cancel the futures in
    # the same way as the time-limit branch below, then let the cancellation
    # go up.
    await conn.reset()
    publishFut.cancelSoon()
    deadline.cancelSoon()
    raise exc

  if not publishFut.finished():
    await conn.reset()
    publishFut.cancelSoon()
    debug "Mix lightpush timed out", pubsubTopic = pubsubTopic, timeout = replyTimeout
    return
      lighpushErrorResult(LightPushErrorCode.SERVICE_NOT_AVAILABLE, MixReplyTimeoutDesc)

  await deadline.cancelAndWait()
  try:
    return await publishFut
  except LPStreamError as exc:
    # A sphinx packet has a fixed size, and this path does not divide messages.
    # A request that the entry connection refuses as too large does not fit on
    # a different path. Report it as too large so that the caller does not
    # retry it.
    # The library has two size checks: the entry connection refuses a frame
    # above `DataSize`, and the packet builder refuses a frame that does not
    # fit next to the return paths.
    if exc.msg.contains("exceeds max msg size") or
        exc.msg.contains("exceeds maximum payload size"):
      debug "Message too large for a mix packet",
        pubsubTopic = pubsubTopic, error = exc.msg
      return lighpushErrorResult(
        LightPushErrorCode.PAYLOAD_TOO_LARGE,
        "message too large for a mix packet: " & exc.msg,
      )
    # libp2p raises a subclass of `LPStreamError` for a reply it cannot read
    # (truncated, reset, closed), and the mix entry connection wraps an
    # unexpected error of its read in the exact type with a known prefix: the
    # request was written, and the exit node may have published the message.
    # The mix library raises the exact type for each failure before the write
    # (the dial of the first hop, the write, a short pool, the packet build).
    # The two get different descriptions, and the mix processor keeps its
    # mark for the first.
    if $exc.name != "LPStreamError" or exc.msg.startsWith(MixReadOnceErrorPrefix):
      debug "Mix lightpush reply unreadable", pubsubTopic = pubsubTopic, error = exc.msg
      return lighpushErrorResult(
        LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        MixReplyUnreadableDesc & ": " & exc.msg,
      )
    # A failure before the write is retryable. The next round builds a new
    # path.
    debug "Mix lightpush failed", pubsubTopic = pubsubTopic, error = exc.msg
    return lighpushErrorResult(
      LightPushErrorCode.SERVICE_NOT_AVAILABLE,
      "Waku lightpush over mix failed: " & exc.msg,
    )

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
    if mixify:
      when defined(libp2p_mix_experimental_exit_is_dest):
        # A sphinx packet has a fixed size. The library gives the payload size
        # that is available next to the return paths. A message that is larger
        # than that cannot go through mix, whatever the request framing adds.
        # The string check in `publishOverMix` covers the framing.
        let maxPayload = getMaxMessageSizeForCodec(WakuLightPushCodec, MixReplySurbs).valueOr:
          return lighpushErrorResult(
            LightPushErrorCode.SERVICE_NOT_AVAILABLE,
            "Waku lightpush with mix not available: " & error,
          )
        let encodedLen = message.encode().buffer.len
        if encodedLen > maxPayload:
          return lighpushErrorResult(
            LightPushErrorCode.PAYLOAD_TOO_LARGE,
            "message too large for a mix packet: " & $encodedLen & " bytes, maximum " &
              $maxPayload,
          )
        # The exit node cannot be the anchor: the library rejects a return
        # path whose last mix hop is the exit node.
        let replyAnchor =
          await node.wakuMix.ensureReplyAnchor(avoid = Opt.some(peer.peerId))
        #TODO: How to handle multiple addresses?
        let conn = node.wakuMix.toConnection(
          MixDestination.exitNode(peer.peerId),
          WakuLightPushCodec,
          MixParameters(
            expectReply: Opt.some(true),
            numSurbs: Opt.some(MixReplySurbs),
            replyTimeout: Opt.some(MixLibraryReplyTimeout),
            replyAnchor: replyAnchor,
          ),
        ).valueOr:
          debug "Could not create mix connection"
          return lighpushErrorResult(
            LightPushErrorCode.SERVICE_NOT_AVAILABLE,
            "Waku lightpush with mix not available",
          )

        return await node.publishOverMix(conn, pubsubTopic, message)
      else:
        # The caller asked for mix. Do not publish in clear text.
        return lighpushErrorResult(
          LightPushErrorCode.SERVICE_NOT_AVAILABLE,
          "Waku lightpush with mix not available: built without libp2p_mix_experimental_exit_is_dest",
        )
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
