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
  libp2p/builders,
  libp2p/transports/tcptransport,
  libp2p/transports/wstransport,
  libp2p/stream/connection,
  libp2p/varint,
  libp2p_mix

import
  ../waku_node,
  ../../waku_core,
  ../../waku_core/topics/sharding,
  ../../waku_lightpush_legacy/client as legacy_lightpush_client,
  ../../waku_lightpush_legacy as legacy_lightpush_protocol,
  ../../waku_lightpush/client as lightpush_client,
  ../../waku_lightpush as lightpush_protocol,
  ../peer_manager,
  ../../common/rate_limit/setting,
  ../../rln,
  ../../rln/rln_plugin,
  ../../utils/requests,
  ../../waku_mix
from ../../rln/types import RlnErrorKind

logScope:
  topics = "waku node lightpush api"

const MountWithoutRelayError* = "cannot mount lightpush because relay is not mounted"

const MixLightpushSurbs* = 1'u8
  ## The number of SURBs in a lightpush request over mix. One SURB, so the reply
  ## uses one path.

proc mixLightpushSize*(
    pubsubTopic: PubsubTopic, message: WakuMessage
): tuple[size, limit: int] =
  ## The bytes that the lightpush client writes on a mix connection for
  ## `message`, and the bytes that remain in one mix message after the codec and
  ## the SURBs. `message` must have its timestamp and its proof.
  let request = LightpushRequest(
    requestId: '0'.repeat(20), # `generateRequestId` gives 20 hex characters
    pubsubTopic: Opt.some(pubsubTopic),
    message: message,
  )
  let encoded = request.encode().len
  # `writeLp` adds the length prefix in the same write.
  let size = vsizeof(uint64(encoded)) + encoded
  let limit = getMaxMessageSizeForCodec(WakuLightPushCodec, MixLightpushSurbs).valueOr:
    error "Could not calculate mix message capacity", error = $error
    0
  return (size, limit)

type
  MixPublishOutcome* {.pure.} = enum
    ## What a lightpush over mix knows about its message when it returns.
    Confirmed ## The exit answered, and the network has the message.
    Unconfirmed ## The request left this node, and no answer came in time.
    Failed ## The request did not leave this node, or the exit refused it.

  MixPublishResult* = object
    outcome*: MixPublishOutcome
    response*: lightpush_protocol.WakuLightPushResult
      ## The answer of the exit, or the fault, as the lightpush client reports it.

  MixRequestLeftHandler* = proc() {.gcsafe, raises: [].}
    ## Runs when the request has left this node, before the wait for the answer.
    ## A caller that a cancel ends during that wait then knows that the message
    ## is on its way.

func mixPublishFailed*(code: LightpushStatusCode, desc: string): MixPublishResult =
  MixPublishResult(
    outcome: MixPublishOutcome.Failed, response: lighpushErrorResult(code, desc)
  )

func mixPublishUnconfirmed(desc: string): MixPublishResult =
  MixPublishResult(
    outcome: MixPublishOutcome.Unconfirmed,
    response: lighpushErrorResult(LightPushErrorCode.SERVICE_NOT_AVAILABLE, desc),
  )

func confirms(response: lightpush_protocol.WakuLightPushResult): bool =
  ## True when the answer of the exit says that the network has the message. A
  ## relay that already has the message refuses it with `AlreadySeenMessageDesc`.
  response.isOk() or (
    response.error.code == LightPushErrorCode.INVALID_MESSAGE and
    response.error.desc == Opt.some(AlreadySeenMessageDesc)
  )

proc publishOverMix*(
    node: WakuNode,
    conn: Connection,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    replyTimeout: Duration = MixReplyTimeout,
    onRequestLeft: MixRequestLeftHandler = nil,
): Future[MixPublishResult] {.async.} =
  ## Publishes on a mix connection. The write carries the request to the first
  ## hop, and the read waits for the answer of the exit. Each gets `replyTimeout`.
  ## `onRequestLeft` runs between the two.
  ## The mix connection bounds neither the first-hop dial nor the write on its
  ## own, and it waits longer for the answer than one attempt should.
  try:
    let request = LightpushRequest(
      requestId: generateRequestId(node.rng),
      pubSubTopic: Opt.some(pubsubTopic),
      message: ensureTimestampSet(message),
    )
    let writing = conn.writeLp(request.encode())
    if not await writing.withTimeout(replyTimeout):
      writing.cancelSoon()
      await conn.reset()
      debug "Mix lightpush request did not leave this node in time",
        pubsubTopic = pubsubTopic, timeout = replyTimeout
      return mixPublishFailed(
        LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        "Waku lightpush over mix timed out before the request left this node",
      )
    if writing.failed():
      await conn.reset()
      debug "Mix lightpush request could not be sent",
        pubsubTopic = pubsubTopic, error = writing.error.msg
      return mixPublishFailed(
        LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        "Waku lightpush over mix could not send the request: " & writing.error.msg,
      )

    # From here on the request is on its way, so a lost answer leaves the message
    # on the network with no confirmation.
    if not onRequestLeft.isNil():
      onRequestLeft()
    let reading = conn.readLp(lightpush_protocol.DefaultMaxPushResponseSize)
    if not await reading.withTimeout(replyTimeout):
      reading.cancelSoon()
      await conn.reset()
      debug "Mix lightpush got no answer in time",
        pubsubTopic = pubsubTopic, timeout = replyTimeout
      return mixPublishUnconfirmed("Waku lightpush over mix timed out")
    await conn.closeWithEOF()
    if reading.failed():
      debug "Mix lightpush could not read the answer",
        pubsubTopic = pubsubTopic, error = reading.error.msg
      return mixPublishUnconfirmed(
        "Waku lightpush over mix could not read the answer: " & reading.error.msg
      )
    let response = LightPushResponse.decode(reading.read()).valueOr:
      debug "Mix lightpush could not decode the answer", pubsubTopic = pubsubTopic
      return
        mixPublishUnconfirmed("Waku lightpush over mix got an answer it cannot decode")
    if response.requestId != request.requestId and
        response.statusCode notin
        [LightPushErrorCode.TOO_MANY_REQUESTS, LightPushErrorCode.PAYLOAD_TOO_LARGE] and
        not (
          response.statusCode == LightPushErrorCode.BAD_REQUEST and
          response.requestId == UnknownRequestId
        ):
      debug "Mix lightpush got an answer for another request",
        requestId = request.requestId, responseRequestId = response.requestId
      return mixPublishUnconfirmed(
        "Waku lightpush over mix got an answer for another request"
      )
    let answer = toPushResult(response)
    return MixPublishResult(
      outcome:
        if answer.confirms(): MixPublishOutcome.Confirmed else: MixPublishOutcome.Failed,
      response: answer,
    )
  except CancelledError as exc:
    # A cancel from the send service ends the attempt. Nothing reads the
    # answer, so the connection goes too.
    await conn.reset()
    raise exc

proc mixPublish*(
    node: WakuNode,
    peer: RemotePeerInfo | PeerInfo,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    replyTimeout: Duration,
    onRequestLeft: MixRequestLeftHandler = nil,
): Future[MixPublishResult] {.async.} =
  ## Opens a mix connection to `peer` as the exit and publishes on it.
  when defined(libp2p_mix_experimental_exit_is_dest):
    #TODO: How to handle multiple addresses?
    let conn = node.wakuMix.toConnection(
      MixDestination.exitNode(peer.peerId),
      WakuLightPushCodec,
      MixParameters(expectReply: Opt.some(true), numSurbs: Opt.some(MixLightpushSurbs)),
        # indicating we only want a single path to be used for reply hence numSurbs = 1
    ).valueOr:
      debug "Could not create mix connection"
      return mixPublishFailed(
        LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        "Waku lightpush with mix not available",
      )
    return
      await node.publishOverMix(conn, pubsubTopic, message, replyTimeout, onRequestLeft)
  else:
    # Fail the build here, not every mixed send at runtime
    {.error: "mix sends need -d:libp2p_mix_experimental_exit_is_dest".}

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
    node.peerManager,
    node.rng,
    pushHandler,
    Opt.some(rateLimit),
    node.wakuRelay.maxEncodedMessageSize,
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

  let msgWithProof = (await attachProof(node.rlnPlugin, message)).valueOr:
    return err("failed to attach RLN proof: " & $error)

  try:
    let pubsubForPublish = resolveLegacyPubsubTopic(
      node, pubsubTopic, message.contentTopic
    ).valueOr:
      return err(error)

    let publishResult =
      await internalLegacyLightpushPublish(node, pubsubForPublish, msgWithProof, peer)

    # Legacy has no status codes, so string-match the RLN error to detect a
    # rejected proof. Ask the backend to refresh what proofs are built against
    # and hand the error back: retrying is the caller's decision, the same way
    # the non-legacy path behaves. A backend without a refresh hook gets the
    # error back unchanged.
    if publishResult.isOk() or not publishResult.error.contains(RlnValidatorErrorMsg):
      return publishResult
    if not node.rlnPlugin.notifyProofRejected():
      return publishResult

    debug "legacy lightpush send rejected as RLN-invalid; RLN proof refresh scheduled"
    return err(RlnProofRefreshScheduledMsg & ": " & publishResult.error)
  except CatchableError:
    return err(getCurrentExceptionMsg())

# TODO: Move to application module (e.g., logosdeliverynode.nim)
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
    node.peerManager,
    node.rng,
    pushHandler,
    node.wakuAutoSharding,
    Opt.some(rateLimit),
    node.wakuRelay.maxEncodedMessageSize,
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
  # The client is always mounted alongside the service, and it cannot dial
  # this node's own peer id.
  let toSelf =
    not node.wakuLightPush.isNil() and peer.peerId == node.peerId() and not mixify
  if not node.wakuLightpushClient.isNil() and not toSelf:
    debug "Publishing message with lightpush",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      target_peer_id = peer.peerId,
      msg_hash = msgHash,
      mixify = mixify
    if mixify:
      let published =
        await node.mixPublish(peer, pubsubTopic, message, node.wakuMix.replyTimeout)
      return published.response
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

proc prepareLightpushMessage(
    node: WakuNode, pubsubTopic: PubsubTopic, message: WakuMessage, mixify: bool
): Future[Result[WakuMessage, ErrorStatus]] {.async.} =
  ## Stamps the message, checks its size for mix, and attaches the RLN proof.
  # toRLNSignal hashes the timestamp into the proof, so fix it before proof gen;
  # the downstream ensureTimestampSet then becomes a no-op.
  let message = ensureTimestampSet(message)

  if mixify:
    # A backend that cannot declare its proof size is a node defect.
    let sizedMessage = node.rlnPlugin.messageForSizeCheck(message).valueOr:
      return err(
        (
          LightPushErrorCode.INTERNAL_SERVER_ERROR,
          Opt.some("failed to determine RLN proof size: " & $error),
        )
      )
    let (size, limit) = mixLightpushSize(pubsubTopic, sizedMessage)
    if size > limit:
      return err(
        (
          LightPushErrorCode.INVALID_MESSAGE,
          Opt.some(
            "message too large for mix: the request has " & $size &
              " bytes, the limit is " & $limit & " bytes"
          ),
        )
      )

  # A Permanent failure comes from the message itself, such as a timestamp
  # outside the validators' bound, so it is the client's INVALID_MESSAGE, as
  # the relay validator answered before proof generation checked it. Any
  # other failure is the node's: it has no proof to offer right now.
  let msgWithProof = (await attachProof(node.rlnPlugin, message)).valueOr:
    let code =
      if error.kind == RlnErrorKind.Permanent:
        LightPushErrorCode.INVALID_MESSAGE
      else:
        LightPushErrorCode.OUT_OF_RLN_PROOF
    return err((code, Opt.some("failed to attach RLN proof: " & $error)))
  return ok(msgWithProof)

proc refreshRlnProofOnRejection(
    node: WakuNode, firstResult: lightpush_protocol.WakuLightPushResult
): lightpush_protocol.WakuLightPushResult =
  ## Schedules an RLN proof refresh when the service refused the proof, and
  ## reports the refusal as `OUT_OF_RLN_PROOF`.
  # Gate the refresh on unambiguously RLN-related failures: 504
  # (OUT_OF_RLN_PROOF) is always RLN; 420 (INVALID_MESSAGE) also covers non-RLN
  # rejections (e.g. oversized), so additionally require RlnValidatorErrorMsg.
  if firstResult.isOk():
    return firstResult
  let isRlnRelatedFailure =
    firstResult.error.code == LightPushErrorCode.OUT_OF_RLN_PROOF or (
      firstResult.error.code == LightPushErrorCode.INVALID_MESSAGE and
      firstResult.error.desc.get("").contains(RlnValidatorErrorMsg)
    )
  if not isRlnRelatedFailure:
    return firstResult

  # Ask the backend to refresh what proofs are built against and return
  # immediately, normalized to 504 with RlnProofRefreshScheduledMsg so callers
  # can tell "stale proof, retry" from a permanent rejection. A backend without
  # a refresh hook gets the result back unchanged.
  if not node.rlnPlugin.notifyProofRejected():
    return firstResult
  debug "lightpush send rejected as RLN-invalid; RLN proof refresh scheduled",
    statusCode = $firstResult.error.code
  return lighpushErrorResult(
    LightPushErrorCode.OUT_OF_RLN_PROOF,
    RlnProofRefreshScheduledMsg & ": " &
      firstResult.error.desc.get($firstResult.error.code),
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

  let msgWithProof = (
    await node.prepareLightpushMessage(pubsubForPublish, message, mixify)
  ).valueOr:
    return err(error)

  let firstResult =
    await lightpushPublishHandler(node, pubsubForPublish, msgWithProof, toPeer, mixify)
  return node.refreshRlnProofOnRejection(firstResult)

proc lightpushPublishOverMix*(
    node: WakuNode,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    peer: RemotePeerInfo,
    onRequestLeft: MixRequestLeftHandler = nil,
): Future[MixPublishResult] {.async.} =
  ## Publishes `message` through `peer` as the mix exit, and tells whether the
  ## exit confirmed it, whether the request left with no answer, or what failed.
  if node.wakuLightpushClient.isNil() or node.wakuMix.isNil():
    debug "Failed to publish message using mix as mix or lightpush is not mounted"
    return mixPublishFailed(
      LightPushErrorCode.SERVICE_NOT_AVAILABLE, "Waku lightpush with mix not available"
    )
  let msgWithProof = (await node.prepareLightpushMessage(pubsubTopic, message, true)).valueOr:
    return MixPublishResult(
      outcome: MixPublishOutcome.Failed,
      response: lightpush_protocol.WakuLightPushResult.err(error),
    )
  var published = await node.mixPublish(
    peer, pubsubTopic, msgWithProof, node.wakuMix.replyTimeout, onRequestLeft
  )
  if published.outcome == MixPublishOutcome.Failed:
    published.response = node.refreshRlnProofOnRejection(published.response)
  return published
