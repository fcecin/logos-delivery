import results, libp2p/crypto/crypto
## Waku Filter client for subscribing and receiving filtered messages

{.push raises: [].}

import
  std/[sequtils, sets, tables],
  chronicles,
  chronos,
  libp2p/protocols/protocol,
  libp2p/peerstore,
  stew/byteutils,
  brokers/broker_context

import
  logos_delivery/waku/[node/peer_manager, waku_core, api/events/filter_subscribe_events],
  ./common,
  ./protocol_metrics,
  ./rpc_codec,
  ./rpc,
  ./subscriptions

logScope:
  topics = "waku filter client"

type WakuFilterClient* = ref object of LPProtocol
  brokerCtx: BrokerContext
  rng: crypto.Rng
  peerManager: PeerManager
  pushHandlers: seq[FilterPushHandler]
  requestedCriteria: Table[PeerId, FilterCriteria]
    ## What this client asked each service node to push.

func toCriteria(
    pubsubTopic: PubsubTopic, contentTopics: seq[ContentTopic]
): FilterCriteria =
  return contentTopics.mapIt((pubsubTopic, it)).toHashSet()

proc addCriteria(wfc: WakuFilterClient, servicePeer: PeerId, criteria: FilterCriteria) =
  if criteria.len > 0:
    wfc.requestedCriteria.mgetOrPut(servicePeer, FilterCriteria()).incl(criteria)

proc removeCriteria(
    wfc: WakuFilterClient, servicePeer: PeerId, criteria: FilterCriteria
) =
  let remaining = wfc.requestedCriteria.getOrDefault(servicePeer) - criteria
  if remaining.len > 0:
    wfc.requestedCriteria[servicePeer] = remaining
  else:
    wfc.requestedCriteria.del(servicePeer)

proc forgetDeletedPeers(wfc: WakuFilterClient) =
  ## Removes the criteria of peers that are not connected and not in the peer store.
  let switch = wfc.peerManager.switch
  let deleted = toSeq(wfc.requestedCriteria.keys).filterIt(
      it notin switch.peerStore[ProtoBook] and not switch.isConnected(it)
    )
  for peerId in deleted:
    wfc.requestedCriteria.del(peerId)

proc isRequested(
    wfc: WakuFilterClient, servicePeer: PeerId, msgPush: MessagePush
): bool =
  ## True when this client asked `servicePeer` to push this message.
  ## The spec 12/WAKU2-FILTER asks a client for this check.
  ## A mix exit can keep a message and push it to each node that it suspects.
  ## A push ends the send of its message, so the exit could see which send ends.
  wfc.requestedCriteria.withValue(servicePeer, criteria):
    return (msgPush.pubsubTopic, msgPush.wakuMessage.contentTopic) in criteria[]
  return false

func generateRequestId(rng: crypto.Rng): string =
  var bytes: array[10, byte]
  rng.generate(bytes)
  return byteutils.toHex(bytes)

proc sendSubscribeRequest(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    filterSubscribeRequest: FilterSubscribeRequest,
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  trace "Sending filter subscribe request",
    peerId = servicePeer.peerId, filterSubscribeRequest

  var connOpt: Opt[Connection]
  try:
    connOpt = await wfc.peerManager.dialPeer(servicePeer, WakuFilterSubscribeCodec)
    if connOpt.isNone():
      trace "Failed to dial filter service peer", servicePeer
      logos_delivery_filter_errors.inc(labelValues = [dialFailure])
      return err(FilterSubscribeError.peerDialFailure($servicePeer))
  except CatchableError:
    let errMsg = "failed to dialPeer: " & getCurrentExceptionMsg()
    trace "failed to dialPeer", error = getCurrentExceptionMsg()
    logos_delivery_filter_errors.inc(labelValues = [dialFailure])
    return err(FilterSubscribeError.badResponse(errMsg))

  let connection = connOpt.get()

  defer:
    await connection.closeWithEOF()

  try:
    await connection.writeLP(filterSubscribeRequest.encode())
  except CatchableError:
    let errMsg =
      "exception in waku_filter_v2 client writeLP: " & getCurrentExceptionMsg()
    trace "exception in waku_filter_v2 client writeLP", error = getCurrentExceptionMsg()
    logos_delivery_filter_errors.inc(labelValues = [writeFailure])
    return err(FilterSubscribeError.badResponse(errMsg))

  var respBuf: seq[byte]
  try:
    respBuf = await connection.readLp(DefaultMaxSubscribeResponseSize)
  except CatchableError:
    let errMsg =
      "exception in waku_filter_v2 client readLp: " & getCurrentExceptionMsg()
    trace "exception in waku_filter_v2 client readLp", error = getCurrentExceptionMsg()
    logos_delivery_filter_errors.inc(labelValues = [readFailure])
    return err(FilterSubscribeError.badResponse(errMsg))

  let response = FilterSubscribeResponse.decode(respBuf).valueOr:
    trace "Failed to decode filter subscribe response", servicePeer
    logos_delivery_filter_errors.inc(labelValues = [decodeRpcFailure])
    return err(FilterSubscribeError.badResponse(decodeRpcFailure))

  # DOS protection rate limit checks does not know about request id
  if response.statusCode != FilterSubscribeErrorKind.TOO_MANY_REQUESTS.uint32 and
      response.requestId != filterSubscribeRequest.requestId:
    trace "Filter subscribe response requestId mismatch", servicePeer, response
    logos_delivery_filter_errors.inc(labelValues = [requestIdMismatch])
    return err(FilterSubscribeError.badResponse(requestIdMismatch))

  if response.statusCode != 200:
    trace "Filter subscribe error response", servicePeer, response
    logos_delivery_filter_errors.inc(labelValues = [errorResponse])
    let cause =
      if response.statusDesc.isSome():
        response.statusDesc.get()
      else:
        "filter subscribe error"
    return err(FilterSubscribeError.parse(response.statusCode, cause = cause))

  return ok()

proc sendUnsubscribeRequest(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    request: FilterSubscribeRequest,
    criteria: FilterCriteria,
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  wfc.removeCriteria(servicePeer.peerId, criteria)
  (await wfc.sendSubscribeRequest(servicePeer, request)).isOkOr:
    # A service node that does not unsubscribe this client still pushes.
    if error.kind != FilterSubscribeErrorKind.NOT_FOUND:
      wfc.addCriteria(servicePeer.peerId, criteria)
    return err(error)
  return ok()

proc ping*(
    wfc: WakuFilterClient, servicePeer: RemotePeerInfo, timeout = chronos.seconds(0)
): Future[FilterSubscribeResult] {.async.} =
  debug "Sending ping", servicePeer = shortLog($servicePeer)
  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest = FilterSubscribeRequest.ping(requestId)

  if timeout > chronos.seconds(0):
    let fut = wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)
    if not await fut.withTimeout(timeout):
      return err(
        FilterSubscribeError.parse(uint32(FilterSubscribeErrorKind.PEER_DIAL_FAILURE))
      )
    return fut.read()

  return await wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)

proc subscribe*(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    contentTopics: ContentTopic | seq[ContentTopic],
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  var contentTopicSeq: seq[ContentTopic]
  when contentTopics is seq[ContentTopic]:
    contentTopicSeq = contentTopics
  else:
    contentTopicSeq = @[contentTopics]

  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest = FilterSubscribeRequest.subscribe(
    requestId = requestId, pubsubTopic = pubsubTopic, contentTopics = contentTopicSeq
  )

  # A service node can push before its response, and also when its response gets lost.
  wfc.addCriteria(servicePeer.peerId, toCriteria(pubsubTopic, contentTopicSeq))
  ?await wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)
  wfc.forgetDeletedPeers()

  OnFilterSubscribeEvent.emit(wfc.brokerCtx, pubsubTopic, contentTopicSeq)

  return ok()

proc unsubscribe*(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    contentTopics: ContentTopic | seq[ContentTopic],
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  var contentTopicSeq: seq[ContentTopic]
  when contentTopics is seq[ContentTopic]:
    contentTopicSeq = contentTopics
  else:
    contentTopicSeq = @[contentTopics]

  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest = FilterSubscribeRequest.unsubscribe(
    requestId = requestId, pubsubTopic = pubsubTopic, contentTopics = contentTopicSeq
  )

  let criteria =
    wfc.requestedCriteria.getOrDefault(servicePeer.peerId) *
    toCriteria(pubsubTopic, contentTopicSeq)
  ?await wfc.sendUnsubscribeRequest(servicePeer, filterSubscribeRequest, criteria)

  OnFilterUnSubscribeEvent.emit(wfc.brokerCtx, pubsubTopic, contentTopicSeq)

  return ok()

proc unsubscribeAll*(
    wfc: WakuFilterClient, servicePeer: RemotePeerInfo
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest =
    FilterSubscribeRequest.unsubscribeAll(requestId = requestId)

  let criteria = wfc.requestedCriteria.getOrDefault(servicePeer.peerId)
  return await wfc.sendUnsubscribeRequest(servicePeer, filterSubscribeRequest, criteria)

proc registerPushHandler*(wfc: WakuFilterClient, handler: FilterPushHandler) =
  wfc.pushHandlers.add(handler)

proc initProtocolHandler(wfc: WakuFilterClient) =
  proc handler(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    ## Notice that the client component is acting as a server of WakuFilterPushCodec messages
    while not conn.atEof():
      var buf: seq[byte]
      try:
        buf = await conn.readLp(int(DefaultMaxPushSize))
      except CancelledError, LPStreamError:
        debug "Error while reading conn", error = getCurrentExceptionMsg()

      let msgPush = MessagePush.decode(buf).valueOr:
        debug "Failed to decode message push", peerId = conn.peerId, error = $error
        logos_delivery_filter_errors.inc(labelValues = [decodeRpcFailure])
        return

      let msg_hash =
        computeMessageHash(msgPush.pubsubTopic, msgPush.wakuMessage).to0xHex()

      trace "Received message push",
        peerId = conn.peerId,
        msg_hash,
        payload = shortLog(msgPush.wakuMessage.payload),
        pubsubTopic = msgPush.pubsubTopic,
        content_topic = msgPush.wakuMessage.contentTopic,
        conn

      if not wfc.isRequested(conn.peerId, msgPush):
        trace "Dropping a message push that this client did not ask for",
          peerId = conn.peerId, msg_hash
        logos_delivery_filter_errors.inc(labelValues = [unsolicitedPush])
        continue

      for handler in wfc.pushHandlers:
        asyncSpawn handler(msgPush.pubsubTopic, msgPush.wakuMessage)

      # Protocol specifies no response for now

  wfc.handler = handler
  wfc.codec = WakuFilterPushCodec

proc new*(T: type WakuFilterClient, peerManager: PeerManager, rng: crypto.Rng): T =
  let brokerCtx = globalBrokerContext()
  let wfc = WakuFilterClient(
    brokerCtx: brokerCtx, rng: rng, peerManager: peerManager, pushHandlers: @[]
  )
  wfc.initProtocolHandler()
  wfc
