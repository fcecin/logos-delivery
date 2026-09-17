import results, libp2p/crypto/crypto
## Waku Filter client for subscribing and receiving filtered messages

{.push raises: [].}

import
  chronicles,
  chronos,
  libp2p/protocols/protocol,
  bearssl/rand,
  stew/byteutils,
  brokers/broker_context

import
  logos_delivery/waku/[node/peer_manager, waku_core, api/events/filter_subscribe_events],
  ./common,
  ./protocol_metrics,
  ./rpc_codec,
  ./rpc

logScope:
  topics = "waku filter client"

type WakuFilterClient* = ref object of LPProtocol
  brokerCtx: BrokerContext
  rng: crypto.Rng
  peerManager: PeerManager
  pushHandlers: seq[FilterPushHandler]

func generateRequestId(rng: crypto.Rng): string =
  var bytes: array[10, byte]
  rng.generate(bytes)
  return byteutils.toHex(bytes)

proc sendSubscribeRequest(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    filterSubscribeRequest: FilterSubscribeRequest,
): Future[FilterSubscribeResult] {.async: (raises: [CancelledError]).} =
  trace "Sending filter subscribe request",
    peerId = servicePeer.peerId, filterSubscribeRequest

  let connection =
    try:
      (await wfc.peerManager.dialPeer(servicePeer, WakuFilterSubscribeCodec)).valueOr:
        trace "Failed to dial filter service peer", servicePeer
        logos_delivery_filter_errors.inc(labelValues = [dialFailure])
        return err(FilterSubscribeError.peerDialFailure($servicePeer))
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      let errMsg = "failed to dialPeer: " & exc.msg
      trace "failed to dialPeer", error = exc.msg
      logos_delivery_filter_errors.inc(labelValues = [errMsg])
      return err(FilterSubscribeError.badResponse(errMsg))

  var cancelled = false
  defer:
    wfc.peerManager.closeDetached(connection, withEof = not cancelled)

  try:
    await connection.writeLP(filterSubscribeRequest.encode().buffer)
  except CancelledError as exc:
    cancelled = true
    raise exc
  except CatchableError as exc:
    let errMsg = "exception in waku_filter_v2 client writeLP: " & exc.msg
    trace "exception in waku_filter_v2 client writeLP", error = exc.msg
    logos_delivery_filter_errors.inc(labelValues = [errMsg])
    return err(FilterSubscribeError.badResponse(errMsg))

  let respBuf =
    try:
      await connection.readLp(DefaultMaxSubscribeResponseSize)
    except CancelledError as exc:
      cancelled = true
      raise exc
    except CatchableError as exc:
      let errMsg = "exception in waku_filter_v2 client readLp: " & exc.msg
      trace "exception in waku_filter_v2 client readLp", error = exc.msg
      logos_delivery_filter_errors.inc(labelValues = [errMsg])
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
): Future[FilterSubscribeResult] {.async: (raises: [CancelledError]).} =
  var contentTopicSeq: seq[ContentTopic]
  when contentTopics is seq[ContentTopic]:
    contentTopicSeq = contentTopics
  else:
    contentTopicSeq = @[contentTopics]

  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest = FilterSubscribeRequest.subscribe(
    requestId = requestId, pubsubTopic = pubsubTopic, contentTopics = contentTopicSeq
  )

  ?await wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)

  OnFilterSubscribeEvent.emit(wfc.brokerCtx, pubsubTopic, contentTopicSeq)

  return ok()

proc unsubscribe*(
    wfc: WakuFilterClient,
    servicePeer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    contentTopics: ContentTopic | seq[ContentTopic],
): Future[FilterSubscribeResult] {.async: (raises: [CancelledError]).} =
  var contentTopicSeq: seq[ContentTopic]
  when contentTopics is seq[ContentTopic]:
    contentTopicSeq = contentTopics
  else:
    contentTopicSeq = @[contentTopics]

  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest = FilterSubscribeRequest.unsubscribe(
    requestId = requestId, pubsubTopic = pubsubTopic, contentTopics = contentTopicSeq
  )

  ?await wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)

  OnFilterUnSubscribeEvent.emit(wfc.brokerCtx, pubsubTopic, contentTopicSeq)

  return ok()

proc unsubscribeAll*(
    wfc: WakuFilterClient, servicePeer: RemotePeerInfo
): Future[FilterSubscribeResult] {.async: (raises: [CancelledError]).} =
  let requestId = generateRequestId(wfc.rng)
  let filterSubscribeRequest =
    FilterSubscribeRequest.unsubscribeAll(requestId = requestId)

  return await wfc.sendSubscribeRequest(servicePeer, filterSubscribeRequest)

proc registerPushHandler*(wfc: WakuFilterClient, handler: FilterPushHandler) =
  wfc.pushHandlers.add(handler)

proc initProtocolHandler(wfc: WakuFilterClient) =
  proc handler(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    ## Notice that the client component is acting as a server of WakuFilterPushCodec messages
    while not conn.atEof():
      let buf =
        try:
          await conn.readLp(int(DefaultMaxPushSize))
        except CancelledError as exc:
          raise exc
        except LPStreamEOFError:
          return
        except LPStreamError as exc:
          debug "Error while reading conn", error = exc.msg
          return

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
