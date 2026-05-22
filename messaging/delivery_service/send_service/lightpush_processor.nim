import chronicles, chronos, results
import std/options
import brokers/broker_context
import waku/waku_core
import waku/waku_lightpush/rpc  # LightPushStatusCode
import waku/waku_lightpush/common  # LightPushErrorCode constants
import waku/waku_core/codecs  # WakuLightPushCodec
import waku/api/requests/lightpush as kernel_lightpush_api
import waku/api/requests/peers as kernel_peers_api

import ./[delivery_task, send_processor]

logScope:
  topics = "send service lightpush processor"

type LightpushSendProcessor* = ref object of BaseSendProcessor

proc new*(
    T: typedesc[LightpushSendProcessor], brokerCtx: BrokerContext
): T =
  return T(brokerCtx: brokerCtx)

proc isLightpushPeerAvailable(
    self: LightpushSendProcessor, pubsubTopic: PubsubTopic
): bool =
  let req = kernel_peers_api.RequestSelectPeer.request(
    self.brokerCtx, WakuLightPushCodec, some(pubsubTopic)
  ).valueOr:
    debug "isLightpushPeerAvailable: broker err", error = error
    return false
  return req.peer.isSome()

method isValidProcessor*(
    self: LightpushSendProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return self.isLightpushPeerAvailable(task.pubsubTopic)

method sendImpl*(
    self: LightpushSendProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.tryCount.inc()
  info "Trying message delivery via Lightpush",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  let peerReq = kernel_peers_api.RequestSelectPeer.request(
    self.brokerCtx, WakuLightPushCodec, some(task.pubsubTopic)
  ).valueOr:
    debug "LightpushSendProcessor.sendImpl: peer broker err", error = error
    task.state = DeliveryState.NextRoundRetry
    return
  if peerReq.peer.isNone():
    debug "No peer available for Lightpush, request pushed back for next round",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry
    return
  let peer = peerReq.peer.get()

  let pubReq = (
    await kernel_lightpush_api.RequestLightpushPublish.request(
      self.brokerCtx, peer, task.pubsubTopic, task.msg
    )
  ).valueOr:
    error "LightpushSendProcessor.sendImpl: broker err", error = error
    task.state = DeliveryState.NextRoundRetry
    return

  if pubReq.publishError.isSome():
    let code = pubReq.publishError.get()
    error "LightpushSendProcessor.sendImpl failed",
      code = $code, desc = pubReq.errorDesc
    case code
    of LightPushErrorCode.NO_PEERS_TO_RELAY, LightPushErrorCode.TOO_MANY_REQUESTS,
        LightPushErrorCode.OUT_OF_RLN_PROOF, LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        LightPushErrorCode.INTERNAL_SERVER_ERROR:
      task.state = DeliveryState.NextRoundRetry
    else:
      # malformed message
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = pubReq.errorDesc
      task.deliveryTime = Moment.now()
    return

  let numLightpushServers = pubReq.relayedPeerCount
  if numLightpushServers > 0:
    info "Message propagated via Lightpush",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.SuccessfullyPropagated
    task.deliveryTime = Moment.now()
  else:
    # Controversial state, publish says ok but no peer. It should not happen.
    debug "Lightpush publish returned zero peers, request pushed back for next round",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry

  return
