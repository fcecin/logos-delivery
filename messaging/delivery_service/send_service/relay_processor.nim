import std/options
import chronos, chronicles
import brokers/broker_context
import waku/waku_core
import waku/waku_relay/protocol # PublishOutcome
import waku/api/requests/health
import waku/api/requests/relay as kernel_relay_api
import messaging/api/types
import ./[delivery_task, send_processor]

logScope:
  topics = "send service relay processor"

type RelaySendProcessor* = ref object of BaseSendProcessor
  fallbackStateToSet: DeliveryState

proc new*(
    T: typedesc[RelaySendProcessor],
    lightpushAvailable: bool,
    brokerCtx: BrokerContext,
): RelaySendProcessor =
  let fallbackStateToSet =
    if lightpushAvailable:
      DeliveryState.FallbackRetry
    else:
      DeliveryState.FailedToDeliver

  return RelaySendProcessor(
    fallbackStateToSet: fallbackStateToSet,
    brokerCtx: brokerCtx,
  )

proc isTopicHealthy(self: RelaySendProcessor, topic: PubsubTopic): bool {.gcsafe.} =
  let healthReport = RequestShardTopicsHealth.request(self.brokerCtx, @[topic]).valueOr:
    error "isTopicHealthy: failed to get health report", topic = topic, error = error
    return false

  if healthReport.topicHealth.len() < 1:
    warn "isTopicHealthy: no topic health entries", topic = topic
    return false
  let health = healthReport.topicHealth[0].health
  debug "isTopicHealthy: topic health is ", topic = topic, health = health
  return health == MINIMALLY_HEALTHY or health == SUFFICIENTLY_HEALTHY

method isValidProcessor*(
    self: RelaySendProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  # Topic health query is not reliable enough after a fresh subscribe...
  # return self.isTopicHealthy(task.pubsubTopic)
  return true

method sendImpl*(self: RelaySendProcessor, task: DeliveryTask) {.async.} =
  task.tryCount.inc()
  info "Trying message delivery via Relay",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  let pubReq = (
    await kernel_relay_api.RequestRelayPublish.request(
      self.brokerCtx, task.pubsubTopic, task.msg
    )
  ).valueOr:
    # Broker-level failure: publish provider unreachable. Fail permanently.
    error "RelaySendProcessor.sendImpl: broker err", error = error
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = error
    return

  # RLN proof failure: permanent failure.
  if pubReq.rlnProofFailed:
    error "RelaySendProcessor: RLN proof generation failed",
      request = task.requestId, msgHash = task.msgHash.to0xHex(), error = pubReq.errorDesc
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = pubReq.errorDesc
    return

  # Message validation failure: permanent failure (malformed).
  if pubReq.validationFailed:
    error "RelaySendProcessor: message validation failed",
      request = task.requestId, msgHash = task.msgHash.to0xHex(), error = pubReq.errorDesc
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = pubReq.errorDesc
    return

  # Underlying wakuRelay.publish failure mode.
  if pubReq.publishError.isSome():
    error "Failed to publish message with relay",
      request = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      error = pubReq.errorDesc
    case pubReq.publishError.get()
    of NoPeersToPublish:
      task.state = self.fallbackStateToSet
    else:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = pubReq.errorDesc
    return

  let noOfPublishedPeers = pubReq.relayedPeerCount
  if noOfPublishedPeers > 0:
    info "Message propagated via Relay",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      noOfPeers = noOfPublishedPeers
    task.state = DeliveryState.SuccessfullyPropagated
    task.deliveryTime = Moment.now()
  else:
    # It shall not happen, but still covering it
    task.state = self.fallbackStateToSet
