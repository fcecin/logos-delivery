import results, chronos, chronicles
import brokers/broker_context
import logos_delivery/waku/[waku_core], logos_delivery/waku/waku_lightpush/[common, rpc]
import logos_delivery/waku/api/publish
import logos_delivery/waku/requests/health_requests
import ./[delivery_task, delivery_plan, send_processor]

logScope:
  topics = "send service relay processor"

type RelaySendProcessor* = ref object of BaseSendProcessor
  publishProc: PushMessageHandler

proc new*(
    T: typedesc[RelaySendProcessor],
    publishProc: PushMessageHandler,
    brokerCtx: BrokerContext,
): RelaySendProcessor =
  return RelaySendProcessor(publishProc: publishProc, brokerCtx: brokerCtx)

proc isTopicHealthy(self: RelaySendProcessor, topic: PubsubTopic): bool {.gcsafe.} =
  let healthReport = RequestShardTopicsHealth.request(self.brokerCtx, @[topic]).valueOr:
    debug "isTopicHealthy: failed to get health report", topic = topic, error = error
    return false

  if healthReport.topicHealth.len() < 1:
    debug "isTopicHealthy: no topic health entries", topic = topic
    return false
  let health = healthReport.topicHealth[0].health
  debug "isTopicHealthy: topic health is ", topic = topic, health = health
  return health == MINIMALLY_HEALTHY or health == SUFFICIENTLY_HEALTHY

method checkBeforeAttempt*(
    self: RelaySendProcessor, task: DeliveryTask
): Result[void, StepResult] {.gcsafe.} =
  # Topic health query is not reliable enough after a fresh subscribe...
  # return self.isTopicHealthy(task.pubsubTopic)
  return ok()

method attempt*(
    self: RelaySendProcessor, task: DeliveryTask
): Future[StepResult] {.async.} =
  debug "Trying message delivery via Relay",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  let noOfPublishedPeers = (await self.publishProc(task.pubsubTopic, task.msg)).valueOr:
    let errorMessage = error.desc.get($error.code)
    debug "Failed to publish message with relay",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex(), error = errorMessage
    if error.isRlnRejection():
      return StepResult.init(StepOutcome.ProofRejected, errorMessage)
    if error.code == LightPushErrorCode.NO_PEERS_TO_RELAY:
      return StepResult.init(StepOutcome.NoRelayPeers, errorMessage)
    return StepResult.init(StepOutcome.Rejected, errorMessage)

  if noOfPublishedPeers > 0:
    debug "Message propagated via Relay",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      noOfPeers = noOfPublishedPeers
  return propagationOutcome(noOfPublishedPeers)
