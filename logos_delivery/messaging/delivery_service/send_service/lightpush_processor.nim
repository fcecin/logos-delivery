import chronicles, chronos, results, brokers/broker_context
import logos_delivery/waku/waku
import logos_delivery/waku/api/publish

import ./[delivery_task, delivery_plan, send_processor]

logScope:
  topics = "send service lightpush processor"

type LightpushSendProcessor* = ref object of BaseSendProcessor
  waku: Waku

proc new*(
    T: typedesc[LightpushSendProcessor], waku: Waku, brokerCtx: BrokerContext
): T =
  return T(waku: waku, brokerCtx: brokerCtx)

method checkBeforeAttempt*(
    self: LightpushSendProcessor, task: DeliveryTask
): Result[void, StepResult] {.gcsafe.} =
  if not self.waku.lightpushPeerAvailable(task.pubsubTopic):
    return err(StepResult.init(StepOutcome.NoLightpushPeer))
  return ok()

method attempt*(
    self: LightpushSendProcessor, task: DeliveryTask
): Future[StepResult] {.async.} =
  debug "Trying message delivery via Lightpush",
    requestId = task.requestId, msgHash = task.loggedHash(), tryCount = task.tryCount

  let numLightpushServers = (
    await self.waku.lightpushPublishToAny(task.pubsubTopic, task.msg)
  ).valueOr:
    let failure = lightpushOutcome(error)
    debug "Lightpush publish failed",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      outcome = failure.outcome,
      error = failure.desc
    return failure

  if numLightpushServers > 0:
    debug "Message propagated via Lightpush",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      servers = numLightpushServers
  return propagationOutcome(numLightpushServers)
