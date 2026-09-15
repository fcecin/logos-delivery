import chronicles, chronos, results, brokers/broker_context
import logos_delivery/waku/waku_core, logos_delivery/waku/waku
import logos_delivery/waku/api/publish
import logos_delivery/waku/waku_mix
import logos_delivery/api/conf/modes

import ./[delivery_task, send_processor]

logScope:
  topics = "send service mix processor"

const MixUnavailableReason* =
  "mix is required but cannot build a path (mix not mounted, or fewer than " &
  $MinMixPoolSize & " mix nodes can carry a packet)"
  ## Reported to the application when a `Required` send finds mix unusable.
  ## The level leaves no second path, so the send ends here.
  ##
  ## "can carry a packet" and not "are known": the pool counts nodes with an
  ## address mix can route to, which is a smaller set than the peers whose mix
  ## key we have seen. An operator who wants the live number has the
  ## `mix_pool_size` gauge and the `mixPoolSize` query.

type MixSendProcessor* = ref object of BaseSendProcessor
  waku: Waku
  fallbackAllowed: bool
  mixWindow: timer.Duration

proc new*(
    T: typedesc[MixSendProcessor],
    waku: Waku,
    brokerCtx: BrokerContext,
    anonymityLevel: AnonymityLevel,
    mixWindow: timer.Duration,
): T =
  return T(
    waku: waku,
    brokerCtx: brokerCtx,
    fallbackAllowed: anonymityLevel == AnonymityLevel.Preferred,
    mixWindow: mixWindow,
  )

method isValidProcessor*(self: MixSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

proc mixWindowElapsed*(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True once a `Preferred` task has spent its whole window on mix. The window
  ## bounds mix attempts that fail, not the wait for a pool: an unusable mix is
  ## decided at once in `sendImpl`.
  return self.fallbackAllowed and task.admissionAge() > self.mixWindow

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if self.mixWindowElapsed(task):
    debug "Mix window elapsed",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      admissionAge = task.admissionAge()
    task.state = DeliveryState.FallbackRetry
    return

  if not self.waku.mixReady():
    # The pool cannot build a path, so the outcome of this round is already
    # known. Holding the task changes nothing that the next round would not
    # find again; the level says what happens instead.
    if self.fallbackAllowed:
      debug "Mix not ready, handing the task to the plain send path",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.FallbackRetry
    else:
      debug "Mix not ready, and the level has no other send path",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = MixUnavailableReason
      task.deliveryTime = Moment.now()
    return

  task.tryCount.inc()
  debug "Trying message delivery via Mix",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  let numLightpushServers = (
    await self.waku.lightpushPublishToAny(task.pubsubTopic, task.msg, mixify = true)
  ).valueOr:
    debug "MixSendProcessor.sendImpl failed", error = error.desc.get($error.code)

    if error.isRlnRejection():
      task.parkForRlnProofRefresh(self.waku)
      return

    case error.code
    of LightPushErrorCode.NO_PEERS_TO_RELAY, LightPushErrorCode.TOO_MANY_REQUESTS,
        LightPushErrorCode.OUT_OF_RLN_PROOF, LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        LightPushErrorCode.INTERNAL_SERVER_ERROR:
      task.state = DeliveryState.NextRoundRetry
    else:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = error.desc.get($error.code)
      task.deliveryTime = Moment.now()
    return

  if numLightpushServers > 0:
    debug "Message propagated via Mix",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.SuccessfullyPropagated
    task.propagatedOverMix = true
    task.deliveryTime = Moment.now()
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
  else:
    debug "Mix publish returned zero peers, request pushed back for next round",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry

  return
