import chronicles, chronos, results, brokers/broker_context
import logos_delivery/waku/waku_core, logos_delivery/waku/waku
import logos_delivery/waku/api/publish
import logos_delivery/waku/waku_mix
import logos_delivery/api/conf/modes

import ./[delivery_task, send_processor]

logScope:
  topics = "send service mix processor"

const MixUnavailableReason* =
  "cannot build a mix path (mix not mounted, or fewer than " & $MinMixPoolSize &
  " mix nodes can carry a packet)"
  ## Reported to the application when a `Required` send finds mix unusable.
  ## The level leaves no second path, so the send ends here.
  ##
  ## "can carry a packet" and not "are known": the pool counts nodes with an
  ## address mix can route to, which is a smaller set than the peers whose mix
  ## key we have seen. An operator who wants the live number has the
  ## `mix_pool_size` gauge.

const MixNoExitReason* =
  "no mix pool member serves lightpush on the shard, so there is no exit to " &
  "build a mix path to"
  ## The pool can build a path, but its last hop must be a lightpush server for
  ## the message's shard, and no pool member is one. Distinct from
  ## `MixUnavailableReason` so the application can tell a cold pool from a pool
  ## without an exit.

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
  ## decided at once in `sendImpl`. Exported for the tests.
  return self.fallbackAllowed and task.admissionAge() > self.mixWindow

proc mixUnusable(self: MixSendProcessor, task: DeliveryTask): Opt[string] =
  ## The reason mix cannot attempt `task` now, if there is one. The first
  ## check is the readiness bound `mixReady()` shares with the health monitor;
  ## the second is the very exit predicate `lightpushPublishToAny(mixify =
  ## true)` applies, so the exit decision cannot disagree with the attempt it
  ## spares. Neither touches the network.
  if not self.waku.mixReady():
    return Opt.some(MixUnavailableReason)
  if self.waku.selectMixLightpushPeer(task.pubsubTopic).isNone():
    return Opt.some(MixNoExitReason)
  return Opt.none(string)

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if self.mixWindowElapsed(task):
    debug "Mix window elapsed",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      admissionAge = task.admissionAge()
    task.state = DeliveryState.FallbackRetry
    return

  let unusable = self.mixUnusable(task)
  if unusable.isSome():
    # The outcome of this round is already known. Holding the task changes
    # nothing that the next round would not find again; the level says what
    # happens instead.
    if self.fallbackAllowed:
      debug "Mix cannot attempt the task, handing it to the plain send path",
        requestId = task.requestId,
        msgHash = task.msgHash.to0xHex(),
        reason = unusable.get()
      task.state = DeliveryState.FallbackRetry
    else:
      debug "Mix cannot attempt the task, and the level has no other send path",
        requestId = task.requestId,
        msgHash = task.msgHash.to0xHex(),
        reason = unusable.get()
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = unusable.get()
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
    task.deliveryTime = Moment.now()
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
  else:
    debug "Mix publish returned zero peers, request pushed back for next round",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry

  return
