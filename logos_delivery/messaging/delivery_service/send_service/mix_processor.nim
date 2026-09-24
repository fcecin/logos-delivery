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
  ## The reason when mix is not mounted or the pool is short. The pool counts
  ## only members with an address that mix routes; `mix_pool_size` shows the count.

const MixSelfHopReason* =
  "this node's announced address is not one mix can route replies to (IPv4 " &
  "TCP or QUIC-v1), so no reply path can be built"
  ## The reason when the encoder rejects this node's own hop, which closes every
  ## reply path: an IPv6-only or name-only announcement, or a NAT mapping that
  ## has not arrived. Mix health reports the same fault.

const MixNoExitReason* =
  "no mix pool member serves lightpush on the shard, so there is no exit to " &
  "build a mix path to"
  ## The reason when no pool member serves lightpush on the message's shard, so
  ## a path has no exit. The pool itself can build a path.

type MixSendProcessor* = ref object of BaseSendProcessor
  waku: Waku
  fallbackAllowed: bool
  mixWindow: timer.Duration
  fellBackReason: string
    ## The reason for the last hand-over to the plain path while mix was
    ## unusable; empty once mix can attempt a task. INFO logs each change, so an
    ## operator sees a `Preferred` node that sends in clear.

proc fellBackFor*(self: MixSendProcessor): string =
  ## The reason for the last hand-over to the plain path while mix was unusable,
  ## or empty once mix can attempt a task. The INFO lines print the same reason.
  self.fellBackReason

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

proc mixWindowElapsed(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True once `task` has spent its whole mix window. At either level, the window
  ## bounds failed mix attempts and the wait for a pool, a self hop or an exit.
  return task.admissionAge() > self.mixWindow

proc mixUnusable(self: MixSendProcessor, task: DeliveryTask): Opt[string] =
  ## The reason mix cannot attempt `task` now, if any, with no network call.
  ## It splits `mixReady()` into the self hop and the pool, then checks the exit
  ## with `selectMixLightpushPeer`, as `lightpushPublishToAny(mixify = true)` does.
  if not self.waku.mixReady():
    if self.waku.mixMounted() and not self.waku.mixSelfHopUsable():
      return Opt.some(MixSelfHopReason)
    return Opt.some(MixUnavailableReason)
  if self.waku.selectMixLightpushPeer(task.pubsubTopic).isNone():
    return Opt.some(MixNoExitReason)
  return Opt.none(string)

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  # Check the reasons before the window, so that the hand-over of a task that
  # mix cannot attempt logs its reason at INFO.
  let unusable = self.mixUnusable(task)
  if unusable.isSome():
    if self.waku.mixMounted() and not self.mixWindowElapsed(task):
      # With mix mounted, each reason can clear while the node starts. The wait
      # keeps a `Required` send alive and a `Preferred` send off the plain path
      # until the window ends. The reaper, if it fires first, reports this reason.
      task.errorDesc = unusable.get()
      task.state = DeliveryState.NextRoundRetry
      return
    # Mix is not mounted, or the window is spent: the level decides now.
    if self.fallbackAllowed:
      if self.fellBackReason != unusable.get():
        self.fellBackReason = unusable.get()
        info "Mix cannot carry messages, sending them over the plain path instead",
          reason = unusable.get()
      debug "Mix cannot attempt the task, handing it to the plain send path",
        requestId = task.requestId,
        msgHash = task.msgHash.to0xHex(),
        reason = unusable.get()
      task.errorDesc = "" # the plain path reports its own outcome
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

  if self.fellBackReason.len > 0:
    self.fellBackReason = ""
    info "Mix can carry messages again"

  if self.fallbackAllowed and self.mixWindowElapsed(task):
    debug "Mix window elapsed",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      admissionAge = task.admissionAge()
    task.errorDesc = ""
    task.state = DeliveryState.FallbackRetry
    return

  task.errorDesc = "" # the attempt reports its own outcome
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
