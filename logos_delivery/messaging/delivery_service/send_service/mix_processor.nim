import chronicles, chronos, results, brokers/broker_context
import logos_delivery/waku/waku_core, logos_delivery/waku/waku
import logos_delivery/waku/api/publish
from logos_delivery/waku/node/waku_node/lightpush import MixTimedOutDesc
import logos_delivery/waku/waku_mix
import logos_delivery/api/conf/modes

import ./[delivery_task, send_processor]

logScope:
  topics = "send service mix processor"

const MixUnavailableReason* =
  "mix is not mounted or knows fewer than " & $MinMixPoolSize & " usable mix nodes"
  ## The pool counts only members with an address that mix routes.

const MixSelfHopReason* = "this node has no address that mix can use for replies"
  ## An IPv6-only or name-only announcement, or a NAT mapping not yet arrived.

const MixNoExitReason* = "no mix node serves lightpush on the shard of the message"
  ## No pool member serves lightpush on the message's shard.

const MixTooLargeReason* = "message too large for mix"
  ## One mix message carries much less than the plain path.

const MixResendInterval* = chronos.seconds(10)
  ## How long a task with an unanswered mix request waits for a confirmation
  ## before it sends the message again over mix.

const MaxMixResends* = 3
  ## The most mix requests of a task after the first one that got no answer.

const MixUnusableGrace* = chronos.seconds(3)
  ## How long after its admission a `Required` task waits for a mounted mix that
  ## cannot attempt it. Retries come `RetryInterval` apart.

type
  MixUnusable* {.pure.} = enum
    ## Why mix cannot attempt a task now.
    None
    Unavailable
    SelfHop
    NoExit

  MixSendProcessor* = ref object of BaseSendProcessor
    waku: Waku
    fallbackAllowed: bool
    mixWindow: timer.Duration
    fellBackReason: MixUnusable
      ## Why the last task went to the plain path; `None` once mix can attempt
      ## a task again. INFO logs each change.
    resendInterval: timer.Duration

proc reason*(unusable: MixUnusable): string =
  ## The text written to `errorDesc` and the log lines.
  case unusable
  of MixUnusable.None: ""
  of MixUnusable.Unavailable: MixUnavailableReason
  of MixUnusable.SelfHop: MixSelfHopReason
  of MixUnusable.NoExit: MixNoExitReason

proc fellBackFor*(self: MixSendProcessor): MixUnusable =
  ## Why the last task went to the plain path, or `None` once mix can attempt a
  ## task again.
  self.fellBackReason

proc new*(
    T: typedesc[MixSendProcessor],
    waku: Waku,
    brokerCtx: BrokerContext,
    anonymityLevel: AnonymityLevel,
    mixWindow: timer.Duration,
    resendInterval = MixResendInterval,
): T =
  return T(
    waku: waku,
    brokerCtx: brokerCtx,
    fallbackAllowed: anonymityLevel == AnonymityLevel.Preferred,
    mixWindow: mixWindow,
    resendInterval: resendInterval,
  )

method isPlain*(self: MixSendProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor*(self: MixSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

proc mixWindowElapsed(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True once `task` has spent its whole mix window, which bounds failed mix
  ## attempts at either level.
  return task.admissionAge() > self.mixWindow

proc mixResendDue(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True when a task with an unanswered mix request can send the message again.
  task.mixResends < MaxMixResends and
    Moment.now() - task.lastMixRequestTime.get(Moment.low()) >= self.resendInterval

proc noteMixRequestLeft(task: DeliveryTask, answered: bool) =
  ## Records a mix request of the task that left this node.
  if task.mixUnanswered:
    task.mixResends.inc()
  task.lastMixRequestTime = Opt.some(Moment.now())
  if not answered:
    task.mixUnanswered = true

proc mixUnusable(self: MixSendProcessor, task: DeliveryTask): MixUnusable =
  ## Why mix cannot attempt `task` now, with no network call. It splits
  ## `mixReady()` into the self hop and the pool, then checks the exit with
  ## `selectMixLightpushPeer`, as `lightpushPublishToAny(mixify = true)` does.
  if not self.waku.mixReady():
    if self.waku.mixMounted() and not self.waku.mixSelfHopUsable():
      return MixUnusable.SelfHop
    return MixUnusable.Unavailable
  if self.waku.selectMixLightpushPeer(task.pubsubTopic).isNone():
    return MixUnusable.NoExit
  return MixUnusable.None

proc decideWithoutMix(
    self: MixSendProcessor, task: DeliveryTask, unusable: MixUnusable
) =
  ## Hands a `Preferred` task to the plain path at once. Fails a `Required` task
  ## with the reason, once its admission is older than `MixUnusableGrace` while
  ## mix is mounted.
  if not self.fallbackAllowed:
    if self.waku.mixMounted() and task.admissionAge() <= MixUnusableGrace:
      task.errorDesc = unusable.reason()
      task.state = DeliveryState.NextRoundRetry
      return
    debug "Mix cannot attempt the task, and the Required anonymity level allows no send without mix",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      reason = unusable.reason()
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = unusable.reason()
    return

  if self.fellBackReason != unusable:
    self.fellBackReason = unusable
    info "Mix cannot carry messages, sending them over the plain path instead",
      reason = unusable.reason()
  debug "Mix cannot attempt the task, handing it to the plain send path",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    reason = unusable.reason()
  task.errorDesc = "" # the plain path reports its own outcome
  task.state = DeliveryState.FallbackRetry

proc decideTooLarge(self: MixSendProcessor, task: DeliveryTask, size, limit: int) =
  ## Fails a `Required` task at once, because no later pass can make the message
  ## fit. Hands a `Preferred` task to the plain path at once.
  if not self.fallbackAllowed:
    debug "Message too large for mix, failing the task",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex(), size, limit
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc =
      MixTooLargeReason & ": the request has " & $size & " bytes, the limit is " & $limit &
      " bytes"
    return
  debug "Message too large for mix, handing it to the plain send path",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex(), size, limit
  task.errorDesc = "" # the plain path reports its own outcome
  task.state = DeliveryState.FallbackRetry

method checkBeforeAdmission*(
    self: MixSendProcessor, task: DeliveryTask
): bool {.gcsafe, raises: [].} =
  if self.fallbackAllowed:
    return true
  let message = self.waku.messageForSizeCheck(task.msg).valueOr:
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = "Failed to determine RLN proof size: " & $error
    return false
  let (size, limit) = self.waku.mixLightpushSize(task.pubsubTopic, message)
  if size > limit:
    self.decideTooLarge(task, size, limit)
    return false
  return true

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if task.mixUnanswered and
      (not self.mixResendDue(task) or self.mixUnusable(task) != MixUnusable.None):
    # The message can be on the network, so the task waits on mix for its
    # confirmation or for its next resend, and never goes to the plain path.
    task.state = DeliveryState.NextRoundRetry
    return

  # No wait can make the message fit, so check the size before the reasons that
  # a wait can clear. A task that is too large has no mix attempt, so it must not
  # get the `anonymized` mark.
  let (size, limit) = self.waku.mixLightpushSize(task.pubsubTopic, task.msg)
  if size > limit:
    self.decideTooLarge(task, size, limit)
    return

  # Check the reasons before the window, so that the hand-over of a task that
  # mix cannot attempt logs its reason at INFO.
  let unusable = self.mixUnusable(task)
  if unusable != MixUnusable.None:
    self.decideWithoutMix(task, unusable)
    return

  if self.fellBackReason != MixUnusable.None:
    self.fellBackReason = MixUnusable.None
    info "Mix can carry messages again"

  if self.fallbackAllowed and self.mixWindowElapsed(task) and not task.mixUnanswered:
    debug "Mix window elapsed, handing the task to the plain send path",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      admissionAge = task.admissionAge()
    task.errorDesc = ""
    task.state = DeliveryState.FallbackRetry
    return

  # A refusal of a message that the network has confirms an earlier mix attempt.
  let mixTriedBefore = task.isAnonymous()

  # Mark before the attempt: the exit publishes before it replies, so a message
  # whose reply is lost is still on the network. From here on, the send service
  # logs no hash for this task at INFO or ERROR.
  task.anonymized = true

  task.errorDesc = "" # the attempt reports its own outcome
  task.tryCount.inc()
  debug "Trying message delivery via Mix",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  let requestLeft = newFuture[void]("mix request left")
  var answered = false
  var publishResult: WakuLightPushResult
  try:
    publishResult = await self.waku.lightpushPublishToAny(
      task.pubsubTopic, task.msg, mixify = true, requestLeft = requestLeft
    )
    answered =
      publishResult.isOk() or publishResult.error.desc.get("") != MixTimedOutDesc
  finally:
    # Also when a stop cancels the attempt, a request that left can be on the
    # network.
    if requestLeft.completed():
      task.noteMixRequestLeft(answered)

  let numLightpushServers = publishResult.valueOr:
    if mixTriedBefore and error.code == LightPushErrorCode.INVALID_MESSAGE and
        error.desc.get("") == AlreadySeenMessageDesc:
      # The relay of the exit has the message. An earlier mix attempt delivered
      # it, and its answer was lost.
      debug "Message already on the network, the mix send is complete",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.SuccessfullyPropagated
      if task.firstPropagatedTime.isNone():
        task.firstPropagatedTime = Opt.some(Moment.now())
      return

    debug "MixSendProcessor.sendImpl failed", error = error.desc.get($error.code)

    if error.isRlnRejection():
      task.parkForRlnProofRefresh(self.waku, error.desc.get($error.code))
      if task.mixUnanswered and task.state == DeliveryState.FailedToDeliver:
        # An earlier request can have delivered the message.
        task.state = DeliveryState.NextRoundRetry
      return

    if task.mixUnanswered:
      # A request of the task got no answer, and the message can be on the
      # network. A refusal of a later request does not change that.
      task.state = DeliveryState.NextRoundRetry
      return

    case error.code
    of LightPushErrorCode.NO_PEERS_TO_RELAY, LightPushErrorCode.TOO_MANY_REQUESTS,
        LightPushErrorCode.OUT_OF_RLN_PROOF, LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        LightPushErrorCode.INTERNAL_SERVER_ERROR:
      task.state = DeliveryState.NextRoundRetry
    else:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = error.desc.get($error.code)
    return

  if numLightpushServers > 0:
    debug "Message propagated via Mix",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.SuccessfullyPropagated
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
  else:
    debug "Mix publish returned zero peers, the task waits for a retry",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry

  return
