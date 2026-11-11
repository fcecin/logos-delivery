import chronicles, chronos, results, brokers/broker_context
import logos_delivery/waku/waku_core, logos_delivery/waku/waku
import logos_delivery/waku/api/publish
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

const MixResendInterval* = chronos.seconds(5)
  ## The wait of a task sent over mix with no answer before its next send over mix.

const MaxMixResends* = 1
  ## The most sends over mix of a task after the first one that got no answer.

const MaxMixResendAge* = chronos.seconds(15)
  ## The age of a message after which mix does not send it again.
  ## Store and RLN validators refuse a message more than 20 s from their clock.

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
      ## The time from the end of a send over mix with no answer to its resend.

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

method deanonymizes*(self: MixSendProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor*(self: MixSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

proc mixWindowElapsed(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True once `task` has spent its whole mix window, which bounds failed mix
  ## attempts at either level.
  return task.admissionAge() > self.mixWindow

proc mixResendDue(self: MixSendProcessor, task: DeliveryTask): bool =
  ## True when a task sent over mix with no answer can send the message again.
  task.resendsAfterNoAnswer < MaxMixResends and task.messageAge() < MaxMixResendAge and
    Moment.now() - task.lastSendFinishedTime.get(Moment.low()) >= self.resendInterval

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

proc decideAfterMixError(
    self: MixSendProcessor, task: DeliveryTask, error: ErrorStatus
) =
  ## Sets the next state of a task whose send over mix got an error answer.
  if error.isRlnRejection():
    if not task.sentWithNoAnswer:
      task.parkForRlnProofRefresh(self.waku, error.desc.get($error.code))
      return
    # An earlier send can have delivered the message, so the task stays on mix.
    # It keeps its proof, because a new proof makes a new relay message.
    task.state = DeliveryState.NextRoundRetry
    return

  if task.sentWithNoAnswer:
    # A send of the task got no answer, and the message can be on the network.
    # A refusal of a later send does not change that.
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

proc applyMixAnswer(
    self: MixSendProcessor, task: DeliveryTask, answer: WakuLightPushResult
) =
  ## Sets the next state of a task from the answer of its send over mix.
  ## Also marks the end of the send on the task, if the attempt counts as a send.
  let unanswered = answer.isErr() and answer.error.isUnanswered()
  # The exit gives `INTERNAL_SERVER_ERROR` when it fails before it publishes.
  # `lightpushPublishToAny` gives it for a fault of this node before the request leaves.
  # Neither one puts the message on the network, so neither one counts as a send.
  if answer.isOk() or unanswered or
      answer.error.code != LightPushErrorCode.INTERNAL_SERVER_ERROR:
    task.markSendFinished(unanswered)

  let numLightpushServers = answer.valueOr:
    if error.isAlreadySeenRejection():
      # The refusal of the exit means what its success answer means.
      debug "The mix exit already has the message",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.setPropagated()
      return
    debug "MixSendProcessor.sendImpl failed", error = error.desc.get($error.code)
    self.decideAfterMixError(task, error)
    return

  if numLightpushServers > 0:
    debug "Message propagated via Mix",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.setPropagated()
  else:
    debug "Mix publish returned zero peers, the task waits for a retry",
      requestId = task.requestId
    task.state = DeliveryState.NextRoundRetry

proc publishUntilAnswered(
    self: MixSendProcessor, task: DeliveryTask
): Future[Opt[WakuLightPushResult]] {.async.} =
  ## Publishes the message over mix until the exit answers or the network has it.
  ## Returns the answer of the exit, or none when the network had the message first.
  if task.networkHasMessage():
    # A copy came before the attempt, so the attempt starts no publish.
    return Opt.none(WakuLightPushResult)
  let publish =
    self.waku.lightpushPublishToAny(task.pubsubTopic, task.msg, mixify = true)
  let networkHasMessageWait = task.waitForNetworkHasMessage()
  try:
    # The exit publishes before it answers, so a copy from the network also answers.
    discard await race(FutureBase(publish), FutureBase(networkHasMessageWait))
  except CancelledError as exc:
    # A stop cut the wait.
    if publish.completed():
      # An answer that came before the stop is the answer of this send.
      self.applyMixAnswer(task, publish.read())
    else:
      await publish.cancelAndWait()
      # The network can have the message, so the send counts as one with no answer.
      task.markSendFinished(unanswered = true)
    raise exc
  finally:
    networkHasMessageWait.cancelSoon()
  if not publish.finished():
    await publish.cancelAndWait()
    return Opt.none(WakuLightPushResult)
  return Opt.some(await publish)

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if task.deanonymized:
    # A plain attempt already showed this node's address, so the task stays plain.
    task.errorDesc = ""
    task.state = DeliveryState.FallbackRetry
    return

  if task.sentWithNoAnswer and
      (not self.mixResendDue(task) or self.mixUnusable(task) != MixUnusable.None):
    # The message can be on the network, so the task waits on mix and never goes plain.
    task.state = DeliveryState.NextRoundRetry
    return

  # No wait can make the message fit, so check the size before the reasons that
  # a wait can clear. A task that is too large has no mix attempt, so it must not
  # get the `anonymized` mark.
  let (size, limit) = self.waku.mixLightpushSize(task.pubsubTopic, task.msg)
  if size > limit:
    if task.sentWithNoAnswer:
      # The message can be on the network, so a task that is too large stays on mix.
      task.state = DeliveryState.NextRoundRetry
      return
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

  if self.fallbackAllowed and self.mixWindowElapsed(task) and not task.sentWithNoAnswer:
    debug "Mix window elapsed, handing the task to the plain send path",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      admissionAge = task.admissionAge()
    task.errorDesc = ""
    task.state = DeliveryState.FallbackRetry
    return

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

  let answer = await self.publishUntilAnswered(task)
  if answer.isNone() or task.networkHasMessage():
    debug "The network has the message, the mix send is complete",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.setPropagated()
    return

  self.applyMixAnswer(task, answer.get())
