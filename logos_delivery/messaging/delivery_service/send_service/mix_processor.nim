import chronicles, chronos, results, metrics, brokers/broker_context, libp2p/peerid
import logos_delivery/waku/waku_core, logos_delivery/waku/waku
import logos_delivery/waku/api/publish
import logos_delivery/waku/waku_lightpush/common
import logos_delivery/api/conf/modes

import ./[delivery_task, send_processor]

logScope:
  topics = "send service mix processor"

type MixSendProcessor* = ref object of BaseSendProcessor
  waku: Waku
  fallbackAllowed: bool
  mixWindow*: timer.Duration
    ## Exported so that a test can set a short window on a running service.
  lastReady: Opt[bool]
    ## The readiness of mix at the last attempt. A change is logged once at
    ## INFO, so an operator sees when mix becomes usable and when it stops.

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

declarePublicCounter logos_delivery_mix_send_attempts_total,
  "mix send attempts by result", labels = ["result"]

method isValidProcessor*(self: MixSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

proc mixWindowElapsed(self: MixSendProcessor, task: DeliveryTask): bool =
  ## The window runs from the first admission, as the delivery deadline does.
  ## A `Preferred` task thus gets its plain window after the mix window in
  ## all cases. A task that waits for rate-limit budget is not admitted, so
  ## the wait does not use the window.
  return self.fallbackAllowed and task.deadlineAge() > self.mixWindow

func answerText(outcome: MixSendOutcome): string =
  if outcome.answer.isErr():
    outcome.answer.error.desc.get($outcome.answer.error.code)
  else:
    ""

func describe*(outcome: MixSendOutcome): string =
  ## The reason of an attempt in words, for the task's error text: the send
  ## service logs it at the deadline and puts it in the error event.
  case outcome.reason
  of MixSendReason.Accepted:
    "accepted by the exit node"
  of MixSendReason.NoExitNode:
    "no exit node: no lightpush server with a mix key is known for the shard"
  of MixSendReason.TooLarge:
    "message too large for a mix packet"
  of MixSendReason.PreWriteFailure:
    "the request did not leave this node: " & outcome.answerText()
  of MixSendReason.ReplyTimeout:
    "the exit node's reply did not arrive within the time limit"
  of MixSendReason.ReplyUnreadable:
    "the exit node's reply could not be read"
  of MixSendReason.ExitRefused:
    "the exit node refused the request: " & outcome.answerText()

func counterLabel(outcome: MixSendOutcome): string =
  case outcome.reason
  of MixSendReason.Accepted:
    "propagated"
  of MixSendReason.NoExitNode:
    "no_exit"
  of MixSendReason.TooLarge:
    "too_large"
  of MixSendReason.PreWriteFailure:
    "pre_write"
  of MixSendReason.ReplyTimeout:
    "timeout"
  of MixSendReason.ReplyUnreadable:
    "unreadable"
  of MixSendReason.ExitRefused:
    if outcome.answer.isErr() and outcome.answer.error.isRlnRejection():
      "rejected"
    else:
      "refused"

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if self.mixWindowElapsed(task):
    if task.mixRequestLeft or task.propagatedViaMix:
      # A request of this task left the node, or may have. The exit node may
      # have published the message, or has seen it. A copy in clear text
      # would connect the sender to it. The task stays on mix until the exit
      # node confirms it or the deadline fails it. This is the rule the
      # levels promise; the branch is not a heuristic.
      debug "Mix window elapsed, but a request may have left through mix; staying on mix",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    else:
      # An expected state change of a `Preferred` node: the message goes out
      # in clear text. Once at INFO per task, so the operator sees it at the
      # default log level.
      if not task.handedToPlainPath:
        info "Mix window elapsed, handing the task to the plain send path",
          requestId = task.requestId,
          msgHash = task.msgHash.to0xHex(),
          age = task.deadlineAge(),
          lastAttemptError = task.errorDesc
        task.handedToPlainPath = true
        logos_delivery_mix_send_attempts_total.inc(labelValues = ["fallback"])
      else:
        debug "Mix window elapsed, handing the task to the plain send path again",
          requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.FallbackRetry
      return

  let ready = self.waku.mixReady()
  if self.lastReady != Opt.some(ready):
    if ready:
      info "Mix is ready to send", mixPoolSize = self.waku.mixPoolSize()
    else:
      info "Mix is not ready to send",
        mixMounted = self.waku.mixMounted(),
        mixPoolSize = self.waku.mixPoolSize(),
        mixPoolRequired = MixPoolSizeRequired
    self.lastReady = Opt.some(ready)
  if not ready:
    logos_delivery_mix_send_attempts_total.inc(labelValues = ["pool_short"])
    let mixMounted = self.waku.mixMounted()
    let mixPoolSize = self.waku.mixPoolSize()
    debug "Mix cannot publish yet, retrying next round",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      mixMounted = mixMounted,
      mixPoolSize = mixPoolSize,
      mixPoolRequired = MixPoolSizeRequired
    # The reason goes on the task, so the deadline log and the error event
    # show it when this condition lasts until the deadline.
    task.errorDesc =
      if not mixMounted:
        "mix is not mounted"
      else:
        "mix pool has " & $mixPoolSize & " of " & $MixPoolSizeRequired & " nodes"
    task.state = DeliveryState.NextRoundRetry
    return

  task.tryCount.inc()
  debug "Trying message delivery via Mix",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount,
    avoidExit = task.avoidMixExit
  task.deliveryPath = DeliveryPath.Mix

  var outcome: MixSendOutcome
  try:
    outcome = await self.waku.lightpushPublishViaMix(
      task.pubsubTopic, task.msg, task.avoidMixExit
    )
  except CancelledError as exc:
    # The service stops during the attempt. Whether the request left this
    # node is not known, so the task keeps the mark: it is never sent in
    # clear text after this.
    task.mixRequestLeft = true
    raise exc

  if outcome.requestLeft():
    task.mixRequestLeft = true
  logos_delivery_mix_send_attempts_total.inc(labelValues = [outcome.counterLabel()])

  if outcome.reason == MixSendReason.Accepted:
    debug "Message propagated via Mix",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex(), exit = outcome.exit
    task.avoidMixExit = Opt.none(PeerId)
    task.mixExit = outcome.exit
    task.state = DeliveryState.SuccessfullyPropagated
    task.propagatedViaMix = true
    task.deliveryTime = Moment.now()
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
    return

  debug "Mix delivery attempt failed",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount,
    certainty = outcome.certainty,
    reason = outcome.reason,
    exit = outcome.exit,
    answer = outcome.answerText()
  # The next attempt avoids this exit node when a different one exists. The
  # reason stays on the task: the send service logs it when the delivery
  # deadline passes, and puts it in the error event.
  task.avoidMixExit = outcome.exit
  task.errorDesc = outcome.describe()

  case outcome.reason
  of MixSendReason.Accepted:
    discard # handled above
  of MixSendReason.NoExitNode, MixSendReason.PreWriteFailure,
      MixSendReason.ReplyTimeout, MixSendReason.ReplyUnreadable:
    task.state = DeliveryState.NextRoundRetry
  of MixSendReason.TooLarge:
    # A retry or a different path cannot make the message fit a sphinx
    # packet. `Preferred` gives the task to the plain path at once: nothing
    # left the node, so the rule allows it. `Required` fails the task at once.
    if self.fallbackAllowed and not task.mixRequestLeft:
      debug "Message too large for mix, handing the task to the plain send path",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.FallbackRetry
    else:
      task.state = DeliveryState.FailedToDeliver
      task.deliveryTime = Moment.now()
  of MixSendReason.ExitRefused:
    if outcome.answer.isOk():
      # Not produced: a refusal carries the exit node's error status.
      task.state = DeliveryState.NextRoundRetry
      return
    let answer = outcome.answer.error
    if answer.isRlnRejection():
      task.parkForRlnProofRefresh(self.waku)
      return
    case answer.code
    of LightPushErrorCode.NO_PEERS_TO_RELAY, LightPushErrorCode.TOO_MANY_REQUESTS,
        LightPushErrorCode.OUT_OF_RLN_PROOF, LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        LightPushErrorCode.INTERNAL_SERVER_ERROR:
      task.state = DeliveryState.NextRoundRetry
    else:
      task.state = DeliveryState.FailedToDeliver
      task.deliveryTime = Moment.now()
