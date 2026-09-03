import std/strutils
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

const MixReceiptWindow* = chronos.seconds(5)
  ## After a mix attempt that got no reply, the time that a `Preferred` task
  ## waits for the sender's own copy of the message before it goes to the
  ## plain path. A lost reply does not mean a lost message: the exit node may
  ## have published it, and a copy in clear text would connect the sender to
  ## it. The node gets its own copy when it is subscribed to the content
  ## topic.

method isValidProcessor*(self: MixSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

proc mixPacketMayHaveLeft*(error: ErrorStatus, exit: Opt[PeerId]): bool =
  ## True when the attempt failed after the packet may have left the node,
  ## so that a copy of the message that the node receives later is a mixed
  ## delivery. False for a failure before the write (no exit node selected,
  ## a message that does not fit a packet, `SERVICE_NOT_AVAILABLE` with a
  ## description other than the lost reply and the unreadable reply), and for
  ## an answer of the exit node that it did not publish (an RLN rejection, no
  ## relay peer, too many requests). Unsure means true: a kept mark costs one
  ## receipt window under `Preferred`, a cleared mark costs a store query for
  ## a mixed message.
  if exit.isNone():
    return false
  if error.code == LightPushErrorCode.PAYLOAD_TOO_LARGE:
    return false
  if error.isRlnRejection():
    return false
  if error.code == LightPushErrorCode.NO_PEERS_TO_RELAY or
      error.code == LightPushErrorCode.TOO_MANY_REQUESTS:
    return false
  if error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE:
    return
      error.desc == Opt.some(MixReplyTimeoutDesc) or
      error.desc.get("").startsWith(MixReplyUnreadableDesc)
  return true

proc receiptWindowOpen(task: DeliveryTask): bool =
  ## True while the sender's own copy of the message can still arrive after
  ## a mix attempt that got no reply.
  return
    task.lastMixSendTime.isSome() and
    Moment.now() - task.lastMixSendTime.get() <= MixReceiptWindow

proc mixWindowElapsed(self: MixSendProcessor, task: DeliveryTask): bool =
  ## The window runs from the first admission, as the delivery deadline does.
  ## A `Preferred` task thus gets its plain window after the mix window in
  ## all cases. A task that waits for rate-limit budget is not admitted, so
  ## the wait does not use the window.
  return self.fallbackAllowed and task.deadlineAge() > self.mixWindow

method sendImpl*(self: MixSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  if self.mixWindowElapsed(task):
    if task.propagatedViaMix:
      # The message went to the network through mix. A copy in clear text
      # would connect the sender to that message, so the plain path does not
      # get the task. The send service removes a mixed message from its cache
      # after the propagation, so no round gets here. The branch is a guard.
      debug "Mix window elapsed, but the message propagated through mix; staying on mix",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    elif task.receiptWindowOpen():
      # A mix attempt got no reply. The exit node may have published the
      # message, and the sender's own copy may arrive. No new attempt while
      # the task waits: an attempt with no reply would open the window again.
      debug "Mix window elapsed, but a mix attempt got no reply; waiting for the sender's own copy before the plain path",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      task.state = DeliveryState.NextRoundRetry
      return
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
      # A copy received from now on is the plain path's delivery.
      task.lastMixSendTime = Opt.none(Moment)
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
    tryCount = task.tryCount

  # The packet leaves the node during this call. A copy of the message that
  # the node receives from now on is a mixed delivery, whatever the reply
  # does. A failure before the write clears the mark below.
  task.lastMixSendTime = Opt.some(Moment.now())
  task.deliveryPath = DeliveryPath.Mix
  let (publishRes, exit) = await self.waku.lightpushPublishViaMix(
    task.pubsubTopic, task.msg, task.avoidMixExit
  )
  let numLightpushServers = publishRes.valueOr:
    debug "Mix delivery attempt failed",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      tryCount = task.tryCount,
      code = $error.code,
      error = error.desc.get(""),
      exit = exit
    # The next attempt avoids this exit node when a different one exists.
    task.avoidMixExit = exit
    if mixPacketMayHaveLeft(error, exit):
      # The packet left the node and no answer said that the exit node did
      # not publish (a lost reply, a reply that could not be read, an
      # internal error). The exit node may have published the message. The
      # mark is set again at the end of the attempt, so that
      # `MixReceiptWindow` runs from here and not from the start.
      task.lastMixSendTime = Opt.some(Moment.now())
    else:
      task.lastMixSendTime = Opt.none(Moment)
    if error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE and
        error.desc == Opt.some(MixReplyTimeoutDesc):
      logos_delivery_mix_send_attempts_total.inc(labelValues = ["timeout"])
    elif exit.isNone():
      logos_delivery_mix_send_attempts_total.inc(labelValues = ["no_exit"])
    elif error.isRlnRejection():
      logos_delivery_mix_send_attempts_total.inc(labelValues = ["rejected"])
    elif error.code == LightPushErrorCode.PAYLOAD_TOO_LARGE:
      logos_delivery_mix_send_attempts_total.inc(labelValues = ["too_large"])
    else:
      logos_delivery_mix_send_attempts_total.inc(labelValues = ["other"])
    # Keep the reason of the last attempt on the task. The send service logs
    # it when the delivery deadline passes, so the log shows why mix did not
    # deliver.
    task.errorDesc = error.desc.get($error.code)

    if error.isRlnRejection():
      task.parkForRlnProofRefresh(self.waku)
      return

    if error.code == LightPushErrorCode.PAYLOAD_TOO_LARGE:
      # A retry or a different path cannot make the message fit a sphinx
      # packet. `Preferred` gives the task to the plain path at once.
      # `Required` fails the task at once.
      if self.fallbackAllowed:
        debug "Message too large for mix, handing the task to the plain send path",
          requestId = task.requestId, msgHash = task.msgHash.to0xHex()
        task.state = DeliveryState.FallbackRetry
      else:
        task.state = DeliveryState.FailedToDeliver
        task.deliveryTime = Moment.now()
      return

    case error.code
    of LightPushErrorCode.NO_PEERS_TO_RELAY, LightPushErrorCode.TOO_MANY_REQUESTS,
        LightPushErrorCode.OUT_OF_RLN_PROOF, LightPushErrorCode.SERVICE_NOT_AVAILABLE,
        LightPushErrorCode.INTERNAL_SERVER_ERROR:
      task.state = DeliveryState.NextRoundRetry
    else:
      task.state = DeliveryState.FailedToDeliver
      task.deliveryTime = Moment.now()
    return

  if numLightpushServers > 0:
    debug "Message propagated via Mix",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex(), exit = exit
    logos_delivery_mix_send_attempts_total.inc(labelValues = ["propagated"])
    task.avoidMixExit = Opt.none(PeerId)
    task.deliveryPath = DeliveryPath.Mix
    task.mixExit = exit
    task.state = DeliveryState.SuccessfullyPropagated
    task.propagatedViaMix = true
    task.deliveryTime = Moment.now()
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
  else:
    debug "Mix publish returned zero peers, request pushed back for next round",
      requestId = task.requestId, exit = exit
    # An exit node without relay peers cannot deliver. The next attempt
    # avoids it, as after an error. The lightpush server maps zero relay
    # peers to an error code, so the error branch covers that case and this
    # branch is a guard, kept from the plain lightpush processor.
    task.avoidMixExit = exit
    task.lastMixSendTime = Opt.none(Moment)
    logos_delivery_mix_send_attempts_total.inc(labelValues = ["other"])
    task.state = DeliveryState.NextRoundRetry

  return
