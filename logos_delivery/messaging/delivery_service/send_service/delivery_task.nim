import results, std/times, chronos
import libp2p/peerid
import brokers/broker_context
import
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/waku/requests/node_requests

type DeliveryPath* {.pure.} = enum
  ## The send path that propagated a message.
  Unknown
  Mix
  Relay
  Lightpush

type DeliveryState* {.pure.} = enum
  Entry
  SuccessfullyPropagated
    # message is known to be sent to the network but not yet validated
  SuccessfullyValidated
    # message is known to be stored at least on one store node, thus validated
  FallbackRetry # retry sending with fallback processor if available
  NextRoundRetry # try sending in next loop
  FailedToDeliver # final state of failed delivery

type DeliveryTask* = ref object
  requestId*: RequestId
  pubsubTopic*: PubsubTopic
  msg*: WakuMessage
  msgHash*: WakuMessageHash
  tryCount*: int
  state*: DeliveryState
  deliveryTime*: Moment
  firstPropagatedTime*: Opt[Moment]
    ## Set once on the first successful propagation; never reset on re-publish.
    ## Anchors the store-validation time cap (see propagationAge).
  firstAdmittedTime*: Opt[Moment]
    ## Set when the task first passes rate-limit admission; `none` while parked
    ## waiting for epoch budget. Guards re-admission on retry. An RLN proof
    ## refresh clears it, so the new proof gets a new nonce.
  deadlineStart*: Opt[Moment]
    ## Set at the first admission and not reset. The delivery deadline and the
    ## `Preferred` mix window run from this time. `firstAdmittedTime` is not
    ## usable for them: an RLN proof refresh clears it, and that would restart
    ## the deadline and the window at each stale proof.
  avoidMixExit*: Opt[PeerId]
    ## The exit node of the last failed mix attempt. The next attempt uses a
    ## different exit node when one exists. Kept on the task: nothing about
    ## an attempt persists across tasks or on the protocol object.
  mixRequestLeft*: bool
    ## A mix attempt wrote its request to the mix connection, or may have. The
    ## message may be on the network, or in the hands of an exit node that
    ## refused it. This never clears. A `Preferred` task with this set does
    ## not go to the plain path: a copy in clear text would connect the
    ## sender to the message. The deadline error names the uncertainty.
  propagatedViaMix*: bool
    ## True when the exit node confirmed the message. The send service does
    ## not validate such a message with a store node: a store query with the
    ## message hash would connect the sender to the message.
  deliveryPath*: DeliveryPath
    ## The path of the last attempt or propagation. The result log lines show
    ## it, so an operator sees a `Preferred` message that went out in clear
    ## text.
  mixExit*: Opt[PeerId] ## The exit node of the mix attempt that propagated the message.
  handedToPlainPath*: bool
    ## The mix processor gave the task to the plain path at least once. The
    ## first hand-off is logged at INFO, the next ones at DEBUG.
  propagateEventEmitted*: bool
  errorDesc*: string

proc new*(
    T: typedesc[DeliveryTask],
    requestId: RequestId,
    envelop: MessageEnvelope,
    brokerCtx: BrokerContext,
): Result[T, string] =
  let msg = envelop.toWakuMessage()
  # TODO: use sync request for such as soon as available
  let relayShardRes = (
    RequestRelayShard.request(brokerCtx, Opt.none(PubsubTopic), envelop.contentTopic)
  ).valueOr:
    debug "RequestRelayShard.request failed", error = error
    return err("Failed create DeliveryTask: " & $error)

  let pubsubTopic = relayShardRes.relayShard.toPubsubTopic()
  let msgHash = computeMessageHash(pubsubTopic, msg)

  return ok(
    T(
      requestId: requestId,
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: msgHash,
      tryCount: 0,
      state: DeliveryState.Entry,
    )
  )

func `==`*(r, l: DeliveryTask): bool =
  if r.isNil() == l.isNil():
    return r.isNil() or r.msgHash == l.msgHash
  else:
    return false

proc messageAge*(self: DeliveryTask): timer.Duration =
  let actual = getNanosecondTime(getTime().toUnixFloat())
  if self.msg.timestamp >= 0 and self.msg.timestamp < actual:
    return nanoseconds(actual - self.msg.timestamp)
  else:
    return ZeroDuration

proc deliveryAge*(self: DeliveryTask): timer.Duration =
  if self.state == DeliveryState.SuccessfullyPropagated:
    return timer.Moment.now() - self.deliveryTime
  else:
    return ZeroDuration

proc propagationAge*(self: DeliveryTask): timer.Duration =
  ## Time elapsed since the message was first successfully propagated.
  ## Stable across re-publishes; ZeroDuration until first propagation.
  if self.firstPropagatedTime.isSome():
    return timer.Moment.now() - self.firstPropagatedTime.get()
  else:
    return ZeroDuration

proc admissionAge*(self: DeliveryTask): timer.Duration =
  ## Time since the task first passed admission; ZeroDuration while never
  ## admitted (still parked waiting for epoch budget).
  if self.firstAdmittedTime.isSome():
    return timer.Moment.now() - self.firstAdmittedTime.get()
  else:
    return ZeroDuration

proc deadlineAge*(self: DeliveryTask): timer.Duration =
  ## Time since the first admission. ZeroDuration before the first admission.
  ## An RLN proof refresh does not reset this value.
  if self.deadlineStart.isSome():
    return timer.Moment.now() - self.deadlineStart.get()
  else:
    return ZeroDuration

proc isEphemeral*(self: DeliveryTask): bool =
  return self.msg.ephemeral

proc needsStoreValidation*(self: DeliveryTask): bool =
  ## True when the send service must confirm the message with a store node.
  ## An ephemeral message is not stored. A message that went through mix is
  ## not confirmed, because the query would show the sender to the store node.
  return not self.isEphemeral() and not self.propagatedViaMix

proc isDeliveryTimedOut*(self: DeliveryTask, maxTime: timer.Duration): bool =
  ## True when a task passed admission more than `maxTime` ago and did not
  ## propagate. A task that did not pass admission (parked for budget) is
  ## exempt. The clock is `deadlineStart`, which nothing resets.
  return
    self.deadlineStart.isSome() and self.firstPropagatedTime.isNone() and
    self.deadlineAge() > maxTime
