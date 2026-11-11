import results, std/times, chronos
import brokers/broker_context
import
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/waku/requests/node_requests

type DeliveryState* {.pure.} = enum
  Entry
  SuccessfullyPropagated
    # message is known to be sent to the network but not yet validated
  SuccessfullyValidated
    # message is known to be stored at least on one store node, thus validated
  FallbackRetry # retry sending with fallback processor if available
  NextRoundRetry # waits for its next attempt
  FailedToDeliver # final state of failed delivery
  Unconfirmed # final state of a send with no confirmation

type DeliveryTask* = ref object
  requestId*: RequestId
  pubsubTopic*: PubsubTopic
  msg*: WakuMessage
  msgHash*: WakuMessageHash
  tryCount*: int
  state*: DeliveryState
  firstPropagatedTime*: Opt[Moment]
    ## Set once on the first successful propagation; never reset on re-publish.
    ## Anchors the archive wait before the first Store query (see
    ## `nextStoreValidationBatch`).
  firstAdmittedTime*: Opt[Moment]
    ## Set when the task first passes rate-limit admission; `none` while parked
    ## waiting for epoch budget. Guards re-admission on retry and anchors the
    ## delivery window (`isDeliveryTimedOut`), so a task parked for budget is not
    ## aged out before it can be sent.
  propagatedEventEmitted*: bool
    ## Set when the task emits `MessagePropagatedEvent`. After a restart, the send
    ## service does not send the task again.
  queuedEventEmitted*: bool
    ## Set once the task has reported itself queued for rate-limit budget, so
    ## the event fires on the first park only and not on every retry.
  anonymized*: bool
    ## Set at the first anonymous send attempt and never cleared; from then on,
    ## the INFO and ERROR records of this task withhold the hash.
    ## `isAnonymous` and `hadSendAttempt` read it too.
  deanonymized*: bool
    ## Set before the first plain send attempt of the task and never cleared.
    ## A plain send attempt deanonymizes the message, whatever the attempt does.
    ## See `isAnonymous`.
  networkHasMessageFuture: Future[void]
    ## Completes once a copy of the message comes to this node from the network.
    ## Relay or filter delivers such a copy.
    ## A send is not a copy, and neither is the answer or the refusal of a relay.
    ## It is never reset, and it is nil until it completes or a send waits for it.
  sentWithNoAnswer*: bool
    ## Set when a send got no answer, so the network can have the message.
    ## A stop that cuts a send also sets it.
    ## With no confirmation, the task ends with `MessageUnconfirmedEvent`, not an error.
    ## Only `MixSendProcessor` sets it.
  resendsAfterNoAnswer*: int
    ## How many sends of the task came after the first one with no answer.
  lastSendFinishedTime*: Opt[Moment]
    ## The end time of the last send of the task over mix.
  lastStoreQueryTime*: Opt[Moment]
    ## When a Store peer was last asked about this task, none before the first query.
  storeValidationWaited*: timer.Duration
    ## The time that `deliver` waited for a Store confirmation of this task. A
    ## stop keeps it, so the validation window continues after a restart.
  deliverHandle*: Future[void]
    ## The `deliver` of the send service for this task. Nil or finished while
    ## it does not run.
  storeAnswer*: Future[void]
    ## Completes when a Store node reports the message. Nil while the send service
    ## does not wait for a Store confirmation of this task.
  resendCount*: int
    ## How many times the send service sent the message again after a Store miss.
  storeMissed*: bool
    ## A Store node answered without the message. Each later Store query for
    ## this task can ask each Store node.
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

  return
    ok(T(requestId: requestId, pubsubTopic: pubsubTopic, msg: msg, msgHash: msgHash))

proc loggedHash*(task: DeliveryTask): string =
  ## The hash for INFO and ERROR records, withheld once the task is anonymized.
  if task.anonymized:
    "withheld"
  else:
    task.msgHash.to0xHex()

proc messageAge*(self: DeliveryTask): timer.Duration =
  let actual = getNanosecondTime(getTime().toUnixFloat())
  if self.msg.timestamp >= 0 and self.msg.timestamp < actual:
    return nanoseconds(actual - self.msg.timestamp)
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

proc isDeliveryTimedOut*(self: DeliveryTask, maxTime: timer.Duration): bool =
  ## True when an admitted task has been trying to deliver longer than `maxTime`
  ## without ever propagating. A task never admitted (parked for budget) is
  ## exempt: the clock runs from admission time, so waiting for budget does not count
  ## against it.
  return
    self.firstAdmittedTime.isSome() and self.firstPropagatedTime.isNone() and
    self.admissionAge() > maxTime

proc isParkedExpired*(self: DeliveryTask, maxAge: timer.Duration): bool =
  ## True when a task never admitted (parked for budget) has outlived `maxAge`,
  ## measured from the message timestamp, so parking cannot grow the backlog
  ## and deliver arbitrarily late.
  return self.firstAdmittedTime.isNone() and self.messageAge() > maxAge

proc getOrCreateNetworkHasMessageFuture(self: DeliveryTask): Future[void] =
  if self.networkHasMessageFuture.isNil():
    self.networkHasMessageFuture = newFuture[void]("the network has the message")
  return self.networkHasMessageFuture

proc setNetworkHasMessage*(self: DeliveryTask) =
  ## Sets `networkHasMessage`, whatever path sent the message.
  ## A second call changes nothing.
  let future = self.getOrCreateNetworkHasMessageFuture()
  if not future.finished():
    future.complete()

func networkHasMessage*(self: DeliveryTask): bool =
  ## True once a copy of the message came to this node from the network.
  return
    not self.networkHasMessageFuture.isNil() and self.networkHasMessageFuture.completed()

proc waitForNetworkHasMessage*(self: DeliveryTask): Future[void] =
  ## Completes once a copy of the message comes to this node from the network.
  ## A cancel of the returned future leaves `networkHasMessage` as it is.
  return self.getOrCreateNetworkHasMessageFuture().join()

func hadSendAttempt*(self: DeliveryTask): bool =
  ## True once the node attempted to send the message, over mix or in the clear.
  self.anonymized or self.deanonymized

proc setPropagated*(self: DeliveryTask) =
  ## Moves the task to `SuccessfullyPropagated`, and keeps its first propagation time.
  self.state = DeliveryState.SuccessfullyPropagated
  if self.firstPropagatedTime.isNone():
    self.firstPropagatedTime = Opt.some(Moment.now())

proc markSendFinished*(self: DeliveryTask, unanswered: bool) =
  ## Marks the end of a send of the task, and keeps whether no answer came.
  if self.sentWithNoAnswer:
    self.resendsAfterNoAnswer.inc()
  self.lastSendFinishedTime = Opt.some(Moment.now())
  if unanswered:
    self.sentWithNoAnswer = true

func isAnonymous*(self: DeliveryTask): bool =
  ## True when the node attempted to send the message over mix, and never in the clear.
  ## A propagated anonymous task went out over mix, so no store node confirms it.
  ## The query would name the message from the address of this node.
  self.anonymized and not self.deanonymized

proc isEphemeral*(self: DeliveryTask): bool =
  return self.msg.ephemeral
