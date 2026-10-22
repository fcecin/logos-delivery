import results, chronos
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

type DeliveryTask* = ref object
  requestId*: RequestId
  pubsubTopic*: PubsubTopic
  msg*: WakuMessage
  priority*: MessagePriority
    ## Accepted from the envelope and kept; not yet used for scheduling.
  msgHash*: WakuMessageHash
    ## Set by `stamp` together with the timestamp. All zeros before the first
    ## attempt.
  queuedAt*: Moment
    ## When `enqueue` took the task. Anchors the parked age (`isParkedExpired`).
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
  propagatedAnonymously*: bool
    ## Set when an anonymous path propagated the message. No store node confirms
    ## it: the query would name the message from this node's own address.
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

  # The message has no timestamp and no hash yet. `stamp` sets them at the
  # first send attempt.
  return ok(
    T(
      requestId: requestId,
      pubsubTopic: pubsubTopic,
      msg: msg,
      priority: envelop.priority,
    )
  )

proc isStamped*(self: DeliveryTask): bool =
  ## True once the task has its wire object: a timestamp and a hash.
  return self.msg.timestamp > 0

proc stamp*(self: DeliveryTask) =
  ## Gives the message the current time as its timestamp and computes its hash.
  ## The send service calls it right before the first send attempt, so the
  ## timestamp is fresh when the message reaches the network. A stamped task
  ## keeps its timestamp and its hash for all its retries.
  self.msg.timestamp = getNowInNanosecondTime()
  self.msgHash = computeMessageHash(self.pubsubTopic, self.msg)

proc clearStamp*(self: DeliveryTask) =
  ## Removes the timestamp, the hash and the proof of a task that no send
  ## processor got. The next attempt stamps the task again.
  self.msg.timestamp = 0
  self.msg.proof = @[]
  self.msgHash = EmptyWakuMessageHash

proc hashHex*(self: DeliveryTask): string =
  ## The hash for the events: `0x` hex once stamped, empty before.
  if self.isStamped():
    return self.msgHash.to0xHex()
  return ""

proc queuedAge*(self: DeliveryTask): timer.Duration =
  ## Time since `enqueue` took the task.
  return timer.Moment.now() - self.queuedAt

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
  ## measured from `enqueue`, so parking cannot grow the backlog and deliver
  ## arbitrarily late.
  return self.firstAdmittedTime.isNone() and self.queuedAge() > maxAge

proc isEphemeral*(self: DeliveryTask): bool =
  return self.msg.ephemeral
