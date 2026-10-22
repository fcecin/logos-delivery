## The send service. It runs one `deliver` for each queued message, with its
## retries, its admission and its Store confirmation.
##

import std/[algorithm, sequtils, tables, typetraits]
import chronos, chronicles
import brokers/broker_context
import
  ./[send_processor, relay_processor, lightpush_processor, mix_processor, delivery_task],
  logos_delivery/waku/[waku_core, waku_store/common],
  logos_delivery/waku/waku,
  logos_delivery/waku/api/[store, publish],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager
import logos_delivery/api/events/messaging_client_events
import logos_delivery/api/conf/modes
import logos_delivery/messaging/messaging_metrics

{.push raises: [].}

logScope:
  topics = "send service"

const DeliveryWindow* = chronos.minutes(1)
  ## The delivery window: how long an admitted task tries before it fails with no
  ## propagation (`Preferred` gets two, see `maxDeliveryTime`). Also the mix
  ## window of `MixSendProcessor` and the default Store validation window. A
  ## window longer than `StampRefreshAge` makes a task renew its message (see
  ## `stampExpired`).

proc maxDeliveryTime*(anonymityLevel: AnonymityLevel): timer.Duration =
  ## `Preferred` gets two windows: one for mix, then one for the plain path.
  if anonymityLevel == AnonymityLevel.Preferred:
    DeliveryWindow + DeliveryWindow
  else:
    DeliveryWindow

const DefaultMaxParkedAge* = chronos.minutes(30)
  ## Parked tasks never admitted within this age (from the message timestamp)
  ## are dropped with a `MessageErrorEvent`. Spans a few default RLN epochs.

const DefaultSendQueueCapacity* = 1000
  ## Hard cap on tasks tracked by the send service; further sends are rejected.

const RetryInterval* = chronos.seconds(1)
  ## The pause between two ticks of `retryLoop`, and between two Store validation
  ## rounds.

const ArchiveTime = chronos.seconds(3)
  ## The time a Store node needs to archive a message. A task is asked about in
  ## Store only after this time.

const StoreValidationQueryTimeout = chronos.seconds(15)
  ## Bounds one Store validation query with its dials. It exceeds one
  ## `DefaultDialTimeout`, so a dead Store peer leaves time for another.

const StampRefreshMargin = chronos.seconds(60)
  ## How much younger than `MaxMessageTimestampSkewPastSeconds` the stamp of a
  ## wire object must be for a retry to send it again: the network latency and
  ## the clock difference between peers, with margin.

const StampRefreshAge* =
  chronos.seconds(MaxMessageTimestampSkewPastSeconds) - StampRefreshMargin
  ## A retry sends the current wire object again only while its stamp is younger
  ## than this. A receiver accepts a timestamp up to
  ## `MaxMessageTimestampSkewPastSeconds` behind its clock, whatever the retry
  ## window of this node, so an older stamp gets a new wire object: a new
  ## timestamp, a new hash and, with RLN, a new proof.

const MaxSendsInFlight* = 50
  ## The number of sends that run at the same time. It bounds the open streams
  ## of this node independent of `sendQueueCapacity`.

type SendService* = ref object of RootObj
  brokerCtx: BrokerContext
  deliveryTasks: OrderedTable[RequestId, DeliveryTask]
    ## Each task that did not get its last event, by request id, in queue order.
    ## A restart starts the tasks in this order.
  running: bool
    ## True while each task has its `deliver`. While false, `enqueue` keeps each
    ## task until `startSendService`.
  admissionLock: AsyncLock
    ## One task at a time runs `admitAndProve` (see `runAdmission`).
  sendSlots: AsyncSemaphore
    ## One slot for each send that runs. An attempt, with its admission, runs
    ## with a slot held, so at most `maxSendsInFlight` sends run at the same time.
  retryTick: AsyncEvent
    ## `retryNow` fires it, and each task that waits on it starts its next attempt.
    ## Chronos wakes the waiters in the order that they started to wait, so the
    ## parked tasks reach `admissionLock` in the order that they parked.
  retryLoopHandle: Future[void] ## The `retryLoop`, nil while the service does not run.
  attempts: int
    ## The number of attempts that ended, in admission or in a send. Only tests
    ## read it.
  subscribeOnSend: bool ## True only for the None anonymity level.
  sendProcessor: BaseSendProcessor
  rateLimitManager: RateLimitManager
    ## Charges each wire object against the per-epoch budget. A retry of the same
    ## wire object is free.

  waku: Waku
  checkStoreForMessages: bool
  storeValidationHandle: Future[void]
    ## The Store validation loop. Nil when Store-based reliability is off.
  maxDeliveryTime*: timer.Duration
    ## How long an admitted task may keep trying before it is failed.
  maxParkedAge*: timer.Duration
    ## How old a never-admitted (parked) task may get before it is failed.
  maxValidationAge*: timer.Duration
    ## How long a task may wait for a Store confirmation before it fails. The
    ## time while the service is stopped does not count.
  archiveTime*: timer.Duration
    ## A task is asked about in Store only once its propagation and its last
    ## Store query are older than this, giving a Store node time to archive it.
    ## Also the pause of the Store validation loop while no Store peer is available.
  sendQueueCapacity*: int
  retryInterval: timer.Duration
  stampRefreshAge: timer.Duration
    ## See `StampRefreshAge`. Zero makes each retry a new wire object.

proc setupSendProcessorChain*(
    waku: Waku, anonymityLevel: AnonymityLevel
): Result[BaseSendProcessor, string] =
  let brokerCtx = waku.brokerCtx
  let isRelayAvail = waku.hasRelay()
  let isLightPushAvail = waku.hasLightpush()

  var processors = newSeq[BaseSendProcessor]()

  case anonymityLevel
  of AnonymityLevel.None:
    discard
  of AnonymityLevel.Preferred, AnonymityLevel.Required:
    if not isLightPushAvail:
      return err("Mix sending needs a lightpush client, which is not mounted")

    let mixProcessor: BaseSendProcessor =
      MixSendProcessor.new(waku, brokerCtx, anonymityLevel, DeliveryWindow)
    if anonymityLevel == AnonymityLevel.Required:
      return ok(mixProcessor)

    processors.add(mixProcessor)

  if isRelayAvail:
    let publishProc = waku.relayPushHandler()
    processors.add(
      RelaySendProcessor.new(isLightPushAvail, publishProc, waku, brokerCtx)
    )
  if isLightPushAvail:
    processors.add(LightpushSendProcessor.new(waku, brokerCtx))

  if processors.len == 0:
    return err("No valid send processor found for the delivery task")

  var currentProcessor: BaseSendProcessor = processors[0]
  for i in 1 ..< processors.len:
    currentProcessor.chain(processors[i])
    currentProcessor = processors[i]
    trace "Send processor chain", index = i, processor = type(processors[i]).name

  return ok(processors[0])

proc new*(
    T: typedesc[SendService],
    preferP2PReliability: bool,
    waku: Waku,
    rateLimitManager: RateLimitManager,
    sendProcessor: BaseSendProcessor,
    anonymityLevel: AnonymityLevel = AnonymityLevel.None,
    maxParkedAge: timer.Duration = DefaultMaxParkedAge,
    sendQueueCapacity: int = DefaultSendQueueCapacity,
    maxValidationAge: timer.Duration = DeliveryWindow,
    retryInterval: timer.Duration = RetryInterval,
    maxSendsInFlight: int = MaxSendsInFlight,
    stampRefreshAge: timer.Duration = StampRefreshAge,
): Result[T, string] =
  # With zero, `retryLoop` ticks at each turn of the event loop, and the waiting
  # tasks retry with no pause.
  if retryInterval <= ZeroDuration:
    return err("retryInterval must be positive")
  if maxSendsInFlight <= 0:
    return err("maxSendsInFlight must be positive")
  if stampRefreshAge < ZeroDuration:
    return err("stampRefreshAge must not be negative")

  let checkStoreForMessages = preferP2PReliability and waku.isStoreMounted()

  let sendService = SendService(
    brokerCtx: waku.brokerCtx,
    deliveryTasks: initOrderedTable[RequestId, DeliveryTask](),
    admissionLock: newAsyncLock(),
    sendSlots: newAsyncSemaphore(maxSendsInFlight),
    retryTick: newAsyncEvent(),
    sendProcessor: sendProcessor,
    rateLimitManager: rateLimitManager,
    waku: waku,
    checkStoreForMessages: checkStoreForMessages,
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
    maxParkedAge: maxParkedAge,
    maxValidationAge: maxValidationAge,
    archiveTime: ArchiveTime,
    sendQueueCapacity: sendQueueCapacity,
    retryInterval: retryInterval,
    stampRefreshAge: stampRefreshAge,
    subscribeOnSend: anonymityLevel == AnonymityLevel.None,
  )

  return ok(sendService)

proc subscribesOnSend*(self: SendService): bool =
  return self.subscribeOnSend

func isFull*(self: SendService): bool =
  return self.deliveryTasks.len >= self.sendQueueCapacity

proc storeConfirmationExpected(self: SendService, task: DeliveryTask): bool =
  ## True when a plain send of this task would wait for a store confirmation:
  ## reliability is on and the message is not ephemeral.
  return self.checkStoreForMessages and not task.isEphemeral()

proc awaitsStoreValidation*(self: SendService, task: DeliveryTask): bool =
  ## True while a propagated task still needs a store node to confirm it. A task
  ## that went out over mix never does: the store query would carry its hash in
  ## clear from this node's own address.
  return
    self.storeConfirmationExpected(task) and
    task.state == DeliveryState.SuccessfullyPropagated and not task.propagatedAnonymously

func storeValidationKey(task: DeliveryTask): Moment =
  ## The last Store query time, or the first propagation for a task never asked.
  if task.lastStoreQueryTime.isSome():
    task.lastStoreQueryTime.get()
  else:
    task.firstPropagatedTime.get()

proc nextStoreValidationBatch*(
    self: SendService, tasks: openArray[DeliveryTask], now: Moment
): seq[DeliveryTask] =
  ## Selects up to one Store page of tasks that await Store validation and whose
  ## propagation and last query are older than `archiveTime`. The tasks that
  ## waited longest come first.
  const batchSize = int(MaxPageSize)
  var eligible: seq[DeliveryTask]
  for task in tasks:
    if not self.awaitsStoreValidation(task):
      continue
    if task.firstPropagatedTime.isNone() or
        now - task.firstPropagatedTime.get() <= self.archiveTime:
      continue
    if task.lastStoreQueryTime.isSome() and
        now - task.lastStoreQueryTime.get() <= self.archiveTime:
      continue
    eligible.add(task)
  eligible.sort(
    proc(a, b: DeliveryTask): int =
      cmp(storeValidationKey(a), storeValidationKey(b))
  )
  if eligible.len > batchSize:
    eligible.setLen(batchSize)
  return eligible

func storeAnswerPending(task: DeliveryTask): bool =
  ## True while the `deliver` of the task waits for a Store confirmation.
  return not task.storeAnswer.isNil() and not task.storeAnswer.finished()

proc checkMsgsInStore(self: SendService, batch: seq[DeliveryTask]) {.async.} =
  ## Asks a Store peer about one batch and confirms the tasks that it reports.
  let now = Moment.now()
  for task in batch:
    task.lastStoreQueryTime = Opt.some(now)

  # TODO: confirm hash format for store query!!!
  let query = self.waku.storeQueryToAny(
    StoreQueryRequest(
      includeData: false,
      messageHashes: batch.mapIt(it.msgHash),
      paginationLimit: Opt.some(uint64(batch.len)),
    )
  )
  if not await query.withTimeout(StoreValidationQueryTimeout):
    debug "Store validation query timed out", messageCount = batch.len
    return

  let storeResp: StoreQueryResponse = query.read().valueOr:
    debug "Failed to get store validation for messages",
      messageCount = batch.len, error = $error
    return

  # The Store peer decides which hashes its answer lists, and it can list hashes
  # that this node did not ask about. Only a task whose `deliver` waits for a Store
  # confirmation takes a hash from the answer, so a message sent over mix is never
  # confirmed.
  var pending = initTable[WakuMessageHash, DeliveryTask]()
  for task in self.deliveryTasks.values:
    if task.storeAnswerPending():
      pending[task.msgHash] = task
  for item in storeResp.messages:
    let task = pending.getOrDefault(item.messageHash)
    if not task.isNil():
      task.storeAnswer.complete()

proc storeValidationLoop(self: SendService) {.async: (raises: [CancelledError]).} =
  ## Confirms propagated tasks against Store, one batch per round, apart from the
  ## sends, so that a slow Store peer does not delay them.
  while true:
    var delay = self.retryInterval
    try:
      let batch = self.nextStoreValidationBatch(
        toSeq(self.deliveryTasks.values).filterIt(it.storeAnswerPending()), Moment.now()
      )
      if batch.len > 0:
        if self.waku.hasStorePeer():
          await self.checkMsgsInStore(batch)
        else:
          debug "Skipping store validation, no store peer available",
            messageCount = batch.len
          delay = self.archiveTime
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      error "Exception in a Store validation round", error = exc.msg
    await sleepAsync(delay)

proc loggedHash(task: DeliveryTask): string =
  ## The hash for INFO and ERROR records, withheld once the task is anonymized,
  ## and unset before the first attempt stamps the task.
  if task.anonymized:
    "withheld"
  elif task.isStamped():
    task.hashHex()
  else:
    "unset"

func isQueued(self: SendService, task: DeliveryTask): bool =
  ## True when `task` itself is in the send queue.
  return self.deliveryTasks.getOrDefault(task.requestId) == task

func finish(self: SendService, task: DeliveryTask) =
  ## Removes a task that got its last event.
  if self.isQueued(task):
    self.deliveryTasks.del(task.requestId)

proc fail(self: SendService, task: DeliveryTask, errorDesc: string) =
  ## Fails the task with `MessageErrorEvent`, its last event.
  task.state = DeliveryState.FailedToDeliver
  task.errorDesc = errorDesc
  MessageErrorEvent.emit(self.brokerCtx, task.requestId, task.hashHex(), errorDesc)
  self.finish(task)

proc failExpired(self: SendService, task: DeliveryTask): bool =
  ## Fails the task when its delivery window or its parked age ended.
  if task.isDeliveryTimedOut(self.maxDeliveryTime):
    # A processor that leaves a task for a retry can write why in `errorDesc`,
    # as the mix processor does for a `Required` task that mix cannot attempt yet.
    # Report that reason if set.
    let errorDesc =
      if task.errorDesc.len > 0:
        task.errorDesc
      else:
        "Unable to send within retry time window"
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = errorDesc,
      age = task.admissionAge()
    self.fail(task, errorDesc)
    return true
  if task.isParkedExpired(self.maxParkedAge):
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = "Parked message too old",
      age = task.queuedAge()
    self.fail(
      task, "No rate-limit budget came within the maximum wait of a queued message"
    )
    return true
  return false

proc reportPropagated(self: SendService, task: DeliveryTask) =
  ## Emits `MessagePropagatedEvent`, and `MessageSentEvent` for a mixed send that
  ## a plain send would confirm in Store.
  # INFO lines reach log collectors, where a hash would tie this node to an
  # anonymized message; `MixSendProcessor` still logs it at DEBUG.
  info "Message successfully propagated",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessagePropagatedEvent.emit(self.brokerCtx, task.requestId, task.hashHex())
  task.propagatedEventEmitted = true
  if task.propagatedAnonymously and self.storeConfirmationExpected(task):
    # The exit's reply completes a mixed send when a plain send would wait for
    # a store confirmation, so both paths end with the same event. The INFO line
    # omits the hash.
    info "Message successfully sent over mix", requestId = task.requestId
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.hashHex())

proc reportTaskQueued(self: SendService, task: DeliveryTask) =
  ## Emits `MessageQueuedEvent` once for a task that waits for epoch budget.
  if task.queuedEventEmitted:
    return

  info "Message queued for rate-limit budget",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessageQueuedEvent.emit(self.brokerCtx, task.requestId, task.hashHex())
  task.queuedEventEmitted = true

proc admitAndProve(self: SendService, task: DeliveryTask): Future[bool] {.async.} =
  ## Charges the epoch budget, then attaches the RLN proof, so a message over the
  ## budget reserves no RLN message id. It charges a task once for each proof. Returns false
  ## when the task waits for its next attempt or failed.
  if not self.sendProcessor.checkBeforeAdmission(task):
    return false

  if not task.admitted:
    # Ephemeral traffic is shed rather than queued so it cannot eat into the
    # budget left for durable messages.
    if task.isEphemeral():
      let quotaState = await self.rateLimitManager.quotaState()
      if quotaState != QuotaState.Normal:
        debug "Dropping ephemeral message as we are approaching rate-limit quota",
          requestId = task.requestId, msgHash = task.hashHex(), quotaState = quotaState
        task.state = DeliveryState.FailedToDeliver
        task.errorDesc = "Ephemeral message dropped: rate limit " & $quotaState
        return false

    (await self.rateLimitManager.admit(task.msg.payload)).isOkOr:
      debug "Over rate-limit budget, task waits for the epoch to roll",
        requestId = task.requestId, msgHash = task.hashHex()
      self.reportTaskQueued(task)
      return false
    task.admitted = true
    if task.firstAdmittedTime.isNone():
      task.firstAdmittedTime = Opt.some(Moment.now())

  if not task.isStamped():
    # The first attempt starts here. The timestamp sets the epoch of the proof
    # and the age that the Store nodes and the RLN validators check, so the task
    # takes it now and not at the call to `send`.
    task.stamp()

  # The message timestamp sets its epoch, so a backend past that epoch or out of
  # budget fails the task. Other errors wait for a retry.
  task.msg = (await self.waku.attachRlnProof(task.msg)).valueOr:
    if not task.stampTried:
      # No send processor got the wire object, so the next attempt makes a new
      # one.
      task.clearStamp()
    case error.kind
    of RlnErrorKind.Permanent, RlnErrorKind.BudgetExhausted:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = "Failed to attach RLN proof: " & $error
    of RlnErrorKind.NotReady, RlnErrorKind.Transient:
      debug "Failed to attach RLN proof, the task waits for a retry",
        requestId = task.requestId, msgHash = task.hashHex(), error = $error
    return false

  return true

func needsAdmission(self: SendService, task: DeliveryTask): bool =
  ## True when the task needs a budget charge, a stamp or an RLN proof. A task
  ## that needs none of them does not wait for another task in `admitAndProve`.
  return not task.admitted or not task.isStamped() or self.waku.needsRlnProof(task.msg)

proc stampExpired(self: SendService, task: DeliveryTask): bool =
  ## True when a retry must not send the current wire object again: its stamp is
  ## older than `stampRefreshAge`, so a receiver would soon refuse it, or its RLN
  ## epoch is not the current one, so a validator refuses its proof and the
  ## backend draws no message id for it. Both come from this node's clock, so the
  ## sender decides before a receiver does.
  if task.stampAge() > self.stampRefreshAge:
    return true
  let stampEpoch = self.waku.rlnEpochIndexOf(task.msg.timestamp).valueOr:
    return false
  let currentEpoch = self.waku.rlnEpochIndexOf(getNowInNanosecondTime()).valueOr:
    return false
  return stampEpoch != currentEpoch

proc runAdmission(
    self: SendService, task: DeliveryTask
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Runs `admitAndProve` with `admissionLock` held. The budget check reads the
  ## RLN quota, which counts only the message ids that earlier proofs reserved,
  ## so each task reserves its id before the next task checks the budget.
  await self.admissionLock.acquire()
  try:
    return await self.admitAndProve(task)
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    error "Exception in admission",
      requestId = task.requestId, msgHash = task.loggedHash(), error = exc.msg
    return false
  finally:
    try:
      self.admissionLock.release()
    except AsyncLockError:
      discard

proc retryNow(self: SendService) =
  ## Starts the next attempt of each task that waits for one.
  self.retryTick.fire()
  self.retryTick.clear()

proc retryLoop(self: SendService) {.async: (raises: [CancelledError]).} =
  ## Runs `retryNow` each `retryInterval`.
  while true:
    await sleepAsync(self.retryInterval)
    self.retryNow()

proc sendUntilPropagated(
    self: SendService, task: DeliveryTask
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Makes the first attempt at once, then one attempt at each tick of
  ## `retryLoop`, until the task propagates. Returns false when the task failed.
  ## A retry whose wire object expired (`stampExpired`) makes a new one first.
  while true:
    # The task holds a send slot through its admission, so the stamp of its first
    # attempt is taken when the send can run at once.
    await self.sendSlots.acquire()
    try:
      if task.isStamped() and task.stampTried and self.stampExpired(task):
        # The old wire object can be on the network, so the new one is a new
        # message, and it is charged as one.
        debug "Retry makes a new wire object",
          requestId = task.requestId,
          oldHash = task.hashHex(),
          stampAge = task.stampAge()
        task.clearStamp()
        task.admitted = false
      if not self.needsAdmission(task) or await self.runAdmission(task):
        task.stampTried = true
        try:
          await self.sendProcessor.process(task)
        except CancelledError as exc:
          raise exc
        except CatchableError as exc:
          # The send path returns remote errors as results, so an exception is a
          # local fault.
          error "Exception in a send attempt",
            requestId = task.requestId, msgHash = task.loggedHash(), error = exc.msg
    finally:
      self.sendSlots.release2()
    self.attempts.inc()
    case task.state
    of DeliveryState.SuccessfullyPropagated:
      self.reportPropagated(task)
      return true
    of DeliveryState.FailedToDeliver:
      # A mix exit may have published the message even though its reply was lost.
      error "Failed to send message",
        requestId = task.requestId, msgHash = task.loggedHash(), error = task.errorDesc
      self.fail(task, task.errorDesc)
      return false
    else:
      # All other states wait for the next attempt: `process` left `NextRoundRetry`,
      # the admission did not admit the task, or an exception ended `process` before
      # it moved `Entry` or `FallbackRetry`.
      task.state = DeliveryState.NextRoundRetry
    if self.failExpired(task):
      return false
    await self.retryTick.wait()

proc waitForStoreValidation(
    self: SendService, task: DeliveryTask
) {.async: (raises: [CancelledError]).} =
  ## Waits for a Store node to report the message, until the waits of the task
  ## add up to `maxValidationAge`.
  let left = self.maxValidationAge - task.storeValidationWaited
  let started = Moment.now()
  task.storeAnswer = newFuture[void]("store confirmation")
  try:
    if left > ZeroDuration and await task.storeAnswer.withTimeout(left):
      task.state = DeliveryState.SuccessfullyValidated
      # An anonymized task gets here only through the clear republish of a
      # `Preferred` send whose mix reply was lost.
      info "Message successfully sent",
        requestId = task.requestId, msgHash = task.loggedHash()
      MessageSentEvent.emit(self.brokerCtx, task.requestId, task.hashHex())
      self.finish(task)
    else:
      debug "Message propagated but not validated by a store node within time window; stop trying.",
        requestId = task.requestId,
        msgHash = task.hashHex(),
        waited = task.storeValidationWaited + (Moment.now() - started)
      recordStoreValidationTimeout()
      self.fail(
        task,
        "Propagated but not confirmed by a store node within the store validation window",
      )
  finally:
    task.storeValidationWaited = task.storeValidationWaited + (Moment.now() - started)
    task.storeAnswer = nil

proc deliver(
    self: SendService, task: DeliveryTask
) {.async: (raises: [CancelledError]).} =
  ## Sends the task until it propagates, then waits for a Store confirmation
  ## when one is expected. Each event goes out when its step ends.
  try:
    if not task.propagatedEventEmitted:
      if not await self.sendUntilPropagated(task):
        return
    if self.awaitsStoreValidation(task):
      await self.waitForStoreValidation(task)
    else:
      self.finish(task)
  except CancelledError as exc:
    # A cancel can stop `process` before it moves `Entry` or `FallbackRetry`.
    # The task waits for its next attempt, as after any other attempt.
    if task.state == DeliveryState.Entry or task.state == DeliveryState.FallbackRetry:
      task.state = DeliveryState.NextRoundRetry
    raise exc

func isRunning(task: DeliveryTask): bool =
  ## True while the `deliver` of the task runs.
  return not task.deliverHandle.isNil() and not task.deliverHandle.finished()

proc startDeliverSoon(self: SendService, task: DeliveryTask) =
  ## Starts `deliver` in a later turn of the event loop, when the service runs
  ## then. The caller of `enqueue` gets its result before any event of the task.
  callSoon(
    proc(udata: pointer) {.gcsafe, raises: [].} =
      if self.running and not task.isRunning() and self.isQueued(task):
        task.deliverHandle = self.deliver(task)
  )

proc enqueue*(self: SendService, task: DeliveryTask): Result[RequestId, string] =
  ## Puts `task` in the send queue and returns its request id. It makes no
  ## attempt and emits no event, so the caller gets the result first. The task
  ## has no timestamp and no hash yet: `admitAndProve` stamps it at its first
  ## attempt. Two sends of one message are two tasks.
  assert(not task.isNil(), "task for enqueue must not be nil")
  if self.deliveryTasks.hasKey(task.requestId):
    return err("Request id already in the send queue")
  if self.isFull():
    return err("Send queue full, retry later")

  task.queuedAt = Moment.now()
  debug "Task added to the send queue", requestId = task.requestId
  self.deliveryTasks[task.requestId] = task
  self.startDeliverSoon(task)
  return ok(task.requestId)

proc startSendService*(self: SendService) =
  ## Starts each task. Call it after `stopSendService` ends. A call while the
  ## service runs does nothing.
  if self.running:
    return
  self.running = true
  for task in self.deliveryTasks.values:
    self.startDeliverSoon(task)
  self.retryLoopHandle = self.retryLoop()
  if self.checkStoreForMessages:
    self.storeValidationHandle = self.storeValidationLoop()

proc stopSendService*(self: SendService) {.async: (raises: []).} =
  ## Cancels each `deliver`, `retryLoop` and the Store validation loop. The tasks
  ## stay in the send queue with no event, and `startSendService` starts them
  ## again.
  self.running = false
  var handles: seq[Future[void]]
  for handle in [self.retryLoopHandle, self.storeValidationHandle]:
    if not handle.isNil():
      handles.add(handle)
  self.retryLoopHandle = nil
  self.storeValidationHandle = nil
  for task in self.deliveryTasks.values:
    if task.isRunning():
      handles.add(task.deliverHandle)
  await cancelAndWait(handles)
