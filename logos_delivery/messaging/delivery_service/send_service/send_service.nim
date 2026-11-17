## The send service. It runs one `deliver` for each queued message, and `deliver` runs
## the message through its delivery plan to its last event (see `delivery_plan`).
##

import std/[algorithm, sequtils, sets, tables]
import chronos, chronicles
import brokers/broker_context
import
  ./[
    delivery_plan, send_processor, relay_processor, lightpush_processor, mix_processor,
    delivery_task,
  ],
  logos_delivery/waku/[waku_core, waku_store/common],
  logos_delivery/waku/waku,
  logos_delivery/waku/api/[store, publish],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager
import logos_delivery/api/events/messaging_client_events
import logos_delivery/api/conf/modes
import logos_delivery/messaging/messaging_metrics

export delivery_plan, send_processor

{.push raises: [].}

logScope:
  topics = "send service"

const DeliveryWindow* = chronos.minutes(1)
  ## The delivery window: how long an admitted task tries before it fails with no
  ## propagation (`Preferred` gets two, see `maxDeliveryTime`). Also the mix
  ## window at `Preferred` and the default Store validation window.

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

const ResendTimeout* = chronos.seconds(5)
  ## Cancels a resend that gets no answer in this time.
  ## A peer that never answers keeps it running until a Store answer or the window end.

const ArchiveTime = chronos.seconds(3)
  ## The time a Store node needs to archive a message. A task is asked about in
  ## Store only after this time.

const StoreValidationQueryTimeout = chronos.seconds(15)
  ## Bounds one Store validation query with its dials. It exceeds one
  ## `DefaultDialTimeout`, so a dead Store peer leaves time for another.

const MaxStoreMissResends* = 3 ## The most resends of one message after a Store miss.

const MaxSendsInFlight* = 50
  ## The number of attempts that run at the same time. It bounds the open streams
  ## of this node independent of `sendQueueCapacity`.

type SendService* = ref object of RootObj
  brokerCtx: BrokerContext
  deliveryTasks: OrderedTable[WakuMessageHash, DeliveryTask]
    ## Each task that did not get its last event, by message hash, in queue order.
    ## A restart starts the tasks in this order.
  running: bool
    ## True while each task has its `deliver`. While false, `enqueue` keeps each
    ## task until `startSendService`.
  admissionLock: AsyncLock
    ## One task at a time runs `admitAndProve` (see `admissionStep`).
  sendSlots: AsyncSemaphore
    ## One slot for each attempt that runs, so at most `maxSendsInFlight`
    ## attempts run at the same time.
  retryTick: AsyncEvent
    ## `retryNow` fires it, and each task that waits on it starts its next round.
    ## Chronos wakes the waiters in the order that they started to wait, so the
    ## parked tasks reach `admissionLock` in the order that they parked.
  retryLoopHandle: Future[void] ## The `retryLoop`, nil while the service does not run.
  attempts: int
    ## The number of attempts that ended, in admission or in a send. Only tests
    ## read it.
  plan: DeliveryPlan
  processors: SendProcessors
  rateLimitManager: RateLimitManager
    ## Charges first transmissions against the per-epoch budget; re-publishes
    ## are free.
  waku: Waku
  checkStoreForMessages: bool
  storeValidationHandle: Future[void]
    ## The Store validation loop. Nil when Store-based reliability is off.
  mixFallback: StepOutcome
    ## Why the last task went from mix to the plain path, `None` once mix can
    ## attempt a task again. INFO logs each change.
  maxDeliveryTime*: timer.Duration
    ## How long an admitted task may keep trying before it is failed.
  mixWindow: timer.Duration ## How long after its admission mix keeps a `Preferred` task.
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

proc setupSendProcessors*(
    waku: Waku, anonymityLevel: AnonymityLevel
): Result[SendProcessors, string] =
  ## The send processors of the protocols that `waku` mounts.
  let brokerCtx = waku.brokerCtx
  var processors = SendProcessors()
  if anonymityLevel != AnonymityLevel.None:
    if not waku.hasLightpush():
      return err("Mix sending needs a lightpush client, which is not mounted")
    processors.mix = MixSendProcessor.new(waku, brokerCtx)
    if anonymityLevel == AnonymityLevel.Required:
      return ok(processors)
  if waku.hasRelay():
    processors.relay = RelaySendProcessor.new(waku.relayPushHandler(), brokerCtx)
  if waku.hasLightpush():
    let lightpush = LightpushSendProcessor.new(waku, brokerCtx)
    processors.lightpush = lightpush
    processors.resender = lightpush
  if processors.transports().len == 0:
    return err("No valid send processor found for the delivery task")
  return ok(processors)

proc new*(
    T: typedesc[SendService],
    preferP2PReliability: bool,
    waku: Waku,
    rateLimitManager: RateLimitManager,
    processors: SendProcessors,
    anonymityLevel: AnonymityLevel = AnonymityLevel.None,
    maxParkedAge: timer.Duration = DefaultMaxParkedAge,
    sendQueueCapacity: int = DefaultSendQueueCapacity,
    maxValidationAge: timer.Duration = DeliveryWindow,
    retryInterval: timer.Duration = RetryInterval,
    maxSendsInFlight: int = MaxSendsInFlight,
): Result[T, string] =
  # With zero, `retryLoop` ticks at each turn of the event loop, and the waiting
  # tasks retry with no pause.
  if retryInterval <= ZeroDuration:
    return err("retryInterval must be positive")
  if maxSendsInFlight <= 0:
    return err("maxSendsInFlight must be positive")
  if maxValidationAge == InfiniteDuration:
    return err("maxValidationAge must be finite")

  let plan = ?buildDeliveryPlan(
    anonymityLevel, processors.transports(), not processors.resender.isNil()
  )
  let checkStoreForMessages = preferP2PReliability and waku.isStoreMounted()

  let sendService = SendService(
    brokerCtx: waku.brokerCtx,
    deliveryTasks: initOrderedTable[WakuMessageHash, DeliveryTask](),
    admissionLock: newAsyncLock(),
    sendSlots: newAsyncSemaphore(maxSendsInFlight),
    retryTick: newAsyncEvent(),
    plan: plan,
    processors: processors,
    rateLimitManager: rateLimitManager,
    waku: waku,
    checkStoreForMessages: checkStoreForMessages,
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
    mixWindow: DeliveryWindow,
    maxParkedAge: maxParkedAge,
    maxValidationAge: maxValidationAge,
    archiveTime: ArchiveTime,
    sendQueueCapacity: sendQueueCapacity,
    retryInterval: retryInterval,
  )

  return ok(sendService)

func isFull*(self: SendService): bool =
  return self.deliveryTasks.len >= self.sendQueueCapacity

func mixFallback*(self: SendService): StepOutcome =
  self.mixFallback

proc storeConfirmationExpected(self: SendService, task: DeliveryTask): bool =
  ## True when a plain send of this task would wait for a store confirmation:
  ## reliability is on and the message is not ephemeral.
  return self.checkStoreForMessages and not task.isEphemeral()

proc afterPropagation*(self: SendService, task: DeliveryTask): DeliveryState =
  return
    afterPropagation(task.propagatedAnonymously, self.storeConfirmationExpected(task))

func awaitsStoreValidation*(task: DeliveryTask): bool =
  ## True while a propagated task waits for a store node to confirm it. A task
  ## that went out over mix never does: the store query would carry its hash in
  ## clear from this node's own address.
  return task.state in StoreWaitStates and not task.propagatedAnonymously

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
    if not task.awaitsStoreValidation():
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

proc queryStoreForTasks(
    self: SendService, shards: seq[PubsubTopic], tasks: seq[DeliveryTask]
) {.async.} =
  ## Sends one Store query about `tasks`, whose messages are in `shards`.
  ## Sets the Store answer of each task.
  ## With no `shards`, any Store peer can answer.
  # TODO: confirm hash format for store query!!!
  let query = self.waku.storeQueryToAny(
    StoreQueryRequest(
      includeData: false,
      messageHashes: tasks.mapIt(it.msgHash),
      paginationLimit: Opt.some(uint64(tasks.len)),
    ),
    shards,
  )
  if not await query.withTimeout(StoreValidationQueryTimeout):
    debug "Store validation query timed out", messageCount = tasks.len
    return

  let storeResp: StoreQueryResponse = query.read().valueOr:
    debug "Failed to get store validation for messages",
      messageCount = tasks.len, error = $error
    return

  # The Store peer decides which hashes its answer lists, and it can list hashes
  # that this node did not ask about. Only a task whose `deliver` waits for a Store
  # confirmation takes a hash from the answer, so a message sent over mix is never
  # confirmed.
  var reported: HashSet[WakuMessageHash]
  for item in storeResp.messages:
    reported.incl(item.messageHash)
    let task = self.deliveryTasks.getOrDefault(item.messageHash)
    if not task.isNil() and not task.storeAnswer.isNil():
      task.storeAnswer.setAnswer(StepOutcome.StoreReported)

  if storeResp.paginationCursor.isSome():
    # The answer is partial: a hash that it lacks can be on the next page.
    return
  for task in tasks:
    if task.msgHash notin reported and not task.storeAnswer.isNil():
      task.storeMissed = true
      task.storeAnswer.setAnswer(StepOutcome.StoreMissed)

proc checkMsgsInStore(self: SendService, batch: seq[DeliveryTask]) {.async.} =
  ## Asks Store about one batch. The tasks that a Store peer did not report go
  ## in one query, which can ask each Store peer. The other tasks go in one
  ## query for each shard.
  let now = Moment.now()
  var missed: seq[DeliveryTask]
  var byShard: OrderedTable[PubsubTopic, seq[DeliveryTask]]
  for task in batch:
    task.lastStoreQueryTime = Opt.some(now)
    if task.storeMissed:
      missed.add(task)
    else:
      byShard.mgetOrPut(task.pubsubTopic, @[]).add(task)
  var checks: seq[Future[void]]
  if missed.len > 0:
    checks.add(self.queryStoreForTasks(@[], missed))
  for shard, tasks in byShard:
    checks.add(self.queryStoreForTasks(@[shard], tasks))
  try:
    for check in checks:
      await check
  finally:
    let running = checks.filterIt(not it.finished())
    if running.len > 0:
      await noCancel allFutures(running.mapIt(it.cancelAndWait()))

proc storeValidationLoop(self: SendService) {.async: (raises: [CancelledError]).} =
  ## Confirms propagated tasks against Store, one batch per round, apart from the
  ## sends, so that a slow Store peer does not delay them.
  while true:
    var delay = self.retryInterval
    try:
      let batch = self.nextStoreValidationBatch(
        toSeq(self.deliveryTasks.values).filterIt(not it.storeAnswer.isNil()),
        Moment.now(),
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

func isQueued(self: SendService, task: DeliveryTask): bool =
  ## True when `task` itself is in the send queue.
  return self.deliveryTasks.getOrDefault(task.msgHash) == task

func finish(self: SendService, task: DeliveryTask) =
  ## Removes a task that got its last event.
  if self.isQueued(task):
    self.deliveryTasks.del(task.msgHash)

func errorText*(task: DeliveryTask): string =
  ## The text of the error event of a task that failed.
  case task.finalOutcome
  of StepOutcome.WindowElapsed:
    "Unable to send within retry time window"
  of StepOutcome.ParkedTooLong:
    "No rate-limit budget came within the maximum wait of a queued message"
  of StepOutcome.StoreWindowElapsed:
    "Propagated but not confirmed by a store node within the store validation window"
  of StepOutcome.TooLargeForMix:
    MixTooLargeReason & ": " & task.lastErrorDesc
  of StepOutcome.ProofSizeUnknown:
    "Failed to determine RLN proof size: " & task.lastErrorDesc
  of StepOutcome.ProofPermanent, StepOutcome.ProofBudgetExhausted:
    "Failed to attach RLN proof: " & task.lastErrorDesc
  of StepOutcome.EphemeralDropped:
    "Ephemeral message dropped: rate limit " & task.lastErrorDesc
  of StepOutcome.MixNotMounted, StepOutcome.MixUnavailable, StepOutcome.MixSelfHop,
      StepOutcome.MixNoExit:
    mixUnusableText(task.finalOutcome)
  of StepOutcome.NoRelayPeers, StepOutcome.ZeroPeers:
    "No relay peer to publish the message to"
  of StepOutcome.Rejected, StepOutcome.ProofRejected:
    task.lastErrorDesc
  else:
    $task.finalOutcome

proc reportPropagated(self: SendService, task: DeliveryTask) =
  # INFO lines reach log collectors, where a hash would tie this node to an
  # anonymized message; `MixSendProcessor` still logs it at DEBUG.
  info "Message successfully propagated",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessagePropagatedEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  task.propagatedEventEmitted = true

proc reportTaskQueued(self: SendService, task: DeliveryTask) =
  ## Emits `MessageQueuedEvent` once for a task that waits for epoch budget.
  if task.queuedEventEmitted:
    return

  info "Message queued for rate-limit budget",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessageQueuedEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  task.queuedEventEmitted = true

proc reportMixFallback(self: SendService, task: DeliveryTask, outcome: StepOutcome) =
  debug "Handing the task from mix to the plain send path",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    outcome = outcome,
    admissionAge = task.admissionAge()
  if outcome in MixUnusableOutcomes and self.mixFallback != outcome:
    self.mixFallback = outcome
    info "Mix cannot carry messages, sending them over the plain path instead",
      reason = mixUnusableText(outcome)

proc reportEnd(self: SendService, task: DeliveryTask) =
  case task.state
  of DeliveryState.Stored:
    # An anonymized task gets here only through the clear republish of a
    # `Preferred` send whose mix reply was lost.
    info "Message successfully sent",
      requestId = task.requestId, msgHash = task.loggedHash()
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  of DeliveryState.MixDelivered:
    # The reply of the exit ends a mixed send, so it gets the event of a stored send.
    info "Message successfully sent over mix", requestId = task.requestId
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  of DeliveryState.NotStored:
    if task.finalOutcome == StepOutcome.StoreWindowElapsed:
      info "Message propagated but not validated by a store node within time window; stop trying.",
        requestId = task.requestId,
        msgHash = task.loggedHash(),
        waited = task.storeValidationWaited
      recordStoreValidationTimeout()
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorText()
    )
  of DeliveryState.NotPropagated:
    # A mix exit may have published the message even though its reply was lost.
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = task.errorText(),
      finalOutcome = task.finalOutcome,
      lastError = task.lastError,
      age =
        if task.finalOutcome == StepOutcome.ParkedTooLong:
          task.messageAge()
        else:
          task.admissionAge()
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorText()
    )
  else:
    discard

func needsAdmission(self: SendService, task: DeliveryTask): bool =
  ## True when the task needs an epoch budget charge or an RLN proof. A task that
  ## needs neither does not wait for another task in `admitAndProve`.
  return task.firstAdmittedTime.isNone() or self.waku.needsRlnProof(task.msg)

func roundStart(self: SendService, task: DeliveryTask): DeliveryState =
  if self.needsAdmission(task):
    DeliveryState.Admission
  else:
    self.plan.chain[0]

proc admitAndProve(
    self: SendService, task: DeliveryTask
): Future[StepResult] {.async.} =
  ## Charges the epoch budget, then attaches the RLN proof, so a message over the
  ## budget reserves no RLN message id. It charges a task once for each proof.
  ## The task records its charge and proof right away, so a stop cannot charge it twice.
  if self.plan.level == AnonymityLevel.Required:
    self.processors.processorOf(self.plan.chain[0]).checkBeforeAdmission(task).isOkOr:
      return error

  if task.firstAdmittedTime.isNone():
    # Ephemeral traffic is shed rather than queued so it cannot eat into the
    # budget left for durable messages.
    if task.isEphemeral():
      let quotaState = await self.rateLimitManager.quotaState()
      if quotaState != QuotaState.Normal:
        debug "Dropping ephemeral message as we are approaching rate-limit quota",
          requestId = task.requestId,
          msgHash = task.msgHash.to0xHex(),
          quotaState = quotaState
        return StepResult.init(StepOutcome.EphemeralDropped, $quotaState)

    (await self.rateLimitManager.admit(task.msg.payload)).isOkOr:
      debug "Over rate-limit budget, task waits for the epoch to roll",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      return StepResult.init(StepOutcome.OverBudget)
    task.firstAdmittedTime = Opt.some(Moment.now())

  # The message timestamp sets its epoch, so a backend past that epoch or out of
  # budget fails the task. Other errors wait for a retry.
  task.msg = (await self.waku.attachRlnProof(task.msg)).valueOr:
    let outcome =
      case error.kind
      of RlnErrorKind.Permanent: StepOutcome.ProofPermanent
      of RlnErrorKind.BudgetExhausted: StepOutcome.ProofBudgetExhausted
      of RlnErrorKind.NotReady: StepOutcome.ProofNotReady
      of RlnErrorKind.Transient: StepOutcome.ProofTransient
    debug "Failed to attach RLN proof",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      outcome = outcome,
      error = $error
    return StepResult.init(outcome, $error)

  return StepResult.init(StepOutcome.Admitted)

proc admissionStep(
    self: SendService, task: DeliveryTask
): Future[StepResult] {.async: (raises: [CancelledError]).} =
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
    return StepResult.init(StepOutcome.StepRaised, exc.msg)
  finally:
    self.admissionLock.release2()

proc runAttempt(
    self: SendService,
    processor: BaseSendProcessor,
    task: DeliveryTask,
    timeout = InfiniteDuration,
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  ## One attempt of `processor`, cancelled after `timeout`.
  ## A processor returns a remote error as a result, so an exception is a local fault.
  let attempt = processor.attempt(task)
  try:
    if not await attempt.withTimeout(timeout):
      debug "Send attempt timed out",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      return StepResult.init(StepOutcome.AttemptTimedOut)
    return attempt.read()
  except CancelledError as exc:
    raise exc
  except CatchableError as exc:
    error "Exception in a send attempt",
      requestId = task.requestId, msgHash = task.loggedHash(), error = exc.msg
    return StepResult.init(StepOutcome.StepRaised, exc.msg)

proc transportStep(
    self: SendService, task: DeliveryTask, state: DeliveryState
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  ## One attempt over the transport of `state`, after checks with no network call.
  let processor = self.processors.processorOf(state)
  # The wait for a slot can be long, so each check comes after it.
  await self.sendSlots.acquire()
  try:
    processor.checkBeforeAttempt(task).isOkOr:
      return error
    if state == DeliveryState.Mix and self.mixFallback != StepOutcome.None:
      self.mixFallback = StepOutcome.None
      info "Mix can carry messages again"
    if task.isDeliveryTimedOut(self.maxDeliveryTime):
      return StepResult.init(StepOutcome.WindowElapsed)
    if state == DeliveryState.Mix and self.plan.level == AnonymityLevel.Preferred and
        task.admissionAge() > self.mixWindow:
      return StepResult.init(StepOutcome.MixWindowElapsed)

    task.tryCount.inc()
    if state == DeliveryState.Mix:
      # Mark before the attempt: the exit publishes before it replies, so a message
      # whose reply is lost is still on the network. From here on, the send service
      # logs no hash for this task at INFO or ERROR.
      task.anonymized = true
    return await self.runAttempt(processor, task)
  finally:
    self.sendSlots.release2()

proc cancelPending(futures: seq[FutureBase]) {.async: (raises: []).} =
  var pending: seq[Future[void]]
  for future in futures:
    if not future.isNil() and not future.finished():
      pending.add(future.cancelAndWait())
  await noCancel allFutures(pending)

proc storeResendStep(
    self: SendService, task: DeliveryTask
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  ## Sends a message that no Store node reported once more. A service node that
  ## has not seen the message, such as a Store node that joined the mesh after the
  ## first propagation, relays it. Relay is not used, as GossipSub drops a message
  ## that it already saw.
  ## A Store report or the end of the Store window ends the step.
  let answer = task.storeAnswer
  if answer.takeAnswer() == StepOutcome.StoreReported:
    return StepResult.init(StepOutcome.StoreReported)
  if answer.deadline <= Moment.now():
    return StepResult.init(StepOutcome.StoreWindowElapsed)
  task.resendCount.inc()
  let resend = self.runAttempt(self.processors.resender, task, ResendTimeout)
  var answered, windowEnd: Future[void]
  try:
    while true:
      # A miss while the resend runs does not start a second resend.
      if answer.takeAnswer() == StepOutcome.StoreReported:
        return StepResult.init(StepOutcome.StoreReported)
      let left = answer.deadline - Moment.now()
      if left <= ZeroDuration:
        return StepResult.init(StepOutcome.StoreWindowElapsed)
      answered = answer.waitAnswer()
      windowEnd = sleepAsync(left)
      discard
        await race(FutureBase(resend), FutureBase(answered), FutureBase(windowEnd))
      if resend.finished():
        let res = await resend
        debug "Resend after a Store miss finished",
          requestId = task.requestId,
          msgHash = task.msgHash.to0xHex(),
          resendCount = task.resendCount,
          outcome = res.outcome
        return res
      await cancelPending(@[FutureBase(answered), FutureBase(windowEnd)])
  finally:
    await cancelPending(
      @[FutureBase(resend), FutureBase(answered), FutureBase(windowEnd)]
    )

proc storeWaitStep(
    self: SendService, task: DeliveryTask
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  ## Waits for a Store answer until the Store validation window of the task ends.
  let answer = task.storeAnswer
  while true:
    let outcome = answer.takeAnswer()
    if outcome != StepOutcome.None:
      return StepResult.init(outcome)
    let left = answer.deadline - Moment.now()
    if left <= ZeroDuration:
      return StepResult.init(StepOutcome.StoreWindowElapsed)
    discard await answer.waitAnswer().withTimeout(left)

proc retryWaitStep(
    self: SendService, task: DeliveryTask
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  if task.isDeliveryTimedOut(self.maxDeliveryTime):
    return StepResult.init(StepOutcome.WindowElapsed)
  if task.isParkedExpired(self.maxParkedAge):
    return StepResult.init(StepOutcome.ParkedTooLong)
  await self.retryTick.wait()
  return StepResult.init(StepOutcome.RetryTick)

proc runStep(
    self: SendService, task: DeliveryTask, state: DeliveryState
): Future[StepResult] {.async: (raises: [CancelledError]).} =
  case state
  of DeliveryState.Admission:
    return await self.admissionStep(task)
  of DeliveryState.Relay, DeliveryState.Lightpush, DeliveryState.Mix:
    return await self.transportStep(task, state)
  of DeliveryState.StoreResend:
    return await self.storeResendStep(task)
  of DeliveryState.NextRoundRetry:
    return await self.retryWaitStep(task)
  of DeliveryState.AwaitingStore:
    return await self.storeWaitStep(task)
  of DeliveryState.Entry, DeliveryState.Stored, DeliveryState.MixDelivered,
      DeliveryState.Propagated, DeliveryState.NotStored, DeliveryState.NotPropagated:
    return StepResult()

proc refreshRlnProof*(task: DeliveryTask, waku: Waku): bool =
  ## The service refused the task's proof as RLN-invalid: its proof went stale
  ## against a moved merkle root. Schedules a background merkle-path refresh and
  ## clears the proof so the next retry regenerates one against the refreshed
  ## path; `attachRlnProof` reuses an existing proof, so without the clear the
  ## rejected bytes would be resent. Resetting admission charges the new nonce
  ## the regenerated proof draws. Without a backend refresh hook a retry would
  ## be rejected the same way, so it returns false.
  if not waku.onRlnProofRejected():
    return false
  task.msg.proof = @[]
  task.firstAdmittedTime = Opt.none(Moment)
  return true

proc applyOutcome(
    self: SendService, task: DeliveryTask, state: DeliveryState, res: StepResult
): bool =
  ## Applies the effects of an outcome.
  ## True when the RLN backend refreshes, so a new proof can pass.
  var proofRefreshed = false
  case res.outcome
  of StepOutcome.Propagated:
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
    if state == DeliveryState.Mix:
      task.propagatedAnonymously = true
  of StepOutcome.ProofRejected:
    if state in RoundStates:
      proofRefreshed = task.refreshRlnProof(self.waku)
  else:
    discard
  if state in RoundStates and res.outcome.isError():
    task.lastError = res.outcome
    task.lastErrorDesc = res.desc
  return proofRefreshed

proc canResend(self: SendService, task: DeliveryTask): bool =
  return
    self.plan.hasResend() and task.resendCount < MaxStoreMissResends and
    self.processors.resender.checkBeforeAttempt(task).isOk()

proc nextState(
    self: SendService,
    task: DeliveryTask,
    state: DeliveryState,
    outcome: StepOutcome,
    proofRefreshed: bool,
): DeliveryState =
  let next = self.plan.nextStep(state, outcome)
  case next
  of NextStep.Unset:
    error "The delivery plan has no next step for an outcome",
      requestId = task.requestId, state = state, outcome = outcome
    if task.firstPropagatedTime.isSome():
      DeliveryState.NotStored
    else:
      DeliveryState.NotPropagated
  of NextStep.StartRound:
    self.roundStart(task)
  of NextStep.AfterPropagation:
    self.afterPropagation(task)
  of NextStep.ResendOrWait:
    if self.canResend(task): DeliveryState.StoreResend else: DeliveryState.AwaitingStore
  of NextStep.GraceOrFail:
    if task.admissionAge() <= MixUnusableGrace:
      DeliveryState.NextRoundRetry
    else:
      DeliveryState.NotPropagated
  of NextStep.RefreshOrFail:
    if proofRefreshed: DeliveryState.NextRoundRetry else: DeliveryState.NotPropagated
  of NextStep.Relay, NextStep.Lightpush, NextStep.Mix, NextStep.NextRoundRetry,
      NextStep.AwaitingStore, NextStep.Stored, NextStep.NotStored,
      NextStep.NotPropagated:
    next.fixedState().get()

proc startStoreWait(self: SendService, task: DeliveryTask) =
  let now = Moment.now()
  task.storeAnswer =
    StoreAnswer.new(now, now + (self.maxValidationAge - task.storeValidationWaited))

proc endStoreWait(task: DeliveryTask) =
  ## Adds the time of the Store wait to the task, so a stop keeps it.
  if task.storeAnswer.isNil():
    return
  task.storeValidationWaited += Moment.now() - task.storeAnswer.openedAt
  task.storeAnswer = nil

proc moveTo(
    self: SendService,
    task: DeliveryTask,
    state: DeliveryState,
    res: StepResult,
    next: DeliveryState,
) =
  ## Moves the task to `next` and emits the events of the transition.
  task.state = next
  if state in RoundStates and next notin RoundStates:
    self.attempts.inc()
  if res.outcome == StepOutcome.Propagated and not task.propagatedEventEmitted:
    self.reportPropagated(task)
  if state == DeliveryState.Admission and res.outcome == StepOutcome.OverBudget:
    self.reportTaskQueued(task)
  if state == DeliveryState.Mix and
      next in {DeliveryState.Relay, DeliveryState.Lightpush}:
    self.reportMixFallback(task, res.outcome)
  if next in StoreWaitStates and task.storeAnswer.isNil():
    self.startStoreWait(task)
  if next.isFinal():
    task.finalOutcome = res.outcome
    task.endStoreWait()
    self.reportEnd(task)
    self.finish(task)

proc step*(
    self: SendService, task: DeliveryTask, state: DeliveryState
): Future[DeliveryState] {.async: (raises: [CancelledError]).} =
  let res = await self.runStep(task, state)
  let proofRefreshed = self.applyOutcome(task, state, res)
  let next = self.nextState(task, state, res.outcome, proofRefreshed)
  self.moveTo(task, state, res, next)
  return next

func resumeState(self: SendService, task: DeliveryTask): DeliveryState =
  ## A propagated task waits for its Store answer again.
  ## Any other task starts a round at once.
  if task.state in StoreWaitStates:
    DeliveryState.AwaitingStore
  else:
    self.roundStart(task)

proc retryNow(self: SendService) =
  ## Starts the next round of each task that waits for one.
  self.retryTick.fire()
  self.retryTick.clear()

proc retryLoop(self: SendService) {.async: (raises: [CancelledError]).} =
  ## Runs `retryNow` each `retryInterval`.
  while true:
    await sleepAsync(self.retryInterval)
    self.retryNow()

proc deliver(
    self: SendService, task: DeliveryTask
) {.async: (raises: [CancelledError]).} =
  ## Runs the steps of the task until a final state.
  ## A stop cancels it and keeps the task in the send queue, in its last state.
  try:
    var state = self.resumeState(task)
    task.state = state
    if state in StoreWaitStates:
      self.startStoreWait(task)
    while not state.isFinal():
      state = await self.step(task, state)
  finally:
    task.endStoreWait()

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
  ## attempt and emits no event, so the caller gets the result first. The network
  ## keeps one copy of a message, so a message that the send queue already has
  ## gets the request id of the queued task.
  assert(not task.isNil(), "task for enqueue must not be nil")
  let queued = self.deliveryTasks.getOrDefault(task.msgHash)
  if not queued.isNil():
    info "Message already in the send queue, request folded into the queued one",
      requestId = task.requestId,
      queuedRequestId = queued.requestId,
      msgHash = task.msgHash.to0xHex()
    return ok(queued.requestId)
  if self.isFull():
    error "Send queue full, request cannot be enqueued",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    return err("Send queue full, retry later")

  info "Task added to the send queue",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()
  self.deliveryTasks[task.msgHash] = task
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
  ## Cancels each `deliver`, `retryLoop` and the Store validation loop.
  ## The tasks stay in the send queue with no event until `startSendService`.
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
