## This module reinforces the publish operation with regular store-v3 requests.
##

import std/[algorithm, sequtils, tables, typetraits]
import chronos, chronicles
import brokers/broker_context
import
  ./[send_processor, relay_processor, lightpush_processor, mix_processor, delivery_task],
  logos_delivery/waku/[waku_core, waku_store/common],
  logos_delivery/waku/waku,
  logos_delivery/waku/api/[store, subscriptions, publish],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager
import logos_delivery/api/events/messaging_client_events
import logos_delivery/api/conf/modes
import logos_delivery/messaging/messaging_metrics

logScope:
  topics = "send service"

# This useful util is missing from sequtils, this extends applyIt with predicate...
template applyItIf*(varSeq, pred, op: untyped) =
  for i in low(varSeq) .. high(varSeq):
    var it {.inject.} = varSeq[i]
    if pred:
      op
      varSeq[i] = it

const MaxTimeInCache* = chronos.minutes(1)
  ## Messages older than this time will get completely forgotten on publication and a
  ## feedback will be given when that happens

proc maxDeliveryTime*(anonymityLevel: AnonymityLevel): timer.Duration =
  ## `Preferred` gets two windows: one for mix, then one for the plain path.
  if anonymityLevel == AnonymityLevel.Preferred:
    MaxTimeInCache + MaxTimeInCache
  else:
    MaxTimeInCache

const DefaultMaxParkedAge* = chronos.minutes(30)
  ## Parked tasks never admitted within this age (from the message timestamp)
  ## are dropped with a `MessageErrorEvent`. Spans a few default RLN epochs.

const DefaultMaxTaskCacheSize* = 1000
  ## Hard cap on tasks tracked by the send service; further sends are rejected.

const ServiceLoopInterval* = chronos.seconds(1)
  ## The time from the end of an attempt to the next retry of its task. It is
  ## also the longest time between two cleanups of the task cache, and the pause
  ## between two Store validation rounds.

const ArchiveTime = chronos.seconds(3)
  ## Estimation of the time we wait until we start confirming that a message has been properly
  ## received and archived by a store node

const StoreValidationQueryTimeout = chronos.seconds(15)
  ## Bounds one Store validation query with its dials. It exceeds one
  ## `DefaultDialTimeout`, so a dead Store peer leaves time for another.

const MaxConcurrentRetries* = 4
  ## The maximum number of retries that run at the same time, in admission or
  ## in a send. A slow retry uses only its own slot, and the next due retry
  ## starts when a slot is free.
  ## The limit also stops a backlog of retries from going out all at once. First
  ## attempts have no limit.

type
  RunningAdmission = object
    task: DeliveryTask
    fut: Future[bool] ## The `admitAndProve` of the task.
    retry: bool ## False for the first attempt of the task.

  RunningSend = object
    task: DeliveryTask
    fut: Future[void] ## The `process` of the send processor chain.
    retry: bool ## Only a retry counts toward `MaxConcurrentRetries`.

type SendService* = ref object of RootObj
  brokerCtx: BrokerContext
  taskCache: seq[DeliveryTask]
    ## Cache that contains the delivery task per message hash.
    ## This is needed to make sure the published messages are properly published

  schedulerHandle: Future[void] ## The scheduler loop, nil while stopped.
  stopping: bool
    ## Set by `stopSendService` and cleared by `startSendService`. While it is
    ## set, `enqueue` refuses tasks and a round starts no admission and no send.
  wakeUp: AsyncEvent
    ## Fired by `enqueue`, by `startSendService`, and by the end of each
    ## admission and send. The scheduler runs a round after each wake-up.
  admission: Opt[RunningAdmission]
    ## The admission that runs, if any. Tasks pass admission one at a time, so
    ## the epoch budget and the RLN message ids go in the order of admission. A
    ## task that needs no admission does not wait for it.
  sends: seq[RunningSend] ## The sends that run. `stopSendService` cancels them.
  rounds: int ## The number of rounds that ran.
  sendProcessor: BaseSendProcessor
  rateLimitManager: RateLimitManager
    ## Charges first transmissions against the per-epoch budget; re-publishes
    ## are free.

  waku: Waku
  checkStoreForMessages: bool
  storeValidationHandle: Future[void]
    ## The Store validation loop. Nil when Store-based reliability is off.
  maxDeliveryTime*: timer.Duration
    ## How long an admitted task may keep trying before it is failed.
  maxParkedAge*: timer.Duration
    ## How old a never-admitted (parked) task may get before it is failed.
  maxValidationAge*: timer.Duration
    ## How long after its first propagation a task may wait for store
    ## confirmation before it is failed.
  archiveTime*: timer.Duration
    ## A task is asked about in Store only once its propagation and its last
    ## Store query are older than this, giving a Store node time to archive it.
    ## Also the pause of the Store validation loop while no Store peer is available.
  maxTaskCacheSize*: int
  serviceLoopInterval: timer.Duration

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
      MixSendProcessor.new(waku, brokerCtx, anonymityLevel, MaxTimeInCache)
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
    maxTaskCacheSize: int = DefaultMaxTaskCacheSize,
    maxValidationAge: timer.Duration = MaxTimeInCache,
    serviceLoopInterval: timer.Duration = ServiceLoopInterval,
): Result[T, string] =
  # The scheduler waits at most this time between two rounds. A wait of zero
  # never returns to the event loop.
  if serviceLoopInterval <= ZeroDuration:
    return err("serviceLoopInterval must be positive")

  let checkStoreForMessages = preferP2PReliability and waku.isStoreMounted()

  let sendService = SendService(
    brokerCtx: waku.brokerCtx,
    taskCache: newSeq[DeliveryTask](),
    schedulerHandle: nil,
    stopping: false,
    wakeUp: newAsyncEvent(),
    sendProcessor: sendProcessor,
    rateLimitManager: rateLimitManager,
    waku: waku,
    checkStoreForMessages: checkStoreForMessages,
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
    maxParkedAge: maxParkedAge,
    maxValidationAge: maxValidationAge,
    archiveTime: ArchiveTime,
    maxTaskCacheSize: maxTaskCacheSize,
    serviceLoopInterval: serviceLoopInterval,
  )

  return ok(sendService)

func isFull*(self: SendService): bool =
  return self.taskCache.len >= self.maxTaskCacheSize

func checkAccepting*(self: SendService): Result[void, string] =
  ## Refuses a new task while the service is stopped or the cache is full.
  if self.stopping:
    return err("Send service is stopped")
  if self.isFull():
    return err("Send queue full, retry later")
  return ok()

proc isStorePeerAvailable*(sendService: SendService): bool =
  return sendService.waku.hasStorePeer()

proc storeConfirmationExpected(self: SendService, task: DeliveryTask): bool =
  ## True when a plain send of this task would wait for a store confirmation:
  ## reliability is on and the message is not ephemeral. `awaitsStoreValidation`
  ## and the mixed completion in `reportTaskResult` both read it.
  return self.checkStoreForMessages and not task.isEphemeral()

proc awaitsStoreValidation*(self: SendService, task: DeliveryTask): bool =
  ## True while a propagated task still needs a store node to confirm it. A task
  ## that went out over mix never does: the store query would carry its hash in
  ## clear from this node's own address. Every store confirmation passes here.
  ## It is false for a running task. So the Store loop cannot confirm a task
  ## before the round that takes its send reports the propagation.
  return
    self.storeConfirmationExpected(task) and
    task.state == DeliveryState.SuccessfullyPropagated and not task.propagatedAnonymously and
    not task.running

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
  ## waited longest come first, and ties keep cache order.
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

  let storedItems = storeResp.messages.mapIt(it.messageHash)

  # The Store peer decides which hashes its answer lists, and it can list hashes
  # that this node did not ask about. Confirm only the tasks that still wait for
  # a Store confirmation, so that a message sent over mix is never confirmed.
  self.taskCache.applyItIf(
    self.awaitsStoreValidation(it) and storedItems.contains(it.msgHash)
  ):
    it.state = DeliveryState.SuccessfullyValidated

proc storeValidationLoop(self: SendService) {.async.} =
  ## Confirms propagated tasks against Store, one batch per round, apart from the
  ## scheduler, so that a slow Store peer does not delay sends.
  while true:
    var delay = self.serviceLoopInterval
    try:
      let batch = self.nextStoreValidationBatch(self.taskCache, Moment.now())
      if batch.len > 0:
        if self.isStorePeerAvailable():
          await self.checkMsgsInStore(batch)
        else:
          debug "Skipping store validation, no store peer available",
            messageCount = batch.len
          delay = self.archiveTime
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      error "Store validation round raised, the loop continues", error = exc.msg
    await sleepAsync(delay)

proc loggedHash(task: DeliveryTask): string =
  ## The hash for INFO and ERROR records, withheld once the task is anonymized.
  if task.anonymized:
    "withheld"
  else:
    task.msgHash.to0xHex()

proc reportTaskResult(self: SendService, task: DeliveryTask) =
  case task.state
  of DeliveryState.SuccessfullyPropagated:
    # TODO: in case of unable to strore check messages shall we report success instead?
    if not task.propagateEventEmitted:
      # INFO lines reach log collectors, where a hash would tie this node to an
      # anonymized message; `MixSendProcessor` still logs it at DEBUG.
      info "Message successfully propagated",
        requestId = task.requestId, msgHash = task.loggedHash()
      MessagePropagatedEvent.emit(
        self.brokerCtx, task.requestId, task.msgHash.to0xHex()
      )
      task.propagateEventEmitted = true

    if task.propagatedAnonymously and not task.sentEventEmitted and
        self.storeConfirmationExpected(task):
      # The exit's reply completes a mixed send when a plain send would wait for
      # a store confirmation, so both paths end with the same event.
      # `sentEventEmitted` keeps it to one; the INFO line omits the hash.
      info "Message successfully sent over mix", requestId = task.requestId
      MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
      task.sentEventEmitted = true
    return
  of DeliveryState.SuccessfullyValidated:
    # An anonymized task reaches this only through the clear republish of a
    # `Preferred` send whose mix reply was lost.
    info "Message successfully sent",
      requestId = task.requestId, msgHash = task.loggedHash()
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
    task.sentEventEmitted = true
    return
  of DeliveryState.FailedToDeliver:
    # The exit may have published the message even though its reply was lost.
    error "Failed to send message",
      requestId = task.requestId, msgHash = task.loggedHash(), error = task.errorDesc
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorDesc
    )
    return
  else:
    # rest of the states are intermediate and does not translate to event
    discard

  # Fail a task that passed admission and did not propagate in its window.
  # evaluateAndCleanUp fails propagated tasks that no store node confirms.
  if task.isDeliveryTimedOut(self.maxDeliveryTime):
    # A processor that leaves a task for a retry can write why in
    # `errorDesc`, as the mix processor does for a `Required` task it holds.
    # Report that reason if set.
    if task.errorDesc.len == 0:
      task.errorDesc = "Unable to send within retry time window"
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = task.errorDesc,
      age = task.admissionAge()
    task.state = DeliveryState.FailedToDeliver
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorDesc
    )
  elif task.isParkedExpired(self.maxParkedAge):
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = "Parked message too old",
      age = task.messageAge()
    task.state = DeliveryState.FailedToDeliver
    MessageErrorEvent.emit(
      self.brokerCtx,
      task.requestId,
      task.msgHash.to0xHex(),
      "Rate-limit budget not available within max parked age",
    )

proc evaluateAndCleanUp*(self: SendService) =
  ## Reports each task that does not run, then removes the finished tasks and
  ## fails the expired ones. A running task keeps its state and its place until
  ## the round that handles the end of its attempt. A task in `Entry` waits for its first
  ## admission, so no reaper fails it before that. The loops that emit go over a
  ## snapshot, because a listener can call `enqueue` inside `emit`.
  for task in self.taskCache.filterIt(
    not it.running and it.state != DeliveryState.Entry
  ):
    self.reportTaskResult(task)
  self.taskCache.keepItIf(
    it.running or (
      it.state != DeliveryState.SuccessfullyValidated and
      it.state != DeliveryState.FailedToDeliver
    )
  )

  # remove propagated messages when no store confirmation will follow
  self.taskCache.keepItIf(
    it.running or
      not (
        it.state == DeliveryState.SuccessfullyPropagated and
        not self.awaitsStoreValidation(it)
      )
  )

  # Fail propagated tasks that no store node confirmed within maxValidationAge.
  # Eviction keys on the state set here, so every failed task is reported.
  let expired = self.taskCache.filterIt(
    not it.running and it.firstPropagatedTime.isSome() and
      it.state != DeliveryState.SuccessfullyValidated and
      it.propagationAge() > self.maxValidationAge
  )
  for task in expired:
    debug "Message propagated but not validated by a store node within time window; stop trying.",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      propagationAge = task.propagationAge()
    recordStoreValidationTimeout()
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc =
      "Propagated but not confirmed by a store node within the store validation window"
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorDesc
    )

  self.taskCache.keepItIf(it.running or it.state != DeliveryState.FailedToDeliver)

proc reportTaskQueued(self: SendService, task: DeliveryTask) =
  ## Announces a task parked for epoch budget, once per task. Each retry enters
  ## the same branch again, so the flag keeps the event to one.
  if task.queuedEventEmitted:
    return

  info "Message queued for rate-limit budget",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessageQueuedEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  task.queuedEventEmitted = true

proc admitAndProve(self: SendService, task: DeliveryTask): Future[bool] {.async.} =
  ## Charges one slot of the epoch budget, then attaches an RLN proof, in that
  ## order, so a message over the budget never draws a nonce. A task is charged
  ## at most once (`firstAdmittedTime`). The proof attach runs again at each
  ## retry until it succeeds, and then it does nothing. So a task that is
  ## charged but has no proof never goes out without one. Returns false while
  ## the task must wait for a retry, or when the task is dropped
  ## (`FailedToDeliver`).
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
        task.state = DeliveryState.FailedToDeliver
        task.errorDesc = "Ephemeral message dropped: rate limit " & $quotaState
        return false

    (await self.rateLimitManager.admit(task.msg.payload)).isOkOr:
      debug "Over rate-limit budget, task waits for the epoch to roll",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      self.reportTaskQueued(task)
      return false
    task.firstAdmittedTime = Opt.some(Moment.now())

  ## This does nothing when RLN is not mounted, or when an earlier attempt
  ## attached a proof. Else it draws the nonce and attaches the proof. The
  ## timestamp of the message sets its epoch. So a backend that moved past that
  ## epoch, or spent its budget, fails the task, because no retry can prove it.
  ## Each other kind of error waits for a retry.
  task.msg = (await self.waku.attachRlnProof(task.msg)).valueOr:
    case error.kind
    of RlnErrorKind.Permanent, RlnErrorKind.BudgetExhausted:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = "Failed to attach RLN proof: " & $error
    of RlnErrorKind.NotReady, RlnErrorKind.Transient:
      debug "Failed to attach RLN proof, the task waits for a retry",
        requestId = task.requestId, error = $error
    return false

  return true

func runningRetries(self: SendService): int =
  ## The retries that send or pass admission now.
  let retryInAdmission = self.admission.isSome() and self.admission.get().retry
  return self.sends.countIt(it.retry) + int(retryInAdmission)

func needsAdmission(self: SendService, task: DeliveryTask): bool =
  ## True when the task needs a charge of the epoch budget or an RLN proof.
  ## `admitAndProve` does both. A task that needs neither draws no budget and no
  ## RLN message id, so it can send while another task passes admission.
  return task.firstAdmittedTime.isNone() or self.waku.needsRlnProof(task.msg)

proc wakeUpOnEnd(self: SendService, fut: FutureBase) =
  ## Wakes the scheduler when `fut` ends. Chronos runs the callback in a later
  ## turn of the event loop, never inside the call that started `fut`.
  let wakeUp = self.wakeUp
  fut.addCallback(
    proc(udata: pointer) {.gcsafe, raises: [].} =
      wakeUp.fire()
  )

proc handleSendEnd(self: SendService, send: RunningSend, now: Moment) =
  ## Clears `running` on the task of a finished or cancelled send. A task left
  ## for a retry is due again after `serviceLoopInterval`.
  let task = send.task
  if send.fut.failed():
    # The send path turns every remote error into a result, so a raise here is
    # a local fault.
    error "Send attempt raised, the task waits for its next retry",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = send.fut.error.msg
  # A raise or a cancel skips the tail of `process` that moves a hand-off or an
  # untried task to `NextRoundRetry`. No round selects `FallbackRetry`, and a
  # round selects an `Entry` task at once, so move both here.
  if task.state == DeliveryState.FallbackRetry or task.state == DeliveryState.Entry:
    task.state = DeliveryState.NextRoundRetry
  task.running = false
  if task.state == DeliveryState.NextRoundRetry:
    task.nextAttemptTime = Opt.some(now + self.serviceLoopInterval)

proc startSend(self: SendService, task: DeliveryTask, retry: bool) =
  let fut = self.sendProcessor.process(task)
  self.sends.add(RunningSend(task: task, fut: fut, retry: retry))
  self.wakeUpOnEnd(fut)

proc handleAdmissionEnd(self: SendService, admission: RunningAdmission, now: Moment) =
  ## Starts the send of an admitted task. Else it clears `running` on the task,
  ## and a task that did not fail waits for a retry.
  let task = admission.task
  if admission.fut.failed():
    # The reapers fail the task with an event if admission keeps raising.
    error "Admission raised, the task waits for its next retry",
      requestId = task.requestId,
      msgHash = task.loggedHash(),
      error = admission.fut.error.msg
  let admitted = admission.fut.completed() and admission.fut.read()
  if admitted and not self.stopping:
    self.startSend(task, admission.retry)
    return
  task.running = false
  if task.state != DeliveryState.FailedToDeliver:
    task.state = DeliveryState.NextRoundRetry
    task.nextAttemptTime = Opt.some(now + self.serviceLoopInterval)

func dueTime(task: DeliveryTask): Moment =
  ## When a retry is due. A task with no time is due first.
  return task.nextAttemptTime.get(Moment.low())

func isDueRetry(task: DeliveryTask, now: Moment): bool =
  return
    task.state == DeliveryState.NextRoundRetry and not task.running and
    task.dueTime() <= now

proc startOrder(self: SendService, now: Moment): seq[DeliveryTask] =
  ## The order in which a round tries to start tasks. The new tasks come first,
  ## in cache order, so a new task never waits for a retry slot. Then the due
  ## retries come, by due time. The sort is stable, so a tie keeps cache order,
  ## and each due retry starts once before a retry starts twice.
  var retries = self.taskCache.filterIt(it.isDueRetry(now))
  retries.sort(
    proc(a, b: DeliveryTask): int =
      cmp(a.dueTime(), b.dueTime())
  )
  return
    self.taskCache.filterIt(not it.running and it.state == DeliveryState.Entry) & retries

proc startTask(self: SendService, task: DeliveryTask) =
  ## Starts the admission of the task, or its send when it needs no admission.
  ## Each task that left `Entry` had an attempt, so this one is a retry.
  let retry = task.state != DeliveryState.Entry
  task.running = true
  task.nextAttemptTime = Opt.none(Moment)
  if not retry:
    self.waku.subscribe(task.msg.contentTopic).isOkOr:
      debug "SendService: failed to subscribe to content topic",
        contentTopic = task.msg.contentTopic, error = error
  if not self.needsAdmission(task):
    self.startSend(task, retry)
    return
  let fut = self.admitAndProve(task)
  self.admission = Opt.some(RunningAdmission(task: task, fut: fut, retry: retry))
  self.wakeUpOnEnd(fut)

proc takeEndedAdmission(self: SendService, now: Moment) =
  ## Takes the admission if it ended.
  if self.admission.isSome() and self.admission.get().fut.finished():
    let admission = self.admission.get()
    self.admission = Opt.none(RunningAdmission)
    self.handleAdmissionEnd(admission, now)

proc runRound*(self: SendService, now = Moment.now()) =
  ## One round of the scheduler, with no suspension. The scheduler loop runs it
  ## after each wake-up. Tests also call it.
  inc self.rounds
  # A wake-up from now on gets a new round.
  self.wakeUp.clear()

  let ended = self.sends.filterIt(it.fut.finished())
  self.sends.keepItIf(not it.fut.finished())
  for send in ended:
    self.handleSendEnd(send, now)

  # `handleAdmissionEnd` and the walk over the start order read `stopping`. Chronos can deliver the
  # cancel of a stop one turn late, so a round can run during a stop.
  self.takeEndedAdmission(now)

  # Report the sends that ended, in the round that takes them. So each event
  # goes out when its send ends.
  self.evaluateAndCleanUp()

  # Go once over the start order. While an admission runs, only a task that
  # needs no admission can start. A task that a listener adds during this walk
  # starts in the next round, which its `enqueue` wakes.
  for task in self.startOrder(now):
    if self.stopping:
      break
    if self.admission.isSome() and self.needsAdmission(task):
      continue
    # No retry slot becomes free inside a round, so no later retry can start.
    if task.state != DeliveryState.Entry and
        self.runningRetries() >= MaxConcurrentRetries:
      break
    self.startTask(task)
    # An admission with no suspension ends inside `startTask`. Take it now, so
    # that the next task can pass admission in this round. A task that failed
    # its admission is reported in the next round, which the end of the
    # admission wakes.
    self.takeEndedAdmission(now)

func roundCount*(self: SendService): int =
  ## The number of rounds that ran. Tests read it to show that the scheduler
  ## returns to the event loop between events.
  return self.rounds

func runningFutures(self: SendService): seq[FutureBase] =
  ## The futures of the admission and of each send that runs.
  var running: seq[FutureBase]
  if self.admission.isSome():
    running.add(self.admission.get().fut)
  for send in self.sends:
    running.add(send.fut)
  return running

proc runUntilIdle*(self: SendService) {.async.} =
  ## Runs rounds until a round leaves no admission and no send that runs, and
  ## no wake-up comes after it. It waits for them between the rounds. A test
  ## drives a service that never started with it. A send that never ends keeps
  ## it waiting.
  while true:
    self.runRound()
    let running = self.runningFutures()
    await allFutures(running)
    # Return to the event loop once in each round, also when each future had
    # ended already. So the end of an admission that a round took can wake the
    # next round, and a time limit around this proc can fire.
    await sleepAsync(ZeroDuration)
    if running.len == 0 and not self.wakeUp.isSet():
      return

func waitLimit(self: SendService, now: Moment): timer.Duration =
  ## How long the scheduler waits when no event wakes it. It wakes for the next
  ## cleanup, and for the next retry that can start. With no task, only an
  ## event wakes it.
  if self.taskCache.len == 0:
    return InfiniteDuration
  var delay = self.serviceLoopInterval
  # A due retry that waits for a slot or for the admission needs no timer. The
  # end of a send or of the admission wakes the scheduler.
  if self.runningRetries() < MaxConcurrentRetries:
    for task in self.taskCache:
      if task.state == DeliveryState.NextRoundRetry and not task.running and
          task.nextAttemptTime.isSome():
        let left = task.nextAttemptTime.get() - now
        if left > ZeroDuration and left < delay:
          delay = left
  return delay

proc schedulerLoop(self: SendService) {.async.} =
  ## Waits for a wake-up or a time limit, then runs a round.
  var roundAt = Moment.now()
  while true:
    # Count the wait from the start of the last round, so that a retry that
    # comes due during that round gets its timer.
    discard await self.wakeUp.wait().withTimeout(self.waitLimit(roundAt))
    roundAt = Moment.now()
    # A raise must not end the loop: nothing watches it until stop, and queued
    # tasks would never get a terminal event.
    try:
      self.runRound(roundAt)
    except CatchableError as exc:
      error "Send service round raised, the loop continues", error = exc.msg
    # A listener can call `enqueue` inside a round, and so set the wake-up. Then
    # the next `wait` does not suspend. Return to the event loop first, so that
    # timers and other callbacks run between two rounds.
    if self.wakeUp.isSet():
      await sleepAsync(ZeroDuration)
    ## TODO: add circuit breaker to avoid infinite looping in case of persistent failures
    ## Use OnlineStateChange observers to pause/resume the loop

proc enqueue*(self: SendService, task: DeliveryTask): Result[void, string] =
  ## Puts `task` in the cache and wakes the scheduler. It makes no send attempt
  ## and emits no event, so the caller gets the result before any event of the
  ## task. The scheduler starts the task in a later round. When a listener calls
  ## `enqueue` inside a round, that round can start the task after the listener
  ## returns.
  assert(not task.isNil(), "task for enqueue must not be nil")
  ?self.checkAccepting()
  # The network keeps one copy of a message, and the cache keys tasks by hash.
  if self.taskCache.anyIt(it.msgHash == task.msgHash):
    return err("Send queue already has a message with the same hash")

  debug "SendService.enqueue: task added to the send queue",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()
  self.taskCache.add(task)
  self.wakeUp.fire()
  return ok()

proc startSendService*(self: SendService) =
  self.stopping = false
  # Clear the event, so that the loop suspends at its first wait. The fire then
  # runs the first round in a later turn of the event loop, after the caller
  # continues. That round starts the tasks that `enqueue` added before the start.
  self.wakeUp.clear()
  self.schedulerHandle = self.schedulerLoop()
  self.wakeUp.fire()
  if self.checkStoreForMessages:
    self.storeValidationHandle = self.storeValidationLoop()

proc stopSendService*(self: SendService) {.async.} =
  self.stopping = true
  var loops: seq[Future[void]]
  for handle in [self.schedulerHandle, self.storeValidationHandle]:
    if not handle.isNil():
      loops.add(handle)
  await cancelAndWait(loops)
  self.schedulerHandle = nil
  self.storeValidationHandle = nil

  # The cancel of the scheduler leaves its admission and its sends running, so
  # cancel them here. Take them first, so that no round takes them again.
  let running = self.runningFutures()
  let admission = self.admission
  self.admission = Opt.none(RunningAdmission)
  var sends: seq[RunningSend]
  swap(sends, self.sends)
  await cancelAndWait(running)

  # A cancelled send gets no event. A send that ended before the stop keeps its
  # state, and the first round after a restart reports it. Each task taken here
  # is due at once after a restart.
  let now = Moment.now()
  if admission.isSome():
    self.handleAdmissionEnd(admission.get(), now)
    admission.get().task.nextAttemptTime = Opt.none(Moment)
  for send in sends:
    self.handleSendEnd(send, now)
    send.task.nextAttemptTime = Opt.none(Moment)
