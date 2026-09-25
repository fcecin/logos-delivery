## This module reinforces the publish operation with regular store-v3 requests.
##

import std/[sequtils, tables, typetraits]
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

template forEach*(varSeq, op: untyped) =
  for i in low(varSeq) .. high(varSeq):
    let it {.inject.} = varSeq[i]
    op

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
  ## Interval at which we check that messages have been properly received by a store node

const ArchiveTime = chronos.seconds(3)
  ## Estimation of the time we wait until we start confirming that a message has been properly
  ## received and archived by a store node

type SendService* = ref object of RootObj
  brokerCtx: BrokerContext
  taskCache: seq[DeliveryTask]
    ## Cache that contains the delivery task per message hash.
    ## This is needed to make sure the published messages are properly published

  serviceLoopHandle: Future[void] ## handle that allows to stop the async task
  sendProcessor: BaseSendProcessor
  rateLimitManager: RateLimitManager
    ## Charges first transmissions against the per-epoch budget; re-publishes
    ## are free.

  waku: Waku
  checkStoreForMessages: bool
  lastStoreCheckTime: Moment ## throttles store validation queries to ArchiveTime cadence
  maxDeliveryTime*: timer.Duration
    ## How long an admitted task may keep trying before it is failed.
  maxParkedAge*: timer.Duration
    ## How old a never-admitted (parked) task may get before it is failed.
  maxValidationAge*: timer.Duration
    ## How long after its first propagation a task may wait for store
    ## confirmation before it is failed.
  maxTaskCacheSize*: int
  inFlightSends: int
    ## Sends accepted but not yet in `taskCache`; counted against the cap so
    ## concurrent sends cannot overshoot it.

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
): Result[T, string] =
  let checkStoreForMessages = preferP2PReliability and waku.isStoreMounted()

  let sendService = SendService(
    brokerCtx: waku.brokerCtx,
    taskCache: newSeq[DeliveryTask](),
    serviceLoopHandle: nil,
    sendProcessor: sendProcessor,
    rateLimitManager: rateLimitManager,
    waku: waku,
    checkStoreForMessages: checkStoreForMessages,
    lastStoreCheckTime: Moment.now(),
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
    maxParkedAge: maxParkedAge,
    maxValidationAge: maxValidationAge,
    maxTaskCacheSize: maxTaskCacheSize,
  )

  return ok(sendService)

proc addTask(self: SendService, task: DeliveryTask) =
  self.taskCache.addUnique(task)

proc isFull*(self: SendService): bool =
  return self.taskCache.len + self.inFlightSends >= self.maxTaskCacheSize

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
  return
    self.storeConfirmationExpected(task) and
    task.state == DeliveryState.SuccessfullyPropagated and not task.propagatedAnonymously

proc checkMsgsInStore(self: SendService, tasksToValidate: seq[DeliveryTask]) {.async.} =
  if tasksToValidate.len() == 0:
    return

  if not isStorePeerAvailable(self):
    debug "Skipping store validation for ",
      messageCount = tasksToValidate.len(), error = "no store peer available"
    return

  var hashesToValidate = tasksToValidate.mapIt(it.msgHash)
  # TODO: confirm hash format for store query!!!

  let storeResp: StoreQueryResponse = (
    await self.waku.storeQueryToAny(
      StoreQueryRequest(includeData: false, messageHashes: hashesToValidate)
    )
  ).valueOr:
    debug "Failed to get store validation for messages",
      hashes = hashesToValidate.mapIt(shortLog(it)), error = $error
    return

  let storedItems = storeResp.messages.mapIt(it.messageHash)

  # Set success state for the tasks found in store that the policy admits: the
  # store peer chooses its answer, so a hash match alone must not confirm a task.
  # The retry below uses only the hashes that this node asked about.
  self.taskCache.applyItIf(
    self.awaitsStoreValidation(it) and storedItems.contains(it.msgHash)
  ):
    it.state = DeliveryState.SuccessfullyValidated

  # set retry state for messages not found in store
  hashesToValidate.keepItIf(not storedItems.contains(it))
  self.taskCache.applyItIf(hashesToValidate.contains(it.msgHash)):
    it.state = DeliveryState.NextRoundRetry

proc checkStoredMessages(self: SendService) {.async.} =
  if not self.checkStoreForMessages:
    return

  # Throttle store queries so they run at most every ArchiveTime (3s), regardless
  # of the 1s service loop cadence.
  if Moment.now() - self.lastStoreCheckTime < ArchiveTime:
    return

  let tasksToValidate = self.taskCache.filterIt(
    self.awaitsStoreValidation(it) and it.propagationAge() > ArchiveTime
  )

  if tasksToValidate.len() == 0:
    return

  self.lastStoreCheckTime = Moment.now()
  await self.checkMsgsInStore(tasksToValidate)

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
    # A processor that leaves a task for the next round can write why in
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
  self.taskCache.forEach(self.reportTaskResult(it))
  self.taskCache.keepItIf(
    it.state != DeliveryState.SuccessfullyValidated and
      it.state != DeliveryState.FailedToDeliver
  )

  # remove propagated messages when no store confirmation will follow
  self.taskCache.keepItIf(
    not (
      it.state == DeliveryState.SuccessfullyPropagated and
      not self.awaitsStoreValidation(it)
    )
  )

  # Fail propagated tasks that no store node confirmed within maxValidationAge.
  # Eviction keys on the state set here, so every failed task is reported.
  let expired = self.taskCache.filterIt(
    it.firstPropagatedTime.isSome() and it.state != DeliveryState.SuccessfullyValidated and
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

  self.taskCache.keepItIf(it.state != DeliveryState.FailedToDeliver)

proc reportTaskQueued(self: SendService, task: DeliveryTask) =
  ## Announces a task parked for epoch budget, once per task. Retry rounds
  ## re-enter the same branch, so the flag is what keeps the event one-shot.
  if task.queuedEventEmitted:
    return

  info "Message queued for rate-limit budget",
    requestId = task.requestId, msgHash = task.loggedHash()
  MessageQueuedEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  task.queuedEventEmitted = true

proc admitAndProve(self: SendService, task: DeliveryTask): Future[bool] {.async.} =
  ## Gates a task's first transmission: charges one epoch slot, then attaches
  ## an RLN proof — strictly in that order, so an over-budget message never
  ## draws a nonce. The slot is charged at most once per task lifetime
  ## (`firstAdmittedTime`); the proof attach is retried each round until it
  ## sticks, then short-circuits, so a task charged but not yet proven never
  ## ships bare. Returns false while the task must stay parked for a later round,
  ## or once it is dropped (`FailedToDeliver`).
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

  ## A no-op when RLN is not mounted, or when a prior round already attached a
  ## proof; otherwise draws the nonce and attaches.
  task.msg = (await self.waku.attachRlnProof(task.msg)).valueOr:
    debug "Failed to attach RLN proof, retrying next round",
      requestId = task.requestId, error = error
    return false

  return true

proc trySendMessages*(self: SendService) {.async.} =
  let tasksToSend = self.taskCache.filterIt(it.state == DeliveryState.NextRoundRetry)

  for task in tasksToSend:
    # Todo, check if it has any perf gain to run them concurrent...
    if not (await self.admitAndProve(task)):
      continue
    await self.sendProcessor.process(task)

proc serviceLoop(self: SendService) {.async.} =
  ## Continuously monitors that the sent messages have been received by a store node
  while true:
    await self.trySendMessages()
    await self.checkStoredMessages()
    self.evaluateAndCleanUp()
    ## TODO: add circuit breaker to avoid infinite looping in case of persistent failures
    ## Use OnlineStateChange observers to pause/resume the loop
    await sleepAsync(ServiceLoopInterval)

proc startSendService*(self: SendService) =
  self.serviceLoopHandle = self.serviceLoop()

proc stopSendService*(self: SendService) {.async.} =
  if not self.serviceLoopHandle.isNil():
    await self.serviceLoopHandle.cancelAndWait()

proc send*(self: SendService, task: DeliveryTask) {.async.} =
  assert(not task.isNil(), "task for send must not be nil")

  debug "SendService.send: processing delivery task",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()

  if self.isFull():
    error "Failed to send message",
      requestId = task.requestId, msgHash = task.loggedHash(), error = "Send queue full"
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), "Send queue full"
    )
    return

  inc self.inFlightSends
  defer:
    dec self.inFlightSends

  # Yield once, so no event reaches the caller before its request id: the
  # messaging API returns the id when `send` suspends, and chronos runs this
  # expired timer after the queued callbacks that carry the id back. Counted
  # before the yield, so the API's `isFull()` sees every send of a burst.
  await sleepAsync(ZeroDuration)

  self.waku.subscribe(task.msg.contentTopic).isOkOr:
    debug "SendService.send: failed to subscribe to content topic",
      contentTopic = task.msg.contentTopic, error = error

  if not (await self.admitAndProve(task)):
    if task.state == DeliveryState.FailedToDeliver:
      self.reportTaskResult(task)
      return
    debug "SendService.send: parking task for a later round",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.NextRoundRetry
    self.addTask(task)
    return

  await self.sendProcessor.process(task)
  reportTaskResult(self, task)
  if task.state != DeliveryState.FailedToDeliver:
    self.addTask(task)
