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

const ServiceLoopInterval* = chronos.seconds(1)

const ArchiveTime = chronos.seconds(3)
  ## Estimation of the time we wait until we start confirming that a message has been properly
  ## received and archived by a store node

const StoreValidationBatchSize = int(MaxPageSize)
  ## Keep each validation batch within one Store response page.

const StoreValidationInterval = chronos.seconds(1)
  ## Minimum delay between completing one query and starting the next.

const StoreQueryDeadline = chronos.seconds(10)
  ## Timeout for the entire Store query, including dials and peer retries.

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
  storeValidationHandle: Future[void]
    ## Runs only when Store-based reliability is enabled.
  maxDeliveryTime*: timer.Duration
    ## How long an admitted task may keep trying before it is failed.

proc setupSendProcessorChain(
    waku: Waku, brokerCtx: BrokerContext, anonymityLevel: AnonymityLevel
): Result[BaseSendProcessor, string] =
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
    sendProcessor: BaseSendProcessor = nil,
    anonymityLevel: AnonymityLevel = AnonymityLevel.None,
): Result[T, string] =
  ## `sendProcessor` overrides the relay/lightpush chain built from `waku`,
  ## letting a caller drive the scheduler against a scripted delivery outcome.
  if not waku.hasRelay() and not waku.hasLightpush():
    return err(
      "Could not create SendService. wakuRelay or wakuLightpushClient should be set"
    )

  let checkStoreForMessages = preferP2PReliability and waku.isStoreMounted()

  let sendProcessorChain =
    if sendProcessor.isNil():
      setupSendProcessorChain(waku, waku.brokerCtx, anonymityLevel).valueOr:
        return err("failed to setup SendProcessorChain: " & $error)
    else:
      sendProcessor

  let sendService = SendService(
    brokerCtx: waku.brokerCtx,
    taskCache: newSeq[DeliveryTask](),
    serviceLoopHandle: nil,
    sendProcessor: sendProcessorChain,
    rateLimitManager: rateLimitManager,
    waku: waku,
    checkStoreForMessages: checkStoreForMessages,
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
  )

  return ok(sendService)

proc addTask(self: SendService, task: DeliveryTask) =
  self.taskCache.addUnique(task)

proc isStorePeerAvailable*(sendService: SendService): bool =
  return sendService.waku.hasStorePeer()

proc storeValidationKey(task: DeliveryTask): Moment =
  ## Order by last query time, or first propagation for an unqueried task.
  if task.lastStoreQueryTime.isSome():
    task.lastStoreQueryTime.get()
  else:
    task.firstPropagatedTime.get()

proc nextStoreValidationBatch*(
    tasks: seq[DeliveryTask], now: Moment, batchSize = StoreValidationBatchSize
): seq[DeliveryTask] =
  ## Select up to `batchSize` propagated, non-ephemeral tasks.
  ## Require propagation and the last query to be older than ArchiveTime.
  ## Select the oldest storeValidationKey values first; keep cache order for ties.
  var eligible: seq[DeliveryTask]
  for task in tasks:
    if task.state != DeliveryState.SuccessfullyPropagated or task.isEphemeral():
      continue
    if task.firstPropagatedTime.isNone() or
        now - task.firstPropagatedTime.get() <= ArchiveTime:
      continue
    if task.lastStoreQueryTime.isSome() and
        now - task.lastStoreQueryTime.get() <= ArchiveTime:
      continue
    eligible.add(task)
  eligible.sort(
    proc(a, b: DeliveryTask): int =
      let ka = storeValidationKey(a)
      let kb = storeValidationKey(b)
      if ka < kb:
        return -1
      if kb < ka:
        return 1
      return 0
  )
  if eligible.len > batchSize:
    eligible.setLen(batchSize)
  return eligible

proc checkMsgsInStore(self: SendService, batch: seq[DeliveryTask]) {.async.} =
  ## Query one batch and confirm matching tasks still pending in the cache.
  if batch.len() == 0 or not isStorePeerAvailable(self):
    return

  let now = Moment.now()
  for task in batch:
    task.lastStoreQueryTime = Opt.some(now)

  let query = self.waku.storeQueryToAny(
    StoreQueryRequest(
      includeData: false,
      messageHashes: batch.mapIt(it.msgHash),
      paginationLimit: Opt.some(uint64(batch.len)),
    )
  )
  if not await query.withTimeout(StoreQueryDeadline):
    debug "Store validation query timed out", hashCount = batch.len
    return
  if query.failed():
    debug "Store validation query raised",
      hashCount = batch.len, error = query.error.msg
    return

  let storeResp: StoreQueryResponse = query.read().valueOr:
    debug "Failed to get store validation for messages",
      hashCount = batch.len, error = $error
    return

  let storedItems = storeResp.messages.mapIt(it.messageHash)

  # Leave unconfirmed messages pending for another Store query.
  # Resending them can trigger duplicate rejection and lightpush rate limits.
  self.taskCache.applyItIf(
    it.state == DeliveryState.SuccessfullyPropagated and storedItems.contains(
      it.msgHash
    )
  ):
    it.state = DeliveryState.SuccessfullyValidated

proc storeValidationLoop(self: SendService) {.async.} =
  ## Validate messages independently of send retries and task cleanup.
  while true:
    try:
      let batch = nextStoreValidationBatch(self.taskCache, Moment.now())
      if batch.len > 0 and not isStorePeerAvailable(self):
        debug "Skipping store validation, no store peer available",
          messageCount = batch.len
        await sleepAsync(ArchiveTime)
        continue
      await self.checkMsgsInStore(batch)
    except CancelledError as exc:
      raise exc
    except CatchableError as exc:
      debug "Store validation round failed", error = exc.msg
    await sleepAsync(StoreValidationInterval)

proc reportTaskResult(self: SendService, task: DeliveryTask) =
  case task.state
  of DeliveryState.SuccessfullyPropagated:
    # TODO: in case of unable to strore check messages shall we report success instead?
    if not task.propagateEventEmitted:
      info "Message successfully propagated",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      MessagePropagatedEvent.emit(
        self.brokerCtx, task.requestId, task.msgHash.to0xHex()
      )
      task.propagateEventEmitted = true
    return
  of DeliveryState.SuccessfullyValidated:
    info "Message successfully sent",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
    return
  of DeliveryState.FailedToDeliver:
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      error = task.errorDesc
    MessageErrorEvent.emit(
      self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorDesc
    )
    return
  else:
    # rest of the states are intermediate and does not translate to event
    discard

  # Fail a task that passed admission and did not propagate in its window.
  # Propagated-but-unvalidated tasks are dropped in evaluateAndCleanUp instead.
  if task.isDeliveryTimedOut(self.maxDeliveryTime):
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      error = "Message too old",
      age = task.admissionAge()
    task.state = DeliveryState.FailedToDeliver
    MessageErrorEvent.emit(
      self.brokerCtx,
      task.requestId,
      task.msgHash.to0xHex(),
      "Unable to send within retry time window",
    )

proc evaluateAndCleanUp(self: SendService) =
  self.taskCache.forEach(self.reportTaskResult(it))
  self.taskCache.keepItIf(
    it.state != DeliveryState.SuccessfullyValidated and
      it.state != DeliveryState.FailedToDeliver
  )

  # remove propagated messages when no store confirmation will follow
  self.taskCache.keepItIf(
    not (
      it.state == DeliveryState.SuccessfullyPropagated and
      (it.isEphemeral() or not self.checkStoreForMessages)
    )
  )

  # Store validation timed out: the message was propagated but never confirmed in a
  # store node within MaxTimeInCache (measured from first propagation). This path emits
  # no app event, so the metric counter below is its only durable signal; drop and count.
  for task in self.taskCache:
    if task.firstPropagatedTime.isSome() and
        task.state != DeliveryState.SuccessfullyValidated and
        task.propagationAge() > MaxTimeInCache:
      debug "Message propagated but not validated by a store node within time window; stop trying.",
        requestId = task.requestId,
        msgHash = task.msgHash.to0xHex(),
        propagationAge = task.propagationAge()
      recordStoreValidationTimeout()

  self.taskCache.keepItIf(
    not (
      it.firstPropagatedTime.isSome() and it.state != DeliveryState.SuccessfullyValidated and
      it.propagationAge() > MaxTimeInCache
    )
  )

proc reportTaskQueued(self: SendService, task: DeliveryTask) =
  ## Announces a task parked for epoch budget, once per task. Retry rounds
  ## re-enter the same branch, so the flag is what keeps the event one-shot.
  if task.queuedEventEmitted:
    return

  info "Message queued for rate-limit budget",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()
  MessageQueuedEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
  task.queuedEventEmitted = true

proc admitAndProve(self: SendService, task: DeliveryTask): Future[bool] {.async.} =
  ## Gates a task's first transmission: charges one epoch slot, then attaches
  ## an RLN proof — strictly in that order, so an over-budget message never
  ## draws a nonce. The slot is charged at most once per task lifetime
  ## (`firstAdmittedTime`); the proof attach is retried each round until it
  ## sticks, then short-circuits, so a task charged but not yet proven never
  ## ships bare. Returns false while the task must stay parked for a later round.
  if task.firstAdmittedTime.isNone():
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
  ## Retry sends, report results and remove completed or expired tasks.
  while true:
    await self.trySendMessages()
    self.evaluateAndCleanUp()
    ## TODO: add circuit breaker to avoid infinite looping in case of persistent failures
    ## Use OnlineStateChange observers to pause/resume the loop
    await sleepAsync(ServiceLoopInterval)

proc startSendService*(self: SendService) =
  self.serviceLoopHandle = self.serviceLoop()
  if self.checkStoreForMessages:
    self.storeValidationHandle = self.storeValidationLoop()

proc stopSendService*(self: SendService) {.async.} =
  ## Cancel and await both loops, including any pending Store query.
  var loops: seq[Future[void]]
  for handle in [self.serviceLoopHandle, self.storeValidationHandle]:
    if not handle.isNil():
      loops.add(handle)
  await cancelAndWait(loops)
  self.serviceLoopHandle = nil
  self.storeValidationHandle = nil

proc send*(self: SendService, task: DeliveryTask) {.async.} =
  assert(not task.isNil(), "task for send must not be nil")

  debug "SendService.send: processing delivery task",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()

  self.waku.subscribe(task.msg.contentTopic).isOkOr:
    debug "SendService.send: failed to subscribe to content topic",
      contentTopic = task.msg.contentTopic, error = error

  if not (await self.admitAndProve(task)):
    debug "SendService.send: parking task for a later round",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.NextRoundRetry
    self.addTask(task)
    return

  await self.sendProcessor.process(task)
  reportTaskResult(self, task)
  if task.state != DeliveryState.FailedToDeliver:
    self.addTask(task)
