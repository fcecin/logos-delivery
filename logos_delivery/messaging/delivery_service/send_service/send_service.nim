## This module reinforces the publish operation with regular store-v3 requests.
##

import std/[sequtils, tables, typetraits]
import chronos, chronicles, metrics
import brokers/broker_context
import
  ./[send_processor, relay_processor, lightpush_processor, mix_processor, delivery_task],
  logos_delivery/waku/[waku_core, waku_store/common],
  logos_delivery/waku/waku,
  logos_delivery/waku/api/[store, subscriptions, publish],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager
import logos_delivery/api/events/messaging_client_events
import logos_delivery/api/conf/modes

logScope:
  topics = "send service"

declarePublicCounter logos_delivery_send_store_validation_timeout_total,
  "messages propagated but dropped without store-node validation within the retry window"

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

proc deliveryTimeFor*(anonymityLevel: AnonymityLevel): timer.Duration =
  ## `Preferred` gets two windows: one `MaxTimeInCache` for mix, then one for
  ## the plain send path.
  if anonymityLevel == AnonymityLevel.Preferred:
    MaxTimeInCache + MaxTimeInCache
  else:
    MaxTimeInCache

const ServiceLoopInterval* = chronos.seconds(1)
  ## Interval at which we check that messages have been properly received by a store node

const MaxConcurrentRetries* = 4
  ## The number of retry attempts in flight at the same time. A mix attempt
  ## can wait `MixReplyTimeout` for a reply. The service loop starts a retry
  ## as an attempt of its own and does not wait for it, so an attempt that
  ## waits delays no other task and no deadline; this limit bounds the
  ## attempts in flight. First attempts are not counted: each send starts
  ## one.

const ArchiveTime = chronos.seconds(3)
  ## Estimation of the time we wait until we start confirming that a message has been properly
  ## received and archived by a store node

type SendService* = ref object of RootObj
  brokerCtx: BrokerContext
  taskCache: seq[DeliveryTask]
    ## Cache that contains the delivery task per message hash.
    ## This is needed to make sure the published messages are properly published

  serviceLoopHandle: Future[void] ## handle that allows to stop the async task
  attempts: seq[tuple[task: DeliveryTask, attempt: Future[void], retry: bool]]
    ## The attempts in flight: the first attempt of each task, started by
    ## `trackedSend`, and the retries, started by the service loop. The
    ## service owns them: a stop cancels each one. A completed attempt leaves
    ## the list at the next sweep.
  anonymityLevel*: AnonymityLevel
    ## The level the chain was built for. Read by `subscribesOnSend`.
  stopped: bool
    ## Set by `stopSendService`. `trackedSend` does not start an attempt on a
    ## stopped service.
  sendProcessor*: BaseSendProcessor
    ## The first processor of the chain. Exported so that a test can get the
    ## mix processor.
  rateLimitManager: RateLimitManager
    ## Charges first transmissions against the per-epoch budget; re-publishes
    ## are free.

  waku: Waku
  checkStoreForMessages: bool
  lastStoreCheckTime: Moment ## throttles store validation queries to ArchiveTime cadence
  maxDeliveryTime*: timer.Duration
    ## Time an admitted task can try before the service fails it.

proc setupSendProcessorChain(
    waku: Waku, brokerCtx: BrokerContext, anonymityLevel: AnonymityLevel
): Result[BaseSendProcessor, string] =
  let isRelayAvail = waku.hasRelay()
  let isLightPushAvail = waku.hasLightpush()

  if anonymityLevel != AnonymityLevel.None and not waku.mixMounted():
    # A configuration made by hand can carry a level without the kernel side
    # of the setting. Without this check, `Required` fails each message at
    # the deadline and `Preferred` sends in clear text after the window.
    return err(
      "anonymityLevel=" & $anonymityLevel &
        " needs the mix protocol, which is not mounted"
    )
  if anonymityLevel != AnonymityLevel.None and not isLightPushAvail:
    return err("Mix sending needs a lightpush client, which is not mounted")

  if anonymityLevel != AnonymityLevel.Required and not isRelayAvail and
      not isLightPushAvail:
    return err("No valid send processor found for the delivery task")

  var processors = newSeq[BaseSendProcessor]()

  if anonymityLevel != AnonymityLevel.None:
    let mixProcessor: BaseSendProcessor =
      MixSendProcessor.new(waku, brokerCtx, anonymityLevel, MaxTimeInCache)
    processors.add(mixProcessor)

    if anonymityLevel == AnonymityLevel.Required:
      # `Required` gets the mix processor only. The plain chain is not built,
      # so a `Required` message cannot go to the network without mix.
      return ok(mixProcessor)

  if isRelayAvail:
    let publishProc = waku.relayPushHandler()
    processors.add(
      RelaySendProcessor.new(isLightPushAvail, publishProc, waku, brokerCtx)
    )
  if isLightPushAvail:
    processors.add(LightpushSendProcessor.new(waku, brokerCtx))

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
    lastStoreCheckTime: Moment.now(),
    maxDeliveryTime: deliveryTimeFor(anonymityLevel),
    anonymityLevel: anonymityLevel,
  )

  return ok(sendService)

proc subscribesOnSend*(self: SendService): bool =
  ## An Edge node with a level above `None` does not subscribe inside a send.
  ## A filter subscribe request goes in clear text from the node's own
  ## address, and a request one second before the first message on the topic
  ## connects the node to the message. A Core node keeps the subscription: a
  ## relay publish needs the mesh, and a gossipsub subscription names the
  ## shard, not the topic.
  return self.anonymityLevel == AnonymityLevel.None or self.waku.hasRelay()

proc addTask(self: SendService, task: DeliveryTask) =
  self.taskCache.addUnique(task)

proc isStorePeerAvailable*(sendService: SendService): bool =
  return sendService.waku.hasStorePeer()

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

  # Set success state for messages found in store
  self.taskCache.applyItIf(storedItems.contains(it.msgHash)):
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
    it.state == DeliveryState.SuccessfullyPropagated and
      it.propagationAge() > ArchiveTime and it.needsStoreValidation()
  )

  if tasksToValidate.len() == 0:
    return

  self.lastStoreCheckTime = Moment.now()
  await self.checkMsgsInStore(tasksToValidate)

proc reportTaskResult(self: SendService, task: DeliveryTask) =
  case task.state
  of DeliveryState.SuccessfullyPropagated:
    # TODO: in case of unable to strore check messages shall we report success instead?
    if not task.propagateEventEmitted:
      info "Message successfully propagated",
        requestId = task.requestId,
        msgHash = task.msgHash.to0xHex(),
        path = task.deliveryPath,
        exit = task.mixExit
      MessagePropagatedEvent.emit(
        self.brokerCtx, task.requestId, task.msgHash.to0xHex()
      )
      task.propagateEventEmitted = true
      if task.propagatedViaMix:
        # A mixed message gets no store confirmation, so the propagation is
        # its final result. Consumers that wait for `MessageSent` (the
        # channels layer) get it at this time. A plain message keeps the
        # contract of the plain path: `MessageSent` comes from the store
        # validation, and a node without it emits `MessagePropagated` only.
        info "Message sent without store confirmation",
          requestId = task.requestId,
          msgHash = task.msgHash.to0xHex(),
          path = task.deliveryPath
        MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
    return
  of DeliveryState.SuccessfullyValidated:
    info "Message successfully sent",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      path = task.deliveryPath
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

  # Fail a task that was admitted but did not propagate in its delivery window.
  # Propagated-but-unvalidated tasks are dropped in evaluateAndCleanUp instead.
  if task.isDeliveryTimedOut(self.maxDeliveryTime):
    error "Failed to send message",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      error = "Message too old",
      age = task.deadlineAge(),
      mixRequestLeft = task.mixRequestLeft,
      lastAttemptError = task.errorDesc
    task.state = DeliveryState.FailedToDeliver
    # A task whose mix request left the node, or may have, is reported as
    # unconfirmed and not as unsent: the message may be on the network. The
    # reason of the last attempt follows, so the application can tell a
    # lost reply from a refusal.
    let summary =
      if task.mixRequestLeft:
        "delivery unconfirmed: the message may have left this node, but no confirmation arrived before the deadline"
      else:
        "Unable to send within retry time window"
    MessageErrorEvent.emit(
      self.brokerCtx,
      task.requestId,
      task.msgHash.to0xHex(),
      if task.errorDesc.len > 0:
        summary & ": " & task.errorDesc
      else:
        summary,
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
      (not it.needsStoreValidation() or not self.checkStoreForMessages)
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
      logos_delivery_send_store_validation_timeout_total.inc()

  self.taskCache.keepItIf(
    not (
      it.firstPropagatedTime.isSome() and it.state != DeliveryState.SuccessfullyValidated and
      it.propagationAge() > MaxTimeInCache
    )
  )

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
      return false
    task.firstAdmittedTime = Opt.some(Moment.now())
    if task.deadlineStart.isNone():
      task.deadlineStart = task.firstAdmittedTime

  ## A no-op when RLN is not mounted, or when a prior round already attached a
  ## proof; otherwise draws the nonce and attaches.
  task.msg = (await self.waku.attachRlnProof(task.msg)).valueOr:
    debug "Failed to attach RLN proof, retrying next round",
      requestId = task.requestId, error = error
    return false

  return true

proc track(self: SendService, task: DeliveryTask, attempt: Future[void], retry: bool) =
  ## Keeps an attempt so that a stop can cancel it, and logs an attempt that
  ## raises: the loop never waits for an attempt, so an error would vanish.
  attempt.addCallback(
    proc(arg: pointer) {.gcsafe, raises: [].} =
      if attempt.failed():
        error "SendService attempt failed",
          requestId = task.requestId, retry = retry, error = attempt.error.msg
  )
  self.attempts.add((task: task, attempt: attempt, retry: retry))

proc sweepAttempts(self: SendService) =
  self.attempts.keepItIf(not it.attempt.finished())

proc hasAttemptInFlight(self: SendService, task: DeliveryTask): bool =
  self.attempts.anyIt(it.task == task and not it.attempt.finished())

proc retryTask(self: SendService, task: DeliveryTask) {.async.} =
  ## One retry attempt, reported at once. A task that the report finalizes as
  ## failed leaves the cache here: `evaluateAndCleanUp` reports each cached
  ## task again, and a failed task would get its error event twice.
  if task.state != DeliveryState.NextRoundRetry:
    # The state changed between the pass that started this attempt and now
    # (a store validation, a stop). No attempt for it.
    return
  if not (await self.admitAndProve(task)):
    return
  await self.sendProcessor.process(task)
  self.reportTaskResult(task)
  if task.state == DeliveryState.FailedToDeliver:
    self.taskCache.keepItIf(it.requestId != task.requestId)

proc trySendMessages*(self: SendService) {.async.} =
  ## Starts a retry attempt for each task that waits for one, up to
  ## `MaxConcurrentRetries` retries in flight. A task with an attempt in
  ## flight gets no second one: a mix attempt waits for its reply, and the
  ## pass comes each second. The attempts are owned by the service and not
  ## awaited here, so a stalled attempt holds one slot and nothing else.
  self.sweepAttempts()
  var retriesInFlight = self.attempts.countIt(it.retry)
  for task in self.taskCache:
    if task.state != DeliveryState.NextRoundRetry or self.hasAttemptInFlight(task):
      continue
    if retriesInFlight >= MaxConcurrentRetries:
      break
    self.track(task, self.retryTask(task), retry = true)
    retriesInFlight.inc()

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
  ## A stop and a start is a supported cycle of the messaging client, so the
  ## start clears the stop.
  self.stopped = false
  self.serviceLoopHandle = self.serviceLoop()

proc reportStopped(self: SendService, task: DeliveryTask) =
  ## Gives a task that did not complete a final event, so that a consumer
  ## that waits for the result of the send does not wait for a service that
  ## stopped. A task that propagated has its event and gets no other.
  if task.state == DeliveryState.SuccessfullyValidated or
      task.state == DeliveryState.FailedToDeliver or task.propagateEventEmitted:
    return
  task.state = DeliveryState.FailedToDeliver
  task.errorDesc = "send service stopped"
  MessageErrorEvent.emit(
    self.brokerCtx, task.requestId, task.msgHash.to0xHex(), task.errorDesc
  )

proc stopSendService*(self: SendService) {.async.} =
  ## Cancels the loop and each attempt in flight, first attempts and retries
  ## alike, then gives each task that did not complete its final event. A
  ## task in its first attempt is not in the cache: its event comes from the
  ## attempt list.
  self.stopped = true
  if not self.serviceLoopHandle.isNil():
    await self.serviceLoopHandle.cancelAndWait()
  for entry in self.attempts:
    if not entry.attempt.finished():
      await entry.attempt.cancelAndWait()
    self.reportStopped(entry.task)
  self.attempts.setLen(0)
  for task in self.taskCache:
    self.reportStopped(task)
  self.taskCache.setLen(0)

proc inFlightSendCount*(self: SendService): int =
  ## Number of attempts, first attempts and retries, that did not complete.
  return self.attempts.countIt(not it.attempt.finished())

proc isStopped*(self: SendService): bool =
  return self.stopped

proc send*(self: SendService, task: DeliveryTask) {.async.} =
  ## The first attempt of `task`. It starts after the send API returned the
  ## request id to the caller: a processor that fails without a network wait
  ## reports the result in this attempt, and the caller needs the request id
  ## before the event. The wait of zero length gives the event loop one turn.
  assert(not task.isNil(), "task for send must not be nil")
  await sleepAsync(ZeroDuration)

  debug "SendService.send: processing delivery task",
    requestId = task.requestId, msgHash = task.msgHash.to0xHex()

  if self.subscribesOnSend():
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

proc trackedSend*(self: SendService, task: DeliveryTask) =
  ## Starts the first attempt of `task` and keeps its future, so that
  ## `stopSendService` can cancel the attempt. A completed attempt leaves the
  ## list at the next call. A stopped service does not start an attempt. A
  ## service that did not start yet puts the task in the cache: the first
  ## round of the loop sends it, and no attempt runs on a node that did not
  ## start.
  if self.stopped:
    debug "SendService.trackedSend: service is stopped, task not started",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    return
  if self.serviceLoopHandle.isNil():
    debug "SendService.trackedSend: service not started, task waits for the first round",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    task.state = DeliveryState.NextRoundRetry
    self.addTask(task)
    return
  self.sweepAttempts()
  self.track(task, self.send(task), retry = false)
