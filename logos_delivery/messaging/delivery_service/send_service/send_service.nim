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
import logos_delivery/api/events/[messaging_client_events, kernel_events]
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
    lastStoreCheckTime: Moment.now(),
    maxDeliveryTime: maxDeliveryTime(anonymityLevel),
  )

  return ok(sendService)

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

proc awaitsStoreValidation*(self: SendService, task: DeliveryTask): bool =
  ## True while a propagated task still needs a store node to confirm it.
  ##
  ## A message that went out over mix never does. The confirmation is a store
  ## query carrying that message's hash, and it travels in clear text from this
  ## node's own address seconds after the message appeared on the network --
  ## handing any observer precisely the link the mixed send just paid to break.
  ## A mixed message is complete when the exit's reply arrives, and
  ## `reportTaskResult` reports it there instead.
  ##
  ## The trade is deliberate and worth stating: a mixed send has weaker delivery
  ## assurance than a plain one, because the only witness it can safely use is
  ## the exit's reply. Anonymity is the thing the caller asked for.
  ##
  ## Exported for the tests.
  return
    self.checkStoreForMessages and task.state == DeliveryState.SuccessfullyPropagated and
    not task.isEphemeral() and not task.propagatedOverMix

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

    if task.propagatedOverMix and not task.seenEventEmitted:
      # The receive service backfills, after an offline window, every message
      # a store node holds on the subscribed topics that this node has not
      # seen, fetching each by hash in clear from this node's own address.
      # "Seen" is fed by the relay and filter handlers; a relay publish
      # delivers the node's own message to them (`triggerSelf`), so a plain
      # send is seen at once. A mixed message reaches the network through the
      # exit and is seen only when it comes back, and one that does not come
      # back in time is named to a store node at the next reconnection. Mark
      # it seen here, as the relay does for its own publish.
      MessageSeenEvent.emit(self.brokerCtx, task.pubsubTopic, task.msg)
      task.seenEventEmitted = true

    if task.propagatedOverMix and not task.sentEventEmitted and
        self.checkStoreForMessages and not task.isEphemeral():
      # A mixed send has no store confirmation, so the exit's reply is its
      # completion -- but only report it where the plain path would have
      # reported one, i.e. where store validation was going to run. This keeps
      # the terminal event path-independent: a reliability-off or ephemeral
      # send ends the same way whether it went plain or over mix. Gated on its
      # own flag, not `propagateEventEmitted`, so a prior plain MessagePropagated
      # does not swallow the mixed completion; `sentEventEmitted` keeps it once.
      info "Message successfully sent over mix",
        requestId = task.requestId, msgHash = task.msgHash.to0xHex()
      MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
      task.sentEventEmitted = true
    return
  of DeliveryState.SuccessfullyValidated:
    info "Message successfully sent",
      requestId = task.requestId, msgHash = task.msgHash.to0xHex()
    MessageSentEvent.emit(self.brokerCtx, task.requestId, task.msgHash.to0xHex())
    task.sentEventEmitted = true
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
      not self.awaitsStoreValidation(it)
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
  # `send` is asyncSpawned by the messaging API, which returns the request id to
  # the caller only after this proc yields. With no RLN and budget to spare the
  # body runs to completion synchronously, so a terminal event reported here --
  # as a Required fail-fast produces -- would fire before the caller (e.g. a
  # reliable channel) has registered the id, and be discarded. Yield once so the
  # caller unwinds and registers first, then report.
  #
  # What the yield guarantees: this proc resumes only after the run that called
  # it has ended, and that run hands the request id to the caller inline (the
  # messaging API never suspends before returning it), so the id is recorded
  # before any event about it can be emitted.
  #
  # TODO: make this hold by construction rather than by a yield. `send` should
  # only mint the id, enqueue the task and wake the service loop (an AsyncEvent,
  # so an immediate outcome does not wait for the next tick); admission, proof
  # and processing then run in the loop, and every terminal event comes from a
  # later scheduling slot than the call that returned the id.
  await sleepAsync(ZeroDuration)
  reportTaskResult(self, task)
  if task.state != DeliveryState.FailedToDeliver:
    self.addTask(task)
