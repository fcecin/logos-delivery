## This module is in charge of taking care of the messages that this node is expecting to
## receive and is backed by store-v3 requests to get an additional degree of certainty
##
## Reconnection backfill: offline while relay is not READY (Core), any
## subscribed shard lacks a healthy filter subscription (Edge), or no Store
## peer is known. Queries Store when back online.
##

import results, std/[algorithm, tables, sequtils, sets]
import chronos, chronicles
import brokers/broker_context
import
  ./backfill,
  logos_delivery/api/conf/messaging_conf,
  logos_delivery/api/events/kernel_events,
  logos_delivery/api/events/messaging_client_events, # MessageReceivedEvent
  logos_delivery/messaging/messaging_metrics,
  logos_delivery/waku/persistency/persistency,
  logos_delivery/waku/[waku_core, waku_core/topics, waku_store/common],
  logos_delivery/waku/waku,
  logos_delivery/waku/api/[store, subscriptions, health],
  logos_delivery/waku/api/events/[health_events, peer_events],
  logos_delivery/waku/requests/health_requests,
  logos_delivery/waku/node/health_monitor/health_status

const MaxMessageLife = chronos.minutes(7) ## Max time we will keep track of rx messages

const PruneOldMsgsPeriod = chronos.minutes(1)

const DelayExtra* = chronos.seconds(5)
  ## Additional security time to overlap the missing messages queries

const ActivityWriteInterval* = chronos.seconds(10)
  ## Least time between two recovery hint writes from received messages.

const CatchUpRetryPeriod* = chronos.seconds(30)
  ## Longest wait between two Store attempts of the catch-up. A subscription
  ## change or a peer change ends the wait early.

const
  DefaultBackfillEnabled = true
  DefaultBackfillRequestTimeout = chronos.seconds(10)
  MinBackfillRequestTimeoutSeconds = 1
  MaxBackfillRequestTimeoutSeconds = 300

type TupleHashAndMsg =
  tuple[hash: WakuMessageHash, msg: WakuMessage, pubsubTopic: PubsubTopic]

type BackfillState* = object
  ## Settings and state of the Store catch-up. `backfill.nim` has the mechanism.
  enabled*: bool
  queryTimeout*: Duration
  task: Future[void] ## the catch-up worker
  workerRunning: bool ## the worker runs and takes the subscription changes
  changes: seq[SubscriptionChange] ## the changes that the worker did not take yet
  wake: AsyncEvent ## a subscription change or a peer change
  caughtUp: AsyncEvent
    ## set when the worker took all changes and no subscribed topic is owed
  lastReceivedAt: Timestamp ## the local time of the last received message
  hintListener: Opt[MessageReceivedEventListener]
    ## advances the hint on each received message, installed after the records are read

type RecvService* = ref object of RootObj
  brokerCtx: BrokerContext
  waku: Waku
  seenMsgListener: MessageSeenEventListener
  protocolHealthListener: EventProtocolHealthChangeListener
  shardHealthListener: EventShardTopicHealthChangeListener
  subscribedEventListener: ContentTopicSubscribedEventListener
  unsubscribedEventListener: ContentTopicUnsubscribedEventListener
  peerEventListener: WakuPeerEventListener

  recentReceivedMsgs: Table[WakuMessageHash, Timestamp]
    ## hash of each message received in the last `MaxMessageLife`, with its
    ## local receipt time
  subscribedAt: Table[BackfillTopic, Timestamp]
    ## the time of the subscription in this run of each subscribed topic

  online: bool ## receive path ready (see hasReadyReceivePath) and a Store peer known
  backfillHandler: Future[void] ## in-flight store backfill task
  reconnectedDuringCheck: bool
    ## the node came back online while a store backfill ran, so it runs one more time
  msgPrunerHandler: Future[void] ## removes too old messages

  startTimeToCheck: Timestamp
  endTimeToCheck: Timestamp

  backfill: BackfillState ## the Store catch-up from the persisted records
  stopping: bool
    ## Lets the catch-up stop at shutdown. A broker request catches
    ## `CancelledError` and does not raise it again, so a cancel may not reach the
    ## catch-up task. Remove this flag when broker requests raise it again.

  delayExtra*: Duration = DelayExtra
  activityWriteInterval*: Duration = ActivityWriteInterval

proc getMissingMsgsFromStore(
    self: RecvService, msgHashes: seq[WakuMessageHash]
): Future[Result[seq[TupleHashAndMsg], string]] {.async.} =
  let storeResp: StoreQueryResponse = (
    await self.waku.storeQueryToAny(
      StoreQueryRequest(includeData: true, messageHashes: msgHashes)
    )
  ).valueOr:
    return err("getMissingMsgsFromStore: " & $error)

  let otherwiseMsg = WakuMessage()
  let otherwiseTopic = PubsubTopic("")
  return ok(
    storeResp.messages.mapIt(
      (
        hash: it.messageHash,
        msg: it.message.get(otherwiseMsg),
        pubsubTopic: it.pubsubTopic.get(otherwiseTopic),
      )
    )
  )

proc processIncomingMessage(
    self: RecvService, pubsubTopic: string, message: WakuMessage, source: MessageSource
): bool =
  ## Return false if the incoming message is from a non-subscribed topic,
  ## or if the message is a duplicate (recently-seen). Otherwise, save it as
  ## recently-seen, emit a MessageReceivedEvent tagged with `source`, and
  ## return true.

  if not self.waku.isContentSubscribed(pubsubTopic, message.contentTopic):
    trace "skipping message as I am not subscribed",
      shard = pubsubTopic, contentTopic = message.contentTopic
    return false

  let msgHash = computeMessageHash(pubsubTopic, message)
  if self.recentReceivedMsgs.hasKey(msgHash):
    trace "skipping duplicate message",
      shard = pubsubTopic,
      contentTopic = message.contentTopic,
      msg_hash = msgHash.to0xHex()
    return false

  # Local receipt time: a message recovered from Store stays known for the
  # full period whatever its own timestamp.
  self.recentReceivedMsgs[msgHash] = getNowInNanosecondTime()
  recordReceived(source, message.payload.len)
  info "Message received",
    msg_hash = msgHash.to0xHex(),
    contentTopic = message.contentTopic,
    pubsubTopic = pubsubTopic,
    source = source
  MessageReceivedEvent.emit(self.brokerCtx, msgHash.to0xHex(), message, source)
  return true

proc checkStore*(self: RecvService) {.async.} =
  ## Checks the store for messages that were not received directly and
  ## delivers them via MessageReceivedEvent, as `MessageSource.History`.
  if not self.waku.isStoreMounted():
    debug "recv service has no store client mounted, skipping store check"
    return

  self.endTimeToCheck = getNowInNanosecondTime()

  ## query store and deliver new recovered messages per subscribed topic
  for (pubsubTopic, contentTopics) in self.waku.subscribedContentTopics():
    let storeResp: StoreQueryResponse = (
      await self.waku.storeQueryToAny(
        StoreQueryRequest(
          includeData: false,
          pubsubTopic: Opt.some(pubsubTopic),
          contentTopics: toSeq(contentTopics),
          startTime: Opt.some(self.startTimeToCheck - self.delayExtra.nanos),
          endTime: Opt.some(self.endTimeToCheck + self.delayExtra.nanos),
        )
      )
    ).valueOr:
      debug "checkStore failed to get remote msgHashes",
        pubsubTopic = pubsubTopic, cTopics = toSeq(contentTopics), error = $error
      continue

    ## compare the msgHashes seen from the store vs the ones received directly
    let msgHashesInStore = storeResp.messages.mapIt(it.messageHash)
    let missedHashes: seq[WakuMessageHash] =
      msgHashesInStore.filterIt(not self.recentReceivedMsgs.hasKey(it))

    if missedHashes.len > 0:
      info "missed messages detected, checking store for missed messages",
        pubsubTopic = pubsubTopic, missedCount = missedHashes.len

      ## Now retrieve the missing WakuMessages and deliver them
      let missingMsgsRet = await self.getMissingMsgsFromStore(missedHashes)
      if missingMsgsRet.isOk():
        for msgTuple in missingMsgsRet.get():
          # A topic gets no message from more than `delayExtra` before its
          # subscription.
          let topic: BackfillTopic = (msgTuple.pubsubTopic, msgTuple.msg.contentTopic)
          if msgTuple.msg.timestamp <
              self.subscribedAt.getOrDefault(topic, 0) - self.delayExtra.nanos:
            trace "Recv service skips a message from before the subscription",
              msg_hash = shortLog(msgTuple.hash), pubsubTopic = msgTuple.pubsubTopic
            continue
          if self.processIncomingMessage(
            msgTuple.pubsubTopic, msgTuple.msg, MessageSource.History
          ):
            debug "recv service store-recovered message",
              msg_hash = shortLog(msgTuple.hash), pubsubTopic = msgTuple.pubsubTopic
      else:
        debug "Failed to retrieve missing messages: ", error = $missingMsgsRet.error

  ## update next check times
  self.startTimeToCheck = self.endTimeToCheck

proc runStoreChecks(self: RecvService) {.async.} =
  ## Runs `checkStore`. When the node comes back online while a check runs,
  ## one more check runs after it, so the new gap gets a check too.
  while true:
    self.reconnectedDuringCheck = false
    await self.checkStore()
    if not self.reconnectedDuringCheck or not self.online:
      return
    info "Recv service backfilling again, the node came back online during the check"

proc hasHealthyFilterSubscription(self: RecvService): bool =
  ## Every subscribed shard has a healthy filter subscription (false with none).
  var shards = 0
  for (shard, _) in self.waku.subscribedContentTopics():
    inc shards
    let shardHealth = RequestEdgeShardHealth.request(self.brokerCtx, shard).valueOr:
      debug "Failed to read the filter subscription health of a shard",
        shard = shard, error = error
      return false
    if shardHealth.health notin
        {TopicHealth.MINIMALLY_HEALTHY, TopicHealth.SUFFICIENTLY_HEALTHY}:
      return false
  return shards > 0

proc hasReadyReceivePath(self: RecvService): bool =
  ## Relay READY, or a healthy filter subscription on every subscribed shard.
  return
    self.waku.reportedProtocolHealth(WakuProtocol.RelayProtocol).health ==
    HealthStatus.READY or self.hasHealthyFilterSubscription()

proc updateReceiveReadiness(self: RecvService) =
  ## Records the time the node goes offline. When the node is back online,
  ## queries Store for the messages missed while offline. Does not retry a
  ## failed Store query, so online needs a Store peer.
  let nowOnline = self.hasReadyReceivePath() and self.waku.hasStorePeer()
  if nowOnline == self.online:
    return
  self.online = nowOnline

  if not nowOnline:
    self.startTimeToCheck = getNowInNanosecondTime()
    return

  # At most one backfill in flight. It runs one more time when the node comes
  # back online during it.
  if self.backfillHandler.isNil() or self.backfillHandler.finished():
    info "recv service backfilling missed messages after coming back online"
    self.backfillHandler = self.runStoreChecks()
  else:
    self.reconnectedDuringCheck = true

proc noteSubscriptionChange(self: RecvService, topic: BackfillTopic, subscribed: bool) =
  ## Keeps the subscription time of each topic, and gives the change to the
  ## catch-up worker.
  let now = getNowInNanosecondTime()
  if subscribed:
    self.subscribedAt[topic] = now
  else:
    self.subscribedAt.del(topic)
  if not self.backfill.workerRunning:
    return
  self.backfill.changes.add(
    SubscriptionChange(
      topic: topic,
      subscribed: subscribed,
      at: now,
      lastReceivedAt: self.backfill.lastReceivedAt,
    )
  )
  self.backfill.caughtUp.clear()
  self.backfill.wake.fire()

proc listenForReadiness(self: RecvService, E: typedesc): auto =
  ## Re-evaluates `online` on each `E` event. An event that changes nothing
  ## is harmless, as `updateReceiveReadiness` acts only on a change.
  let listener = E.listen(
    self.brokerCtx,
    proc(event: E) {.async: (raises: []).} =
      self.updateReceiveReadiness(),
  ).valueOr:
    error "Failed to set a receive readiness listener", event = $E, error = error
    quit(QuitFailure)
  return listener

proc listenForSubscriptions(self: RecvService, E: typedesc, subscribed: bool): auto =
  ## Gives each `E` change to `noteSubscriptionChange`, and re-evaluates
  ## `online`.
  let listener = E.listen(
    self.brokerCtx,
    proc(event: E) {.async: (raises: []).} =
      self.noteSubscriptionChange((event.shard, event.contentTopic), subscribed)
      self.updateReceiveReadiness(),
  ).valueOr:
    error "Failed to set a subscription listener", event = $E, error = error
    quit(QuitFailure)
  return listener

proc listenForReceipts(
    self: RecvService, job: persistency.Job
): Result[MessageReceivedEventListener, string] =
  ## Every accepted message, live or from Store, sets `lastReceivedAt` to now.
  ## It also moves the hint to now, at most one time per
  ## `activityWriteInterval`.
  var lastWrite = Moment()
  let onReceived = proc(event: MessageReceivedEvent) {.async: (raises: []).} =
    self.backfill.lastReceivedAt = getNowInNanosecondTime()
    let now = Moment.now()
    if not job.running or now - lastWrite < self.activityWriteInterval:
      return
    lastWrite = now # before the await, so a burst writes one time
    try:
      await job.writeRecoveryHint(self.backfill.lastReceivedAt)
    except CancelledError:
      discard
  return MessageReceivedEvent.listen(self.brokerCtx, onReceived)

proc applyQueuedChanges(
    self: RecvService,
    records: var Table[BackfillTopic, TopicRecord],
    subscribed: var HashSet[BackfillTopic],
    upgradeHint: Opt[Timestamp],
): seq[TxOp] =
  ## Takes the queued subscription changes and applies them (see
  ## `applyChanges`). Returns the writes of the records that changed.
  var changes: seq[SubscriptionChange]
  swap(changes, self.backfill.changes)
  return applyChanges(records, subscribed, changes, upgradeHint)

proc catchUpWorker(self: RecvService, job: persistency.Job) {.async.} =
  ## Gets from Store the messages that the node missed on the topics that it
  ## had before. Runs until stop. Each recorded topic is live or owed (see
  ## `TopicRecord`). At start, each live record becomes owed (see `atStart`).
  ## When the app subscribes an owed topic, a pass queries
  ## `[at - BackfillOverlap, cutoff)`, and the topic is then live from
  ## `cutoff`, which the pass takes at the turn of the topic. A topic with no record is new. Its subscription makes a live
  ## record and gets no history.
  defer:
    self.backfill.workerRunning = false
    self.backfill.changes.setLen(0)
  let hint = (await job.readRecoveryHint()).valueOr:
    if self.stopping:
      return
    warn "Failed to read the Store catch-up recovery hint", reason = error
    Opt.none(Timestamp) # the same as no hint
  let stored = (await job.readTopicRecords(hint)).valueOr:
    if not self.stopping:
      warn "Store catch-up stopped for this run", reason = error
    return
  # A hint needs a received message, which needs a subscription, which writes
  # a record. So a hint with no record comes from a node that kept no records.
  # In a run that starts in this state, a topic with no record is owed from
  # the hint.
  let upgradeHint =
    if stored.len == 0:
      hint
    else:
      Opt.none(Timestamp)
  var (records, startOps) = recordsAtStart(stored, hint)
  var subscribed: HashSet[BackfillTopic]
  defer:
    if job.running: # a stop loses no queued change
      await job.writeTopicRecords(
        self.applyQueuedChanges(records, subscribed, upgradeHint)
      )
  await job.writeTopicRecords(startOps) # before the receipt listener can move the hint
  if self.stopping:
    return # stop drops the receipt listener only when it exists
  let receipts = self.listenForReceipts(job).valueOr:
    warn "Store catch-up aborted", reason = error
    return
  self.backfill.hintListener = Opt.some(receipts)
  let starts = newTable[BackfillTopic, Timestamp]()
    # the next page start of each owed topic
  let query: BackfillQuery = proc(
      request: StoreQueryRequest
  ): Future[Result[StoreQueryResponse, string]] {.async.} =
    if self.stopping:
      return err("receive service is stopping")
    return await self.waku.storeQueryToAny(request)
  let deliver: BackfillDeliver = proc(
      pubsubTopic: PubsubTopic, message: WakuMessage
  ): bool {.gcsafe, raises: [].} =
    if not self.waku.isContentSubscribed(pubsubTopic, message.contentTopic):
      return false
    discard self.processIncomingMessage(pubsubTopic, message, MessageSource.History)
    return true
  let onPeerEvent = proc(event: WakuPeerEvent) {.async: (raises: []).} =
    self.backfill.wake.fire() # any peer change can make a Store peer available
  let peers = WakuPeerEvent.listen(self.brokerCtx, onPeerEvent).valueOr:
    warn "Store catch-up aborted", reason = error
    return
  defer:
    await WakuPeerEvent.dropListener(self.brokerCtx, peers)
  while true:
    if not job.running:
      warn "Store catch-up aborted", reason = "persistency job is closed"
      return
    await job.writeTopicRecords(
      self.applyQueuedChanges(records, subscribed, upgradeHint)
    )
    var pending: seq[BackfillTopic]
    for topic in subscribed:
      if records.getOrDefault(topic).owed:
        pending.add(topic)
    pending.sort()
    if pending.len == 0:
      if self.backfill.changes.len == 0:
        self.backfill.caughtUp.fire()
      await self.backfill.wake.wait()
      self.backfill.wake.clear()
      continue
    if not self.waku.hasStorePeer():
      discard await self.backfill.wake.wait().withTimeout(CatchUpRetryPeriod)
        # nobody to ask yet
      self.backfill.wake.clear()
      continue
    var failed = false
    for topic in pending:
      # Take the changes before each topic, so a long pass holds no change.
      await job.writeTopicRecords(
        self.applyQueuedChanges(records, subscribed, upgradeHint)
      )
      if topic notin subscribed:
        continue # unsubscribed during the pass, the topic stays owed
      let cutoff = getNowInNanosecondTime()
        # the end of the range of this topic, from here its messages come live
      starts.setStartIfMissing(topic, records.getOrDefault(topic))
      # One topic at a time, so a stop repeats only the topic in progress.
      if await catchUpTopic(
        topic, starts, cutoff, self.backfill.queryTimeout, query, deliver
      ):
        await job.writeTopicRecords(records.setLive([topic], cutoff))
      else:
        failed = true # the topic stays owed, and the pass goes on
      if self.stopping:
        return
    if failed:
      discard await self.backfill.wake.wait().withTimeout(CatchUpRetryPeriod)
        # a topic failed, ask again
      self.backfill.wake.clear()

proc waitForCatchUp*(self: RecvService): Future[bool] {.async.} =
  ## True when the catch-up worker took all subscription changes and no
  ## subscribed topic is owed, and at once when no worker runs. False when the
  ## worker ends first.
  let task = self.backfill.task
  if task.isNil() or self.backfill.caughtUp.isSet():
    return true
  if task.finished():
    return false
  let caughtUp = self.backfill.caughtUp.wait()
  let ended = task.join()
  try:
    discard await race(caughtUp, ended)
  finally:
    caughtUp.cancelSoon()
    ended.cancelSoon()
  return caughtUp.completed()

proc init*(T: type BackfillState, conf: MessagingClientConf): Result[T, string] =
  ## Rejects a timeout outside `MinBackfillRequestTimeoutSeconds` ..
  ## `MaxBackfillRequestTimeoutSeconds`.
  var queryTimeout = DefaultBackfillRequestTimeout
  if conf.backfillRequestTimeoutSeconds.isSome():
    let seconds = conf.backfillRequestTimeoutSeconds.get()
    if seconds < MinBackfillRequestTimeoutSeconds or
        seconds > MaxBackfillRequestTimeoutSeconds:
      return err(
        "backfillRequestTimeoutSeconds must be between " &
          $MinBackfillRequestTimeoutSeconds & " and " & $MaxBackfillRequestTimeoutSeconds &
          ", got " & $seconds
      )
    queryTimeout = chronos.seconds(seconds)
  return ok(
    T(
      enabled: conf.backfillEnabled.get(DefaultBackfillEnabled),
      queryTimeout: queryTimeout,
    )
  )

proc new*(T: typedesc[RecvService], waku: Waku, backfill: BackfillState): T =
  ## The storeClient will help to acquire any possible missed messages

  let now = getNowInNanosecondTime()
  var recvService = RecvService(
    waku: waku, startTimeToCheck: now, brokerCtx: waku.brokerCtx, backfill: backfill
  )

  return recvService

proc loopPruneOldMessages(self: RecvService) {.async.} =
  while true:
    let oldestAllowedTime = getNowInNanosecondTime() - MaxMessageLife.nanos
    var expired: seq[WakuMessageHash]
    for msgHash, rxTime in self.recentReceivedMsgs:
      if rxTime <= oldestAllowedTime:
        expired.add(msgHash)
    for msgHash in expired:
      self.recentReceivedMsgs.del(msgHash)
    await sleepAsync(PruneOldMsgsPeriod)

proc startRecvService*(self: RecvService, job: persistency.Job) =
  ## `job` is the messaging layer's Persistency job, nil when the layer has none.
  self.stopping = false
  self.msgPrunerHandler = self.loopPruneOldMessages()

  self.seenMsgListener = MessageSeenEvent.listen(
    self.brokerCtx,
    proc(event: MessageSeenEvent) {.async: (raises: []).} =
      discard
        self.processIncomingMessage(event.topic, event.message, MessageSource.Live),
  ).valueOr:
    error "Failed to set MessageSeenEvent listener", error = error
    quit(QuitFailure)

  # From here on, the catch-up worker gets each subscription change in order.
  self.backfill.workerRunning = self.backfill.enabled and not job.isNil()
  self.backfill.changes = @[]
  self.backfill.wake = newAsyncEvent()
  self.backfill.caughtUp = newAsyncEvent()
  self.backfill.lastReceivedAt = 0
  self.subscribedAt.clear()
  for topic in backfillTopics(self.waku.subscribedContentTopics()):
    self.noteSubscriptionChange(topic, subscribed = true)

  # All of these can change `online`. Subscriptions and peers have no health event.
  self.protocolHealthListener = self.listenForReadiness(EventProtocolHealthChange)
  self.shardHealthListener = self.listenForReadiness(EventShardTopicHealthChange)
  self.subscribedEventListener =
    self.listenForSubscriptions(ContentTopicSubscribedEvent, subscribed = true)
  self.unsubscribedEventListener =
    self.listenForSubscriptions(ContentTopicUnsubscribedEvent, subscribed = false)
  self.peerEventListener = self.listenForReadiness(WakuPeerEvent)

  # The initial read starts no backfill.
  self.online = self.hasReadyReceivePath()

  if self.backfill.workerRunning:
    self.backfill.task = self.catchUpWorker(job)

proc stopRecvService*(self: RecvService) {.async.} =
  self.stopping = true
  await MessageSeenEvent.dropListener(self.brokerCtx, self.seenMsgListener)
  await EventProtocolHealthChange.dropListener(
    self.brokerCtx, self.protocolHealthListener
  )
  await EventShardTopicHealthChange.dropListener(
    self.brokerCtx, self.shardHealthListener
  )
  await ContentTopicSubscribedEvent.dropListener(
    self.brokerCtx, self.subscribedEventListener
  )
  await ContentTopicUnsubscribedEvent.dropListener(
    self.brokerCtx, self.unsubscribedEventListener
  )
  await WakuPeerEvent.dropListener(self.brokerCtx, self.peerEventListener)
  if self.backfill.hintListener.isSome():
    await MessageReceivedEvent.dropListener(
      self.brokerCtx, self.backfill.hintListener.get()
    )
    self.backfill.hintListener = Opt.none(MessageReceivedEventListener)
  var tasks: seq[Future[void]]
  for task in [self.backfillHandler, self.msgPrunerHandler, self.backfill.task]:
    if not task.isNil():
      tasks.add(task)
  await cancelAndWait(tasks) # every cancel requested before any wait
  self.backfillHandler = nil
  self.msgPrunerHandler = nil
  self.backfill.task = nil
