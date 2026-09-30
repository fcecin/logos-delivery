## This module is in charge of taking care of the messages that this node is expecting to
## receive and is backed by store-v3 requests to get an additional degree of certainty
##
## Reconnection backfill: offline while relay is not READY (Core), any
## subscribed shard lacks a healthy filter subscription (Edge), or no Store
## peer is known. Queries Store when back online.
##

import results, std/[tables, sequtils, sets]
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
from logos_delivery/waku/waku_archive/archive import MaxMessageTimestampVariance

const MaxMessageLife = chronos.minutes(7) ## Max time we will keep track of rx messages

const PruneOldMsgsPeriod = chronos.minutes(1)

const DelayExtra* = chronos.seconds(5)
  ## Additional security time to overlap the missing messages queries

const ActivityWriteInterval* = chronos.seconds(10)
  ## Least time between two recovery hint writes from received messages.

const CatchUpRetryPeriod* = chronos.seconds(30)
  ## Longest wait between two Store attempts of the startup catch-up. A new
  ## subscription or a peer change ends the wait early.

const FirstRunHistory* = chronos.hours(24)
  ## The catch-up of a node with no recovery hint goes back this far.

const MaxCheckAttempts* = 10
  ## Store passes of one reconnection check before it stops with content topics
  ## that are not complete. The next time that the node comes back online, a
  ## new check reads them again.

const CatchUpSettlePeriod* = chronos.seconds(10)
  ## The wait for one more subscription after the startup catch-up is complete.

const
  DefaultBackfillEnabled = true
  DefaultBackfillRequestTimeout = chronos.seconds(10)
  MinBackfillRequestTimeoutSeconds = 1
  MaxBackfillRequestTimeoutSeconds = 300

type BackfillState* = object
  ## Settings and state of the startup catch-up. `backfill.nim` has the mechanism.
  enabled*: bool
  queryTimeout*: Duration
  task: Future[void] ## the startup catch-up
  hintListener: Opt[MessageReceivedEventListener]
    ## advances the hint on each received message, installed after the hint is read

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

  receivePathReady: bool
    ## the result of `hasReadyReceivePath` at the last readiness event
  online: bool ## receive path ready (see hasReadyReceivePath) and a Store peer known
  backfillHandler: Future[void] ## in-flight store backfill task
  msgPrunerHandler: Future[void] ## removes too old messages

  startTimeToCheck: Timestamp
    ## The start of the next reconnection check. The node has all messages from
    ## before this time. A live message and a complete check move it forward.
  recheckRequested: bool
    ## Set when the node comes back online while a check runs. The check then
    ## reads all content topics again.

  backfill: BackfillState ## the startup catch-up from the persisted hint
  stopping: bool
    ## Lets the startup catch-up stop at shutdown. A broker request catches
    ## `CancelledError` and does not raise it again, so a cancel may not reach the
    ## catch-up task. Remove this flag when broker requests raise it again.

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
  let now = getNowInNanosecondTime()
  self.recentReceivedMsgs[msgHash] = now
  # A running check keeps its start, so that the pruner keeps the hashes of its
  # window.
  let checkRunning =
    not self.backfillHandler.isNil() and not self.backfillHandler.finished()
  if source == MessageSource.Live and self.receivePathReady and not checkRunning:
    self.startTimeToCheck = max(self.startTimeToCheck, now - BackfillOverlap)
  recordReceived(source, message.payload.len)
  info "Message received",
    msg_hash = msgHash.to0xHex(),
    contentTopic = message.contentTopic,
    pubsubTopic = pubsubTopic,
    source = source
  MessageReceivedEvent.emit(self.brokerCtx, msgHash.to0xHex(), message, source)
  return true

proc queryAnyStore(self: RecvService): BackfillQuery =
  ## Sends a query to any Store peer. Gives an error when the service stops.
  return proc(
      request: StoreQueryRequest
  ): Future[Result[StoreQueryResponse, string]] {.async.} =
    if self.stopping:
      return err("receive service is stopping")
    return await self.waku.storeQueryToAny(request)

proc deliverFromStore(self: RecvService): BackfillDeliver =
  ## Delivers a message from Store as `MessageSource.History`. Gives false for
  ## a message of a content topic that the node does not subscribe to.
  return proc(
      pubsubTopic: PubsubTopic, message: WakuMessage
  ): bool {.gcsafe, raises: [].} =
    if not self.waku.isContentSubscribed(pubsubTopic, message.contentTopic):
      return false
    discard self.processIncomingMessage(pubsubTopic, message, MessageSource.History)
    return true

proc checkStore*(self: RecvService) {.async.} =
  ## Checks the store for messages that were not received directly and
  ## delivers them via MessageReceivedEvent, as `MessageSource.History`.
  ## Reads all pages of each subscribed content topic from `startTimeToCheck`.
  ## Tries a failed content topic again after `CatchUpRetryPeriod`. Moves
  ## `startTimeToCheck` forward only when all content topics are complete.
  if not self.waku.isStoreMounted():
    debug "recv service has no store client mounted, skipping store check"
    return

  let since = self.startTimeToCheck
  var completed: HashSet[BackfillTopic]
  let progress = newTable[BackfillTopic, Timestamp]() # a failed topic's next page start
  self.recheckRequested = false
  var attempt = 0
  while not self.stopping:
    inc attempt
    let checkEnd = getNowInNanosecondTime()
    let pending =
      backfillTopics(self.waku.subscribedContentTopics()).filterIt(it notin completed)
    let exhausted = await runCatchUpPass(
      pending,
      progress,
      since,
      checkEnd + DelayExtra.nanos,
      self.backfill.queryTimeout,
      self.queryAnyStore(),
      self.deliverFromStore(),
    )
    for topic in exhausted:
      completed.incl(topic)
    if self.recheckRequested:
      # The node went offline and came back during the check.
      self.recheckRequested = false
      completed.clear()
      progress.clear()
      attempt = 0
      continue
    if exhausted.len == pending.len:
      self.startTimeToCheck = max(self.startTimeToCheck, checkEnd - DelayExtra.nanos)
      return
    if attempt >= MaxCheckAttempts:
      warn "Store check stopped with content topics that are not complete",
        attempts = attempt, incomplete = pending.len - exhausted.len
      return
    debug "checkStore did not complete all content topics, it tries them again",
      attempt = attempt, completed = exhausted.len, pending = pending.len
    await sleepAsync(CatchUpRetryPeriod)
    if not self.online:
      # The next time that the node comes back online starts a new check.
      return

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
  ## When the node is back online, queries Store for the messages missed since
  ## `startTimeToCheck`. Online needs a Store peer. The node can lose its peers
  ## some time before it sees the loss, so the time it goes offline does not
  ## set the start of the check.
  self.receivePathReady = self.hasReadyReceivePath()
  let nowOnline = self.receivePathReady and self.waku.hasStorePeer()
  if nowOnline == self.online:
    return
  self.online = nowOnline

  if not nowOnline:
    return

  # At most one backfill in flight. A running check reads all topics again.
  if self.backfillHandler.isNil() or self.backfillHandler.finished():
    info "recv service backfilling missed messages after coming back online"
    self.backfillHandler = self.checkStore()
  else:
    self.recheckRequested = true

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

proc listenForReceipts(
    brokerCtx: BrokerContext, job: persistency.Job
): Result[MessageReceivedEventListener, string] =
  ## Every accepted message, live or from Store, moves the hint to now, at
  ## most one time per `ActivityWriteInterval`.
  var lastWrite = Moment()
  let onReceived = proc(event: MessageReceivedEvent) {.async: (raises: []).} =
    let now = Moment.now()
    if not job.running or now - lastWrite < ActivityWriteInterval:
      return
    lastWrite = now # before the await, so a burst writes one time
    try:
      await job.writeRecoveryHint(getNowInNanosecondTime())
    except CancelledError:
      discard
  return MessageReceivedEvent.listen(brokerCtx, onReceived)

proc startupCatchUp(self: RecvService, job: persistency.Job) {.async.} =
  ## Run once at startup to fetch missed messages from Store.
  ## Received messages update the saved time on disk. This catch-up keeps
  ## using the value it read at startup. If the time is missing or cannot
  ## be read, query from 24 hours before startup.
  let startedAt = getNowInNanosecondTime() # before the first await
  let stored = (await job.readRecoveryHint()).valueOr:
    if self.stopping:
      return
    warn "Failed to read the Store catch-up recovery hint", reason = error
    Opt.none(Timestamp) # the same as no hint
  let since =
    if stored.isSome():
      stored.get() - BackfillOverlap
    else:
      await job.writeRecoveryHint(startedAt)
        # a first run stores its start for the next run
      startedAt - FirstRunHistory.nanos
  let receipts = listenForReceipts(self.brokerCtx, job).valueOr:
    warn "Store catch-up aborted", reason = error
    return
  self.backfill.hintListener = Opt.some(receipts)
  var completed: HashSet[BackfillTopic]
  let progress = newTable[BackfillTopic, Timestamp]() # a failed topic's next page start
  var settleUntil: Opt[Moment] # set when there is nothing left to do
  let query = self.queryAnyStore()
  let deliver = self.deliverFromStore()
  let wake = newAsyncEvent() # a new subscription or a peer change
  let onSubscribed = proc(event: ContentTopicSubscribedEvent) {.async: (raises: []).} =
    wake.fire()
  let onPeerEvent = proc(event: WakuPeerEvent) {.async: (raises: []).} =
    wake.fire() # any peer change can make a Store peer available
  let subscriptions = ContentTopicSubscribedEvent.listen(self.brokerCtx, onSubscribed).valueOr:
    warn "Store catch-up aborted", reason = error
    return
  defer:
    await ContentTopicSubscribedEvent.dropListener(self.brokerCtx, subscriptions)
  let peers = WakuPeerEvent.listen(self.brokerCtx, onPeerEvent).valueOr:
    warn "Store catch-up aborted", reason = error
    return
  defer:
    await WakuPeerEvent.dropListener(self.brokerCtx, peers)
  while true:
    if not job.running:
      warn "Store catch-up aborted", reason = "persistency job is closed"
      return
    let pending =
      backfillTopics(self.waku.subscribedContentTopics()).filterIt(it notin completed)
    if pending.len == 0:
      if completed.len > 0:
        # Caught up. The app can subscribe more topics after this. Wait for the
        # next subscription, and stop when none comes. A wake that brings no
        # work does not extend the wait.
        if settleUntil.isNone():
          settleUntil = Opt.some(Moment.now() + CatchUpSettlePeriod)
        let remaining = settleUntil.get() - Moment.now()
        if remaining <= ZeroDuration or not await wake.wait().withTimeout(remaining):
          break
      else:
        await wake.wait() # nothing subscribed yet
      wake.clear()
      continue
    settleUntil = Opt.none(Moment) # new work, the settle wait starts after it
    if not self.waku.hasStorePeer():
      discard await wake.wait().withTimeout(CatchUpRetryPeriod) # nobody to ask yet
      wake.clear()
      continue
    let cutoff = getNowInNanosecondTime()
      # the end of this pass, from here the messages come live
    let exhausted = await runCatchUpPass(
      pending, progress, since, cutoff, self.backfill.queryTimeout, query, deliver
    )
    if self.stopping:
      return
    for topic in exhausted:
      completed.incl(topic)
    if exhausted.len < pending.len:
      discard
        await wake.wait().withTimeout(CatchUpRetryPeriod) # a topic failed, ask again
      wake.clear()

proc waitForStartupCatchUp*(self: RecvService) {.async.} =
  ## Waits for the startup catch-up to exit. Cancelling the wait leaves the
  ## catch-up running.
  if not self.backfill.task.isNil():
    await self.backfill.task.join()

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
    waku: waku,
    startTimeToCheck: now - DelayExtra.nanos,
    brokerCtx: waku.brokerCtx,
    backfill: backfill,
  )

  return recvService

proc loopPruneOldMessages(self: RecvService) {.async.} =
  while true:
    # Keeps the hashes that the next check can get from Store again.
    let oldestAllowedTime = min(
      getNowInNanosecondTime() - MaxMessageLife.nanos,
      self.startTimeToCheck - MaxMessageTimestampVariance,
    )
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

  # All of these can change `online`. Subscriptions and peers have no health event.
  self.protocolHealthListener = self.listenForReadiness(EventProtocolHealthChange)
  self.shardHealthListener = self.listenForReadiness(EventShardTopicHealthChange)
  self.subscribedEventListener = self.listenForReadiness(ContentTopicSubscribedEvent)
  self.unsubscribedEventListener =
    self.listenForReadiness(ContentTopicUnsubscribedEvent)
  self.peerEventListener = self.listenForReadiness(WakuPeerEvent)

  # The initial read starts no backfill.
  self.receivePathReady = self.hasReadyReceivePath()
  self.online = self.receivePathReady

  if self.backfill.enabled and not job.isNil():
    self.backfill.task = self.startupCatchUp(job)

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
