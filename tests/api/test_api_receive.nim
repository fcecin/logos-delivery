{.used.}

import results, std/[sequtils, net, sets, os, osproc, tempfiles, strutils]
import chronos, metrics, testutils/unittests, stew/byteutils
import libp2p/[peerid, peerinfo, crypto/crypto]
import brokers/broker_context
import ../testlib/[common, wakucore, wakunode, wakunodeconf, testasync, short_intervals]
import ../waku_archive/archive_utils
import logos_delivery/messaging/messaging_client
import logos_delivery/messaging/messaging_metrics
import logos_delivery/messaging/messaging_client_lifecycle
import logos_delivery/messaging/delivery_service/recv_service
import logos_delivery/messaging/delivery_service/recv_service/backfill
import logos_delivery/waku/persistency/persistency
import logos_delivery/api/events/kernel_events
import logos_delivery/waku/requests/health_requests
import logos_delivery/waku/node/health_monitor/health_status
import logos_delivery/waku/api/health
import logos_delivery/api/conf/modes
import logos_delivery/api/conf/logos_delivery_conf
from logos_delivery/waku/waku_store/common import
  WakuStoreCodec, StoreQueryRequest, StoreQueryResult, StoreError, ErrorCode
from logos_delivery/waku/waku_store/protocol_metrics import logos_delivery_store_queries
from logos_delivery/waku/waku_filter_v2/subscriptions import removePeer

import
  logos_delivery,
  logos_delivery/waku/[
    waku_node,
    waku_core,
    node/peer_manager,
    api/events/health_events,
    waku_relay/protocol,
    waku_archive,
    waku_archive/common as archive_common,
  ]
import logos_delivery/waku/factory/waku_conf
import tools/confutils/cli_args
import logos_delivery/api/conf/messaging_conf

const TestTimeout = chronos.seconds(60)

type ReceiveEventListenerManager = ref object
  brokerCtx: BrokerContext
  receivedListener: MessageReceivedEventListener
  receivedEvent: AsyncEvent
  receivedMessages: seq[WakuMessage]
  receivedSources: seq[MessageSource] ## one per `receivedMessages` entry
  targetCount: int

proc newReceiveEventListenerManager(
    brokerCtx: BrokerContext, expectedCount: int = 1
): ReceiveEventListenerManager =
  let manager = ReceiveEventListenerManager(
    brokerCtx: brokerCtx, receivedMessages: @[], targetCount: expectedCount
  )
  manager.receivedEvent = newAsyncEvent()

  manager.receivedListener = MessageReceivedEvent
    .listen(
      brokerCtx,
      proc(event: MessageReceivedEvent) {.async: (raises: []).} =
        manager.receivedMessages.add(event.message)
        manager.receivedSources.add(event.source)
        if manager.receivedMessages.len >= manager.targetCount:
          manager.receivedEvent.fire()
      ,
    )
    .expect("Failed to listen to MessageReceivedEvent")

  return manager

proc teardown(manager: ReceiveEventListenerManager) {.async.} =
  await MessageReceivedEvent.dropListener(manager.brokerCtx, manager.receivedListener)

proc waitForEvents(
    manager: ReceiveEventListenerManager, timeout: Duration
): Future[bool] {.async.} =
  return await manager.receivedEvent.wait().withTimeout(timeout)

proc noMoreEvents(
    manager: ReceiveEventListenerManager, window = chronos.seconds(1)
): Future[bool] {.async.} =
  ## True when no message arrives within `window`.
  manager.targetCount = manager.receivedMessages.len + 1
  manager.receivedEvent.clear()
  return not await manager.waitForEvents(window)

proc payloads(manager: ReceiveEventListenerManager): seq[string] =
  return manager.receivedMessages.mapIt(string.fromBytes(it.payload))

proc waitForPayloadSource(
    manager: ReceiveEventListenerManager, text: string
): Future[Opt[MessageSource]] {.async.} =
  ## The source of the first message with the payload `text`, once it
  ## arrives, or none when it does not arrive within `TestTimeout`.
  let deadline = Moment.now() + TestTimeout
  while true:
    let index = manager.payloads().find(text)
    if index >= 0 and index < manager.receivedSources.len:
      return Opt.some(manager.receivedSources[index])
    if Moment.now() >= deadline:
      return Opt.none(MessageSource)
    await sleepAsync(50.milliseconds)

proc createApiNodeConf(
    numShards: uint16 = 1, mode = LogosDeliveryMode.Core
): WakuNodeConf =
  ## The shared test defaults, plus a resolver that stays on this machine: the
  ## restarted child process must not wait on a real DNS server.
  var conf = defaultTestWakuNodeConf(mode = mode, numShards = numShards)
  conf.dnsAddrsNameServers = @[parseIpAddress("127.0.0.1")]
  return conf

proc backfillOverrides(enabled = true): MessagingClientConf =
  return MessagingClientConf(backfillEnabled: Opt.some(enabled))

proc nodeConf(kernel: WakuNodeConf, messaging: MessagingClientConf): LogosDeliveryConf =
  return LogosDeliveryConf(
    kernelConf: KernelConf(kernel), messagingConf: Opt.some(messaging)
  )

proc caughtUp(node: LogosDelivery): Future[bool] {.async.} =
  ## True when the catch-up of `node` takes all subscription changes and has
  ## no owed subscribed topic within `TestTimeout`, or when no catch-up runs.
  let done = node.messagingClient.recvService.waitForCatchUp()
  if not await done.withTimeout(TestTimeout):
    return false
  return done.read()

proc catchUpEnded(node: LogosDelivery): Future[bool] {.async.} =
  ## True when the catch-up of `node` ends within `TestTimeout`, before it
  ## takes all subscription changes.
  let done = node.messagingClient.recvService.waitForCatchUp()
  if not await done.withTimeout(TestTimeout):
    return false
  return not done.read()

proc writeCatchUpState(
    root: string,
    hint = Opt.none(Timestamp),
    records: seq[(BackfillTopic, TopicRecord)] = @[],
): Future[bool] {.async.} =
  ## Writes the catch-up state of an earlier run into `root`, before a node
  ## opens it. True when the writes are stored.
  let persistency = Persistency.new(root).expect("open root")
  defer:
    persistency.close()
  let job = persistency.openJob(MessagingJobId).expect("open job")
  await job.writeTopicRecords(records.mapIt(topicRecordOp(it[0], it[1]).get()))
  if hint.isSome():
    await job.writeRecoveryHint(hint.get())
  for _ in 0 ..< 100:
    let storedRecords =
      (await job.readTopicRecords()).valueOr(newSeq[(BackfillTopic, TopicRecord)]())
    let storedHint = (await job.readRecoveryHint()).valueOr(Opt.none(Timestamp))
    if storedRecords.len == records.len and storedHint == hint:
      return true
    await sleepAsync(20.milliseconds)
  return false

type TestNetwork = ref object
  storeNode: WakuNode
  archiveDriver: ArchiveDriver
    ## the store node's archive, for rows the archive would reject
  publisher: WakuNode
  subscriber: LogosDelivery
  storeNodePeerInfo: RemotePeerInfo
  missedPayload: seq[byte]
  events: ReceiveEventListenerManager
    ## listening from before the subscription: with a known Store peer the
    ## catch-up delivers as soon as the topic is subscribed
  ownedRoot: string ## temp storage root created here, removed at teardown

proc setupNetwork(
    testTopic: ContentTopic,
    storageRoot = "",
    knowStorePeer = true,
    messaging = backfillOverrides(),
    mode = LogosDeliveryMode.Core,
    remoteFilter = false,
    localStore = false,
    numShards: uint16 = 1,
    storeNodeShards: seq[uint16] = @[],
    activityWriteInterval = Opt.none(Duration),
    history = Opt.some(chronos.hours(2)),
    alsoOwed: seq[ContentTopic] = @[],
    subscribe = true,
): Future[TestNetwork] {.async.} =
  ## A started subscriber on `testTopic` with one message archived between its
  ## start and its subscription, so only Store can deliver it. With `history`,
  ## the subscriber starts with an owed record for `testTopic` from that long
  ## ago, as after a restart, so its catch-up gets the message. With none,
  ## `testTopic` is new. `alsoOwed` topics get the same record. With
  ## `knowStorePeer` the store node is a known service peer that the Store
  ## client dials on demand, as a configured store node is. The root is on
  ## disk so a later process reads the hint, and an empty `storageRoot` gets
  ## a temporary one. With `remoteFilter` the store node serves filter. With
  ## `localStore` the subscriber serves Store. `storeNodeShards` limits the
  ## shards the store node advertises. `testTopic` and `alsoOwed` must
  ## autoshard to shard 0. `activityWriteInterval` replaces the subscriber's
  ## short one. With `subscribe` false, the subscriber does not subscribe
  ## `testTopic`.
  let ownedRoot =
    if storageRoot.len == 0:
      createTempDir("recv-api-", "")
    else:
      ""
  let root = if ownedRoot.len > 0: ownedRoot else: storageRoot
  let shard = PubsubTopic("/waku/2/rs/3/0")
  if history.isSome():
    let owedAt = now() - history.get().nanos
    # A failure here shows as a missing setup message in the test.
    discard await writeCatchUpState(
      root,
      records = (@[testTopic] & alsoOwed).mapIt(
        ((shard, it), TopicRecord(at: owedAt, owed: true))
      ),
    )

  proc dummyHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    discard

  # store node: archive + store + relay, subscribed to the shard
  var storeNode: WakuNode
  var archiveDriver: ArchiveDriver
  lockNewGlobalBrokerContext:
    storeNode = newTestWakuNode(generateSecp256k1Key())
    let advertised =
      if storeNodeShards.len > 0:
        storeNodeShards
      else:
        toSeq(0'u16 ..< numShards)
    storeNode.mountMetadata(TestClusterId, advertised).expect(
      "Failed to mount metadata on storeNode"
    )
    (await storeNode.mountRelay()).expect("Failed to mount relay on storeNode")
    archiveDriver = newSqliteArchiveDriver()
    storeNode.mountArchive(archiveDriver).expect("Failed to mount archive")
    await storeNode.mountStore()
    if remoteFilter:
      await storeNode.mountFilter()
    await storeNode.mountLibp2pPing()
    await storeNode.start()
  storeNode.subscribe((kind: PubsubSub, topic: shard), dummyHandler).expect(
    "Failed to sub storeNode"
  )

  let storeNodePeerInfo = storeNode.peerInfo.toRemotePeerInfo()

  # publisher: relay, connected to the store so its messages get archived
  var publisher: WakuNode
  lockNewGlobalBrokerContext:
    publisher = newTestWakuNode(generateSecp256k1Key())
    publisher.mountMetadata(TestClusterId, toSeq(0'u16 ..< numShards)).expect(
      "Failed to mount metadata on publisher"
    )
    (await publisher.mountRelay()).expect("Failed to mount relay on publisher")
    await publisher.mountLibp2pPing()
    await publisher.start()
  publisher.subscribe((kind: PubsubSub, topic: shard), dummyHandler).expect(
    "Failed to sub publisher"
  )

  await publisher.connectToNodes(@[storeNodePeerInfo])

  var meshFormed = false
  for _ in 0 ..< 50:
    if publisher.wakuRelay.getNumPeersInMesh(shard).valueOr(0) > 0:
      meshFormed = true
      break
    await sleepAsync(100.milliseconds)
  if not meshFormed:
    raiseAssert "publisher<->store relay mesh did not form in time"

  # Started, without peers. The message is archived after the service start
  # and before the subscription, inside the range to catch up.
  var subscriber: LogosDelivery
  lockNewGlobalBrokerContext:
    var conf = createApiNodeConf(numShards, mode)
    conf.localStoragePath = root
    if localStore:
      conf.store = Opt.some(true)
      conf.storeMessageDbUrl = "sqlite://" & (root / "local-store.sqlite3")
    subscriber = (await LogosDelivery.new(nodeConf(conf, messaging))).expect(
      "Failed to create subscriber"
    )
    subscriber.shortenIntervals()
    if activityWriteInterval.isSome():
      subscriber.messagingClient.recvService.activityWriteInterval =
        activityWriteInterval.get()
    (await subscriber.start()).expect("Failed to start subscriber")

  let missedPayload = "This message was missed".toBytes()
  let missedMsg = WakuMessage(
    payload: missedPayload, contentTopic: testTopic, version: 0, timestamp: now()
  )
  discard (await publisher.publish(Opt.some(shard), missedMsg)).expect(
    "Publish missed msg failed"
  )
  # Relay publish returns before the archive write. The subscription wakes
  # the catch-up, so the message must be archived first.
  block waitArchive:
    for _ in 0 ..< 50:
      let query = archive_common.ArchiveQuery(
        includeData: false, contentTopics: @[testTopic], pubsubTopic: Opt.some(shard)
      )
      let res = await storeNode.wakuArchive.findMessages(query)
      if res.isOk() and res.get().hashes.len > 0:
        break waitArchive
      await sleepAsync(100.milliseconds)
    raiseAssert "Message was not archived in time"

  let events = newReceiveEventListenerManager(subscriber.waku.brokerCtx, 1)
  if knowStorePeer:
    subscriber.waku.node.peerManager.addServicePeer(storeNodePeerInfo, WakuStoreCodec)
  if subscribe:
    (await subscriber.messagingClient.subscribe(testTopic)).expect(
      "Failed to subscribe"
    )

  return TestNetwork(
    storeNode: storeNode,
    archiveDriver: archiveDriver,
    publisher: publisher,
    subscriber: subscriber,
    storeNodePeerInfo: storeNodePeerInfo,
    missedPayload: missedPayload,
    events: events,
    ownedRoot: ownedRoot,
  )

proc teardown(net: TestNetwork) {.async.} =
  if not isNil(net.events):
    await net.events.teardown()
    net.events = nil
  if not isNil(net.subscriber):
    (await net.subscriber.stop()).expect("Failed to stop subscriber")
    net.subscriber = nil
  if not isNil(net.publisher):
    await net.publisher.stop()
    net.publisher = nil
  if not isNil(net.storeNode):
    await net.storeNode.stop()
    net.storeNode = nil
  if net.ownedRoot.len > 0:
    removeDir(net.ownedRoot)
    net.ownedRoot = ""

const RestartTopic = ContentTopic("/waku/2/recv-process-restart/proto")
const TestShard = PubsubTopic("/waku/2/rs/3/0")
const SecondShard = PubsubTopic("/waku/2/rs/3/1") ## shard 1 of a two-shard network
const OfflineCount = 105 ## archived between two subscriber processes, two Store pages
const RestartDelayExtra = chronos.milliseconds(500)
  ## the restarted process's `delayExtra`
const Hour = chronos.hours(1).nanos
const Minute = chronos.minutes(1).nanos

proc runRestartedReceiver(
    storageRoot, storePeer: string, expectedCount: int, backfillEnabled: bool
) {.async.} =
  ## Child process on the same root with a new identity. It must recover
  ## exactly `expectedCount` messages by automatic catch-up. When it expects
  ## messages, every archived message must be among them.
  var conf = createApiNodeConf()
  conf.localStoragePath = storageRoot
  let subscriber = (
    await LogosDelivery.new(nodeConf(conf, backfillOverrides(backfillEnabled)))
  ).expect("new process subscriber")
  subscriber.shortenIntervals()
  subscriber.messagingClient.recvService.delayExtra = RestartDelayExtra
  let events =
    newReceiveEventListenerManager(subscriber.waku.brokerCtx, max(expectedCount, 1))
  (await subscriber.start()).expect("start new process subscriber")
  subscriber.waku.node.peerManager.addServicePeer(
    parsePeerInfo(storePeer).get(), WakuStoreCodec
  )
  (await subscriber.messagingClient.subscribe(RestartTopic)).expect("resubscribe")
  if expectedCount == 0:
    await sleepAsync(3.seconds)
  else:
    doAssert await events.waitForEvents(TestTimeout)
    # Wait for a possible over-delivery.
    await sleepAsync(1.seconds)
  doAssert events.receivedMessages.len == expectedCount,
    "expected " & $expectedCount & " recovered messages, got " &
      $events.receivedMessages.len
  let payloads = events.receivedMessages.mapIt(string.fromBytes(it.payload)).toHashSet()
  doAssert payloads.len == expectedCount
  if expectedCount > 0:
    for i in 0 ..< OfflineCount:
      doAssert "process-offline-" & $i in payloads
  # Everything a restarted process recovers comes from Store.
  doAssert events.receivedSources.allIt(it == MessageSource.History),
    "recovered messages must be reported as history, got " & $events.receivedSources
  await events.teardown()
  (await subscriber.stop()).expect("stop new process subscriber")

if paramCount() == 5 and paramStr(1) == "--recv-restart-child":
  waitFor runRestartedReceiver(
    paramStr(2), paramStr(3), parseInt(paramStr(4)), paramStr(5) == "enabled"
  )
  quit(QuitSuccess)

proc archiveOffline(net: TestNetwork) {.async.} =
  ## Archives `OfflineCount` messages while no subscriber process runs.
  for i in 0 ..< OfflineCount:
    await net.storeNode.wakuArchive.handleMessage(
      TestShard,
      WakuMessage(
        payload: ("process-offline-" & $i).toBytes(),
        contentTopic: RestartTopic,
        timestamp: now(),
      ),
    )

proc runRestartedProcess(
    net: TestNetwork, storageRoot: string, expectedCount: int, backfillEnabled = true
): Future[bool] {.async.} =
  ## Runs a new process on `storageRoot` until it recovers `expectedCount`
  ## messages. True when the process ends in time and reports success.
  let storePeer =
    $net.storeNodePeerInfo.addrs[0] & "/p2p/" & $net.storeNodePeerInfo.peerId
  let child = startProcess(
    getAppFilename(),
    args = @[
      "--recv-restart-child",
      storageRoot,
      storePeer,
      $expectedCount,
      if backfillEnabled: "enabled" else: "disabled",
    ],
    options = {poParentStreams},
  )
  defer:
    if child.running():
      child.terminate()
    child.close()
  let deadline = Moment.now() + TestTimeout + 10.seconds
  while child.running() and Moment.now() < deadline:
    await sleepAsync(50.milliseconds)
  if child.running():
    return false # the restarted process did not finish in time
  return child.waitForExit() == 0

proc waitForArchived(
    net: TestNetwork, topic: ContentTopic, count: int
): Future[bool] {.async.} =
  ## True when the archive holds `count` messages of `topic`.
  for _ in 0 ..< 100:
    let query = archive_common.ArchiveQuery(
      includeData: false, contentTopics: @[topic], pubsubTopic: Opt.some(TestShard)
    )
    let res = await net.storeNode.wakuArchive.findMessages(query)
    if res.isOk() and res.get().hashes.len >= count:
      return true
    await sleepAsync(100.milliseconds)
  return false

proc archiveAt(
    net: TestNetwork,
    topic: ContentTopic,
    at: Timestamp,
    text: string,
    shard = TestShard,
): Future[WakuMessage] {.async.} =
  ## Puts a message with the timestamp `at` directly into the archive driver,
  ## because the archive rejects a timestamp outside its tolerance.
  let msg = WakuMessage(payload: text.toBytes(), contentTopic: topic, timestamp: at)
  (await net.archiveDriver.put(computeMessageHash(shard, msg), shard, msg)).expect(
    "archive put"
  )
  return msg

proc waitForAdvance(
    job: Job, past: Timestamp, within = 15.seconds
): Future[Opt[Timestamp]] {.async.} =
  ## The stored hint, after it moves past `past`, or none when it does not
  ## move within `within`. A catch-up that learns its Store peer after the
  ## subscription waits one retry period first.
  let deadline = Moment.now() + within
  while Moment.now() < deadline:
    let stored = (await job.readRecoveryHint()).valueOr(Opt.none(Timestamp))
    if stored.isSome() and stored.get() > past:
      return stored
    await sleepAsync(100.milliseconds)
  return Opt.none(Timestamp)

proc knowStorePeer(net: TestNetwork) =
  ## Registers the store node as a service peer. The Store client dials it on demand.
  net.subscriber.waku.node.peerManager.addServicePeer(
    net.storeNodePeerInfo, WakuStoreCodec
  )

proc startSubscriber(
    net: TestNetwork, root: string, storePeer: RemotePeerInfo
) {.async.} =
  ## Starts a new subscriber on `root`, with `storePeer` as a known Store peer
  ## and a short `delayExtra`. The new one subscribes nothing.
  var subscriber: LogosDelivery
  lockNewGlobalBrokerContext:
    var conf = createApiNodeConf()
    conf.localStoragePath = root
    subscriber = (await LogosDelivery.new(nodeConf(conf, backfillOverrides()))).expect(
      "create the restarted subscriber"
    )
    subscriber.shortenIntervals()
    subscriber.messagingClient.recvService.delayExtra = RestartDelayExtra
    (await subscriber.start()).expect("start the restarted subscriber")
  net.subscriber = subscriber
  net.events = newReceiveEventListenerManager(subscriber.waku.brokerCtx, 1)
  subscriber.waku.node.peerManager.addServicePeer(storePeer, WakuStoreCodec)

proc restartSubscriber(net: TestNetwork, root: string) {.async.} =
  ## Stops the subscriber and starts a new one on `root` (see
  ## `startSubscriber`), with the store node as its Store peer.
  await net.events.teardown()
  (await net.subscriber.stop()).expect("stop the subscriber")
  await net.startSubscriber(root, net.storeNodePeerInfo)

proc waitForRecord(
    job: Job, topic: BackfillTopic, owed: bool
): Future[Opt[TopicRecord]] {.async.} =
  ## The stored record of `topic`, once it exists with `owed`, or none when it
  ## does not within five seconds.
  for _ in 0 ..< 100:
    let records =
      (await job.readTopicRecords()).valueOr(newSeq[(BackfillTopic, TopicRecord)]())
    for (stored, record) in records:
      if stored == topic and record.owed == owed:
        return Opt.some(record)
    await sleepAsync(50.milliseconds)
  return Opt.none(TopicRecord)

proc unchangedStoreQueryCount(): Future[float64] {.async.} =
  ## The count of Store queries in this process, once no query arrives for
  ## one second, or the last count after `TestTimeout`.
  var count = logos_delivery_store_queries.value()
  let deadline = Moment.now() + TestTimeout
  while Moment.now() < deadline:
    await sleepAsync(1.seconds)
    let next = logos_delivery_store_queries.value()
    if next == count:
      return count
    count = next
  return count

proc joinMesh(net: TestNetwork, peer: RemotePeerInfo): Future[bool] {.async.} =
  ## Connects the subscriber to `peer`. True when it has a relay mesh within
  ## ten seconds, so a message the publisher relays reaches the subscriber
  ## live. Polls the mesh, because the Store dial can connect them before this.
  await net.subscriber.waku.node.connectToNodes(@[peer])
  for _ in 0 ..< 100:
    if net.subscriber.waku.node.wakuRelay.getNumPeersInMesh(TestShard).valueOr(0) > 0:
      return true
    await sleepAsync(100.milliseconds)
  return false

proc joinMesh(net: TestNetwork): Future[bool] {.async.} =
  ## Joins the relay mesh through the store node.
  return await net.joinMesh(net.storeNodePeerInfo)

proc tunnel(
    net: TestNetwork, topic: ContentTopic, offline: Future[bool]
): Future[Opt[WakuMessage]] {.async.} =
  ## Disconnects the subscriber from the store node, waits for `offline`, then
  ## puts one message straight into the archive. Direct insertion keeps Store
  ## the only path, because the relay cache can give a published message at
  ## reconnection. None when `offline` or the archive write does not come.
  await net.subscriber.waku.node.disconnectNode(net.storeNodePeerInfo)
  if not await offline:
    return Opt.none(WakuMessage)
  let msg = WakuMessage(
    payload: "archived in the tunnel".toBytes(), contentTopic: topic, timestamp: now()
  )
  await net.storeNode.wakuArchive.handleMessage(TestShard, msg)
  if not await net.waitForArchived(topic, 2):
    return Opt.none(WakuMessage)
  return Opt.some(msg)

proc publishLive(net: TestNetwork, topic: ContentTopic, text: string) {.async.} =
  ## A message the subscriber receives live, through the relay mesh.
  let msg = WakuMessage(payload: text.toBytes(), contentTopic: topic, timestamp: now())
  discard (await net.publisher.publish(Opt.some(TestShard), msg)).expect("publish live")

proc requiredAnonymity(): MessagingClientConf =
  ## A `Required` node with an empty mix pool, so `ConnectionStatus` stays
  ## `Disconnected`.
  return MessagingClientConf(
    backfillEnabled: Opt.some(true), anonymityLevel: Opt.some(AnonymityLevel.Required)
  )

proc waitForProtocolHealth(
    waku: Waku, protocol: WakuProtocol, health: HealthStatus
): Future[bool] {.async.} =
  ## True once the kernel reports `protocol` at `health`, false after
  ## `TestTimeout`. Registers the listener, then reads the stored status.
  let future = newFuture[void]("waitForProtocolHealth")
  let handler: EventProtocolHealthChangeListenerProc = proc(
      e: EventProtocolHealthChange
  ) {.async: (raises: []), gcsafe.} =
    if not future.finished and e.protocolHealth.protocol == $protocol and
        e.protocolHealth.health == health:
      future.complete()
  let handle = EventProtocolHealthChange.listen(waku.brokerCtx, handler).valueOr:
    return false
  try:
    if waku.reportedProtocolHealth(protocol).health == health:
      return true
    return await future.withTimeout(TestTimeout)
  finally:
    await EventProtocolHealthChange.dropListener(waku.brokerCtx, handle)

proc noopRelayHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
  discard

proc newFilterServiceNode(shardId: uint16): Future[WakuNode] {.async.} =
  ## A started relay and filter service node that advertises shard `shardId` only.
  let shard = PubsubTopic("/waku/2/rs/" & $TestClusterId & "/" & $shardId)
  var node: WakuNode
  lockNewGlobalBrokerContext:
    node = newTestWakuNode(generateSecp256k1Key())
    node.mountMetadata(TestClusterId, @[shardId]).expect("mount metadata")
    (await node.mountRelay()).expect("mount relay")
    await node.mountFilter()
    await node.mountLibp2pPing()
    await node.start()
  let subscribed = node.subscribe((kind: PubsubSub, topic: shard), noopRelayHandler)
  if subscribed.isErr():
    await node.stop()
    raiseAssert "filter service node subscribe: " & subscribed.error
  return node

proc newStoreOnlyNode(): Future[(WakuNode, ArchiveDriver)] {.async.} =
  ## A started node that serves Store from its own archive, with no relay.
  var node: WakuNode
  var driver: ArchiveDriver
  lockNewGlobalBrokerContext:
    node = newTestWakuNode(generateSecp256k1Key())
    node.mountMetadata(TestClusterId, @[0'u16]).expect("mount metadata")
    driver = newSqliteArchiveDriver()
    node.mountArchive(driver).expect("mount archive")
    await node.mountStore()
    await node.start()
  return (node, driver)

proc filterSubscriptionHealthy(net: TestNetwork, shard = TestShard): bool =
  ## True when the subscription manager reports `shard`'s filter subscription healthy.
  let shardHealth = RequestEdgeShardHealth.request(net.subscriber.waku.brokerCtx, shard).valueOr:
    return false
  return
    shardHealth.health in
    {TopicHealth.MINIMALLY_HEALTHY, TopicHealth.SUFFICIENTLY_HEALTHY}

proc waitForFilterSubscriptionHealth(
    net: TestNetwork, healthy: bool, shard = TestShard
): Future[bool] {.async.} =
  ## True once the subscription manager reports `shard`'s filter subscription
  ## at `healthy`, false after `TestTimeout`. Registers the listener, then
  ## reads the stored health.
  let future = newFuture[void]("waitForFilterSubscriptionHealth")
  let handler: EventShardTopicHealthChangeListenerProc = proc(
      e: EventShardTopicHealthChange
  ) {.async: (raises: []), gcsafe.} =
    if future.finished or e.topic != shard:
      return
    let isHealthy =
      e.health in {TopicHealth.MINIMALLY_HEALTHY, TopicHealth.SUFFICIENTLY_HEALTHY}
    if isHealthy == healthy:
      future.complete()
  let brokerCtx = net.subscriber.waku.brokerCtx
  let handle = EventShardTopicHealthChange.listen(brokerCtx, handler).valueOr:
    return false
  try:
    if net.filterSubscriptionHealthy(shard) == healthy:
      return true
    return await future.withTimeout(TestTimeout)
  finally:
    await EventShardTopicHealthChange.dropListener(brokerCtx, handle)

proc bringOnline(net: TestNetwork): Future[bool] {.async.} =
  ## Connects the subscriber to the store node. True when the kernel reports
  ## relay READY.
  let relayReady = waitForProtocolHealth(
    net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
  )
  await net.subscriber.waku.node.connectToNodes(@[net.storeNodePeerInfo])
  return await relayReady

## Few multi-phase cases. Each `test` block costs three GC-tracked globals, and
## the refc runtime caps the waku test binary at 3500.

suite "Messaging API, Receive Service (store recovery)":
  asyncTest "a new process recovers what was archived while it was down":
    # Phase 1: the first session keeps a record for the topic. The next process
    # recovers the setup message and all messages archived while stopped,
    # across two Store pages.
    block:
      let root = createTempDir("recv-api-process-", "")
      defer:
        removeDir(root)
      let net = await setupNetwork(RestartTopic, root)
      defer:
        await net.teardown()
      (await net.subscriber.stop()).expect("stop previous session")
      net.subscriber = nil
      await net.archiveOffline()
      # Separates these messages from the child's start time.
      await sleepAsync(1.seconds)
      check await net.runRestartedProcess(root, OfflineCount + 1)

    # Phase 2: disabled, only the reconnection check runs, and its `delayExtra`
    # lookback is waited out first, so the child retrieves nothing. The saved
    # timestamp stays for a later run.
    block:
      let root = createTempDir("recv-api-disabled-", "")
      defer:
        removeDir(root)
      let net = await setupNetwork(RestartTopic, root)
      defer:
        await net.teardown()
      (await net.subscriber.stop()).expect("stop previous session")
      net.subscriber = nil
      await net.archiveOffline()
      await sleepAsync(RestartDelayExtra + 1.seconds)
      check await net.runRestartedProcess(root, 0, backfillEnabled = false)
      check await net.runRestartedProcess(root, OfflineCount + 1)

    # Phase 3: a first run keeps a live record for a topic from its first
    # subscription. It writes no hint while no message arrives.
    block:
      let root = createTempDir("recv-api-first-run-", "")
      defer:
        removeDir(root)
      var conf = createApiNodeConf()
      conf.localStoragePath = root
      var node: LogosDelivery
      lockNewGlobalBrokerContext:
        node = (await LogosDelivery.new(nodeConf(conf, backfillOverrides()))).expect(
          "create node"
        )
        node.shortenIntervals()
        (await node.start()).expect("start node")
      let topic = ContentTopic("/waku/2/recv-first-run/proto")
      let subscribedAt = now()
      (await node.messagingClient.subscribe(topic)).expect("subscribe")
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      let record = await job.waitForRecord((TestShard, topic), owed = false)
      check record.isSome()
      let recordAt = record.get(TopicRecord()).at
      check recordAt >= subscribedAt and recordAt <= now()
      check (await job.waitForAdvance(0, within = chronos.seconds(1))).isNone()
      (await node.stop()).expect("stop node")

    # Phase 4: a first subscription gets no history, and the catch-up sends
    # no Store query for it. The node is online before the subscription, so
    # no reconnection check runs, and nothing else in this phase queries Store.
    block:
      let owedTopic = ContentTopic("/waku/2/recv-first-subscription-owed/proto")
      let newTopic = ContentTopic("/waku/2/recv-first-subscription-new/proto")
      let net = await setupNetwork(owedTopic)
      defer:
        await net.teardown()
      let events = net.events
      check await events.waitForEvents(TestTimeout) # the setup message
      check await net.joinMesh()
      check await net.subscriber.caughtUp()
      let before = await net.archiveAt(
        newTopic, now() - Minute, "archived before the first subscription"
      )
      let queries = await unchangedStoreQueryCount()
      (await net.subscriber.messagingClient.subscribe(newTopic)).expect("subscribe")
      check await net.subscriber.caughtUp()
      check (await unchangedStoreQueryCount()) == queries
      # A live message arrives, so the subscription works.
      events.targetCount = 2
      events.receivedEvent.clear()
      await net.publishLive(newTopic, "live after the first subscription")
      check await events.waitForEvents(TestTimeout)
      check await events.noMoreEvents()
      check events.payloads() ==
        @["This message was missed", "live after the first subscription"]

    # Phase 5: no hint while nobody can be asked. When a Store peer is known,
    # the catch-up delivers the setup message, and that receipt writes the
    # hint.
    block:
      let root = createTempDir("recv-api-advance-", "")
      defer:
        removeDir(root)
      let topic = ContentTopic("/waku/2/recv-advance/proto")
      let net = await setupNetwork(topic, root, knowStorePeer = false)
      defer:
        await net.teardown()
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      check (await job.waitForAdvance(0, within = 1500.milliseconds)).isNone()
      net.knowStorePeer()
      let written =
        await job.waitForAdvance(0, within = CatchUpRetryPeriod + 15.seconds)
      check written.isSome() and written.get(0) < now()

    # Phase 6: after the catch-up, a received message advances the hint to
    # its receipt time, at most once per `activityWriteInterval`.
    block:
      let root = createTempDir("recv-api-live-", "")
      defer:
        removeDir(root)
      let topic = ContentTopic("/waku/2/recv-live/proto")
      # The second live message must land inside the interval.
      let net =
        await setupNetwork(topic, root, activityWriteInterval = Opt.some(3.seconds))
      defer:
        await net.teardown()
      let events = net.events
      let activityWriteInterval =
        net.subscriber.messagingClient.recvService.activityWriteInterval
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      check await net.subscriber.caughtUp()
      check await events.waitForEvents(TestTimeout) # the setup message
      # The setup message's receipt writes the first value past its timestamp.
      let setupAt = events.receivedMessages.mapIt(it.timestamp).foldl(max(a, b), 0'i64)
      let setupWrite = (await job.waitForAdvance(setupAt)).get(0)
      check setupWrite > 0 and setupWrite <= now()
      check await net.joinMesh()
      await sleepAsync(activityWriteInterval) # past the throttle of that write
      events.targetCount = 2
      events.receivedEvent.clear()
      let beforeLive = now()
      await net.publishLive(topic, "live one")
      check await events.waitForEvents(TestTimeout)
      let afterFirst = (await job.waitForAdvance(setupWrite)).get(0)
      check afterFirst >= beforeLive
      # A second message inside the interval leaves the hint as is.
      events.targetCount = 3
      events.receivedEvent.clear()
      await net.publishLive(topic, "live two")
      check await events.waitForEvents(TestTimeout)
      check (await job.waitForAdvance(afterFirst, within = 1500.milliseconds)).isNone()
      # After the interval the next one writes again.
      await sleepAsync(activityWriteInterval)
      events.targetCount = 4
      events.receivedEvent.clear()
      await net.publishLive(topic, "live three")
      check await events.waitForEvents(TestTimeout)
      check (await job.waitForAdvance(afterFirst)).isSome()
      # The setup message came from Store, the published ones came live.
      check events.receivedSources ==
        @[
          MessageSource.History, MessageSource.Live, MessageSource.Live,
          MessageSource.Live,
        ]

    # Phase 7: the hint moves on a live receipt while the catch-up waits on a
    # Store peer that it cannot get to. The catch-up continues to retry behind it.
    block:
      let root = createTempDir("recv-api-dead-peer-", "")
      defer:
        removeDir(root)
      let topic = ContentTopic("/waku/2/recv-dead-peer/proto")
      let net = await setupNetwork(topic, root, knowStorePeer = false)
      defer:
        await net.teardown()
      let events = net.events
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      check (await job.readRecoveryHint()) ==
        Result[Opt[Timestamp], string].ok(Opt.none(Timestamp))
      # An address that nobody answers, under a peer id of its own.
      let deadId = PeerId
        .init(generateSecp256k1Key().getPublicKey().expect("public key"))
        .expect("peer id")
      let dead =
        parsePeerInfo("/ip4/10.255.255.1/tcp/60000/p2p/" & $deadId).expect("dead peer")
      net.subscriber.waku.node.peerManager.addServicePeer(dead, WakuStoreCodec)
      # Relay through the publisher only. Live delivery works, and Store does not.
      check await net.joinMesh(net.publisher.peerInfo.toRemotePeerInfo())
      await net.publishLive(topic, "live while Store is dead")
      check await events.waitForEvents(TestTimeout)
      let moved = await job.waitForAdvance(0)
      check moved.isSome() and moved.get(0) <= now()
      let liveIdx = events.receivedMessages.mapIt(string.fromBytes(it.payload)).find(
          "live while Store is dead"
        )
      check liveIdx >= 0 and events.receivedSources[liveIdx] == MessageSource.Live

  asyncTest "recv_service recovers a missed message through a known Store peer":
    # Phase 1: the catch-up dials the known Store peer.
    block:
      let history = $MessageSource.History
      let countBefore = logos_delivery_recv_messages.value([history])
      let bytesBefore = logos_delivery_recv_message_bytes.value([history])
      let net = await setupNetwork(ContentTopic("/waku/2/recv-test/proto"))
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 1
      if eventManager.receivedMessages.len > 0:
        check eventManager.receivedMessages[0].payload == net.missedPayload
        check eventManager.receivedSources[0] == MessageSource.History
      check logos_delivery_recv_messages.value([history]) == countBefore + 1
      check logos_delivery_recv_message_bytes.value([history]) ==
        bytesBefore + float64(net.missedPayload.len)

    # Phase 2: a Store peer learned after the subscription, by connecting to
    # it, is asked as soon as the connection is reported.
    block:
      let net = await setupNetwork(
        ContentTopic("/waku/2/recv-learned-peer-test/proto"), knowStorePeer = false
      )
      defer:
        await net.teardown()
      let eventManager = net.events
      check await net.bringOnline()
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 1
      if eventManager.receivedMessages.len > 0:
        check eventManager.receivedMessages[0].payload == net.missedPayload

    # Phase 3: storage closed under a running node ends the catch-up with a
    # warning when the connection wakes it. The node continues to run, and
    # the reconnection check delivers on its own.
    block:
      let net = await setupNetwork(
        ContentTopic("/waku/2/recv-storage-lost/proto"), knowStorePeer = false
      )
      defer:
        await net.teardown()
      GetPersistency
        .request(net.subscriber.waku.brokerCtx)
        .expect("persistency")
        .closeJob(MessagingJobId)
      check await net.bringOnline()
      check await net.subscriber.catchUpEnded()
      check net.subscriber.isRunning()

    # Phase 4: the hot path. After the catch-up, the reconnection check
    # recovers a message archived during a tunnel (offline, then online).
    block:
      let topic = ContentTopic("/waku/2/recv-tunnel-test/proto")
      let net = await setupNetwork(topic)
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout) # the setup message
      check await net.joinMesh() # the Store dial can connect them before this
      # the kernel must report relay READY before the tunnel waits for NOT_READY
      check await waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      let tunnelled = await net.tunnel(
        topic,
        waitForProtocolHealth(
          net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
        ),
      )
      check tunnelled.isSome()
      let gapMsg = tunnelled.get(WakuMessage())
      eventManager.targetCount = 2
      eventManager.receivedEvent.clear()
      await net.subscriber.waku.node.connectToNodes(@[net.storeNodePeerInfo])
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 2 and
        eventManager.receivedMessages[^1].payload == gapMsg.payload
      # The setup message and the gap message were both recovered from Store.
      check eventManager.receivedSources ==
        @[MessageSource.History, MessageSource.History]

    # Phase 5: a new topic that the app subscribes while the node is offline
    # gets the messages from after its subscription through the reconnection
    # check, and none from before it. This limit applies with the catch-up on
    # and off.
    for enabled in [true, false]:
      let topicA = ContentTopic("/waku/2/recv-offline-a/proto")
      let topicB = ContentTopic("/waku/2/recv-offline-b/proto")
      let net = await setupNetwork(topicA, messaging = backfillOverrides(enabled))
      defer:
        await net.teardown()
      let events = net.events
      net.subscriber.messagingClient.recvService.delayExtra = RestartDelayExtra
      if enabled:
        check await events.waitForEvents(TestTimeout) # the setup message
      check await net.joinMesh()
      check await waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      check await net.subscriber.caughtUp()
      # The relay peer leaves the shard and stays connected, so the node
      # stays offline until the peer comes back to the shard.
      let offline = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
      )
      net.storeNode.unsubscribe((kind: PubsubSub, topic: TestShard)).expect(
        "store node unsubscribe"
      )
      check await offline
      let before = await net.archiveAt(topicB, now(), "B before its subscription")
      await sleepAsync(RestartDelayExtra + 500.milliseconds)
      (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B")
      let after = await net.archiveAt(topicB, now(), "B after its subscription")
      net.storeNode
        .subscribe((kind: PubsubSub, topic: TestShard), noopRelayHandler)
        .expect("store node subscribe")
      check (await events.waitForPayloadSource(string.fromBytes(after.payload))) ==
        Opt.some(MessageSource.History)
      # One check delivers the two messages of B, so `before` arrives with
      # `after` or never.
      check events.receivedMessages.filterIt(it.contentTopic == topicB).mapIt(
        string.fromBytes(it.payload)
      ) == @[string.fromBytes(after.payload)]
      if enabled:
        # With the catch-up off, the setup message can arrive live or from
        # the first reconnection check.
        check await events.noMoreEvents()
        check events.payloads() ==
          @["This message was missed", string.fromBytes(after.payload)]
        check events.receivedSources == @[MessageSource.History, MessageSource.History]

    # Phase 6: the node comes back online while a reconnection check runs.
    # That check runs one more time when it ends, so the new gap gets a check
    # too. The Store peer is not a relay peer, so its query stays in flight
    # while the relay peer goes away and comes back. The relay cache can give
    # the setup message live, so the checks look at the gap message only.
    block:
      let topic = ContentTopic("/waku/2/recv-online-during-check/proto")
      let net =
        await setupNetwork(topic, knowStorePeer = false, history = Opt.none(Duration))
      defer:
        await net.teardown()
      let events = net.events
      net.subscriber.messagingClient.recvService.delayExtra = RestartDelayExtra
      let (storeOnly, storeOnlyDriver) = await newStoreOnlyNode()
      defer:
        await storeOnly.stop()
      net.subscriber.waku.node.peerManager.addServicePeer(
        storeOnly.peerInfo.toRemotePeerInfo(), WakuStoreCodec
      )
      # The relay peer is the publisher. It serves no Store, so the Store-only
      # node is the only Store peer.
      let relayPeer = net.publisher.peerInfo.toRemotePeerInfo()
      check await net.joinMesh(relayPeer)
      check await waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      discard await unchangedStoreQueryCount() # the first reconnection check ended
      # The Store peer answers each query only after `reply`.
      let requested = newAsyncEvent()
      let reply = newAsyncEvent()
      let storeHandler = storeOnly.wakuStore.requestHandler
      storeOnly.wakuStore.requestHandler = proc(
          request: StoreQueryRequest
      ): Future[StoreQueryResult] {.async.} =
        requested.fire()
        await reply.wait()
        return await storeHandler(request)
      # The node goes offline and comes back online. A check starts and waits
      # in the Store peer.
      var offline = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
      )
      await net.subscriber.waku.node.disconnectNode(relayPeer)
      check await offline
      var online = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      # GossipSub delays a new mesh with a peer for some time after a
      # disconnect. Relay READY needs a connected peer only, so the test
      # connects and waits for it.
      await net.subscriber.waku.node.connectToNodes(@[relayPeer])
      check await online
      check await requested.wait().withTimeout(TestTimeout)
      # Offline and online again while that check waits. The message comes
      # after the window of that check.
      offline = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
      )
      await net.subscriber.waku.node.disconnectNode(relayPeer)
      check await offline
      await sleepAsync(RestartDelayExtra + 500.milliseconds)
      let gapMsg = WakuMessage(
        payload: "archived during the second gap".toBytes(),
        contentTopic: topic,
        timestamp: now(),
      )
      (
        await storeOnlyDriver.put(
          computeMessageHash(TestShard, gapMsg), TestShard, gapMsg
        )
      ).expect("archive put")
      online = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      await net.subscriber.waku.node.connectToNodes(@[relayPeer])
      check await online
      reply.fire()
      check (await events.waitForPayloadSource("archived during the second gap")) ==
        Opt.some(MessageSource.History)

  asyncTest "the receive service follows its receive peers, not ConnectionStatus":
    ## Under #4238, `ConnectionStatus` stays `Disconnected` in every phase.
    # Phase 1: the reconnection backfill runs when the relay peer is back. The
    # catch-up has nothing left to do, so only the reconnection backfill can
    # deliver the tunnel message.
    block:
      let topic = ContentTopic("/waku/2/recv-required-tunnel/proto")
      let net = await setupNetwork(topic, messaging = requiredAnonymity())
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout) # the setup message
      check await net.joinMesh()
      # the kernel must report relay READY before the tunnel waits for NOT_READY
      check await waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      check await net.subscriber.caughtUp()
      # mix is not ready, so #4238 holds `ConnectionStatus` at `Disconnected`
      check net.subscriber.waku.reportedProtocolHealth(WakuProtocol.MixProtocol).health !=
        HealthStatus.READY
      let tunnelled = await net.tunnel(
        topic,
        waitForProtocolHealth(
          net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
        ),
      )
      check tunnelled.isSome()
      let gapMsg = tunnelled.get(WakuMessage())
      eventManager.targetCount = 2
      eventManager.receivedEvent.clear()
      await net.subscriber.waku.node.connectToNodes(@[net.storeNodePeerInfo])
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 2 and
        eventManager.receivedMessages[^1].payload == gapMsg.payload
      # mix is still not ready
      check net.subscriber.waku.reportedProtocolHealth(WakuProtocol.MixProtocol).health !=
        HealthStatus.READY

    # Phase 2: the catch-up queries a Store peer as soon as the kernel reports
    # its connection. A message from an hour before the start is
    # outside the reconnection backfill's window, so only the catch-up can
    # deliver it.
    block:
      let topic = ContentTopic("/waku/2/recv-required-learned-peer/proto")
      let net = await setupNetwork(
        topic, knowStorePeer = false, messaging = requiredAnonymity()
      )
      defer:
        await net.teardown()
      let eventManager = net.events
      let oldMsg = await net.archiveAt(topic, now() - Hour, "archived an hour ago")
      eventManager.targetCount = 2
      await net.subscriber.waku.node.connectToNodes(@[net.storeNodePeerInfo])
      check await eventManager.waitForEvents(CatchUpRetryPeriod - 5.seconds)
      check eventManager.receivedMessages.mapIt(it.payload).contains(oldMsg.payload)

    # Phase 3: the relay peer stays connected, unsubscribes from the shard,
    # then subscribes again. The peer serves filter too, so the filter
    # client's protocol health stays READY through the outage.
    block:
      let topic = ContentTopic("/waku/2/recv-required-resubscribe/proto")
      let net =
        await setupNetwork(topic, messaging = requiredAnonymity(), remoteFilter = true)
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout) # the setup message
      check await net.joinMesh()
      # the kernel must report relay READY before the test waits for NOT_READY
      check await waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.READY
      )
      check await net.subscriber.caughtUp()
      check net.subscriber.waku.reportedProtocolHealth(
        WakuProtocol.FilterClientProtocol
      ).health == HealthStatus.READY
      let offline = waitForProtocolHealth(
        net.subscriber.waku, WakuProtocol.RelayProtocol, HealthStatus.NOT_READY
      )
      net.storeNode.unsubscribe((kind: PubsubSub, topic: TestShard)).expect(
        "store node unsubscribe"
      )
      check await offline
      let gapMsg = await net.archiveAt(topic, now(), "archived while unsubscribed")
      eventManager.targetCount = 2
      eventManager.receivedEvent.clear()
      net.storeNode
        .subscribe((kind: PubsubSub, topic: TestShard), noopRelayHandler)
        .expect("store node subscribe")
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 2 and
        eventManager.receivedMessages[^1].payload == gapMsg.payload

    # Phase 4: Edge mode. The filter service drops the subscription while the
    # connection stays. The subscription manager notices on its next ping and
    # subscribes again. The test archives the message after the ping, because
    # the offline window starts there.
    block:
      let topic = ContentTopic("/waku/2/recv-required-edge-filter/proto")
      let net = await setupNetwork(
        topic,
        messaging = requiredAnonymity(),
        mode = LogosDeliveryMode.Edge,
        remoteFilter = true,
      )
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout) # the setup message
      check await net.waitForFilterSubscriptionHealth(healthy = true)
      check await net.subscriber.caughtUp()
      # The gap message must be archived before the resubscription's Store check.
      net.subscriber.waku.node.subscriptionManager.edgeFilterSubLoopDebounce = 1.seconds
      let offline = net.waitForFilterSubscriptionHealth(healthy = false)
      await net.storeNode.wakuFilter.subscriptions.removePeer(
        net.subscriber.waku.node.switch.peerInfo.peerId
      )
      check await offline
      check net.subscriber.waku.reportedProtocolHealth(
        WakuProtocol.FilterClientProtocol
      ).health == HealthStatus.READY
      let gapMsg = await net.archiveAt(
        topic, now(), "archived while the filter subscription was gone"
      )
      eventManager.targetCount = 2
      eventManager.receivedEvent.clear()
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 2 and
        eventManager.receivedMessages[^1].payload == gapMsg.payload

    # Phase 5: a local Store keeps the Store client's protocol health READY
    # with no remote peer. The catch-up queries the first remote Store peer
    # as soon as the kernel reports its identify.
    block:
      let topic = ContentTopic("/waku/2/recv-required-local-store/proto")
      let net = await setupNetwork(
        topic, knowStorePeer = false, messaging = requiredAnonymity(), localStore = true
      )
      defer:
        await net.teardown()
      let eventManager = net.events
      check net.subscriber.waku.reportedProtocolHealth(WakuProtocol.StoreClientProtocol).health ==
        HealthStatus.READY
      let oldMsg = await net.archiveAt(topic, now() - Hour, "archived an hour ago too")
      eventManager.targetCount = 2
      await net.subscriber.waku.node.connectToNodes(@[net.storeNodePeerInfo])
      check await eventManager.waitForEvents(CatchUpRetryPeriod - 5.seconds)
      check eventManager.receivedMessages.mapIt(it.payload).contains(oldMsg.payload)

    # Phase 6: Edge, two shards, no filter service on shard 1. Subscribing it
    # takes the node offline. Unsubscribe shard 0, archive on shard 1, start a
    # shard 1 filter service. Backfill must deliver the archived message.
    block:
      # with two shards, `/recv-a/1` maps to shard 0 and `/recv-b/1` maps to shard 1
      let topicA = ContentTopic("/recv-a/1/edge-two-shards/proto")
      let topicB = ContentTopic("/recv-b/1/edge-two-shards/proto")
      let net = await setupNetwork(
        topicA,
        messaging = requiredAnonymity(),
        mode = LogosDeliveryMode.Edge,
        remoteFilter = true,
        numShards = 2,
        storeNodeShards = @[0'u16],
      )
      defer:
        await net.teardown()
      let eventManager = net.events
      check await eventManager.waitForEvents(TestTimeout) # the setup message
      check await net.waitForFilterSubscriptionHealth(healthy = true)
      check await net.subscriber.caughtUp()
      (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B")
      # no service peer advertises shard 1, so its filter subscription stays down
      await sleepAsync(2.seconds) # past the subscription loop's debounce
      check not net.filterSubscriptionHealthy(SecondShard)
      check net.filterSubscriptionHealthy(TestShard)
      net.subscriber.messagingClient.unsubscribe(topicA).expect("unsubscribe A")
      # archived after the unsubscribe, inside the offline window
      let gapMsg = await net.archiveAt(
        topicB,
        now(),
        "archived after the unsubscribe, before a filter service",
        SecondShard,
      )
      # shard 1 gets its filter service
      let filterNode = await newFilterServiceNode(1'u16)
      defer:
        await filterNode.stop()
      eventManager.targetCount = 2
      eventManager.receivedEvent.clear()
      let healthyB = net.waitForFilterSubscriptionHealth(healthy = true, SecondShard)
      await net.subscriber.waku.node.connectToNodes(
        @[filterNode.peerInfo.toRemotePeerInfo()]
      )
      check await healthyB
      check await eventManager.waitForEvents(TestTimeout)
      check eventManager.receivedMessages.len == 2 and
        eventManager.receivedMessages[^1].payload == gapMsg.payload
      check eventManager.receivedSources ==
        @[MessageSource.History, MessageSource.History]

  asyncTest "a topic that the node had gets its history at any time, a new topic gets none":
    ## The app subscribes its topics one call at a time, and it can subscribe a
    ## topic that it had before long after the start. The catch-up then gets
    ## the history of that topic. A topic that the node never had gets none.
    let topicA = ContentTopic("/waku/2/recv-late-a/proto")
    let topicB = ContentTopic("/waku/2/recv-late-b/proto")
    let topicC = ContentTopic("/waku/2/recv-late-c/proto")
    let topicD = ContentTopic("/waku/2/recv-late-d/proto")
    let net = await setupNetwork(topicA, alsoOwed = @[topicC, topicD])
    defer:
      await net.teardown()
    let events = net.events
    check await events.waitForEvents(TestTimeout) # topic A, the setup message
    check await net.subscriber.caughtUp()

    # Phase 1: B is new. A message archived before its subscription does not
    # arrive.
    let msgB =
      await net.archiveAt(topicB, now() - Minute, "archived before B subscribed")
    (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B")
    check await net.subscriber.caughtUp()
    check await events.noMoreEvents()
    check string.fromBytes(msgB.payload) notin events.payloads()

    # Subscribing a topic that is already subscribed announces nothing.
    var announced = 0
    let onSubscribed = proc(
        event: ContentTopicSubscribedEvent
    ) {.async: (raises: []).} =
      inc announced
    let announcements = ContentTopicSubscribedEvent
      .listen(net.subscriber.waku.brokerCtx, onSubscribed)
      .expect("listen")
    (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B again")
    await sleepAsync(200.milliseconds)
    await ContentTopicSubscribedEvent.dropListener(
      net.subscriber.waku.brokerCtx, announcements
    )
    check announced == 0

    # Phase 2: C is owed. Long after the catch-up of A, its subscription
    # still gets a message archived before it.
    let msgC =
      await net.archiveAt(topicC, now() - Minute, "archived before C subscribed")
    events.targetCount = 2
    events.receivedEvent.clear()
    (await net.subscriber.messagingClient.subscribe(topicC)).expect("subscribe C")
    check await events.waitForEvents(TestTimeout)
    check events.receivedMessages.len == 2 and
      events.receivedMessages[^1].payload == msgC.payload
    check events.receivedSources == @[MessageSource.History, MessageSource.History]

    # Phase 3: the kernel subscribes D while messaging is stopped. The next
    # messaging start takes the subscriptions of the kernel, so D, which is
    # owed, gets a message archived before that start.
    let msgD =
      await net.archiveAt(topicD, now() - Minute, "archived before messaging started")
    await net.subscriber.messagingClient.stop()
    net.subscriber.waku.subscribe(topicD).expect("subscribe D in the kernel")
    check net.subscriber.messagingClient.start().isOk()
    check (await events.waitForPayloadSource(string.fromBytes(msgD.payload))) ==
      Opt.some(MessageSource.History)

  asyncTest "an unsubscribed topic gets no live message, and its gap comes from Store at the next subscription":
    ## The node drops a message of an unsubscribed topic. When the app
    ## subscribes the topic again, the catch-up gets the messages from the time
    ## of the last message that the node received before the unsubscription.
    let topic = ContentTopic("/waku/2/recv-resubscribe-test/proto")
    let net = await setupNetwork(topic)
    defer:
      await net.teardown()
    let eventManager = net.events
    # The reconnection check cannot deliver the gap message, because it is
    # older than the new subscription by more than `delayExtra`.
    net.subscriber.messagingClient.recvService.delayExtra = RestartDelayExtra

    check await eventManager.waitForEvents(TestTimeout)
    check await net.subscriber.caughtUp()
    check eventManager.receivedMessages.len == 1

    net.subscriber.messagingClient.unsubscribe(topic).expect("unsubscribe")
    let archivedMsg = WakuMessage(
      payload: "archived while unsubscribed".toBytes(),
      contentTopic: topic,
      timestamp: now(),
    )
    discard (await net.publisher.publish(Opt.some(TestShard), archivedMsg)).expect(
      "publish while unsubscribed"
    )
    check await net.waitForArchived(topic, 2)
    # The node drops the live message of the unsubscribed topic.
    check eventManager.receivedMessages.len == 1

    await sleepAsync(RestartDelayExtra + 500.milliseconds)
    eventManager.targetCount = 2
    eventManager.receivedEvent.clear()
    (await net.subscriber.messagingClient.subscribe(topic)).expect("resubscribe")
    check await eventManager.waitForEvents(TestTimeout)
    check await net.subscriber.caughtUp()
    check await eventManager.noMoreEvents()
    check eventManager.payloads() ==
      @["This message was missed", "archived while unsubscribed"]
    check eventManager.receivedSources == @[
      MessageSource.History, MessageSource.History
    ]

  asyncTest "each topic has its own catch-up window across restarts":
    # Phase 1: the state of an earlier run, written into the root, has the
    # hint three hours ago. T was subscribed two hours ago, after the hint, so
    # its window starts at its subscription. K was subscribed four hours ago,
    # so its window starts at the hint. U is new and gets no history.
    block:
      let root = createTempDir("recv-api-windows-", "")
      defer:
        removeDir(root)
      let topicT = ContentTopic("/waku/2/recv-window-t/proto")
      let topicK = ContentTopic("/waku/2/recv-window-k/proto")
      let topicU = ContentTopic("/waku/2/recv-window-u/proto")
      let start = now()
      let hint = start - 3 * Hour
      let tSubscribedAt = start - 2 * Hour
      check await writeCatchUpState(
        root,
        hint = Opt.some(hint),
        records = @[
          ((TestShard, topicT), TopicRecord(at: tSubscribedAt, owed: false)),
          ((TestShard, topicK), TopicRecord(at: start - 4 * Hour, owed: false)),
        ],
      )
      let net = await setupNetwork(
        ContentTopic("/waku/2/recv-window-setup/proto"),
        root,
        history = Opt.none(Duration),
        subscribe = false,
      )
      defer:
        await net.teardown()
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      let beforeT =
        await net.archiveAt(topicT, tSubscribedAt - Minute, "T before its subscription")
      let afterT =
        await net.archiveAt(topicT, tSubscribedAt + Minute, "T after its subscription")
      let afterHintK =
        await net.archiveAt(topicK, hint + 10 * Minute, "K after the hint")
      let beforeU =
        await net.archiveAt(topicU, start - 10 * Minute, "U before its subscription")
      let events = net.events
      (await net.subscriber.messagingClient.subscribe(topicT)).expect("subscribe T")
      (await net.subscriber.messagingClient.subscribe(topicU)).expect("subscribe U")
      check await events.waitForEvents(TestTimeout)
      check await net.subscriber.caughtUp()
      check await events.noMoreEvents()
      check events.payloads() == @[string.fromBytes(afterT.payload)]

      # Phase 2: a live message moves the hint past the window of K. K is not
      # subscribed in this run. The app unsubscribes T.
      check await net.joinMesh()
      events.targetCount = 2
      events.receivedEvent.clear()
      let beforeLive = now()
      await net.publishLive(topicT, "T live")
      check await events.waitForEvents(TestTimeout)
      check (await job.waitForAdvance(start)).isSome()
      let beforeUnsubscribe = now()
      net.subscriber.messagingClient.unsubscribe(topicT).expect("unsubscribe T")
      let unsubscribedT = await job.waitForRecord((TestShard, topicT), owed = true)
      check unsubscribedT.isSome()
      # T is owed from the receipt of "T live", before the unsubscription.
      let owedAt = unsubscribedT.get(TopicRecord()).at
      check owedAt >= beforeLive and owedAt < beforeUnsubscribe
      let gapT = await net.archiveAt(
        topicT,
        unsubscribedT.get(TopicRecord(at: now())).at + 1,
        "T after its unsubscribe",
      )
      await sleepAsync(RestartDelayExtra + 500.milliseconds)

      # Phase 3: a restart. K gets its messages from the hint of the first run.
      # T gets its messages from its unsubscribe.
      await net.restartSubscriber(root)
      let restarted = net.events
      restarted.targetCount = 2
      (await net.subscriber.messagingClient.subscribe(topicK)).expect("subscribe K")
      (await net.subscriber.messagingClient.subscribe(topicT)).expect("subscribe T")
      check await restarted.waitForEvents(TestTimeout)
      check await net.subscriber.caughtUp()
      check await restarted.noMoreEvents()
      let payloads = restarted.payloads()
      check string.fromBytes(afterHintK.payload) in payloads
      check string.fromBytes(gapT.payload) in payloads
      check string.fromBytes(beforeT.payload) notin payloads
      check string.fromBytes(afterT.payload) notin payloads
      check restarted.receivedSources.allIt(it == MessageSource.History)

    # Phase 4: in a run that starts with a hint and no topic record, a topic
    # with no record gets the messages from the hint. A run that subscribes
    # nothing keeps this rule for the next run. After a run that writes a
    # record, a topic with no record is new.
    block:
      let root = createTempDir("recv-api-upgrade-", "")
      defer:
        removeDir(root)
      let topicV = ContentTopic("/waku/2/recv-upgrade-v/proto")
      let topicW = ContentTopic("/waku/2/recv-upgrade-w/proto")
      let topicX = ContentTopic("/waku/2/recv-upgrade-x/proto")
      check await writeCatchUpState(root, hint = Opt.some(now() - Hour))
      let net = await setupNetwork(
        ContentTopic("/waku/2/recv-upgrade-setup/proto"),
        root,
        history = Opt.none(Duration),
        subscribe = false,
      )
      defer:
        await net.teardown()
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      let downtimeV =
        await net.archiveAt(topicV, now() - 30 * Minute, "V during the downtime")
      check await net.subscriber.caughtUp() # the first run subscribes nothing
      await net.restartSubscriber(root)
      let events = net.events
      (await net.subscriber.messagingClient.subscribe(topicV)).expect("subscribe V")
      check await events.waitForEvents(TestTimeout)
      check await net.subscriber.caughtUp()
      check await events.noMoreEvents()
      check events.payloads() == @[string.fromBytes(downtimeV.payload)]
      check (await job.waitForRecord((TestShard, topicV), owed = false)).isSome()
      check (await job.waitForAdvance(now() - Minute)).isSome()
      # A second topic of the same run gets the messages from the hint too.
      let downtimeX =
        await net.archiveAt(topicX, now() - 20 * Minute, "X during the downtime")
      (await net.subscriber.messagingClient.subscribe(topicX)).expect("subscribe X")
      check (await events.waitForPayloadSource(string.fromBytes(downtimeX.payload))) ==
        Opt.some(MessageSource.History)
      check await net.subscriber.caughtUp()
      # After the hint that the receipt of V wrote.
      let beforeW = await net.archiveAt(topicW, now(), "W before its subscription")
      await sleepAsync(RestartDelayExtra + 500.milliseconds)

      await net.restartSubscriber(root)
      (await net.subscriber.messagingClient.subscribe(topicW)).expect("subscribe W")
      check await net.subscriber.caughtUp()
      check await net.events.noMoreEvents()
      check string.fromBytes(beforeW.payload) notin net.events.payloads()

    # Phase 5: the catch-up of A ends, and the Store peer does not answer the
    # query of B until `reply`. The app subscribes a new topic N, and the node stops. The next run
    # finds A live, B owed and N live. So it repeats only the topic in
    # progress, and it does not take N as new. A Store query of A in the next
    # run starts after the range that the first run completed.
    block:
      let root = createTempDir("recv-api-stop-in-pass-", "")
      defer:
        removeDir(root)
      let topicA = ContentTopic("/waku/2/recv-stop-in-pass-a/proto")
      let topicB = ContentTopic("/waku/2/recv-stop-in-pass-b/proto")
      let topicN = ContentTopic("/waku/2/recv-stop-in-pass-n/proto")
      let net = await setupNetwork(
        topicA, root, knowStorePeer = false, alsoOwed = @[topicB], subscribe = false
      )
      defer:
        await net.teardown()
      let (storeOnly, storeOnlyDriver) = await newStoreOnlyNode()
      defer:
        await storeOnly.stop()
      # Older than the overlap of the next run.
      let msgA = WakuMessage(
        payload: "A in the Store peer".toBytes(),
        contentTopic: topicA,
        timestamp: now() - 2 * Minute,
      )
      (await storeOnlyDriver.put(computeMessageHash(TestShard, msgA), TestShard, msgA)).expect(
        "archive put"
      )
      # The Store peer answers a query of B only after `reply`.
      # It also counts the queries of A that start before `repeatFrom`.
      let requested = newAsyncEvent()
      let reply = newAsyncEvent()
      defer:
        reply.fire()
      var repeatFrom = Timestamp(0)
      var repeatedA = 0
      let storeHandler = storeOnly.wakuStore.requestHandler
      storeOnly.wakuStore.requestHandler = proc(
          request: StoreQueryRequest
      ): Future[StoreQueryResult] {.async.} =
        if topicA in request.contentTopics and request.startTime.get(0) < repeatFrom:
          inc repeatedA
        if topicB in request.contentTopics:
          requested.fire()
          await reply.wait()
        return await storeHandler(request)
      net.subscriber.waku.node.peerManager.addServicePeer(
        storeOnly.peerInfo.toRemotePeerInfo(), WakuStoreCodec
      )
      let subscribedAt = now()
      (await net.subscriber.messagingClient.subscribe(topicA)).expect("subscribe A")
      (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B")
      check (await net.events.waitForPayloadSource("A in the Store peer")) ==
        Opt.some(MessageSource.History)
      check await requested.wait().withTimeout(TestTimeout)
      let subscribedN = now()
      (await net.subscriber.messagingClient.subscribe(topicN)).expect("subscribe N")
      await net.events.teardown()
      net.events = nil
      (await net.subscriber.stop()).expect("stop the subscriber")
      net.subscriber = nil
      block:
        let persistency = Persistency.new(root).expect("open root")
        defer:
          persistency.close()
        let job = persistency.openJob(MessagingJobId).expect("open job")
        let recordA = await job.waitForRecord((TestShard, topicA), owed = false)
        check recordA.isSome()
        check recordA.get(TopicRecord()).at >= subscribedAt
        let recordB = await job.waitForRecord((TestShard, topicB), owed = true)
        check recordB.isSome()
        check recordB.get(TopicRecord(at: now())).at < subscribedAt - Hour
        let recordN = await job.waitForRecord((TestShard, topicN), owed = false)
        check recordN.isSome()
        let recordAt = recordN.get(TopicRecord()).at
        check recordAt >= subscribedN and recordAt <= now()
      # The next run queries A from the end of the first catch-up, minus the
      # overlap, so the message of A does not come again.
      reply.fire()
      repeatFrom = subscribedAt - BackfillOverlap
      await net.startSubscriber(root, storeOnly.peerInfo.toRemotePeerInfo())
      (await net.subscriber.messagingClient.subscribe(topicA)).expect("subscribe A")
      (await net.subscriber.messagingClient.subscribe(topicB)).expect("subscribe B")
      check await net.subscriber.caughtUp()
      check repeatedA == 0
      check "A in the Store peer" notin net.events.payloads()

    # Phase 6: the worker takes the subscription changes before each topic
    # of a pass. While the Store peer does not answer B, the app subscribes a
    # new topic N, unsubscribes C and D, and subscribes C again. N gets its
    # record while the pass runs. The range of C ends after these changes, so
    # C gets a message from the time when it was not subscribed. D is not
    # subscribed at its turn, so it stays owed.
    block:
      let root = createTempDir("recv-api-changes-in-pass-", "")
      defer:
        removeDir(root)
      let topicB = ContentTopic("/waku/2/recv-changes-in-pass-b/proto")
      let topicC = ContentTopic("/waku/2/recv-changes-in-pass-c/proto")
      let topicD = ContentTopic("/waku/2/recv-changes-in-pass-d/proto")
      let topicN = ContentTopic("/waku/2/recv-changes-in-pass-n/proto")
      let net = await setupNetwork(
        topicB,
        root,
        knowStorePeer = false,
        alsoOwed = @[topicC, topicD],
        subscribe = false,
      )
      defer:
        await net.teardown()
      let (storeOnly, storeOnlyDriver) = await newStoreOnlyNode()
      defer:
        await storeOnly.stop()
      # The Store peer does not answer a query of B until `replyB`, or a query
      # of C until `replyC`.
      let requestedB = newAsyncEvent()
      let replyB = newAsyncEvent()
      let requestedC = newAsyncEvent()
      let replyC = newAsyncEvent()
      defer:
        replyB.fire()
        replyC.fire()
      let storeHandler = storeOnly.wakuStore.requestHandler
      storeOnly.wakuStore.requestHandler = proc(
          request: StoreQueryRequest
      ): Future[StoreQueryResult] {.async.} =
        if topicB in request.contentTopics:
          requestedB.fire()
          await replyB.wait()
        if topicC in request.contentTopics:
          requestedC.fire()
          await replyC.wait()
        return await storeHandler(request)
      for topic in [topicB, topicC, topicD]:
        (await net.subscriber.messagingClient.subscribe(topic)).expect("subscribe")
      net.subscriber.waku.node.peerManager.addServicePeer(
        storeOnly.peerInfo.toRemotePeerInfo(), WakuStoreCodec
      )
      # A new topic wakes the worker, so B, C and D are in one pass.
      (
        await net.subscriber.messagingClient.subscribe(
          ContentTopic("/waku/2/recv-changes-in-pass-wake/proto")
        )
      ).expect("subscribe a new topic")
      check await requestedB.wait().withTimeout(TestTimeout)
      (await net.subscriber.messagingClient.subscribe(topicN)).expect("subscribe N")
      net.subscriber.messagingClient.unsubscribe(topicC).expect("unsubscribe C")
      net.subscriber.messagingClient.unsubscribe(topicD).expect("unsubscribe D")
      let msgC = WakuMessage(
        payload: "C while unsubscribed".toBytes(),
        contentTopic: topicC,
        timestamp: now(),
      )
      (await storeOnlyDriver.put(computeMessageHash(TestShard, msgC), TestShard, msgC)).expect(
        "archive put"
      )
      (await net.subscriber.messagingClient.subscribe(topicC)).expect(
        "subscribe C again"
      )
      replyB.fire()
      check await requestedC.wait().withTimeout(TestTimeout)
      let persistency = Persistency.new(root).expect("open root")
      defer:
        persistency.close()
      let job = persistency.openJob(MessagingJobId).expect("open job")
      check (await job.waitForRecord((TestShard, topicN), owed = false)).isSome()
      replyC.fire()
      check (await net.events.waitForPayloadSource("C while unsubscribed")) ==
        Opt.some(MessageSource.History)
      check await net.subscriber.caughtUp()
      check (await job.waitForRecord((TestShard, topicD), owed = false)).isNone()

    # Phase 7: a Store failure of F does not stop the pass, so G gets its
    # message. F stays owed, and a later pass gets its message.
    block:
      let topicF = ContentTopic("/waku/2/recv-failing-f/proto")
      let topicG = ContentTopic("/waku/2/recv-failing-g/proto")
      let net = await setupNetwork(
        topicF, knowStorePeer = false, alsoOwed = @[topicG], subscribe = false
      )
      defer:
        await net.teardown()
      let (storeOnly, storeOnlyDriver) = await newStoreOnlyNode()
      defer:
        await storeOnly.stop()
      for (topic, text) in [
        (topicF, "F in the Store peer"), (topicG, "G in the Store peer")
      ]:
        let msg = WakuMessage(
          payload: text.toBytes(), contentTopic: topic, timestamp: now() - Minute
        )
        (await storeOnlyDriver.put(computeMessageHash(TestShard, msg), TestShard, msg)).expect(
          "archive put"
        )
      # The Store peer answers a query of F with an error while `failF` is set.
      var failF = true
      let storeHandler = storeOnly.wakuStore.requestHandler
      storeOnly.wakuStore.requestHandler = proc(
          request: StoreQueryRequest
      ): Future[StoreQueryResult] {.async.} =
        if failF and topicF in request.contentTopics:
          return err(StoreError(kind: ErrorCode.SERVICE_UNAVAILABLE))
        return await storeHandler(request)
      (await net.subscriber.messagingClient.subscribe(topicF)).expect("subscribe F")
      (await net.subscriber.messagingClient.subscribe(topicG)).expect("subscribe G")
      net.subscriber.waku.node.peerManager.addServicePeer(
        storeOnly.peerInfo.toRemotePeerInfo(), WakuStoreCodec
      )
      # A new topic wakes the worker, so F and G are in one pass.
      (
        await net.subscriber.messagingClient.subscribe(
          ContentTopic("/waku/2/recv-failing-wake/proto")
        )
      ).expect("subscribe a new topic")
      check (await net.events.waitForPayloadSource("G in the Store peer")) ==
        Opt.some(MessageSource.History)
      failF = false
      # A new topic wakes the worker before `CatchUpRetryPeriod` ends.
      (
        await net.subscriber.messagingClient.subscribe(
          ContentTopic("/waku/2/recv-failing-wake-again/proto")
        )
      ).expect("subscribe a new topic again")
      check (await net.events.waitForPayloadSource("F in the Store peer")) ==
        Opt.some(MessageSource.History)

  asyncTest "messaging runs without durable storage":
    ## Phase 1: a started node with `:memory:` keeps its records in memory. A
    ## topic that the node had before waits for a Store peer, and stop ends
    ## the catch-up.
    block:
      var node: LogosDelivery
      lockNewGlobalBrokerContext:
        node = (await LogosDelivery.new(testNodeConf(createApiNodeConf()))).expect(
          "create node"
        )
        node.shortenIntervals()
        (await node.start()).expect("start node")
      check GetPersistency.request(node.waku.brokerCtx).isOk()
      let topic = ContentTopic("/waku/2/recv-memory-only/proto")
      # No message arrives, so the topic is owed from its first subscription.
      (await node.messagingClient.subscribe(topic)).expect("subscribe")
      node.messagingClient.unsubscribe(topic).expect("unsubscribe")
      (await node.messagingClient.subscribe(topic)).expect("subscribe again")
      let recvService = node.messagingClient.recvService
      check not (await recvService.waitForCatchUp().withTimeout(chronos.seconds(1)))
      let ended = recvService.waitForCatchUp()
      check node.isRunning()
      (await node.stop()).expect("stop node")
      check await ended.withTimeout(chronos.seconds(1))
      check ended.completed() and not ended.read()
      check not node.isRunning()
    ## Phase 2: no Persistency provider (transport not started). Messaging
    ## starts. Catch-up is suspended with a warning.
    block:
      var node: LogosDelivery
      lockNewGlobalBrokerContext:
        node = (await LogosDelivery.new(testNodeConf(createApiNodeConf()))).expect(
          "create node"
        )
        node.shortenIntervals()
      check GetPersistency.request(node.waku.brokerCtx).isErr()
      check node.messagingClient.start().isOk()
      (
        await node.messagingClient.subscribe(
          ContentTopic("/waku/2/recv-no-provider/proto")
        )
      ).expect("subscribe")
      await sleepAsync(1500.milliseconds)
      check node.isRunning()
      await node.messagingClient.stop()
      check not node.isRunning()
    ## Phase 3: an out-of-range catch-up setting fails node creation. The
    ## full range checks are in the unit test.
    let bad = MessagingClientConf(backfillRequestTimeoutSeconds: Opt.some(0'i64))
    lockNewGlobalBrokerContext:
      check (await LogosDelivery.new(nodeConf(createApiNodeConf(), bad))).isErr()
    ## Phase 4: a job whose file path is a directory suspends catch-up with a
    ## warning. The node keeps running.
    block:
      let root = createTempDir("recv-api-badjob-", "")
      defer:
        removeDir(root)
      createDir(root / "messaging.db")
      var conf = createApiNodeConf()
      conf.localStoragePath = root
      var node: LogosDelivery
      lockNewGlobalBrokerContext:
        node = (await LogosDelivery.new(nodeConf(conf, backfillOverrides()))).expect(
          "create node"
        )
        node.shortenIntervals()
        (await node.start()).expect("start node")
      (await node.messagingClient.subscribe(ContentTopic("/waku/2/recv-bad-job/proto"))).expect(
        "subscribe"
      )
      check node.isRunning()
      (await node.stop()).expect("stop node")
