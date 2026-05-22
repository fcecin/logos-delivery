## Subscription engine: content-topic interest tracking, relay-mode pubsub
## subscription bookkeeping, edge-mode filter peer subscription maintenance.
## Type bodies live in ./waku_node.nim.

import std/[sequtils, sets, tables, options], chronos, chronicles, results, metrics
import libp2p/[peerid, peerinfo]
import brokers/broker_context

import
  waku/[
    waku_core,
    waku_core/topics,
    waku_core/topics/sharding,
    waku_node,
    waku_relay,
    waku_filter_v2/common as filter_common,
    waku_filter_v2/protocol as filter_protocol,
    waku_archive,
    waku_store_sync,
    api/events/health,
    events/peer_events,
    api/events/message,
    api/requests/health,
    requests/health_requests,
    node/peer_manager,
    node/health_monitor/topic_health,
    node/health_monitor/connection_status,
  ]
import waku/api/requests/filter as kernel_filter_api
import waku/api/requests/subscription

func toTopicHealth*(peersCount: int): TopicHealth =
  if peersCount >= HealthyThreshold:
    TopicHealth.SUFFICIENTLY_HEALTHY
  elif peersCount > 0:
    TopicHealth.MINIMALLY_HEALTHY
  else:
    TopicHealth.UNHEALTHY

proc isRelayMounted(self: WakuSubscriptionManager): bool =
  not self.node.wakuRelay.isNil()

proc isFilterMounted(self: WakuSubscriptionManager): bool =
  not self.node.wakuFilterClient.isNil()

iterator relaySubscribedTopics*(
    self: WakuSubscriptionManager
): (PubsubTopic, HashSet[ContentTopic]) =
  ## Iterate relay-subscribed content topics, batched per shard. Skips shards with no interest.
  for pubsub, topics in self.relayContentTopicSubs.pairs:
    if topics.len == 0:
      continue
    yield (pubsub, topics)

iterator edgeSubscribedTopics*(
    self: WakuSubscriptionManager
): (PubsubTopic, HashSet[ContentTopic]) =
  ## Iterate edge-subscribed content topics, batched per shard. Skips shards with no interest.
  for pubsub, topics in self.edgeContentTopicSubs.pairs:
    if topics.len == 0:
      continue
    yield (pubsub, topics)

proc edgeFilterPeerCount*(sm: WakuSubscriptionManager, shard: PubsubTopic): int =
  sm.edgeFilterSubStates.withValue(shard, state):
    return state.peers.len
  return 0

proc new*(T: typedesc[WakuSubscriptionManager], node: WakuNode): T =
  WakuSubscriptionManager(
    node: node,
    relayContentTopicSubs: initTable[PubsubTopic, HashSet[ContentTopic]](),
    edgeContentTopicSubs: initTable[PubsubTopic, HashSet[ContentTopic]](),
    directShardSubs: initHashSet[PubsubTopic](),
  )

# Relay mesh subscription bookkeeping

proc registerRelayHandler(
    self: WakuSubscriptionManager,
    shard: PubsubTopic,
    appHandler: WakuRelayHandler = nil,
): bool =
  ## Subscribe the relay mesh to shard with the single fan-out handler. Returns
  ## true iff a fresh mesh subscription was created; false if already subscribed
  ## (only the optional appHandler is re-recorded). The fan-out handler forwards
  ## each message to filter, archive and store-sync, emits MessageSeenEvent, then
  ## invokes the optional kernel-API app handler.
  let node = self.node
  let alreadySubscribed = node.wakuRelay.isSubscribed(shard)

  if not appHandler.isNil():
    if not alreadySubscribed or not node.legacyAppHandlers.hasKey(shard):
      node.legacyAppHandlers[shard] = appHandler
    else:
      debug "Legacy appHandler already exists for active shard, ignoring new handler",
        shard = shard

  if alreadySubscribed:
    return false

  proc traceHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    let msgSizeKB = msg.payload.len / 1000
    waku_node_messages.inc(labelValues = ["relay"])
    waku_histogram_message_size.observe(msgSizeKB)

  proc filterHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if node.wakuFilter.isNil():
      return
    await node.wakuFilter.handleMessage(topic, msg)

  proc archiveHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if node.wakuArchive.isNil():
      return
    await node.wakuArchive.handleMessage(topic, msg)

  proc syncHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if node.wakuStoreReconciliation.isNil():
      return
    node.wakuStoreReconciliation.messageIngress(topic, msg)

  proc internalHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    MessageSeenEvent.emit(node.brokerCtx, topic, msg)

  let uniqueTopicHandler = proc(
      topic: PubsubTopic, msg: WakuMessage
  ): Future[void] {.async, gcsafe.} =
    await traceHandler(topic, msg)
    await filterHandler(topic, msg)
    await archiveHandler(topic, msg)
    await syncHandler(topic, msg)
    await internalHandler(topic, msg)

    # Invoke the kernel-API app handler if one is registered.
    if node.legacyAppHandlers.hasKey(topic) and not node.legacyAppHandlers[topic].isNil():
      await node.legacyAppHandlers[topic](topic, msg)

  node.wakuRelay.subscribe(shard, uniqueTopicHandler)
  return true

proc meshSubscribe(
    self: WakuSubscriptionManager, shard: PubsubTopic, handler: WakuRelayHandler = nil
) =
  ## Idempotent relay-mesh subscribe. Emits PubsubSub only on a fresh mesh sub.
  if self.registerRelayHandler(shard, handler):
    self.node.topicSubscriptionQueue.emit((kind: SubscriptionKind.PubsubSub, topic: shard))

proc meshUnsubscribe(self: WakuSubscriptionManager, shard: PubsubTopic) =
  ## Tear down the relay-mesh subscription for shard and drop its app handler.
  ## Emits PubsubUnsub only if the mesh was actually subscribed.
  if self.node.legacyAppHandlers.hasKey(shard):
    self.node.legacyAppHandlers.del(shard)
  if self.node.wakuRelay.isSubscribed(shard):
    self.node.wakuRelay.unsubscribe(shard)
    self.node.topicSubscriptionQueue.emit(
      (kind: SubscriptionKind.PubsubUnsub, topic: shard)
    )

proc held(self: WakuSubscriptionManager, shard: PubsubTopic): bool =
  ## A shard's relay-mesh subscription is held while it has a direct shard
  ## subscription or any relay content-topic interest. Edge interest does not
  ## hold the mesh.
  self.directShardSubs.contains(shard) or
    self.relayContentTopicSubs.getOrDefault(shard).len > 0

proc resolveShard(
    self: WakuSubscriptionManager,
    topic: ContentTopic,
    shardOp: Option[PubsubTopic],
): Result[PubsubTopic, string] =
  ## Derive the shard for a content topic: use shardOp when given (required
  ## under static/manual sharding), otherwise auto-shard.
  let shardObj = ?deduceRelayShard(self.node, topic, shardOp)
  return ok(PubsubTopic($shardObj))

# Relay content-topic interest

proc addRelayContentTopicInterest(
    self: WakuSubscriptionManager, shard: PubsubTopic, topic: ContentTopic
) =
  if not self.relayContentTopicSubs.hasKey(shard):
    self.relayContentTopicSubs[shard] = initHashSet[ContentTopic]()
  self.relayContentTopicSubs.withValue(shard, cTopics):
    cTopics[].incl(topic)

proc removeRelayContentTopicInterest(
    self: WakuSubscriptionManager, shard: PubsubTopic, topic: ContentTopic
) =
  self.relayContentTopicSubs.withValue(shard, cTopics):
    cTopics[].excl(topic)
    if cTopics[].len == 0:
      self.relayContentTopicSubs.del(shard)

# Edge content-topic interest (drives the filter maintenance loop)

proc addEdgeContentTopicInterest(
    self: WakuSubscriptionManager, shard: PubsubTopic, topic: ContentTopic
) =
  var changed = false
  if not self.edgeContentTopicSubs.hasKey(shard):
    self.edgeContentTopicSubs[shard] = initHashSet[ContentTopic]()
    changed = true
  self.edgeContentTopicSubs.withValue(shard, cTopics):
    if not cTopics[].contains(topic):
      cTopics[].incl(topic)
      changed = true
  if changed and not isNil(self.edgeFilterWakeup):
    self.edgeFilterWakeup.fire()

proc removeEdgeContentTopicInterest(
    self: WakuSubscriptionManager, shard: PubsubTopic, topic: ContentTopic
) =
  var changed = false
  self.edgeContentTopicSubs.withValue(shard, cTopics):
    if cTopics[].contains(topic):
      cTopics[].excl(topic)
      changed = true
      if cTopics[].len == 0:
        self.edgeContentTopicSubs.del(shard)
  if changed and not isNil(self.edgeFilterWakeup):
    self.edgeFilterWakeup.fire()

proc isRelaySubscribed*(
    self: WakuSubscriptionManager, shard: PubsubTopic, contentTopic: ContentTopic
): bool {.raises: [].} =
  self.relayContentTopicSubs.withValue(shard, cTopics):
    return cTopics[].contains(contentTopic)
  return false

proc isEdgeSubscribed*(
    self: WakuSubscriptionManager, shard: PubsubTopic, contentTopic: ContentTopic
): bool {.raises: [].} =
  self.edgeContentTopicSubs.withValue(shard, cTopics):
    return cTopics[].contains(contentTopic)
  return false

# The four-operation subscription surface.
# subscribeShard/unsubscribeShard: direct (0/1) shard interest.
# subscribeContentTopic/unsubscribeContentTopic: per-content-topic interest.
# Content-topic ops take an optional shard: derived under auto-sharding,
# supplied under static/manual sharding. A shard's relay-mesh subscription is
# held while a direct shard subscription or any content-topic interest keeps it;
# the pubsub topic is torn down when nothing holds it.

proc subscribeShard*(
    self: WakuSubscriptionManager,
    shard: PubsubTopic,
    handler: WakuRelayHandler = nil,
): Result[void, string] =
  if not self.isRelayMounted() and not self.isFilterMounted():
    return err("WakuSubscriptionManager requires either Relay or Filter Client.")

  self.directShardSubs.incl(shard)
  if self.isRelayMounted():
    self.meshSubscribe(shard, handler)

  return ok()

proc unsubscribeShard*(
    self: WakuSubscriptionManager, shard: PubsubTopic
): Result[void, string] =
  if not self.isRelayMounted() and not self.isFilterMounted():
    return err("WakuSubscriptionManager requires either Relay or Filter Client.")

  # Remove the direct interest only; the pubsub topic stays up if content-topic interest holds it.
  self.directShardSubs.excl(shard)
  if self.isRelayMounted() and not self.held(shard):
    self.meshUnsubscribe(shard)

  return ok()

# Relay content-topic subscription (gossipsub mesh)

proc relaySubscribeContentTopic*(
    self: WakuSubscriptionManager,
    topic: ContentTopic,
    shardOp: Option[PubsubTopic] = none[PubsubTopic](),
): Result[void, string] =
  if not self.isRelayMounted():
    return err("relaySubscribeContentTopic requires Relay mounted.")

  let shard = ?self.resolveShard(topic, shardOp)
  self.meshSubscribe(shard, nil)
  self.addRelayContentTopicInterest(shard, topic)
  return ok()

proc relayUnsubscribeContentTopic*(
    self: WakuSubscriptionManager,
    topic: ContentTopic,
    shardOp: Option[PubsubTopic] = none[PubsubTopic](),
): Result[void, string] =
  if not self.isRelayMounted():
    return err("relayUnsubscribeContentTopic requires Relay mounted.")

  let shard = ?self.resolveShard(topic, shardOp)
  self.removeRelayContentTopicInterest(shard, topic)

  # Tear the mesh down only when nothing holds it.
  if not self.held(shard):
    self.meshUnsubscribe(shard)

  return ok()

# Edge content-topic subscription (filter; driver reconciles peers)

proc edgeSubscribe*(
    self: WakuSubscriptionManager,
    topic: ContentTopic,
    shardOp: Option[PubsubTopic] = none[PubsubTopic](),
): Result[void, string] =
  if not self.isFilterMounted():
    return err("edgeSubscribe requires a Filter Client mounted.")

  let shard = ?self.resolveShard(topic, shardOp)
  self.addEdgeContentTopicInterest(shard, topic)
  return ok()

proc edgeUnsubscribe*(
    self: WakuSubscriptionManager,
    topic: ContentTopic,
    shardOp: Option[PubsubTopic] = none[PubsubTopic](),
): Result[void, string] =
  if not self.isFilterMounted():
    return err("edgeUnsubscribe requires a Filter Client mounted.")

  let shard = ?self.resolveShard(topic, shardOp)
  self.removeEdgeContentTopicInterest(shard, topic)
  return ok()

# Edge Filter driver

const EdgeFilterSubscribeTimeout = chronos.seconds(15)
  ## Timeout for a single filter subscribe/unsubscribe RPC to a service peer.
const EdgeFilterPingTimeout = chronos.seconds(5)
  ## Timeout for a filter ping.
const EdgeFilterLoopInterval = chronos.seconds(30)
  ## Interval for the edge filter maintenance loop.
const EdgeFilterSubLoopDebounce = chronos.seconds(1)
  ## Debounce delay to coalesce wakeups into a single reconciliation pass.

type EdgeDialTask = object
  peer: RemotePeerInfo
  shard: PubsubTopic
  topics: seq[ContentTopic]

proc updateShardHealth(
    self: WakuSubscriptionManager, shard: PubsubTopic, state: var EdgeFilterSubState
) =
  ## Recompute and emit health for a shard after its peer set changed.
  let newHealth = toTopicHealth(state.peers.len)
  if newHealth != state.currentHealth:
    state.currentHealth = newHealth
    EventShardTopicHealthChange.emit(self.node.brokerCtx, shard, newHealth)

proc removePeer(self: WakuSubscriptionManager, shard: PubsubTopic, peerId: PeerId) =
  ## Remove a peer from edgeFilterSubStates for the shard, update health, and
  ## wake the sub loop to dial a replacement. Best-effort unsubscribe.
  self.edgeFilterSubStates.withValue(shard, state):
    var peer: RemotePeerInfo
    var found = false
    for p in state.peers:
      if p.peerId == peerId:
        peer = p
        found = true
        break
    if not found:
      return

    state.peers.keepItIf(it.peerId != peerId)
    self.updateShardHealth(shard, state[])
    self.edgeFilterWakeup.fire()

    if self.isFilterMounted():
      self.edgeContentTopicSubs.withValue(shard, topics):
        let ct = toSeq(topics[])
        if ct.len > 0:
          let brokerCtx = self.node.brokerCtx
          proc doUnsubscribe() {.async.} =
            discard await kernel_filter_api.RequestFilterUnsubscribe.request(
              brokerCtx, peer, shard, ct
            )

          asyncSpawn doUnsubscribe()

type SendChunkedFilterRpcKind = enum
  FilterSubscribe
  FilterUnsubscribe

proc sendChunkedFilterRpc(
    self: WakuSubscriptionManager,
    peer: RemotePeerInfo,
    shard: PubsubTopic,
    topics: seq[ContentTopic],
    kind: SendChunkedFilterRpcKind,
): Future[bool] {.async.} =
  ## Send a chunked filter subscribe or unsubscribe RPC. Returns true on
  ## success. On failure the peer is removed and false returned.
  try:
    var i = 0
    while i < topics.len:
      let chunk =
        topics[i ..< min(i + filter_protocol.MaxContentTopicsPerRequest, topics.len)]
      var failed = false
      case kind
      of FilterSubscribe:
        let fut = kernel_filter_api.RequestFilterSubscribe.request(
          self.node.brokerCtx, peer, shard, chunk
        )
        if not (await fut.withTimeout(EdgeFilterSubscribeTimeout)) or fut.read().isErr():
          failed = true
      of FilterUnsubscribe:
        let fut = kernel_filter_api.RequestFilterUnsubscribe.request(
          self.node.brokerCtx, peer, shard, chunk
        )
        if not (await fut.withTimeout(EdgeFilterSubscribeTimeout)) or fut.read().isErr():
          failed = true
      if failed:
        trace "sendChunkedFilterRpc: chunk failed",
          op = kind, shard = shard, peer = peer.peerId
        self.removePeer(shard, peer.peerId)
        return false
      i += filter_protocol.MaxContentTopicsPerRequest
  except CatchableError as exc:
    debug "sendChunkedFilterRpc: failed",
      op = kind, shard = shard, peer = peer.peerId, err = exc.msg
    self.removePeer(shard, peer.peerId)
    return false
  return true

proc syncFilterDeltas(
    self: WakuSubscriptionManager,
    peer: RemotePeerInfo,
    shard: PubsubTopic,
    added: seq[ContentTopic],
    removed: seq[ContentTopic],
) {.async.} =
  ## Push content topic changes (adds/removes) to an already-tracked peer.
  if added.len > 0:
    if not await self.sendChunkedFilterRpc(peer, shard, added, FilterSubscribe):
      return

  if removed.len > 0:
    discard await self.sendChunkedFilterRpc(peer, shard, removed, FilterUnsubscribe)

proc dialFilterPeer(
    self: WakuSubscriptionManager,
    peer: RemotePeerInfo,
    shard: PubsubTopic,
    contentTopics: seq[ContentTopic],
) {.async.} =
  ## Subscribe a new peer to all content topics on a shard and start tracking it.
  self.edgeFilterSubStates.withValue(shard, state):
    state.pendingPeers.incl(peer.peerId)

  try:
    if not await self.sendChunkedFilterRpc(peer, shard, contentTopics, FilterSubscribe):
      return

    self.edgeFilterSubStates.withValue(shard, state):
      if state.peers.anyIt(it.peerId == peer.peerId):
        trace "dialFilterPeer: peer already tracked, skipping duplicate",
          shard = shard, peer = peer.peerId
        return

      state.peers.add(peer)
      self.updateShardHealth(shard, state[])
      trace "dialFilterPeer: successfully subscribed to all chunks",
        shard = shard, peer = peer.peerId, totalPeers = state.peers.len
    do:
      trace "dialFilterPeer: shard removed while subscribing, discarding result",
        shard = shard, peer = peer.peerId
  finally:
    self.edgeFilterSubStates.withValue(shard, state):
      state.pendingPeers.excl(peer.peerId)

proc pingFilterPeer(
    self: WakuSubscriptionManager, peerId: PeerId, peer: RemotePeerInfo
): Future[(PeerId, bool)] {.async: (raises: []).} =
  let req = (
    await kernel_filter_api.RequestFilterPing.request(
      self.node.brokerCtx, peer, EdgeFilterPingTimeout
    )
  ).valueOr:
    return (peerId, false)
  return (peerId, req.pingOk)

proc edgeFilterMaintenanceLoop*(self: WakuSubscriptionManager) {.async.} =
  ## Periodically pings all connected filter service peers. Peers that fail the ping are removed.
  while true:
    await sleepAsync(EdgeFilterLoopInterval)

    if not self.isFilterMounted():
      warn "filter client is nil within edge filter maintenance loop"
      continue

    var connected = initTable[PeerId, RemotePeerInfo]()
    for state in self.edgeFilterSubStates.values:
      for peer in state.peers:
        if self.node.peerManager.switch.peerStore.isConnected(peer.peerId):
          connected[peer.peerId] = peer

    var alive = initHashSet[PeerId]()

    if connected.len > 0:
      # Ping all connected peers concurrently; survivors go in `alive`.
      var pingFuts: seq[Future[(PeerId, bool)]]
      for peerId, peer in connected:
        pingFuts.add(self.pingFilterPeer(peerId, peer))
      for f in pingFuts:
        let (peerId, ok) = await f
        if ok:
          alive.incl(peerId)

    var changed = false
    for shard, state in self.edgeFilterSubStates.mpairs:
      let oldLen = state.peers.len
      state.peers.keepItIf(it.peerId notin connected or alive.contains(it.peerId))

      if state.peers.len < oldLen:
        changed = true
        self.updateShardHealth(shard, state)
        trace "Edge Filter health degraded by Ping failure",
          shard = shard, new = state.currentHealth

    if changed:
      self.edgeFilterWakeup.fire()

proc selectFilterCandidates(
    self: WakuSubscriptionManager, shard: PubsubTopic, exclude: HashSet[PeerId], needed: int
): seq[RemotePeerInfo] =
  ## Select filter service peer candidates for a shard.

  # Start with every filter server peer that can serve the shard
  var allCandidates = self.node.peerManager.selectPeers(
    filter_common.WakuFilterSubscribeCodec, some(shard)
  )

  # Remove all already used in this shard or being dialed for it
  allCandidates.keepItIf(it.peerId notin exclude)

  # Collect peer IDs already tracked on other shards
  var trackedOnOther = initHashSet[PeerId]()
  for otherShard, otherState in self.edgeFilterSubStates.pairs:
    if otherShard != shard:
      for peer in otherState.peers:
        trackedOnOther.incl(peer.peerId)

  # Prefer peers we already have a connection to first, preserving shuffle
  var candidates =
    allCandidates.filterIt(it.peerId in trackedOnOther) &
    allCandidates.filterIt(it.peerId notin trackedOnOther)

  # We need to return 'needed' peers only
  if candidates.len > needed:
    candidates.setLen(needed)
  return candidates

proc edgeFilterSubLoop*(self: WakuSubscriptionManager) {.async.} =
  ## Reconciles filter subscriptions with the desired state.
  var lastSynced = initTable[PubsubTopic, HashSet[ContentTopic]]()

  while true:
    await self.edgeFilterWakeup.wait()
    await sleepAsync(EdgeFilterSubLoopDebounce)
    self.edgeFilterWakeup.clear()
    trace "edgeFilterSubLoop: woke up"

    if not self.isFilterMounted():
      trace "edgeFilterSubLoop: wakuFilterClient is nil, skipping"
      continue

    let desired = self.edgeContentTopicSubs

    trace "edgeFilterSubLoop: desired state", numShards = desired.len

    let allShards = toHashSet(toSeq(desired.keys)) + toHashSet(toSeq(lastSynced.keys))

    # Step 1: read state across all shards; build dial tasks and shards to delete.

    var dialTasks: seq[EdgeDialTask]
    var shardsToDelete: seq[PubsubTopic]

    for shard in allShards:
      let currTopics = desired.getOrDefault(shard)
      let prevTopics = lastSynced.getOrDefault(shard)

      if shard notin self.edgeFilterSubStates:
        self.edgeFilterSubStates[shard] =
          EdgeFilterSubState(currentHealth: TopicHealth.UNHEALTHY)

      let addedTopics = toSeq(currTopics - prevTopics)
      let removedTopics = toSeq(prevTopics - currTopics)

      self.edgeFilterSubStates.withValue(shard, state):
        state.peers.keepItIf(
          self.node.peerManager.switch.peerStore.isConnected(it.peerId)
        )
        state.pending.keepItIf(not it.finished)

        if addedTopics.len > 0 or removedTopics.len > 0:
          for peer in state.peers:
            asyncSpawn self.syncFilterDeltas(peer, shard, addedTopics, removedTopics)

        if currTopics.len == 0:
          shardsToDelete.add(shard)
        else:
          self.updateShardHealth(shard, state[])

          let needed = max(0, HealthyThreshold - state.peers.len - state.pending.len)

          if needed > 0:
            let tracked = state.peers.mapIt(it.peerId).toHashSet() + state.pendingPeers
            let candidates = self.selectFilterCandidates(shard, tracked, needed)
            let toDial = min(needed, candidates.len)

            trace "edgeFilterSubLoop: shard reconciliation",
              shard = shard,
              num_peers = state.peers.len,
              num_pending = state.pending.len,
              num_needed = needed,
              num_available = candidates.len,
              toDial = toDial

            for i in 0 ..< toDial:
              dialTasks.add(
                EdgeDialTask(
                  peer: candidates[i], shard: shard, topics: toSeq(currTopics)
                )
              )

    # Step 2: execute deferred shard deletion and dial tasks.

    for shard in shardsToDelete:
      self.edgeFilterSubStates.withValue(shard, state):
        for fut in state.pending:
          if not fut.finished:
            await fut.cancelAndWait()
      self.edgeFilterSubStates.del(shard)

    for task in dialTasks:
      let fut = self.dialFilterPeer(task.peer, task.shard, task.topics)
      self.edgeFilterSubStates.withValue(task.shard, state):
        state.pending.add(fut)

    lastSynced = desired

proc startEdgeFilterLoops(self: WakuSubscriptionManager): Result[void, string] =
  ## Start the edge filter orchestration loops.
  ## Only valid in edge mode (relay nil, filter client present).
  self.edgeFilterWakeup = newAsyncEvent()

  self.peerEventListener = EventWakuPeer.listen(
    self.node.brokerCtx,
    proc(evt: EventWakuPeer) {.async: (raises: []), gcsafe.} =
      if evt.kind == EventWakuPeerKind.EventDisconnected or
          evt.kind == EventWakuPeerKind.EventMetadataUpdated:
        self.edgeFilterWakeup.fire()
    ,
  ).valueOr:
    return err("Failed to listen to peer events for edge filter: " & error)

  self.edgeFilterSubLoopFut = self.edgeFilterSubLoop()
  self.edgeFilterMaintenanceLoopFut = self.edgeFilterMaintenanceLoop()
  return ok()

proc stopEdgeFilterLoops(self: WakuSubscriptionManager) {.async: (raises: []).} =
  ## Stop the edge filter orchestration loops and clean up pending futures.
  if not isNil(self.edgeFilterSubLoopFut):
    await self.edgeFilterSubLoopFut.cancelAndWait()
    self.edgeFilterSubLoopFut = nil

  if not isNil(self.edgeFilterMaintenanceLoopFut):
    await self.edgeFilterMaintenanceLoopFut.cancelAndWait()
    self.edgeFilterMaintenanceLoopFut = nil

  for shard, state in self.edgeFilterSubStates:
    for fut in state.pending:
      if not fut.finished:
        await fut.cancelAndWait()

  await EventWakuPeer.dropListener(self.node.brokerCtx, self.peerEventListener)

# WakuSubscriptionManager lifecycle.
# start/stopWakuSubscriptionManager orchestrate the relay and edge paths and
# register/clear broker providers.

proc startWakuSubscriptionManager*(self: WakuSubscriptionManager): Result[void, string] =
  RequestEdgeShardHealth.setProvider(
    self.node.brokerCtx,
    proc(shard: PubsubTopic): Result[RequestEdgeShardHealth, string] =
      self.edgeFilterSubStates.withValue(shard, state):
        return ok(RequestEdgeShardHealth(health: state.currentHealth))
      return ok(RequestEdgeShardHealth(health: TopicHealth.NOT_SUBSCRIBED)),
  ).isOkOr:
    error "Can't set provider for RequestEdgeShardHealth", error = error

  RequestEdgeFilterPeerCount.setProvider(
    self.node.brokerCtx,
    proc(): Result[RequestEdgeFilterPeerCount, string] =
      var minPeers = high(int)
      for state in self.edgeFilterSubStates.values:
        minPeers = min(minPeers, state.peers.len)
      if minPeers == high(int):
        minPeers = 0
      return ok(RequestEdgeFilterPeerCount(peerCount: minPeers)),
  ).isOkOr:
    error "Can't set provider for RequestEdgeFilterPeerCount", error = error

  # The four-operation subscription surface on the broker.
  RequestRelaySubscribeShard.setProvider(
    self.node.brokerCtx,
    proc(shard: PubsubTopic): Result[RequestRelaySubscribeShard, string] =
      self.subscribeShard(shard).isOkOr:
        return err(error)
      return ok(RequestRelaySubscribeShard(subscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestRelaySubscribeShard", error = error

  RequestRelayUnsubscribeShard.setProvider(
    self.node.brokerCtx,
    proc(shard: PubsubTopic): Result[RequestRelayUnsubscribeShard, string] =
      self.unsubscribeShard(shard).isOkOr:
        return err(error)
      return ok(RequestRelayUnsubscribeShard(unsubscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestRelayUnsubscribeShard", error = error

  RequestRelaySubscribeContentTopic.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestRelaySubscribeContentTopic, string] =
      self.relaySubscribeContentTopic(contentTopic, shard).isOkOr:
        return err(error)
      return ok(RequestRelaySubscribeContentTopic(subscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestRelaySubscribeContentTopic", error = error

  RequestRelayUnsubscribeContentTopic.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestRelayUnsubscribeContentTopic, string] =
      self.relayUnsubscribeContentTopic(contentTopic, shard).isOkOr:
        return err(error)
      return ok(RequestRelayUnsubscribeContentTopic(unsubscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestRelayUnsubscribeContentTopic", error = error

  RequestEdgeSubscribe.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestEdgeSubscribe, string] =
      self.edgeSubscribe(contentTopic, shard).isOkOr:
        return err(error)
      return ok(RequestEdgeSubscribe(subscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestEdgeSubscribe", error = error

  RequestEdgeUnsubscribe.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestEdgeUnsubscribe, string] =
      self.edgeUnsubscribe(contentTopic, shard).isOkOr:
        return err(error)
      return ok(RequestEdgeUnsubscribe(unsubscribed: true)),
  ).isOkOr:
    error "Can't set provider for RequestEdgeUnsubscribe", error = error

  RequestIsRelaySubscribed.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestIsRelaySubscribed, string] =
      let resolved = ?self.resolveShard(contentTopic, shard)
      return ok(
        RequestIsRelaySubscribed(subscribed: self.isRelaySubscribed(resolved, contentTopic))
      ),
  ).isOkOr:
    error "Can't set provider for RequestIsRelaySubscribed", error = error

  RequestIsEdgeSubscribed.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestIsEdgeSubscribed, string] =
      let resolved = ?self.resolveShard(contentTopic, shard)
      return ok(
        RequestIsEdgeSubscribed(subscribed: self.isEdgeSubscribed(resolved, contentTopic))
      ),
  ).isOkOr:
    error "Can't set provider for RequestIsEdgeSubscribed", error = error

  RequestIsSubscribed.setProvider(
    self.node.brokerCtx,
    proc(
        contentTopic: ContentTopic, shard: Option[PubsubTopic]
    ): Result[RequestIsSubscribed, string] =
      let resolved = ?self.resolveShard(contentTopic, shard)
      # Default multiplexing: relay if mounted, else edge.
      return ok(
        RequestIsSubscribed(
          subscribed:
            if self.isRelayMounted():
              self.isRelaySubscribed(resolved, contentTopic)
            else:
              self.isEdgeSubscribed(resolved, contentTopic)
        )
      ),
  ).isOkOr:
    error "Can't set provider for RequestIsSubscribed", error = error

  RequestRelaySubscribedTopics.setProvider(
    self.node.brokerCtx,
    proc(): Result[RequestRelaySubscribedTopics, string] =
      var topics: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]
      for shard, cTopics in self.relaySubscribedTopics:
        topics.add((shard: shard, contentTopics: toSeq(cTopics)))
      return ok(RequestRelaySubscribedTopics(topics: topics)),
  ).isOkOr:
    error "Can't set provider for RequestRelaySubscribedTopics", error = error

  # Default multiplexing: relay if mounted, else edge.
  RequestSubscribedTopics.setProvider(
    self.node.brokerCtx,
    proc(): Result[RequestSubscribedTopics, string] =
      var topics: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]
      if self.isRelayMounted():
        for shard, cTopics in self.relaySubscribedTopics:
          topics.add((shard: shard, contentTopics: toSeq(cTopics)))
      else:
        for shard, cTopics in self.edgeSubscribedTopics:
          topics.add((shard: shard, contentTopics: toSeq(cTopics)))
      return ok(RequestSubscribedTopics(topics: topics)),
  ).isOkOr:
    error "Can't set provider for RequestSubscribedTopics", error = error

  RequestEdgeSubscribedTopics.setProvider(
    self.node.brokerCtx,
    proc(): Result[RequestEdgeSubscribedTopics, string] =
      var topics: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]
      for shard, cTopics in self.edgeSubscribedTopics:
        topics.add((shard: shard, contentTopics: toSeq(cTopics)))
      return ok(RequestEdgeSubscribedTopics(topics: topics)),
  ).isOkOr:
    error "Can't set provider for RequestEdgeSubscribedTopics", error = error

  # Fan out shard-health changes to per-content-topic health events. A content
  # topic's health is its shard's health. Set up in both modes.
  self.shardHealthListener = EventShardTopicHealthChange.listen(
    self.node.brokerCtx,
    proc(evt: EventShardTopicHealthChange) {.async: (raises: []), gcsafe.} =
      let cTopics =
        self.relayContentTopicSubs.getOrDefault(evt.topic) +
        self.edgeContentTopicSubs.getOrDefault(evt.topic)
      for ct in cTopics:
        EventContentTopicHealthChange.emit(self.node.brokerCtx, ct, evt.health)
    ,
  ).valueOr:
    return err("Failed to listen to shard health events: " & error)

  if not self.isRelayMounted():
    return self.startEdgeFilterLoops()

  # Core mode: auto-subscribe relay to all autosharding shards.
  if self.node.wakuAutoSharding.isSome():
    let autoSharding = self.node.wakuAutoSharding.get()
    let clusterId = autoSharding.clusterId
    let numShards = autoSharding.shardCountGenZero

    if numShards > 0:
      for i in 0 ..< numShards:
        let shardObj = RelayShard(clusterId: clusterId, shardId: uint16(i))
        self.subscribeShard(PubsubTopic($shardObj)).isOkOr:
          error "Failed to auto-subscribe Relay to cluster shard: ",
            shard = $shardObj, error = error
  else:
    info "WakuSubscriptionManager has no AutoSharding configured; skipping auto-subscribe."

  return ok()

proc stopWakuSubscriptionManager*(self: WakuSubscriptionManager) {.async: (raises: []).} =
  if not self.isRelayMounted():
    await self.stopEdgeFilterLoops()
  await EventShardTopicHealthChange.dropListener(
    self.node.brokerCtx, self.shardHealthListener
  )
  RequestEdgeShardHealth.clearProvider(self.node.brokerCtx)
  RequestEdgeFilterPeerCount.clearProvider(self.node.brokerCtx)
  RequestRelaySubscribeShard.clearProvider(self.node.brokerCtx)
  RequestRelayUnsubscribeShard.clearProvider(self.node.brokerCtx)
  RequestRelaySubscribeContentTopic.clearProvider(self.node.brokerCtx)
  RequestRelayUnsubscribeContentTopic.clearProvider(self.node.brokerCtx)
  RequestEdgeSubscribe.clearProvider(self.node.brokerCtx)
  RequestEdgeUnsubscribe.clearProvider(self.node.brokerCtx)
  RequestIsRelaySubscribed.clearProvider(self.node.brokerCtx)
  RequestIsEdgeSubscribed.clearProvider(self.node.brokerCtx)
  RequestIsSubscribed.clearProvider(self.node.brokerCtx)
  RequestRelaySubscribedTopics.clearProvider(self.node.brokerCtx)
  RequestEdgeSubscribedTopics.clearProvider(self.node.brokerCtx)
  RequestSubscribedTopics.clearProvider(self.node.brokerCtx)
