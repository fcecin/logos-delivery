{.used.}

## End-to-end tests for the anonymity levels on a mix network in one process.
##
## Five `WakuNode` instances make the network: three mix hops, one lightpush
## server with mix that is the last node of the sphinx path (`exit_is_dest`),
## and one relay peer of that server. A path has two hops and the exit node.
## The relay peer receives what the server publishes. The sender is a
## `LogosDelivery` stack made with the Messaging API: an Edge node with
## `anonymityLevel` set, the mix nodes given as `mixnode` entries, and the
## exit node given as its lightpush service peer.
## The tests use the real configuration path, the real send-processor chain,
## real sphinx path construction and a real reply. There is no stub.

import std/[net, sequtils, strutils]
import chronos, chronicles, testutils/unittests, results, stew/byteutils
import
  libp2p/[peerid, peerstore, switch],
  libp2p_mix/[curve25519, padding, mix_protocol],
  brokers/broker_context
import
  logos_delivery,
  logos_delivery/waku/[waku_node, waku_core, waku_mix],
  logos_delivery/waku/node/waku_node/lightpush,
  logos_delivery/waku/waku_lightpush/common,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/[publish, subscriptions],
  logos_delivery/api/types,
  logos_delivery/api/conf/[modes, messaging_conf, channels_conf, logos_delivery_conf],
  logos_delivery/api/events/[messaging_client_events, reliable_channel_manager_events],
  logos_delivery/messaging/delivery_service/send_service/[send_service, mix_processor],
  logos_delivery/channels/[reliable_channel_manager, types],
  logos_delivery/channels/encryption/noop_encryption,
  tools/confutils/cli_args
import ../testlib/[common, wakucore, wakunode, testasync]

const
  TestClusterId = 3'u16
  TestShard = 0'u16
  TestPubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  TestContentTopic = ContentTopic("/waku/2/default-content/proto")
  DeliveryTimeout = chronos.seconds(30)
    ## A mix round trip takes some hundred milliseconds. The limit is longer
    ## because the send service retries one time each second, and the relay
    ## mesh of the exit node can be incomplete at the first attempt.
  QuietPeriod = chronos.seconds(4)
    ## Time for some send-service rounds. The tests use it to show that a send
    ## did not occur.
  ShortMixWindow = chronos.seconds(2)
    ## Replaces the mix window of 60 s of a `Preferred` sender, so that a test
    ## can see the fallback.
  StopBudget = chronos.seconds(3)
    ## Maximum time for the stop of a sender with a mix send in progress.

proc freeTcpPort(): Port =
  ## Mix puts the address of the node in each return path that the node
  ## builds. Thus a mix node must know its port before it is built. It cannot
  ## read the port after a bind to port 0.
  let sock = newSocket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
  defer:
    sock.close()
  sock.setSockOpt(OptReuseAddr, true)
  sock.bindAddr(Port(0), "127.0.0.1")
  return sock.getLocalAddr()[1]

proc fullAddress(node: WakuNode): string =
  ## The form `/ip4/.../tcp/.../p2p/...` that `lightpushnode` and `mixnode`
  ## use. This is the listen address, not `announcedAddresses`: `WakuNode.start`
  ## changes an announced loopback address to the LAN address of the host, and
  ## these nodes listen on the loopback address only.
  $node.switch.peerInfo.listenAddrs[0] & "/p2p/" & $node.switch.peerInfo.peerId

proc mixBootnode(node: WakuNode): MixNodePubInfo =
  ## One `mixnode` entry: the full multiaddr of the peer and its mix public key.
  MixNodePubInfo(multiAddr: node.fullAddress(), pubKey: node.wakuMix.pubKey)

proc newNetworkNode(): WakuNode =
  let node = newTestWakuNode(
    generateSecp256k1Key(),
    parseIpAddress("127.0.0.1"),
    freeTcpPort(),
    quicEnabled = false,
    clusterId = TestClusterId,
    subscribeShards = @[TestShard],
  )
  # All nodes report the same cluster. The peer manager of the sender
  # disconnects and forgets a peer when the metadata request fails or gives a
  # different cluster. A hop that the sender forgets leaves the mix pool.
  node.mountMetadata(TestClusterId, @[TestShard]).isOkOr:
    raiseAssert "failed to mount metadata: " & error
  return node

proc mountMixOn(node: WakuNode) {.async.} =
  let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
  (await node.mountMix(TestClusterId, mixPrivKey, newSeq[MixNodePubInfo]())).isOkOr:
    raiseAssert "failed to mount mix: " & error

proc newMixHop(): Future[WakuNode] {.async.} =
  ## A hop does not need a pool. The address of the next hop is in the packet.
  ## A hop must be reachable and must run the mix protocol.
  var node: WakuNode
  lockNewGlobalBrokerContext:
    node = newNetworkNode()
    await node.mountMixOn()
    await node.start()
  return node

proc newLightpushServer(withMix: bool): Future[WakuNode] {.async.} =
  ## A relay node that serves lightpush. With `withMix` it is also a mix node.
  ## With `exit_is_dest`, a sender can select such a node as the exit node.
  var node: WakuNode
  lockNewGlobalBrokerContext:
    node = newNetworkNode()
    (await node.mountRelay()).isOkOr:
      raiseAssert "failed to mount relay"
    (await node.mountLightPush()).isOkOr:
      raiseAssert "failed to mount lightpush: " & error
    if withMix:
      await node.mountMixOn()
    await node.start()
  return node

proc newRelayPeer(): Future[WakuNode] {.async.} =
  var node: WakuNode
  lockNewGlobalBrokerContext:
    node = newNetworkNode()
    (await node.mountRelay()).isOkOr:
      raiseAssert "failed to mount relay"
    await node.start()
  return node

proc watchInbox(node: WakuNode, all: ref seq[WakuMessage] = nil): Future[WakuMessage] =
  ## Subscribes the relay of `node` to the test shard and returns a future for
  ## the first message that arrives. When `all` is given, each message also
  ## goes into `all`. A node accepts one handler for the shard, so a test that
  ## needs more than the first message reads `all`.
  let received = newFuture[WakuMessage]("mix e2e inbox")
  proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async, gcsafe.} =
    if not all.isNil():
      all[].add(msg)
    if not received.finished():
      received.complete(msg)

  node.subscribe((kind: PubsubSub, topic: TestPubsubTopic), handler).isOkOr:
    raiseAssert "failed to subscribe: " & error
  return received

type SendWatch = ref object
  ## The result of a send, as the application sees it through the events.
  brokerCtx: BrokerContext
  propagated: Future[RequestId]
  sent: Future[RequestId]
  failed: Future[string]
  propagatedListener: MessagePropagatedEventListener
  sentListener: MessageSentEventListener
  errorListener: MessageErrorEventListener

proc watchSend(brokerCtx: BrokerContext): SendWatch =
  let watch = SendWatch(brokerCtx: brokerCtx)
  watch.propagated = newFuture[RequestId]("mix e2e propagated")
  watch.sent = newFuture[RequestId]("mix e2e sent")
  watch.failed = newFuture[string]("mix e2e failed")
  watch.propagatedListener = MessagePropagatedEvent.listen(
    brokerCtx,
    proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
      if not watch.propagated.finished():
        watch.propagated.complete(event.requestId)
    ,
  ).valueOr:
    raiseAssert error
  watch.sentListener = MessageSentEvent.listen(
    brokerCtx,
    proc(event: MessageSentEvent) {.async: (raises: []).} =
      if not watch.sent.finished():
        watch.sent.complete(event.requestId)
    ,
  ).valueOr:
    raiseAssert error
  watch.errorListener = MessageErrorEvent.listen(
    brokerCtx,
    proc(event: MessageErrorEvent) {.async: (raises: []).} =
      if not watch.failed.finished():
        watch.failed.complete(event.error)
    ,
  ).valueOr:
    raiseAssert error
  return watch

proc stop(watch: SendWatch) {.async.} =
  await MessagePropagatedEvent.dropListener(watch.brokerCtx, watch.propagatedListener)
  await MessageSentEvent.dropListener(watch.brokerCtx, watch.sentListener)
  await MessageErrorEvent.dropListener(watch.brokerCtx, watch.errorListener)

proc newAnonymousSender(
    level: AnonymityLevel,
    mixnodes: seq[MixNodePubInfo],
    lightpushServer: WakuNode,
    entryLayer = EntryLayer.messaging,
    colocationLimit = 0,
): Future[LogosDelivery] {.async.} =
  ## Made with the Messaging API: `anonymityLevel`, the mix bootnodes and the
  ## lightpush service peer are messaging overrides, as in a structured JSON
  ## configuration. The level makes the kernel mount mix.
  var ldConf = LogosDeliveryConf.init(
    entryLayer = entryLayer,
    mode = messaging_conf.LogosDeliveryMode.Edge,
    preset = "",
    messagingOverrides = MessagingClientConf(
      anonymityLevel: Opt.some(level),
      clusterId: Opt.some(TestClusterId),
      numShardsInCluster: Opt.some(1'u16),
      listenIpv4: Opt.some(parseIpAddress("127.0.0.1")),
        # No port: the library default is port 0. The node learns its port when
        # the socket binds, after mix is mounted. The test shows that mix gets
        # the bound port for its return paths.
      discv5UdpPort: Opt.some(Port(0)),
      mixnodes: Opt.some(mixnodes),
      lightpushnode: Opt.some(lightpushServer.fullAddress()),
    ),
    channelsOverrides = ReliableChannelManagerConf(),
  ).valueOr:
    raiseAssert error
  var kernel = WakuNodeConf(ldConf.kernelConf)
  # No NAT override: the library default announces the listen address, and
  # the node listens on the loopback address. The REST server has no messaging
  # setting; the test process cannot bind its default port for each node.
  kernel.rest = false
  if colocationLimit > 0:
    kernel.colocationLimit = colocationLimit
  ldConf.kernelConf = KernelConf(kernel)

  var sender: LogosDelivery
  lockNewGlobalBrokerContext:
    sender = (await LogosDelivery.new(ldConf)).valueOr:
      raiseAssert error
    (await sender.start()).isOkOr:
      raiseAssert "failed to start the sender: " & error
  return sender

proc peerId(node: WakuNode): PeerId =
  node.switch.peerInfo.peerId

suite "Mix send path - end to end over an in-process mixnet":
  var
    hops {.threadvar.}: seq[WakuNode]
    exitNode {.threadvar.}: WakuNode
    relayPeer {.threadvar.}: WakuNode
    inbox {.threadvar.}: Future[WakuMessage]
    received {.threadvar.}: ref seq[WakuMessage]
    mixnet {.threadvar.}: seq[MixNodePubInfo]

  asyncSetup:
    hops = @[]
    for _ in 0 ..< 3:
      hops.add(await newMixHop())
    exitNode = await newLightpushServer(withMix = true)
    relayPeer = await newRelayPeer()

    # The exit node publishes each lightpush request on relay. It needs a
    # relay peer for that.
    discard exitNode.watchInbox()
    received = new seq[WakuMessage]
    inbox = relayPeer.watchInbox(received)
    await relayPeer.connectToNodes(@[exitNode.peerInfo.toRemotePeerInfo()])

    # Three hops and the exit node: the smallest pool for a path of two hops
    # and the exit node, with two more hops for the return path.
    mixnet = (hops & @[exitNode]).mapIt(it.mixBootnode())

  asyncTeardown:
    await allFutures((hops & @[exitNode, relayPeer]).mapIt(it.stop()))

  asyncTest "a Required send reaches the network through mix without ever touching the exit":
    let sender = await newAnonymousSender(AnonymityLevel.Required, mixnet, exitNode)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    let senderId = sender.waku.node.peerId()

    check:
      sender.waku.node.getMixNodePoolSize() == 4
      sender.waku.mixReady()
      # libp2p does not remove the address of a configured mix node.
      sender.waku.node.peerManager.switch.peerStore[AddressBook]
        .entries(exitNode.peerId())
        .anyIt(it.confidence == AddressConfidence.Infinite)

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    let requestId = (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "over the mixnet")
      )
    ).valueOr:
      raiseAssert error

    let propagated = await watch.propagated.withTimeout(DeliveryTimeout)
    check:
      propagated
      not watch.failed.finished()
    if propagated:
      check watch.propagated.read() == requestId

    # An Edge node with a level above None does not subscribe inside a send:
    # a filter subscribe request in clear text would connect the sender to
    # the message.
    check not sender.waku.isSubscribed(TestContentTopic).valueOr(false)

    # An Edge sender has no store to confirm the message with. The sent event
    # follows the propagation. The channel layer waits for this event.
    let sent = await watch.sent.withTimeout(DeliveryTimeout)
    check sent
    if sent:
      check watch.sent.read() == requestId

    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the exit's relay peer never received the message"
    let delivered = inbox.read()
    check:
      delivered.contentTopic == TestContentTopic
      delivered.payload == "over the mixnet".toBytes()

    # The sender dialed a hop only. The exit node got the message from the
    # last hop and answered on the return path. Thus the sender and the exit
    # node have no connection to each other.
    check:
      not exitNode.switch.isConnected(senderId)
      not sender.waku.node.switch.isConnected(exitNode.peerId())
      hops.anyIt(it.switch.isConnected(senderId))

  asyncTest "a Preferred send uses mix while mix can deliver":
    ## The fallback starts after the mix window of 60 s. A message that
    ## arrives in seconds went through mix. The connection table of the exit
    ## node shows it.
    let sender = await newAnonymousSender(AnonymityLevel.Preferred, mixnet, exitNode)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "best effort, still mixed")
      )
    ).valueOr:
      raiseAssert error

    check await watch.propagated.withTimeout(DeliveryTimeout)
    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the exit's relay peer never received the message"
    check:
      inbox.read().payload == "best effort, still mixed".toBytes()
      not exitNode.switch.isConnected(sender.waku.node.peerId())

  asyncTest "a message too large for mix falls back at once under Preferred and fails at once under Required":
    ## The mix library has two size checks. The entry connection refuses a
    ## frame above `DataSize`. The packet builder refuses a frame that does not
    ## fit next to the return path, which is smaller. The two levels must not
    ## keep such a message: the `Preferred` window and the `Required` deadline
    ## are 60 s.
    let withSurb = getMaxMessageSizeForCodec(WakuLightPushCodec, MixReplySurbs).expect(
        "max payload with one return path"
      )
    for size in [DataSize, withSurb + 1]:
      let tooBig = newSeq[byte](size)

      block preferred:
        let before = received[].len
        let sender =
          await newAnonymousSender(AnonymityLevel.Preferred, mixnet, exitNode)
        defer:
          (await sender.stop()).isOkOr:
            raiseAssert "failed to stop the sender: " & error
        let watch = watchSend(sender.waku.brokerCtx)
        defer:
          await watch.stop()

        discard (
          await sender.messagingClient.send(
            MessageEnvelope.init(TestContentTopic, tooBig)
          )
        ).valueOr:
          raiseAssert error

        # The plain lightpush path delivers the message. It dials the exit node.
        check await watch.propagated.withTimeout(QuietPeriod)
        let deadline = Moment.now() + QuietPeriod
        while received[].len == before and Moment.now() < deadline:
          await sleepAsync(chronos.milliseconds(50))
        if received[].len == before:
          raiseAssert "the relay peer never received the oversized message"
        check:
          received[][^1].payload.len == size
          exitNode.switch.isConnected(sender.waku.node.peerId())

      block required:
        let sender = await newAnonymousSender(AnonymityLevel.Required, mixnet, exitNode)
        defer:
          (await sender.stop()).isOkOr:
            raiseAssert "failed to stop the sender: " & error
        let watch = watchSend(sender.waku.brokerCtx)
        defer:
          await watch.stop()

        discard (
          await sender.messagingClient.send(
            MessageEnvelope.init(TestContentTopic, tooBig)
          )
        ).valueOr:
          raiseAssert error

        check await watch.failed.withTimeout(QuietPeriod)
        if watch.failed.finished():
          check "too large" in watch.failed.read()
        check not watch.propagated.finished()

  asyncTest "a Required send never falls back to a plain lightpush peer":
    ## The only lightpush server that the sender knows is not a mix node, and
    ## no mix node that it knows serves lightpush. Thus there is no exit node.
    ## `Required` waits for one. It must not use the plain server. A `None`
    ## sender uses the plain server in its first round.
    let plainServer = await newLightpushServer(withMix = false)
    defer:
      await plainServer.stop()
    let fourthHop = await newMixHop()
    defer:
      await fourthHop.stop()
    # A message that the plain server publishes gets to the relay peer.
    discard plainServer.watchInbox()
    await relayPeer.connectToNodes(@[plainServer.peerInfo.toRemotePeerInfo()])

    let hopsOnly = (hops & @[fourthHop]).mapIt(it.mixBootnode())
    let sender =
      await newAnonymousSender(AnonymityLevel.Required, hopsOnly, plainServer)
    let senderId = sender.waku.node.peerId()

    check:
      sender.waku.mixReady() # a path could be built ...
      sender.waku.lightpushPeerAvailable(TestPubsubTopic) # ... and a plain send would go
      sender.waku.selectMixLightpushPeer(TestPubsubTopic).isNone()
        # ... but no exit qualifies

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "must not leave in the clear")
      )
    ).valueOr:
      raiseAssert error

    await sleepAsync(QuietPeriod)
    check:
      not watch.propagated.finished()
      not watch.failed.finished() # still retrying, well inside the delivery window
      not inbox.finished()
      not plainServer.switch.isConnected(senderId)

    # A node with a task in its retry loop stops at once.
    let stopStarted = Moment.now()
    (await sender.stop()).isOkOr:
      raiseAssert "failed to stop the sender: " & error
    check:
      Moment.now() - stopStarted < StopBudget
      sender.messagingClient.sendService.inFlightSendCount() == 0
      # The application learns that the send did not complete.
      watch.failed.finished() and "stopped" in watch.failed.read()

  asyncTest "a Preferred send falls back to the plain path after the mix window":
    ## The same network as the previous case: mix can build paths but has no
    ## exit node, so mix cannot deliver. A `Preferred` sender keeps mix for the
    ## window and then uses the plain path. The test sets a short window in
    ## place of 60 s to see the fallback.
    let plainServer = await newLightpushServer(withMix = false)
    defer:
      await plainServer.stop()
    let fourthHop = await newMixHop()
    defer:
      await fourthHop.stop()
    discard plainServer.watchInbox()
    await relayPeer.connectToNodes(@[plainServer.peerInfo.toRemotePeerInfo()])

    let hopsOnly = (hops & @[fourthHop]).mapIt(it.mixBootnode())
    let sender =
      await newAnonymousSender(AnonymityLevel.Preferred, hopsOnly, plainServer)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    MixSendProcessor(sender.messagingClient.sendService.sendProcessor).mixWindow =
      ShortMixWindow

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    let sentAt = Moment.now()
    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "after the window, in the clear")
      )
    ).valueOr:
      raiseAssert error

    check await watch.propagated.withTimeout(DeliveryTimeout)
    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the relay peer never received the message"
    check:
      inbox.read().payload == "after the window, in the clear".toBytes()
      Moment.now() - sentAt >= ShortMixWindow # mix had the task for the window
      plainServer.switch.isConnected(sender.waku.node.peerId()) # then the plain path

  asyncTest "a send from a reliable channel does not fit a mix packet and fails at once under Required":
    ## The channel layer wraps each segment in an SDS envelope with a bloom
    ## filter of a fixed size. The envelope of a short payload is about 19 KB.
    ## A mix packet holds about 3 KB. Thus a channel send cannot go through
    ## mix until the mix path divides messages. `Required` fails the send at
    ## once, the channel layer gets the error, and nothing leaves the node.
    let sender = await newAnonymousSender(
      AnonymityLevel.Required, mixnet, exitNode, entryLayer = EntryLayer.channels
    )
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    setNoopEncryption()
    const channelId = ChannelId("mix-e2e-channel")
    discard sender.reliableChannelManager
      .createReliableChannel(channelId, TestContentTopic, SdsParticipantID("local"))
      .expect("createReliableChannel")

    let channelSent = newFuture[RequestId]("mix e2e channel sent")
    let sentListener = ChannelMessageSentEvent.listen(
      sender.waku.brokerCtx,
      proc(event: ChannelMessageSentEvent) {.async: (raises: []).} =
        if not channelSent.finished() and event.channelId == channelId:
          channelSent.complete(event.requestId)
      ,
    ).valueOr:
      raiseAssert error
    defer:
      await ChannelMessageSentEvent.dropListener(sender.waku.brokerCtx, sentListener)
    let channelFailed = newFuture[string]("mix e2e channel failed")
    let errorListener = ChannelMessageErrorEvent.listen(
      sender.waku.brokerCtx,
      proc(event: ChannelMessageErrorEvent) {.async: (raises: []).} =
        if not channelFailed.finished() and event.channelId == channelId:
          channelFailed.complete(event.error)
      ,
    ).valueOr:
      raiseAssert error
    defer:
      await ChannelMessageErrorEvent.dropListener(sender.waku.brokerCtx, errorListener)
    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    discard (
      await sender.reliableChannelManager.send(channelId, "from a channel".toBytes())
    ).valueOr:
      raiseAssert error

    # The size check runs before the first mix connection, so the failure is
    # visible in the first round.
    let reported = await channelFailed.withTimeout(QuietPeriod)
    check reported
    if reported:
      check "segments failed" in channelFailed.read()
    check:
      watch.failed.finished() and "too large" in watch.failed.read()
      not channelSent.finished()
      not watch.propagated.finished()
      not inbox.finished()
      not exitNode.switch.isConnected(sender.waku.node.peerId())

  asyncTest "an attempt with no reply moves the next attempt to another exit node":
    ## The configured lightpush server is a mix node that does not serve
    ## lightpush. The first attempt goes to it and gets no reply. The sender
    ## knows a second mix node that serves lightpush on the shard. The next
    ## attempt goes to the second node.
    let deadExit = await newMixHop()
    defer:
      await deadExit.stop()
    let network = (hops & @[deadExit, exitNode]).mapIt(it.mixBootnode())
    let sender = await newAnonymousSender(AnonymityLevel.Required, network, deadExit)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    # The sender learns the protocols and the shards of the second node, as it
    # does from an ENR or from a metadata exchange.
    let senderStore = sender.waku.node.peerManager.switch.peerStore
    senderStore[ProtoBook][exitNode.peerId()] = @[WakuLightPushCodec]
    senderStore.setShardInfo(exitNode.peerId(), @[TestShard])
    check:
      sender.waku.selectMixLightpushPeer(TestPubsubTopic).get().peerId ==
        deadExit.peerId() # the configured server comes first ...
      sender.waku
        .selectMixLightpushPeer(TestPubsubTopic, avoid = Opt.some(deadExit.peerId()))
        .get().peerId == exitNode.peerId()
        # ... and the second node when the first is avoided

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    let sentAt = Moment.now()
    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "through the second exit node")
      )
    ).valueOr:
      raiseAssert error

    check await watch.propagated.withTimeout(DeliveryTimeout)
    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the relay peer never received the message"
    # No check on a connection between the sender and the second node: the
    # second node is in the pool, so the first attempt can use it as a hop.
    check:
      inbox.read().payload == "through the second exit node".toBytes()
      Moment.now() - sentAt >= MixReplyTimeout # the first attempt waited for a reply

  asyncTest "an exit node without a relay peer is avoided on the next attempt":
    ## The configured exit node serves lightpush but has no relay peer, so it
    ## answers each request with an error. The next attempt goes to the
    ## second exit node, without a wait for a reply.
    let lonelyExit = await newLightpushServer(withMix = true)
    defer:
      await lonelyExit.stop()
    let network = (hops & @[lonelyExit, exitNode]).mapIt(it.mixBootnode())
    let sender = await newAnonymousSender(AnonymityLevel.Required, network, lonelyExit)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    let senderStore = sender.waku.node.peerManager.switch.peerStore
    senderStore[ProtoBook][exitNode.peerId()] = @[WakuLightPushCodec]
    senderStore.setShardInfo(exitNode.peerId(), @[TestShard])

    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    let sentAt = Moment.now()
    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "past an exit node without peers")
      )
    ).valueOr:
      raiseAssert error

    check await watch.propagated.withTimeout(DeliveryTimeout)
    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the relay peer never received the message"
    check:
      inbox.read().payload == "past an exit node without peers".toBytes()
      Moment.now() - sentAt < MixReplyTimeout # the first exit node answered at once

  asyncTest "the IP colocation limit spares the configured mix nodes":
    ## Every node of the test network is on one IP. With a limit of two, the
    ## third connection to that IP evicts and deletes an earlier peer. A
    ## configured mix node stays: deleted from the store, it leaves the pool
    ## for the life of the process.
    let sender = await newAnonymousSender(
      AnonymityLevel.Required, mixnet, exitNode, colocationLimit = 2
    )
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
    await sender.waku.node.connectToNodes(
      (hops & @[exitNode, relayPeer]).mapIt(it.peerInfo.toRemotePeerInfo())
    )
    await sleepAsync(chronos.milliseconds(500)) # the peer events run
    let senderStore = sender.waku.node.peerManager.switch.peerStore
    check:
      sender.waku.node.getMixNodePoolSize() == 4
      (hops & @[exitNode]).allIt(senderStore.isPinned(it.peerId()))
      (hops & @[exitNode]).allIt(senderStore[AddressBook].entries(it.peerId()).len > 0)

  asyncTest "stopping the sender while a mix reply is pending completes at once":
    ## The exit node stops after the sender learned it. The last hop cannot
    ## deliver the packet, so no reply comes, and each attempt waits for the
    ## full time limit. The sender is in an attempt most of the time. The stop
    ## must not wait for the attempt.
    let deadExit = await newLightpushServer(withMix = true)
    let network = (hops & @[deadExit]).mapIt(it.mixBootnode())
    let sender = await newAnonymousSender(AnonymityLevel.Required, network, deadExit)
    await deadExit.stop()
    let watch = watchSend(sender.waku.brokerCtx)
    defer:
      await watch.stop()

    discard (
      await sender.messagingClient.send(
        MessageEnvelope.init(TestContentTopic, "reply goes nowhere")
      )
    ).valueOr:
      raiseAssert error
    # The first attempt waits for a reply that cannot arrive.
    await sleepAsync(chronos.seconds(1))
    check not watch.propagated.finished()

    let stopStarted = Moment.now()
    (await sender.stop()).isOkOr:
      raiseAssert "failed to stop the sender: " & error
    check:
      Moment.now() - stopStarted < StopBudget
      sender.messagingClient.sendService.inFlightSendCount() == 0
      # The task was in its first attempt, not in the cache. The application
      # learns that the send did not complete all the same.
      watch.failed.finished() and "stopped" in watch.failed.read()
