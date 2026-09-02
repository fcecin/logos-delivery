{.used.}

## End-to-end coverage for the anonymity levels over a real, in-process mixnet.
##
## Five plain `WakuNode`s make up the network: three mix hops, one mix-mounted
## lightpush server that terminates the sphinx path (`exit_is_dest`) and runs
## the lightpush handler on the payload in-process, and a relay peer of that
## server which receives whatever it publishes. The sender is a full
## `LogosDelivery` stack built through the Messaging API surface: an Edge node
## whose `anonymityLevel` is what mounts mix, seeded with the mix nodes in the
## `--mixnode` form and the exit as its lightpush service peer. What runs is the
## real configuration path, the real send-processor chain, real sphinx path
## construction and a real SURB reply, with nothing stubbed.

import std/[net, sequtils]
import chronos, chronicles, testutils/unittests, results, stew/byteutils
import libp2p/[peerid, peerstore, switch], libp2p_mix/curve25519, brokers/broker_context
import
  logos_delivery,
  logos_delivery/waku/[waku_node, waku_core, waku_mix],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/api/types,
  logos_delivery/api/conf/[modes, messaging_conf, channels_conf, logos_delivery_conf],
  logos_delivery/api/events/messaging_client_events,
  tools/confutils/cli_args
import ../testlib/[common, wakucore, wakunode, testasync]

const
  TestClusterId = 3'u16
  TestShard = 0'u16
  TestPubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  TestContentTopic = ContentTopic("/waku/2/default-content/proto")
  DeliveryTimeout = chronos.seconds(30)
    ## Generous: a mix round trip is a few hundred milliseconds, but the send
    ## service retries once a second and a first attempt can lose the race
    ## against the exit's relay mesh forming.
  QuietPeriod = chronos.seconds(4)
    ## Long enough for several send-service rounds to have run when asserting
    ## that a send did *not* happen.

proc freeTcpPort(): Port =
  ## Mix bakes a node's own address into every SURB it builds, so a mix node has
  ## to know its port before it is built rather than reading it back after
  ## binding on port 0.
  let sock = newSocket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
  defer:
    sock.close()
  sock.setSockOpt(OptReuseAddr, true)
  sock.bindAddr(Port(0), "127.0.0.1")
  return sock.getLocalAddr()[1]

proc fullAddress(node: WakuNode): string =
  ## `/ip4/.../tcp/.../p2p/...`: the form `--lightpushnode` and `--mixnode` take.
  ## The bound listen address, not `announcedAddresses`: `WakuNode.start`
  ## rewrites a loopback announced address to the host's LAN address, which
  ## these loopback-bound nodes do not listen on.
  $node.switch.peerInfo.listenAddrs[0] & "/p2p/" & $node.switch.peerInfo.peerId

proc mixBootnode(node: WakuNode): MixNodePubInfo =
  ## One `--mixnode` entry: the peer's full multiaddr and its mix public key.
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
  # Every node reports the same cluster. The sender's peer manager disconnects
  # and forgets a peer whose metadata request fails or disagrees, and forgetting
  # a hop removes it from the mix pool with it.
  node.mountMetadata(TestClusterId, @[TestShard]).isOkOr:
    raiseAssert "failed to mount metadata: " & error
  return node

proc mountMixOn(node: WakuNode) {.async.} =
  let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
  (await node.mountMix(TestClusterId, mixPrivKey, newSeq[MixNodePubInfo]())).isOkOr:
    raiseAssert "failed to mount mix: " & error

proc newMixHop(): Future[WakuNode] {.async.} =
  ## A hop needs no pool of its own: the next hop's address travels inside the
  ## packet, so it only has to be dialable and run the mix protocol.
  var node: WakuNode
  lockNewGlobalBrokerContext:
    node = newNetworkNode()
    await node.mountMixOn()
    await node.start()
  return node

proc newLightpushServer(withMix: bool): Future[WakuNode] {.async.} =
  ## A relay node serving lightpush. With `withMix` it is also a mix node, which
  ## under `exit_is_dest` is what lets a sender pick it as the sphinx exit.
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

proc watchInbox(node: WakuNode): Future[WakuMessage] =
  ## Subscribes `node`'s relay to the test shard and returns a future for the
  ## first message that arrives on it.
  let received = newFuture[WakuMessage]("mix e2e inbox")
  proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async, gcsafe.} =
    if not received.finished():
      received.complete(msg)

  node.subscribe((kind: PubsubSub, topic: TestPubsubTopic), handler).isOkOr:
    raiseAssert "failed to subscribe: " & error
  return received

type SendWatch = ref object
  ## The messaging layer's verdict on a send, as the application would see it.
  brokerCtx: BrokerContext
  propagated: Future[RequestId]
  failed: Future[string]
  propagatedListener: MessagePropagatedEventListener
  errorListener: MessageErrorEventListener

proc watchSend(brokerCtx: BrokerContext): SendWatch =
  let watch = SendWatch(brokerCtx: brokerCtx)
  watch.propagated = newFuture[RequestId]("mix e2e propagated")
  watch.failed = newFuture[string]("mix e2e failed")
  watch.propagatedListener = MessagePropagatedEvent.listen(
    brokerCtx,
    proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
      if not watch.propagated.finished():
        watch.propagated.complete(event.requestId)
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
  await MessageErrorEvent.dropListener(watch.brokerCtx, watch.errorListener)

proc newAnonymousSender(
    level: AnonymityLevel, mixnodes: seq[MixNodePubInfo], lightpushServer: WakuNode
): Future[LogosDelivery] {.async.} =
  ## Built through the Messaging API surface: `anonymityLevel` is a messaging
  ## override, and it is what makes the kernel mount mix. The mix bootnodes and
  ## the lightpush service peer are kernel settings, exactly as they would be in
  ## a JSON config or on the command line.
  var ldConf = LogosDeliveryConf.init(
    entryLayer = EntryLayer.messaging,
    mode = messaging_conf.LogosDeliveryMode.Edge,
    preset = "",
    messagingOverrides = MessagingClientConf(
      anonymityLevel: Opt.some(level),
      clusterId: Opt.some(TestClusterId),
      numShardsInCluster: Opt.some(1'u16),
      listenIpv4: Opt.some(parseIpAddress("127.0.0.1")),
      p2pTcpPort: Opt.some(freeTcpPort()),
    ),
    channelsOverrides = ReliableChannelManagerConf(),
  ).valueOr:
    raiseAssert error
  var kernel = WakuNodeConf(ldConf.kernelConf)
  kernel.nat = "extip:127.0.0.1" # announce the loopback bind address as it is
  kernel.rest = false
  kernel.discv5UdpPort = Port(0)
  kernel.mixnodes = mixnodes
  kernel.lightpushnode = lightpushServer.fullAddress()
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
    mixnet {.threadvar.}: seq[MixNodePubInfo]

  asyncSetup:
    hops = @[]
    for _ in 0 ..< 3:
      hops.add(await newMixHop())
    exitNode = await newLightpushServer(withMix = true)
    relayPeer = await newRelayPeer()

    # The exit publishes what it receives over lightpush into its relay mesh,
    # and needs a peer there to publish to.
    discard exitNode.watchInbox()
    inbox = relayPeer.watchInbox()
    await relayPeer.connectToNodes(@[exitNode.peerInfo.toRemotePeerInfo()])

    # Three hops plus the exit: the smallest pool from which mix can pick a
    # three-hop path that ends at a fourth node.
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
      # A configured mix node is pinned against libp2p's address pruning.
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

    if not await inbox.withTimeout(DeliveryTimeout):
      raiseAssert "the exit's relay peer never received the message"
    let delivered = inbox.read()
    check:
      delivered.contentTopic == TestContentTopic
      delivered.payload == "over the mixnet".toBytes()

    # The sender only ever dialed a hop; the exit learned the message from the
    # last hop and answered through the SURB, so neither side of the pair has a
    # connection to the other.
    check:
      not exitNode.switch.isConnected(senderId)
      not sender.waku.node.switch.isConnected(exitNode.peerId())
      hops.anyIt(it.switch.isConnected(senderId))

  asyncTest "a BestEffort send uses mix while mix can deliver":
    ## The fallback only opens after the mix window, a minute later; a message
    ## that arrives within seconds arrived over mix, and the exit's connection
    ## table proves it.
    let sender = await newAnonymousSender(AnonymityLevel.BestEffort, mixnet, exitNode)
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

  asyncTest "a Required send never falls back to a plain lightpush peer":
    ## The only lightpush server the sender knows is not a mix node, and no mix
    ## node it knows serves lightpush, so there is no exit to build a path to.
    ## `Required` keeps waiting for one; it must not use the plain server, which
    ## a `None` sender would have reached in its first round.
    let plainServer = await newLightpushServer(withMix = false)
    defer:
      await plainServer.stop()
    let fourthHop = await newMixHop()
    defer:
      await fourthHop.stop()
    # Anything the plain server publishes would reach the relay peer.
    discard plainServer.watchInbox()
    await relayPeer.connectToNodes(@[plainServer.peerInfo.toRemotePeerInfo()])

    let hopsOnly = (hops & @[fourthHop]).mapIt(it.mixBootnode())
    let sender =
      await newAnonymousSender(AnonymityLevel.Required, hopsOnly, plainServer)
    defer:
      (await sender.stop()).isOkOr:
        raiseAssert "failed to stop the sender: " & error
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
