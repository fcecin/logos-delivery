{.used.}

## Mix test fixtures. The network sender announces a closed port, as a node
## behind NAT does.

import
  std/[net, sequtils, strutils, tables],
  chronos,
  results,
  stew/byteutils,
  libp2p/[multiaddress, peerid, peerinfo, switch],
  libp2p/crypto/crypto,
  libp2p/stream/connection,
  libp2p_mix/[curve25519, mix_protocol]
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/common/rate_limit/setting,
  logos_delivery/waku/discovery/[peer_discovery_interface, peer_discovery_conversion],
  ./[wakucore, wakunode]

const MixNodeCount* = 4

proc mountMixWith*(
    node: WakuNode,
    addressPolicy: PeerAddressPolicy,
    bootnodes: seq[MixNodePubInfo] = @[],
) {.async.} =
  let keys = generateKeyPair().expect("mix key pair")
  (await node.mountMix(DefaultClusterId, keys.privateKey, bootnodes, addressPolicy)).isOkOr:
    raiseAssert "mountMix: " & $error

proc mixNode*(
    addressPolicy: PeerAddressPolicy = mixAddressPolicy(false),
    bootnodes: seq[MixNodePubInfo] = @[],
): Future[WakuNode] {.async.} =
  ## A node with mix that never starts, so it dials nothing. It takes the
  ## public address policy, for policy tests.
  let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
  await node.mountMixWith(addressPolicy, bootnodes)
  return node

proc loopbackNode(port: int, quicEnabled: bool): WakuNode =
  let node = newTestWakuNode(
    generateSecp256k1Key(),
    parseIpAddress("127.0.0.1"),
    Port(port),
    quicEnabled = quicEnabled,
  )
  node.mountMetadata(uint32(DefaultClusterId), @[0'u16]).expect("metadata")
  return node

proc startNodeWithoutMix*(port: int, quicEnabled = false): Future[WakuNode] {.async.} =
  ## A started node without mix, for a real dial.
  let node = loopbackNode(port, quicEnabled)
  await node.start()
  return node

proc startMixNode*(
    port: int,
    addressPolicy: PeerAddressPolicy = defaultAddressPolicy,
    quicEnabled = false,
    interval = chronos.hours(1),
): Future[WakuNode] {.async.} =
  ## A started node with mix. It takes each address, for real dials on
  ## loopback. With the default `interval`, the pool loop runs once.
  let node = loopbackNode(port, quicEnabled)
  await node.mountMixWith(addressPolicy)
  node.wakuMix.pool.poolLoopInterval = interval
  await node.start()
  return node

proc discover*(
    node: WakuNode,
    addrs: seq[string],
    peerId = PeerId.init(generateSecp256k1Key()).tryGet(),
): PeerId {.discardable.} =
  ## Stores a discovery record with a new mix key, for a new or known peer.
  let keys = generateKeyPair().expect("mix key pair")
  let found = DiscoveredPeer(
    peerId: $peerId,
    addrs: addrs,
    services: @[DiscoveredService(id: MixProtocolID, data: @(keys.publicKey))],
  )
  node.peerManager.addPeer(found.toRemotePeerInfo().expect("discovered peer"))
  return peerId

proc bootnode*(address: string): MixNodePubInfo =
  let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
  let keys = generateKeyPair().expect("mix key pair")
  MixNodePubInfo(multiAddr: address & "/p2p/" & $peerId, pubKey: keys.publicKey)

proc peerId*(entry: MixNodePubInfo): PeerId =
  parsePeerInfo(entry.multiAddr).tryGet().peerId

proc deadPeers*(node: WakuNode, ports: varargs[int]): seq[PeerId] =
  ## Discovered mix peers on loopback ports where no mix node listens.
  ports.mapIt(node.discover(@["/ip4/127.0.0.1/tcp/" & $it]))

proc inPool*(node: WakuNode, peerId: PeerId): bool =
  node.wakuMix.nodePool.get(peerId).isSome()

proc failed*(node: WakuNode, peerId: PeerId): bool =
  node.wakuMix.pool.failed(peerId)

proc addOtherConnection*(node: WakuNode, port: int): Future[WakuNode] {.async.} =
  ## Starts a node without mix and connects `node` to it. A failed mix dial
  ## counts only while the node has another connection.
  let other = await startNodeWithoutMix(port)
  await node.switch.connect(other.peerInfo.peerId, other.peerInfo.addrs)
  return other

proc hopOf*(node: WakuNode, peerId: PeerId): MultiAddress =
  node.wakuMix.nodePool.get(peerId).expect("pool entry").multiAddr

# The mixnet fixture has 4 connected mix nodes (relay, lightpush, mix).

type
  ExitHold* = ref object
    ## Holds the first lightpush request at the exit until the test releases it.
    armed: bool
    entered*: AsyncEvent
    release*: AsyncEvent

  MixNet* = object
    nodes*: seq[WakuNode]
    infos*: seq[MixNodePubInfo]
    arrivals: ref seq[string]

proc new*(T: type ExitHold): ExitHold =
  ExitHold(armed: true, entered: newAsyncEvent(), release: newAsyncEvent())

proc exit*(net: MixNet): WakuNode =
  net.nodes[0]

proc mountHeldLightpush*(node: WakuNode, hold: ExitHold) =
  ## Lightpush over relay, whose first request waits for the test.
  let relayHandler = getRelayPushHandler(node.wakuRelay)
  let handler: PushMessageHandler = proc(
      pubsubTopic: PubsubTopic, message: WakuMessage
  ): Future[WakuLightPushResult] {.async.} =
    if hold.armed:
      hold.armed = false
      hold.entered.fire()
      await hold.release.wait()
    return await relayHandler(pubsubTopic, message)
  node.wakuLightPush = WakuLightPush.new(
    node.peerManager,
    node.rng,
    handler,
    node.wakuAutoSharding,
    Opt.none(RateLimitSetting),
  )
  node.switch.mount(node.wakuLightPush, protocolMatcher(WakuLightPushCodec))

proc startMixNodes*(basePort: int, hold: ExitHold = nil): Future[MixNet] {.async.} =
  var net = MixNet(arrivals: new(seq[string]))
  var keys: seq[FieldElement]
  for i in 0 ..< MixNodeCount:
    # The default transports, quic and tcp, as on the fleet.
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(basePort + i)
    )
    let kp = generateKeyPair().expect("mix key")
    keys.add(kp.privateKey)
    net.nodes.add(node)
    net.infos.add(
      MixNodePubInfo(
        multiAddr:
          "/ip4/127.0.0.1/tcp/" & $(basePort + i) & "/p2p/" & $node.peerInfo.peerId,
        pubKey: kp.publicKey,
      )
    )

  for i in 0 ..< MixNodeCount:
    let node = net.nodes[i]
    (await node.mountRelay()).expect("relay")
    if i == 0 and not hold.isNil():
      node.mountHeldLightpush(hold)
    else:
      (await node.mountLightpush()).expect("lightpush")
    node.mountMetadata(uint32(DefaultClusterId), @[0'u16]).expect("metadata")
    let peers = (0 ..< MixNodeCount).toSeq().filterIt(it != i).mapIt(net.infos[it])
    (await node.mountMix(DefaultClusterId, keys[i], peers, defaultAddressPolicy)).expect(
      "mix"
    )

  for node in net.nodes:
    await node.start()
  for i in 0 ..< MixNodeCount:
    for j in i + 1 ..< MixNodeCount:
      await net.nodes[i].switch.connect(
        net.nodes[j].peerInfo.peerId, net.nodes[j].peerInfo.addrs
      )
    let arrivals = net.arrivals
    proc receive(topic: PubsubTopic, msg: WakuMessage) {.async.} =
      let payload = string.fromBytes(msg.payload)
      if payload.startsWith("mix-nat-") and payload notin arrivals[]:
        arrivals[].add(payload)

    net.nodes[i].subscribe((kind: PubsubSub, topic: DefaultPubsubTopic), receive).expect(
      "subscribe"
    )
  await sleepAsync(chronos.seconds(2))
  return net

proc addNatSender*(
    net: MixNet,
    port: int,
    closedPort: int,
    bootnodes: seq[MixNodePubInfo],
    quicEnabled = true,
): Future[WakuNode] {.async.} =
  ## A lightpush client with mix that announces only `closedPort`, where nothing
  ## listens.
  let sender = newTestWakuNode(
    generateSecp256k1Key(),
    parseIpAddress("127.0.0.1"),
    Port(port),
    quicEnabled = quicEnabled,
    extMultiAddrs = @[MultiAddress.init("/ip4/127.0.0.1/tcp/" & $closedPort).tryGet()],
    extMultiAddrsOnly = true,
  )
  sender.mountLightpushClient()
  sender.mountMetadata(uint32(DefaultClusterId), @[0'u16]).expect("metadata")
  await sender.mountMixWith(defaultAddressPolicy, bootnodes)
  await sender.start()
  return sender

proc stop*(net: MixNet, senders: seq[WakuNode]) {.async.} =
  for node in senders:
    await node.stop()
  for node in net.nodes:
    await node.stop()

proc connectExit*(net: MixNet, sender: WakuNode) {.async.} =
  ## Connects the sender to the exit. A failed mix dial counts only while the
  ## sender has another connection.
  await sender.switch.connect(net.exit.peerInfo.peerId, net.exit.peerInfo.addrs)

proc connectedNodes*(net: MixNet, sender: WakuNode): seq[int] =
  (0 ..< MixNodeCount).toSeq().filterIt(
    sender.switch.isConnected(net.nodes[it].peerInfo.peerId)
  )

proc disconnectAll*(net: MixNet, sender: WakuNode) {.async.} =
  ## Drops every connection between the sender and the mix nodes.
  for node in net.nodes:
    await sender.switch.disconnect(node.peerInfo.peerId)
  await sleepAsync(chronos.milliseconds(100))
  doAssert net.connectedNodes(sender).len == 0

proc connectOnly*(net: MixNet, sender: WakuNode, index: int) {.async.} =
  ## Leaves the sender with one outbound connection, to node `index`.
  await net.disconnectAll(sender)
  await sender.switch.connect(
    net.nodes[index].peerInfo.peerId, net.nodes[index].peerInfo.addrs
  )
  doAssert net.connectedNodes(sender) == @[index]

proc noInboundConnections*(sender: WakuNode): bool =
  ## True when the sender opened each of its connections itself.
  for peerId, muxers in sender.switch.connManager.getConnections():
    for muxer in muxers:
      if muxer.connection.transportDir == Direction.In:
        return false
  return true

type SendOutcome* = object
  acked*: bool
  published*: bool
  error*: string
  elapsed*: Duration

proc send*(
    net: MixNet, sender: WakuNode, label: string
): Future[SendOutcome] {.async.} =
  let marker = "mix-nat-" & label
  let message =
    fakeWakuMessage(payload = marker, contentTopic = "/mix-nat/1/probe/proto")
  let start = Moment.now()
  let response = await sender.lightpushPublish(
    Opt.some(DefaultPubsubTopic),
    message,
    Opt.some(net.exit.peerInfo.toRemotePeerInfo()),
    mixify = true,
  )
  let elapsed = Moment.now() - start
  # Relay carries the message apart from the lightpush answer.
  await sleepAsync(chronos.milliseconds(300))
  return SendOutcome(
    acked: response.isOk(),
    published: marker in net.arrivals[],
    error:
      if response.isErr():
        response.error.desc.get($response.error.code)
      else:
        "",
    elapsed: elapsed,
  )

proc silentServer*(port: int, accepted: ref int = nil): StreamServer =
  ## Takes TCP connections and never answers. Counts them in `accepted`.
  proc serve(server: StreamServer, transp: StreamTransport) {.async: (raises: []).} =
    if not accepted.isNil():
      accepted[].inc()
    try:
      await sleepAsync(chronos.seconds(30))
    except CancelledError:
      discard
    await transp.closeWait()

  let server = createStreamServer(
    initTAddress("127.0.0.1", Port(port)), serve, {ServerFlags.ReuseAddr}
  )
  server.start()
  return server
