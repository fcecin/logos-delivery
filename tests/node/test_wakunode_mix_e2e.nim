{.used.}

## End-to-end mix transport, in process and same build: 4 core nodes
## (relay + lightpush + mix) and one sender publish over the real sphinx
## path -- entry, two hops, exit-as-destination, relay publish, SURB reply.
##
## The mix unit tests drive the pool with fake peers, so a transport
## regression is invisible to them; this suite is the one place a broken
## sphinx path, exit layer or reply path fails loudly.

import
  std/[net, strutils, sequtils],
  testutils/unittests,
  chronos,
  results,
  stew/byteutils,
  libp2p/crypto/crypto,
  libp2p/peerid,
  libp2p/multiaddress,
  libp2p_mix/[curve25519, mix_protocol]

import
  logos_delivery/waku/
    [waku_core, node/peer_manager, waku_node, waku_mix, waku_lightpush],
  ../testlib/[wakucore, wakunode, testasync]

const
  NumCore = 4
  BatchSize = 4 ## concurrent mixed sends in the batch test
  ReceiveTimeout = chronos.seconds(10)
    ## Budget for the message to reach the sender over relay once the exit's
    ## reply is back, so it covers the gossipsub hop only, with room for a
    ## loaded CI host.

type MixNet = object
  nodes: seq[WakuNode] # 0 ..< NumCore are core, NumCore is the sender
  pubInfos: seq[MixNodePubInfo]

proc sender(mixnet: MixNet): WakuNode =
  mixnet.nodes[NumCore]

proc subscribeCores(mixnet: MixNet, shard: PubsubTopic) =
  ## The cores relay the shard; nothing reads what they receive.
  for i in 0 ..< NumCore:
    proc coreHandler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
      discard

    mixnet.nodes[i].subscribe((kind: PubsubSub, topic: shard), coreHandler).isOkOr:
      raiseAssert "subscribe core " & $i & ": " & $error

proc setupMixNet(basePort: int, senderAnnounce: string = ""): Future[MixNet] {.async.} =
  ## Builds and starts the 5-node mixnet on fixed local ports. A non-empty
  ## `senderAnnounce` replaces the sender's announced address (what a NATed
  ## node effectively does), while its real listener keeps working.
  var mixnet = MixNet()
  var mixPrivs: seq[FieldElement] = @[]

  for i in 0 .. NumCore:
    let port = basePort + i
    let n =
      if senderAnnounce.len > 0 and i == NumCore:
        newTestWakuNode(
          generateSecp256k1Key(),
          parseIpAddress("127.0.0.1"),
          Port(port),
          extMultiAddrs = @[MultiAddress.init(senderAnnounce).tryGet()],
          extMultiAddrsOnly = true,
        )
      else:
        newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(port))
    let kp = generateKeyPair().expect("mix key pair")
    mixnet.nodes.add(n)
    mixPrivs.add(kp.privateKey)
    mixnet.pubInfos.add(
      MixNodePubInfo(
        multiAddr: "/ip4/127.0.0.1/tcp/" & $port & "/p2p/" & $n.peerInfo.peerId,
        pubKey: kp.publicKey,
      )
    )

  for i in 0 .. NumCore:
    let others = (0 .. NumCore).toSeq().filterIt(it != i).mapIt(mixnet.pubInfos[it])
    if i < NumCore:
      (await mixnet.nodes[i].mountRelay()).isOkOr:
        raiseAssert "mountRelay core " & $i & ": " & $error
      (await mixnet.nodes[i].mountLightpush()).isOkOr:
        raiseAssert "mountLightpush core " & $i & ": " & $error
    else:
      (await mixnet.nodes[i].mountRelay()).isOkOr:
        raiseAssert "mountRelay sender: " & $error
      mixnet.nodes[i].mountLightpushClient()
    (await mixnet.nodes[i].mountMix(DefaultClusterId, mixPrivs[i], others)).isOkOr:
      raiseAssert "mountMix " & $i & ": " & $error

  for n in mixnet.nodes:
    await n.start()

  for i in 0 .. NumCore:
    let peers = (0 .. NumCore).toSeq().filterIt(it != i).mapIt(
        mixnet.nodes[it].peerInfo.toRemotePeerInfo()
      )
    await mixnet.nodes[i].connectToNodes(peers)

  return mixnet

proc teardownMixNet(mixnet: MixNet) {.async.} =
  for n in mixnet.nodes:
    await n.stop()

suite "Waku Mix - end to end transport":
  asyncTest "a message crosses the mixnet and the reply comes back":
    let mixnet = await setupMixNet(23840)
    defer:
      await teardownMixNet(mixnet)

    check mixnet.sender().getMixNodePoolSize() == NumCore

    let shard = DefaultPubsubTopic
    let marker = "mix-e2e-" & $Moment.now()
    let arrival = newFuture[string]("mix-e2e-arrival")

    proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
      let p = string.fromBytes(msg.payload)
      if p.startsWith(marker) and not arrival.finished():
        arrival.complete(p)

    mixnet.sender().subscribe((kind: PubsubSub, topic: shard), handler).isOkOr:
      raiseAssert "subscribe sender: " & $error
    mixnet.subscribeCores(shard)

    await sleepAsync(chronos.seconds(2)) # gossipsub mesh

    let msg = WakuMessage(
      payload: toBytes(marker & " over mix"),
      contentTopic: "/mix-e2e/1/probe/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )
    let dest = mixnet.nodes[0].peerInfo.toRemotePeerInfo()
    let res = await mixnet.sender().lightpushPublish(
      Opt.some(shard), msg, Opt.some(dest), mixify = true
    )

    # The SURB reply is the lightpush response itself.
    check res.isOk()

    # The exit really published: the message comes back to the sender on relay.
    # The handler completes the future only on the marker, so arrival is proof.
    check await arrival.withTimeout(ReceiveTimeout)

  asyncTest "four mixed sends in flight at once all get their own reply":
    ## Four mixed sends from one node at once must each complete on their own
    ## reply and release their own credentials: one mix connection, one SURB
    ## credential group, replies dispatched by SURB id. A caller that batches
    ## its sends relies on this.
    let mixnet = await setupMixNet(23900)
    defer:
      await teardownMixNet(mixnet)

    let shard = DefaultPubsubTopic
    let marker = "mix-e2e-batch-" & $Moment.now()
    var arrivals: seq[string]
    let allArrived = newFuture[void]("mix-e2e-batch-arrivals")

    proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
      let p = string.fromBytes(msg.payload)
      if p.startsWith(marker) and p notin arrivals:
        arrivals.add(p)
        if arrivals.len == BatchSize and not allArrived.finished():
          allArrived.complete()

    mixnet.sender().subscribe((kind: PubsubSub, topic: shard), handler).isOkOr:
      raiseAssert "subscribe sender: " & $error
    mixnet.subscribeCores(shard)

    await sleepAsync(chronos.seconds(2)) # gossipsub mesh

    let dest = mixnet.nodes[0].peerInfo.toRemotePeerInfo()
    var sends: seq[Future[WakuLightPushResult]]
    for i in 0 ..< BatchSize:
      let msg = WakuMessage(
        payload: toBytes(marker & " batch " & $i),
        contentTopic: "/mix-e2e/1/probe/proto",
        version: 2,
        timestamp: getNowInNanosecondTime(),
      )
      sends.add(
        mixnet.sender().lightpushPublish(
          Opt.some(shard), msg, Opt.some(dest), mixify = true
        )
      )
    await allFutures(sends)

    # Every send completed on its own reply; an ok result already means the
    # exit relayed to at least one peer.
    for send in sends:
      check send.read().isOk()

    # All four came back over relay, and no SURB credential is left behind.
    check await allArrived.withTimeout(ReceiveTimeout)
    check:
      arrivals.len == BatchSize
      mixnet.sender().wakuMix.surbCredsLen() == 0

  asyncTest "the reply arrives over existing connections, not the advertised address":
    ## What a NATed sender looks like: the announced address is undialable,
    ## but every hop holds a live connection to the sender. buildSurb embeds
    ## the announced address; delivery must still work because the delivering
    ## hop reuses its connection to the sender's peer id.
    let mixnet = await setupMixNet(23860, senderAnnounce = "/ip4/127.0.0.1/tcp/23879")
    defer:
      await teardownMixNet(mixnet)

    let shard = DefaultPubsubTopic
    let msg = WakuMessage(
      payload: toBytes("mix-e2e-natsim over mix"),
      contentTopic: "/mix-e2e/1/probe/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )
    mixnet.subscribeCores(shard)
    await sleepAsync(chronos.seconds(2))

    let dest = mixnet.nodes[1].peerInfo.toRemotePeerInfo()
    let res = await mixnet.sender().lightpushPublish(
      Opt.some(shard), msg, Opt.some(dest), mixify = true
    )
    check res.isOk()
