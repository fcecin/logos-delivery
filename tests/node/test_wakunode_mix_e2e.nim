{.used.}

## End-to-end mix transport, in process and same build: 4 core nodes
## (relay + lightpush + mix) and one sender publish over the real sphinx
## path -- entry, two hops, exit-as-destination, relay publish, SURB reply.
##
## The mix unit tests drive the pool with fake peers, so a transport
## regression is invisible to them; this suite is the one place a broken
## sphinx path, exit layer or reply path fails loudly.

import std/[net, strutils, sequtils]
import testutils/unittests, chronos, results
import stew/byteutils
import libp2p/crypto/crypto, libp2p/peerid, libp2p/multiaddress
import libp2p_mix/curve25519

import
  logos_delivery/waku/
    [waku_core, node/peer_manager, waku_node, waku_mix, waku_lightpush],
  ../testlib/[wakucore, wakunode, testasync]

const
  NumCore = 4
  ReceiveTimeout = chronos.seconds(10)

type MixNet = object
  nodes: seq[WakuNode] # 0 ..< NumCore are core, NumCore is the sender
  pubInfos: seq[MixNodePubInfo]

proc senderOf(net: MixNet): WakuNode =
  net.nodes[NumCore]

proc setupMixNet(basePort: int, senderAnnounce: string = ""): Future[MixNet] {.async.} =
  ## Builds and starts the 5-node mixnet on fixed local ports. A non-empty
  ## `senderAnnounce` replaces the sender's announced address (what a NATed
  ## node effectively does), while its real listener keeps working.
  var net = MixNet()
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
    net.nodes.add(n)
    mixPrivs.add(kp.privateKey)
    net.pubInfos.add(
      MixNodePubInfo(
        multiAddr: "/ip4/127.0.0.1/tcp/" & $port & "/p2p/" & $n.peerInfo.peerId,
        pubKey: kp.publicKey,
      )
    )

  for i in 0 .. NumCore:
    let others = (0 .. NumCore).toSeq().filterIt(it != i).mapIt(net.pubInfos[it])
    if i < NumCore:
      (await net.nodes[i].mountRelay()).isOkOr:
        raiseAssert "mountRelay core " & $i & ": " & $error
      (await net.nodes[i].mountLightpush()).isOkOr:
        raiseAssert "mountLightpush core " & $i & ": " & $error
    else:
      (await net.nodes[i].mountRelay()).isOkOr:
        raiseAssert "mountRelay sender: " & $error
      net.nodes[i].mountLightpushClient()
    (await net.nodes[i].mountMix(DefaultClusterId, mixPrivs[i], others)).isOkOr:
      raiseAssert "mountMix " & $i & ": " & $error

  for n in net.nodes:
    await n.start()

  for i in 0 .. NumCore:
    let peers = (0 .. NumCore).toSeq().filterIt(it != i).mapIt(
        net.nodes[it].peerInfo.toRemotePeerInfo()
      )
    await net.nodes[i].connectToNodes(peers)

  return net

proc teardownMixNet(net: MixNet) {.async.} =
  for n in net.nodes:
    await n.stop()

suite "Waku Mix - end to end transport":
  asyncTest "a message crosses the mixnet and the reply comes back":
    let net = await setupMixNet(23840)
    defer:
      await teardownMixNet(net)

    check net.senderOf().getMixNodePoolSize() == NumCore

    let shard = DefaultPubsubTopic
    let marker = "mix-e2e-" & $Moment.now()
    let arrival = newFuture[string]("mix-e2e-arrival")

    proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
      let p = string.fromBytes(msg.payload)
      if p.startsWith(marker) and not arrival.finished():
        arrival.complete(p)

    net.senderOf().subscribe((kind: PubsubSub, topic: shard), handler).isOkOr:
      raiseAssert "subscribe sender: " & $error
    for i in 0 ..< NumCore:
      proc coreHandler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
        discard

      net.nodes[i].subscribe((kind: PubsubSub, topic: shard), coreHandler).isOkOr:
        raiseAssert "subscribe core " & $i & ": " & $error

    await sleepAsync(chronos.seconds(2)) # gossipsub mesh

    let msg = WakuMessage(
      payload: toBytes(marker & " over mix"),
      contentTopic: "/mix-e2e/1/probe/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )
    let dest = net.nodes[0].peerInfo.toRemotePeerInfo()
    let res = await net.senderOf().lightpushPublish(
      Opt.some(shard), msg, Opt.some(dest), mixify = true
    )

    # The SURB reply is the lightpush response itself.
    check res.isOk()

    # The exit really published: the message comes back to the sender on relay.
    let arrived = await arrival.withTimeout(ReceiveTimeout)
    check arrived
    if arrived:
      check arrival.read().startsWith(marker)

  asyncTest "the reply arrives over existing connections, not the advertised address":
    ## What a NATed sender looks like: the announced address is undialable,
    ## but every hop holds a live connection to the sender. buildSurb embeds
    ## the announced address; delivery must still work because the delivering
    ## hop reuses its connection to the sender's peer id.
    let net = await setupMixNet(23860, senderAnnounce = "/ip4/127.0.0.1/tcp/23879")
    defer:
      await teardownMixNet(net)

    let shard = DefaultPubsubTopic
    let msg = WakuMessage(
      payload: toBytes("mix-e2e-natsim over mix"),
      contentTopic: "/mix-e2e/1/probe/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )
    for i in 0 ..< NumCore:
      proc coreHandler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
        discard

      net.nodes[i].subscribe((kind: PubsubSub, topic: shard), coreHandler).isOkOr:
        raiseAssert "subscribe core " & $i & ": " & $error
    await sleepAsync(chronos.seconds(2))

    let dest = net.nodes[1].peerInfo.toRemotePeerInfo()
    let res = await net.senderOf().lightpushPublish(
      Opt.some(shard), msg, Opt.some(dest), mixify = true
    )
    check res.isOk()
