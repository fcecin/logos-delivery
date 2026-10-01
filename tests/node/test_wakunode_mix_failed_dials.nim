{.used.}

## Failed dials and the mix pool (#4352). Only a failed dial of this node counts.

import
  std/[net, sequtils, strutils, tables],
  testutils/unittests,
  chronos,
  metrics,
  results,
  stew/byteutils,
  libp2p/[multiaddress, peerid, peerinfo, peerstore, switch],
  libp2p/crypto/crypto,
  libp2p/stream/connection,
  libp2p_mix/mix_protocol
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/waku_mix/protocol_metrics,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/delivery_dialer,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/discovery/peer_discovery_interface,
  ../testlib/[testasync, wakucore, wakumix]

suite "Waku Mix - failed reply hop dials":
  asyncTest "no dialable reply candidate gives a fast failure, nothing sent, then recovery":
    let net = await startMixNodes(25360)
    # The exit is real. One attempt dials both deadBootnodes.
    let deadBootnodes = deadBootnodes(25376, 25377)
    let sender = await net.addNatSender(25364, 25379, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)
    check sender.getMixNodePoolSize() == 3

    let failuresBefore = logos_delivery_mix_reply_hop_failures.value()
    let first = await net.send(sender, "deadBootnodes-first")
    check:
      logos_delivery_mix_reply_hop_failures.value() == failuresBefore + 1
      not first.acked
      first.error.contains(NoReplyHopError)
      first.elapsed < MixReplyHopTimeout + chronos.seconds(1)
      not first.published
      sender.wakuMix.surbCredsLen() == 0
      deadIds.allIt(store[NumberFailedConnBook][it] == 1)
      sender.getMixNodePoolSize() == 1

    # With no candidate left, the next attempt stops before any dial.
    let second = await net.send(sender, "deadBootnodes-second")
    check:
      logos_delivery_mix_reply_hop_failures.value() == failuresBefore + 2
      not second.acked
      not second.published
      second.elapsed < chronos.seconds(1)
      deadIds.allIt(store[NumberFailedConnBook][it] == 1)
      sender.wakuMix.surbCredsLen() == 0

    sender.wakuMix.addBootNodes(net.infos[1 ..^ 1])
    let third = await net.send(sender, "deadBootnodes-recovered")
    check:
      third.acked
      third.published
      sender.wakuMix.surbCredsLen() == 0

  asyncTest "an offline sender blames no peer for its failed dials":
    let net = await startMixNodes(25600)
    let deadBootnodes = deadBootnodes(25616, 25617)
    let sender = await net.addNatSender(25604, 25619, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)
    let setOnline = sender.peerManager.getOnlineStateObserver()

    setOnline(false)
    let offline = await net.send(sender, "offline-deadBootnodes")
    check:
      not offline.acked
      not offline.published
      sender.wakuMix.surbCredsLen() == 0
      deadIds.allIt(store[NumberFailedConnBook][it] == 0)
      sender.getMixNodePoolSize() == 3

  asyncTest "a send in the next pass joins the dial that is still running":
    let net = await startMixNodes(25800)
    let hanging = @[silentServer(25816), silentServer(25817)]
    let deadBootnodes = deadBootnodes(25816, 25817)
    let sender = await net.addNatSender(25804, 25819, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
      for server in hanging:
        server.stop()
        await server.closeWait()
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)

    let started = Moment.now()
    let first = await net.send(sender, "next-pass-1")
    let second = await net.send(sender, "next-pass-2")
    check:
      not first.acked
      not second.acked
    await sleepAsync(started + DefaultDialTimeout + chronos.seconds(1) - Moment.now())
    check deadIds.allIt(store[NumberFailedConnBook][it] == 1)
    await sleepAsync(
      started + 2 * DefaultDialTimeout + chronos.seconds(1) - Moment.now()
    )
    check deadIds.allIt(store[NumberFailedConnBook][it] == 1)

  asyncTest "a send after the pool stops dials nothing and records nothing":
    let net = await startMixNodes(25840)
    let deadBootnodes = deadBootnodes(25856, 25857)
    let sender = await net.addNatSender(25844, 25859, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)

    await sender.wakuMix.pool.stop()
    let outcome = await net.send(sender, "after-stop")
    check:
      not outcome.acked
      not outcome.published
      outcome.error.contains(MixStoppingError)
      outcome.elapsed < chronos.seconds(1)
      deadIds.allIt(store[NumberFailedConnBook][it] == 0)

  asyncTest "a blocked quic address does not hide a tcp address that works":
    ## The quic address drops every packet, as behind a firewall.
    let target = await startNodeWithoutMix(25720)
    let node = await startMixNode(25721, quicEnabled = true)
    let blackhole = newDatagramTransport(
      proc(t: DatagramTransport, a: TransportAddress) {.async: (raises: []).} =
        discard,
      local = initTAddress("127.0.0.1:0"),
    )
    defer:
      await blackhole.closeWait()
      await node.stop()
      await target.stop()

    let deadQuic = "/ip4/127.0.0.1/udp/" & $blackhole.localAddress().port & "/quic-v1"
    let targetId =
      node.discover(@[deadQuic, "/ip4/127.0.0.1/tcp/25720"], target.peerInfo.peerId)
    check $node.hopOf(targetId) == deadQuic

    let exitId = PeerId.init(generateSecp256k1Key()).tryGet()
    let started = Moment.now()
    check (await node.wakuMix.prepareReplyPath(exitId)).isOk()
    check:
      Moment.now() - started < MixReplyHopTimeout
      node.switch.peerStore[NumberFailedConnBook][targetId] == 0
      $node.hopOf(targetId) == "/ip4/127.0.0.1/tcp/25720"

suite "Waku Mix - unreachable pool members":
  asyncTest "a peer with a failed dial stays off paths, also after discovery finds it again":
    let net = await startMixNodes(25440)
    let sender = await net.addNatSender(25444, 25459, net.infos)
    defer:
      await net.stop(@[sender])
    for i in 0 ..< MixNodeCount:
      await sender.switch.connect(
        net.nodes[i].peerInfo.peerId, net.nodes[i].peerInfo.addrs
      )

    let dead = sender.discover(@["/ip4/127.0.0.1/tcp/25458"])
    check sender.getMixNodePoolSize() == MixNodeCount + 1
    sender.peerManager.recordDialFailure(dead, "test")
    check sender.getMixNodePoolSize() == MixNodeCount

    # Discovery cannot clear the dial record.
    sender.discover(@["/ip4/127.0.0.1/tcp/25458"], dead)
    check sender.getMixNodePoolSize() == MixNodeCount

    for i in 0 ..< 4:
      let outcome = await net.send(sender, "known-dead-" & $i)
      check:
        outcome.acked
        outcome.published
        sender.wakuMix.surbCredsLen() == 0

  asyncTest "a failed dial to the first hop takes that peer out of the pool":
    ## Every first hop candidate is unreachable, so the first hop dial fails on any path.
    let net = await startMixNodes(25480)
    let deadBootnodes = deadBootnodes(25496, 25497, 25498)
    let sender = await net.addNatSender(25484, 25499, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)

    let sent = await sender.wakuMix.anonymizeLocalProtocolSend(
      newAsyncQueue[seq[byte]](),
      toBytes("first-hop"),
      WakuLightPushCodec,
      MixDestination.exitNode(net.exit.peerInfo.peerId),
      0'u8,
    )
    check sent.isErr()
    let failed = deadIds.filterIt(store[NumberFailedConnBook][it] == 1)
    check failed.len == 1
    if failed.len == 1:
      check:
        not sender.inPool(failed[0])
        sender.getMixNodePoolSize() == 3
        deadIds.filterIt(it != failed[0]).allIt(store[NumberFailedConnBook][it] == 0)

  asyncTest "a silent first hop leaves the pool after the send stops its dial":
    ## The send stops its dial before libp2p has a result. A pool dial follows.
    let net = await startMixNodes(25960)
    let servers = @[silentServer(25976), silentServer(25977), silentServer(25978)]
    let deadBootnodes = deadBootnodes(25976, 25977, 25978)
    let sender = await net.addNatSender(25964, 25979, @[net.infos[0]] & deadBootnodes)
    defer:
      await net.stop(@[sender])
      for server in servers:
        server.stop()
        await server.closeWait()
    let store = sender.switch.peerStore
    let deadIds = deadBootnodes.mapIt(it.peerId)

    # `withTimeout` stops the send, as `publishOverMix` does.
    let sending = sender.wakuMix.anonymizeLocalProtocolSend(
      newAsyncQueue[seq[byte]](),
      toBytes("silent-first-hop"),
      WakuLightPushCodec,
      MixDestination.exitNode(net.exit.peerInfo.peerId),
      0'u8,
    )
    check:
      not await sending.withTimeout(chronos.seconds(1))
      deadIds.allIt(store[NumberFailedConnBook][it] == 0)

    await sleepAsync(DefaultDialTimeout + chronos.seconds(1))
    let failed = deadIds.filterIt(store[NumberFailedConnBook][it] == 1)
    check failed.len == 1
    if failed.len == 1:
      check:
        not sender.inPool(failed[0])
        deadIds.filterIt(it != failed[0]).allIt(store[NumberFailedConnBook][it] == 0)

suite "Waku Mix - failed dials and reconnect":
  asyncTest "the wait between dials doubles and has a limit":
    let node = await mixNode()
    node.wakuMix.pool.reconnectBackoff = chronos.seconds(1)
    node.wakuMix.pool.reconnectMaxBackoff = chronos.seconds(5)
    check:
      node.wakuMix.pool.backoff(1) == chronos.seconds(1)
      node.wakuMix.pool.backoff(2) == chronos.seconds(2)
      node.wakuMix.pool.backoff(3) == chronos.seconds(4)
      node.wakuMix.pool.backoff(4) == chronos.seconds(5)
      node.wakuMix.pool.backoff(100) == chronos.seconds(5)

  asyncTest "a missing reply records nothing against any peer":
    let hold = ExitHold.new()
    let net = await startMixNodes(25880, hold)
    let sender = await net.addNatSender(25884, 25899, net.infos)
    defer:
      await net.stop(@[sender])
    let store = sender.switch.peerStore

    # The sender loses its connections while the request is at the exit.
    await net.connectOnly(sender, 1)
    let sending = net.send(sender, "missing-reply")
    check await hold.entered.wait().withTimeout(chronos.seconds(5))
    await net.disconnectAll(sender)
    hold.release.fire()

    let lost = await sending
    check:
      not lost.acked
      lost.error.contains("timed out")
      lost.published
      net.nodes.allIt(store[NumberFailedConnBook][it.peerInfo.peerId] == 0)
      sender.getMixNodePoolSize() == MixNodeCount

  asyncTest "a failed mix stream dial counts only for a pool peer at its hop":
    ## nim-libp2p-mix also dials addresses that another node chose.
    let node = await startMixNode(25920)
    defer:
      await node.stop()
    let store = node.switch.peerStore
    let dead = MultiAddress.init("/ip4/127.0.0.1/tcp/25921").tryGet()
    let otherDead = MultiAddress.init("/ip4/127.0.0.1/tcp/25923").tryGet()
    let mixPeer = node.discover(@[$dead])
    let lightpushPeer = node.discover(@[$dead])
    let packetAddressPeer = node.discover(@[$dead])
    let lateMixPeer = node.discover(@[$dead])
    let unknown = PeerId.init(generateSecp256k1Key()).tryGet()

    proc dialFails(
        peerId: PeerId, codec: string, address = dead
    ): Future[bool] {.async.} =
      try:
        discard await node.switch.dial(peerId, @[address], @[codec])
        return false
      except DialFailedError:
        return true

    check:
      await dialFails(mixPeer, MixProtocolID)
      await dialFails(lightpushPeer, WakuLightPushCodec)
      await dialFails(unknown, MixProtocolID)
      await dialFails(packetAddressPeer, MixProtocolID, otherDead)
    check:
      store[NumberFailedConnBook][mixPeer] == 1
      not node.inPool(mixPeer)
      store[NumberFailedConnBook][lightpushPeer] == 0
      node.inPool(lightpushPeer)
      store[NumberFailedConnBook][unknown] == 0
      store[NumberFailedConnBook][packetAddressPeer] == 0
      node.inPool(packetAddressPeer)

    let stamp = store[LastFailedConnBook][mixPeer]
    check:
      await dialFails(mixPeer, MixProtocolID)
      store[NumberFailedConnBook][mixPeer] == 1
      store[LastFailedConnBook][mixPeer] == stamp

    # After the pool stops, a failure records nothing.
    await node.wakuMix.pool.stop()
    check:
      await dialFails(lateMixPeer, MixProtocolID)
      store[NumberFailedConnBook][lateMixPeer] == 0

  asyncTest "a mix dial that fails at a limit of this node blames no peer":
    let node = await startMixNode(25930)
    defer:
      await node.stop()
    let dead = MultiAddress.init("/ip4/127.0.0.1/tcp/25931").tryGet()
    let peer = node.discover(@[$dead])
    for error in [
      "failed getOutgoingSlot in establishConnection: Total outgoing connections limit reached",
      "Per peer connections limit reached", "internalConnect can't dial self!",
    ]:
      for observer in DeliveryDialer(node.switch.dialer).dialFailureObservers:
        observer(peer, @[dead], @[MixProtocolID], error)
    check:
      node.switch.peerStore[NumberFailedConnBook][peer] == 0
      node.inPool(peer)

  asyncTest "a stopped dial at an address from a packet starts no pool dial":
    ## The hop address refuses at once, so a wrong pool dial would record at once.
    let hanging = silentServer(25926)
    let node = await startMixNode(25927)
    defer:
      await node.stop()
      hanging.stop()
      await hanging.closeWait()
    let store = node.switch.peerStore
    let peerId = node.discover(@["/ip4/127.0.0.1/tcp/25928"])
    let packetAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/25926").tryGet()

    let stopped = node.switch.dial(peerId, @[packetAddress], @[MixProtocolID])
    await sleepAsync(chronos.milliseconds(200))
    await stopped.cancelAndWait()
    await sleepAsync(chronos.milliseconds(500))
    check:
      store[NumberFailedConnBook][peerId] == 0
      node.inPool(peerId)

  asyncTest "a stream that fails on a standing connection records nothing":
    ## The peer does not serve mix. The connection stays.
    let target = await startNodeWithoutMix(25924)
    let node = await startMixNode(25925)
    defer:
      await node.stop()
      await target.stop()
    let targetId = target.peerInfo.peerId
    let address = MultiAddress.init("/ip4/127.0.0.1/tcp/25924").tryGet()
    node.discover(@[$address], targetId)
    await node.switch.connect(targetId, @[address])
    check node.inPool(targetId)

    expect DialFailedError:
      discard await node.switch.dial(targetId, @[address], @[MixProtocolID])
    let store = node.switch.peerStore
    check:
      node.switch.isConnected(targetId)
      store[NumberFailedConnBook][targetId] == 0
      store[ConnectionBook][targetId] != CannotConnect
      node.inPool(targetId)

  asyncTest "a pool loop pass removes a peer whose address has expired":
    let node = await mixNode()
    let peerId = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    check node.getMixNodePoolSize() == 1

    let book = node.switch.peerStore[AddressBook]
    var entries = book.book[peerId]
    for entry in entries.mitems:
      entry.lastUpdated = Moment.now() - chronos.hours(2)
    book.book[peerId] = entries # No handler runs for a direct write.
    check node.getMixNodePoolSize() == 1

    await node.wakuMix.pool.maintain()
    check node.getMixNodePoolSize() == 0

  asyncTest "a reconnect returns a peer to the pool after a successful dial":
    let target = await startNodeWithoutMix(25520)
    let node = await startMixNode(25521)
    defer:
      await node.stop()
      await target.stop()

    node.discover(@["/ip4/127.0.0.1/tcp/25520"], target.peerInfo.peerId)
    let store = node.switch.peerStore
    let targetId = target.peerInfo.peerId
    check node.inPool(targetId)

    node.peerManager.recordDialFailure(targetId, "test")
    check not node.inPool(targetId)

    # The backoff is not over, so the pass dials nothing.
    await node.wakuMix.pool.reconnect()
    check:
      store[NumberFailedConnBook][targetId] == 1
      not node.switch.isConnected(targetId)

    node.wakuMix.pool.reconnectBackoff = chronos.milliseconds(0)
    await node.wakuMix.pool.reconnect()
    check:
      store[NumberFailedConnBook][targetId] == 0
      node.switch.isConnected(targetId)
      node.inPool(targetId)

  asyncTest "a reconnect dials over quic and the hop moves to quic":
    let target = await startNodeWithoutMix(25740, quicEnabled = true)
    let node = await startMixNode(25741, quicEnabled = true)
    node.wakuMix.pool.reconnectBackoff = chronos.milliseconds(0)
    defer:
      await node.stop()
      await target.stop()

    # tcp comes first, as in an announced address list.
    let targetId = node.discover(
      @["/ip4/127.0.0.1/tcp/25740", "/ip4/127.0.0.1/udp/25740/quic-v1"],
      target.peerInfo.peerId,
    )
    check $node.hopOf(targetId) == "/ip4/127.0.0.1/tcp/25740"
    node.peerManager.recordDialFailure(targetId, "test")
    check not node.inPool(targetId)

    await node.wakuMix.pool.reconnect()
    check:
      node.switch.peerStore[NumberFailedConnBook][targetId] == 0
      node.inPool(targetId)
      $node.hopOf(targetId) == "/ip4/127.0.0.1/udp/25740/quic-v1"

  asyncTest "a peer that keeps failing waits longer before the next dial":
    let node = await startMixNode(25530)
    defer:
      await node.stop()

    let dead = node.discover(@["/ip4/127.0.0.1/tcp/25539"])
    let store = node.switch.peerStore
    node.peerManager.recordDialFailure(dead, "test")
    # The first failure is old. The second one comes from the dial of this pass.
    store[LastFailedConnBook][dead] = Moment.init(0, Second)
    node.wakuMix.pool.reconnectBackoff = chronos.seconds(30)
    await node.wakuMix.pool.reconnect()
    check:
      store[NumberFailedConnBook][dead] == 2
      not node.inPool(dead)

    # Now it waits 60 seconds, so the next pass leaves it alone.
    await node.wakuMix.pool.reconnect()
    check store[NumberFailedConnBook][dead] == 2

  asyncTest "a reconnect does not dial a peer whose address the policy refuses":
    let node = await startMixNode(25540, addressPolicy = mixAddressPolicy(false))
    defer:
      await node.stop()

    let privatePeer = node.discover(@["/ip4/192.168.3.4/tcp/30303"])
    let store = node.switch.peerStore
    node.peerManager.recordDialFailure(privatePeer, "test")
    store[LastFailedConnBook][privatePeer] = Moment.init(0, Second)
    await node.wakuMix.pool.reconnect()
    check store[NumberFailedConnBook][privatePeer] == 1

  asyncTest "the pool loop runs while mix runs":
    let target = await startNodeWithoutMix(25550)
    let node = await startMixNode(25551, interval = chronos.milliseconds(100))
    node.wakuMix.pool.reconnectBackoff = chronos.milliseconds(0)
    defer:
      await node.stop()
      await target.stop()

    let targetId = node.discover(@["/ip4/127.0.0.1/tcp/25550"], target.peerInfo.peerId)
    node.peerManager.recordDialFailure(targetId, "test")
    check not node.inPool(targetId)

    checkUntilTimeout:
      node.inPool(targetId)
