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
    # The exit is real. One attempt dials both dead peers.
    let sender = await net.addNatSender(25364, 25379, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
    await net.connectExit(sender)
    let deadIds = sender.deadPeers(25376, 25377)
    check sender.getMixNodePoolSize() == 3

    let failuresBefore = logos_delivery_mix_reply_hop_failures.value()
    let first = await net.send(sender, "dead-first")
    check:
      logos_delivery_mix_reply_hop_failures.value() == failuresBefore + 1
      not first.acked
      first.error.contains(NoReplyHopError)
      first.elapsed < MixReplyHopTimeout + chronos.seconds(1)
      not first.published
      sender.wakuMix.surbCredsLen() == 0
      deadIds.allIt(sender.failed(it))
      sender.getMixNodePoolSize() == 1

    # With no candidate left, the next attempt stops before any dial.
    let second = await net.send(sender, "dead-second")
    check:
      logos_delivery_mix_reply_hop_failures.value() == failuresBefore + 2
      not second.acked
      not second.published
      second.elapsed < chronos.seconds(1)
      deadIds.allIt(sender.failed(it))
      sender.wakuMix.surbCredsLen() == 0

    sender.wakuMix.addBootNodes(net.infos[1 ..^ 1])
    let third = await net.send(sender, "dead-recovered")
    check:
      third.acked
      third.published
      sender.wakuMix.surbCredsLen() == 0

  asyncTest "a sender with no connection counts no failed dial against any peer":
    ## The dials cannot tell a dead peer from a network that does not work.
    let net = await startMixNodes(25600)
    let sender = await net.addNatSender(25604, 25619, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
    let deadIds = sender.deadPeers(25616, 25617)

    check sender.switch.connectedPeers().len == 0
    let isolated = await net.send(sender, "isolated-dead")
    check:
      not isolated.acked
      not isolated.published
      sender.wakuMix.surbCredsLen() == 0
      deadIds.allIt(not sender.failed(it))
      sender.getMixNodePoolSize() == 3

  asyncTest "a send in the next pass joins the dial that is still running":
    let net = await startMixNodes(25800)
    let accepted = new int
    let hanging = @[silentServer(25816, accepted), silentServer(25817, accepted)]
    let sender = await net.addNatSender(25804, 25819, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
      for server in hanging:
        server.stop()
        await server.closeWait()
    await net.connectExit(sender)
    let deadIds = sender.deadPeers(25816, 25817)

    let started = Moment.now()
    let first = await net.send(sender, "next-pass-1")
    let second = await net.send(sender, "next-pass-2")
    check:
      not first.acked
      not second.acked
    await sleepAsync(started + DefaultDialTimeout + chronos.seconds(1) - Moment.now())
    check deadIds.allIt(sender.failed(it))
    await sleepAsync(
      started + 2 * DefaultDialTimeout + chronos.seconds(1) - Moment.now()
    )
    # One dial reached each dead peer. The second send joined it.
    check:
      deadIds.allIt(sender.failed(it))
      accepted[] == 2

  asyncTest "a send after the pool stops dials nothing and records nothing":
    let net = await startMixNodes(25840)
    let sender = await net.addNatSender(25844, 25859, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
    await net.connectExit(sender)
    let deadIds = sender.deadPeers(25856, 25857)

    await sender.wakuMix.pool.stop()
    let outcome = await net.send(sender, "after-stop")
    check:
      not outcome.acked
      not outcome.published
      outcome.error.contains(MixStoppingError)
      outcome.elapsed < chronos.seconds(1)
      deadIds.allIt(not sender.failed(it))

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
      not node.failed(targetId)
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
    sender.wakuMix.pool.countFailure(dead)
    check sender.getMixNodePoolSize() == MixNodeCount

    # Discovery does not clear the failed dial.
    sender.discover(@["/ip4/127.0.0.1/tcp/25458"], dead)
    check sender.getMixNodePoolSize() == MixNodeCount

    for i in 0 ..< 4:
      let outcome = await net.send(sender, "known-dead-" & $i)
      check:
        outcome.acked
        outcome.published
        sender.wakuMix.surbCredsLen() == 0

  asyncTest "a failed dial to the first hop takes that peer out of the pool":
    ## Each candidate for the first hop is unreachable, so each path fails at it.
    let net = await startMixNodes(25480)
    let sender = await net.addNatSender(25484, 25499, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
    await net.connectExit(sender)
    let deadIds = sender.deadPeers(25496, 25497, 25498)

    let sent = await sender.wakuMix.anonymizeLocalProtocolSend(
      newAsyncQueue[seq[byte]](),
      toBytes("first-hop"),
      WakuLightPushCodec,
      MixDestination.exitNode(net.exit.peerInfo.peerId),
      0'u8,
    )
    check sent.isErr()
    let failed = deadIds.filterIt(sender.failed(it))
    check failed.len == 1
    if failed.len == 1:
      check:
        not sender.inPool(failed[0])
        sender.getMixNodePoolSize() == 3
        deadIds.filterIt(it != failed[0]).allIt(not sender.failed(it))

  asyncTest "a silent first hop leaves the pool when the send cancels its dial":
    ## The send cancels its dial before libp2p has a result. The cancel counts.
    let net = await startMixNodes(25960)
    let servers = @[silentServer(25976), silentServer(25977), silentServer(25978)]
    let sender = await net.addNatSender(25964, 25979, @[net.infos[0]])
    defer:
      await net.stop(@[sender])
      for server in servers:
        server.stop()
        await server.closeWait()
    await net.connectExit(sender)
    let deadIds = sender.deadPeers(25976, 25977, 25978)

    # `withTimeout` cancels the send, as `publishOverMix` does.
    let sending = sender.wakuMix.anonymizeLocalProtocolSend(
      newAsyncQueue[seq[byte]](),
      toBytes("silent-first-hop"),
      WakuLightPushCodec,
      MixDestination.exitNode(net.exit.peerInfo.peerId),
      0'u8,
    )
    check not await sending.withTimeout(chronos.seconds(1))
    await sleepAsync(chronos.milliseconds(200))
    let failed = deadIds.filterIt(sender.failed(it))
    check failed.len == 1
    if failed.len == 1:
      check:
        not sender.inPool(failed[0])
        deadIds.filterIt(it != failed[0]).allIt(not sender.failed(it))

suite "Waku Mix - failed dials and the wait":
  asyncTest "a missing reply records nothing against any peer":
    let hold = ExitHold.new()
    let net = await startMixNodes(25880, hold)
    let sender = await net.addNatSender(25884, 25899, net.infos)
    defer:
      await net.stop(@[sender])

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
      net.nodes.allIt(not sender.failed(it.peerInfo.peerId))
      sender.getMixNodePoolSize() == MixNodeCount

  asyncTest "a failed mix stream dial counts only for a pool peer at its hop":
    ## nim-libp2p-mix also dials addresses that another node chose.
    let node = await startMixNode(25920)
    let other = await node.addOtherConnection(25922)
    defer:
      await node.stop()
      await other.stop()
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
      node.failed(mixPeer)
      not node.inPool(mixPeer)
      not node.failed(lightpushPeer)
      node.inPool(lightpushPeer)
      not node.failed(unknown)
      not node.failed(packetAddressPeer)
      node.inPool(packetAddressPeer)

    # The peer is out of the pool, so a second failure does not count.
    check:
      await dialFails(mixPeer, MixProtocolID)
      node.failed(mixPeer)

    # After the pool stops, a failure records nothing.
    await node.wakuMix.pool.stop()
    check:
      await dialFails(lateMixPeer, MixProtocolID)
      not node.failed(lateMixPeer)

  asyncTest "a cancelled dial at an address from a packet records nothing":
    ## A dial to an address that another node chose says nothing about the peer.
    let hanging = silentServer(25926)
    let node = await startMixNode(25927)
    let other = await node.addOtherConnection(25929)
    defer:
      await node.stop()
      await other.stop()
      hanging.stop()
      await hanging.closeWait()
    let peerId = node.discover(@["/ip4/127.0.0.1/tcp/25928"])
    let packetAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/25926").tryGet()

    let cancelled = node.switch.dial(peerId, @[packetAddress], @[MixProtocolID])
    await sleepAsync(chronos.milliseconds(200))
    await cancelled.cancelAndWait()
    await sleepAsync(chronos.milliseconds(500))
    check:
      not node.failed(peerId)
      node.inPool(peerId)

  asyncTest "a stream that fails on an existing connection records nothing":
    ## The peer does not serve mix. The connection stays.
    let target = await startNodeWithoutMix(25924)
    let node = await startMixNode(25925)
    let other = await node.addOtherConnection(25933)
    defer:
      await node.stop()
      await target.stop()
      await other.stop()
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
      not node.failed(targetId)
      store[ConnectionBook][targetId] != CannotConnect
      node.inPool(targetId)

  asyncTest "a discovered peer keeps its expired address, and leaves with no news":
    let node = await mixNode()
    let peerId = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    check node.getMixNodePoolSize() == 1

    let book = node.switch.peerStore[AddressBook]
    var entries = book.book[peerId]
    for entry in entries.mitems:
      entry.lastUpdated = Moment.now() - chronos.hours(2)
    book.book[peerId] = entries # No handler runs for a direct write.
    await node.wakuMix.pool.maintain()
    check:
      node.switch.peerStore[AddressBook][peerId].len == 0
      $node.hopOf(peerId) == "/ip4/1.1.3.3/tcp/30303"

    node.wakuMix.pool.discoveredTtl = ZeroDuration
    await node.wakuMix.pool.maintain()
    check node.getMixNodePoolSize() == 0

  asyncTest "a failed peer returns to paths after the wait, with no dial":
    let accepted = new int
    proc serve(server: StreamServer, transp: StreamTransport) {.async: (raises: []).} =
      accepted[].inc()
      await transp.closeWait()

    let server = createStreamServer(
      initTAddress("127.0.0.1", Port(25545)), serve, {ServerFlags.ReuseAddr}
    )
    server.start()
    let node = await startMixNode(25546)
    defer:
      await node.stop()
      server.stop()
      await server.closeWait()
    let peerId = node.discover(@["/ip4/127.0.0.1/tcp/25545"])
    node.wakuMix.pool.countFailure(peerId)
    await node.wakuMix.pool.maintain()
    check not node.inPool(peerId)

    node.wakuMix.pool.failureWait = ZeroDuration
    await node.wakuMix.pool.maintain()
    await sleepAsync(chronos.milliseconds(300))
    check:
      node.inPool(peerId)
      not node.failed(peerId)
      accepted[] == 0

  asyncTest "the pool loop returns a failed peer while mix runs":
    let node = await startMixNode(25550, interval = chronos.milliseconds(100))
    defer:
      await node.stop()
    let peerId = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    node.wakuMix.pool.failureWait = chronos.milliseconds(300)
    node.wakuMix.pool.countFailure(peerId)
    check not node.inPool(peerId)

    checkUntilTimeout:
      node.inPool(peerId)
