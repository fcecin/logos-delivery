{.used.}

## Reply paths (#4357). The fixture is in `tests/testlib/wakumix.nim`.

import
  std/[net, sequtils, strutils],
  testutils/unittests,
  chronos,
  results,
  libp2p/[multiaddress, peerid, peerinfo, switch],
  libp2p/crypto/crypto,
  libp2p/stream/connection,
  libp2p_mix/mix_protocol
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/discovery/peer_discovery_interface,
  ../testlib/[wakucore, wakumix]

suite "Waku Mix - reply path selection":
  asyncTest "the last reply hop is always a peer with a connection to the sender":
    let net = await startMixNodes(25200)
    let sender = await net.addNatSender(25204, 25219, net.infos)
    defer:
      await net.stop(@[sender])
    await net.connectOnly(sender, 1)

    let exitId = net.exit.peerInfo.peerId
    let connectedId = net.nodes[1].peerInfo.peerId
    let senderId = sender.peerInfo.peerId
    var firstHops: seq[PeerId]
    for _ in 0 ..< 60:
      let path = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
      check:
        path.len == 3
        path[2].peerId == senderId
        path[1].peerId == connectedId
        path[0].peerId != connectedId
        path[0].peerId notin [exitId, senderId]
      if path[0].peerId notin firstHops:
        firstHops.add(path[0].peerId)
    # The hop before the last one stays random among the other candidates.
    check firstHops.len == 2

  asyncTest "a sender whose pool peers all dialed it sends at once":
    ## An inbound connection carries the reply, so the sender dials nothing.
    let net = await startMixNodes(26140)
    let sender = await net.addNatSender(26144, 26159, net.infos)
    defer:
      await net.stop(@[sender])
    await net.disconnectAll(sender)
    let exitId = net.exit.peerInfo.peerId
    for i in 1 ..< MixNodeCount:
      await net.nodes[i].switch.connect(
        sender.peerInfo.peerId, sender.switch.peerInfo.listenAddrs
      )
    let started = Moment.now()
    check:
      (await sender.wakuMix.prepareReplyPath(exitId)).isOk()
      Moment.now() - started < chronos.seconds(1)
      net.nodes[1 ..^ 1].allIt(
        sender.switch.connManager.selectMuxer(it.peerInfo.peerId, Direction.Out).isNil()
      )

  asyncTest "the reply path ends at the current self hop":
    let net = await startMixNodes(25560)
    let sender = await net.addNatSender(25564, 25579, net.infos)
    defer:
      await net.stop(@[sender])
    await net.connectOnly(sender, 2)
    let exitId = net.exit.peerInfo.peerId

    let before = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
    check $before[2].multiAddr == "/ip4/127.0.0.1/tcp/25579"

    let moved = MultiAddress.init("/ip4/127.0.0.1/tcp/25578").tryGet()
    check sender.wakuMix.updateSelfHop(@[moved], @[]) == Opt.some(moved)
    let after = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
    check:
      after[2].peerId == sender.peerInfo.peerId
      after[2].multiAddr == moved
      after[1].peerId == net.nodes[2].peerInfo.peerId

  asyncTest "no reply path when only the exit, or no peer, has a connection":
    let net = await startMixNodes(25240)
    let sender = await net.addNatSender(25244, 25259, net.infos)
    defer:
      await net.stop(@[sender])
    let exitId = net.exit.peerInfo.peerId

    await net.disconnectAll(sender)
    check sender.wakuMix.replyPath(exitId, exitId).error == NoReplyHopError

    await net.connectOnly(sender, 0)
    check sender.wakuMix.replyPath(exitId, exitId).error == NoReplyHopError

suite "Waku Mix - a sender behind NAT, end to end":
  for (suffix, quicEnabled, transport, basePort) in [
    ("", true, "/quic-v1", 25280), (" over tcp", false, "/tcp/", 26160)
  ]:
    asyncTest "a sender with one connection" & suffix & " gets every reply over it":
      ## With the random last reply hop of nim-libp2p-mix, 12 sends pass by chance in
      ## under 0.1% of runs.
      let net = await startMixNodes(basePort)
      let sender = await net.addNatSender(
        basePort + 4, basePort + 19, net.infos, quicEnabled = quicEnabled
      )
      defer:
        await net.stop(@[sender])

      for i in 0 ..< 12:
        await net.connectOnly(sender, 1)
        check transport in $sender.hopOf(net.nodes[1].peerInfo.peerId)
        let outcome = await net.send(sender, "one-connection-" & $i)
        check:
          outcome.acked
          outcome.published
          outcome.elapsed < MixReplyTimeout
          sender.wakuMix.surbCredsLen() == 0
          sender.noInboundConnections()

  asyncTest "a sender with no connection opens one before the send":
    let net = await startMixNodes(25320)
    let sender = await net.addNatSender(25324, 25339, net.infos)
    defer:
      await net.stop(@[sender])

    for i in 0 ..< 3:
      await net.disconnectAll(sender)
      let outcome = await net.send(sender, "unconnected-" & $i)
      check:
        outcome.acked
        outcome.published
        sender.wakuMix.surbCredsLen() == 0
        net.connectedNodes(sender).anyIt(it != 0)
        sender.noInboundConnections()

  asyncTest "a connection lost before the reply gives a bounded failure, then recovery":
    let hold = ExitHold.new()
    let net = await startMixNodes(25400, hold)
    let sender = await net.addNatSender(25404, 25419, net.infos)
    defer:
      await net.stop(@[sender])

    # Node 1 is the only possible last reply hop.
    await net.connectOnly(sender, 1)
    let sending = net.send(sender, "lost-connection")
    check await hold.entered.wait().withTimeout(chronos.seconds(5))
    # Cut every connection of the sender while the request is at the exit.
    await net.disconnectAll(sender)
    hold.release.fire()

    let lost = await sending
    check:
      not lost.acked
      lost.error.contains("timed out")
      lost.elapsed < MixReplyTimeout + chronos.seconds(1)
      # The exit published the message. Only the reply was lost.
      lost.published
      sender.wakuMix.surbCredsLen() == 0

    let next = await net.send(sender, "lost-connection-recovered")
    check:
      next.acked
      next.published
      sender.wakuMix.surbCredsLen() == 0
