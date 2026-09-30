{.used.}

## Reply links (#4357). The fixture is in `tests/testlib/wakumix.nim`.

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
    let net = await setupCores(25200)
    let sender = await net.addNatSender(25204, 25219, net.infos)
    defer:
      await net.stop(@[sender])
    await net.linkOnly(sender, 1)

    let exitId = net.exit.peerInfo.peerId
    let linkedId = net.cores[1].peerInfo.peerId
    let senderId = sender.peerInfo.peerId
    var firstHops: seq[PeerId]
    for _ in 0 ..< 60:
      let path = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
      check:
        path.len == 3
        path[2].peerId == senderId
        path[1].peerId == linkedId
        path[0].peerId != linkedId
        path[0].peerId notin [exitId, senderId]
      if path[0].peerId notin firstHops:
        firstHops.add(path[0].peerId)
    # The hop before the last one stays random among the other candidates.
    check firstHops.len == 2

  asyncTest "a sender whose pool peers all dialed it sends at once":
    ## An inbound link carries the reply, so the sender dials nothing.
    let net = await setupCores(26140)
    let sender = await net.addNatSender(26144, 26159, net.infos)
    defer:
      await net.stop(@[sender])
    await net.isolate(sender)
    let exitId = net.exit.peerInfo.peerId
    for i in 1 ..< CoreCount:
      await net.cores[i].switch.connect(
        sender.peerInfo.peerId, sender.switch.peerInfo.listenAddrs
      )
    let started = Moment.now()
    check:
      (await sender.wakuMix.prepareReplyLink(exitId)).isOk()
      Moment.now() - started < chronos.seconds(1)
      net.cores[1 ..^ 1].allIt(
        sender.switch.connManager.selectMuxer(it.peerInfo.peerId, Direction.Out).isNil()
      )

  asyncTest "the reply path ends at the current own hop":
    let net = await setupCores(25560)
    let sender = await net.addNatSender(25564, 25579, net.infos)
    defer:
      await net.stop(@[sender])
    await net.linkOnly(sender, 2)
    let exitId = net.exit.peerInfo.peerId

    let before = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
    check $before[2].multiAddr == "/ip4/127.0.0.1/tcp/25579"

    let moved = MultiAddress.init("/ip4/127.0.0.1/tcp/25578").tryGet()
    check sender.wakuMix.updateSelfHop(@[moved], @[]) == Opt.some(moved)
    let after = sender.wakuMix.replyPath(exitId, exitId).expect("reply path")
    check:
      after[2].peerId == sender.peerInfo.peerId
      after[2].multiAddr == moved
      after[1].peerId == net.cores[2].peerInfo.peerId

  asyncTest "no reply path when only the exit, or no peer, has a connection":
    let net = await setupCores(25240)
    let sender = await net.addNatSender(25244, 25259, net.infos)
    defer:
      await net.stop(@[sender])
    let exitId = net.exit.peerInfo.peerId

    await net.isolate(sender)
    check sender.wakuMix.replyPath(exitId, exitId).error == NoReplyLinkError

    await net.linkOnly(sender, 0)
    check sender.wakuMix.replyPath(exitId, exitId).error == NoReplyLinkError

suite "Waku Mix - a sender behind NAT, end to end":
  for (suffix, quicEnabled, transport, basePort) in [
    ("", true, "/quic-v1", 25280), (" over tcp", false, "/tcp/", 26160)
  ]:
    asyncTest "a sparse sender" & suffix & " gets every reply over its one connection":
      ## With the random last reply hop of nim-libp2p-mix, 12 sends pass by chance in
      ## under 0.1% of runs.
      let net = await setupCores(basePort)
      let sender = await net.addNatSender(
        basePort + 4, basePort + 19, net.infos, quicEnabled = quicEnabled
      )
      defer:
        await net.stop(@[sender])

      for i in 0 ..< 12:
        await net.linkOnly(sender, 1)
        check transport in $sender.hopOf(net.cores[1].peerInfo.peerId)
        let outcome = await net.send(sender, "sparse-" & $i)
        check:
          outcome.acked
          outcome.published
          outcome.elapsed < MixReplyTimeout
          sender.wakuMix.surbCredsLen() == 0
          sender.noInboundConnections()

  asyncTest "a sender with no connection opens one before the send":
    let net = await setupCores(25320)
    let sender = await net.addNatSender(25324, 25339, net.infos)
    defer:
      await net.stop(@[sender])

    for i in 0 ..< 3:
      await net.isolate(sender)
      let outcome = await net.send(sender, "unlinked-" & $i)
      check:
        outcome.acked
        outcome.published
        sender.wakuMix.surbCredsLen() == 0
        net.linkedCores(sender).anyIt(it != 0)
        sender.noInboundConnections()

  asyncTest "a reply link lost before the reply gives a bounded failure, then recovery":
    let hold = ExitHold.new()
    let net = await setupCores(25400, hold)
    let sender = await net.addNatSender(25404, 25419, net.infos)
    defer:
      await net.stop(@[sender])

    # Core 1 is the only possible last reply hop.
    await net.linkOnly(sender, 1)
    let sending = net.send(sender, "lost-link")
    check await hold.entered.wait().withTimeout(chronos.seconds(5))
    # Cut every link of the sender while the request is at the exit.
    await net.isolate(sender)
    hold.release.fire()

    let lost = await sending
    check:
      not lost.acked
      lost.error.contains("timed out")
      lost.elapsed < MixReplyTimeout + chronos.seconds(1)
      # The exit published the message. Only the reply was lost.
      lost.published
      sender.wakuMix.surbCredsLen() == 0

    let next = await net.send(sender, "lost-link-recovered")
    check:
      next.acked
      next.published
      sender.wakuMix.surbCredsLen() == 0
