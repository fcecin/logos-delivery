{.used.}

## The mix pool keeps its mix nodes when the peer store deletes them, and keeps
## its own record of failed dials.

import
  std/[net, sequtils],
  testutils/unittests,
  chronos,
  results,
  libp2p/[multiaddress, peerid, peerinfo, peerstore, switch],
  libp2p/crypto/crypto,
  libp2p_mix/[curve25519, mix_node, mix_protocol, pool]
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/node/delivery_dialer,
  ../testlib/[wakucore, wakumix]

suite "Waku Mix - the pool keeps its mix nodes":
  asyncTest "a configured node stays in the pool after a peer store delete":
    let configured = bootnode("/ip4/1.2.3.4/tcp/30303")
    let node = await mixNode(bootnodes = @[configured])
    let peerId = configured.peerId
    let before = node.wakuMix.nodePool.get(peerId).expect("pool entry")

    node.switch.peerStore.delete(peerId)
    check:
      peerId notin node.switch.peerStore[MixPubKeyBook]
      node.inPool(peerId)
    let after = node.wakuMix.nodePool.get(peerId).expect("pool entry")
    check:
      after.multiAddr == before.multiAddr
      after.mixPubKey == before.mixPubKey

    # A configured node does not age.
    node.wakuMix.pool.discoveredTtl = ZeroDuration
    await node.wakuMix.pool.maintain()
    check node.inPool(peerId)

  asyncTest "a send works after the peer store deletes each configured node":
    let net = await startMixNodes(26400)
    let sender = await net.addNatSender(26404, 26419, net.infos)
    defer:
      await net.stop(@[sender])
    await net.disconnectAll(sender)
    for info in net.infos:
      sender.switch.peerStore.delete(info.peerId)
    check sender.getMixNodePoolSize() == MixNodeCount

    let outcome = await net.send(sender, "after-delete")
    check:
      outcome.acked
      outcome.published

  asyncTest "a discovered node stays after a peer store delete, and leaves with no news":
    let node = await mixNode()
    let peerId = node.discover(@["/ip4/1.1.3.3/tcp/30303"])

    node.switch.peerStore.delete(peerId)
    await node.wakuMix.pool.maintain()
    check:
      node.inPool(peerId)
      $node.hopOf(peerId) == "/ip4/1.1.3.3/tcp/30303"

    node.wakuMix.pool.discoveredTtl = ZeroDuration
    await node.wakuMix.pool.maintain()
    check not node.inPool(peerId)

  asyncTest "a peer store delete of a connected peer keeps its pool entry":
    let target = await startNodeWithoutMix(26420)
    let node = await startMixNode(26421)
    defer:
      await node.stop()
      await target.stop()
    let targetId = node.discover(@["/ip4/127.0.0.1/tcp/26420"], target.peerInfo.peerId)
    await node.switch.connect(targetId, target.peerInfo.addrs)

    node.switch.peerStore.delete(targetId)
    check:
      node.inPool(targetId)
      $node.hopOf(targetId) == "/ip4/127.0.0.1/tcp/26420"

    # A connection is news, so the node does not age.
    node.wakuMix.pool.discoveredTtl = ZeroDuration
    await node.wakuMix.pool.maintain()
    check node.inPool(targetId)

  asyncTest "discovery updates a configured node, and a delete changes no hop":
    let configured = bootnode("/ip4/1.2.3.4/tcp/30303")
    let node = await mixNode(bootnodes = @[configured])
    let peerId = configured.peerId

    node.discover(@["/ip4/1.2.3.9/tcp/30303"], peerId)
    let discovered = node.switch.peerStore[MixPubKeyBook][peerId]
    check:
      discovered != configured.pubKey
      # The address from discovery comes before the configured address.
      $node.hopOf(peerId) == "/ip4/1.2.3.9/tcp/30303"
      node.wakuMix.nodePool.get(peerId).expect("pool entry").mixPubKey == discovered

    # The address that this node dialed last comes first.
    node.switch.peerStore[LastSeenOutboundBook][peerId] =
      Opt.some(MultiAddress.init("/ip4/1.2.3.10/tcp/30303").tryGet())
    check $node.hopOf(peerId) == "/ip4/1.2.3.10/tcp/30303"

    node.switch.peerStore.delete(peerId)
    check:
      $node.hopOf(peerId) == "/ip4/1.2.3.10/tcp/30303"
      node.wakuMix.nodePool.get(peerId).expect("pool entry").mixPubKey == discovered

  asyncTest "the configured address carries the hop when discovery gives only a name":
    ## Fleet nodes announce a dns4 name, which a hop cannot carry.
    let configured = bootnode("/ip4/1.2.3.4/tcp/30303")
    let node = await mixNode(bootnodes = @[configured])
    let peerId = configured.peerId

    node.switch.peerStore.delete(peerId)
    node.discover(@["/dns4/node.test/tcp/30303"], peerId)
    check node.inPool(peerId)
    if node.inPool(peerId):
      check $node.hopOf(peerId) == "/ip4/1.2.3.4/tcp/30303"

  asyncTest "a configured node keeps its copies when the configuration adds it again":
    let configured = bootnode("/ip4/1.2.3.4/tcp/30303")
    let node = await mixNode(bootnodes = @[configured])
    let peerId = configured.peerId
    node.switch.peerStore[ProtoBook][peerId] = @["/test/exit/1"]
    node.switch.peerStore.delete(peerId)

    # `addBootNodes` adds the entries that a name lookup resolved after the mount.
    node.wakuMix.addBootNodes(@[configured])
    check node.wakuMix.pool.hasProtocol(peerId, "/test/exit/1")

  asyncTest "a record without a mix key is no news":
    let node = await mixNode()
    node.wakuMix.pool.discoveredTtl = chronos.seconds(2)
    let silent = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    let active = node.discover(@["/ip4/1.1.3.4/tcp/30303"])

    await sleepAsync(chronos.milliseconds(1900))
    # The record of a node that stopped mix has no mix key.
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        silent, @[MultiAddress.init("/ip4/1.1.3.5/tcp/30303").tryGet()]
      )
    )
    node.discover(@["/ip4/1.1.3.4/tcp/30303"], active)
    await sleepAsync(chronos.milliseconds(200))
    await node.wakuMix.pool.maintain()
    check:
      not node.inPool(silent)
      node.inPool(active)

  asyncTest "the pool never holds this node itself":
    let node = await mixNode()
    let own = node.switch.peerInfo
    let keys = generateKeyPair().expect("mix key pair")
    node.wakuMix.pool.add(
      MixPubInfo.init(
        own.peerId,
        MultiAddress.init("/ip4/1.2.3.4/tcp/30303").tryGet(),
        keys.publicKey,
        own.publicKey.skkey,
      )
    )
    # The peer manager skips this node, so write the peer store directly.
    node.switch.peerStore.addPeer(
      RemotePeerInfo.init(
        own.peerId,
        @[MultiAddress.init("/ip4/1.2.3.4/tcp/30303").tryGet()],
        mixPubKey = Opt.some(keys.publicKey),
      )
    )
    check:
      node.switch.peerStore[MixPubKeyBook][own.peerId] == keys.publicKey
      not node.inPool(own.peerId)
      node.getMixNodePoolSize() == 0

  asyncTest "the limit of discovered nodes removes a node off the pool first, then the oldest":
    let configured =
      @[bootnode("/ip4/1.2.3.4/tcp/30303"), bootnode("/ip4/1.2.3.5/tcp/30303")]
    let node = await mixNode(bootnodes = configured)
    node.wakuMix.pool.maxDiscovered = 2
    let older = node.discover(@["/ip4/1.1.3.1/tcp/30303"])
    # The policy refuses this address, so the node is not a pool member.
    node.discover(@["/ip4/192.168.3.1/tcp/30303"])

    let newer = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    check:
      node.inPool(older)
      node.inPool(newer)

    let newest = node.discover(@["/ip4/1.1.3.4/tcp/30303"])
    check:
      not node.inPool(older)
      node.inPool(newer)
      node.inPool(newest)
      configured.allIt(node.inPool(it.peerId))
      node.getMixNodePoolSize() == 4

suite "Waku Mix - the failed dials of the pool":
  asyncTest "at the limit of discovered nodes, a node with a failed dial goes first":
    let node = await mixNode()
    node.wakuMix.pool.maxDiscovered = 2
    let older = node.discover(@["/ip4/1.1.3.1/tcp/30303"])
    let failed = node.discover(@["/ip4/1.1.3.2/tcp/30303"])
    node.wakuMix.pool.countFailure(failed)

    let newer = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    check:
      # The pool no longer knows the failed node, so its failed dial is gone.
      not node.failed(failed)
      node.inPool(older)
      node.inPool(newer)

  asyncTest "a failed dial does not take a configured node off paths":
    let configured = bootnode("/ip4/1.2.3.4/tcp/30303")
    let node = await mixNode(bootnodes = @[configured])
    node.wakuMix.pool.countFailure(configured.peerId)
    check:
      not node.failed(configured.peerId)
      node.inPool(configured.peerId)

    # A discovered node with a failed dial returns to paths when the
    # configuration adds it.
    let entry = bootnode("/ip4/1.2.3.5/tcp/30303")
    node.discover(@["/ip4/1.2.3.5/tcp/30303"], entry.peerId)
    node.wakuMix.pool.countFailure(entry.peerId)
    check not node.inPool(entry.peerId)
    node.wakuMix.addBootNodes(@[entry])
    check:
      not node.failed(entry.peerId)
      node.inPool(entry.peerId)

  asyncTest "a configured node gets no more dials after its dial fails":
    let accepted = new int
    proc serve(server: StreamServer, transp: StreamTransport) {.async: (raises: []).} =
      accepted[].inc()
      await transp.closeWait()

    let server = createStreamServer(
      initTAddress("127.0.0.1", Port(26460)), serve, {ServerFlags.ReuseAddr}
    )
    server.start()
    let node = await startMixNode(26461)
    let other = await node.addOtherConnection(26462)
    defer:
      await node.stop()
      await other.stop()
      server.stop()
      await server.closeWait()
    let configured = bootnode("/ip4/127.0.0.1/tcp/26460")
    node.wakuMix.addBootNodes(@[configured])
    let peerId = configured.peerId
    let hop = MultiAddress.init("/ip4/127.0.0.1/tcp/26460").tryGet()

    check:
      not await node.wakuMix.pool.dial(peerId)
      accepted[] == 1
      not node.failed(peerId)
      node.inPool(peerId)

    await node.wakuMix.pool.maintain()
    # A send that cancels its first hop dial starts no pool dial either.
    for handler in DeliveryDialer(node.switch.dialer).dialEventHandlers:
      handler(DialEventKind.Cancelled, peerId, @[hop], @[MixProtocolID], "")
    await sleepAsync(chronos.milliseconds(300))
    check:
      accepted[] == 1
      node.inPool(peerId)

  asyncTest "a failed peer stays out after a peer store delete and a new record":
    let node = await mixNode()
    let peerId = node.discover(@["/ip4/1.1.3.3/tcp/30303"])
    node.wakuMix.pool.countFailure(peerId)

    node.switch.peerStore.delete(peerId)
    node.discover(@["/ip4/1.1.3.3/tcp/30303"], peerId)
    check:
      node.failed(peerId)
      not node.inPool(peerId)

  asyncTest "mix and the peer manager keep separate records of failed dials":
    let node = await startMixNode(26430)
    let other = await node.addOtherConnection(26431)
    defer:
      await node.stop()
      await other.stop()
    let store = node.switch.peerStore

    let mixDead = node.discover(@["/ip4/127.0.0.1/tcp/26432"])
    check:
      not await node.wakuMix.pool.dial(mixDead)
      node.failed(mixDead)
      store[NumberFailedConnBook][mixDead] == 0
      store[ConnectionBook][mixDead] != CannotConnect

    let relayDead = node.discover(@["/ip4/127.0.0.1/tcp/26433"])
    check:
      not await node.peerManager.connectPeer(store.getPeer(relayDead))
      store[NumberFailedConnBook][relayDead] == 1
      not node.failed(relayDead)
      node.inPool(relayDead)

  asyncTest "a failed dial counts only while this node has another connection":
    let node = await startMixNode(26440)
    defer:
      await node.stop()
    let dead = node.discover(@["/ip4/127.0.0.1/tcp/26441"])
    check:
      node.switch.connectedPeers().len == 0
      not await node.wakuMix.pool.dial(dead)
      not node.failed(dead)
      node.inPool(dead)

    let other = await node.addOtherConnection(26442)
    defer:
      await other.stop()
    check:
      not await node.wakuMix.pool.dial(dead)
      node.failed(dead)
      not node.inPool(dead)
