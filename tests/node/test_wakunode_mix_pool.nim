{.used.}

## The mix pool keeps its mix nodes when the peer store deletes them.

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
