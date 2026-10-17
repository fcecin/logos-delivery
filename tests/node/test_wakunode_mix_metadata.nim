{.used.}

import testutils/unittests, chronos, results
import libp2p/[crypto/crypto, multiaddress, peerid, switch]
import libp2p_mix/curve25519

import
  logos_delivery/waku/[waku_core, waku_node, waku_mix],
  logos_delivery/waku/common/waku_protocol,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/node/health_monitor/
    [health_status, protocol_health, node_health_monitor],
  ../testlib/[wakucore, wakunode]

suite "Waku Mix - managed pool":
  asyncTest "the metadata of a mix exit stays until the exit leaves the pool":
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, mixKeys.privateKey, @[], addressPolicy = defaultAddressPolicy
      )
    ).expect("mount mix")
    let peerKey = generateSecp256k1Key()
    let peerId = PeerId.init(peerKey).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    let address = MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[address],
        protocols = @[WakuLightPushCodec],
        mixPubKey = Opt.some(hopKeys.publicKey),
      )
    )
    node.switch.peerStore[ShardBook][peerId] = @[0'u16]

    node.switch.peerStore.delete(peerId)

    check:
      node.wakuMix.nodePool.get(peerId).isSome()
      node.wakuMix.peerMetadata.hasProtocol(peerId, WakuLightPushCodec)
      node.wakuMix.peerMetadata.hasShard(peerId, DefaultClusterId, 0'u16)

    discard node.wakuMix.nodePool.remove(peerId)

    check:
      not node.wakuMix.peerMetadata.hasProtocol(peerId, WakuLightPushCodec)
      not node.wakuMix.peerMetadata.hasShard(peerId, DefaultClusterId, 0'u16)

  asyncTest "mix health keeps its exit after the peer store deletes the pool members":
    ## `hasMixExit` reads the kept metadata, so a delete of every member leaves
    ## the mix health READY.
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, mixKeys.privateKey, @[], addressPolicy = defaultAddressPolicy
      )
    ).expect("mount mix")
    var peerIds: seq[PeerId]
    for port in 1 .. MinMixPoolSize:
      let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
      node.peerManager.addPeer(
        RemotePeerInfo.init(
          peerId,
          @[MultiAddress.init("/ip4/127.0.0.1/tcp/" & $port).expect("address")],
          protocols = @[WakuLightPushCodec],
          mixPubKey = Opt.some(generateKeyPair().expect("hop keys").publicKey),
        )
      )
      peerIds.add(peerId)
    let monitor = NodeHealthMonitor.new(node)
    check monitor.getSyncProtocolHealthInfo(WakuProtocol.MixProtocol).health ==
      HealthStatus.READY

    for peerId in peerIds:
      node.switch.peerStore.delete(peerId)

    check:
      node.getMixNodePoolSize() == MinMixPoolSize
      monitor.getSyncProtocolHealthInfo(WakuProtocol.MixProtocol).health ==
        HealthStatus.READY

  asyncTest "an address book write moves a discovered node to its new hop address":
    ## Identify and dials write the address book without a mix key, so only the
    ## address handler sees the new address.
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, mixKeys.privateKey, @[], addressPolicy = defaultAddressPolicy
      )
    ).expect("mount mix")
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    let first = MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")
    let second = MultiAddress.init("/ip4/127.0.0.1/tcp/2").expect("address")
    node.peerManager.addPeer(
      RemotePeerInfo.init(peerId, @[first], mixPubKey = Opt.some(hopKeys.publicKey))
    )
    check node.wakuMix.nodePool.get(peerId).get().multiAddr == first

    node.switch.peerStore[AddressBook][peerId] = @[second]

    check node.wakuMix.nodePool.get(peerId).get().multiAddr == second

  asyncTest "a discovered node takes its last outbound address as the hop address":
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, mixKeys.privateKey, @[], addressPolicy = defaultAddressPolicy
      )
    ).expect("mount mix")
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    let first = MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")
    let second = MultiAddress.init("/ip4/127.0.0.1/tcp/2").expect("address")
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId, @[first, second], mixPubKey = Opt.some(hopKeys.publicKey)
      )
    )
    check node.wakuMix.nodePool.get(peerId).get().multiAddr == first

    node.switch.peerStore[LastSeenOutboundBook][peerId] = Opt.some(second)

    check node.wakuMix.nodePool.get(peerId).get().multiAddr == second

  asyncTest "a repeated record after a peer store delete renews a discovered node":
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, mixKeys.privateKey, @[], addressPolicy = defaultAddressPolicy
      )
    ).expect("mount mix")
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let record = RemotePeerInfo.init(
      peerId,
      @[MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")],
      mixPubKey = Opt.some(generateKeyPair().expect("hop keys").publicKey),
    )
    node.peerManager.addPeer(record)
    let info = node.wakuMix.pool.get(peerId).expect("pool member")
    node.switch.peerStore.delete(peerId)
    node.wakuMix.pool.add(
      info, MixNodeSource.Discovered, now = Moment.now() - DiscoveredMixNodeTtl
    )

    node.peerManager.addPeer(record)
    node.wakuMix.pool.maintain()

    check peerId in node.wakuMix.pool

  asyncTest "a configured mix node stays after a peer store delete and the record lifetime":
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId,
        mixKeys.privateKey,
        @[
          MixNodePubInfo(
            multiAddr: "/ip4/127.0.0.1/tcp/1/p2p/" & $peerId, pubKey: hopKeys.publicKey
          )
        ],
        addressPolicy = defaultAddressPolicy,
      )
    ).expect("mount mix")
    check node.wakuMix.nodePool.get(peerId).isSome()

    node.switch.peerStore.delete(peerId)
    node.wakuMix.pool.maintain(Moment.now() + DiscoveredMixNodeTtl + 1.seconds)

    check node.wakuMix.nodePool.get(peerId).isSome()
