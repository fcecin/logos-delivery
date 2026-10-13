{.used.}

import testutils/unittests, chronos, results
import libp2p/[crypto/crypto, multiaddress, peerid, switch]
import libp2p_mix/curve25519

import
  logos_delivery/waku/[waku_core, waku_node, waku_mix],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  ../testlib/[wakucore, wakunode]

suite "Waku Mix - retained service metadata":
  asyncTest "peer store deletion preserves the mix exit metadata":
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

  asyncTest "the default mount excludes private mix hops":
    let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
    defer:
      await node.stop()
    let mixKeys = generateKeyPair().expect("mix keys")
    (await node.mountMix(DefaultClusterId, mixKeys.privateKey, @[])).expect("mount mix")
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")],
        mixPubKey = Opt.some(hopKeys.publicKey),
      )
    )
    check:
      node.wakuMix.nodePool.contains(peerId)
      node.wakuMix.nodePool.get(peerId).isNone()
      node.getMixNodePoolSize() == 0
