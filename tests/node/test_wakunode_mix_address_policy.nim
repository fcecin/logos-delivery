{.used.}

import testutils/unittests, chronos, chronicles, results
import libp2p/[crypto/crypto, multiaddress, peerid, switch]
import libp2p_mix/curve25519
import brokers/broker_context
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_mix],
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  ../testlib/[wakucore, wakunode, wakunodeconf]

suite "Waku Mix - hop address policy":
  asyncTest "the default mount keeps a loopback hop off mix paths":
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
    check node.wakuMix.nodePool.get(peerId).isNone()
    check node.getMixNodePoolSize() == 0

  asyncTest "the permissive mount keeps a loopback hop on mix paths":
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
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")],
        mixPubKey = Opt.some(hopKeys.publicKey),
      )
    )
    check node.wakuMix.nodePool.get(peerId).isSome()
    check node.getMixNodePoolSize() == 1

  test "the flag picks the policy of the mount":
    let private = MultiAddress.init("/ip4/192.168.1.20/tcp/60000").expect("address")
    let public = MultiAddress.init("/ip4/1.1.1.3/tcp/60000").expect("address")
    check:
      not mixAddressPolicy(false)(private)
      mixAddressPolicy(false)(public)
      mixAddressPolicy(true)(private)

  asyncTest "the node factory mounts mix with the policy of the config":
    ## `MixConf.allowAllAddresses` reaches the mount through the factory.
    let loopback = MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")
    for allowAll in [false, true]:
      var conf = defaultTestWakuNodeConf().toWakuConf().expect("conf")
      let keys = generateKeyPair().expect("mix key pair")
      conf.mixConf = Opt.some(
        MixConf(
          mixKey: keys.privateKey,
          mixPubKey: keys.publicKey,
          allowAllAddresses: allowAll,
        )
      )
      var waku: Waku
      lockNewGlobalBrokerContext:
        waku = (await Waku.new(conf)).expect("Waku.new")
      defer:
        discard await waku.stop()
      check waku.node.wakuMix.nodePool.accepts(loopback) == allowAll

  asyncTest "the default mount does not choose a loopback exit":
    var conf = defaultTestWakuNodeConf().toWakuConf().expect("conf")
    let keys = generateKeyPair().expect("mix key pair")
    conf.mixConf = Opt.some(MixConf(mixKey: keys.privateKey, mixPubKey: keys.publicKey))
    var waku: Waku
    lockNewGlobalBrokerContext:
      waku = (await Waku.new(conf)).expect("Waku.new")
    defer:
      discard await waku.stop()
    let shard = PubsubTopic("/waku/2/rs/" & $conf.clusterId & "/0")
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    waku.node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("address")],
        protocols = @[WakuLightPushCodec],
        shards = @[0'u16],
        mixPubKey = Opt.some(generateKeyPair().expect("hop keys").publicKey),
      )
    )
    waku.node.peerManager.switch.peerStore.setShardInfo(peerId, @[0'u16])

    check:
      waku.lightpushPeerAvailable(shard)
      waku.selectMixLightpushPeer(shard).isNone()
