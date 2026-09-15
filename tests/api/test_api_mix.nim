{.used.}

import std/net
import chronos, chronicles, testutils/unittests, results
import libp2p/[peerid, multiaddress, crypto/crypto]
import libp2p_mix/curve25519

import
  logos_delivery/waku/waku,
  logos_delivery/waku/api/debug,
  logos_delivery/waku/waku_mix,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/api/conf/messaging_conf,
  logos_delivery/waku/factory/waku_conf
import ../testlib/[testasync, wakucore]

## `mixPoolSize` reports how many mix nodes a packet could be routed through
## right now. It is a number and not a verdict on purpose: what a pool of a
## given size is worth is the consumer's judgement, not this layer's.

proc testConf(): WakuConf =
  var conf = MessagingClientConf()
    .toWakuNodeConf(messaging_conf.LogosDeliveryMode.Core).valueOr:
      raiseAssert error
  conf.listenAddress = parseIpAddress("0.0.0.0")
  conf.tcpPort = Port(0)
  conf.discv5UdpPort = Port(0)
  conf.clusterId = Opt.some(3'u16)
  conf.numShardsInNetwork = 1
  conf.rest = false
  return conf.toWakuConf().valueOr:
    raiseAssert error

suite "Kernel API - mix pool size":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc addMixPeer(address: string): PeerId =
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let mixKeys = generateKeyPair().expect("mix key pair")
    waku.node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init(address).tryGet()],
        mixPubKey = Opt.some(mixKeys.publicKey),
      )
    )
    return peerId

  asyncTest "a node without mix says so, rather than reporting an empty pool":
    ## "not mounted" and "mounted, still finding peers" are different states,
    ## and a caller deciding whether to send needs to tell them apart.
    let res = await waku.mixPoolSize()
    check res.isErr()

  asyncTest "a mounted node reports the nodes a packet could go through":
    let mixKeys = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(3'u16, mixKeys.privateKey, @[])).isOkOr:
      raiseAssert "Failed to mount mix: " & $error

    check (await waku.mixPoolSize()).get() == 0

    discard addMixPeer("/ip4/127.0.0.1/tcp/60201")
    discard addMixPeer("/ip4/127.0.0.1/tcp/60202")
    check (await waku.mixPoolSize()).get() == 2

    # A mix key with an address mix cannot route is not a node it can use.
    discard addMixPeer("/dns4/node.test/tcp/60203")
    check (await waku.mixPoolSize()).get() == 2
