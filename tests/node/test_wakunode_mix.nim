{.used.}

import testutils/unittests, chronos, results
import libp2p/[crypto/crypto, peerid, multiaddress]
import libp2p_mix/curve25519

import
  logos_delivery/waku/[waku_core, node/peer_manager, waku_node, waku_mix],
  ../testlib/[wakucore, wakunode, testasync]

## `poolSize` answers "how many mix nodes can carry a packet". `mixReady` and
## the `mix_pool_size` gauge are both built on it, so it must describe the live
## pool and count only the nodes a path can actually use.

suite "Waku Mix - pool size":
  var node {.threadvar.}: WakuNode

  asyncSetup:
    node = newTestWakuNode(generateSecp256k1Key())

    # Mounted before the start, as the node factory does: a protocol cannot be
    # mounted on a switch that is already running.
    let mixKeys = generateKeyPair().expect("mix key pair")
    (await node.mountMix(DefaultClusterId, mixKeys.privateKey, @[])).isOkOr:
      raiseAssert "Failed to mount mix: " & $error

    await node.start()

  asyncTeardown:
    await node.stop()

  proc addMixPeer(address: string): PeerId =
    ## What discovery does when it learns a peer's mix key: the key and the
    ## address it came with go into the peer store, and the pool is a view
    ## over that.
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let mixKeys = generateKeyPair().expect("mix key pair")
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init(address).tryGet()],
        mixPubKey = Opt.some(mixKeys.publicKey),
      )
    )
    return peerId

  asyncTest "a node with no mix peers has an empty pool":
    ## The bootstrap list is empty here, which is what a preset-configured node
    ## starts with.
    check node.getMixNodePoolSize() == 0

  asyncTest "only the peers mix can route count towards the pool":
    ## Mix takes IPv4 TCP and QUIC-v1. The other two peers have a mix key and
    ## no address a path can use, so they are known but not usable.
    discard addMixPeer("/ip4/127.0.0.1/tcp/60001")
    discard addMixPeer("/ip4/127.0.0.1/udp/60002/quic-v1")
    discard addMixPeer("/dns4/node.test/tcp/60003")
    discard addMixPeer("/ip6/::1/tcp/60004")

    check node.getMixNodePoolSize() == 2

  asyncTest "the pool follows the peers discovery brings in":
    ## The count is not fixed at mount: it is the live pool, so it moves as
    ## mix keys arrive.
    check node.getMixNodePoolSize() == 0

    for port in 60010 .. 60012:
      discard addMixPeer("/ip4/127.0.0.1/tcp/" & $port)
    check node.getMixNodePoolSize() == 3

    discard addMixPeer("/ip4/127.0.0.1/tcp/60013")
    check node.getMixNodePoolSize() == 4

  asyncTest "mix is not ready until enough peers can carry a packet":
    ## `mixReady` is `poolSize() >= MinMixPoolSize`, so an unroutable peer must
    ## not push a node over the line.
    for port in 60020 .. 60022:
      discard addMixPeer("/ip4/127.0.0.1/tcp/" & $port)
    discard addMixPeer("/dns4/node.test/tcp/60023")

    check node.getMixNodePoolSize() == 3 # the name is known, and unusable

    discard addMixPeer("/ip4/127.0.0.1/tcp/60024")
    check node.getMixNodePoolSize() == MinMixPoolSize
