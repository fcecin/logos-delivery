{.push raises: [].}

import chronicles, chronos, results, metrics

import
  libp2p/crypto/curve25519,
  libp2p/crypto/crypto,
  libp2p_mix,
  libp2p_mix/mix_node,
  libp2p_mix/mix_protocol,
  libp2p_mix/mix_metrics,
  libp2p_mix/delay_strategy,
  libp2p/[multiaddress, peerid],
  eth/common/keys

import
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/waku_enr,
  logos_delivery/waku/node/peer_manager/waku_peer_store

logScope:
  topics = "waku mix"

const MixHopMeanDelayMs = 50'u16
  ## Mean of the random delay each mix hop adds before it forwards a packet.

const MinMixPoolSize* = 4
  ## Smallest pool from which mix can build a path. A path has `PathLength` (3)
  ## positions: two hops and the exit node. With `exit_is_dest` the exit node
  ## is a pool member, and the library selects the hops from the other pool
  ## members. The library needs `PathLength` other members. Thus the pool needs
  ## the exit node and three more nodes. The return path takes two of those
  ## three.

type
  WakuMix* = ref object of MixProtocol
    peerManager*: PeerManager
    clusterId: uint16
    pubKey*: Curve25519Key

  WakuMixResult*[T] = Result[T, string]

  MixNodePubInfo* = object
    multiAddr*: string
    pubKey*: Curve25519Key

proc processBootNodes(
    bootnodes: seq[MixNodePubInfo], peermgr: PeerManager, mix: WakuMix
) =
  var count = 0
  for node in bootnodes:
    let pInfo = parsePeerInfo(node.multiAddr).valueOr:
      error "Failed to get peer id from multiaddress: ",
        error = error, multiAddr = $node.multiAddr
      continue
    let peerId = pInfo.peerId
    var peerPubKey: crypto.PublicKey
    if not peerId.extractPublicKey(peerPubKey):
      warn "Failed to extract public key from peerId, skipping node", peerId = peerId
      continue

    if peerPubKey.scheme != PKScheme.Secp256k1:
      warn "Peer public key is not Secp256k1, skipping node",
        peerId = peerId, scheme = peerPubKey.scheme
      continue

    ## The wire address, without the `/p2p/<id>` part. `parsePeerInfo` takes
    ## the peer id from that part. Mix compares pool addresses with its
    ## transport patterns, and an address with the peer id does not match. Such
    ## a node stays in the pool until the first path construction, and then
    ## mix removes it because it has no usable address.
    let multiAddr = pInfo.addrs[0]

    ## INVARIANT: a configured mix node keeps its address with the confidence
    ## `Infinite`, so libp2p does not remove the address. The order of the two
    ## calls below keeps this invariant. Call `nodePool.add` first and
    ## `peermgr.addPeer` second.
    ## - `nodePool.add` writes the address with `Infinite` confidence, but only
    ##   when the address book does not have the address yet.
    ## - `addPeer` writes the address with `Medium` confidence. The libp2p
    ##   address book does not lower a confidence that it already holds.
    ## With `addPeer` first, the address gets `Medium`, `nodePool.add` finds
    ## the address and does nothing, and libp2p removes the address after
    ## 1 hour. The review commit "fix(mix): keep bootstrap mix node addresses
    ## out of libp2p pruning" used that order, and this code reverses it. The
    ## test "a statically configured mix node is a usable pool entry, pinned
    ## against pruning" in tests/messaging/test_send_service_mix.nim fails
    ## when the order is wrong.
    let mixPubInfo = MixPubInfo.init(peerId, multiAddr, node.pubKey, peerPubKey.skkey)
    mix.nodePool.add(mixPubInfo)
    count.inc()

    peermgr.addPeer(
      RemotePeerInfo.init(
        peerId, @[multiAddr], publicKey = peerPubKey, mixPubKey = Opt.some(node.pubKey)
      )
    )
  mix_pool_size.set(count)
  info "using mix bootstrap nodes ", count = count

proc new*(
    T: typedesc[WakuMix],
    nodeAddr: string,
    peermgr: PeerManager,
    clusterId: uint16,
    mixPrivKey: Curve25519Key,
    bootnodes: seq[MixNodePubInfo],
): WakuMixResult[T] =
  let mixPubKey = public(mixPrivKey)
  info "mixPubKey", mixPubKey = mixPubKey
  let nodeMultiAddr = MultiAddress.init(nodeAddr).valueOr:
    return err("failed to parse mix node address: " & $nodeAddr & ", error: " & error)
  let localMixNodeInfo = initMixNodeInfo(
    peermgr.switch.peerInfo.peerId, nodeMultiAddr, mixPubKey, mixPrivKey,
    peermgr.switch.peerInfo.publicKey.skkey, peermgr.switch.peerInfo.privateKey.skkey,
  )

  var m = WakuMix(peerManager: peermgr, clusterId: clusterId, pubKey: mixPubKey)
  procCall MixProtocol(m).init(
    localMixNodeInfo,
    peermgr.switch,
    delayStrategy = Opt.some(
      DelayStrategy(
        ExponentialDelayStrategy.new(
          meanDelay = MixHopMeanDelayMs, rng = crypto.newRng()
        )
      )
    ),
  )

  processBootNodes(bootnodes, peermgr, m)

  if m.nodePool.len < MinMixPoolSize:
    ## Not a warning: the pool is the peer store's mix book, so discovery
    ## (kademlia, rendezvous) keeps filling it after mount. Starting short of a
    ## full path is the normal boot state, not a misconfigured node.
    info "mix cannot publish yet, waiting for more mix nodes",
      poolSize = m.nodePool.len, required = MinMixPoolSize
  return ok(m)

proc poolSize*(mix: WakuMix): int =
  mix.nodePool.len

# Mix Protocol
