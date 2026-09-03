{.push raises: [].}

import std/[sequtils, sets]
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
  libp2p/crypto/rng,
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
    anchorRng: rng.Rng ## Shuffles the pool when the node picks a reply anchor.
    anchorFailed: HashSet[PeerId]
      ## Pool nodes whose anchor dial failed since the last anchor was set.
      ## Skipped until every usable candidate has failed, then cleared.
    replyAnchor*: Opt[PeerId]
      ## The pool node that delivers the replies of this node's mixed sends.
      ## See `ensureReplyAnchor`.

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

  let rng = crypto.newRng()
  var m = WakuMix(
    peerManager: peermgr, clusterId: clusterId, pubKey: mixPubKey, anchorRng: rng
  )
  procCall MixProtocol(m).init(
    localMixNodeInfo,
    peermgr.switch,
    delayStrategy = Opt.some(
      DelayStrategy(
        ExponentialDelayStrategy.new(meanDelay = MixHopMeanDelayMs, rng = rng)
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

const MixAnchorDialTimeout* = chronos.seconds(3)
  ## The time one dial of a reply anchor candidate may take inside a mixed
  ## publish. One candidate per publish, with this limit, keeps the cost of a
  ## lost anchor below the reply limit of the attempt.

proc usableAsAnchor(mix: WakuMix, peerId: PeerId): bool =
  ## A pool node the library can place on a path: a mix key next to a usable
  ## address and a libp2p key. A key from discovery before identify, or an
  ## entry that path construction evicted, has no such record.
  mix.nodePool.get(peerId).isSome()

proc dropReplyAnchor*(mix: WakuMix, reason: string) =
  ## Forgets the anchor; the next publish picks another. Called when the
  ## library rejects the anchor of a send.
  mix.replyAnchor.withValue(anchor):
    info "Mix reply anchor dropped", peer = anchor, reason = reason
  mix.replyAnchor = Opt.none(PeerId)

proc ensureReplyAnchor*(
    mix: WakuMix, avoid = Opt.none(PeerId)
): Future[Opt[PeerId]] {.async.} =
  ## Returns the reply anchor: a pool node this node holds a connection to.
  ## The library places the anchor as the hop that delivers every reply, so
  ## the reply arrives over that connection. A node that nothing can dial (a
  ## device behind NAT) receives replies only this way; without an anchor the
  ## hop before this node is a random pool node, and the reply arrives only
  ## when that node happens to hold a connection to this node.
  ##
  ## Keeps the current anchor while its connection lives, the pool still has
  ## a usable record for it, and it is not `avoid`, the exit node of the send
  ## at hand, which the library does not accept as the anchor. Otherwise
  ## tries one usable pool node, at random, with `MixAnchorDialTimeout`: one
  ## dial per publish, so a lost anchor costs an attempt at most that much.
  ## A node whose dial failed is skipped until every usable candidate has
  ## failed. Returns none when no anchor is at hand; the send then goes
  ## without one.
  ##
  ## What the anchor learns: this node's address, which its first hops learn
  ## anyway, and the timing of replies. It does not see the forward packet of
  ## the same message, unless the random forward path starts at it.
  let switch = mix.peerManager.switch
  mix.replyAnchor.withValue(anchor):
    if avoid != Opt.some(anchor) and switch.isConnected(anchor) and
        mix.usableAsAnchor(anchor):
      return mix.replyAnchor
  var candidates = mix.nodePool.peerIds().filterIt(
      it != switch.peerInfo.peerId and avoid != Opt.some(it) and
        it notin mix.anchorFailed and mix.usableAsAnchor(it)
    )
  if candidates.len == 0:
    # Every usable candidate failed a dial since the last anchor: start over
    # at the next publish.
    mix.anchorFailed.clear()
    if mix.replyAnchor.isSome():
      info "Mix reply anchor lost: no usable pool node accepts a connection",
        previous = mix.replyAnchor.get()
    mix.replyAnchor = Opt.none(PeerId)
    return mix.replyAnchor
  mix.anchorRng.shuffle(candidates)
  let candidate = candidates[0]
  let peer = switch.peerStore.getPeer(candidate)
  let connected = await mix.peerManager.connectPeer(
    peer, dialTimeout = MixAnchorDialTimeout, source = "mix reply anchor"
  )
  if connected:
    mix.anchorFailed.clear()
    if mix.replyAnchor != Opt.some(candidate):
      info "Mix reply anchor set", peer = candidate
    mix.replyAnchor = Opt.some(candidate)
    return mix.replyAnchor
  mix.anchorFailed.incl(candidate)
  if mix.replyAnchor.isSome():
    info "Mix reply anchor lost: the candidate did not answer",
      previous = mix.replyAnchor.get(), candidate = candidate
  mix.replyAnchor = Opt.none(PeerId)
  return mix.replyAnchor

# Mix Protocol
