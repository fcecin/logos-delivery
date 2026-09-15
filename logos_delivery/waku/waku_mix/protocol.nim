{.push raises: [].}

import std/strutils
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

const MinMixPoolSize* = 4
  ## The smallest pool that mix can build a path from. `PathLength` is 3, and
  ## with `exit_is_dest` the exit node is a pool member and not one of the hops.

type
  WakuMix* = ref object of MixProtocol
    peerManager*: PeerManager
    clusterId: uint16
    pubKey*: Curve25519Key

  WakuMixResult*[T] = Result[T, string]

  MixNodePubInfo* = object
    multiAddr*: string
    pubKey*: Curve25519Key

proc parseMixNode*(entry: string): Result[MixNodePubInfo, string] =
  ## Parses a `multiaddr:mixPublicKey` mix node entry, the form taken both by
  ## the `--mix-node` argument and by the network presets.
  ##
  ## The address is not required to be a literal IPv4. The fleets publish
  ## `dns4` names, and a name is what survives a node moving hosts, so a name
  ## is what a preset should pin. `mountMix` resolves every entry before the
  ## pool sees it, so mix still only ever holds an address it can route.
  # Split on the last colon, not every colon: an IPv6 multiaddress carries
  # colons of its own, and the key never does.
  let parts = entry.rsplit(':', maxsplit = 1)
  if parts.len != 2:
    return err("expected `multiaddr:mixPublicKey`, got: " & entry)

  discard MultiAddress.init(parts[0]).valueOr:
    return err("invalid multiaddress in mix node entry: " & parts[0])

  # `ncrutils.fromHex` decodes a valid prefix and ignores trailing junk, so
  # validate the exact shape first: a mix key is 2*Curve25519KeySize hex
  # characters, nothing more.
  if parts[1].len != Curve25519KeySize * 2 or not parts[1].allCharsInSet(HexDigits):
    return err(
      "a mix public key is " & $(Curve25519KeySize * 2) & " hex characters, got: " &
        parts[1]
    )

  # The address must carry a /p2p/<peer id>: processBootNodes needs it, and
  # checking here rejects a bad entry at config time rather than dropping it at
  # mount with an error log.
  discard parsePeerInfo(parts[0]).valueOr:
    return err("a mix node needs a /p2p/<peer id> in its multiaddress: " & parts[0])

  return ok(
    MixNodePubInfo(
      multiAddr: parts[0], pubKey: intoCurve25519Key(ncrutils.fromHex(parts[1]))
    )
  )

proc poolSize*(mix: WakuMix): int =
  ## The number of mix nodes that can carry a packet, which is what `mixReady`
  ## and the `mix_pool_size` gauge are meant to describe.
  ##
  ## A known mix key is not enough. Mix routes IPv4 TCP and QUIC-v1 addresses
  ## only, so a peer whose key arrived without such an address sits in
  ## `MixNodePool.len` -- a raw count of `MixPubKeyBook` -- while no path can
  ## use it as a hop or an exit. Counting keys alone reports a pool that mix
  ## cannot build a path from.
  ##
  ## This is the only writer of the gauge: the pool changes whenever discovery
  ## learns a mix key, and there is no upstream hook to observe that, so the
  ## metric is refreshed here, where the live value is computed. The health
  ## monitor recomputes on every peer event, which is exactly when the pool
  ## moves, so the gauge follows a bootstrapping node without any traffic.
  ##
  ## Walks the pool, where the raw count did not. Callers on a hot path should
  ## know that; `mixReady` calls it once per send attempt, which is fine at any
  ## pool size a node actually reaches.
  var routable = 0
  for peerId in mix.nodePool.peerIds():
    if mix.nodePool.get(peerId).isSome():
      routable.inc()
  mix_pool_size.set(routable)
  return routable

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

    # The wire address, without the `/p2p/<id>` part. Mix compares pool
    # addresses with its transport patterns, and the suffix stops the match.
    let multiAddr = pInfo.addrs[0]

    # The pool entry comes first: `nodePool.add` writes `Infinite` confidence,
    # and libp2p does not lower a confidence that it holds.
    let mixPubInfo = MixPubInfo.init(peerId, multiAddr, node.pubKey, peerPubKey.skkey)
    mix.nodePool.add(mixPubInfo)
    count.inc()

    peermgr.addPeer(
      RemotePeerInfo.init(
        peerId, @[multiAddr], publicKey = peerPubKey, mixPubKey = Opt.some(node.pubKey)
      )
    )
  # `count` is entries accepted, which is not the pool: one name can answer with
  # several addresses, and they collapse onto one peer.
  let routable = mix.poolSize()
  info "Using mix bootstrap nodes", entries = count, poolSize = routable

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
        ExponentialDelayStrategy.new(meanDelay = 50'u16, rng = crypto.newRng())
      )
    ),
  )

  processBootNodes(bootnodes, peermgr, m)

  let usable = m.poolSize()
  if usable < MinMixPoolSize:
    info "Mix cannot publish yet, waiting for more mix nodes",
      poolSize = usable, required = MinMixPoolSize
  return ok(m)

# Mix Protocol
