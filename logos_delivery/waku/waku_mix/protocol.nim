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
  libp2p_mix/multiaddr as mix_multiaddr,
  libp2p_mix/delay_strategy,
  libp2p_mix/spam_protection,
  libp2p_mix/pool,
  libp2p_mix/managed_pool,
  libp2p/[multiaddress, peerid],
  eth/common/keys

import
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/waku_enr,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  ./peer_metadata

export pool, managed_pool, peer_metadata

logScope:
  topics = "waku mix"

const MinMixPoolSize* = 4
  ## The smallest pool that mix can build a path from. `PathLength` is 3, and
  ## with `exit_is_dest` the exit node is a pool member and not one of the hops.

const DiscoveredMixNodeTtl* = chronos.hours(1)
  ## How long a discovered mix node stays in the pool without a new record or
  ## a connection.

const MaxDiscoveredMixNodes* = 1000
  ## The maximum number of discovered mix nodes in the pool.

type
  WakuMix* = ref object of MixProtocol
    peerManager*: PeerManager
    clusterId: uint16
    pubKey*: Curve25519Key
    pool*: ManagedMixNodePool ## The mix pool. `nodePool` of `MixProtocol` is this pool.
    peerMetadata*: MixPeerMetadataStore
    hopMissing: bool
      ## `true` when the last hop derivation found no address the encoder accepts.
      ## The hop that mix still holds is then a leftover, unusable even if it
      ## encodes.

  WakuMixResult*[T] = Result[T, string]

  MixNodePubInfo* = object
    multiAddr*: string
    pubKey*: Curve25519Key

const RoutableMixTransport = mapOr(
  TCP_IP4,
  QUIC_V1_IP4,
  mapAnd(DNS4, mapEq("tcp")),
  mapAnd(mapAnd(DNS4, mapEq("udp")), mapEq("quic-v1")),
)
  ## What `parseMixNode` accepts: the transports `MixNodePool.get` routes, and
  ## a `dns4` name for one of them. `parseMixNode` matches the base transport, as
  ## the pool does, so a circuit relay over one of them passes.

proc parseMixNode*(entry: string): Result[MixNodePubInfo, string] =
  ## Parses a `multiaddr:mixPublicKey` entry from `--mixnode` or a preset. It
  ## accepts a `dns4` name, which the node resolves after the mount, and refuses
  ## an address on a transport that the pool cannot route.
  # Split on the last colon: an address can hold colons (IPv6), a key cannot.
  let parts = entry.rsplit(':', maxsplit = 1)
  if parts.len != 2:
    return err("expected `multiaddr:mixPublicKey`, got: " & entry)

  discard MultiAddress.init(parts[0]).valueOr:
    return err("invalid multiaddress in mix node entry: " & parts[0])

  # `ncrutils.fromHex` ignores trailing junk, so check the exact shape first: a
  # mix key is 2*Curve25519KeySize hex characters.
  if parts[1].len != Curve25519KeySize * 2 or not parts[1].allCharsInSet(HexDigits):
    return err(
      "a mix public key is " & $(Curve25519KeySize * 2) & " hex characters, got: " &
        parts[1]
    )

  # `processBootNodes` needs a /p2p/<peer id>, so refuse an entry without one at
  # config. The message carries the parser's own reason.
  let pInfo = parsePeerInfo(parts[0]).valueOr:
    return err(
      "the peer address parser refused the mix node entry (" & error & "): " & parts[0]
    )

  # Require a transport the pool routes once the name is resolved; the peer
  # info parser also takes WebSocket, IPv6, dns6, dns and dnsaddr. Match the base
  # transport, as the pool does, so a relayed entry counts by the relay's address.
  let base = mix_multiaddr.getBaseTransport(pInfo.addrs[0]).valueOr:
    return err("mix cannot read the transport of the mix node entry: " & parts[0])
  if not RoutableMixTransport.match(base):
    return err(
      "mix routes IPv4 TCP or QUIC-v1 only, directly or through a circuit relay (a dns4 name is resolved after the mount), got: " &
        parts[0]
    )

  return ok(
    MixNodePubInfo(
      multiAddr: parts[0], pubKey: intoCurve25519Key(ncrutils.fromHex(parts[1]))
    )
  )

proc poolSize*(mix: WakuMix): int =
  ## The number of pool members a path can use now.
  return mix.nodePool.len

proc acceptedHopAddress(mix: WakuMix, peerId: PeerId): Opt[MultiAddress] =
  ## The last outbound address of a peer when the pool accepts it, as
  ## `MixNodePool` does. If not, the first address of its record that the
  ## pool accepts.
  let store = mix.peerManager.switch.peerStore
  let lastSeen = store[LastSeenOutboundBook][peerId]
  if lastSeen.isSome() and mix.pool.accepts(lastSeen.get()):
    return lastSeen
  for candidate in store[AddressBook][peerId]:
    if mix.pool.accepts(candidate):
      return Opt.some(candidate)
  Opt.none(MultiAddress)

proc learnDiscoveredNode(mix: WakuMix, peerId: PeerId) =
  ## Adds or refreshes a discovered mix node from the peer store books.
  let store = mix.peerManager.switch.peerStore
  let mixPubKey = store[MixPubKeyBook][peerId]
  if mixPubKey == default(Curve25519Key):
    return
  let address = mix.acceptedHopAddress(peerId).valueOr:
    return
  var libp2pPubKey = store[KeyBook][peerId]
  if libp2pPubKey.scheme != PKScheme.Secp256k1:
    if not peerId.extractPublicKey(libp2pPubKey) or
        libp2pPubKey.scheme != PKScheme.Secp256k1:
      return
  mix.pool.add(
    MixPubInfo.init(peerId, address, mixPubKey, libp2pPubKey.skkey),
    MixNodeSource.Discovered,
  )

func mixAddressPolicy*(allowAllAddresses: bool): PeerAddressPolicy =
  ## The hop address policy of the mount. `publicDirectAddressPolicy` accepts
  ## only public addresses that are not relay routes. `allowAllAddresses` picks
  ## `defaultAddressPolicy`, which accepts all addresses, for a local simulation,
  ## a test or a private network.
  if allowAllAddresses: defaultAddressPolicy else: publicDirectAddressPolicy

proc updatePoolSize*(size: int) =
  ## Sets `mix_pool_size`; this is its only writer. The mount, `addBootNodes`
  ## and each health pass publish the count they just read: routability can
  ## change when no peer-store handler fires, as when an `AddressBook` entry's
  ## TTL runs out.
  mix_pool_size.set(size)

proc processBootNodes(
    bootnodes: seq[MixNodePubInfo], peermgr: PeerManager, mix: WakuMix
) =
  var count = 0
  var refused: seq[string]
  for node in bootnodes:
    let pInfo = parsePeerInfo(node.multiAddr).valueOr:
      error "Failed to get peer id from multiaddress: ",
        error = error, multiAddr = $node.multiAddr
      continue
    let peerId = pInfo.peerId

    # A fleet node finds itself in its own preset. Skip that entry, or mix could
    # draw this node as a hop or an exit and route a packet to itself.
    if peerId == peermgr.switch.peerInfo.peerId:
      debug "Skipping a mix bootstrap node that is this node itself", peerId = peerId
      continue

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

    # The pool entry comes first, so the peer store handlers find a configured
    # node and do not add it as a discovered one.
    let mixPubInfo = MixPubInfo.init(peerId, multiAddr, node.pubKey, peerPubKey.skkey)
    mix.nodePool.add(mixPubInfo)
    count.inc()
    if not mix.pool.accepts(multiAddr):
      refused.add(node.multiAddr)

    # A name that resolves to two addresses makes two entries. The peer store
    # keeps both, as the pool does.
    var addrs = peermgr.switch.peerStore[AddressBook][peerId]
    if multiAddr notin addrs:
      addrs.add(multiAddr)
    peermgr.addPeer(
      RemotePeerInfo.init(
        peerId, addrs, publicKey = peerPubKey, mixPubKey = Opt.some(node.pubKey)
      )
    )
  # `count` is the number of entries added to the pool, also the entries that
  # the address policy refuses. The addresses of one peer make one member.
  let routable = mix.poolSize()
  info "Using mix bootstrap nodes", entries = count, poolSize = routable
  if refused.len > 0:
    warn "Configured mix nodes that the address policy refuses stay off mix paths",
      refused = refused

proc addBootNodes*(mix: WakuMix, bootnodes: seq[MixNodePubInfo]) =
  ## Adds bootstrap nodes resolved after the mount, and publishes the pool size.
  processBootNodes(bootnodes, mix.peerManager, mix)
  updatePoolSize(mix.poolSize())

proc new*(
    T: typedesc[WakuMix],
    nodeAddr: string,
    peermgr: PeerManager,
    clusterId: uint16,
    mixPrivKey: Curve25519Key,
    bootnodes: seq[MixNodePubInfo],
    spamProtection: Opt[SpamProtection] = Opt.none(SpamProtection),
    addressPolicy: PeerAddressPolicy = publicDirectAddressPolicy,
): WakuMixResult[T] =
  let mixPubKey = public(mixPrivKey)
  info "mixPubKey", mixPubKey = mixPubKey
  let nodeMultiAddr = MultiAddress.init(nodeAddr).valueOr:
    return err("failed to parse mix node address: " & $nodeAddr & ", error: " & error)
  let localMixNodeInfo = initMixNodeInfo(
    peermgr.switch.peerInfo.peerId, nodeMultiAddr, mixPubKey, mixPrivKey,
    peermgr.switch.peerInfo.publicKey.skkey, peermgr.switch.peerInfo.privateKey.skkey,
  )

  let switch = peermgr.switch
  let pool = ManagedMixNodePool.new(
    ManagedPoolConfig(
      addressPolicy: addressPolicy,
      discoveredTtl: DiscoveredMixNodeTtl,
      maxDiscovered: MaxDiscoveredMixNodes,
      isConnected: proc(peerId: PeerId): bool {.gcsafe, raises: [].} =
        switch.isConnected(peerId),
    ),
    selfId = switch.peerInfo.peerId,
  )
  var m =
    WakuMix(peerManager: peermgr, clusterId: clusterId, pubKey: mixPubKey, pool: pool)
  procCall MixProtocol(m).init(
    localMixNodeInfo,
    peermgr.switch,
    spamProtection = spamProtection,
    addressPolicy = addressPolicy,
    delayStrategy = Opt.some(
      if spamProtection.isSome():
        DelayStrategy(SpamProtectionDelayStrategy.new(rng = crypto.newRng()))
      else:
        DelayStrategy(
          ExponentialDelayStrategy.new(meanDelay = 50'u16, rng = crypto.newRng())
        )
    ),
  )

  # `init` sets `nodePool` to the peer store view. The library must read the managed pool.
  m.nodePool = pool
  m.peerMetadata = MixPeerMetadataStore.new(pool, switch)
  let store = switch.peerStore
  store[MixPubKeyBook].addHandler(
    proc(peerId: PeerId) {.gcsafe, raises: [].} =
      m.learnDiscoveredNode(peerId)
  )
  # Identify and dials also write the address book, and a dial writes the last
  # outbound address. The handler skips a configured node and a suspended node.
  # For another node in the pool, it acts only when the hop address changes,
  # so an address that expires in the book does not renew the node.
  let onHopAddress = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    if pool.source(peerId) == Opt.some(MixNodeSource.Configured) or
        pool.suspendedUntil(peerId).isSome():
      return
    if peerId notin pool or
        m.acceptedHopAddress(peerId) !=
        pool.get(peerId).map(
          proc(info: MixPubInfo): MultiAddress =
            info.multiAddr
        ):
      m.learnDiscoveredNode(peerId)
  store[AddressBook].addHandler(onHopAddress)
  store[LastSeenOutboundBook].addHandler(onHopAddress)
  for peerId in store[MixPubKeyBook].book.keys:
    m.learnDiscoveredNode(peerId)
  processBootNodes(bootnodes, peermgr, m)

  let usable = m.poolSize()
  updatePoolSize(usable)

  if usable < MinMixPoolSize:
    info "Mix cannot publish yet, waiting for more mix nodes",
      poolSize = usable, required = MinMixPoolSize
  return ok(m)

method start*(mix: WakuMix) {.async: (raises: [CancelledError]).} =
  await procCall MixProtocol(mix).start()
  mix.pool.start()

method stop*(mix: WakuMix) {.async: (raises: []).} =
  await mix.pool.stop()
  await procCall MixProtocol(mix).stop()

proc selfHopMissing*(mix: WakuMix): bool =
  ## True when the last derivation of this node's own hop found nothing to set.
  mix.hopMissing

proc selfHopUsable*(mix: WakuMix): bool =
  ## True when the encoder accepts this node's own hop (IPv4 TCP or QUIC-v1, or
  ## a circuit relay over one) and `hopMissing` is not set. Every reply path and
  ## cover packet fails at build time on a hop that the encoder rejects.
  if mix.hopMissing:
    return false
  let info = mix.localMixPubInfo()
  return mix_multiaddr.multiAddrToBytes(info.peerId, info.multiAddr).isOk()

proc updateSelfHop*(
    mix: WakuMix, preferred: seq[MultiAddress], fallback: seq[MultiAddress]
): Opt[MultiAddress] =
  ## Sets this node's own hop to the first candidate in `preferred`, then in
  ## `fallback`, that the library accepts, and returns it. When none encodes,
  ## the hop stays as it was, `hopMissing` is set, and the result is none.
  for candidate in preferred & fallback:
    if mix.setLocalMultiAddr(candidate).isOk():
      mix.hopMissing = false
      return Opt.some(candidate)
  mix.hopMissing = true
  return Opt.none(MultiAddress)

# Mix Protocol
