## The mix pool. It follows the peer store of the node, and keeps one hop
## address for each pool member.

{.push raises: [].}

import chronicles, chronos, results

import
  libp2p/crypto/curve25519,
  libp2p/crypto/crypto,
  libp2p_mix,
  libp2p_mix/mix_protocol,
  libp2p_mix/multiaddr as mix_multiaddr,
  libp2p/[multiaddress, peerid, peerinfo, peerstore, peeraddrpolicy, switch, wire]

import
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store

export peeraddrpolicy

logScope:
  topics = "waku mix pool"

const MinMixPoolSize* = 4
  ## The smallest pool that mix can build a path from. `PathLength` is 3, and
  ## with `exit_is_dest` the exit node is a pool member and not one of the hops.

type MixPool* = ref object
  peerManager: PeerManager
  policy: PeerAddressPolicy ## The addresses that a hop can carry.
  known: MixNodePool ## Each peer with a mix key, in the peer store of the node.
  members: PeerStore ## One hop address for each pool member.
  nodePool: MixNodePool ## The pool over `members` that nim-libp2p-mix reads.
  loop: Future[void]
  # The setting is public for tests.
  poolLoopInterval*: Duration = chronos.seconds(15)

const publicDirectAddressPolicy* = proc(ma: MultiAddress): bool {.gcsafe, raises: [].} =
  ## A public address that is not a relay route. Only a relay client can dial
  ## a relay route.
  isPublicMA(ma) and not isCircuitRelayMA(ma)

proc mixAddressPolicy*(allowPrivateAddresses: bool): PeerAddressPolicy =
  ## The address policy of `--mix-allow-private-addresses`.
  if allowPrivateAddresses: defaultAddressPolicy else: publicDirectAddressPolicy

proc accepts*(pool: MixPool, address: MultiAddress): bool =
  ## True when the policy accepts `address`. For a relay route, the policy must
  ## also accept the address of the relay, because the previous hop dials it.
  let base = mix_multiaddr.getBaseTransport(address).valueOr:
    return false
  return
    dialableAddrs(pool.policy, [address]).len == 1 and
    dialableAddrs(pool.policy, [base]).len == 1

proc usableHopAddress(pool: MixPool, peerId: PeerId, address: MultiAddress): bool =
  ## True when the address policy and the encoder both accept `address`.
  return
    pool.accepts(address) and mix_multiaddr.multiAddrToBytes(peerId, address).isOk()

proc store(pool: MixPool): PeerStore =
  pool.peerManager.switch.peerStore

proc hopAddresses(pool: MixPool, peerId: PeerId): seq[MultiAddress] =
  ## The addresses of `peerId` that a hop can carry, the last dialed first.
  var candidates: seq[MultiAddress]
  let lastSeen = pool.store[LastSeenOutboundBook][peerId]
  if lastSeen.isSome():
    candidates.add(lastSeen.get().stripPeerId())
  candidates.add(pool.store[AddressBook][peerId])
  var carried: seq[MultiAddress]
  for address in candidates:
    if address notin carried and pool.usableHopAddress(peerId, address):
      carried.add(address)
  return carried

proc hopAddress(pool: MixPool, peerId: PeerId): Opt[MultiAddress] =
  ## The address that a hop through `peerId` encodes.
  let carried = pool.hopAddresses(peerId)
  if carried.len == 0:
    return Opt.none(MultiAddress)
  return Opt.some(carried[0])

proc poolEntry(pool: MixPool, peerId: PeerId): Opt[MixPubInfo] =
  ## The pool entry for `peerId`. It needs a secp256k1 key and a hop address.
  let store = pool.store
  if peerId == pool.peerManager.switch.peerInfo.peerId:
    return Opt.none(MixPubInfo)
  let mixPubKey = store[MixPubKeyBook][peerId]
  if mixPubKey == default(Curve25519Key):
    return Opt.none(MixPubInfo)
  # `addPeer` and `MixNodePool.add` write the key book with each mix key.
  let pubKey = store[KeyBook][peerId]
  if pubKey.scheme != Secp256k1:
    return Opt.none(MixPubInfo)
  let address = pool.hopAddress(peerId).valueOr:
    return Opt.none(MixPubInfo)
  return Opt.some(MixPubInfo.init(peerId, address, mixPubKey, pubKey.skkey))

proc len*(pool: MixPool): int =
  ## The number of pool members.
  pool.members[MixPubKeyBook].len

proc nodePool*(pool: MixPool): MixNodePool =
  ## The pool that nim-libp2p-mix draws paths from.
  pool.nodePool

proc refreshPeer(pool: MixPool, peerId: PeerId) =
  ## Makes the pool entry of `peerId` agree with the peer store of the node.
  let members = pool.members
  let hop = pool.poolEntry(peerId).valueOr:
    if peerId in members[MixPubKeyBook]:
      discard members[MixPubKeyBook].del(peerId)
      trace "Mix peer left the pool", peerId = peerId
    discard members[AddressBook].del(peerId)
    discard members[KeyBook].del(peerId)
    return

  let key = crypto.PublicKey(scheme: Secp256k1, skkey: hop.libp2pPubKey)
  if members[KeyBook][peerId] != key:
    members[KeyBook][peerId] = key
  if members[AddressBook][peerId] != @[hop.multiAddr]:
    # `set` keeps an `Infinite` entry that the new list leaves out.
    discard members[AddressBook].del(peerId)
    members[AddressBook].set(peerId, @[hop.multiAddr], AddressConfidence.Infinite)
  # The key comes last, so a pool change handler sees a complete entry.
  if members[MixPubKeyBook][peerId] != hop.mixPubKey:
    members[MixPubKeyBook][peerId] = hop.mixPubKey
    trace "Mix peer joined the pool", peerId = peerId, hop = $hop.multiAddr

proc refresh(pool: MixPool) =
  ## Refreshes each peer of the pool. Book handlers cover the changes between
  ## passes.
  for peerId in pool.known.peerIds():
    pool.refreshPeer(peerId)
  for peerId in pool.nodePool.peerIds():
    if peerId notin pool.store[MixPubKeyBook]:
      pool.refreshPeer(peerId)

proc addBookHandlers(pool: MixPool) =
  ## Refreshes a peer when a book that decides its entry changes. The handlers
  ## cannot be removed.
  let store = pool.store
  let onChange = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    if peerId in store[MixPubKeyBook] or peerId in pool.members[MixPubKeyBook]:
      pool.refreshPeer(peerId)
  store[MixPubKeyBook].addHandler(onChange)
  store[AddressBook].addHandler(onChange)
  store[LastSeenOutboundBook].addHandler(onChange)
  store[KeyBook].addHandler(onChange)

proc addChangeHandler*(pool: MixPool, handler: PeerBookChangeHandler) =
  ## Calls `handler` when a peer joins or leaves the pool.
  pool.members[MixPubKeyBook].addHandler(handler)

proc add*(pool: MixPool, info: MixPubInfo) =
  ## Adds a configured mix node to the peer store.
  pool.known.add(info)

proc maintain*(pool: MixPool) {.async: (raises: [CancelledError]).} =
  ## One pass of the pool loop. It finds the changes that no book handler
  ## reports, such as an expired address. Public for tests.
  pool.refresh()

proc poolLoop(pool: MixPool) {.async: (raises: [CancelledError]).} =
  while true:
    await pool.maintain()
    await sleepAsync(pool.poolLoopInterval)

proc start*(pool: MixPool) =
  ## Runs the pool loop.
  if pool.loop.isNil() or pool.loop.finished():
    pool.loop = pool.poolLoop()

proc stop*(pool: MixPool) {.async: (raises: []).} =
  ## Cancels the loop.
  if not pool.loop.isNil():
    await pool.loop.cancelAndWait()
    pool.loop = nil

proc new*(
    T: typedesc[MixPool], peerManager: PeerManager, policy: PeerAddressPolicy
): T =
  let members = PeerStore.new(nil)
  let pool = T(
    peerManager: peerManager,
    policy: policy,
    known: MixNodePool.new(peerManager.switch.peerStore),
    members: members,
    nodePool: MixNodePool.new(members),
  )
  pool.addBookHandlers()
  pool.refresh()
  return pool
