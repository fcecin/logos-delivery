## The mix pool. It keeps each mix node that this node knows, with a copy of its
## addresses, protocols, ENR and shards, and one hop address for each pool
## member. A delete in the peer store of the node does not remove a mix node,
## change its hop or end its role as an exit.

{.push raises: [].}

import std/[sequtils, tables]
import chronicles, chronos, results

import
  libp2p/crypto/curve25519,
  libp2p/crypto/[crypto, secp],
  libp2p_mix,
  libp2p_mix/mix_protocol,
  libp2p_mix/multiaddr as mix_multiaddr,
  libp2p/[multiaddress, peerid, peerinfo, peerstore, peeraddrpolicy, switch, wire]
from eth/p2p/discoveryv5/enr import Record

import
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/waku_enr/sharding

export peeraddrpolicy

logScope:
  topics = "waku mix pool"

const MinMixPoolSize* = 4
  ## The smallest pool that mix can build a path from. `PathLength` is 3, and
  ## with `exit_is_dest` the exit node is a pool member and not one of the hops.

type
  MixNodeSource {.pure.} = enum
    Discovered ## A peer record with a mix key.
    Configured ## `--mixnode` or a preset. The node stays for the life of the pool.

  KnownMixNode = object
    ## A mix node that the pool knows. It is a pool member while it has a hop
    ## address.
    source: MixNodeSource
    mixPubKey: Curve25519Key
    libp2pPubKey: SkPublicKey
    lastDialed: Opt[MultiAddress]
      ## The address of the last outbound connection of this node to the node.
    stored: seq[MultiAddress] ## The last hop addresses in the peer store.
    configured: seq[MultiAddress] ## The addresses of `--mixnode` or a preset.
    # The exit choice reads these copies of the peer store.
    protocols: seq[string]
    enr: Record
    shards: seq[uint16]
    lastSeen: Moment ## The last peer record, successful dial or connection.

type MixPool* = ref object
  peerManager: PeerManager
  policy: PeerAddressPolicy ## The addresses that a hop can carry.
  known: Table[PeerId, KnownMixNode] ## Each mix node that the pool knows.
  members: PeerStore ## One hop address for each pool member.
  nodePool: MixNodePool ## The pool over `members` that nim-libp2p-mix reads.
  dials: Table[PeerId, Future[bool].Raising([CancelledError])]
    ## The running mix dial of each peer. A later attempt joins it.
  dialsStopped: bool ## True from `stop` to `start`. Then no send starts a dial.
  loop: Future[void]
  # The settings are public for tests.
  poolLoopInterval*: Duration = chronos.seconds(15)
  discoveredTtl*: Duration = chronos.hours(1)
    ## A discovered node leaves after this time with no peer record, successful
    ## dial or connection.
  maxDiscovered*: int = 1000 ## The limit of discovered nodes.

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
  ## The addresses of `peerId` that a hop can carry, from the copies of the pool.
  ## The last dialed address comes first, then the last peer store addresses,
  ## then the configured addresses.
  var candidates: seq[MultiAddress]
  pool.known.withValue(peerId, node):
    if node.lastDialed.isSome():
      candidates.add(node.lastDialed.get())
    candidates.add(node.stored)
    candidates.add(node.configured)
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
  ## The pool entry for `peerId`. The pool must know the node, and the node
  ## needs a hop address.
  var mixPubKey: Curve25519Key
  var libp2pPubKey: SkPublicKey
  pool.known.withValue(peerId, node):
    mixPubKey = node.mixPubKey
    libp2pPubKey = node.libp2pPubKey
  do:
    return Opt.none(MixPubInfo)
  let address = pool.hopAddress(peerId).valueOr:
    return Opt.none(MixPubInfo)
  return Opt.some(MixPubInfo.init(peerId, address, mixPubKey, libp2pPubKey))

proc len*(pool: MixPool): int =
  ## The number of pool members.
  pool.members[MixPubKeyBook].len

proc nodePool*(pool: MixPool): MixNodePool =
  ## The pool that nim-libp2p-mix draws paths from.
  pool.nodePool

proc refreshPeer(pool: MixPool, peerId: PeerId) =
  ## Makes the pool entry of `peerId` agree with the known node.
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

proc copyPeerInfo(pool: MixPool, peerId: PeerId) =
  ## Copies the addresses, protocols, ENR and shards that the peer store has for
  ## `peerId`. When the peer store has none, after a delete or an expiry, the
  ## pool keeps its copy. A record with only addresses that a hop cannot carry
  ## empties the copy of the addresses.
  let store = pool.store
  let lastDialed = store[LastSeenOutboundBook][peerId]
  let stored = store[AddressBook][peerId]
  let protocols = store[ProtoBook][peerId]
  let record = store[ENRBook][peerId]
  let shards = store[ShardBook][peerId]
  pool.known.withValue(peerId, node):
    if lastDialed.isSome():
      node.lastDialed = Opt.some(lastDialed.get().stripPeerId())
    if stored.len > 0:
      node.stored = stored.filterIt(pool.usableHopAddress(peerId, it))
    if protocols.len > 0:
      node.protocols = protocols
    if record.raw.len > 0:
      node.enr = record
    if shards.len > 0:
      node.shards = shards

proc hasProtocol*(pool: MixPool, peerId: PeerId, protocol: string): bool =
  ## True when the last known protocols of the mix node `peerId` have `protocol`.
  pool.known.withValue(peerId, node):
    return protocol in node.protocols
  return false

proc hasShard*(pool: MixPool, peerId: PeerId, cluster, shard: uint16): bool =
  ## True when the last known ENR or shards of the mix node `peerId` have `shard`
  ## of `cluster`.
  pool.known.withValue(peerId, node):
    return node.enr.containsShard(cluster, shard) or shard in node.shards
  return false

proc makeRoom(pool: MixPool) =
  ## Removes one discovered node when the pool has `maxDiscovered` of them. A
  ## node that is not a pool member goes first, then the node whose last record,
  ## dial or connection is the oldest.
  let members = pool.members[MixPubKeyBook]
  var count = 0
  var removed: Opt[PeerId]
  var removedRank: (bool, Moment)
  for peerId, node in pool.known:
    if node.source == MixNodeSource.Configured:
      continue
    count.inc()
    let rank = (peerId in members, node.lastSeen)
    if removed.isNone() or rank < removedRank:
      removed = Opt.some(peerId)
      removedRank = rank
  if count < pool.maxDiscovered or removed.isNone():
    return
  pool.known.del(removed.get())
  trace "Mix peer removed at the limit of discovered nodes", peerId = removed.get()
  pool.refreshPeer(removed.get())

proc learn(pool: MixPool, peerId: PeerId) =
  ## Copies the mix key that the peer store has for `peerId`. When the peer
  ## store deletes the key, the pool keeps the node.
  let mixPubKey = pool.store[MixPubKeyBook][peerId]
  if mixPubKey == default(Curve25519Key) or
      peerId == pool.peerManager.switch.peerInfo.peerId:
    return
  if peerId notin pool.known:
    # The peer id holds the libp2p key of the hop.
    var libp2pPubKey: crypto.PublicKey
    if not peerId.extractPublicKey(libp2pPubKey) or libp2pPubKey.scheme != Secp256k1:
      return
    pool.makeRoom()
    pool.known[peerId] =
      KnownMixNode(source: MixNodeSource.Discovered, libp2pPubKey: libp2pPubKey.skkey)
  pool.known.withValue(peerId, node):
    # The last key wins, also for a configured node.
    node.mixPubKey = mixPubKey
    node.lastSeen = Moment.now()
  pool.copyPeerInfo(peerId)
  pool.refreshPeer(peerId)

proc addBookHandlers(pool: MixPool) =
  ## Follows the peer store books that change a pool entry. The handlers cannot
  ## be removed. They read only these books, because a peer store delete walks
  ## the books and a read can add a book.
  let store = pool.store
  store[MixPubKeyBook].addHandler(
    proc(peerId: PeerId) {.gcsafe, raises: [].} =
      pool.learn(peerId)
  )
  let onPeerInfo = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    if peerId in pool.known:
      pool.copyPeerInfo(peerId)
      pool.refreshPeer(peerId)
  store[AddressBook].addHandler(onPeerInfo)
  store[LastSeenOutboundBook].addHandler(onPeerInfo)
  store[ProtoBook].addHandler(onPeerInfo)
  store[ENRBook].addHandler(onPeerInfo)
  store[ShardBook].addHandler(onPeerInfo)

proc addChangeHandler*(pool: MixPool, handler: PeerBookChangeHandler) =
  ## Calls `handler` when a peer joins or leaves the pool.
  pool.members[MixPubKeyBook].addHandler(handler)

proc add*(pool: MixPool, info: MixPubInfo) =
  ## Adds a configured mix node. The node stays for the life of the pool. The
  ## call also writes the node to the peer store, so that this node can dial it.
  if info.peerId == pool.peerManager.switch.peerInfo.peerId:
    return
  # A node that the pool knows keeps its copies.
  var node = pool.known.getOrDefault(info.peerId)
  node.source = MixNodeSource.Configured
  if info.multiAddr notin node.configured:
    node.configured.add(info.multiAddr)
  node.mixPubKey = info.mixPubKey
  node.libp2pPubKey = info.libp2pPubKey
  node.lastSeen = Moment.now()
  pool.known[info.peerId] = node
  MixNodePool.new(pool.store).add(info)
  pool.refreshPeer(info.peerId)

proc dialPeer(
    pool: MixPool, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Dials `peerId` at its hop addresses. An existing connection counts as a
  ## success.
  let addresses = pool.hopAddresses(peerId)
  if addresses.len == 0:
    return false
  try:
    await pool.peerManager.switch.connect(peerId, addresses).wait(DefaultDialTimeout)
  except AsyncTimeoutError:
    debug "Mix peer dial timed out", peerId = peerId, addresses = $addresses
    return false
  except DialFailedError as exc:
    debug "Mix peer dial failed",
      peerId = peerId, addresses = $addresses, error = exc.msg
    return false
  pool.known.withValue(peerId, node):
    node.lastSeen = Moment.now()
  debug "Mix peer dial succeeded", peerId = peerId
  return true

proc dial*(pool: MixPool, peerId: PeerId): Future[bool].Raising([CancelledError]) =
  ## Returns the running dial of `peerId`, or starts a new one. A second dial
  ## would wait on the libp2p dial lock and then dial again.
  pool.dials.withValue(peerId, running):
    if not running[].finished():
      return running[]
  var done: seq[PeerId]
  for id, running in pool.dials:
    if running.finished():
      done.add(id)
  for id in done:
    pool.dials.del(id)
  let started = pool.dialPeer(peerId)
  pool.dials[peerId] = started
  return started

proc stopped*(pool: MixPool): bool =
  ## True from `stop` to `start`.
  pool.dialsStopped

proc age(pool: MixPool) =
  ## Removes each discovered node with no peer record, successful dial or
  ## connection for `discoveredTtl`.
  let now = Moment.now()
  var old: seq[PeerId]
  for peerId, node in pool.known.mpairs():
    if node.source == MixNodeSource.Configured:
      continue
    if pool.peerManager.switch.isConnected(peerId):
      node.lastSeen = now
    elif now - node.lastSeen >= pool.discoveredTtl:
      old.add(peerId)
  for peerId in old:
    pool.known.del(peerId)
    trace "Mix peer removed, no record for too long", peerId = peerId
    pool.refreshPeer(peerId)

proc maintain*(pool: MixPool) {.async: (raises: [CancelledError]).} =
  ## One pass of the pool loop. It removes old discovered nodes. Public for
  ## tests.
  pool.age()

proc poolLoop(pool: MixPool) {.async: (raises: [CancelledError]).} =
  while true:
    await pool.maintain()
    await sleepAsync(pool.poolLoopInterval)

proc start*(pool: MixPool) =
  ## Allows dials again and runs the pool loop.
  pool.dialsStopped = false
  if pool.loop.isNil() or pool.loop.finished():
    pool.loop = pool.poolLoop()

proc stop*(pool: MixPool) {.async: (raises: []).} =
  ## Cancels the loop and the dials, and blocks new ones until `start`.
  pool.dialsStopped = true
  if not pool.loop.isNil():
    await pool.loop.cancelAndWait()
    pool.loop = nil
  await noCancel allFutures(toSeq(pool.dials.values()).mapIt(it.cancelAndWait()))
  pool.dials.clear()

proc new*(
    T: typedesc[MixPool], peerManager: PeerManager, policy: PeerAddressPolicy
): T =
  let members = PeerStore.new(nil)
  let pool = T(
    peerManager: peerManager,
    policy: policy,
    members: members,
    nodePool: MixNodePool.new(members),
  )
  pool.addBookHandlers()
  for peerId in toSeq(pool.store[MixPubKeyBook].book.keys()):
    pool.learn(peerId)
  return pool
