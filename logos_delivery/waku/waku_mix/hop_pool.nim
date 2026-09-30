## The pool of mix hops. It follows the peer store of the node, keeps one hop
## address for each eligible peer, and dials failed peers again.

{.push raises: [].}

import std/[sequtils, strutils, tables]
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
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/node/delivery_dialer

export peeraddrpolicy

logScope:
  topics = "waku mix pool"

const
  MinMixPoolSize* = 4
    ## The smallest pool that mix can build a path from. `PathLength` is 3, and
    ## with `exit_is_dest` the exit node is a pool member and not one of the hops.
  RevalidateDialsPerPass = 2

type MixHopPool* = ref object
  peerManager: PeerManager
  policy: PeerAddressPolicy ## The addresses that a hop can carry.
  known: MixNodePool ## Each peer with a mix key, in the peer store of the node.
  eligible: PeerStore ## One hop address for each peer that paths can use.
  pathPool: MixNodePool ## The pool over `eligible` that nim-libp2p-mix reads.
  dials: Table[PeerId, Future[bool].Raising([CancelledError])]
    ## The running mix dial of each peer. A later attempt joins it.
  dialsStopped: bool
    ## True from `stop` to `start`. Then no send or observer starts a dial.
  loop: Future[void]
  # The three tunables are public for tests.
  maintenanceInterval*: Duration = chronos.seconds(15)
  revalidateBackoff*: Duration = chronos.seconds(30)
  revalidateMaxBackoff*: Duration = chronos.minutes(30)

const publicDirectAddressPolicy* = proc(ma: MultiAddress): bool {.gcsafe, raises: [].} =
  ## A public address that is not a relay route. Only a relay client can dial
  ## a relay route.
  isPublicMA(ma) and not isCircuitRelayMA(ma)

proc hopPolicyFor*(privateHops: bool): PeerAddressPolicy =
  ## The hop policy of `--mix-private-hops`.
  if privateHops: defaultAddressPolicy else: publicDirectAddressPolicy

proc accepts*(pool: MixHopPool, address: MultiAddress): bool =
  ## True when the policy accepts `address`, and for a relay route also the
  ## relay address, which the previous hop dials.
  let base = mix_multiaddr.getBaseTransport(address).valueOr:
    return false
  return
    dialableAddrs(pool.policy, [address]).len == 1 and
    dialableAddrs(pool.policy, [base]).len == 1

proc hopCarries(pool: MixHopPool, peerId: PeerId, address: MultiAddress): bool =
  ## True when the hop policy and the encoder both accept `address`.
  return
    pool.accepts(address) and mix_multiaddr.multiAddrToBytes(peerId, address).isOk()

proc store(pool: MixHopPool): PeerStore =
  pool.peerManager.switch.peerStore

proc hopAddresses(pool: MixHopPool, peerId: PeerId): seq[MultiAddress] =
  ## The addresses of `peerId` that a hop can carry, the last dialed first.
  var candidates: seq[MultiAddress]
  let lastSeen = pool.store[LastSeenOutboundBook][peerId]
  if lastSeen.isSome():
    candidates.add(lastSeen.get().stripPeerId())
  candidates.add(pool.store[AddressBook][peerId])
  var carried: seq[MultiAddress]
  for address in candidates:
    if address notin carried and pool.hopCarries(peerId, address):
      carried.add(address)
  return carried

proc hopAddress(pool: MixHopPool, peerId: PeerId): Opt[MultiAddress] =
  ## The address that a hop through `peerId` encodes.
  let carried = pool.hopAddresses(peerId)
  if carried.len == 0:
    return Opt.none(MultiAddress)
  return Opt.some(carried[0])

proc eligibleHop(pool: MixHopPool, peerId: PeerId): Opt[MixPubInfo] =
  ## The pool entry for `peerId`. It needs a secp256k1 key, a hop address and
  ## no failed-dial record.
  let store = pool.store
  if peerId == pool.peerManager.switch.peerInfo.peerId:
    return Opt.none(MixPubInfo)
  let mixPubKey = store[MixPubKeyBook][peerId]
  if mixPubKey == default(Curve25519Key):
    return Opt.none(MixPubInfo)
  if store[NumberFailedConnBook][peerId] > 0:
    return Opt.none(MixPubInfo)
  # `addPeer` and `MixNodePool.add` write the key book with each mix key.
  let pubKey = store[KeyBook][peerId]
  if pubKey.scheme != Secp256k1:
    return Opt.none(MixPubInfo)
  let address = pool.hopAddress(peerId).valueOr:
    return Opt.none(MixPubInfo)
  return Opt.some(MixPubInfo.init(peerId, address, mixPubKey, pubKey.skkey))

proc len*(pool: MixHopPool): int =
  ## The number of pool members.
  pool.eligible[MixPubKeyBook].len

proc paths*(pool: MixHopPool): MixNodePool =
  ## The pool that nim-libp2p-mix draws paths from.
  pool.pathPool

proc syncHop(pool: MixHopPool, peerId: PeerId) =
  ## Makes the pool entry of `peerId` agree with the peer store of the node.
  let eligible = pool.eligible
  let hop = pool.eligibleHop(peerId).valueOr:
    if peerId in eligible[MixPubKeyBook]:
      discard eligible[MixPubKeyBook].del(peerId)
      trace "Mix peer left the pool", peerId = peerId
    discard eligible[AddressBook].del(peerId)
    discard eligible[KeyBook].del(peerId)
    return

  let key = crypto.PublicKey(scheme: Secp256k1, skkey: hop.libp2pPubKey)
  if eligible[KeyBook][peerId] != key:
    eligible[KeyBook][peerId] = key
  if eligible[AddressBook][peerId] != @[hop.multiAddr]:
    # `set` keeps an `Infinite` entry that the new list leaves out.
    discard eligible[AddressBook].del(peerId)
    eligible[AddressBook].set(peerId, @[hop.multiAddr], AddressConfidence.Infinite)
  # The key comes last, so a pool change handler sees a complete entry.
  if eligible[MixPubKeyBook][peerId] != hop.mixPubKey:
    eligible[MixPubKeyBook][peerId] = hop.mixPubKey
    trace "Mix peer joined the pool", peerId = peerId, hop = $hop.multiAddr

proc refresh(pool: MixHopPool) =
  ## Syncs the whole pool. Book handlers cover the changes between passes.
  for peerId in pool.known.peerIds():
    pool.syncHop(peerId)
  for peerId in pool.pathPool.peerIds():
    if peerId notin pool.store[MixPubKeyBook]:
      pool.syncHop(peerId)

proc followPeerStore(pool: MixHopPool) =
  ## Syncs a peer when a book that decides its entry changes. The handlers
  ## cannot be removed.
  let store = pool.store
  let onChange = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    if peerId in store[MixPubKeyBook] or peerId in pool.eligible[MixPubKeyBook]:
      pool.syncHop(peerId)
  store[MixPubKeyBook].addHandler(onChange)
  store[AddressBook].addHandler(onChange)
  store[LastSeenOutboundBook].addHandler(onChange)
  store[KeyBook].addHandler(onChange)
  store[NumberFailedConnBook].addHandler(onChange)

proc addChangeHandler*(pool: MixHopPool, handler: PeerBookChangeHandler) =
  ## Calls `handler` when a peer joins or leaves the pool.
  pool.eligible[MixPubKeyBook].addHandler(handler)

proc add*(pool: MixHopPool, info: MixPubInfo) =
  ## Adds a configured mix node to the peer store.
  pool.known.add(info)

proc isLocalDialLimit(error: string): bool =
  ## True when a dial failed at a limit of this node (libp2p gives text only).
  return
    "getOutgoingSlot" in error or "connections limit reached" in error or
    "Outbound stream budget exceeded" in error or "can't dial self" in error

proc recordFailure(pool: MixHopPool, peerId: PeerId, reason: string, error = reason) =
  ## Records a failed dial against `peerId`, except while this node is offline
  ## or at a local limit. So one outage does not empty the whole pool.
  if not pool.peerManager.isOnline() or isLocalDialLimit(error):
    trace "Mix dial failed for a cause of this node", peerId = peerId, error = error
    return
  pool.peerManager.recordDialFailure(peerId, reason)

proc dialHop(
    pool: MixHopPool, peerId: PeerId
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Dials `peerId` at its hop addresses and records the result. An existing
  ## connection counts as a success.
  let addresses = pool.hopAddresses(peerId)
  if addresses.len == 0:
    return false
  try:
    await pool.peerManager.switch.connect(peerId, addresses).wait(DefaultDialTimeout)
  except AsyncTimeoutError:
    debug "Mix peer dial timed out", peerId = peerId, addresses = $addresses
    pool.recordFailure(peerId, "mix dial timed out")
    return false
  except DialFailedError as exc:
    debug "Mix peer dial failed",
      peerId = peerId, addresses = $addresses, error = exc.msg
    pool.recordFailure(peerId, "mix dial failed", exc.msg)
    return false
  pool.peerManager.recordDialSuccess(peerId, "mix")
  debug "Mix peer dial succeeded", peerId = peerId
  return true

proc dial*(pool: MixHopPool, peerId: PeerId): Future[bool].Raising([CancelledError]) =
  ## The running dial of `peerId`, or a new one. A second dial would only wait
  ## on the libp2p dial lock and dial again.
  pool.dials.withValue(peerId, running):
    if not running[].finished():
      return running[]
  var done: seq[PeerId]
  for id, running in pool.dials:
    if running.finished():
      done.add(id)
  for id in done:
    pool.dials.del(id)
  let started = pool.dialHop(peerId)
  pool.dials[peerId] = started
  return started

proc stopped*(pool: MixHopPool): bool =
  ## True from `stop` to `start`.
  pool.dialsStopped

proc poolHopDial(
    pool: MixHopPool, peerId: PeerId, addrs: seq[MultiAddress], protos: seq[string]
): bool =
  ## True for a mix stream dial to a pool peer at its hop address, with no
  ## connection. nim-libp2p-mix also dials addresses from the packets of other
  ## nodes, which prove nothing.
  if pool.dialsStopped or MixProtocolID notin protos:
    return false
  let hop = pool.pathPool.get(peerId).valueOr:
    return false
  return addrs == @[hop.multiAddr] and not pool.peerManager.switch.isConnected(peerId)

proc recordStreamDialFailure(
    pool: MixHopPool,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    protos: seq[string],
    error: string,
) =
  ## Records a failed stream dial that `poolHopDial` accepts.
  if not pool.poolHopDial(peerId, addrs, protos):
    return
  debug "Mix stream dial failed", peerId = peerId, error = error
  pool.recordFailure(peerId, "mix stream dial failed", error)

proc followStoppedDial(
    pool: MixHopPool, peerId: PeerId, addrs: seq[MultiAddress], protos: seq[string]
) =
  ## A send stops its entry dial at `MixReplyTimeout`, before a tcp dial to a
  ## host that is down has a result. One pool dial follows and records it.
  if not pool.poolHopDial(peerId, addrs, protos):
    return
  debug "Mix entry dial stopped, the pool dials the peer", peerId = peerId
  discard pool.dial(peerId)

proc backoff*(pool: MixHopPool, failures: int): Duration =
  ## The wait after `failures` failed dials in a row before the next dial.
  ## Public for tests.
  var pause = pool.revalidateBackoff
  for _ in 1 ..< failures:
    if pause >= pool.revalidateMaxBackoff:
      break
    pause = pause * 2
  return min(pause, pool.revalidateMaxBackoff)

proc revalidate*(pool: MixHopPool) {.async: (raises: [CancelledError]).} =
  ## Dials again the peers with a failed dial whose backoff is over. A success
  ## returns the peer to the pool. The pool has its own backoff, because a fleet
  ## node must come back soon and discovery cannot clear a record. Public for
  ## tests.
  if not pool.peerManager.isOnline():
    return
  let now = dialFailureClock()
  var due: seq[PeerId]
  for peerId in pool.known.peerIds():
    let failures = pool.store[NumberFailedConnBook][peerId]
    if failures == 0 or pool.hopAddress(peerId).isNone():
      continue
    if now >= pool.store[LastFailedConnBook][peerId] + pool.backoff(failures):
      due.add(peerId)
  await allFutures(due[0 ..< min(due.len, RevalidateDialsPerPass)].mapIt(pool.dial(it)))

proc maintain*(pool: MixHopPool) {.async: (raises: [CancelledError]).} =
  ## One maintenance pass. It sees what no handler reports (an address TTL),
  ## and dials failed peers again. Public for tests.
  pool.refresh()
  await pool.revalidate()

proc maintenanceLoop(pool: MixHopPool) {.async: (raises: [CancelledError]).} =
  while true:
    await pool.maintain()
    await sleepAsync(pool.maintenanceInterval)

proc start*(pool: MixHopPool) =
  ## Allows dials again and runs the maintenance loop.
  pool.dialsStopped = false
  if pool.loop.isNil() or pool.loop.finished():
    pool.loop = pool.maintenanceLoop()

proc stop*(pool: MixHopPool) {.async: (raises: []).} =
  ## Cancels the loop and the dials, and blocks new ones until `start`.
  pool.dialsStopped = true
  if not pool.loop.isNil():
    await pool.loop.cancelAndWait()
    pool.loop = nil
  await noCancel allFutures(toSeq(pool.dials.values()).mapIt(it.cancelAndWait()))
  pool.dials.clear()

proc new*(
    T: typedesc[MixHopPool], peerManager: PeerManager, policy: PeerAddressPolicy
): T =
  let eligible = PeerStore.new(nil)
  let pool = T(
    peerManager: peerManager,
    policy: policy,
    known: MixNodePool.new(peerManager.switch.peerStore),
    eligible: eligible,
    pathPool: MixNodePool.new(eligible),
  )
  pool.followPeerStore()
  # The dialer sees each hop dial of nim-libp2p-mix.
  if peerManager.switch.dialer of DeliveryDialer:
    let dialer = DeliveryDialer(peerManager.switch.dialer)
    dialer.dialFailureObservers.add(
      proc(
          peerId: PeerId, addrs: seq[MultiAddress], protos: seq[string], error: string
      ) {.gcsafe, raises: [].} =
        pool.recordStreamDialFailure(peerId, addrs, protos, error)
    )
    dialer.dialStopObservers.add(
      proc(
          peerId: PeerId, addrs: seq[MultiAddress], protos: seq[string]
      ) {.gcsafe, raises: [].} =
        pool.followStoppedDial(peerId, addrs, protos)
    )
  pool.refresh()
  return pool
