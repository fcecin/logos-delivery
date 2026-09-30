{.push raises: [].}

## Advertising this node on the kademlia service-discovery network.
##
## Every participating node registers interest in `/logos/delivery`, and a node
## that serves anything (relay, store, filter, lightpush) also advertises
## itself under it. That gives the network one place to look for its own
## members, instead of each node only being findable through whichever
## protocol-specific service it happens to run.
##
## Which backends take part is decided by what they declare, not by naming
## them: a backend receives this only if its `keyKinds` includes `service`. That is
## true of both kademlia hosts -- in-process and plugin -- and false of discv5,
## whose advertising means mutating our own ENR and which rejects `service:` keys
## outright.

import std/base64
import chronos, chronicles, results
import libp2p_mix/mix_protocol
import libp2p/protocols/connectivity/autonat/types
import
  logos_delivery/waku/discovery/peer_discovery_interface,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/waku_enr/capabilities,
  logos_delivery/waku/waku_mix

logScope:
  topics = "waku discovery advertise"

const SvcKey = ServiceKeyPrefix & LogosDeliveryServiceId

const
  AdvertFormatVersion* = 1'u8
    ## Bumped whenever the layout below changes, so a reader can refuse a
    ## payload it does not understand instead of misreading it.

  MaxAdvertLen* = 32
    ## Hard ceiling on the advertised payload: libp2p validates the `data` of a
    ## ServiceInfo and rejects anything larger. The JSON shape this replaced ran
    ## to ~188 bytes and could never be advertised at all. For scale, the one
    ## service advertised successfully in production -- mix -- carries a 32-byte
    ## Curve25519 key and nothing else.

  MaxRawLen = 24 ## Base64 of 24 bytes is exactly 32 characters, unpadded.

  AdvertHeaderLen = 4
  MaxShardBitmapLen* = MaxRawLen - AdvertHeaderLen
    ## 20 bytes, so shards 0..159 are representable. Higher indices are dropped
    ## rather than silently aliased onto a lower bit, which would advertise
    ## membership of a shard we are not on.

proc selfAdvertisementData*(conf: WakuConf, shards: seq[uint16]): seq[byte] =
  ## The payload published alongside the advertisement: base64 of a compact
  ## binary record, at most `MaxAdvertLen` bytes on the wire.
  ##
  ## Layout before encoding, little-endian bit order within each bitmap byte:
  ##
  ##   [0]      format version
  ##   [1..2]   cluster id, big-endian uint16
  ##   [3]      capabilities bitfield
  ##   [4..]    shard bitmap, one bit per shard, only as long as it needs to be
  ##
  ## Everything a peer needs in order to decide whether to dial us: which
  ## network, what we serve, which shards we are on. `cluster` travels with
  ## `shards` because a shard index is meaningless without it. Capabilities
  ## travel as the bitfield rather than as protocol id strings -- the same
  ## information, one byte instead of roughly a hundred. The node's version
  ## string used to be included and is not: it is the single largest field, it
  ## is not a selection criterion, and it does not fit the budget.
  ##
  ## Base64 rather than the raw bytes, even though the plugin ABI carries
  ## `data` as a length-counted byte array and the in-process backend would
  ## take binary happily. The plugin-hosted path reaches its provider over
  ## logos-core, whose generated client marshals arguments as JSON strings, and
  ## a JSON string must be valid UTF-8 -- a raw record throws
  ## `type_error.316` there and takes the hosting module down with it.
  ## The two hosts must publish byte-identical payloads to be able to find each
  ## other, so the encoding belongs here, once, rather than on one path only.
  var raw = newSeq[byte](AdvertHeaderLen)
  raw[0] = AdvertFormatVersion
  raw[1] = byte(conf.clusterId shr 8)
  raw[2] = byte(conf.clusterId and 0xff)
  raw[3] = byte(conf.wakuFlags)

  for shard in shards:
    let idx = int(shard)
    let byteIdx = idx div 8
    if byteIdx >= MaxShardBitmapLen:
      warn "shard index too large to advertise",
        shard = shard, max = MaxShardBitmapLen * 8
      continue
    while raw.len <= AdvertHeaderLen + byteIdx:
      raw.add(0'u8)
    raw[AdvertHeaderLen + byteIdx] =
      raw[AdvertHeaderLen + byteIdx] or byte(1'u8 shl (idx mod 8))

  return cast[seq[byte]](base64.encode(raw))

type ServiceBackend = tuple[discovery: IPeerDiscovery, id: string]

proc serviceBackends(
    discoveries: seq[IPeerDiscovery]
): Future[seq[ServiceBackend]] {.async: (raises: []).} =
  ## The backends that take service keys, with their ids.
  var backends: seq[ServiceBackend]
  for discovery in discoveries:
    let info = (await discovery.backendInfo()).valueOr:
      debug "skipping backend with unreadable info", reason = error
      continue
    if ServiceKind in info.keyKinds:
      backends.add((discovery, info.id))
  return backends

proc advertiseSelf*(
    discoveries: seq[IPeerDiscovery], conf: WakuConf, shards: seq[uint16]
): Future[void] {.async: (raises: []).} =
  ## Registers interest in `/logos/delivery` on every service-capable backend,
  ## and advertises this node there when it serves something. `shards` is what
  ## the node is actually subscribed to -- the caller resolves that, since the
  ## configured list is not the same thing under autosharding.
  ##
  ## Best-effort per backend: a node whose discovery could not announce it is
  ## degraded, not broken, and the other backends should still get their turn.
  let serves = conf.wakuFlags.isServiceNode()
  let data =
    if serves:
      selfAdvertisementData(conf, shards)
    else:
      @[]

  for discovery in discoveries:
    let info = (await discovery.backendInfo()).valueOr:
      debug "skipping backend with unreadable info", reason = error
      continue

    if ServiceKind notin info.keyKinds:
      continue

    (await discovery.registerInterest(SvcKey)).isOkOr:
      warn "could not register interest in the delivery network",
        backend = info.id, reason = error

    if not serves:
      continue

    (await discovery.startAdvertising(SvcKey, data)).isOkOr:
      warn "could not advertise this node on the delivery network",
        backend = info.id, reason = error
      continue

    info "advertising this node on the delivery network",
      backend = info.id, protocols = conf.wakuFlags.toCodecs()

proc mixHopOffer*(
    conf: WakuConf,
    selfHopServes: bool,
    reachability: NetworkReachability,
    hopSource: MixHopSource,
): Result[void, string] =
  ## Ok when this node can be a hop for other nodes, else the reason. A
  ## `Reported` hop makes no offer, because an autonat v1 dial-back over quic
  ## passes the NAT mapping of this node. For the same reason `Reachable` is
  ## weak evidence for an `Own` hop too.
  if conf.mixConf.isNone():
    return err("mix is not mounted")
  if not conf.wakuFlags.isServiceNode():
    return err("a client-only node sends over mix but does not forward for others")
  if not selfHopServes:
    return err("the hop policy does not accept the own mix hop address")
  if hopSource == MixHopSource.Reported:
    return err(
      "only other peers reported the address of the own mix hop. Set " &
        "--nat=extip or --ext-multiaddr to offer this node as a mix hop"
    )
  if reachability != NetworkReachability.Reachable:
    return err(
      "autonat does not report that other nodes can dial this node (" & $reachability &
        ")"
    )
  return ok()

proc writeMixOffer(
    discoveries: seq[IPeerDiscovery], conf: WakuConf, offered: bool, renew: bool
): Future[void] {.async: (raises: []).} =
  ## Starts or stops the mix advertisement on each backend. With `renew`, a stop
  ## comes first, because a backend ignores a second start.
  let key = ServiceKeyPrefix & MixProtocolID
  let data = @(conf.mixConf.get().mixPubKey)
  for (discovery, id) in await serviceBackends(discoveries):
    if offered:
      if renew:
        (await discovery.stopAdvertising(key)).isOkOr:
          debug "Could not stop the mix advertisement before its renewal",
            backend = id, reason = error
      (await discovery.startAdvertising(key, data)).isOkOr:
        warn "could not advertise this node as a mix node", backend = id, reason = error
        continue
      info "advertising this node as a mix node", backend = id
    else:
      (await discovery.stopAdvertising(key)).isOkOr:
        warn "Could not stop advertising this node as a mix node",
          backend = id, reason = error

proc updateMixAdvertisement*(
    discoveries: seq[IPeerDiscovery],
    conf: WakuConf,
    mix: WakuMix,
    reachability: NetworkReachability,
    hopSource: MixHopSource,
): Future[void] {.async: (raises: []).} =
  ## Starts or stops the hop offer as `mixHopOffer` decides, on a change only.
  ## Call it when the reachability or the own hop changes. `Unknown` keeps an
  ## offer, and makes one for a `Configured` hop, because autonat cannot confirm
  ## a node that only takes inbound connections. Of the reachability values,
  ## only `NotReachable` stops it.
  if mix.isNil() or conf.mixConf.isNone():
    return
  let evidence =
    if reachability == NetworkReachability.Unknown and
        (mix.offer.offered or hopSource == MixHopSource.Configured):
      NetworkReachability.Reachable
    else:
      reachability
  let allowed = mixHopOffer(conf, mix.selfHopServes(), evidence, hopSource)
  # A service node logs each new reason for no offer once.
  if allowed.isErr() and allowed.error != mix.offer.refusal and
      conf.wakuFlags.isServiceNode():
    mix.offer.refusal = allowed.error
    if not mix.offer.offered:
      info "Not offering this node as a mix hop to other nodes", reason = allowed.error
  if allowed.isOk():
    mix.offer.refusal = ""
  if allowed.isOk() == mix.offer.offered:
    return
  mix.offer.offered = allowed.isOk()
  if allowed.isOk():
    info "Offering this node as a mix hop to other nodes"
  else:
    info "Stopping the offer of this node as a mix hop", reason = allowed.error
  await writeMixOffer(discoveries, conf, mix.offer.offered, renew = false)

proc advertiseMix*(
    discoveries: seq[IPeerDiscovery],
    conf: WakuConf,
    mix: WakuMix,
    reachability: NetworkReachability,
    hopSource: MixHopSource,
): Future[void] {.async: (raises: []).} =
  ## Registers interest in mix nodes on each backend, then makes the hop offer
  ## when `mixHopOffer` allows it.
  ##
  ## This goes through the interface rather than through
  ## `KademliaDiscoveryConf.servicesToAdvertise`, which is where it used to be
  ## injected at conf time. That reached only the in-process backend -- the conf
  ## object belongs to it, and the two kademlia hosts are mutually exclusive --
  ## so a node running mix with plugin-hosted discovery advertised nothing and
  ## found no mix peers. Same route as `advertiseSelf`, so both hosts get it.
  ##
  ## Only the service id and the key bytes travel: whether a record is signed
  ## here or by libp2p is each backend's business (see `startAdvertising` on
  ## the interface).
  if conf.mixConf.isNone():
    return

  let key = ServiceKeyPrefix & MixProtocolID
  for (discovery, id) in await serviceBackends(discoveries):
    (await discovery.registerInterest(key)).isOkOr:
      warn "could not register interest in mix peers", backend = id, reason = error

  if not mix.isNil() and mix.offer.offered:
    # The offer came before the backends started. Make it again.
    await writeMixOffer(discoveries, conf, offered = true, renew = true)
  else:
    await updateMixAdvertisement(discoveries, conf, mix, reachability, hopSource)
