{.push raises: [].}

## Maps a NAT strategy onto libp2p's NATService, and provides the port
## mapper and the leased udp mapping for sockets outside the switch.

import std/net
import chronos, chronicles, results
import
  libp2p/services/natservice,
  libp2p/services/nat/[portmapper, upnp_mapper, natpmp_mapper]
import ./nat_strategy

export nat_strategy, natservice, portmapper

logScope:
  topics = "nat"

const NatDiscoveryTimeout = DefaultNatDiscoveryTimeoutMs.int64.milliseconds

  ## Lease for udp mappings outside the NATService (discv5). RFC 6886 uses
  ## lease 0 to delete a mapping, so the lease is finite and renewed.

proc natPortMapper*(strategy: NatStrategy): Opt[PortMapper] =
  ## The libp2p port mapper for a resolved strategy. Only NatUpnp and
  ## NatPmp have one. Resolve NatAny with resolveNatStrategy first.
  try:
    case strategy.kind
    of NatUpnp:
      Opt.some(PortMapper(UpnpMapper.new()))
    of NatPmp:
      Opt.some(PortMapper(NatPmpMapper.new()))
    of NatAny, NatNone, NatExtIp:
      Opt.none(PortMapper)
  except ResourceExhaustedError as e:
    error "Failed to construct NAT port mapper", err = e.msg
    Opt.none(PortMapper)

type ProbeMapperFactory* =
  proc(strategy: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].}
  ## Supplies the probe mapper for a strategy. Tests inject scripted
  ## mappers. Production uses natPortMapper.

proc closeProbeLater(mapper: PortMapper, discoveryTimeout: Duration) =
  ## Deferred detached close: a probe worker inside its C call cannot
  ## stall node start. The grace outlasts the worker's retry rounds.
  proc closeLater() {.async: (raises: []).} =
    try:
      await sleepAsync(discoveryTimeout * 5 + chronos.seconds(1))
    except CancelledError:
      discard
    await mapper.close()

  asyncSpawn closeLater()

proc resolveNatStrategy*(
    strategy: NatStrategy,
    discoveryTimeout = NatDiscoveryTimeout,
    mapperFor: ProbeMapperFactory = nil,
): Future[NatStrategy] {.async: (raises: [CancelledError]).} =
  ## Resolve NatAny with one startup probe: UPnP first, then NAT-PMP.
  ## No answer resolves to NatNone. Every other kind passes through.
  if strategy.kind != NatAny:
    return strategy

  for candidate in [NatStrategy(kind: NatUpnp), NatStrategy(kind: NatPmp)]:
    let mapper = (
      if mapperFor.isNil():
        natPortMapper(candidate)
      else:
        mapperFor(candidate)
    ).valueOr:
      continue
    let found = await mapper.discover(discoveryTimeout)
    mapper.closeProbeLater(discoveryTimeout)
    if found.isOk():
      info "resolved --nat any", winner = $candidate
      return candidate
    info "NAT gateway probe failed", strategy = $candidate, err = found.error

  warn "--nat any: no gateway answered discovery; continuing without port mapping"
  NatStrategy(kind: NatNone)

proc natConfig*(
    strategy: NatStrategy, discoveryTimeout = NatDiscoveryTimeout
): Opt[NATConfig] =
  ## The libp2p NATConfig for a resolved strategy. Only NatUpnp and
  ## NatPmp get one. The NatExtIp address is static state in NetConfig.
  case strategy.kind
  of NatUpnp:
    Opt.some(upnpConfig(discoveryTimeout = discoveryTimeout))
  of NatPmp:
    Opt.some(natPmpConfig(discoveryTimeout = discoveryTimeout))
  of NatAny, NatNone, NatExtIp:
    Opt.none(NATConfig)
