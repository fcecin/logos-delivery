{.push raises: [].}

## Maps a NAT strategy onto libp2p's NATService and provides its port mapper.
## The discv5 udp socket stays unmapped. A NATed node consumes discovery
## through outbound queries.

import std/net
import chronos, chronicles, results
import
  libp2p/services/natservice,
  libp2p/services/nat/[portmapper, upnp_mapper, natpmp_mapper]
import ./nat_strategy

export nat_strategy, natservice, portmapper

logScope:
  topics = "waku nat"

const NatDiscoveryTimeout = DefaultNatDiscoveryTimeoutMs.int64.milliseconds

proc natPortMapper*(strategy: NatStrategy): Opt[PortMapper] =
  ## The libp2p port mapper for the Upnp and Pmp strategies.
  ## Resolve Any first with resolveNatStrategy.
  try:
    case strategy.kind
    of NatStrategyKind.Upnp:
      return Opt.some(PortMapper(UpnpMapper.new()))
    of NatStrategyKind.Pmp:
      return Opt.some(PortMapper(NatPmpMapper.new()))
    of NatStrategyKind.Any, NatStrategyKind.None, NatStrategyKind.ExtIp:
      return Opt.none(PortMapper)
  except ResourceExhaustedError as e:
    error "Failed to construct NAT port mapper", err = e.msg
    return Opt.none(PortMapper)

type ProbeMapperFactory* =
  proc(strategy: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].}
  ## Supplies the probe mapper for a strategy.
  ## Tests inject scripted mappers. Production uses natPortMapper.

proc probeAndClose(
    mapper: PortMapper, discoveryTimeout: Duration
): Future[Result[IpAddress, string]] {.async: (raises: [CancelledError]).} =
  ## Discover through the mapper, then close it before returning,
  ## cancellation included. close() joins the mapper's worker thread.
  ## With a silently dropping gateway a NAT-PMP worker retries past the
  ## timeout (libnatpmp schedule, 127.75 s ceiling) and the close holds
  ## startup for the remainder.
  try:
    return await mapper.discover(discoveryTimeout)
  finally:
    await mapper.close()

proc resolveNatStrategy*(
    strategy: NatStrategy,
    discoveryTimeout = NatDiscoveryTimeout,
    mapperFor: ProbeMapperFactory = nil,
): Future[NatStrategy] {.async: (raises: [CancelledError]).} =
  ## Resolve the Any strategy with one startup probe: UPnP first, then
  ## NAT-PMP. No answer resolves to None. Every other kind passes through.
  ## Every constructed probe mapper is closed before this returns.
  if strategy.kind != NatStrategyKind.Any:
    return strategy

  for candidate in [
    NatStrategy(kind: NatStrategyKind.Upnp), NatStrategy(kind: NatStrategyKind.Pmp)
  ]:
    let mapper = (
      if mapperFor.isNil():
        natPortMapper(candidate)
      else:
        mapperFor(candidate)
    ).valueOr:
      continue
    let found = await mapper.probeAndClose(discoveryTimeout)
    if found.isOk():
      info "resolved --nat any", winner = $candidate
      return candidate
    info "NAT gateway probe failed", strategy = $candidate, err = found.error

  warn "--nat any: no gateway answered discovery; continuing without port mapping"
  return NatStrategy(kind: NatStrategyKind.None)

proc natConfig*(
    strategy: NatStrategy, discoveryTimeout = NatDiscoveryTimeout
): Opt[NATConfig] =
  ## The libp2p NATConfig for the Upnp and Pmp strategies.
  ## The ExtIp address is static state in NetConfig.
  case strategy.kind
  of NatStrategyKind.Upnp:
    return Opt.some(upnpConfig(discoveryTimeout = discoveryTimeout))
  of NatStrategyKind.Pmp:
    return Opt.some(natPmpConfig(discoveryTimeout = discoveryTimeout))
  of NatStrategyKind.Any, NatStrategyKind.None, NatStrategyKind.ExtIp:
    return Opt.none(NATConfig)
