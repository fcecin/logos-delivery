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

const NatUdpLeaseSeconds* = uint32(DefaultLeaseDuration.seconds)
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

proc mapUdpPort*(
    mapper: PortMapper,
    internalPort: Port,
    externalPort: Port,
    discoveryTimeout = NatDiscoveryTimeout,
): Future[Result[tuple[externalIp: IpAddress, externalPort: Port], string]] {.
    async: (raises: [CancelledError])
.} =
  ## Discover the gateway and map one udp port with a finite lease, for
  ## sockets outside the switch (discv5). The caller renews the lease.
  let externalIp = (await mapper.discover(discoveryTimeout)).valueOr:
    return err("NAT discovery failed: " & error)
  let granted = (
    await mapper.map(internalPort, externalPort, mpUdp, NatUdpLeaseSeconds)
  ).valueOr:
    return err("NAT port mapping failed: " & error)
  return ok((externalIp: externalIp, externalPort: granted))

type UdpMappingAnnouncer* =
  proc(externalIp: Opt[IpAddress], externalPort: Opt[Port]) {.gcsafe, raises: [].}
  ## Receives announcement changes for a kept udp mapping. `some` values
  ## announce that external endpoint. `none` announces the bind port again.

const RenewalAttemptsPerLease = 3
  ## In-lease renewal attempts on the RFC 6886 halving schedule. Without
  ## a cap the halving series shrinks the spacing toward zero.

proc keepUdpMappingAlive*(
    mapper: PortMapper,
    internalPort: Port,
    desiredExternalPort: Port,
    announce: UdpMappingAnnouncer,
    leaseDuration = chronos.seconds(int64(NatUdpLeaseSeconds)),
    discoveryTimeout = NatDiscoveryTimeout,
): Future[void] {.async: (raises: []).} =
  ## Acquire a udp mapping and keep it alive on the RFC 6886 schedule.
  ## The keeper owns the mapper: it unmaps and closes on exit.
  var granted = desiredExternalPort
  var grantedIp = Opt.none(IpAddress)
  var haveLease = false
  var projected = false
  var grantedAt = Moment.now()
  var attempt = 0
  var firstAttempt = true
  try:
    while true:
      let wakeAt =
        if not haveLease:
          if firstAttempt:
            Moment.now()
          else:
            # acquisition and recovery probing, at half-lease cadence
            Moment.now() + leaseDuration div 2
        elif attempt < RenewalAttemptsPerLease:
          # the halving schedule: 1/2, 3/4, 7/8 of the lease, from the grant
          grantedAt + leaseDuration - leaseDuration div (2 shl attempt)
        else:
          # the lease ends here. The revert below fires on this wake.
          grantedAt + leaseDuration
      firstAttempt = false
      let now = Moment.now()
      if wakeAt > now:
        await sleepAsync(wakeAt - now)

      if projected and Moment.now() - grantedAt >= leaseDuration:
        warn "NAT mapping lease expired without a successful renewal; " &
          "reverting the announced port", lapsedExternalPort = granted
        projected = false
        haveLease = false
        announce(Opt.none(IpAddress), Opt.none(Port))

      let attemptAt = Moment.now()
      let mapped = await mapUdpPort(mapper, internalPort, granted, discoveryTimeout)
      if mapped.isErr():
        inc attempt
        debug "NAT udp mapping attempt failed", err = mapped.error, attempt
      else:
        attempt = 0
        grantedAt = attemptAt
        haveLease = true
        let (externalIp, newGranted) = mapped.get()
        if not projected:
          info "NAT udp mapping usable; announcing the external endpoint",
            externalIp = externalIp, externalPort = newGranted
          announce(Opt.some(externalIp), Opt.some(newGranted))
        elif newGranted != granted or Opt.some(externalIp) != grantedIp:
          warn "NAT udp mapping moved; re-announcing",
            previous = granted, externalIp = externalIp, externalPort = newGranted
          announce(Opt.some(externalIp), Opt.some(newGranted))
        projected = true
        granted = newGranted
        grantedIp = Opt.some(externalIp)
  except CancelledError:
    discard
  finally:
    if haveLease:
      discard await noCancel(mapper.unmap(granted, mpUdp))
    await noCancel(mapper.close())
