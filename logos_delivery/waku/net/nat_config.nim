{.push raises: [].}

## Maps a NAT strategy (see `nat_strategy`) onto libp2p's NATService.
##
## `NatAny` is resolved once, at node start: probe UPnP first, then
## NAT-PMP; the winner becomes the strategy. The resolved strategy
## selects libp2p's `NATConfig` mode (`upnpConfig` / `natPmpConfig`).
## The switch's NATService then owns gateway discovery, port mapping and
## lease refresh.
##
## Also provides the strategy's port mapper and the leased udp mapping
## for sockets outside the switch (discv5).

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
  ## Lease requested for udp mappings made outside the switch's NATService
  ## (discv5). A permanent (0) lease cannot be used: RFC 6886 gives a zero
  ## lifetime the meaning "delete the mapping", and libp2p's NAT-PMP
  ## mapper rejects it. UPnP firmware also caps long leases (24h
  ## observed). The caller must renew the mapping well inside this lease.

proc natPortMapper*(strategy: NatStrategy): Opt[PortMapper] =
  ## The libp2p port mapper for a resolved strategy: `NatUpnp` and
  ## `NatPmp` get their protocol's mapper. Every other kind has none.
  ## `NatAny` is unresolved user intent: resolve it with
  ## `resolveNatStrategy` first.
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

proc resolveNatStrategy*(
    strategy: NatStrategy, discoveryTimeout = NatDiscoveryTimeout
): Future[NatStrategy] {.async: (raises: [CancelledError]).} =
  ## Resolves `NatAny` with one startup probe: UPnP first, then NAT-PMP.
  ## The first protocol whose gateway discovery answers wins. When
  ## neither answers, the result is `NatNone` and the process runs
  ## without port mapping.
  ##
  ## Every other kind passes through unchanged, without probing. For
  ## `NatUpnp` and `NatPmp` the switch's NATService performs its own
  ## discovery and retries on its refresh interval.
  if strategy.kind != NatAny:
    return strategy

  for candidate in [NatStrategy(kind: NatUpnp), NatStrategy(kind: NatPmp)]:
    let mapper = natPortMapper(candidate).valueOr:
      continue
    let found = await mapper.discover(discoveryTimeout)
    await mapper.close()
    if found.isOk():
      info "resolved --nat any", winner = $candidate
      return candidate
    info "NAT gateway probe failed", strategy = $candidate, err = found.error

  warn "--nat any: no gateway answered discovery; continuing without port mapping"
  NatStrategy(kind: NatNone)

proc natConfig*(
    strategy: NatStrategy, discoveryTimeout = NatDiscoveryTimeout
): Opt[NATConfig] =
  ## The libp2p `NATConfig` for a resolved strategy. `NatUpnp` and
  ## `NatPmp` turn the NATService's port mapping on in their protocol's
  ## mode. `NatAny` is unresolved user intent (see `resolveNatStrategy`)
  ## and gets none. `NatNone` and `NatExtIp` get no NATService at all:
  ## the static external IP of `NatExtIp` goes into `NetConfig` and the
  ## ENR before the switch exists, and libp2p's explicit-ip address
  ## mapper would drop dns4 announced addresses.
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
  ## Discover the gateway through `mapper` and map a single udp port with a
  ## `NatUdpLeaseSeconds` lease, for sockets that live outside the switch
  ## and its NATService (discv5). The caller renews the mapping: the lease
  ## is finite and must be requested again before it expires.
  let externalIp = (await mapper.discover(discoveryTimeout)).valueOr:
    return err("NAT discovery failed: " & error)
  let granted = (
    await mapper.map(internalPort, externalPort, mpUdp, NatUdpLeaseSeconds)
  ).valueOr:
    return err("NAT port mapping failed: " & error)
  return ok((externalIp: externalIp, externalPort: granted))

type UdpMappingAnnouncer* = proc(port: Opt[Port]) {.gcsafe, raises: [].}
  ## Receives announcement changes for a kept udp mapping. `some` announces
  ## that external port. `none` announces the bind port again.

const RenewalAttemptsPerLease = 3
  ## In-lease renewal attempts on the RFC 6886 schedule: at 1/2, 3/4 and
  ## 7/8 of the lease, measured from the grant. Without a cap the halving
  ## series shrinks the spacing toward zero.

proc keepUdpMappingAlive*(
    mapper: PortMapper,
    internalPort: Port,
    grantedPort: Port,
    projectedAtStart: bool,
    shouldProject: proc(): bool {.gcsafe, raises: [].},
    announce: UdpMappingAnnouncer,
    leaseDuration = chronos.seconds(int64(NatUdpLeaseSeconds)),
    discoveryTimeout = NatDiscoveryTimeout,
): Future[void] {.async: (raises: []).} =
  ## Renew a udp mapping through `mapper` on the renewal schedule of
  ## RFC 6886 (NAT-PMP): first halfway into the lease, then halving the
  ## time that remains, `RenewalAttemptsPerLease` tries per lease. Each
  ## renewal asks for the port the gateway granted before. The keeper
  ## owns `mapper` and closes it on exit.
  ##
  ## Attempt times are measured from the moment just before the granting
  ## request. The gateway's lease clock cannot start earlier than that
  ## moment. Each attempt sleeps until its absolute time, so request
  ## latency cannot drift the schedule or push an attempt past the lease.
  ##
  ## The announcement follows the mapping while `shouldProject` returns
  ## true. A renewal granted on a different port causes a new
  ## announcement. When every in-lease attempt has failed, the gateway
  ## drops the mapping the moment the lease ends. At that instant the
  ## keeper announces the bind port again, and probes at a flat
  ## half-lease interval until the gateway answers again.
  var granted = grantedPort
  var grantedAt = Moment.now()
  var projected = projectedAtStart
  var attempt = 0
  try:
    while true:
      let wakeAt =
        if attempt < RenewalAttemptsPerLease:
          # the halving schedule: 1/2, 3/4, 7/8 of the lease, from the grant
          grantedAt + leaseDuration - leaseDuration div (2 shl attempt)
        elif attempt == RenewalAttemptsPerLease:
          # the lease ends here; the revert below fires on this wake
          grantedAt + leaseDuration
        else:
          # lease lost; probe for a fresh mapping without a deadline
          Moment.now() + leaseDuration div 2
      let now = Moment.now()
      if wakeAt > now:
        await sleepAsync(wakeAt - now)

      if projected and Moment.now() - grantedAt >= leaseDuration:
        warn "NAT mapping lease expired without a successful renewal; " &
          "reverting the announced port", lapsedExternalPort = granted
        projected = false
        announce(Opt.none(Port))

      let attemptAt = Moment.now()
      let mapped = await mapUdpPort(mapper, internalPort, granted, discoveryTimeout)
      if mapped.isErr():
        inc attempt
        debug "NAT mapping renewal failed", err = mapped.error, attempt
      else:
        attempt = 0
        grantedAt = attemptAt
        let newGranted = mapped.get().externalPort
        let project = shouldProject()
        if project and not projected:
          info "NAT mapping usable; announcing the external port",
            externalPort = newGranted
          announce(Opt.some(newGranted))
        elif projected and not project:
          info "external address lost; reverting the announced port"
          announce(Opt.none(Port))
        elif project and newGranted != granted:
          warn "NAT mapping renewed on a different external port; re-announcing",
            previous = granted, newGranted = newGranted
          announce(Opt.some(newGranted))
        projected = project
        granted = newGranted
  except CancelledError:
    discard
  finally:
    await mapper.close()
