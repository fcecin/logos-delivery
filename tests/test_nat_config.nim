{.used.}

import std/net
import testutils/unittests, chronos, results
import libp2p/services/nat/[portmapper, upnp_mapper, natpmp_mapper]
import ../logos_delivery/waku/net/nat_config

suite "NAT config - strategy parsing":
  test "valid strategies parse":
    check:
      parseNatStrategy("any").get().kind == NatAny
      parseNatStrategy("none").get().kind == NatNone
      parseNatStrategy("upnp").get().kind == NatUpnp
      parseNatStrategy("pmp").get().kind == NatPmp

  test "parsing is case-insensitive":
    check:
      parseNatStrategy("UPnP").get().kind == NatUpnp
      parseNatStrategy("NONE").get().kind == NatNone

  test "extip carries the address":
    let strategy = parseNatStrategy("extip:203.0.113.7").get()
    check:
      strategy.kind == NatExtIp
      $strategy.extIp == "203.0.113.7"

  test "invalid mechanism is rejected":
    check:
      parseNatStrategy("bogus").isErr()
      parseNatStrategy("").isErr()

  test "invalid extip address is rejected":
    check:
      parseNatStrategy("extip:notanip").isErr()

  test "strategies render back to their config strings":
    check:
      $parseNatStrategy("any").get() == "any"
      $parseNatStrategy("extip:203.0.113.7").get() == "extip:203.0.113.7"

type StubMapper = ref object of PortMapper
  ## Scripted port mapper: discovery yields `ip` when set, an error
  ## otherwise. `mapRejections` makes that many leading map() calls fail,
  ## like a router that refuses a request. `grantedPort`, when set, is
  ## granted instead of the requested external port, like a router that
  ## assigns a port of its own choosing.
  ip: Opt[IpAddress]
  mapRejections: int
  grantedPort: Opt[Port]
  discoverCalls: int
  mapCalls: int
  unmapCalls: int
  closeCalls: int
  lastMapProto: MapProto
  lastMapLease: uint32

method discover(
    self: StubMapper, timeout: Duration
): Future[Result[IpAddress, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  inc self.discoverCalls
  let ip = self.ip.valueOr:
    return err("stub discovery failure")
  return ok(ip)

method map(
    self: StubMapper,
    internalPort: Port,
    externalPort: Port,
    proto: MapProto,
    lease: uint32,
): Future[Result[Port, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  inc self.mapCalls
  self.lastMapProto = proto
  self.lastMapLease = lease
  # Mirror libp2p's NAT-PMP mapper: a permanent (0) lease is invalid input -
  # RFC 6886 uses a zero lifetime to delete a mapping. Keeping the stub as
  # strict as the strictest real mapper prevents lease-semantics bugs from
  # hiding behind a permissive test double.
  if lease == 0:
    return err("stub: lease 0 rejected (RFC 6886 delete semantics)")
  if self.mapRejections > 0:
    dec self.mapRejections
    return err("stub mapping rejection")
  return ok(self.grantedPort.get(externalPort))

method unmap(
    self: StubMapper, externalPort: Port, proto: MapProto
): Future[Result[void, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  inc self.unmapCalls
  return ok()

method close(self: StubMapper) {.async: (raises: []), gcsafe.} =
  inc self.closeCalls

proc stub(ip = ""): StubMapper =
  let address =
    if ip.len > 0:
      Opt.some(parseIpAddress(ip))
    else:
      Opt.none(IpAddress)
  StubMapper(ip: address)

suite "NAT config - resolveNatStrategy":
  asyncTest "every strategy but any passes through unchanged":
    for s in ["upnp", "pmp", "none", "extip:203.0.113.7"]:
      let strategy = parseNatStrategy(s).get()
      check $(await resolveNatStrategy(strategy)) == $strategy

  asyncTest "any always resolves to a concrete strategy":
    ## A real probe with a tiny timeout. Whatever this network answers,
    ## the result is never `any`: the switch and the discv5 keeper read
    ## a concrete protocol or none at all.
    let resolved = await resolveNatStrategy(NatStrategy(kind: NatAny), 50.millis)
    check resolved.kind in {NatUpnp, NatPmp, NatNone}

suite "NAT config - natConfig":
  test "upnp and pmp turn port mapping on in their protocol's mode":
    let
      upnp = natConfig(parseNatStrategy("upnp").get())
      pmp = natConfig(parseNatStrategy("pmp").get())
    check:
      upnp.get().portMapping.get().mode == PortMappingMode.Upnp
      pmp.get().portMapping.get().mode == PortMappingMode.NatPmp

  test "any, none and extip get no config":
    for s in ["any", "none", "extip:203.0.113.7"]:
      check natConfig(parseNatStrategy(s).get()).isNone()

suite "NAT config - natPortMapper":
  test "upnp and pmp name their protocol's mapper":
    let
      upnp = natPortMapper(parseNatStrategy("upnp").get())
      pmp = natPortMapper(parseNatStrategy("pmp").get())
    check:
      upnp.get() of UpnpMapper
      pmp.get() of NatPmpMapper

  test "any, none and extip have no mapper":
    for s in ["any", "none", "extip:203.0.113.7"]:
      check natPortMapper(parseNatStrategy(s).get()).isNone()

suite "NAT config - leased udp port mapping":
  asyncTest "maps the port with a finite lease through the mapper":
    let inner = stub("203.0.113.1")

    let res = await mapUdpPort(PortMapper(inner), Port(9000), Port(9000))
    check:
      res.get().externalIp == parseIpAddress("203.0.113.1")
      res.get().externalPort == Port(9000)
      inner.discoverCalls == 1
      inner.mapCalls == 1
      inner.lastMapProto == mpUdp
      # never 0: NAT-PMP has no permanent lease (0 deletes the mapping);
      # the caller renews inside this lease instead
      inner.lastMapLease == NatUdpLeaseSeconds

  asyncTest "renewal can request the previously granted external port":
    let inner = stub("203.0.113.1")

    let res = await mapUdpPort(PortMapper(inner), Port(9000), Port(51234))
    check:
      res.get().externalPort == Port(51234)

  asyncTest "reports discovery failure":
    let res = await mapUdpPort(PortMapper(stub()), Port(9000), Port(9000))
    check:
      res.isErr()

  asyncTest "reports mapping failure":
    let inner = stub("203.0.113.1")
    inner.mapRejections = 1

    let res = await mapUdpPort(PortMapper(inner), Port(9000), Port(9000))
    check:
      res.isErr()

suite "NAT config - udp lease keeper":
  asyncTest "an expired lease reverts the announcement and recovery restores it":
    ## Four failed renewals exhaust the whole lease: the three in-lease
    ## attempts of the halving schedule and the probe at the expiry
    ## instant, where the keeper must announce the revert exactly once.
    ## When a probe succeeds again, it must announce the port again.
    let inner = stub("203.0.113.1")
    inner.mapRejections = 4
    var announces: seq[Opt[Port]]

    let keeper = keepUdpMappingAlive(
      PortMapper(inner),
      Port(9000),
      Port(9000),
      projectedAtStart = true,
      shouldProject = proc(): bool {.gcsafe, raises: [].} =
        true,
      announce = proc(port: Opt[Port]) {.gcsafe, raises: [].} =
        announces.add(port),
      leaseDuration = 60.millis,
    )
    await sleepAsync(400.millis)
    await keeper.cancelAndWait()

    check:
      announces == @[Opt.none(Port), Opt.some(Port(9000))]
      inner.closeCalls == 1 # the keeper owns and closes the mapper

  asyncTest "a renewal granted on a different port re-announces":
    ## The router assigns a port of its own choosing on renewal; the
    ## announcement must follow it.
    let inner = stub("203.0.113.1")
    inner.grantedPort = Opt.some(Port(51234))
    var announces: seq[Opt[Port]]

    let keeper = keepUdpMappingAlive(
      PortMapper(inner),
      Port(9000),
      Port(9000),
      projectedAtStart = true,
      shouldProject = proc(): bool {.gcsafe, raises: [].} =
        true,
      announce = proc(port: Opt[Port]) {.gcsafe, raises: [].} =
        announces.add(port),
      leaseDuration = 60.millis,
    )
    await sleepAsync(180.millis)
    await keeper.cancelAndWait()

    check:
      announces.len >= 1
      announces[0] == Opt.some(Port(51234))

  asyncTest "nothing is announced while shouldProject stays false":
    let inner = stub("203.0.113.1")
    var announces: seq[Opt[Port]]

    let keeper = keepUdpMappingAlive(
      PortMapper(inner),
      Port(9000),
      Port(9000),
      projectedAtStart = false,
      shouldProject = proc(): bool {.gcsafe, raises: [].} =
        false,
      announce = proc(port: Opt[Port]) {.gcsafe, raises: [].} =
        announces.add(port),
      leaseDuration = 60.millis,
    )
    await sleepAsync(180.millis)
    await keeper.cancelAndWait()

    check announces.len == 0
