{.used.}

import std/[net, sequtils, strutils]
import testutils/unittests, chronos, results
import libp2p/[multiaddress, switch, wire]
import libp2p/services/natservice
import libp2p/services/wildcardresolverservice
import libp2p/protocols/connectivity/relay/relay
import libp2p/services/nat/[portmapper, upnp_mapper, natpmp_mapper]
import ../logos_delivery/waku/net/nat_config
import ../logos_delivery/waku/node/waku_switch
import ./testlib/[common, wakucore]

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
  ## Scripted port mapper. mapRejections fails that many leading map
  ## calls, and grantedPort overrides the granted external port.
  ip: Opt[IpAddress]
  mapRejections: int
  grantedPort: Opt[Port]
  discoverCalls: int
  mapCalls: int
  unmapCalls: int
  lastUnmapPort: Port
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
  # Lease 0 is invalid input, as in libp2p's NAT-PMP mapper. RFC 6886
  # uses a zero lifetime to delete a mapping.
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
  self.lastUnmapPort = externalPort
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

  asyncTest "any resolves to upnp when its discovery answers first":
    var probed: seq[NatStrategyKind]
    let answering = PortMapper(stub("203.0.113.1"))
    let silent = PortMapper(stub())
    let resolved = await resolveNatStrategy(
      NatStrategy(kind: NatAny),
      mapperFor = proc(s: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].} =
        probed.add(s.kind)
        if s.kind == NatUpnp:
          Opt.some(answering)
        else:
          Opt.some(silent),
    )
    check:
      resolved.kind == NatUpnp
      probed == @[NatUpnp]

  asyncTest "any falls back to pmp when upnp discovery fails":
    let answering = PortMapper(stub("203.0.113.1"))
    let silent = PortMapper(stub())
    let resolved = await resolveNatStrategy(
      NatStrategy(kind: NatAny),
      mapperFor = proc(s: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].} =
        if s.kind == NatPmp:
          Opt.some(answering)
        else:
          Opt.some(silent),
    )
    check resolved.kind == NatPmp

  asyncTest "any resolves to none when no gateway answers":
    let silent = PortMapper(stub())
    let resolved = await resolveNatStrategy(
      NatStrategy(kind: NatAny),
      mapperFor = proc(s: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].} =
        Opt.some(silent),
    )
    check resolved.kind == NatNone

  asyncTest "probe mappers close after the grace":
    let silent = stub()
    discard await resolveNatStrategy(
      NatStrategy(kind: NatAny),
      discoveryTimeout = 10.millis,
      mapperFor = proc(s: NatStrategy): Opt[PortMapper] {.gcsafe, raises: [].} =
        Opt.some(PortMapper(silent)),
    )
    check silent.closeCalls == 0
    await sleepAsync(1300.millis)
    check silent.closeCalls == 2

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
    ## Constructors spawn a worker thread. Leaving mappers unclosed
    ## crashed later suites when the GC reclaimed their contexts.
    waitFor upnp.get().close()
    waitFor pmp.get().close()

  test "any, none and extip have no mapper":
    for s in ["any", "none", "extip:203.0.113.7"]:
      check natPortMapper(parseNatStrategy(s).get()).isNone()

suite "NAT config - NATService pipeline":
  asyncTest "a wildcard bind maps and announces through the real NATService":
    ## The tripwire for the default deployment: a 0.0.0.0 bind, waku's
    ## switch composition, the real setupMappings, a scripted gateway.
    let privateIfaces = expandWildcardAddresses(
        @[MultiAddress.init("/ip4/0.0.0.0/tcp/0").get()]
      )
      .filterIt(it.isPrivateMA())
    if privateIfaces.len == 0:
      skip()
    else:
      let inner = stub("203.0.113.9")
      inner.grantedPort = Opt.some(Port(61000))
      let switch =
        newTestSwitch(address = Opt.some(MultiAddress.init("/ip4/0.0.0.0/tcp/0").get()))
      switch.services.keepItIf(it of NATService)
      switch.peerInfo.addressMappers.insert(wildcardExpansionMapper(), 0)
      let svc = NATService.new(
        natConfig(parseNatStrategy("pmp").get()).get(),
        rng(),
        portMapperFactory = proc(
            mode: PortMappingMode
        ): Opt[PortMapper] {.gcsafe, raises: [].} =
          Opt.some(PortMapper(inner)),
      )
      switch.services.add(Service(svc))
      svc.setup(switch)
      await switch.start()

      check:
        inner.discoverCalls >= 1
        inner.mapCalls >= 1
        switch.peerInfo.addrs.anyIt(($it).contains("203.0.113.9"))
        not switch.peerInfo.addrs.anyIt(($it).contains("0.0.0.0"))

      await switch.stop()

suite "NAT config - wildcard expansion":
  test "non-wildcard addresses pass unchanged":
    let a = MultiAddress.init("/ip4/127.0.0.1/tcp/1234").get()
    check expandWildcardAddresses(@[a]) == @[a]

  test "a wildcard expands per interface, keeping port and suffix":
    let expanded =
      expandWildcardAddresses(@[MultiAddress.init("/ip4/0.0.0.0/tcp/1234/ws").get()])
    check:
      expanded.len >= 1
      expanded.allIt(($it).endsWith("/tcp/1234/ws"))
      not expanded.anyIt("0.0.0.0" in $it)

  test "a v6 wildcard also expands to v4 interfaces":
    let expanded = expandWildcardAddresses(@[MultiAddress.init("/ip6/::/tcp/9").get()])
    check expanded.anyIt("/ip4/" in $it)

suite "NAT config - switch composition":
  test "the switch has no wildcard service and one build-time mapper":
    let switch = newWakuSwitch(rng = rng(), circuitRelay = Relay.new())
    check:
      not switch.services.anyIt(it of WildcardAddressResolverService)
      switch.peerInfo.addressMappers.len == 1
