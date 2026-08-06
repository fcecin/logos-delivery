{.used.}

## Regression tests for the announced-addresses pipeline: how the
## configured and granted sources combine into what the node announces.

import
  results,
  std/[sequtils, strutils, net],
  testutils/unittests,
  chronos,
  libp2p/crypto/crypto,
  libp2p/multiaddress,
  libp2p/switch,
  libp2p/wire,
  libp2p/services/natservice
import
  logos_delivery/waku/[
    waku_core,
    waku_node,
    waku_enr,
    net/net_config,
    net/nat_config,
    factory/internal_config,
    factory/waku_conf,
    factory/builder,
  ],
  ./testlib/common,
  ./testlib/wakucore,
  ./testlib/wakunode

proc buildNode(
    bindPort: Port,
    quicBindPort: Port,
    dns4DomainName = Opt.none(string),
    extIp = Opt.none(IpAddress),
    extPort = Opt.none(Port),
    bindIp = parseIpAddress("127.0.0.1"),
): WakuNode =
  ## A node built straight from NetConfig, to express port combinations
  ## that newTestWakuNode normalizes away.
  let nodeKey = generateSecp256k1Key()
  let netConf = NetConfig.init(
    clusterId = DefaultClusterId,
    bindIp = bindIp,
    bindPort = bindPort,
    quicBindPort = Opt.some(quicBindPort),
    quicEnabled = true,
    dns4DomainName = dns4DomainName,
    extIp = extIp,
    extPort = extPort,
  ).valueOr:
    raiseAssert "invalid NetConfig: " & $error

  var enrBuilder = EnrBuilder.init(nodeKey)
  enrBuilder.withIpAddressAndPorts(
    ipAddr = netConf.enrIp, tcpPort = netConf.enrPort, udpPort = netConf.discv5UdpPort
  )
  let record = enrBuilder.build().valueOr:
    raiseAssert "invalid ENR: " & $error

  var builder = WakuNodeBuilder.init()
  builder.withRng(rng())
  builder.withNodeKey(nodeKey)
  builder.withRecord(record)
  builder.withNetworkConfiguration(netConf)
  builder.withSwitchConfiguration(maxConnections = Opt.some(50))
  builder.build().get()

suite "Announced addresses":
  asyncTest "NAT-mapped addresses keep updating announced addresses after start":
    ## The NATService renews mappings on every peerInfo.update. Its
    ## output must keep flowing into the announced addresses.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    # A NATService that has discovered an external IP.
    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("203.0.113.9"))
    node.switch.services.add(Service(natSvc))

    # A stand-in for the NATService's own mapper. It injects the mapped
    # external address into the chain.
    let mapped = MultiAddress.init("/ip4/203.0.113.9/tcp/4444").get()
    node.switch.peerInfo.addressMappers.insert(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        return addrs & @[mapped],
      0,
    )

    ## The hook the factory uses to refresh the ENR must fire when the
    ## announced addresses change.
    var changeSignalled = false
    node.onAnnouncedAddressesChange = proc() {.gcsafe, raises: [].} =
      changeSignalled = true

    await node.switch.peerInfo.update()
    check:
      mapped in node.announcedAddresses
      changeSignalled
    await node.stop()

  asyncTest "a loopback bind announces loopback":
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()
    check node.announcedAddresses.allIt("127.0.0.1" in $it)
    await node.stop()

  asyncTest "a wildcard bind announces the primary IP":
    let node = buildNode(
      bindPort = Port(0), quicBindPort = Port(0), bindIp = parseIpAddress("0.0.0.0")
    )
    await node.start()
    updateAnnouncedAddrWithPrimaryIpAddr(node).isOkOr:
      raiseAssert error
    check:
      node.announcedAddresses.len > 0
      node.announcedAddresses.allIt("0.0.0.0" notin $it and "127.0.0.1" notin $it)
    await node.stop()

  asyncTest "an extip entry with port 0 keeps its host":
    let node = buildNode(
      bindPort = Port(0),
      quicBindPort = Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.7")),
      extPort = Opt.some(Port(0)),
    )
    await node.start()
    let rendered = node.announcedAddresses.mapIt($it)
    check:
      rendered.anyIt("203.0.113.7" in it)
      not rendered.anyIt("/tcp/0" in it)
    await node.stop()

  asyncTest "a dns4 entry with port 0 gets the bound tcp port":
    ## The port component is read directly, so a dns-hosted placeholder
    ## is substituted too and the name is kept.
    let node = buildNode(
      bindPort = Port(0),
      quicBindPort = Port(0),
      dns4DomainName = Opt.some("node.example.org"),
    )
    await node.start()
    let rendered = node.announcedAddresses.mapIt($it)
    check:
      rendered.anyIt("/dns4/node.example.org/tcp/" in it)
      not rendered.anyIt("/tcp/0" in it)
    await node.stop()

  asyncTest "a dynamically allocated quic port is resolved at start":
    ## Substitution gives every placeholder its transport's bound port.
    let node = buildNode(bindPort = Port(61893), quicBindPort = Port(0))
    await node.start()
    let rendered = node.announcedAddresses.mapIt($it)
    check:
      rendered.anyIt("/udp/" in it and "/quic-v1" in it)
      not rendered.anyIt("/udp/0/" in it)
    await node.stop()

  asyncTest "a NAT strategy vouches for no external address on any transport":
    ## The configured derivation carries only what the operator vouches
    ## for. A NAT strategy grants endpoints at runtime, not in config.
    let conf = defaultTestWakuConf()
    conf.quicConf = Opt.some(QuicConf(port: Port(60820)))
    conf.webSocketConf = Opt.some(
      WebSocketConf(port: Port(60822), secureConf: Opt.none(WebSocketSecureConf))
    )
    conf.endpointConf.natStrategy = NatStrategy(kind: NatUpnp)
    conf.endpointConf.p2pTcpPort = Port(60821)

    let netConf = (
      await networkConfiguration(
        conf.clusterId, conf.endpointConf, conf.discv5Conf, conf.webSocketConf,
        conf.quicConf, conf.wakuFlags, conf.dnsAddrsNameServers,
      )
    ).valueOr:
      raiseAssert "networkConfiguration failed: " & error

    check:
      netConf.announcedAddresses.len > 0
      netConf.announcedAddresses.allIt(not it.isPublicMA())

  asyncTest "extip vouches for every enabled transport's external address":
    ## With extip the operator vouches for the endpoint. The external
    ## ports are the bind ports, for every enabled transport.
    let conf = defaultTestWakuConf()
    conf.quicConf = Opt.some(QuicConf(port: Port(60820)))
    conf.webSocketConf = Opt.some(
      WebSocketConf(port: Port(60822), secureConf: Opt.none(WebSocketSecureConf))
    )
    conf.endpointConf.natStrategy =
      NatStrategy(kind: NatExtIp, extIp: parseIpAddress("203.0.113.9"))
    conf.endpointConf.p2pTcpPort = Port(60821)

    let netConf = (
      await networkConfiguration(
        conf.clusterId, conf.endpointConf, conf.discv5Conf, conf.webSocketConf,
        conf.quicConf, conf.wakuFlags, conf.dnsAddrsNameServers,
      )
    ).valueOr:
      raiseAssert "networkConfiguration failed: " & error

    check:
      netConf.announcedAddresses.anyIt("203.0.113.9/tcp/60821" in $it)
      netConf.announcedAddresses.anyIt(
        "203.0.113.9" in $it and "udp/60820/quic-v1" in $it
      )
      netConf.announcedAddresses.anyIt("203.0.113.9/tcp/60822" in $it and "/ws" in $it)

suite "Announced addresses - multi-homed NAT":
  asyncTest "every mapped endpoint survives a resync from the recomputed config":
    ## NetConfig carries one external endpoint per transport, the ENR slot
    ## shape. The resync must keep the mapped set, not replace it.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("203.0.113.9"))
    node.switch.services.add(Service(natSvc))

    # Two interfaces mapped to two external tcp ports on the same gateway.
    let mappedA = MultiAddress.init("/ip4/203.0.113.9/tcp/4444").get()
    let mappedB = MultiAddress.init("/ip4/203.0.113.9/tcp/5555").get()
    node.switch.peerInfo.addressMappers.insert(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        return addrs & @[mappedA, mappedB],
      0,
    )
    await node.switch.peerInfo.update()

    check:
      mappedA in node.natMappedExternalAddresses()
      mappedB in node.natMappedExternalAddresses()
      mappedA in node.announcedAddresses
      mappedB in node.announcedAddresses

    ## The mapped endpoints derive from their own granted source. A
    ## narrow configured list loses no mapping.
    node.setConfigAnnouncedAddresses(@[mappedA])
    node.recomputeAnnouncedAddresses()

    check:
      mappedA in node.announcedAddresses
      mappedB in node.announcedAddresses # lost if the base replaced the set

    await node.stop()

  asyncTest "a lapsed NAT mapping stops being announced":
    ## The lease expires and the NATService passes the listen addresses
    ## through. The dead external endpoint must stop being announced.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("203.0.113.9"))
    node.switch.services.add(Service(natSvc))

    let operatorAddr = MultiAddress.init("/ip4/93.184.216.34/tcp/1234").get()
    let mapped = MultiAddress.init("/ip4/203.0.113.9/tcp/4444").get()

    ## The mapped endpoint lives only in its granted source. Nothing of
    ## it remains when the mapping lapses.
    node.setConfigAnnouncedAddresses(node.announcedAddresses & @[operatorAddr])

    let mappingAlive = new(bool)
    mappingAlive[] = true
    node.switch.peerInfo.addressMappers.insert(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        if mappingAlive[]:
          return addrs & @[mapped]
        return addrs,
      0,
    )

    await node.switch.peerInfo.update()
    check:
      mapped in node.announcedAddresses
      operatorAddr in node.announcedAddresses

    mappingAlive[] = false
    await node.switch.peerInfo.update()
    check:
      mapped notin node.announcedAddresses # stale endpoint must be dropped
      operatorAddr in node.announcedAddresses # operator intent survives

    await node.stop()

  asyncTest "an operator address at the mapped IP survives a lapsed mapping":
    ## A typed --ext-multiaddr at the NAT's external IP is configured,
    ## not granted. Only the granted endpoint dies with the mapping.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("93.184.216.34"))
    node.switch.services.add(Service(natSvc))

    let operatorAtMappedIp = MultiAddress.init("/ip4/93.184.216.34/tcp/1234").get()
    let mapped = MultiAddress.init("/ip4/93.184.216.34/tcp/4444").get()
    node.setConfigAnnouncedAddresses(node.announcedAddresses & @[operatorAtMappedIp])

    let mappingAlive = new(bool)
    mappingAlive[] = true
    node.switch.peerInfo.addressMappers.insert(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        if mappingAlive[]:
          return addrs & @[mapped]
        return addrs,
      0,
    )

    await node.switch.peerInfo.update()
    check:
      mapped in node.announcedAddresses
      operatorAtMappedIp in node.announcedAddresses

    mappingAlive[] = false
    await node.switch.peerInfo.update()
    check:
      mapped notin node.announcedAddresses # the granted endpoint lapses
      operatorAtMappedIp in node.announcedAddresses # the vouched one stays

    await node.stop()

  asyncTest "a NAT refresh does not revert the primary-IP announce rewrite":
    ## Regression: the primary-IP rewrite must land in the configured
    ## source, or the next NATService refresh reverts it.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    ## A NATService whose discovery found no gateway: attached, with no
    ## external IP and no mappings.
    let natSvc = NATService.new(upnpConfig(), rng())
    node.switch.services.add(Service(natSvc))

    node.addressSources.configAnnounced =
      @[MultiAddress.init("/ip4/0.0.0.0/tcp/1234").get()]
    node.announcedAddresses = node.addressSources.configAnnounced

    updateAnnouncedAddrWithPrimaryIpAddr(node).isOkOr:
      raiseAssert "primary ip rewrite failed: " & $error

    let afterRewrite = node.announcedAddresses
    check "0.0.0.0" notin $node.addressSources.configAnnounced

    ## The refresh tick.
    await node.switch.peerInfo.update()
    check node.announcedAddresses == afterRewrite

    await node.stop()

suite "Announced addresses - derived pipeline":
  asyncTest "relay reservation addresses survive a NAT recompute tick":
    ## Regression: reservations are an input to the recompute. A direct
    ## write is lost on the next refresh tick.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    let natSvc = NATService.new(upnpConfig(), rng())
    node.switch.services.add(Service(natSvc))

    let relayAddr = MultiAddress
      .init(
        "/ip4/93.184.216.34/tcp/4001/p2p/" &
          "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"
      )
      .get()

    ## What the onReservation hook does on a reservation.
    node.addressSources.relayReserved = @[relayAddr]
    node.recomputeAnnouncedAddresses()
    check node.announcedAddresses == @[relayAddr]

    ## The refresh tick must not revert it.
    await node.switch.peerInfo.update()
    check node.announcedAddresses == @[relayAddr]

    ## All reservations lost: the announced set falls back to the base.
    node.addressSources.relayReserved = @[]
    await node.switch.peerInfo.update()
    check relayAddr notin node.announcedAddresses

    await node.stop()

  asyncTest "extMultiAddrsOnly announces the configured set and nothing else":
    ## The configured source is announced exactly as given. Granted
    ## state is not merged in.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()
    node.extMultiAddrsOnly = true

    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("93.184.216.34"))
    node.switch.services.add(Service(natSvc))

    let operatorAddr = MultiAddress.init("/ip4/89.163.1.2/tcp/1000").get()
    let mapped = MultiAddress.init("/ip4/93.184.216.34/tcp/4444").get()
    node.setConfigAnnouncedAddresses(@[operatorAddr])

    node.switch.peerInfo.addressMappers.insert(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        return addrs & @[mapped],
      0,
    )

    await node.switch.peerInfo.update()
    check:
      node.announcedAddresses == @[operatorAddr]
      mapped notin node.announcedAddresses

    await node.stop()

suite "Announced addresses - ENR multiaddrs field":
  test "the multiaddrs field is the announced set minus the scalar endpoint":
    let scalar = MultiAddress.init("/ip4/8.8.8.8/tcp/60000").get()
    let secondHome = MultiAddress.init("/ip4/1.1.1.1/tcp/60000").get()
    let circuit = MultiAddress
      .init(
        "/ip4/93.184.216.34/tcp/4001/p2p/" &
          "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"
      )
      .get()
    let ws = MultiAddress.init("/ip4/8.8.8.8/tcp/60001/ws").get()

    let loopback = MultiAddress.init("/ip4/127.0.0.1/tcp/60000").get()

    check:
      enrRemainder(
        @[scalar, secondHome, circuit, ws, loopback],
        Opt.some(parseIpAddress("8.8.8.8")),
        Opt.some(Port(60000)),
      ) == @[secondHome, circuit, ws]

  test "without scalar fields every announced address is carried":
    let a = MultiAddress.init("/ip4/8.8.8.8/tcp/60000").get()
    let b = MultiAddress.init("/ip4/1.1.1.1/tcp/60000").get()
    check:
      enrRemainder(@[a, b], Opt.none(IpAddress), Opt.none(Port)) == @[a, b]

  test "private addresses drop; circuit and name addresses carry":
    let lan = MultiAddress.init("/ip4/192.168.1.10/tcp/60000").get()
    let dns = MultiAddress.init("/dns4/node.example.org/tcp/443/wss").get()
    let circuitPrivateRelay = MultiAddress
      .init(
        "/ip4/192.168.1.20/tcp/4001/p2p/" &
          "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"
      )
      .get()
    let publicAddr = MultiAddress.init("/ip4/9.9.9.9/tcp/60000").get()

    check:
      enrRemainder(
        @[lan, dns, circuitPrivateRelay, publicAddr],
        Opt.none(IpAddress),
        Opt.none(Port),
      ) == @[dns, circuitPrivateRelay, publicAddr]
