{.used.}

## Regression tests for the announced-addresses pipeline: how the configured
## source (what the operator wrote down) and the granted sources (NAT
## mappings, relay reservations - given at runtime, on terms) combine into
## what the node announces.

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

proc buildNode(bindPort: Port, quicBindPort: Port): WakuNode =
  ## A node built straight from NetConfig, so port combinations that
  ## newTestWakuNode normalizes away (e.g. quic dynamic while tcp is
  ## explicit) can be expressed.
  let nodeKey = generateSecp256k1Key()
  let netConf = NetConfig.init(
    clusterId = DefaultClusterId,
    bindIp = parseIpAddress("127.0.0.1"),
    bindPort = bindPort,
    quicBindPort = Opt.some(quicBindPort),
    quicEnabled = true,
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
    ## The NATService renews mappings on every peerInfo.update (the refresh
    ## loop beats every 30 minutes); its output must keep flowing into the
    ## announced addresses instead of being frozen at start time.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    # A NATService that has discovered an external IP.
    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("203.0.113.9"))
    node.switch.services.add(Service(natSvc))

    # A mapper standing in for the NATService's own: injects the mapped
    # external address into the chain, as the real one does after a
    # successful port mapping.
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

  asyncTest "a dynamically allocated quic port postpones the primary-IP rewrite":
    ## The "is any port dynamically allocated" decision must see every
    ## transport: a config with an explicit tcp port but a dynamic quic port
    ## is a dynamic config, and the primary-IP announce rewrite is postponed
    ## for it, exactly as it is when the tcp port is dynamic.
    let node = buildNode(bindPort = Port(61893), quicBindPort = Port(0))
    await node.start()
    check node.announcedAddresses.allIt("127.0.0.1" in $it)
    await node.stop()

  asyncTest "a NAT strategy vouches for no external address on any transport":
    ## The configured derivation carries only what the operator vouches
    ## for. With a NAT strategy the external endpoint is granted at
    ## runtime, not configured, so the derivation produces no external
    ## address for any transport - mapped endpoints reach the announced
    ## addresses only through their own source.
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
    ## With `extip:` the operator vouches for the endpoint, and in absence
    ## of a manual port config the external ports are assumed to be the
    ## bind ports - for every enabled transport, websocket included.
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
    ## A multi-homed node gets one NAT mapping per listening interface. The
    ## recomputed NetConfig can only carry one external IP and one port per
    ## transport, because that is what the ENR's ip/tcp/udp slots hold. The
    ## resync must therefore keep the mapped set rather than replace it,
    ## otherwise every endpoint past the first is dropped from what the node
    ## announces and from the ENR's multiaddrs field.
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

    ## Whatever the configured source is replaced with, the announced set
    ## derives the mapped endpoints from their own granted source, so no
    ## mapping is lost to a narrow configured list.
    node.setConfigAnnouncedAddresses(@[mappedA])
    node.recomputeAnnouncedAddresses()

    check:
      mappedA in node.announcedAddresses
      mappedB in node.announcedAddresses # lost if the base replaced the set

    await node.stop()

  asyncTest "a lapsed NAT mapping stops being announced":
    ## Gateway reboots, lease expires: the NATService stops mapping and its
    ## stage passes the listen addresses through. The node must stop
    ## advertising the external endpoint that no longer forwards, while the
    ## operator's own public entry survives.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    let natSvc = NATService.new(upnpConfig(), rng())
    natSvc.externalIp = Opt.some(parseIpAddress("203.0.113.9"))
    node.switch.services.add(Service(natSvc))

    let operatorAddr = MultiAddress.init("/ip4/93.184.216.34/tcp/1234").get()
    let mapped = MultiAddress.init("/ip4/203.0.113.9/tcp/4444").get()

    ## The configured source holds only bound and operator addresses; the
    ## mapped endpoint lives in its own granted source, so nothing of it
    ## remains anywhere when the mapping lapses.
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
    ## An operator who types an --ext-multiaddr at the NAT's external IP
    ## vouches for it like for any other typed address: it is configured,
    ## not granted, so it does not follow the mapping lifecycle. Only the
    ## granted endpoint dies with the mapping.
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
    ## Regression: the fold recomputes the announced set from the base on
    ## every peerInfo.update, and the NATService runs one every 30 minutes.
    ## When the primary-IP rewrite reached only the announced set, that
    ## refresh recomputed from a base still holding the bind-IP form and
    ## reverted the node to it - on the default --nat any with no gateway,
    ## which is most cloud and container deployments.
    let node = buildNode(bindPort = Port(0), quicBindPort = Port(0))
    await node.start()

    ## A NATService whose discovery never found a gateway: attached, but with
    ## no external IP and no mappings.
    let natSvc = NATService.new(upnpConfig(), rng())
    node.switch.services.add(Service(natSvc))

    node.addressSources.configAnnounced =
      @[MultiAddress.init("/ip4/127.0.0.1/tcp/1234").get()]
    node.announcedAddresses = node.addressSources.configAnnounced

    updateAnnouncedAddrWithPrimaryIpAddr(node).isOkOr:
      raiseAssert "primary ip rewrite failed: " & $error

    let afterRewrite = node.announcedAddresses
    check "127.0.0.1" notin $node.addressSources.configAnnounced

    ## The refresh tick.
    await node.switch.peerInfo.update()
    check node.announcedAddresses == afterRewrite

    await node.stop()

suite "Announced addresses - derived pipeline":
  asyncTest "relay reservation addresses survive a NAT recompute tick":
    ## Regression: the circuit-relay onReservation hook wrote the announced
    ## addresses directly. The next recompute (the NATService refreshes
    ## every 30 minutes) derived them from the config source again and
    ## removed the relay addresses. Reservations are now an input to the
    ## recompute.
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
    ## The operator takes full responsibility: the configured source is
    ## announced exactly as given, and granted state is not merged in.
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
    let scalar = MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get()
    let secondHome = MultiAddress.init("/ip4/198.51.100.7/tcp/60000").get()
    let circuit = MultiAddress
      .init(
        "/ip4/93.184.216.34/tcp/4001/p2p/" &
          "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"
      )
      .get()
    let ws = MultiAddress.init("/ip4/203.0.113.9/tcp/60001/ws").get()

    let loopback = MultiAddress.init("/ip4/127.0.0.1/tcp/60000").get()

    check:
      enrRemainder(
        @[scalar, secondHome, circuit, ws, loopback],
        Opt.some(parseIpAddress("203.0.113.9")),
        Opt.some(Port(60000)),
      ) == @[secondHome, circuit, ws]

  test "without scalar fields every announced address is carried":
    let a = MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get()
    let b = MultiAddress.init("/ip4/198.51.100.7/tcp/60000").get()
    check:
      enrRemainder(@[a, b], Opt.none(IpAddress), Opt.none(Port)) == @[a, b]
