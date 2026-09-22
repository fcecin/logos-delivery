{.used.}

## libp2p commits the final announced addresses into peerInfo after
## the address mappers run. These tests check that node.announcedAddresses
## and the ENR multiaddrs field copy that committed set.

import results
import std/[net, sequtils, strutils]
import testutils/unittests, chronos
import libp2p/[multiaddress, switch, wire]
import libp2p/crypto/crypto as libp2pcrypto
import libp2p/services/natservice
import libp2p/services/nat/portmapper
import eth/p2p/discoveryv5/protocol as discv5_protocol
import ../logos_delivery/waku/discovery/waku_discv5
import eth/keys, eth/p2p/discoveryv5/enr
import stew/byteutils
import
  ../logos_delivery/waku/node/waku_node,
  ../logos_delivery/waku/waku,
  ../logos_delivery/waku/waku_core,
  ../logos_delivery/waku/waku_enr,
  ../logos_delivery/waku/net/net_config
import ./testlib/[common, wakucore, wakunode]

const CircuitAddr =
  "/ip4/93.184.216.34/tcp/4001/p2p/" &
  "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"

type RecordingMapper = ref object of PortMapper
  grantIp: IpAddress
  grantPort: Port
  mappedInternal: seq[Port]

method discover(
    self: RecordingMapper, timeout: Duration
): Future[Result[IpAddress, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  return ok(self.grantIp)

method map(
    self: RecordingMapper, internalPort: Port, externalPort: Port, proto: MapProto
): Future[Result[MappedPort, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  self.mappedInternal.add(internalPort)
  return ok(MappedPort(externalIp: self.grantIp, externalPort: self.grantPort))

method unmap(
    self: RecordingMapper, externalPort: Port, proto: MapProto
): Future[Result[void, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  return ok()

method close(self: RecordingMapper) {.async: (raises: []), gcsafe.} =
  discard

type EagerUpdate = ref object of Service
  ## Runs a peerInfo update while the switch starts, before the node
  ## resolves its announced addresses. The base mapper must drop port-0
  ## entries during that update, or the NATService maps port 0.

method setup(self: EagerUpdate, switch: Switch) {.raises: [ServiceSetupError].} =
  discard

method start(self: EagerUpdate, switch: Switch) {.async: (raises: [CancelledError]).} =
  await switch.peerInfo.update()

method stop(self: EagerUpdate, switch: Switch) {.async: (raises: [CancelledError]).} =
  discard

suite "Announced addresses":
  asyncTest "the resolved base reaches peerInfo and the API projection":
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0))
    await node.start()
    check:
      node.announcedAddresses.len > 0
      node.announcedAddresses == node.switch.peerInfo.addrs
      node.announcedAddresses.allIt("/tcp/0" notin $it and "/udp/0/" notin $it)
      node.announcedAddresses.allIt("0.0.0.0" notin $it)
    await node.stop()

  asyncTest "a loopback bind stays loopback in the base":
    ## Rewriting loopback to the LAN IP once crashed the test suite
    ## because it announces endpoints nothing listens on.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()
    check:
      node.announcedAddresses.len > 0
      node.announcedAddresses.allIt("/ip4/127.0.0.1/" in $it)
    await node.stop()

  asyncTest "an operator host containing the wildcard substring survives intact":
    ## The host string contains "0.0.0.0". Text replacement corrupted it.
    ## Matching the parsed IP keeps it unchanged.
    let tricky = MultiAddress.init("/ip4/10.0.0.0/tcp/60123").get()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[tricky],
    )
    await node.start()
    check:
      tricky in node.announcedAddresses
    await node.stop()

  asyncTest "a circuit route flows through the chain, and removal converges on the next update":
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()

    let circuit = MultiAddress.init(CircuitAddr).get()
    var injecting = true
    node.switch.peerInfo.addressMappers.add(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        if injecting:
          return @[circuit] & addrs
        return addrs
    )

    ## The service's own update carries the route.
    await node.switch.peerInfo.update()
    check:
      circuit in node.switch.peerInfo.addrs
      circuit in node.announcedAddresses
      node.announcedAddresses.anyIt(not it.isCircuitRelayMA())

    ## libp2p keeps peerInfo unchanged on removal.
    ## The stale route stays until the next update.
    injecting = false
    check circuit in node.announcedAddresses ## stale until next update
    await node.switch.peerInfo.update() ## any later natural commit
    check:
      circuit notin node.switch.peerInfo.addrs
      circuit notin node.announcedAddresses
    await node.stop()

  asyncTest "NAT restart derives from the configured intent":
    var recorders: seq[RecordingMapper]
    var grantPort = Port(62001)
    let grantIp = parseIpAddress("203.0.113.77")
    let factory = proc(mode: PortMappingMode): Opt[PortMapper] {.gcsafe, raises: [].} =
      let rec = RecordingMapper(grantIp: grantIp, grantPort: grantPort)
      {.gcsafe.}:
        recorders.add(rec)
      Opt.some(PortMapper(rec))

    ## A private configured address for NATService to map.
    ## The test runs the same on every machine.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[MultiAddress.init("/ip4/192.168.77.7/tcp/60111").get()],
    )
    let natSvc = NATService.new(upnpConfig(), rng(), portMapperFactory = factory)
    ## The eager service updates before start resolves the announced addresses.
    node.switch.services.add(Service(EagerUpdate()))
    node.switch.services.add(Service(natSvc))

    await node.start()
    let mappersAfterFirstStart = node.switch.peerInfo.addressMappers.len
    check node.announcedAddresses.anyIt("203.0.113.77" in $it and "62001" in $it)
    await node.stop()

    grantPort = Port(62002)
    await node.start()
    check:
      ## The mapper count stays flat across restarts.
      node.switch.peerInfo.addressMappers.len == mappersAfterFirstStart
      ## The new grant is announced. The mapper got the configured address.
      node.announcedAddresses.anyIt("203.0.113.77" in $it and "62002" in $it)
      recorders.allIt(Port(0) notin it.mappedInternal)
    await node.stop()

  asyncTest "the ENR carries a NAT grant, not the interface behind the wildcard bind":
    ## Bound to the wildcard host, the node announces its primary interface.
    ## The ENR omits it: nobody outside vouched for it. A private configured
    ## address gets a NAT grant, which replaces it in the committed set. That
    ## one the ENR carries. (The configured private address makes the test
    ## the same on every machine. QUIC is off so the bind stays wildcard.)
    var recorders: seq[RecordingMapper]
    let grantIp = parseIpAddress("203.0.113.77")
    let factory = proc(mode: PortMappingMode): Opt[PortMapper] {.gcsafe, raises: [].} =
      let rec = RecordingMapper(grantIp: grantIp, grantPort: Port(62004))
      {.gcsafe.}:
        recorders.add(rec)
      Opt.some(PortMapper(rec))

    let configured = MultiAddress.init("/ip4/192.168.77.7/tcp/60111").get()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extMultiAddrs = @[configured],
      quicEnabled = false,
    )
    await node.start()
    ## No mapping: the interface and the configured address are announced.
    ## the ENR carries the configured one only.
    var typed = node.enr.toTyped().expect("typed")
    check:
      node.announcedAddresses.len == 2
      typed.ip == Opt.some([192'u8, 168, 77, 7])
      typed.tcp == Opt.some(60111'u16)
      typed.multiaddrs.get(@[]) == @[configured]
    await node.stop()

    node.switch.services.add(Service(EagerUpdate()))
    node.switch.services.add(
      Service(NATService.new(upnpConfig(), rng(), portMapperFactory = factory))
    )
    await node.start()
    let grant = MultiAddress.init("/ip4/203.0.113.77/tcp/62004").get()
    typed = node.enr.toTyped().expect("typed")
    let field = typed.multiaddrs.get(@[])
    check:
      grant in node.announcedAddresses
      configured notin node.announcedAddresses
      typed.ip == Opt.some([203'u8, 0, 113, 77])
      typed.tcp == Opt.some(62004'u16)
      grant in field
      field.allIt(it.getIp().get(default(IpAddress)) == grantIp)
    await node.stop()

  asyncTest "an address the operator configured is carried when it equals the interface":
    ## Bound to the wildcard host, the operator also configures the primary
    ## interface as an announced address (port 0: the bound port). The
    ## interface entry the node resolves from the wildcard has the same value.
    ## The configured one wins: the ENR carries it.
    var primary = parseIpAddress("127.0.0.1")
    try:
      primary = getPrimaryIPAddr()
    except CatchableError:
      discard
    let configured = MultiAddress.init(primary, tcpProtocol, Port(0))
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extMultiAddrs = @[configured],
      quicEnabled = false,
    )
    await node.start()
    let bound = getPorts(node.switch.peerInfo.listenAddrs).expect("ports").tcpPort
    let expected = MultiAddress.init(primary, tcpProtocol, bound.get(Port(0)))
    let typed = node.enr.toTyped().expect("typed")
    check:
      bound.isSome() and bound.get() != Port(0)
      expected in node.announcedAddresses
      typed.ip == Opt.some(primary.address_v4)
      typed.tcp == Opt.some(bound.get(Port(0)).uint16)
      expected in typed.multiaddrs.get(@[])
    await node.stop()

    ## The control: the same node without the configured address carries
    ## no host.
    let control = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false
    )
    await control.start()
    let controlTyped = control.enr.toTyped().expect("typed")
    check:
      controlTyped.ip.isNone()
      controlTyped.multiaddrs.get(@[]).len == 0
    await control.stop()

  asyncTest "a NAT grant that goes away takes its endpoint out of the ENR":
    ## A mapper stands in for the NAT service: it replaces the base set with
    ## the grant, and later gives the base set back, as the service does when
    ## the mapping is lost. The withdrawn endpoint must not survive in the
    ## scalars. The bound port and no host is the state to return to.
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false
    )
    await node.start()
    let grant = MultiAddress.init("/ip4/203.0.113.77/tcp/62004").get()
    var granting = true
    node.switch.peerInfo.addressMappers.add(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        if granting:
          return @[grant]
        return addrs
    )
    await node.switch.peerInfo.update()
    var typed = node.enr.toTyped().expect("typed")
    check:
      node.announcedAddresses == @[grant]
      typed.ip == Opt.some([203'u8, 0, 113, 77])
      typed.tcp == Opt.some(62004'u16)
      typed.multiaddrs.get(@[]) == @[grant]

    granting = false
    await node.switch.peerInfo.update()
    let bound = getPorts(node.switch.peerInfo.listenAddrs).expect("ports").tcpPort
    typed = node.enr.toTyped().expect("typed")
    check:
      grant notin node.announcedAddresses
      typed.ip.isNone()
      typed.tcp == Opt.some(bound.get(Port(0)).uint16)
      typed.multiaddrs.get(@[]).len == 0
      node.enr.toRemotePeerInfo().isErr() ## nothing a peer can dial
    await node.stop()

  asyncTest "a wildcard override stays out of the ENR field after start":
    ## `ext-multiaddr-only` bypasses the resolution. A wildcard host in the
    ## override reaches the committed set as it is. The field does not take it.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[
        MultiAddress.init("/ip4/0.0.0.0/udp/61004/quic-v1").get(),
        MultiAddress.init("/ip4/203.0.113.44/tcp/60123").get(),
      ],
      extMultiAddrsOnly = true,
    )
    check node.enr.toTyped().expect("typed").multiaddrs.get(@[]).len == 0
    await node.start()
    let typed = node.enr.toTyped().expect("typed")
    check:
      node.announcedAddresses.len == 2
      typed.multiaddrs.get(@[]) ==
        @[MultiAddress.init("/ip4/203.0.113.44/tcp/60123").get()]
      typed.ip == Opt.some([203'u8, 0, 113, 44])
      typed.tcp == Opt.some(60123'u16)
    await node.stop()

  asyncTest "an unchanged owned update needs the explicit copy":
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()
    let committed = node.switch.peerInfo.addrs

    node.announcedAddresses = @[]
    await node.switch.peerInfo.update()
    check node.announcedAddresses.len == 0 ## unchanged commit, observer silent

    node.copyCommittedAddresses()
    check node.announcedAddresses == committed
    await node.stop()

  asyncTest "ext-multiaddr-only bypasses the chain from before start":
    let ext = MultiAddress.init("/ip4/203.0.113.44/tcp/60123").get()
    var recorders: seq[RecordingMapper]
    let grantIp = parseIpAddress("203.0.113.77")
    let factory = proc(mode: PortMappingMode): Opt[PortMapper] {.gcsafe, raises: [].} =
      let rec = RecordingMapper(grantIp: grantIp, grantPort: Port(62003))
      {.gcsafe.}:
        recorders.add(rec)
      Opt.some(PortMapper(rec))

    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[ext],
      extMultiAddrsOnly = true,
    )
    ## The override is active from construction, before any start.
    check node.switch.peerInfo.announcedAddrs == @[ext]

    node.switch.services.add(Service(EagerUpdate()))
    node.switch.services.add(
      Service(NATService.new(upnpConfig(), rng(), portMapperFactory = factory))
    )
    await node.start()
    check:
      node.announcedAddresses == @[ext]
      node.switch.peerInfo.addrs == @[ext]
      ## libp2p skipped the chain. The factory ran and every mapper stayed idle.
      recorders.len >= 1
      recorders.allIt(it.mappedInternal.len == 0)
    await node.stop()

  test "the ENR refresh trims to the largest fitting prefix and keeps shards":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    var builder = EnrBuilder.init(key)
    builder
      .withWakuRelaySharding(RelayShards(clusterId: 1, shardIds: @[0'u16, 1, 2, 3]))
      .expect("shards")
    node.enr = builder.build().expect("record")

    var addrs: seq[MultiAddress]
    for i in 0 ..< 6:
      addrs.add(MultiAddress.init(CircuitAddr).get())
      addrs.add(MultiAddress.init("/ip4/203.0.113." & $i & "/tcp/60000").get())
    node.announcedAddresses = addrs

    check refreshEnrAddrs(node, key, nil).isOk()

    let typed = node.enr.toTyped().expect("typed")
    let decoded = typed.multiaddrs.expect("multiaddrs field")
    check:
      decoded.len > 0
      decoded.len < addrs.len ## oversized input was trimmed
      decoded[0].isCircuitRelayMA() ## relay routes sort first and stay
      node.enr.toTyped().expect("typed").relaySharding().isSome()

  test "an empty committed set clears the ENR multiaddrs field":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    node.announcedAddresses = @[MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get()]
    check refreshEnrAddrs(node, key, nil).isOk()
    node.announcedAddresses = @[]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check typed.multiaddrs.expect("field").len == 0

  test "the live discv5 record takes the refresh and copies back":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))

    var builder = EnrBuilder.init(key)
    builder
      .withWakuRelaySharding(RelayShards(clusterId: 1, shardIds: @[0'u16, 5]))
      .expect("shards")
    let seedRecord = builder.build().expect("record")

    let keyBytes = key.getRawBytes().expect("raw")
    let ethPk = keys.PrivateKey.fromHex(byteutils.toHex(keyBytes)).expect("pk")
    let proto = discv5_protocol.newProtocol(
      ethPk,
      enrIp = Opt.none(IpAddress),
      enrTcpPort = Opt.none(Port),
      enrUdpPort = Opt.none(Port),
      previousRecord = Opt.some(seedRecord),
      bindPort = Port(9909),
      bindIp = Opt.none(IpAddress),
    )
    let wd = WakuDiscoveryV5(protocol: proto)
    let seqBefore = proto.localNode.record.seqNum

    node.announcedAddresses = @[
      MultiAddress.init(CircuitAddr).get(),
      MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get(),
    ]
    check refreshEnrAddrs(node, key, wd).isOk()

    let live = proto.localNode.record
    let typed = live.toTyped().expect("typed")
    check:
      typed.multiaddrs.expect("field").len == 2
      typed.ip == Opt.some([203'u8, 0, 113, 9]) ## the scalars follow the committed set
      typed.tcp == Opt.some(60000'u16)
      typed.relaySharding().isSome() ## shards stay after the field update
      live.seqNum > seqBefore
      node.enr == live ## copy-back

  proc socketlessDiscv5(
      key: libp2pcrypto.PrivateKey, seed: enr.Record
  ): WakuDiscoveryV5 =
    ## A discv5 protocol over `seed`, never started: no socket, no peers.
    let keyBytes = key.getRawBytes().expect("raw")
    let ethPk = keys.PrivateKey.fromHex(byteutils.toHex(keyBytes)).expect("pk")
    let proto = discv5_protocol.newProtocol(
      ethPk,
      enrIp = Opt.none(IpAddress),
      enrTcpPort = Opt.none(Port),
      enrUdpPort = Opt.none(Port),
      previousRecord = Opt.some(seed),
      bindPort = Port(9909),
      bindIp = Opt.none(IpAddress),
    )
    return WakuDiscoveryV5(protocol: proto)

  proc shardedRecord(key: libp2pcrypto.PrivateKey): enr.Record =
    var builder = EnrBuilder.init(key)
    builder.withWakuRelaySharding(RelayShards(clusterId: 1, shardIds: @[0'u16])).expect(
      "shards"
    )
    return builder.build().expect("record")

  test "a discv5 address learned from peers survives a later refresh":
    ## discv5 pairs `ip` with `udp` from what its peers report back. A later
    ## commit (a relay reservation, a NAT renewal) must not write the LAN
    ## host over that pair, and must not pair the LAN port with the learned
    ## host either: the LAN endpoint travels in the multiaddrs field.
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    let wd = socketlessDiscv5(key, shardedRecord(key))
    let proto = wd.protocol

    ## Before discv5 knows its address, the refresh writes the LAN host.
    node.announcedAddresses = @[MultiAddress.init("/ip4/192.168.7.5/tcp/60000").get()]
    check refreshEnrAddrs(node, key, wd).isOk()
    check:
      proto.localNode.record.toTyped().expect("typed").ip ==
        Opt.some([192'u8, 168, 7, 5])
      proto.localNode.address.isNone()

    ## discv5 learns the public pair from its peers.
    let learned = parseIpAddress("203.0.113.77")
    check proto.updateExternalIp(learned, Port(9000))
    check proto.localNode.address == Opt.some(Address(ip: learned, port: Port(9000)))

    ## A relay reservation lands: the committed set changes, the LAN host
    ## stays in it.
    node.announcedAddresses = @[
      MultiAddress.init(CircuitAddr).get(),
      MultiAddress.init("/ip4/192.168.7.5/tcp/60001").get(),
    ]
    check refreshEnrAddrs(node, key, wd).isOk()
    var typed = proto.localNode.record.toTyped().expect("typed")
    let dialable = proto.localNode.record.toRemotePeerInfo().expect("peer info").addrs
    check:
      typed.ip == Opt.some([203'u8, 0, 113, 77]) ## the learned host stays
      typed.udp == Opt.some(9000'u16)
      typed.tcp.isNone() ## no TCP endpoint is known on the learned host
      typed.multiaddrs.expect("field").len == 2
      typed.relaySharding().isSome()
      dialable.allIt(not ($it).startsWith("/ip4/203.0.113.77/tcp/"))
      MultiAddress.init("/ip4/192.168.7.5/tcp/60001").get() in dialable
      proto.localNode.address == Opt.some(Address(ip: learned, port: Port(9000)))
      node.enr == proto.localNode.record ## copy-back

    ## A TCP endpoint on the learned host (a NAT mapping): its port is
    ## written, also when another endpoint comes first in the set.
    node.announcedAddresses = @[
      MultiAddress.init("/ip4/198.51.100.7/tcp/61000").get(),
      MultiAddress.init("/ip4/203.0.113.77/tcp/60005").get(),
    ]
    check refreshEnrAddrs(node, key, wd).isOk()
    typed = proto.localNode.record.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([203'u8, 0, 113, 77])
      typed.tcp == Opt.some(60005'u16)
      typed.udp == Opt.some(9000'u16)
      typed.multiaddrs.expect("field").len == 2
      proto.localNode.address == Opt.some(Address(ip: learned, port: Port(9000)))
      node.enr == proto.localNode.record

  test "a discv5 update reaches the node's record through the reconcile step":
    ## discv5 writes the learned host into its own record and tells nobody:
    ## the node's copy (what MyENR shows) is behind, and the live record
    ## pairs the learned host with the `tcp` it had. The reconcile step,
    ## which `Waku` runs on a timer, writes the addresses again. Until it
    ## runs, both records stay as discv5 left them: that is the limit.
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    let wd = socketlessDiscv5(key, shardedRecord(key))
    let proto = wd.protocol

    node.announcedAddresses = @[MultiAddress.init("/ip4/192.168.7.5/tcp/60000").get()]
    check refreshEnrAddrs(node, key, wd).isOk()
    check reconcileEnrAddrs(node, key, wd).get(true) == false ## in step, nothing to do

    let learned = parseIpAddress("203.0.113.77")
    check proto.updateExternalIp(learned, Port(9000))

    ## The limit, as discv5 leaves it: the copy is behind, the live record
    ## names a TCP endpoint nobody serves.
    var live = proto.localNode.record.toTyped().expect("typed")
    check:
      node.enr != proto.localNode.record
      node.enr.toTyped().expect("typed").ip == Opt.some([192'u8, 168, 7, 5])
      live.ip == Opt.some([203'u8, 0, 113, 77])
      live.tcp == Opt.some(60000'u16)

    ## The step repairs both, once. A second run has nothing to do.
    check reconcileEnrAddrs(node, key, wd).get(false) == true
    live = proto.localNode.record.toTyped().expect("typed")
    let dialable = proto.localNode.record.toRemotePeerInfo().expect("peer info").addrs
    check:
      node.enr == proto.localNode.record
      live.ip == Opt.some([203'u8, 0, 113, 77])
      live.udp == Opt.some(9000'u16)
      live.tcp.isNone()
      live.relaySharding().isSome()
      dialable == @[MultiAddress.init("/ip4/192.168.7.5/tcp/60000").get()]
      proto.localNode.address == Opt.some(Address(ip: learned, port: Port(9000)))
    check reconcileEnrAddrs(node, key, wd).get(true) == false

  test "the live record built for an IPv6 host takes an IPv4 endpoint cleanly":
    ## The same builder record as a concrete IPv6 bind produces: `ip6` with
    ## its port as `tcp`. The refresh on the live record must not leave the
    ## IPv6 host paired with the new IPv4 port.
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    var builder = EnrBuilder.init(key)
    builder.withWakuRelaySharding(RelayShards(clusterId: 1, shardIds: @[0'u16])).expect(
      "shards"
    )
    builder.withIpAddressAndPorts(
      ipAddr = Opt.some(parseIpAddress("2001:db8::1")), tcpPort = Opt.some(Port(60000))
    )
    let wd = socketlessDiscv5(key, builder.build().expect("record"))
    require wd.protocol.localNode.record.toTyped().expect("typed").ip6.isSome()

    node.announcedAddresses = @[
      MultiAddress.init("/ip4/192.0.2.1/tcp/61000").get(),
      MultiAddress.init("/ip6/2001:db8::1/tcp/60000").get(),
    ]
    check refreshEnrAddrs(node, key, wd).isOk()
    let live = wd.protocol.localNode.record
    let typed = live.toTyped().expect("typed")
    let dialable = live.toRemotePeerInfo().expect("peer info").addrs
    check:
      typed.ip == Opt.some([192'u8, 0, 2, 1])
      typed.tcp == Opt.some(61000'u16)
      typed.ip6.isNone()
      typed.relaySharding().isSome()
      MultiAddress.init("/ip6/2001:db8::1/tcp/61000").get() notin dialable
      MultiAddress.init("/ip6/2001:db8::1/tcp/60000").get() in dialable
      node.enr == live
