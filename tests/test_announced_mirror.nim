{.used.}

## The announced-address mirror: the committed peerInfo set is
## authoritative; the API projection and the ENR multiaddrs follow it.

import results
import std/[net, sequtils, strutils]
import testutils/unittests, chronos
import libp2p/[multiaddress, peerinfo, switch, wire]
import libp2p/crypto/crypto as libp2pcrypto
import libp2p/services/natservice
import libp2p/services/identify_pusher
import libp2p/services/hpservice
import libp2p/protocols/connectivity/relay/relay
import libp2p/peerstore
import libp2p/services/nat/portmapper
import eth/p2p/discoveryv5/protocol as discv5_protocol
import ../logos_delivery/waku/discovery/waku_discv5
import eth/keys, eth/p2p/discoveryv5/enr
import stew/byteutils
import
  ../logos_delivery/waku/waku_core,
  ../logos_delivery/waku/factory/waku_conf,
  ../logos_delivery/waku/net/net_config,
  ../logos_delivery/waku/node/waku_node,
  ../logos_delivery/waku/waku,
  ../logos_delivery/waku/waku_enr
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
    self: RecordingMapper,
    internalPort: Port,
    externalPort: Port,
    proto: MapProto,
    lease: uint32,
): Future[Result[Port, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  self.mappedInternal.add(internalPort)
  return ok(self.grantPort)

method unmap(
    self: RecordingMapper, externalPort: Port, proto: MapProto
): Future[Result[void, string]] {.async: (raises: [CancelledError]), gcsafe.} =
  return ok()

method close(self: RecordingMapper) {.async: (raises: []), gcsafe.} =
  discard

type EagerUpdate = ref object of Service
  ## Runs a peerInfo update during service startup, before the base
  ## resolves. The zero-port filter must hold in that window.

method setup(self: EagerUpdate, switch: Switch) {.raises: [ServiceSetupError].} =
  discard

method start(self: EagerUpdate, switch: Switch) {.async: (raises: [CancelledError]).} =
  await switch.peerInfo.update()

method stop(self: EagerUpdate, switch: Switch) {.async: (raises: [CancelledError]).} =
  discard

suite "Announced addresses mirror":
  asyncTest "the resolved base reaches peerInfo and the API projection":
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0))
    await node.start()
    check:
      node.baseIsReady()
      node.announcedAddresses.len > 0
      node.announcedAddresses == node.switch.peerInfo.addrs
      node.announcedAddresses.allIt("/tcp/0" notin $it and "/udp/0/" notin $it)
      node.announcedAddresses.allIt("0.0.0.0" notin $it)
    await node.stop()

  asyncTest "a loopback bind stays loopback in the base":
    ## Rewriting loopback to the LAN IP once crashed the test suite:
    ## it announces endpoints nothing listens on.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()
    check:
      node.announcedAddresses.len > 0
      node.announcedAddresses.allIt("/ip4/127.0.0.1/" in $it)
    await node.stop()

  asyncTest "an operator host containing the wildcard substring survives intact":
    ## The host string contains "0.0.0.0". Text replacement corrupted
    ## it; matching the parsed IP keeps it unchanged.
    let tricky = MultiAddress.init("/ip4/10.0.0.0/tcp/60123").get()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[tricky],
    )
    await node.start()
    check:
      node.baseIsReady()
      tricky in node.announcedAddresses
    await node.stop()

  asyncTest "a circuit route flows through the chain, and removal converges via the callback path":
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

    ## libp2p fires no update on removal, so the stale route stays
    ## until the next update.
    injecting = false
    check circuit in node.announcedAddresses ## stale until next update
    await node.switch.peerInfo.update() ## any later natural commit
    check:
      circuit notin node.switch.peerInfo.addrs
      circuit notin node.announcedAddresses
    await node.stop()

  asyncTest "NAT restart derives from intent, never from the previous output":
    var recorders: seq[RecordingMapper]
    var grantPort = Port(62001)
    let grantIp = parseIpAddress("203.0.113.77")
    let factory = proc(mode: PortMappingMode): Opt[PortMapper] {.gcsafe, raises: [].} =
      let rec = RecordingMapper(grantIp: grantIp, grantPort: grantPort)
      {.gcsafe.}:
        recorders.add(rec)
      Opt.some(PortMapper(rec))

    ## A private configured address for NATService to map, so the test
    ## does not depend on the machine's interfaces.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[MultiAddress.init("/ip4/192.168.77.7/tcp/60111").get()],
    )
    let natSvc = NATService.new(upnpConfig(), rng(), portMapperFactory = factory)
    ## The eager service updates first: the NAT mapper must never see
    ## a zero port.
    node.switch.services.add(Service(EagerUpdate()))
    node.switch.services.add(Service(natSvc))

    await node.start()
    let mappersAfterFirstStart = node.switch.peerInfo.addressMappers.len
    check node.announcedAddresses.anyIt("203.0.113.77" in $it and "62001" in $it)
    await node.stop()

    grantPort = Port(62002)
    await node.start()
    check:
      ## The base mapper does not accumulate across restarts.
      node.switch.peerInfo.addressMappers.len == mappersAfterFirstStart
      ## The new grant is announced: the mapper got the configured
      ## private address, not the previous run's output.
      node.announcedAddresses.anyIt("203.0.113.77" in $it and "62002" in $it)
      recorders.allIt(Port(0) notin it.mappedInternal)
    await node.stop()

  asyncTest "an unchanged owned update needs the explicit mirror copy":
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()
    let committed = node.switch.peerInfo.addrs

    node.announcedAddresses = @[]
    await node.switch.peerInfo.update()
    check node.announcedAddresses.len == 0 ## unchanged commit: observer silent

    node.mirrorCommittedAddrs()
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
      ## The factory ran, yet no mapper got a request: the chain never
      ## ran.
      recorders.len >= 1
      recorders.allIt(it.mappedInternal.len == 0)
    await node.stop()

  test "NetConfig.init rejects ext-only with empty or zero-port sets":
    check NetConfig
      .init(
        bindIp = parseIpAddress("127.0.0.1"),
        bindPort = Port(60200),
        extMultiAddrsOnly = true,
      )
      .isErr()
    check NetConfig
      .init(
        bindIp = parseIpAddress("127.0.0.1"),
        bindPort = Port(60200),
        extMultiAddrs = @[MultiAddress.init("/ip4/203.0.113.9/tcp/0").get()],
        extMultiAddrsOnly = true,
      )
      .isErr()
    check NetConfig
      .init(
        bindIp = parseIpAddress("127.0.0.1"),
        bindPort = Port(60200),
        extMultiAddrs = @[MultiAddress.init("/ip4/203.0.113.9/udp/0/quic-v1").get()],
        extMultiAddrsOnly = true,
      )
      .isErr()

  asyncTest "ext-multiaddr-only rejects empty lists and zero ports":
    let conf = defaultTestWakuConf()
    conf.endpointConf.extMultiAddrsOnly = true
    conf.endpointConf.extMultiAddrs = @[]
    check conf.validate().isErr()

    conf.endpointConf.extMultiAddrs =
      @[MultiAddress.init("/ip4/203.0.113.9/tcp/0").get()]
    check conf.validate().isErr()

    conf.endpointConf.extMultiAddrs =
      @[MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get()]
    check conf.validate().isOk()

  asyncTest "the ENR refresh trims to the largest fitting prefix and keeps shards":
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
      decoded[0].isCircuitRelayMA() ## relay routes sort first and survive
      node.enr.toTyped().expect("typed").relaySharding().isSome()

  asyncTest "an empty committed set clears the ENR multiaddrs field":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    node.announcedAddresses = @[MultiAddress.init("/ip4/203.0.113.9/tcp/60000").get()]
    check refreshEnrAddrs(node, key, nil).isOk()
    node.announcedAddresses = @[]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check typed.multiaddrs.expect("field").len == 0

  asyncTest "the live discv5 record takes the refresh and copies back":
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
      typed.relaySharding().isSome() ## shards survive the field update
      live.seqNum > seqBefore
      node.enr == live ## copy-back

  asyncTest "waku service setup keeps the builder services alive":
    ## Filtering switch.services once removed IdentifyPusher: mounted
    ## at build but never started, and address pushes went silent.
    let conf = defaultTestWakuConf()
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    node.setupSwitchServices(conf, Relay.new(), rng())
    check:
      node.switch.services.anyIt(it of IdentifyPusher)
      node.switch.services.anyIt(it of NATService) == false ## none configured here
    await node.start()
    check node.switch.peerInfo.protocols.anyIt("id/push" in it)
    await node.stop()

  asyncTest "a committed address change pushes to a connected peer":
    let nodeA =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    let nodeB =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    nodeA.setupSwitchServices(defaultTestWakuConf(), Relay.new(), rng())
    nodeB.setupSwitchServices(defaultTestWakuConf(), Relay.new(), rng())
    await allFutures(nodeA.start(), nodeB.start())
    await nodeB.connectToNodes(@[nodeA.switch.peerInfo.toRemotePeerInfo()])

    let circuit = MultiAddress.init(CircuitAddr).get()
    nodeA.switch.peerInfo.addressMappers.add(
      proc(
          addrs: seq[MultiAddress]
      ): Future[seq[MultiAddress]] {.gcsafe, async: (raises: [CancelledError]).} =
        return @[circuit] & addrs
    )
    await nodeA.switch.peerInfo.update()

    var pushed = false
    for _ in 0 ..< 200:
      await sleepAsync(20.milliseconds)
      let known = nodeB.switch.peerStore[AddressBook][nodeA.switch.peerInfo.peerId]
      if known.anyIt(it == circuit):
        pushed = true
        break
    check pushed
    await allFutures(nodeA.stop(), nodeB.stop())

  test "hasZeroPort reads the port bytes on every transport shape":
    check:
      MultiAddress.init("/ip4/1.2.3.4/tcp/0").get().hasZeroPort()
      MultiAddress.init("/dns4/x.example.org/tcp/0/wss").get().hasZeroPort()
      MultiAddress.init("/ip4/1.2.3.4/udp/0/quic-v1").get().hasZeroPort()
      not MultiAddress.init("/ip4/1.2.3.4/tcp/60000").get().hasZeroPort()
      not MultiAddress.init("/dns4/x.example.org/tcp/443/wss").get().hasZeroPort()

  test "ENR multiaddr encoding keeps the relay identity":
    let circuit = MultiAddress.init(CircuitAddr).get()
    let key = generateSecp256k1Key()
    var builder = EnrBuilder.init(key)
    builder.withMultiaddrs(@[circuit])
    let record = builder.build().expect("record")
    let typed = record.toTyped().expect("typed")
    let decoded = typed.multiaddrs.expect("multiaddrs field")
    check:
      decoded.len == 1
      "/p2p/" in $decoded[0]
      "/p2p-circuit" in $decoded[0]
