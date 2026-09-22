{.used.}

## The ENR must name endpoints a peer can use, at every moment a caller can
## read it. A node bound to the wildcard host with unpinned ports (the library
## defaults) used to advertise `0.0.0.0` and port 0 in the ENR scalars and the
## wildcard host in its QUIC multiaddr from `new` until the end of `start`,
## and `0.0.0.0` in `ip` forever. A library host reads `MyENR` whenever it
## likes, so the record before `start` is as visible as the one after.

import std/[net, nativesockets, sequtils, strutils]
import results, testutils/unittests, chronos, chronicles
import eth/p2p/discoveryv5/enr
import libp2p/[multiaddress, wire]
import eth/p2p/discoveryv5/protocol as discv5_protocol
import
  ../logos_delivery,
  ../logos_delivery/api/conf/[logos_delivery_conf, kernel_conf],
  ../logos_delivery/waku/waku,
  ../logos_delivery/waku/waku_core,
  ../logos_delivery/waku/waku_enr,
  ../logos_delivery/waku/factory/waku_state_info,
  ../logos_delivery/waku/net/net_config,
  ../logos_delivery/waku/net/nat_strategy,
  ../logos_delivery/waku/node/waku_node
import ./testlib/[common, wakucore, wakunode, testasync, wakunodeconf]

proc freePort(kind: SockType, protocol: nativesockets.Protocol): Port =
  ## A port the host hands out right now: pinned for the test, not fixed in
  ## the source, so two suites on one box do not collide.
  let socket = newSocket(AF_INET, kind, protocol)
  defer:
    socket.close()
  socket.bindAddr(Port(0), "127.0.0.1")
  return socket.getLocalAddr()[1]

proc unpinnedConf(): WakuNodeConf =
  ## Wildcard host, TCP, QUIC and discv5 on port 0: the library defaults.
  var conf = defaultTestWakuNodeConf()
  conf.quicSupport = true
  conf.quicPort = Port(0)
  return conf

proc pinnedConf(): WakuNodeConf =
  ## Wildcard host with the TCP, QUIC and discv5 ports pinned.
  var conf = unpinnedConf()
  conf.tcpPort = freePort(SOCK_STREAM, IPPROTO_TCP)
  conf.quicPort = freePort(SOCK_DGRAM, IPPROTO_UDP)
  conf.discv5UdpPort = freePort(SOCK_DGRAM, IPPROTO_UDP)
  return conf

proc toWaku(conf: WakuNodeConf): Future[Waku] {.async.} =
  let wakuConf = conf.toWakuConf().valueOr:
    raiseAssert error
  return (await Waku.new(wakuConf)).valueOr:
    raiseAssert error

proc enrMultiaddrs(typed: typed_record.TypedRecord): seq[MultiAddress] =
  typed.multiaddrs.get(@[])

proc allDialable(addrs: seq[MultiAddress]): bool =
  ## Parsed, not matched as text: `10.0.0.0` is a host, `0.0.0.0` is not.
  addrs.allIt(it.isDialableMA())

proc ma(s: string): MultiAddress =
  MultiAddress.init(s).expect(s)

proc dialAddrs(record: enr.Record): seq[MultiAddress] =
  ## What a peer would dial from the record: the field, plus the scalar pairs.
  record.toRemotePeerInfo().expect("peer info").addrs

suite "ENR endpoints before start":
  asyncTest "unpinned ports: the record omits the scalars and the wildcard multiaddr":
    ## What a library host reads with `MyENR` before `start` returns.
    let waku = await unpinnedConf().toWaku()
    let typed = waku.node.enr.toTyped().expect("typed record")
    check:
      typed.ip.isNone()
      typed.tcp.isNone()
      typed.udp.isNone()
      typed.enrMultiaddrs().allDialable()

  asyncTest "pinned ports: the record carries the ports, and no wildcard host":
    let conf = pinnedConf()
    let waku = await conf.toWaku()
    let typed = waku.node.enr.toTyped().expect("typed record")
    check:
      typed.ip.isNone()
      typed.tcp == Opt.some(conf.tcpPort.uint16)
      typed.udp == Opt.some(conf.discv5UdpPort.uint16)
      typed.enrMultiaddrs().allDialable()

  asyncTest "an external ip with unpinned ports: the host, and no port 0":
    var conf = unpinnedConf()
    conf.nat = "extip:203.0.113.9"
    let waku = await conf.toWaku()
    let typed = waku.node.enr.toTyped().expect("typed record")
    check:
      typed.ip == Opt.some([203'u8, 0, 113, 9])
      typed.tcp.isNone()
      typed.udp.isNone()
      typed.enrMultiaddrs().allDialable()

  asyncTest "the library path: MyENR before start has nothing a peer cannot use":
    let delivery = (
      await LogosDelivery.new(LogosDeliveryConf.init(KernelConf(unpinnedConf())))
    ).valueOr:
      raiseAssert error
    let uri = delivery.waku.stateInfo.getNodeInfoItem(NodeInfoId.MyENR)
    let typed = enr.Record.fromURI(uri).expect("enr from uri").toTyped().expect("typed")
    check:
      typed.ip.isNone()
      typed.tcp.isNone()
      typed.udp.isNone()
      typed.enrMultiaddrs().allDialable()

proc checkNoExternalAddress(conf: WakuNodeConf) {.async.} =
  ## Igor's expected row after start: the bound ports, no host, no multiaddr.
  ## The node announces its primary interface to the peers it connects to,
  ## but the ENR does not carry it.
  let waku = await conf.toWaku()
  (await waku.start()).isOkOr:
    raiseAssert error
  defer:
    discard await waku.stop()

  let typed = waku.node.enr.toTyped().expect("typed record")
  let expectedAnnounced = if conf.quicSupport: 2 else: 1 ## TCP, and QUIC
  check:
    waku.node.ports.tcp != 0
    waku.node.ports.discv5Udp != 0
    typed.ip.isNone()
    typed.tcp == Opt.some(waku.node.ports.tcp)
    typed.udp == Opt.some(waku.node.ports.discv5Udp)
    typed.enrMultiaddrs().len == 0
    waku.node.announcedAddresses.len == expectedAnnounced
    waku.node.announcedAddresses.allDialable()
  if conf.tcpPort != Port(0):
    check typed.tcp == Opt.some(conf.tcpPort.uint16)
  if conf.discv5UdpPort != Port(0):
    check typed.udp == Opt.some(conf.discv5UdpPort.uint16)

suite "ENR endpoints after start":
  asyncTest "no external address known: the bound ports, no host, no multiaddr":
    await checkNoExternalAddress(unpinnedConf())

  asyncTest "no external address known, QUIC off":
    var conf = unpinnedConf()
    conf.quicSupport = false
    await checkNoExternalAddress(conf)

  asyncTest "no external address known, pinned ports, QUIC off":
    var conf = pinnedConf()
    conf.quicSupport = false
    await checkNoExternalAddress(conf)

  asyncTest "pinned discv5 port: the record, the live record and discv5 agree after start":
    ## With a pinned discv5 port the seed record gives discv5 an address of
    ## its own before the rebuild at start. The rebuilt record and the live
    ## record must still agree, and a later known host must still land.
    let conf = pinnedConf()
    let waku = await conf.toWaku()
    (await waku.start()).isOkOr:
      raiseAssert error
    defer:
      discard await waku.stop()

    check not waku.wakuDiscv5.isNil()
    var typed = waku.node.enr.toTyped().expect("typed record")
    check:
      typed.ip.isNone()
      typed.tcp == Opt.some(conf.tcpPort.uint16)
      typed.udp == Opt.some(conf.discv5UdpPort.uint16)
      typed.enrMultiaddrs().len == 0
      waku.wakuDiscv5.protocol.localNode.record == waku.node.enr
      waku.wakuDiscv5.protocol.localNode.address.isNone() ## nothing learned yet

    ## A later commit with a host known from outside, as a NAT grant is: the
    ## scalars follow.
    waku.node.announcedAddresses = @[ma("/ip4/198.51.100.7/tcp/61000")]
    check refreshEnrAddrs(
      waku.node, waku.node.switch.peerInfo.privateKey, waku.wakuDiscv5
    )
      .isOk()
    typed = waku.node.enr.toTyped().expect("typed record")
    check:
      typed.ip == Opt.some([198'u8, 51, 100, 7])
      typed.tcp == Opt.some(61000'u16)
      typed.udp == Opt.some(conf.discv5UdpPort.uint16)
      typed.enrMultiaddrs() == @[ma("/ip4/198.51.100.7/tcp/61000")]
      waku.wakuDiscv5.protocol.localNode.record == waku.node.enr

  asyncTest "an external ip: the record carries it and the bound ports after start":
    var conf = unpinnedConf()
    conf.nat = "extip:203.0.113.9"
    let waku = await conf.toWaku()
    (await waku.start()).isOkOr:
      raiseAssert error
    defer:
      discard await waku.stop()

    let typed = waku.node.enr.toTyped().expect("typed record")
    let announced = waku.node.announcedAddresses
    check:
      waku.node.ports.tcp != 0
      typed.ip == Opt.some([203'u8, 0, 113, 9])
      typed.tcp == Opt.some(waku.node.ports.tcp)
      typed.udp == Opt.some(waku.node.ports.discv5Udp)
      announced.len == 2
      announced.allIt(
        it.getIp().get(default(IpAddress)) == parseIpAddress("203.0.113.9")
      )
      typed.enrMultiaddrs() == announced

  asyncTest "the library path: MyENR after start has the bound ports and no host":
    let delivery = (
      await LogosDelivery.new(LogosDeliveryConf.init(KernelConf(unpinnedConf())))
    ).valueOr:
      raiseAssert error
    (await delivery.start()).isOkOr:
      raiseAssert error
    defer:
      discard await delivery.stop()

    let uri = delivery.waku.stateInfo.getNodeInfoItem(NodeInfoId.MyENR)
    let typed = enr.Record.fromURI(uri).expect("enr from uri").toTyped().expect("typed")
    let node = delivery.waku.node
    check:
      node.ports.tcp != 0
      node.ports.discv5Udp != 0
      typed.ip.isNone()
      typed.tcp == Opt.some(node.ports.tcp)
      typed.udp == Opt.some(node.ports.discv5Udp)
      typed.enrMultiaddrs().len == 0
      node.announcedAddresses.len == 2
      node.announcedAddresses.allDialable()

  asyncTest "a bare node on the wildcard host advertises no endpoint":
    ## A `WakuNode` without a `Waku` around it. Its primary interface stays
    ## out of the record, as it does for a `Waku`. (The test node lib binds
    ## QUIC to loopback instead of the wildcard host, so QUIC is off here.)
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false
    )
    await node.start()
    defer:
      await node.stop()
    let typed = node.enr.toTyped().expect("typed")
    let bound = getPorts(node.switch.peerInfo.listenAddrs).expect("ports").tcpPort
    check:
      bound.isSome() and bound.get() != Port(0)
      typed.ip.isNone()
      typed.tcp == Opt.some(bound.get(Port(0)).uint16) ## the bound port, no host
      typed.enrMultiaddrs().len == 0
      node.announcedAddresses.len > 0
      node.announcedAddresses.allDialable()

  asyncTest "a configured external port stays beside its host after start":
    ## The external host and port are configured and the name is announced,
    ## so no announced IPv4 endpoint decides the scalars. The local TCP port
    ## differs: the record keeps the configured port beside the configured
    ## host, never the bound port.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.9")),
      extPort = Opt.some(Port(60000)),
      dns4DomainName = Opt.some("example.com"),
    )
    let before = node.enr.toTyped().expect("typed")
    check:
      before.ip == Opt.some([203'u8, 0, 113, 9])
      before.tcp == Opt.some(60000'u16)
    await node.start()
    defer:
      await node.stop()
    let typed = node.enr.toTyped().expect("typed")
    let bound = getPorts(node.switch.peerInfo.listenAddrs).expect("ports").tcpPort
    check:
      bound.isSome() and bound.get() != Port(0) and bound.get() != Port(60000)
      typed.ip == Opt.some([203'u8, 0, 113, 9])
      typed.tcp == Opt.some(60000'u16)
      typed.enrMultiaddrs().anyIt($it == "/dns4/example.com/tcp/60000")

  asyncTest "the host of the running configuration replaces the one from construction":
    ## `Waku.start` resolves the network configuration again (a name can
    ## answer differently by then). With the name announced, no carried
    ## IPv4 endpoint decides the host: the record after start follows the
    ## running configuration, not the host remembered from construction.
    var conf = unpinnedConf()
    conf.dns4DomainName = "example.com"
    conf.nat = "extip:203.0.113.9"
    let waku = await conf.toWaku()
    check waku.node.enr.toTyped().expect("typed").ip == Opt.some([203'u8, 0, 113, 9])
    waku.conf.endpointConf.natStrategy =
      parseNatStrategy("extip:203.0.113.10").expect("nat strategy")
    (await waku.start()).isOkOr:
      raiseAssert error
    defer:
      discard await waku.stop()
    let typed = waku.node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([203'u8, 0, 113, 10])
      typed.tcp == Opt.some(waku.node.ports.tcp)
      typed.enrMultiaddrs().anyIt(($it).startsWith("/dns4/example.com/"))

  asyncTest "a bare node on a concrete host advertises it":
    ## The operator chose the host, as the test suites do with loopback.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.start()
    defer:
      await node.stop()
    let typed = node.enr.toTyped().expect("typed")
    let announced = node.announcedAddresses.filterIt(it.isP2pTcpAddress())
    check announced.len == 1
    let tcpPort =
      if announced.len == 1:
        initTAddress(announced[0]).get(default(TransportAddress)).port
      else:
        Port(0)
    check:
      tcpPort != Port(0)
      typed.ip == Opt.some([127'u8, 0, 0, 1])
      typed.tcp == Opt.some(tcpPort.uint16)
      typed.enrMultiaddrs() == node.announcedAddresses

suite "ENR endpoints follow discv5":
  asyncTest "a host discv5 learned reaches MyENR through the loop, which stop ends":
    ## discv5 writes the learned host into its own record and tells nobody.
    ## `Waku` polls the live record. The test shortens the interval and
    ## watches `MyENR` change with no address commit and no repair call.
    var conf = unpinnedConf()
    let waku = await conf.toWaku()
    waku.enrReconcileInterval = 100.milliseconds
    (await waku.start()).isOkOr:
      raiseAssert error
    check not waku.wakuDiscv5.isNil()
    check not waku.enrReconcileLoopHandle.isNil()

    proc myEnrIp(): Opt[array[4, byte]] =
      let uri = waku.stateInfo.getNodeInfoItem(NodeInfoId.MyENR)
      enr.Record.fromURI(uri).expect("enr from uri").toTyped().expect("typed").ip

    check myEnrIp().isNone()
    let learned = parseIpAddress("203.0.113.77")
    check waku.wakuDiscv5.protocol.updateExternalIp(learned, Port(9000))

    ## Bounded wait: many ticks of the shortened interval.
    var seen = false
    for _ in 0 ..< 60:
      if myEnrIp() == Opt.some([203'u8, 0, 113, 77]):
        seen = true
        break
      await sleepAsync(50.milliseconds)
    let typed = waku.node.enr.toTyped().expect("typed")
    check:
      seen
      typed.udp == Opt.some(9000'u16)
      typed.tcp.isNone() ## no endpoint the ENR carries is on the learned host
      waku.node.enr == waku.wakuDiscv5.protocol.localNode.record

    (await waku.stop()).isOkOr:
      raiseAssert error
    check:
      not waku.enrReconcileLoopHandle.isNil()
      waku.enrReconcileLoopHandle.isNil() or waku.enrReconcileLoopHandle.finished()

suite "ENR endpoint selection":
  const Relay =
    "/ip4/93.184.216.34/tcp/4001/p2p/" &
    "16Uiu2HAm7YEh2wwbYNvayrSQe2bdm1aL4FnhCLkvSNaScMxcgt4n/p2p-circuit"

  type Written =
    tuple[ip: Opt[array[4, byte]], tcp: Opt[uint16], field: seq[MultiAddress]]

  proc writtenAfter(addrs: seq[MultiAddress]): Written =
    ## The scalars and the field of a record without addresses after one
    ## refresh with `addrs` as the announced set, on a node with no
    ## configured host (a wildcard bind. QUIC off keeps it a wildcard).
    let key = generateSecp256k1Key()
    let node =
      newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false)
    var builder = EnrBuilder.init(key)
    builder.withWakuRelaySharding(RelayShards(clusterId: 1, shardIds: @[0'u16])).expect(
      "shards"
    )
    node.enr = builder.build().expect("record")
    node.announcedAddresses = addrs
    refreshEnrAddrs(node, key, nil).expect("refresh")
    let typed = node.enr.toTyped().expect("typed")
    return (ip: typed.ip, tcp: typed.tcp, field: typed.enrMultiaddrs())

  test "the first IPv4 TCP endpoint a peer can dial, after every other kind":
    ## `ip` and `tcp` are the IPv4 pair of the record. Each entry before the
    ## candidate is skipped for a different reason. The field keeps every
    ## entry a peer can use, and no placeholder.
    let keptInField = @[
      ma(Relay), ## a relay route
      ma("/dns4/node.example.com/tcp/60000"), ## no IP literal
      ma("/ip4/203.0.113.1/tcp/8000/ws"), ## WebSocket
      ma("/ip4/203.0.113.2/udp/60000/quic-v1"), ## not TCP
      ma("/ip6/2001:db8::1/tcp/60000"), ## IPv6 travels in the multiaddrs field
    ]
    let placeholders = @[
      ma("/ip4/0.0.0.0/tcp/60000"), ## the wildcard host
      ma("/ip4/203.0.113.3/tcp/0"), ## an unresolved port
      ma("/ip4/0.0.0.0/udp/61004/quic-v1"), ## a wildcard QUIC override
    ]
    let candidate = ma("/ip4/203.0.113.9/tcp/60000")
    let written = writtenAfter(keptInField & placeholders & candidate)
    check:
      written.ip == Opt.some([203'u8, 0, 113, 9])
      written.tcp == Opt.some(60000'u16)
      written.field == keptInField & candidate
    for entry in keptInField:
      check writtenAfter(@[entry]) ==
        (ip: Opt.none(array[4, byte]), tcp: Opt.none(uint16), field: @[entry])
    for entry in placeholders:
      check writtenAfter(@[entry]) == (
        ip: Opt.none(array[4, byte]),
        tcp: Opt.none(uint16),
        field: newSeq[MultiAddress](),
      )

suite "ENR endpoints follow the announced addresses":
  test "a refresh writes the first dialable TCP endpoint into ip and tcp":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0))
    node.announcedAddresses =
      @[ma("/ip4/203.0.113.9/tcp/60000"), ma("/ip4/203.0.113.9/udp/60000/quic-v1")]
    check refreshEnrAddrs(node, key, nil).isOk()
    var typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([203'u8, 0, 113, 9])
      typed.tcp == Opt.some(60000'u16)

    ## The set changes, as it does when a NAT mapping arrives: the scalars follow.
    node.announcedAddresses = @[ma("/ip4/198.51.100.7/tcp/61000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([198'u8, 51, 100, 7])
      typed.tcp == Opt.some(61000'u16)

  test "a refresh with no dialable TCP endpoint falls back to the configured host":
    ## Nothing is kept from the previous record: an endpoint that went away
    ## goes away with it. The configured host (here a concrete bind host)
    ## stays, because it is a fact of the configuration, not of the record.
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("127.0.0.1"), Port(0))
    node.announcedAddresses = @[ma("/ip4/203.0.113.9/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    check node.enr.toTyped().expect("typed").ip == Opt.some([203'u8, 0, 113, 9])
    node.announcedAddresses = @[ma("/dns4/relay.example.com/tcp/443/wss")]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([127'u8, 0, 0, 1])
      typed.tcp.isNone() ## the node is not started: no bound port
      typed.enrMultiaddrs() == node.announcedAddresses

  test "a refresh with no dialable TCP endpoint and a wildcard host leaves no host":
    let key = generateSecp256k1Key()
    let node =
      newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false)
    node.announcedAddresses = @[ma("/ip4/203.0.113.9/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    node.announcedAddresses = @[ma("/dns4/relay.example.com/tcp/443/wss")]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip.isNone()
      typed.tcp.isNone()

  test "a refresh keeps the shards and the other fields":
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0))
    let seqBefore = node.enr.seqNum
    let shardsBefore = node.enr.toTyped().expect("typed").relaySharding()
    check shardsBefore.isSome()
    node.announcedAddresses = @[ma("/ip4/203.0.113.9/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check:
      typed.relaySharding() == shardsBefore
      node.enr.seqNum > seqBefore
      node.enr.toRemotePeerInfo().expect("peer info").peerId == node.peerId()

  test "an IPv6 endpoint ahead of the IPv4 one does not pair its port with the IPv4 host":
    ## nim-eth writes an IPv6 host as `ip6` but its port as `tcp`, next to a
    ## retained `ip`: the record would name an IPv4 endpoint nobody serves.
    let key = generateSecp256k1Key()
    let node =
      newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false)
    node.announcedAddresses = @[ma("/ip4/192.0.2.1/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    node.announcedAddresses =
      @[ma("/ip6/2001:db8::1/tcp/61000"), ma("/ip4/192.0.2.1/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    var typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([192'u8, 0, 2, 1])
      typed.tcp == Opt.some(60000'u16)
      typed.ip6.isNone()
      typed.enrMultiaddrs() == node.announcedAddresses

    ## An IPv6-only set on a wildcard bind: no host in the scalars at all,
    ## the field carries the IPv6 endpoint. Nothing is kept from before.
    node.announcedAddresses = @[ma("/ip6/2001:db8::1/tcp/61000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip.isNone()
      typed.tcp.isNone()
      typed.ip6.isNone()
      typed.enrMultiaddrs() == node.announcedAddresses

  test "a record built for an IPv6 host does not keep that host next to an IPv4 port":
    ## The builder writes a concrete IPv6 bind host as `ip6` with its port as
    ## `tcp`. A refresh that writes an IPv4 endpoint at another port must
    ## not leave the IPv6 host behind, paired with the new port.
    let key = generateSecp256k1Key()
    let node = newTestWakuNode(key, parseIpAddress("0.0.0.0"), Port(0))
    var builder = EnrBuilder.init(key)
    builder.withIpAddressAndPorts(
      ipAddr = Opt.some(parseIpAddress("2001:db8::1")), tcpPort = Opt.some(Port(60000))
    )
    node.enr = builder.build().expect("record")
    check node.enr.toTyped().expect("typed").ip6.isSome()

    node.announcedAddresses =
      @[ma("/ip4/192.0.2.1/tcp/61000"), ma("/ip6/2001:db8::1/tcp/60000")]
    check refreshEnrAddrs(node, key, nil).isOk()
    let typed = node.enr.toTyped().expect("typed")
    check:
      typed.ip == Opt.some([192'u8, 0, 2, 1])
      typed.tcp == Opt.some(61000'u16)
      typed.ip6.isNone()
      ma("/ip6/2001:db8::1/tcp/61000") notin node.enr.dialAddrs()
      ma("/ip6/2001:db8::1/tcp/60000") in node.enr.dialAddrs()
