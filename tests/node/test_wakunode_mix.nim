{.used.}

import std/[sequtils, strutils]
import testutils/unittests, chronos, results
import libp2p/[crypto/crypto, peerid, multiaddress]
import libp2p_mix, libp2p_mix/curve25519

import
  logos_delivery/waku/[
    waku_core,
    common/waku_protocol,
    net/net_config,
    node/enr_addresses,
    node/waku_node,
    node/health_monitor/health_status,
    node/health_monitor/protocol_health,
    node/health_monitor/node_health_monitor,
    waku_mix,
  ],
  ../testlib/[wakucore, wakunode, testasync]

## Mix's own hop closes every reply path this node asks for. On every address
## commit the node sets it to the first address mix can encode: a direct address
## known from outside, the ENR endpoint, a relay route, then the resolved set.

proc selfHop(node: WakuNode): MultiAddress =
  node.wakuMix.localMixPubInfo().multiAddr

proc boundTcpPort(node: WakuNode): Port =
  getPorts(node.switch.peerInfo.listenAddrs).expect("bound ports").tcpPort.get()

proc mountTestMix(node: WakuNode) {.async.} =
  let mixKeys = generateKeyPair().expect("mix key pair")
  (await node.mountMix(DefaultClusterId, mixKeys.privateKey, @[])).isOkOr:
    raiseAssert "Failed to mount mix: " & $error

proc mixHealth(node: WakuNode): ProtocolHealth =
  NodeHealthMonitor.new(node).getSyncProtocolHealthInfo(WakuProtocol.MixProtocol)

suite "Waku Mix - the node's own hop":
  asyncTest "the hop follows the bound port once the node starts":
    ## The mount takes port 0; `start()` binds the real port, and the hop must
    ## follow it.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.mountTestMix()
    check $node.selfHop() == "/ip4/127.0.0.1/tcp/0"

    await node.start()
    check:
      node.selfHop() == node.announcedAddresses[0]
      $node.selfHop() != "/ip4/127.0.0.1/tcp/0"
      node.wakuMix.selfHopUsable()
    await node.stop()

  asyncTest "the default deployment takes the primary interface, not the wildcard":
    ## A wildcard bind with nothing configured leaves only the resolved set, so
    ## the hop is its resolved host, which replaces the mount's `0.0.0.0`.
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false
    )
    await node.mountTestMix()
    check $node.selfHop() == "/ip4/0.0.0.0/tcp/0"

    await node.start()
    check:
      node.enrAddresses().len == 0
      node.selfHop() == node.announcedAddresses[0]
      "0.0.0.0" notin $node.selfHop()
      node.wakuMix.selfHopUsable()
    await node.stop()

  asyncTest "an address known from outside outranks the primary interface":
    ## Peers learn the operator-configured address, so it outranks the primary
    ## interface that a wildcard bind resolves to.
    let outside = MultiAddress.init("/ip4/203.0.113.9/tcp/60000").tryGet()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extMultiAddrs = @[outside],
      quicEnabled = false,
    )
    await node.mountTestMix()
    check $node.selfHop() == "/ip4/0.0.0.0/tcp/0"

    await node.start()
    check:
      node.enrAddresses() == @[outside]
      node.announcedAddresses.len == 2
      node.selfHop() == outside
    await node.stop()

  asyncTest "an explicit bind host stays first, as in the ENR":
    ## A concrete `--listen-address` is announced ahead of an `--ext-multiaddr`.
    ## The hop follows the order of the ENR scalars, so both name one endpoint.
    let outside = MultiAddress.init("/ip4/203.0.113.9/tcp/60000").tryGet()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[outside],
      quicEnabled = false,
    )
    await node.mountTestMix()
    await node.start()
    check:
      node.enrAddresses().len == 2
      node.selfHop() == node.announcedAddresses[0]
      $node.selfHop() == "/ip4/127.0.0.1/tcp/" & $node.boundTcpPort()
    await node.stop()

  asyncTest "a direct operator address outranks a relay route":
    ## The autorelay mapper announces a circuit route first. Only a relay client
    ## can dial it and the ENR scalars never carry it, so a direct address wins.
    let relayId = PeerId.init(generateSecp256k1Key()).tryGet()
    let circuit = MultiAddress
      .init("/ip4/203.0.113.1/tcp/60000/p2p/" & $relayId & "/p2p-circuit")
      .tryGet()
    let direct = MultiAddress.init("/ip4/203.0.113.5/tcp/60001").tryGet()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("127.0.0.1"),
      Port(0),
      extMultiAddrs = @[circuit, direct],
      extMultiAddrsOnly = true,
      quicEnabled = false,
    )
    await node.mountTestMix()
    check node.selfHop() == circuit # the mount takes the first announced address

    await node.start()
    check:
      node.announcedAddresses == @[circuit, direct]
      node.selfHop() == direct
    await node.stop()

  asyncTest "a node announcing a name takes the host the name resolved to":
    ## A fleet node announces a name, which mix cannot encode. The hop is the
    ## ENR endpoint: the external IP that the factory resolved the name to.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.9")),
      extPort = Opt.some(Port(30303)),
      dns4DomainName = Opt.some("node.test"),
      quicEnabled = false,
    )
    await node.mountTestMix()
    check:
      $node.selfHop() == "/dns4/node.test/tcp/30303"
      not node.wakuMix.selfHopUsable()

    await node.start()
    check:
      node.announcedAddresses.allIt("/dns4/" in $it)
      $node.selfHop() == "/ip4/203.0.113.9/tcp/30303"
      node.wakuMix.selfHopUsable()
    await node.stop()

  asyncTest "the host a name resolved to outranks a relay route":
    ## A fleet node with a relay route: the hop is the resolved host, as in the
    ## ENR, and the relay route comes after every direct endpoint.
    let relayId = PeerId.init(generateSecp256k1Key()).tryGet()
    let circuit = MultiAddress
      .init("/ip4/203.0.113.1/tcp/60000/p2p/" & $relayId & "/p2p-circuit")
      .tryGet()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.9")),
      extPort = Opt.some(Port(30303)),
      extMultiAddrs = @[circuit],
      dns4DomainName = Opt.some("node.test"),
      quicEnabled = false,
    )
    await node.mountTestMix()
    await node.start()
    check:
      node.announcedAddresses ==
        @[MultiAddress.init("/dns4/node.test/tcp/30303").tryGet(), circuit]
      node.enrAddresses().len == 2
      $node.selfHop() == "/ip4/203.0.113.9/tcp/30303"
    await node.stop()

  asyncTest "an operator address outranks the host a name resolved to":
    let outside = MultiAddress.init("/ip4/198.51.100.7/tcp/60001").tryGet()
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.9")),
      extPort = Opt.some(Port(30303)),
      extMultiAddrs = @[outside],
      dns4DomainName = Opt.some("node.test"),
      quicEnabled = false,
    )
    await node.mountTestMix()
    await node.start()
    check node.selfHop() == outside
    await node.stop()

  asyncTest "a name resolved again at start moves the hop with the ENR host":
    ## `Waku.start` resolves a dns4 name again and hands the result to the ENR
    ## scalars. The hop follows, so the record and the reply path name one host.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extIp = Opt.some(parseIpAddress("203.0.113.9")),
      extPort = Opt.some(Port(30303)),
      dns4DomainName = Opt.some("node.test"),
      quicEnabled = false,
    )
    await node.mountTestMix()
    await node.start()
    check $node.selfHop() == "/ip4/203.0.113.9/tcp/30303"

    let resolvedAgain = NetConfig
      .init(
        bindIp = parseIpAddress("0.0.0.0"),
        bindPort = Port(0),
        extIp = Opt.some(parseIpAddress("198.51.100.7")),
        extPort = Opt.some(Port(30303)),
        dns4DomainName = Opt.some("node.test"),
      )
      .expect("NetConfig")
    node.updateEnrConfiguredEndpoint(resolvedAgain)
    check $node.selfHop() == "/ip4/198.51.100.7/tcp/30303"
    await node.stop()

  asyncTest "a host discv5 confirmed outranks the primary interface":
    ## A host that discv5 confirmed is known from outside; a commit after start
    ## gives it to mix.
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("0.0.0.0"), Port(0), quicEnabled = false
    )
    await node.mountTestMix()
    await node.start()
    check "0.0.0.0" notin $node.selfHop()

    node.enrLearnedEndpoint =
      Opt.some(DiscoveryEndpoint((ip: parseIpAddress("203.0.113.9"), udp: Port(9000))))
    node.copyCommittedAddresses()
    check $node.selfHop() == "/ip4/203.0.113.9/tcp/" & $node.boundTcpPort()
    await node.stop()

  asyncTest "a node with no address mix can encode starts, and mix says so":
    ## A name and nothing else on a wildcard bind. The node starts, keeps the
    ## mount's hop, and mix health reports not ready with the reason.
    let node = newTestWakuNode(
      generateSecp256k1Key(),
      parseIpAddress("0.0.0.0"),
      Port(0),
      extPort = Opt.some(Port(30303)),
      dns4DomainName = Opt.some("node.test"),
      quicEnabled = false,
    )
    await node.mountTestMix()
    await node.start()

    let health = node.mixHealth()
    check:
      node.started
      $node.selfHop() == "/dns4/node.test/tcp/30303"
      node.wakuMix.selfHopMissing()
      not node.wakuMix.selfHopUsable()
      health.health == HealthStatus.NOT_READY
      "replies" in health.desc.get("")
    await node.stop()

  asyncTest "a derivation that finds nothing marks a leftover unusable even if it encodes":
    ## The mount's hop can encode and still be a placeholder. After a derivation
    ## that finds nothing it is unusable, until a derivation finds a hop.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.mountTestMix()
    let name = MultiAddress.init("/dns4/node.test/tcp/30303").tryGet()
    check node.wakuMix.selfHopUsable() # the mount-time hop encodes

    check:
      node.wakuMix.updateSelfHop(@[name], @[name]).isNone()
      $node.selfHop() == "/ip4/127.0.0.1/tcp/0" # left as it was
      node.wakuMix.selfHopMissing()
      not node.wakuMix.selfHopUsable()

    let direct = MultiAddress.init("/ip4/127.0.0.1/tcp/60000").tryGet()
    check:
      node.wakuMix.updateSelfHop(@[name], @[direct]) == Opt.some(direct)
      not node.wakuMix.selfHopMissing()
      node.wakuMix.selfHopUsable()
    await node.stop()

  asyncTest "a restart re-derives the hop":
    ## `stop()` clears the resolved base, and the commit in the next `start()`
    ## replaces a hop that went stale in between.
    let node =
      newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))
    await node.mountTestMix()
    await node.start()
    check node.selfHop() == node.announcedAddresses[0]
    await node.stop()

    let stale = MultiAddress.init("/ip4/127.0.0.1/tcp/1").tryGet()
    node.wakuMix.setLocalMultiAddr(stale).expect("an IPv4 TCP hop")
    await node.start()
    check:
      node.selfHop() != stale
      node.selfHop() == node.announcedAddresses[0]
      node.wakuMix.selfHopUsable()
    await node.stop()
