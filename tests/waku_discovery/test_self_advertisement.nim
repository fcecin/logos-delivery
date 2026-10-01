{.used.}

import std/[base64, net, sequtils, strutils]
import chronos, results, testutils/unittests
import brokers/broker_implement
import libp2p/protocols/connectivity/autonat/[types, service]
import libp2p/services/reachabilityobservers
import libp2p_mix/[curve25519, mix_protocol]
import
  logos_delivery/waku/discovery/self_advertisement,
  logos_delivery/waku/discovery/peer_discovery_interface,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/waku_enr/capabilities,
  logos_delivery/waku/[waku_core, waku_mix, waku_node]
import logos_delivery/waku/waku_core/codecs
import tools/confutils/cli_args
import logos_delivery/waku/waku
import ../testlib/[wakucore, wakunode, wakunodeconf]

## A backend that records what it was asked to do, so the fan-out rules can be
## checked without standing up a DHT.
type FakeBackend = ref object of IPeerDiscovery
  id: string
  kinds: seq[string]
  advertised: seq[string]
  advertisedData: seq[seq[byte]]
  interests: seq[string]
  stopped: seq[string]

BrokerImplement FakeBackend of IPeerDiscovery:
  proc new(T: typedesc[FakeBackend], id: string, kinds: seq[string]): FakeBackend =
    FakeBackend(id: id, kinds: kinds)

  method backendInfo(
      self: FakeBackend
  ): Future[Result[DiscoveryBackendInfo, string]] {.async.} =
    ok(DiscoveryBackendInfo(id: self.id, running: true, keyKinds: self.kinds))

  method startAdvertising(
      self: FakeBackend, key: string, data: seq[byte]
  ): Future[Result[void, string]] {.async.} =
    self.advertised.add(key)
    self.advertisedData.add(data)
    ok()

  method registerInterest(
      self: FakeBackend, key: string
  ): Future[Result[void, string]] {.async.} =
    self.interests.add(key)
    ok()

  ## Not exercised here; the interface requires every verb.
  method startDiscovery(self: FakeBackend): Future[Result[void, string]] {.async.} =
    ok()

  method stopDiscovery(self: FakeBackend): Future[Result[void, string]] {.async.} =
    ok()

  method lookupServicePeers(
      self: FakeBackend, key: string, limit: int
  ): Future[Result[seq[DiscoveredPeer], string]] {.async.} =
    ok(newSeq[DiscoveredPeer]())

  method lookupRandom(
      self: FakeBackend
  ): Future[Result[seq[DiscoveredPeer], string]] {.async.} =
    ok(newSeq[DiscoveredPeer]())

  method stopAdvertising(
      self: FakeBackend, key: string
  ): Future[Result[void, string]] {.async.} =
    self.stopped.add(key)
    ok()

  method unregisterInterest(
      self: FakeBackend, key: string
  ): Future[Result[void, string]] {.async.} =
    ok()

  method addBootstrapEntries(
      self: FakeBackend, entries: seq[string]
  ): Future[Result[void, string]] {.async.} =
    ok()

proc confWith(flags: CapabilitiesBitfield): WakuConf =
  var conf = defaultWakuNodeConf().valueOr:
    raiseAssert error
  conf.clusterId = Opt.some(16'u16)
  var wakuConf = conf.toWakuConf().valueOr:
    raiseAssert error
  wakuConf.wakuFlags = flags
  wakuConf.subscribeShards = @[0'u16, 3'u16]
  wakuConf

const CoreFlags =
  CapabilitiesBitfield.init(relay = true, store = true, lightpush = true)
const EdgeFlags = CapabilitiesBitfield.init()
const TestShards = @[0'u16, 3'u16]

suite "Self advertisement":
  asyncTest "a serving node advertises and registers interest":
    let kad = FakeBackend.create("service", @["service", "topic", "cap"])
    await advertiseSelf(@[IPeerDiscovery(kad)], confWith(CoreFlags), TestShards)
    check:
      kad.advertised == @["service:" & LogosDeliveryServiceId]
      kad.interests == @["service:" & LogosDeliveryServiceId]

  asyncTest "an edge node registers interest but advertises nothing":
    ## Nothing to be found for, but it still needs to find others.
    let kad = FakeBackend.create("service", @["service", "topic", "cap"])
    await advertiseSelf(@[IPeerDiscovery(kad)], confWith(EdgeFlags), TestShards)
    check:
      kad.advertised.len == 0
      kad.interests == @["service:" & LogosDeliveryServiceId]

  asyncTest "relay alone is enough to be worth finding":
    let kad = FakeBackend.create("service", @["service", "topic", "cap"])
    await advertiseSelf(
      @[IPeerDiscovery(kad)],
      confWith(CapabilitiesBitfield.init(relay = true)),
      TestShards,
    )
    check kad.advertised.len == 1

  asyncTest "both kademlia hosts take part, discv5 does not":
    ## Selected by what a backend declares it understands, not by its name --
    ## discv5 rejects svc: keys, so asking it would only produce warnings.
    let internal = FakeBackend.create("service", @["service", "topic", "cap"])
    let plugin = FakeBackend.create("service-ext", @["service", "topic", "cap"])
    let discv5 = FakeBackend.create("discv5", @["topic", "cap", ""])
    await advertiseSelf(
      @[IPeerDiscovery(internal), IPeerDiscovery(plugin), IPeerDiscovery(discv5)],
      confWith(CoreFlags),
      TestShards,
    )
    check:
      internal.advertised.len == 1
      plugin.advertised.len == 1
      discv5.advertised.len == 0
      discv5.interests.len == 0

  test "the payload carries version, cluster, capabilities and shards":
    let raw = cast[seq[byte]](base64.decode(
      cast[string](selfAdvertisementData(confWith(CoreFlags), TestShards))
    ))
    check:
      raw.len == 5 # 4-byte header + one bitmap byte covering shards 0 and 3
      raw[0] == AdvertFormatVersion
      (uint16(raw[1]) shl 8 or uint16(raw[2])) == 16'u16
      CapabilitiesBitfield(raw[3]).supportsCapability(Capabilities.Relay)
      CapabilitiesBitfield(raw[3]).supportsCapability(Capabilities.Store)
      CapabilitiesBitfield(raw[3]).supportsCapability(Capabilities.Lightpush)
      not CapabilitiesBitfield(raw[3]).supportsCapability(Capabilities.Filter)
      raw[4] == 0b0000_1001'u8 # shards 0 and 3

  test "the payload stays within the size libp2p will accept":
    ## The JSON shape this replaced ran to ~188 bytes and could never be
    ## advertised. Checked with a deliberately wide shard set, not just the
    ## two-shard fixture.
    var manyShards: seq[uint16]
    for i in 0'u16 ..< 200'u16:
      manyShards.add(i)
    check selfAdvertisementData(confWith(CoreFlags), manyShards).len <= MaxAdvertLen

  test "the payload is valid UTF-8, so it survives a JSON transport":
    ## The plugin-hosted path marshals `data` as a JSON string on its way to the
    ## provider. A raw binary record throws there and takes the hosting module
    ## down, so every byte has to be printable ASCII.
    var manyShards: seq[uint16]
    for i in 0'u16 ..< 160'u16:
      manyShards.add(i)
    for data in [
      selfAdvertisementData(confWith(CoreFlags), TestShards),
      selfAdvertisementData(confWith(EdgeFlags), @[]),
      selfAdvertisementData(confWith(CoreFlags), manyShards),
    ]:
      check data.allIt(it >= 0x20'u8 and it < 0x7f'u8)

  test "a shard index beyond the bitmap is dropped, not aliased":
    let raw = cast[seq[byte]](base64.decode(
      cast[string](selfAdvertisementData(confWith(CoreFlags), @[0'u16, 9999'u16]))
    ))
    check:
      raw.len == 5
      raw[4] == 0b0000_0001'u8 # shard 0 only; 9999 did not fold onto a low bit

const MixKey = ServiceKeyPrefix & MixProtocolID

proc mixConfWith(flags: CapabilitiesBitfield): WakuConf =
  var conf = confWith(flags)
  let keys = generateKeyPair().expect("mix key pair")
  conf.mixConf = Opt.some(MixConf(mixKey: keys.privateKey, mixPubKey: keys.publicKey))
  conf

type AdvertiseCase = ref object
  node: WakuNode ## Never started. Its self hop is a loopback address.
  conf: WakuConf
  kad: FakeBackend
  backends: seq[IPeerDiscovery]

proc advertiseCase(
    flags = CoreFlags, addressPolicy: PeerAddressPolicy = defaultAddressPolicy
): Future[AdvertiseCase] {.async.} =
  let node = newTestWakuNode(
    generateSecp256k1Key(),
    parseIpAddress("127.0.0.1"),
    Port(25610),
    quicEnabled = false,
  )
  let keys = generateKeyPair().expect("mix key pair")
  (await node.mountMix(DefaultClusterId, keys.privateKey, @[], addressPolicy)).isOkOr:
    raiseAssert "mountMix: " & $error
  let kad = FakeBackend.create("service", @["service", "topic", "cap"])
  return AdvertiseCase(
    node: node, conf: mixConfWith(flags), kad: kad, backends: @[IPeerDiscovery(kad)]
  )

proc report(
    c: AdvertiseCase, reachability: NetworkReachability, source = SelfHopSource.Local
) {.async.} =
  await updateMixAdvertisement(c.backends, c.conf, c.node.wakuMix, reachability, source)

proc advertise(
    c: AdvertiseCase, reachability: NetworkReachability, source = SelfHopSource.Local
) {.async.} =
  await advertiseMix(c.backends, c.conf, c.node.wakuMix, reachability, source)

proc advertised(c: AdvertiseCase): bool =
  c.node.wakuMix.advertised

proc startMixWaku(nodeConf: WakuNodeConf): Future[Waku] {.async.} =
  var conf = nodeConf
  conf.mix = Opt.some(true)
  conf.mixPrivateHops = true
  let wakuConf = conf.toWakuConf().valueOr:
    raiseAssert error
  let waku = (await Waku.new(wakuConf)).valueOr:
    raiseAssert error
  (await waku.start()).isOkOr:
    raiseAssert error
  return waku

suite "Mix advertisement":
  test "a service node with an allowed self hop advertises itself once autonat confirms":
    check canAdvertiseMix(
      mixConfWith(CoreFlags), true, NetworkReachability.Reachable, SelfHopSource.Local
    )
      .isOk()

  test "each missing condition stops the advertisement":
    for (conf, allowed, reachability, source) in [
      (confWith(CoreFlags), true, NetworkReachability.Reachable, SelfHopSource.Local),
      (mixConfWith(EdgeFlags), true, NetworkReachability.Reachable, SelfHopSource.Local),
      (
        mixConfWith(CoreFlags),
        false,
        NetworkReachability.Reachable,
        SelfHopSource.Local,
      ),
      (mixConfWith(CoreFlags), true, NetworkReachability.Unknown, SelfHopSource.Local),
      (
        mixConfWith(CoreFlags),
        true,
        NetworkReachability.NotReachable,
        SelfHopSource.Local,
      ),
      (
        mixConfWith(CoreFlags),
        true,
        NetworkReachability.Reachable,
        SelfHopSource.Observed,
      ),
    ]:
      check canAdvertiseMix(conf, allowed, reachability, source).isErr()

  asyncTest "the advertisement follows autonat, and the interest in mix peers stays":
    let c = await advertiseCase()
    await c.advertise(NetworkReachability.Unknown)
    check:
      c.kad.interests == @[MixKey]
      c.kad.advertised.len == 0
      not c.advertised

    await c.report(NetworkReachability.Reachable)
    check:
      c.kad.advertised == @[MixKey]
      c.kad.advertisedData == @[@(c.conf.mixConf.get().mixPubKey)]
      c.advertised

    # The same report again changes nothing.
    await c.report(NetworkReachability.Reachable)
    check c.kad.advertised.len == 1

    await c.report(NetworkReachability.NotReachable)
    check:
      c.kad.stopped == @[MixKey]
      not c.advertised
      c.kad.interests == @[MixKey]

  asyncTest "an unknown reachability keeps the advertisement, not reachable stops it":
    let c = await advertiseCase()
    for reachability in [
      NetworkReachability.Reachable, NetworkReachability.Unknown,
      NetworkReachability.Reachable, NetworkReachability.Unknown,
    ]:
      await c.report(reachability)
    check:
      c.kad.advertised == @[MixKey]
      c.kad.stopped.len == 0
      c.advertised

    await c.report(NetworkReachability.NotReachable)
    check:
      c.kad.stopped == @[MixKey]
      not c.advertised

    # With no advertisement on, `Unknown` does not start one.
    await c.report(NetworkReachability.Unknown)
    check:
      c.kad.advertised == @[MixKey]
      not c.advertised

  asyncTest "a configured self hop advertises without autonat, not reachable stops it":
    let c = await advertiseCase()
    await c.report(NetworkReachability.Unknown, SelfHopSource.Configured)
    check:
      c.kad.advertised == @[MixKey]
      c.advertised

    await c.report(NetworkReachability.NotReachable, SelfHopSource.Configured)
    check:
      c.kad.stopped == @[MixKey]
      not c.advertised

  asyncTest "a self hop that only other peers observed does not advertise":
    ## An autonat v1 dial-back can pass the NAT mapping of this node.
    let c = await advertiseCase()
    await c.report(NetworkReachability.Reachable, SelfHopSource.Observed)
    check:
      not c.advertised
      c.kad.advertised.len == 0
      "only other peers observed" in c.node.wakuMix.notAdvertisingReason

    for source in [SelfHopSource.Local, SelfHopSource.Configured]:
      await c.report(NetworkReachability.Reachable, source)
      check c.advertised
      await c.report(NetworkReachability.NotReachable, source)
      check not c.advertised

  asyncTest "a client-only node finds mix peers but never advertises itself":
    let c = await advertiseCase(flags = EdgeFlags)
    await c.advertise(NetworkReachability.Reachable)
    await c.report(NetworkReachability.Reachable)
    check:
      c.kad.interests == @[MixKey]
      c.kad.advertised.len == 0
      not c.advertised

  asyncTest "a node whose self hop the public policy refuses does not advertise itself":
    ## The loopback hop can end its own reply paths. Other nodes cannot dial it.
    let c = await advertiseCase(addressPolicy = mixAddressPolicy(false))
    await c.advertise(NetworkReachability.Reachable)
    check:
      c.node.wakuMix.selfHopUsable()
      not c.node.wakuMix.selfHopAllowed()
      c.kad.interests == @[MixKey]
      c.kad.advertised.len == 0

suite "Mix advertisement in a running node":
  asyncTest "autonat reports drive the mix advertisement":
    ## The autonat observer of `Waku.new` reaches the mix of the node.
    let conf = defaultTestWakuNodeConf()
    let waku = await startMixWaku(conf)
    defer:
      discard await waku.stop()
    require not waku.node.wakuMix.isNil()
    check:
      waku.node.wakuMix.selfHopAllowed()
      waku.node.selfHopSource() == SelfHopSource.Local
      not waku.node.wakuMix.advertised

    await waku.autonat.reachabilityObservers.notify(
      NetworkReachability.Reachable, Opt.some(1.0)
    )
    check waku.node.wakuMix.advertised

    await waku.autonat.reachabilityObservers.notify(
      NetworkReachability.NotReachable, Opt.some(1.0)
    )
    check not waku.node.wakuMix.advertised

  asyncTest "a node on a configured address advertises itself at start":
    ## As a bootstrap node, with `--ext-multiaddr`. No autonat report comes.
    var conf = defaultTestWakuNodeConf()
    conf.tcpPort = Port(25622)
    conf.extMultiAddrs = @["/ip4/127.0.0.1/tcp/25622"]
    conf.extMultiAddrsOnly = true
    let waku = await startMixWaku(conf)
    defer:
      discard await waku.stop()
    require not waku.node.wakuMix.isNil()
    check:
      waku.node.selfHopSource() == SelfHopSource.Configured
      waku.node.wakuMix.advertised

    await waku.autonat.reachabilityObservers.notify(
      NetworkReachability.NotReachable, Opt.some(1.0)
    )
    check not waku.node.wakuMix.advertised
