{.used.}

import
  testutils/unittests,
  chronos,
  results,
  metrics,
  libp2p/[crypto/crypto, peerid, multiaddress],
  libp2p/nameresolving/nameresolver,
  libp2p_mix/[curve25519, mix_metrics]

import
  logos_delivery/waku/[waku_core, node/peer_manager, waku_node, waku_mix],
  ../testlib/[wakucore, wakunode, testasync]

## `poolSize` answers "how many mix nodes can carry a packet". `mixReady` and
## the `mix_pool_size` gauge are both built on it, so it must describe the live
## pool and count only the nodes a path can actually use.

suite "Waku Mix - pool size":
  var node {.threadvar.}: WakuNode

  asyncSetup:
    node = newTestWakuNode(generateSecp256k1Key())

    # Mounted before the start, as the node factory does: a protocol cannot be
    # mounted on a switch that is already running.
    let mixKeys = generateKeyPair().expect("mix key pair")
    (await node.mountMix(DefaultClusterId, mixKeys.privateKey, @[])).isOkOr:
      raiseAssert "Failed to mount mix: " & $error

    await node.start()

  asyncTeardown:
    await node.stop()

  proc addMixPeer(address: string): PeerId =
    ## What discovery does when it learns a peer's mix key: the key and the
    ## address it came with go into the peer store, and the pool is a view
    ## over that.
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let mixKeys = generateKeyPair().expect("mix key pair")
    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init(address).tryGet()],
        mixPubKey = Opt.some(mixKeys.publicKey),
      )
    )
    return peerId

  asyncTest "a node with no mix peers has an empty pool":
    ## The bootstrap list is empty here, which is what a preset-configured node
    ## starts with.
    check node.getMixNodePoolSize() == 0

  asyncTest "only the peers mix can route count towards the pool":
    ## Mix takes IPv4 TCP and QUIC-v1. The other two peers have a mix key and
    ## no address a path can use, so they are known but not usable.
    discard addMixPeer("/ip4/127.0.0.1/tcp/60001")
    discard addMixPeer("/ip4/127.0.0.1/udp/60002/quic-v1")
    discard addMixPeer("/dns4/node.test/tcp/60003")
    discard addMixPeer("/ip6/::1/tcp/60004")

    check:
      node.getMixNodePoolSize() == 2
      mix_pool_size.value() == 2.0

  asyncTest "the pool follows the peers discovery brings in":
    ## The count is not fixed at mount: it is the live pool, so it moves as
    ## mix keys arrive.
    check node.getMixNodePoolSize() == 0

    for port in 60010 .. 60012:
      discard addMixPeer("/ip4/127.0.0.1/tcp/" & $port)
    check node.getMixNodePoolSize() == 3

    discard addMixPeer("/ip4/127.0.0.1/tcp/60013")
    check:
      node.getMixNodePoolSize() == 4
      mix_pool_size.value() == 4.0

  asyncTest "mix is not ready until enough peers can carry a packet":
    ## `mixReady` is `poolSize() >= MinMixPoolSize`, so an unroutable peer must
    ## not push a node over the line.
    for port in 60020 .. 60022:
      discard addMixPeer("/ip4/127.0.0.1/tcp/" & $port)
    discard addMixPeer("/dns4/node.test/tcp/60023")

    check node.getMixNodePoolSize() == 3 # the name is known, and unusable

    discard addMixPeer("/ip4/127.0.0.1/tcp/60024")
    check node.getMixNodePoolSize() == MinMixPoolSize

  asyncTest "the gauge at mount counts routable bootnodes, not parsed ones":
    ## An IPv6 bootnode parses at mount and is unroutable for mix, so the
    ## parsed count and the routable count differ there; the gauge and the
    ## pool size must both follow the routable one.
    proc bootnode(address: string): MixNodePubInfo =
      let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
      let keys = generateKeyPair().expect("mix key pair")
      return
        MixNodePubInfo(multiAddr: address & "/p2p/" & $peerId, pubKey: keys.publicKey)

    let other = newTestWakuNode(generateSecp256k1Key())
    let mixKeys = generateKeyPair().expect("mix key pair")
    (
      await other.mountMix(
        DefaultClusterId,
        mixKeys.privateKey,
        @[bootnode("/ip4/127.0.0.1/tcp/60030"), bootnode("/ip6/::1/tcp/60031")],
      )
    ).isOkOr:
      raiseAssert "Failed to mount mix: " & $error

    check:
      other.getMixNodePoolSize() == 1
      mix_pool_size.value() == 1.0

suite "Waku Mix - pool size without mix":
  asyncTest "a node that never mounted mix reports an empty pool":
    ## `getMixNodePoolSize` is a public accessor; "no mix" is a state a caller
    ## may be in, and must read as zero, not as a nil dereference.
    let node = newTestWakuNode(generateSecp256k1Key())
    check node.getMixNodePoolSize() == 0

type StubResolver = ref object of NameResolver
  ## Answers every name with one address, which is all the mix bootstrap path
  ## asks of a resolver.
  answer: string

method resolveTxt(
    self: StubResolver, address: string
): Future[seq[string]] {.async: (raises: [CancelledError]).} =
  return @[]

method resolveIp(
    self: StubResolver, address: string, port: Port, domain: Domain = Domain.AF_UNSPEC
): Future[seq[TransportAddress]] {.
    async: (raises: [CancelledError, TransportAddressError])
.} =
  return @[initTAddress(self.answer, port)]

proc mixBootnode(address: string): MixNodePubInfo =
  ## A bootstrap entry as a preset ships it: an address with a peer id, and the
  ## node's mix public key.
  let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
  let mixKeys = generateKeyPair().expect("mix key pair")
  return
    MixNodePubInfo(multiAddr: address & "/p2p/" & $peerId, pubKey: mixKeys.publicKey)

type EmptyResolver = ref object of NameResolver
  ## What `DnsResolver` returns for a name with no record, or with no
  ## reachable server: an empty answer, not an error.

method resolveTxt(
    self: EmptyResolver, address: string
): Future[seq[string]] {.async: (raises: [CancelledError]).} =
  return @[]

method resolveIp(
    self: EmptyResolver, address: string, port: Port, domain: Domain = Domain.AF_UNSPEC
): Future[seq[TransportAddress]] {.
    async: (raises: [CancelledError, TransportAddressError])
.} =
  return @[]

type RaisingResolver = ref object of NameResolver

method resolveTxt(
    self: RaisingResolver, address: string
): Future[seq[string]] {.async: (raises: [CancelledError]).} =
  return @[]

method resolveIp(
    self: RaisingResolver,
    address: string,
    port: Port,
    domain: Domain = Domain.AF_UNSPEC,
): Future[seq[TransportAddress]] {.
    async: (raises: [CancelledError, TransportAddressError])
.} =
  raise newException(TransportAddressError, "scripted resolver failure")

type TwoAddressResolver = ref object of NameResolver ## A name with two A records.

method resolveTxt(
    self: TwoAddressResolver, address: string
): Future[seq[string]] {.async: (raises: [CancelledError]).} =
  return @[]

method resolveIp(
    self: TwoAddressResolver,
    address: string,
    port: Port,
    domain: Domain = Domain.AF_UNSPEC,
): Future[seq[TransportAddress]] {.
    async: (raises: [CancelledError, TransportAddressError])
.} =
  return @[initTAddress("127.0.0.1", port), initTAddress("127.0.0.2", port)]

suite "Waku Mix - bootstrap nodes":
  ## Presets pin names, because a name survives a fleet node moving hosts, and
  ## `mountMix` spends the name before the pool is built: mix routes literal
  ## IPv4 TCP and QUIC-v1 addresses only.

  proc mountWith(
      bootnodes: seq[MixNodePubInfo], nameResolver: NameResolver = nil
  ): Future[WakuNode] {.async.} =
    let node = newTestWakuNode(generateSecp256k1Key(), nameResolver = nameResolver)
    let mixKeys = generateKeyPair().expect("mix key pair")
    (await node.mountMix(DefaultClusterId, mixKeys.privateKey, bootnodes)).isOkOr:
      raiseAssert "Failed to mount mix: " & $error
    await node.start()
    return node

  asyncTest "literal addresses seed the pool at mount":
    ## What a seeded preset buys: a pool that can build a path from the first
    ## second, instead of one that waits for discovery.
    let node = await mountWith(
      @[
        mixBootnode("/ip4/127.0.0.1/tcp/60101"),
        mixBootnode("/ip4/127.0.0.1/tcp/60102"),
        mixBootnode("/ip4/127.0.0.1/tcp/60103"),
        mixBootnode("/ip4/127.0.0.1/tcp/60104"),
      ]
    )

    check node.getMixNodePoolSize() == MinMixPoolSize
    await node.stop()

  asyncTest "a name the node cannot resolve is dropped, and the rest still mount":
    ## `newTestWakuNode` has no name resolver, so the two names cannot be spent.
    ## They are dropped rather than failing the mount: one unreachable name
    ## must not cost the node every other mix node it was given.
    let node = await mountWith(
      @[
        mixBootnode("/ip4/127.0.0.1/tcp/60111"),
        mixBootnode("/dns4/delivery-01.example.invalid/tcp/30303"),
        mixBootnode("/ip4/127.0.0.1/tcp/60112"),
        mixBootnode("/dns4/delivery-02.example.invalid/tcp/30303"),
      ]
    )

    check node.getMixNodePoolSize() == 2
    await node.stop()

  asyncTest "a name is resolved into the address mix routes":
    ## The mechanism a seeded preset rests on: the preset pins
    ## `/dns4/<host>/tcp/30303`, and the pool ends up holding the literal the
    ## name answered with. Nothing but the name is stored in the preset, so a
    ## fleet node that moves keeps working.
    let node = await mountWith(
      @[
        mixBootnode("/dns4/delivery-01.example.invalid/tcp/30301"),
        mixBootnode("/dns4/delivery-02.example.invalid/tcp/30302"),
        mixBootnode("/dns4/delivery-03.example.invalid/tcp/30303"),
        mixBootnode("/dns4/delivery-04.example.invalid/tcp/30304"),
      ],
      StubResolver(answer: "127.0.0.1"),
    )

    check node.getMixNodePoolSize() == MinMixPoolSize
    await node.stop()

  asyncTest "a name that answers with no address is dropped, the rest still mount":
    ## What a dead or stale fleet name looks like through the real resolver:
    ## `DnsResolver` answers with an empty list rather than raising.
    let node = await mountWith(
      @[
        mixBootnode("/ip4/127.0.0.1/tcp/60121"),
        mixBootnode("/dns4/gone-01.example.invalid/tcp/30303"),
        mixBootnode("/ip4/127.0.0.1/tcp/60122"),
        mixBootnode("/dns4/gone-02.example.invalid/tcp/30303"),
      ],
      EmptyResolver(),
    )

    check node.getMixNodePoolSize() == 2
    await node.stop()

  asyncTest "a lookup that raises is dropped, the rest still mount":
    let node = await mountWith(
      @[
        mixBootnode("/ip4/127.0.0.1/tcp/60131"),
        mixBootnode("/dns4/broken-01.example.invalid/tcp/30303"),
        mixBootnode("/ip4/127.0.0.1/tcp/60132"),
        mixBootnode("/dns4/broken-02.example.invalid/tcp/30303"),
      ],
      RaisingResolver(),
    )

    check node.getMixNodePoolSize() == 2
    await node.stop()

  asyncTest "a name that answers with two addresses is one pool member with two addresses":
    let entry = mixBootnode("/dns4/delivery-01.example.invalid/tcp/30303")
    let peerId = parsePeerInfo(entry.multiAddr).get().peerId
    let node = await mountWith(@[entry], TwoAddressResolver())

    check:
      node.getMixNodePoolSize() == 1
      node.peerManager.switch.peerStore.getPeer(peerId).addrs.len == 2
    await node.stop()

type NeverResolver = ref object of NameResolver
  waits: seq[Future[void].Raising([CancelledError])]

method resolveTxt(
    self: NeverResolver, address: string
): Future[seq[string]] {.async: (raises: [CancelledError]).} =
  return @[]

method resolveIp(
    self: NeverResolver, address: string, port: Port, domain: Domain = Domain.AF_UNSPEC
): Future[seq[TransportAddress]] {.
    async: (raises: [CancelledError, TransportAddressError])
.} =
  let wait = sleepAsync(chronos.minutes(10))
  self.waits.add(wait)
  await wait
  return @[]

suite "Waku Mix - name resolution at mount":
  ## A preset name that does not answer within MixNodeResolveTimeout must be
  ## dropped, the literal kept, and the mount must survive -- never abort the
  ## node start with an escaped CancelledError. Costs the 10 s timeout it tests.
  asyncTest "a name that never answers is dropped, the mount survives":
    let resolver = NeverResolver()
    let node = newTestWakuNode(generateSecp256k1Key(), nameResolver = resolver)
    let keys = generateKeyPair().expect("mix key pair")
    let pid = PeerId.init(generateSecp256k1Key()).tryGet()
    # Bounded well above the 10 s budget: a regression of the cancel loop must
    # fail here in seconds, not hold the test for the resolver's whole sleep.
    let mount = node.mountMix(
      DefaultClusterId,
      keys.privateKey,
      @[
        MixNodePubInfo(
          multiAddr: "/ip4/127.0.0.1/tcp/60401/p2p/" & $pid, pubKey: keys.publicKey
        ),
        MixNodePubInfo(
          multiAddr: "/dns4/never-01.invalid/tcp/30303/p2p/" & $pid,
          pubKey: keys.publicKey,
        ),
        MixNodePubInfo(
          multiAddr: "/dns4/never-02.invalid/tcp/30303/p2p/" & $pid,
          pubKey: keys.publicKey,
        ),
      ],
    )
    check await mount.withTimeout(chronos.seconds(30))
    if mount.finished():
      mount.read().isOkOr:
        raiseAssert "mount failed: " & error
    check node.getMixNodePoolSize() == 1 # the literal survived
    # The mount cancelled every lookup itself, not just the first one.
    check resolver.waits.len == 2
    for wait in resolver.waits:
      check wait.cancelled()
    await node.stop()
