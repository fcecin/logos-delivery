{.used.}

import std/[sequtils, times, tables]
import brokers/broker_context
import logos_delivery/waku/requests/node_state_requests
import testutils/unittests, chronos, results
import
  libp2p/[
    peerstore,
    peerid,
    multiaddress,
    crypto/crypto,
    switch,
    stream/connection,
    stream/lpstream,
  ]
import libp2p_mix
import libp2p_mix/mix_protocol
import libp2p/crypto/curve25519
import libp2p_mix/[peer_selection, pool, curve25519]
import
  logos_delivery/waku/[
    node/waku_node,
    node/peer_manager,
    node/peer_manager/waku_peer_store,
    waku_core,
    waku_mix,
  ]
import ../testlib/[wakucore, wakunode]

proc newTestNode(): Future[WakuNode] {.async.} =
  lockNewGlobalBrokerContext:
    return newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0))

proc waitUntil(condition: proc(): bool {.gcsafe, raises: [].}): Future[bool] {.async.} =
  ## Poll a peer-event side effect; the switch runs its handlers asynchronously.
  let deadline = Moment.now() + chronos.seconds(5)
  while not condition() and Moment.now() < deadline:
    await sleepAsync(chronos.milliseconds(10))
  return condition()

proc candidate(address = "/ip4/8.8.8.8/tcp/60000"): RemotePeerInfo =
  let identity = generateSecp256k1Key()
  RemotePeerInfo.init(
    PeerId.init(identity.getPublicKey().get()).get(),
    @[MultiAddress.init(address).get()],
    protocols = @["/vac/waku/relay/2.0.0"],
    publicKey = identity.getPublicKey().get(),
    mixPubKey = Opt.some(generateKeyPair().get().publicKey),
  )

suite "Delivery provides the mix peers":
  asyncTest "peer providers belong to each manager and survive a stop and restart":
    let first = await newTestNode()
    let second = await newTestNode()
    defer:
      first.peerManager.stop()
      second.peerManager.stop()
    let firstCtx = first.peerManager.configureMix(true).get()
    let secondCtx = second.peerManager.configureMix().get()
    let peer = candidate("/ip4/127.0.0.1/tcp/60000")
    first.peerManager.addPeer(peer)
    second.peerManager.addPeer(peer)
    check:
      firstCtx == first.brokerCtx
      secondCtx == second.brokerCtx
      firstCtx != secondCtx
      firstCtx.count([]).get() == 1
      secondCtx.count([]).get() == 0
    first.peerManager.stop()
    check:
      not ReadMixPeer.isProvided(firstCtx)
      not StoreMixPeer.isProvided(firstCtx)
      not RemoveMixPeer.isProvided(firstCtx)
      not ListMixPeerIds.isProvided(firstCtx)
      ReadMixPeer.isProvided(secondCtx)
      first.switch.peerStore[MixPubKeyBook][peer.peerId] == peer.mixPubKey.get()
    first.peerManager.startMixPeerProviders().get()
    check:
      firstCtx.count([]).get() == 1
      secondCtx.count([]).get() == 0
      first.peerManager.configureMix(true).get() == firstCtx

  asyncTest "mix providers share the node context without clearing other requests":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
      GetNodePeerManager.clearProvider(node.brokerCtx)
    GetNodePeerManager
      .setProvider(
        node.brokerCtx,
        proc(): Result[PeerManager, string] =
          ok(node.peerManager),
      )
      .expect("register node-state provider")
    let ctx = node.peerManager.configureMix().get()
    check:
      ctx == node.brokerCtx
      ReadMixPeer.isProvided(ctx)
      SelectReplyPeer.isProvided(ctx)
      GetNodePeerManager.request(ctx).get() == node.peerManager
    node.peerManager.stop()
    check:
      not ReadMixPeer.isProvided(ctx)
      not SelectReplyPeer.isProvided(ctx)
      GetNodePeerManager.request(ctx).get() == node.peerManager

  asyncTest "node restart registers peer providers before restarting mix":
    let node = await newTestNode()
    let keys = generateKeyPair().get()
    (
      await node.mountMix(
        DefaultClusterId, keys.privateKey, @[], allowAllAddresses = true
      )
    ).expect("mount")
    let ctx = node.wakuMix.brokerCtx
    let peer = candidate("/ip4/127.0.0.1/tcp/60000")
    node.peerManager.addPeer(peer)
    await node.start()
    await node.stop()
    check:
      not ReadMixPeer.isProvided(ctx)
      not SelectReplyPeer.isProvided(ctx)
      node.getMixNodePoolSize() == 0
      node.switch.peerStore[MixPubKeyBook][peer.peerId] == peer.mixPubKey.get()
    await node.start()
    try:
      check:
        ReadMixPeer.isProvided(ctx)
        SelectReplyPeer.isProvided(ctx)
        node.wakuMix.poolSize() == 1
        node.wakuMix.selfHopUsable()
    finally:
      await node.stop()
    check not ReadMixPeer.isProvided(ctx)

  asyncTest "mount uses the host store and sees subsequent discovery without copying":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let keys = generateKeyPair().get()
    (await node.mountMix(DefaultClusterId, keys.privateKey, @[])).expect("mount")
    check node.wakuMix.brokerCtx == node.brokerCtx
    let peer = candidate()
    check node.wakuMix.poolSize() == 0
    node.peerManager.addPeer(peer)
    check:
      node.wakuMix.poolSize() == 1
      node.wakuMix.brokerCtx.get(peer.peerId).get().get().mixPubKey ==
        peer.mixPubKey.get()
    let replacement = generateKeyPair().get().publicKey
    peer.mixPubKey = Opt.some(replacement)
    node.peerManager.addPeer(peer)
    check node.wakuMix.brokerCtx.get(peer.peerId).get().get().mixPubKey == replacement
    peer.mixPubKey = Opt.some(default(Curve25519Key))
    node.peerManager.addPeer(peer)
    check node.wakuMix.poolSize() == 0
    node.wakuMix.addBootNodes(
      @[
        MixNodePubInfo(
          multiAddr: $peer.addrs[0] & "/p2p/" & $peer.peerId, pubKey: replacement
        )
      ]
    )
    check:
      node.wakuMix.poolSize() == 1
      node.switch.peerStore[SourceBook][peer.peerId] == Static
      node.switch.peerStore[AddressBook].book[peer.peerId][0].confidence ==
        AddressConfidence.Infinite
    await node.stop()

  asyncTest "exclusion, recovery and store mutations preserve non-mix peer information":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let store = node.switch.peerStore
    let ctx = node.peerManager.configureMix().get()
    let peer = candidate()
    node.peerManager.addPeer(peer)
    store.excludeFromMix(peer.peerId)
    check:
      ctx.count([]).get() == 0
      ctx.get(peer.peerId).get().isNone()
      store.getPeer(peer.peerId).protocols == peer.protocols
      store.getPeer(peer.peerId).mixPubKey == peer.mixPubKey
    store.excludeFromMix(peer.peerId, false)
    let info = ctx.get(peer.peerId).get().get()
    check ctx.count([]).get() == 1
    check ctx.remove(peer.peerId).get()
    check:
      ctx.count([]).get() == 0
      store[AddressBook][peer.peerId] == peer.addrs
      store[KeyBook][peer.peerId] == peer.publicKey
    ctx.add(info).get()
    check:
      ctx.count([]).get() == 1
      store[MixPubKeyBook][peer.peerId] == info.mixPubKey
      store[ProtoBook][peer.peerId] == peer.protocols

  asyncTest "strict policy filters outbound hints too; permissive policy is host owned":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let peer = candidate()
    node.peerManager.addPeer(peer)
    let store = node.switch.peerStore
    let ctx = node.peerManager.configureMix().get()
    let loopback = MultiAddress.init("/ip4/127.0.0.1/tcp/60000").get()
    store[LastSeenOutboundBook][peer.peerId] = Opt.some(loopback)
    check ctx.get(peer.peerId).get().get().multiAddr == peer.addrs[0]
    discard node.peerManager.configureMix(true).get()
    check ctx.get(peer.peerId).get().get().multiAddr == loopback
    for address in [
      "/ip4/127.0.0.1/tcp/60000",
      "/ip4/10.0.0.1/tcp/60000",
      "/ip4/0.0.0.0/tcp/60000",
      "/ip4/169.254.1.1/tcp/60000",
      "/ip4/100.64.0.1/tcp/60000",
      "/ip4/203.0.113.1/tcp/60000",
      "/ip4/224.0.0.1/tcp/60000",
      "/ip4/255.255.255.255/tcp/60000",
      "/ip4/8.8.8.8/tcp/0",
      "/dns4/example.org/tcp/60000",
      "/ip4/8.8.8.8/tcp/60000/p2p/" & $peer.peerId & "/p2p-circuit",
    ]:
      let parsed = MultiAddress.init(address).get()
      check:
        not parsed.acceptsMixAddress()
        parsed.acceptsMixAddress(allowAllAddresses = true)
    discard store[AddressBook].del(peer.peerId)
    discard node.peerManager.configureMix(false).get()
    check ctx.count([]).get() == 0
    discard node.peerManager.configureMix(true).get()
    check:
      ctx.count([]).get() == 1
      store[KeyBook][peer.peerId] == peer.publicKey

  asyncTest "Kademlia mix discovery needs no ENR and deletion removes candidacy":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let peer = candidate()
    check peer.enr.isNone()
    node.peerManager.addPeer(peer, Kademlia)
    let ctx = node.peerManager.configureMix().get()
    check ctx.count([]).get() == 1
    node.switch.peerStore.delete(peer.peerId)
    check:
      ctx.count([]).get() == 0
      ctx.get(peer.peerId).get().isNone()

  asyncTest "application insertion preserves addresses and durability while stopped":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let peer = candidate()
    node.peerManager.addPeer(peer, Kademlia)
    let ctx = node.peerManager.configureMix().get()
    node.peerManager.stop()
    let address = MultiAddress.init("/ip4/8.8.4.4/tcp/60001").get()
    node.peerManager.storeMixPeer(
      MixPubInfo.init(peer.peerId, address, peer.mixPubKey.get(), peer.publicKey.skkey)
    )
    node.peerManager.startMixPeerProviders().get()
    node.peerManager.addPeer(peer, Kademlia)
    let store = node.switch.peerStore
    check:
      store[MixAliveBook][peer.peerId] == Moment()
      address in store[AddressBook][peer.peerId]
      peer.addrs[0] in store[AddressBook][peer.peerId]
      ctx.count([]).get() == 1

  asyncTest "the source obeys existing Delivery connection backoff and its expiry":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let peer = candidate()
    node.peerManager.addPeer(peer)
    let store = node.switch.peerStore
    let ctx = node.peerManager.configureMix().get()
    store[NumberFailedConnBook][peer.peerId] = 1
    store[LastFailedConnBook][peer.peerId] = Moment.init(getTime().toUnix(), Second)
    check ctx.count([]).get() == 0
    store[LastFailedConnBook][peer.peerId] = Moment.init(1, Second)
    check ctx.count([]).get() == 1
    store[NumberFailedConnBook][peer.peerId] = node.peerManager.maxFailedAttempts
    check ctx.count([]).get() == 0
    check store[MixPubKeyBook][peer.peerId] == peer.mixPubKey.get()

  asyncTest "default address policy admits a private own hop but not private peers":
    let node = await newTestNode()
    let keys = generateKeyPair().get()
    (await node.mountMix(DefaultClusterId, keys.privateKey, @[])).get()
    defer:
      node.peerManager.stop()
    # A public QUIC-v1 endpoint is accepted like a public TCP endpoint.
    node.peerManager.addPeer(candidate("/ip4/8.8.4.4/udp/60000/quic-v1"))
    check node.wakuMix.poolSize() == 1
    for address in ["/ip4/127.0.0.1/tcp/60000", "/ip4/192.168.1.2/tcp/60000"]:
      let hop = MultiAddress.init(address).get()
      check node.wakuMix.updateSelfHop(@[hop], @[]).isSome()
      check node.wakuMix.selfHopUsable()
      node.peerManager.addPeer(candidate(address))
      check node.wakuMix.poolSize() == 1

proc interruptedReplyPreparation(action: string) {.async.} =
  var accepted: seq[StreamTransport]
  let ready = newAsyncEvent()
  let server = createStreamServer(
    initTAddress("127.0.0.1:0"),
    proc(server: StreamServer, client: StreamTransport) {.async: (raises: []).} =
      accepted.add(client)
      ready.fire(),
  )
  server.start()
  defer:
    server.stop()
    await server.closeWait()
    for transport in accepted:
      await transport.closeWait()
  let node = await newTestNode()
  let key = generateKeyPair().get()
  (
    await node.mountMix(
      DefaultClusterId,
      key.privateKey,
      @[],
      allowAllAddresses = true,
      replyPeerTimeout =
        (if action == "timeout": chronos.milliseconds(50)
        else: chronos.seconds(5)),
    )
  ).expect("mount mix")
  await node.start()
  defer:
    await node.stop()
  for _ in 0 ..< 4:
    node.peerManager.addPeer(
      candidate("/ip4/127.0.0.1/tcp/" & $server.localAddress().port)
    )
  const Codec = "/test/delivery-reply/1"
  node.wakuMix.registerDestReadBehavior(Codec, readLp(1024))
  let conn = node.wakuMix
    .toConnection(
      MixDestination.exitNode(node.wakuMix.brokerCtx.peerIds().get()[^1]),
      Codec,
      MixParameters(expectReply: Opt.some(true), numSurbs: Opt.some(2'u8)),
    )
    .get()
  defer:
    await conn.close()
  let writing = conn.writeLp(@[1'u8])
  await ready.wait().wait(chronos.seconds(2))
  case action
  of "timeout":
    expect LPStreamError:
      await writing.wait(chronos.seconds(2))
  of "close":
    await conn.close().wait(chronos.seconds(2))
    expect LPStreamClosedError:
      await writing
  of "stop":
    await node.wakuMix.stop().wait(chronos.seconds(2))
    expect LPStreamError:
      await writing
  else:
    await writing.cancelAndWait()
  check node.wakuMix.surbCredsLen == 0

suite "Delivery selection of the final reply peer":
  asyncTest "a connected exit is excluded and Delivery connects one eligible peer and reuses it":
    let sender = await newTestNode()
    let exit = await newTestNode()
    let first = await newTestNode()
    let second = await newTestNode()
    let keys = generateKeyPair().get()
    (
      await sender.mountMix(
        DefaultClusterId, keys.privateKey, @[], allowAllAddresses = true
      )
    ).expect("mount")
    await sender.start()
    await exit.start()
    await first.start()
    await second.start()
    defer:
      await sender.stop()
      await exit.stop()
      await first.stop()
      await second.stop()
    for node in [exit, first, second]:
      let peer = node.peerInfo.toRemotePeerInfo()
      peer.mixPubKey = Opt.some(generateKeyPair().get().publicKey)
      sender.peerManager.addPeer(peer)
    await sender.switch.connect(exit.peerInfo.peerId, exit.peerInfo.addrs)
    let excluded = @[sender.peerInfo.peerId, exit.peerInfo.peerId]
    let candidates = [first.peerInfo.peerId, second.peerInfo.peerId]
    let selected = (await sender.wakuMix.selectReplyPeer(excluded)).get()
    check:
      selected in candidates
      sender.switch.isConnected(selected)
      sender.switch.connectedPeers().len == 2
    # The connected set now has one eligible member, and it is reused.
    for _ in 0 ..< 8:
      check (await sender.wakuMix.selectReplyPeer(excluded)).get() == selected

  asyncTest "the cold-start reply dial selects among the eligible peers at random":
    var peers: seq[WakuNode]
    for _ in 0 ..< 3:
      let node = await newTestNode()
      await node.start()
      peers.add(node)
    defer:
      for node in peers:
        await node.stop()
    let candidates = peers.mapIt(it.peerInfo.peerId)
    var selected: seq[PeerId]
    for _ in 0 ..< 12:
      # Each sender starts with no connection, so each draw is a cold start.
      let sender = await newTestNode()
      let keys = generateKeyPair().get()
      (
        await sender.mountMix(
          DefaultClusterId, keys.privateKey, @[], allowAllAddresses = true
        )
      ).expect("mount")
      await sender.start()
      for node in peers:
        let peer = node.peerInfo.toRemotePeerInfo()
        peer.mixPubKey = Opt.some(generateKeyPair().get().publicKey)
        sender.peerManager.addPeer(peer)
      let peer = (await sender.wakuMix.selectReplyPeer(@[sender.peerInfo.peerId])).get()
      check:
        peer in candidates
        sender.switch.isConnected(peer)
      if peer notin selected:
        selected.add(peer)
      await sender.stop()
    # Twelve cold starts over three peers select more than one peer.
    check selected.len >= 2

  asyncTest "Delivery bounds stalled reply preparation without retaining credentials":
    await interruptedReplyPreparation("timeout")

  asyncTest "entry close cancels Delivery's pending reply connection":
    await interruptedReplyPreparation("close")

  asyncTest "protocol stop cancels Delivery's pending reply connection":
    await interruptedReplyPreparation("stop")

  asyncTest "send cancellation cancels Delivery's pending reply connection":
    await interruptedReplyPreparation("cancel")

suite "Mix broker errors":
  asyncTest "failed mount preserves conflicting providers and permits retry":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    StoreMixPeer
      .setProvider(
        node.brokerCtx,
        proc(info: MixPubInfo): Result[void, string] =
          ok(),
      )
      .get()
    let keys = generateKeyPair().get()
    let mounted = await node.mountMix(DefaultClusterId, keys.privateKey, @[])
    check mounted.isErr
    check node.wakuMix.isNil()
    check not ReadMixPeer.isProvided(node.brokerCtx)
    check StoreMixPeer.isProvided(node.brokerCtx)
    node.peerManager.stop()
    check StoreMixPeer.isProvided(node.brokerCtx)
    StoreMixPeer.clearProvider(node.brokerCtx)
    check (await node.mountMix(DefaultClusterId, keys.privateKey, @[])).isOk

  asyncTest "configuration returns an invalid timeout error":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    check node.peerManager.configureMix(replyPeerTimeout = ZeroDuration).isErr

  asyncTest "population reads report their own provider errors":
    let node = await newTestNode()
    defer:
      node.peerManager.stop()
    let keys = generateKeyPair().get()
    (await node.mountMix(DefaultClusterId, keys.privateKey, @[])).get()
    node.peerManager.addPeer(candidate())
    check node.wakuMix.poolSize() == 1
    StoreMixPeer.clearProvider(node.brokerCtx)
    check node.wakuMix.poolSize() == 1
    ReadMixPeer
      .replaceProvider(
        node.brokerCtx,
        proc(peerId: PeerId): Result[MixPeerRecord, string] =
          err("peer source unavailable"),
      )
      .get()
    check node.brokerCtx.count([]).isErr
    check node.wakuMix.poolSize() == 0
