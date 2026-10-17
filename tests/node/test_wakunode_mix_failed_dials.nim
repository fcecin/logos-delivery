{.used.}

## The library reports each hop dial through `onHopDial`. `WakuMix` decides
## what a failed dial means for the pool. These tests call the handler with
## each outcome, and one test lets a real send fail on a dead first hop.

import std/sequtils
import testutils/unittests, chronos, results, metrics, stew/byteutils
import libp2p/[crypto/crypto, multiaddress, peerid, switch, dial, connmanager]
import libp2p_mix, libp2p_mix/[curve25519, mix_protocol]

import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/delivery_dialer,
  ../testlib/[wakucore, wakunode]

const DeadAddress = "/ip4/127.0.0.1/tcp/1"
  ## A reserved port that refuses each connection at once.

proc startMixNode(bootnodes: seq[MixNodePubInfo] = @[]): Future[WakuNode] {.async.} =
  let node = newTestWakuNode(
    generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0), quicEnabled = false
  )
  let mixKeys = generateKeyPair().expect("mix keys")
  (
    await node.mountMix(
      DefaultClusterId,
      mixKeys.privateKey,
      bootnodes,
      addressPolicy = defaultAddressPolicy,
    )
  ).expect("mount mix")
  await node.start()
  return node

proc startOtherConnection(node: WakuNode): Future[WakuNode] {.async.} =
  ## A connection to a node without mix. A failed dial counts only while this
  ## node has one.
  let other = newTestWakuNode(
    generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0), quicEnabled = false
  )
  await other.start()
  await node.connectToNodes(@[other.peerInfo.toRemotePeerInfo()])
  doAssert node.switch.isConnected(other.peerInfo.peerId)
  return other

proc discoverDeadPeer(node: WakuNode): PeerId =
  ## A mix node record at the dead address, as discovery writes it.
  let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
  let hopKeys = generateKeyPair().expect("hop keys")
  node.peerManager.addPeer(
    RemotePeerInfo.init(
      peerId,
      @[MultiAddress.init(DeadAddress).expect("address")],
      mixPubKey = Opt.some(hopKeys.publicKey),
    )
  )
  doAssert node.wakuMix.nodePool.get(peerId).isSome()
  return peerId

proc hopAddresses(node: WakuNode, peerId: PeerId): seq[MultiAddress] =
  ## The hop address of a pool member, as the library dials it for a path of
  ## this node. It is empty for a suspended node.
  node.wakuMix.pool
    .get(peerId)
    .map(
      proc(info: MixPubInfo): seq[MultiAddress] =
        @[info.multiAddr]
    )
    .get(@[])

proc report(
    node: WakuNode,
    peerId: PeerId,
    outcome: MixHopDialOutcome,
    hadConnection = false,
    error: ref CatchableError = nil,
) =
  node.wakuMix.onHopDial(
    peerId, node.hopAddresses(peerId), outcome, hadConnection, error
  )

proc onPaths(node: WakuNode, peerId: PeerId): bool =
  node.wakuMix.nodePool.get(peerId).isSome()

suite "Waku Mix - failed hop dials":
  asyncTest "a failed dial takes a discovered mix node off mix paths for the backoff":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let failures = logos_delivery_mix_dial_failures.value()
    let before = Moment.now()

    node.report(peerId, MixHopDialOutcome.Failed)

    check:
      not node.onPaths(peerId)
      peerId in node.wakuMix.pool
      node.getMixNodePoolSize() == 0
      logos_delivery_mix_dial_failures.value() == failures + 1
    let until = node.wakuMix.pool.suspendedUntil(peerId)
    check:
      until.isSome()
      until.get() >= before + MixFailureBackoff

  asyncTest "a failed dial at an address other than the hop address records nothing":
    ## A node that forwards a packet dials the address that the sender wrote.
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let failures = logos_delivery_mix_dial_failures.value()

    node.wakuMix.onHopDial(
      peerId,
      @[MultiAddress.init("/ip4/127.0.0.1/tcp/2").expect("address")],
      MixHopDialOutcome.Failed,
      false,
      nil,
    )

    check:
      node.onPaths(peerId)
      logos_delivery_mix_dial_failures.value() == failures

  asyncTest "a second failed dial to a suspended mix node records nothing":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let addresses = node.hopAddresses(peerId)
    let failures = logos_delivery_mix_dial_failures.value()

    node.report(peerId, MixHopDialOutcome.Failed)
    let until = node.wakuMix.pool.suspendedUntil(peerId)
    node.wakuMix.onHopDial(peerId, addresses, MixHopDialOutcome.Failed, false, nil)

    check:
      logos_delivery_mix_dial_failures.value() == failures + 1
      node.wakuMix.pool.suspendedUntil(peerId) == until

  asyncTest "a failed dial while this node stops records nothing":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    node.switch.abortDials()

    node.report(peerId, MixHopDialOutcome.Failed)
    node.switch.resumeDials()

    check node.onPaths(peerId)

  asyncTest "a successful dial takes the latest hop address of a suspended mix node":
    ## Identify writes the new address during the dial, while the node is still
    ## suspended, so the address handler skips it.
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    node.report(peerId, MixHopDialOutcome.Failed)
    check not node.onPaths(peerId)
    let moved = MultiAddress.init("/ip4/127.0.0.1/tcp/2").expect("address")
    node.switch.peerStore[AddressBook][peerId] = @[moved]

    node.wakuMix.onHopDial(peerId, @[moved], MixHopDialOutcome.Succeeded, false, nil)

    check node.wakuMix.nodePool.get(peerId).get().multiAddr == moved

  asyncTest "a cancelled dial takes a discovered mix node off mix paths":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()

    node.report(peerId, MixHopDialOutcome.Cancelled)

    check not node.onPaths(peerId)

  asyncTest "a successful dial puts a suspended mix node back on mix paths":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    node.report(peerId, MixHopDialOutcome.Failed)
    check not node.onPaths(peerId)

    node.report(peerId, MixHopDialOutcome.Succeeded)

    check:
      node.onPaths(peerId)
      node.wakuMix.pool.suspendedUntil(peerId).isNone()

  asyncTest "a failed dial with an existing connection records nothing":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let failures = logos_delivery_mix_dial_failures.value()

    node.report(peerId, MixHopDialOutcome.Failed, hadConnection = true)

    check:
      node.onPaths(peerId)
      logos_delivery_mix_dial_failures.value() == failures

  asyncTest "a dial that the connection limit of this node refuses records nothing":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let limit = newException(TooManyConnectionsError, "limit reached")
    let error = newException(DialFailedError, "failed getOutgoingSlot", limit)
    let failures = logos_delivery_mix_dial_failures.value()

    node.report(peerId, MixHopDialOutcome.Failed, error = error)

    check:
      node.onPaths(peerId)
      logos_delivery_mix_dial_failures.value() == failures

  asyncTest "a failed dial records nothing while this node has no other connection":
    let node = await startMixNode()
    defer:
      await node.stop()
    let peerId = node.discoverDeadPeer()
    check node.switch.connectedPeers().len == 0
    let failures = logos_delivery_mix_dial_failures.value()

    node.report(peerId, MixHopDialOutcome.Failed)

    check:
      node.onPaths(peerId)
      logos_delivery_mix_dial_failures.value() == failures

  asyncTest "a failed dial to a configured mix node records nothing":
    let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
    let hopKeys = generateKeyPair().expect("hop keys")
    let node = await startMixNode(
      @[
        MixNodePubInfo(
          multiAddr: DeadAddress & "/p2p/" & $peerId, pubKey: hopKeys.publicKey
        )
      ]
    )
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    check node.onPaths(peerId)
    let failures = logos_delivery_mix_dial_failures.value()

    node.report(peerId, MixHopDialOutcome.Failed)

    check:
      node.onPaths(peerId)
      logos_delivery_mix_dial_failures.value() == failures

  asyncTest "a successful dial renews the record of a discovered mix node":
    ## `refresh` also sets the record time, so a node with an old record stays in
    ## the pool after the next maintenance pass. The peer store delete leaves
    ## `learnDiscoveredNode` no record to read, so only `refresh` renews the node.
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    let info = node.wakuMix.pool.get(peerId).expect("pool member")
    let old = Moment.now() - DiscoveredMixNodeTtl
    node.wakuMix.pool.add(info, MixNodeSource.Discovered, now = old)
    node.switch.peerStore.del(peerId)

    node.report(peerId, MixHopDialOutcome.Succeeded)
    node.wakuMix.pool.maintain()

    check peerId in node.wakuMix.pool

  asyncTest "a new discovery record does not end the backoff":
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    let peerId = node.discoverDeadPeer()
    node.report(peerId, MixHopDialOutcome.Failed)
    check not node.onPaths(peerId)

    node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init("/ip4/127.0.0.1/tcp/2").expect("address")],
        mixPubKey = Opt.some(generateKeyPair().expect("hop keys").publicKey),
      )
    )

    check not node.onPaths(peerId)

  asyncTest "a failed dial of the first hop of a send takes that node off mix paths":
    ## Each pool member is dead. The send dials its first hop, the dial fails,
    ## and the library reports it. Only that hop leaves mix paths.
    let node = await startMixNode()
    let other = await node.startOtherConnection()
    defer:
      await node.stop()
      await other.stop()
    var deadIds: seq[PeerId]
    for _ in 0 ..< 5:
      deadIds.add(node.discoverDeadPeer())
    let exitId = deadIds[0]

    let sent = await node.wakuMix.anonymizeLocalProtocolSend(
      newAsyncQueue[seq[byte]](),
      toBytes("first-hop"),
      WakuLightPushCodec,
      MixDestination.exitNode(exitId),
      0'u8,
    )

    check sent.isErr()
    let suspended = deadIds.filterIt(not node.onPaths(it))
    check:
      suspended.len == 1
      node.getMixNodePoolSize() == 4
    if suspended.len == 1:
      check suspended[0] != exitId
