{.used.}

## Mix test fixtures.

import
  std/[net, sequtils, strutils, tables],
  chronos,
  results,
  stew/byteutils,
  libp2p/[multiaddress, peerid, peerinfo, switch],
  libp2p/crypto/crypto,
  libp2p/stream/connection,
  libp2p_mix/[curve25519, mix_protocol]
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/common/rate_limit/setting,
  logos_delivery/waku/discovery/[peer_discovery_interface, peer_discovery_conversion],
  ./[wakucore, wakunode]

const MixNodeCount* = 4

proc mountMixWith*(
    node: WakuNode,
    addressPolicy: PeerAddressPolicy,
    bootnodes: seq[MixNodePubInfo] = @[],
) {.async.} =
  let keys = generateKeyPair().expect("mix key pair")
  (await node.mountMix(DefaultClusterId, keys.privateKey, bootnodes, addressPolicy)).isOkOr:
    raiseAssert "mountMix: " & $error

proc mixNode*(
    addressPolicy: PeerAddressPolicy = mixAddressPolicy(false),
    bootnodes: seq[MixNodePubInfo] = @[],
): Future[WakuNode] {.async.} =
  ## A node with mix that never starts, so it dials nothing. It takes the
  ## public address policy, for policy tests.
  let node = newTestWakuNode(generateSecp256k1Key(), quicEnabled = false)
  await node.mountMixWith(addressPolicy, bootnodes)
  return node

proc loopbackNode(port: int, quicEnabled: bool): WakuNode =
  let node = newTestWakuNode(
    generateSecp256k1Key(),
    parseIpAddress("127.0.0.1"),
    Port(port),
    quicEnabled = quicEnabled,
  )
  node.mountMetadata(uint32(DefaultClusterId), @[0'u16]).expect("metadata")
  return node

proc startNodeWithoutMix*(port: int, quicEnabled = false): Future[WakuNode] {.async.} =
  ## A started node without mix, for a real dial.
  let node = loopbackNode(port, quicEnabled)
  await node.start()
  return node

proc startMixNode*(
    port: int,
    addressPolicy: PeerAddressPolicy = defaultAddressPolicy,
    quicEnabled = false,
    interval = chronos.hours(1),
): Future[WakuNode] {.async.} =
  ## A started node with mix. It takes each address, for real dials on
  ## loopback. With the default `interval`, the pool loop runs once.
  let node = loopbackNode(port, quicEnabled)
  await node.mountMixWith(addressPolicy)
  node.wakuMix.pool.poolLoopInterval = interval
  await node.start()
  return node

proc discover*(
    node: WakuNode,
    addrs: seq[string],
    peerId = PeerId.init(generateSecp256k1Key()).tryGet(),
): PeerId {.discardable.} =
  ## Stores a discovery record with a new mix key, for a new or known peer.
  let keys = generateKeyPair().expect("mix key pair")
  let found = DiscoveredPeer(
    peerId: $peerId,
    addrs: addrs,
    services: @[DiscoveredService(id: MixProtocolID, data: @(keys.publicKey))],
  )
  node.peerManager.addPeer(found.toRemotePeerInfo().expect("discovered peer"))
  return peerId

proc bootnode*(address: string): MixNodePubInfo =
  let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
  let keys = generateKeyPair().expect("mix key pair")
  MixNodePubInfo(multiAddr: address & "/p2p/" & $peerId, pubKey: keys.publicKey)

proc peerId*(entry: MixNodePubInfo): PeerId =
  parsePeerInfo(entry.multiAddr).tryGet().peerId

proc deadBootnodes*(ports: varargs[int]): seq[MixNodePubInfo] =
  ## Bootstrap entries on loopback ports where no mix node listens.
  ports.mapIt(bootnode("/ip4/127.0.0.1/tcp/" & $it))

proc inPool*(node: WakuNode, peerId: PeerId): bool =
  node.wakuMix.nodePool.get(peerId).isSome()

proc hopOf*(node: WakuNode, peerId: PeerId): MultiAddress =
  node.wakuMix.nodePool.get(peerId).expect("pool entry").multiAddr
