{.push raises: [].}

import std/tables
import results, eth/p2p/discoveryv5/enr
import libp2p/[peerid, peerstore, switch]
import libp2p_mix/pool

import ../node/peer_manager/waku_peer_store, ../waku_enr/sharding

type
  MixPeerMetadata = object
    protocols: seq[string]
    enr: Record
    shards: seq[uint16]

  MixPeerMetadataStore* = ref object
    pool: MixNodePool
    switch: Switch
    peers: Table[PeerId, MixPeerMetadata]

proc copyPeer(self: MixPeerMetadataStore, peerId: PeerId) =
  if not self.pool.contains(peerId):
    return
  let store = self.switch.peerStore
  var metadata = self.peers.getOrDefault(peerId)
  let protocols = store[ProtoBook][peerId]
  if protocols.len > 0:
    if self.switch.isConnected(peerId):
      metadata.protocols = protocols
    else:
      for protocol in protocols:
        if protocol notin metadata.protocols:
          metadata.protocols.add(protocol)
  let record = store[ENRBook][peerId]
  if record.raw.len > 0:
    metadata.enr = record
  let shards = store[ShardBook][peerId]
  if shards.len > 0:
    metadata.shards = shards
  self.peers[peerId] = metadata

proc hasProtocol*(self: MixPeerMetadataStore, peerId: PeerId, protocol: string): bool =
  return protocol in self.peers.getOrDefault(peerId).protocols

proc hasShard*(
    self: MixPeerMetadataStore, peerId: PeerId, cluster, shard: uint16
): bool =
  let metadata = self.peers.getOrDefault(peerId)
  return metadata.enr.containsShard(cluster, shard) or shard in metadata.shards

proc new*(T: typedesc[MixPeerMetadataStore], pool: MixNodePool, switch: Switch): T =
  let self = T(pool: pool, switch: switch)
  let store = switch.peerStore
  let protocols = store[ProtoBook]
  let records = store[ENRBook]
  let shards = store[ShardBook]
  let onMetadata = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    if not pool.stopped:
      self.copyPeer(peerId)
  protocols.addHandler(onMetadata)
  records.addHandler(onMetadata)
  shards.addHandler(onMetadata)
  discard pool.addChangeHandler(
    proc(event: MixPoolEvent) {.gcsafe, raises: [].} =
      if event.kind == MixPoolEventKind.Removed:
        self.peers.del(event.peerId)
      elif event.known:
        self.copyPeer(event.peerId)
  )
  for peerId in pool.knownPeerIds():
    self.copyPeer(peerId)
  return self
