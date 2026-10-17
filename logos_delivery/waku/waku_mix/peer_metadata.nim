{.push raises: [].}

import std/tables
import eth/p2p/discoveryv5/enr
import libp2p/[peerid, peerstore, switch]
import libp2p_mix/managed_pool

import ../node/peer_manager/waku_peer_store, ../waku_enr/sharding

type
  MixPeerMetadata = object
    protocols: seq[string]
    enr: Record
    shards: seq[uint16]

  MixPeerMetadataStore* = ref object
    ## The protocols, the ENR and the shards of each pool member. The peer
    ## store can delete the entries of a peer at any time. This store keeps a
    ## copy for the exit choice, and deletes the copy when the member leaves
    ## the pool.
    pool: ManagedMixNodePool
    switch: Switch
    peers: Table[PeerId, MixPeerMetadata]

proc copyPeer(self: MixPeerMetadataStore, peerId: PeerId) =
  if peerId notin self.pool:
    return
  let store = self.switch.peerStore
  var metadata = self.peers.getOrDefault(peerId)
  let protocols = store[ProtoBook][peerId]
  if protocols.len > 0:
    if self.switch.isConnected(peerId):
      # Identify writes the full list on a connection, so it replaces the copy.
      metadata.protocols = protocols
    else:
      # After a delete, the book has only the protocols of a discovery record.
      for protocol in protocols:
        if protocol notin metadata.protocols:
          metadata.protocols.add(protocol)
  # After a peer store delete, the book can get a record that is older than the copy.
  let record = store[ENRBook][peerId]
  if record.raw.len > 0 and record.seqNum >= metadata.enr.seqNum:
    metadata.enr = record
  let shards = store[ShardBook][peerId]
  if shards.len > 0:
    metadata.shards = shards
  self.peers[peerId] = metadata

proc hasProtocol*(self: MixPeerMetadataStore, peerId: PeerId, protocol: string): bool =
  ## True when the last known protocols of the pool member have `protocol`.
  return protocol in self.peers.getOrDefault(peerId).protocols

proc hasShard*(
    self: MixPeerMetadataStore, peerId: PeerId, cluster, shard: uint16
): bool =
  ## True when the last known ENR or shards of the pool member have `shard` of
  ## `cluster`.
  let metadata = self.peers.getOrDefault(peerId)
  return metadata.enr.containsShard(cluster, shard) or shard in metadata.shards

proc new*(
    T: typedesc[MixPeerMetadataStore], pool: ManagedMixNodePool, switch: Switch
): T =
  let self = T(pool: pool, switch: switch)
  let store = switch.peerStore
  let onMetadata = proc(peerId: PeerId) {.gcsafe, raises: [].} =
    self.copyPeer(peerId)
  store[ProtoBook].addHandler(onMetadata)
  store[ENRBook].addHandler(onMetadata)
  store[ShardBook].addHandler(onMetadata)
  discard pool.addChangeHandler(
    proc(peerId: PeerId, present: bool) {.gcsafe, raises: [].} =
      if present:
        self.copyPeer(peerId)
      else:
        self.peers.del(peerId)
  )
  for peerId in pool.allPeerIds():
    self.copyPeer(peerId)
  return self
