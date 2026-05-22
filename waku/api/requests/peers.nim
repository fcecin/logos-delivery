{.push raises: [].}

## Waku API: Peer manager broker request types.

import std/options
import libp2p/peerid
import brokers/[broker_context, request_broker]
import waku/waku_core/[topics/pubsub_topic, peers]

# Select a single peer that supports the given protocol codec.
RequestBroker(sync):
  type RequestSelectPeer* = object
    peer*: Option[RemotePeerInfo]

  proc signature(
    proto: string, shard: Option[PubsubTopic]
  ): Result[RequestSelectPeer, string]

# Select all peers that support the given protocol codec.
RequestBroker(sync):
  type RequestSelectPeers* = object
    peers*: seq[RemotePeerInfo]

  proc signature(
    proto: string, shard: Option[PubsubTopic]
  ): Result[RequestSelectPeers, string]

# Check whether the given peerId is currently connected.
RequestBroker(sync):
  type RequestIsPeerConnected* = object
    connected*: bool

  proc signature(peerId: PeerId): Result[RequestIsPeerConnected, string]

{.pop.}
