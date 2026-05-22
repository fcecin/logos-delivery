{.push raises: [].}

## Waku API: Lightpush broker request types.

import std/options
import chronos
import brokers/[broker_context, request_broker]
import waku/waku_core/[message/message, topics/pubsub_topic, peers]
import waku/waku_lightpush/rpc  # LightPushStatusCode

export LightPushStatusCode

# Publish a WakuMessage on a pubsub topic via lightpush to the supplied peer.
RequestBroker:
  type RequestLightpushPublish* = object
    relayedPeerCount*: uint32
    publishError*: Option[LightPushStatusCode]
    errorDesc*: string

  proc signature(
    peer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    wakuMessage: WakuMessage,
  ): Future[Result[RequestLightpushPublish, string]]

{.pop.}
