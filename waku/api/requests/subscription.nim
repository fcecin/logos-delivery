{.push raises: [].}

## Waku API: kernel subscription broker types.
##
## Protocol-explicit subscribe/unsubscribe/is-subscribed surface owned by
## WakuSubscriptionManager. Relay (gossipsub): shard ops + content-topic ops.
## Edge (managed filter): content-topic only. Content-topic ops carry an
## optional shard: derived under auto-sharding, supplied under static sharding.
## Providers installed by startWakuSubscriptionManager.

import std/options
import brokers/[broker_context, request_broker]
import waku/waku_core/[topics/content_topic, topics/pubsub_topic]

# ---- Relay (gossipsub) ----

RequestBroker(sync):
  type RequestRelaySubscribeShard* = object
    subscribed*: bool

  proc signature(shard: PubsubTopic): Result[RequestRelaySubscribeShard, string]

RequestBroker(sync):
  type RequestRelayUnsubscribeShard* = object
    unsubscribed*: bool

  proc signature(shard: PubsubTopic): Result[RequestRelayUnsubscribeShard, string]

RequestBroker(sync):
  type RequestRelaySubscribeContentTopic* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestRelaySubscribeContentTopic, string]

RequestBroker(sync):
  type RequestRelayUnsubscribeContentTopic* = object
    unsubscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestRelayUnsubscribeContentTopic, string]

# ---- Edge (managed filter; content-topic only) ----

RequestBroker(sync):
  type RequestEdgeSubscribe* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestEdgeSubscribe, string]

RequestBroker(sync):
  type RequestEdgeUnsubscribe* = object
    unsubscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestEdgeUnsubscribe, string]

# ---- Read ops ----

# Is the content topic subscribed on the relay surface? shard optional:
# derived under auto-sharding, supplied under static/manual sharding.
RequestBroker(sync):
  type RequestIsRelaySubscribed* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestIsRelaySubscribed, string]

# Is the content topic subscribed on the edge surface?
RequestBroker(sync):
  type RequestIsEdgeSubscribed* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestIsEdgeSubscribed, string]

# Is the content topic subscribed on the node's primary surface? Default
# multiplexing: relay if mounted, else edge.
RequestBroker(sync):
  type RequestIsSubscribed* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic, shard: Option[PubsubTopic]
  ): Result[RequestIsSubscribed, string]

# Snapshot of every relay-subscribed shard with its content-topic interest set.
RequestBroker(sync):
  type RequestRelaySubscribedTopics* = object
    topics*: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]

# Snapshot of the node's primary-surface subscribed shards with their
# content-topic interest sets. Default multiplexing: relay if mounted, else edge.
RequestBroker(sync):
  type RequestSubscribedTopics* = object
    topics*: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]

# Snapshot of every edge-subscribed shard with its content-topic interest set.
RequestBroker(sync):
  type RequestEdgeSubscribedTopics* = object
    topics*: seq[tuple[shard: PubsubTopic, contentTopics: seq[ContentTopic]]]

{.pop.}
