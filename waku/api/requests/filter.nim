{.push raises: [].}

## Waku API: Filter v2 broker request types.

import std/options
import chronos
import brokers/[broker_context, request_broker]
import waku/waku_core/[topics/content_topic, topics/pubsub_topic, peers]
import waku/waku_filter_v2/common

export FilterSubscribeErrorKind

RequestBroker:
  type RequestFilterSubscribe* = object
    subscribed*: bool
    subscribeError*: Option[FilterSubscribeErrorKind]
    errorDesc*: string

  proc signature(
    servicePeer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    contentTopics: seq[ContentTopic],
  ): Future[Result[RequestFilterSubscribe, string]]

RequestBroker:
  type RequestFilterUnsubscribe* = object
    unsubscribed*: bool
    subscribeError*: Option[FilterSubscribeErrorKind]
    errorDesc*: string

  proc signature(
    servicePeer: RemotePeerInfo,
    pubsubTopic: PubsubTopic,
    contentTopics: seq[ContentTopic],
  ): Future[Result[RequestFilterUnsubscribe, string]]

RequestBroker:
  type RequestFilterPing* = object
    pingOk*: bool
    subscribeError*: Option[FilterSubscribeErrorKind]
    errorDesc*: string

  proc signature(
    servicePeer: RemotePeerInfo, timeout: Duration
  ): Future[Result[RequestFilterPing, string]]

{.pop.}
