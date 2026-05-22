{.push raises: [].}

## Messaging API broker request types.

import chronos
import brokers/[broker_context, request_broker]
import waku/waku_core/[topics/content_topic]
import ./types

# Subscribe to a content topic.
RequestBroker:
  type RequestMessagingSubscribe* = object
    subscribed*: bool

  proc signature(
    contentTopic: ContentTopic
  ): Future[Result[RequestMessagingSubscribe, string]]

# Unsubscribe from a content topic. Sync.
RequestBroker(sync):
  type RequestMessagingUnsubscribe* = object
    unsubscribed*: bool

  proc signature(
    contentTopic: ContentTopic
  ): Result[RequestMessagingUnsubscribe, string]

# Send a message. Returns a RequestId for tracking via message-lifecycle events.
RequestBroker:
  type RequestMessagingSend* = object
    requestId*: RequestId

  proc signature(
    envelope: MessageEnvelope
  ): Future[Result[RequestMessagingSend, string]]

{.pop.}
