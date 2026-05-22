{.push raises: [].}

import results
import std/options
import waku/waku_core/[topics/content_topic, topics/pubsub_topic]
import waku/api/requests/subscription as kernel_subscription_api
import messaging/messaging_client_type

proc unsubscribe*(
    client: MessagingClient, contentTopic: ContentTopic
): Result[void, string] =
  if client.isNil():
    return err("MessagingClient.unsubscribe: client is nil")
  if client.relayMounted:
    kernel_subscription_api.RequestRelayUnsubscribeContentTopic.request(
      client.brokerCtx, contentTopic, none[PubsubTopic]()
    ).isOkOr:
      return err(error)
  else:
    kernel_subscription_api.RequestEdgeUnsubscribe.request(
      client.brokerCtx, contentTopic, none[PubsubTopic]()
    ).isOkOr:
      return err(error)
  return ok()

{.pop.}
