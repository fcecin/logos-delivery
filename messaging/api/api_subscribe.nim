{.push raises: [].}

import chronos, results
import std/options
import waku/waku_core/[topics/content_topic, topics/pubsub_topic]
import waku/api/requests/subscription as kernel_subscription_api
import messaging/messaging_client_type

proc subscribe*(
    client: MessagingClient, contentTopic: ContentTopic
): Future[Result[void, string]] {.async: (raises: []).} =
  if client.isNil():
    return err("MessagingClient.subscribe: client is nil")
  if client.relayMounted:
    kernel_subscription_api.RequestRelaySubscribeContentTopic.request(
      client.brokerCtx, contentTopic, none[PubsubTopic]()
    ).isOkOr:
      return err(error)
  else:
    kernel_subscription_api.RequestEdgeSubscribe.request(
      client.brokerCtx, contentTopic, none[PubsubTopic]()
    ).isOkOr:
      return err(error)
  return ok()

{.pop.}
