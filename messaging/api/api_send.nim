{.push raises: [].}

import chronos, chronicles, results
import std/options
import stew/byteutils
import waku/waku_core
import waku/api/requests/subscription as kernel_subscription_api
import messaging/messaging_client_type
import messaging/delivery_service/delivery_service
import messaging/delivery_service/send_service
import messaging/delivery_service/send_service/delivery_task
import ./api_subscribe
import ./types

logScope:
  topics = "messaging-api send"

proc send*(
    client: MessagingClient, envelope: MessageEnvelope
): Future[Result[RequestId, string]] {.async: (raises: []).} =
  ## Send a message envelope. Auto-subscribes to the content topic if needed.
  ## Returns a RequestId for tracking via the message-lifecycle events.
  if client.isNil() or client.deliveryService.isNil():
    return err("MessagingClient.send: client/deliveryService is nil")

  let subR = kernel_subscription_api.RequestIsSubscribed.request(
    client.brokerCtx, envelope.contentTopic, none[PubsubTopic]()
  )
  let isSubbed = subR.isOk() and subR.get().subscribed
  if not isSubbed:
    info "Auto-subscribing to topic on send", contentTopic = envelope.contentTopic
    (await subscribe(client, envelope.contentTopic)).isOkOr:
      warn "Failed to auto-subscribe", error = error
      return err("Failed to auto-subscribe before sending: " & error)

  let requestId = RequestId.new(client.rng)
  let deliveryTask = DeliveryTask.new(
    requestId, envelope, client.brokerCtx
  ).valueOr:
    return err("MessagingClient.send: failed to create delivery task: " & error)

  info "MessagingClient.send: scheduling delivery task",
    requestId = $requestId,
    pubsubTopic = deliveryTask.pubsubTopic,
    contentTopic = deliveryTask.msg.contentTopic,
    msgHash = deliveryTask.msgHash.to0xHex()

  asyncSpawn client.deliveryService.sendService.send(deliveryTask)

  return ok(requestId)

{.pop.}
