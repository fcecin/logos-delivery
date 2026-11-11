## Messaging layer API — send operation.
import results, chronos, chronicles

import logos_delivery/api/types
import logos_delivery/api/events/messaging_client_events
import logos_delivery/messaging/messaging_client
import logos_delivery/waku/waku
import logos_delivery/waku/api/subscriptions
import logos_delivery/messaging/delivery_service/send_service
import logos_delivery/messaging/delivery_service/send_service/delivery_task

proc acceptSend(self: MessagingClient, envelope: MessageEnvelope): SendResult =
  ## The body of `send`. It is not async, so it cannot suspend.
  self.checkApiAvailability().isOkOr:
    return err(SendError.init(SendErrorKind.Other, error))
  # The task sets the message timestamp, and so the message hash.
  let deliveryTask = DeliveryTask.new(envelope, self.waku.brokerCtx).valueOr:
    error "Failed to create delivery task", error
    return err(
      SendError.init(
        SendErrorKind.Other,
        "MessagingClient.send: Failed to create delivery task: " & error,
      )
    )
  let msgHash = deliveryTask.msgHash

  # A refused send gets no subscribe.
  self.sendService.checkAcceptable(deliveryTask).isOkOr:
    debug "Send refused", msgHash = msgHash.to0xHex(), error = $error
    return err(error)
  if self.sendService.subscribesOnSend():
    let isSubbed = self.waku.isSubscribed(envelope.contentTopic).valueOr(false)
    if not isSubbed:
      debug "Auto-subscribing to topic on send", contentTopic = envelope.contentTopic
      self.waku.subscribe(envelope.contentTopic).isOkOr:
        error "Failed to auto-subscribe", error, msgHash = msgHash.to0xHex()
        return err(
          SendError.init(
            SendErrorKind.Other, "Failed to auto-subscribe before sending: " & error
          )
        )

  ?self.sendService.accept(deliveryTask)
  return ok(msgHash)

proc send*(
    self: MessagingClient, envelope: MessageEnvelope
): Future[SendResult] {.async.} =
  ## Sets the timestamp of the message and returns the hash of the message.
  ## It refuses a message with a `SendError`, and then no event comes.
  ## At the `None` level, it subscribes to the content topic.
  ## It never suspends, so the caller gets the hash before any event, also over the FFI.
  return self.acceptSend(envelope)
