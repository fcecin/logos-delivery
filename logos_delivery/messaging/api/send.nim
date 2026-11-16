## Messaging layer API — send operation.
import results, chronos, chronicles

import logos_delivery/api/types
import logos_delivery/api/events/messaging_client_events
import logos_delivery/messaging/messaging_client
import logos_delivery/waku/waku
import logos_delivery/waku/waku_core
import logos_delivery/messaging/delivery_service/send_service
import logos_delivery/messaging/delivery_service/send_service/delivery_task

proc newTask(
    self: MessagingClient,
    envelope: MessageEnvelope,
    timestamp = getNowInNanosecondTime(),
): Result[DeliveryTask, SendError] =
  self.checkApiAvailability().isOkOr:
    return err(SendError.init(SendErrorKind.Other, error))
  let deliveryTask = DeliveryTask.new(envelope, self.waku.brokerCtx, timestamp).valueOr:
    error "Failed to create delivery task", error
    return err(
      SendError.init(SendErrorKind.Other, "Failed to create delivery task: " & error)
    )
  return ok(deliveryTask)

proc acceptSend(self: MessagingClient, envelope: MessageEnvelope): SendResult =
  ## The body of `send`. It is not async, so it cannot suspend.
  let deliveryTask = ?self.newTask(envelope)
  ?self.sendService.enqueue(deliveryTask)
  return ok(deliveryTask.msgHash)

proc send*(
    self: MessagingClient, envelope: MessageEnvelope
): Future[SendResult] {.async.} =
  ## Sets the timestamp of the message and returns the hash of the message.
  ## It refuses a message with a `SendError`, and then no event comes.
  ## It never suspends, so the caller gets the hash before any event, also over the FFI.
  ## The node then makes up to `MaxSendAttempts` attempts of the message, and
  ## the events with the hash of the message report the result.
  return self.acceptSend(envelope)

proc sendAttempt*(
    self: MessagingClient,
    envelope: MessageEnvelope,
    timestamp: Timestamp,
    transport: Transport,
    timeout: Duration,
    lightpushPeers = Opt.none(seq[RemotePeerInfo]),
    storePeers = Opt.none(seq[RemotePeerInfo]),
): Future[Result[SendAttempt, SendError]] {.async: (raises: [CancelledError]).} =
  ## Makes one attempt of a message over one transport, and emits no event.
  ## `envelope` and `timestamp` identify the message.
  ## A later attempt with the same two values sends the same message with the same RLN proof.
  ## A refused message gets a `SendError`, and the node then keeps nothing of it.
  let deliveryTask = ?self.newTask(envelope, timestamp)
  return await self.sendService.sendAttempt(
    deliveryTask, transport, timeout, lightpushPeers, storePeers
  )
