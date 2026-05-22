{.push raises: [].}

import chronos, chronicles, results
import libp2p/crypto/crypto
import brokers/broker_context

import waku/waku_core
import waku/api/requests/protocols as protocols_api
import messaging/delivery_service/delivery_service
import messaging/api/types
import messaging/messaging_client_type

import messaging/api/api_subscribe
import messaging/api/api_unsubscribe
import messaging/api/api_send

import messaging/api/messaging as messaging_brokers

export messaging_client_type
export api_subscribe, api_unsubscribe, api_send

logScope:
  topics = "messaging-client"

proc registerMessagingApiProviders(
    client: MessagingClient
): Result[void, string] =
  ## Bind the messaging broker providers to the client API procs.
  messaging_brokers.RequestMessagingSubscribe.setProvider(
    client.brokerCtx,
    proc(
        contentTopic: ContentTopic
    ): Future[Result[messaging_brokers.RequestMessagingSubscribe, string]] {.async.} =
      (await subscribe(client, contentTopic)).isOkOr:
        return err(error)
      return ok(messaging_brokers.RequestMessagingSubscribe(subscribed: true)),
  ).isOkOr:
    return err("registerMessagingApiProviders: RequestMessagingSubscribe: " & error)

  messaging_brokers.RequestMessagingUnsubscribe.setProvider(
    client.brokerCtx,
    proc(
        contentTopic: ContentTopic
    ): Result[messaging_brokers.RequestMessagingUnsubscribe, string] =
      unsubscribe(client, contentTopic).isOkOr:
        return err(error)
      return ok(messaging_brokers.RequestMessagingUnsubscribe(unsubscribed: true)),
  ).isOkOr:
    return err("registerMessagingApiProviders: RequestMessagingUnsubscribe: " & error)

  messaging_brokers.RequestMessagingSend.setProvider(
    client.brokerCtx,
    proc(
        envelope: MessageEnvelope
    ): Future[Result[messaging_brokers.RequestMessagingSend, string]] {.async.} =
      let reqId = (await send(client, envelope)).valueOr:
        return err(error)
      return ok(messaging_brokers.RequestMessagingSend(requestId: reqId)),
  ).isOkOr:
    return err("registerMessagingApiProviders: RequestMessagingSend: " & error)

  ok()

proc new*(
    T: type MessagingClient, brokerCtx: BrokerContext, preferP2PReliability: bool
): MessagingClient =
  ## Construct a messaging-layer client bound to `brokerCtx`.
  MessagingClient(
    brokerCtx: brokerCtx,
    rng: crypto.newRng(),
    preferP2PReliability: preferP2PReliability,
  )

proc start*(self: MessagingClient): Future[Result[void, string]] {.async: (raises: []).} =
  ## Bring the messaging layer up.
  if self.isNil():
    return err("MessagingClient.start: client is nil")

  # Mounted protocols come from the kernel broker.
  let status = protocols_api.RequestProtocolMountStatus.request(self.brokerCtx).valueOr:
    return err("MessagingClient.start: protocol mount status query failed: " & error)
  self.relayMounted = status.relayMounted
  self.filterMounted = status.filterMounted

  self.deliveryService = DeliveryService.new(
    self.preferP2PReliability,
    self.brokerCtx,
    status.relayMounted,
    status.lightpushMounted,
    status.storeMounted,
  ).valueOr:
    return err("DeliveryService.new failed: " & error)

  self.deliveryService.startDeliveryService().isOkOr:
    return err("startDeliveryService failed: " & error)

  registerMessagingApiProviders(self).isOkOr:
    return err("registerMessagingApiProviders failed: " & error)
  ok()

proc stop*(self: MessagingClient): Future[Result[void, string]] {.async: (raises: []).} =
  ## Stop inner components and clear the messaging API providers.
  if self.isNil():
    return err("MessagingClient.stop: client is nil")

  if not self.deliveryService.isNil():
    try:
      await self.deliveryService.stopDeliveryService()
    except CatchableError as e:
      return err("stopDeliveryService raised: " & e.msg)

  messaging_brokers.RequestMessagingSubscribe.clearProvider(self.brokerCtx)
  messaging_brokers.RequestMessagingUnsubscribe.clearProvider(self.brokerCtx)
  messaging_brokers.RequestMessagingSend.clearProvider(self.brokerCtx)

  return ok()

{.pop.}
