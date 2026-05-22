## This module helps to ensure the correct transmission and reception of messages

import results
import chronos, chronicles
import brokers/broker_context
import ./recv_service, ./send_service

type DeliveryService* = ref object
  sendService*: SendService
  recvService*: RecvService

proc new*(
    T: type DeliveryService,
    useP2PReliability: bool,
    brokerCtx: BrokerContext,
    relayMounted: bool,
    lightpushMounted: bool,
    storeMounted: bool,
): Result[T, string] =
  let sendService = ?SendService.new(
    useP2PReliability, brokerCtx, relayMounted, lightpushMounted, storeMounted
  )
  let recvService = RecvService.new(brokerCtx)

  return ok(
    DeliveryService(
      sendService: sendService,
      recvService: recvService,
    )
  )

proc startDeliveryService*(self: DeliveryService): Result[void, string] =
  self.recvService.startRecvService()
  self.sendService.startSendService()
  return ok()

proc stopDeliveryService*(self: DeliveryService) {.async.} =
  await self.sendService.stopSendService()
  await self.recvService.stopRecvService()
