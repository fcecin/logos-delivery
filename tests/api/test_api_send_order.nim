{.used.}

import results
import chronos, testutils/unittests
import brokers/broker_context
import ../testlib/[futures, testasync, wakunodeconf]
import logos_delivery
import logos_delivery/api/conf/messaging_conf
import logos_delivery/api/events/messaging_client_events
import logos_delivery/api/messaging_client_api
import logos_delivery/messaging/delivery_service/send_service
import logos_delivery/waku/api/subscriptions

## The messaging API returns the request id before any event of the request.
## `MessagingClient.send` does not suspend, so each caller gets the id in its own
## call stack. The send service makes the first attempt after `send` returns.
## The node has a budget of 0, so each send emits an event with no
## network call. A durable message gets `MessageQueuedEvent`, and an ephemeral
## one gets `MessageErrorEvent`. An event before its id would show.

const OrderTopic = ContentTopic("/test/1/send-order/proto")

type OrderLog = ref object
  ids: seq[RequestId] ## The ids that the callers got, in order.
  events: seq[RequestId]
  early: seq[RequestId] ## The events for an id that no caller had yet.

proc record(log: OrderLog, id: RequestId) =
  log.events.add(id)
  if id notin log.ids:
    log.early.add(id)

proc keepId(log: OrderLog, res: Result[RequestId, string]) =
  ## Keeps the id as soon as the caller has it. A refused send keeps an empty
  ## id, which the test then finds.
  log.ids.add(res.get(RequestId("")))

proc noBudgetNode(capacity: uint): Future[LogosDelivery] {.async.} =
  ## A started node whose rate limit admits no message.
  var node: LogosDelivery
  lockNewGlobalBrokerContext:
    node = (
      await LogosDelivery.new(
        KernelConf(defaultTestNodeConf().kernel),
        MessagingClientConf(
          rateLimitEnabled: Opt.some(true),
          rateLimitMessagesPerEpoch: Opt.some(0'u64),
          sendQueueCapacity: Opt.some(capacity),
        ),
        ReliableChannelManagerConf(),
      )
    ).expect("LogosDelivery.new")
    (await node.start()).expect("start")
  return node

suite "Messaging API - the request id comes before the events":
  asyncTest "send and the MessagingSend provider return a finished future":
    ## The future is finished when the call returns, so no turn of the event
    ## loop can run between the enqueue and the caller. The task is in the cache
    ## at once.
    let node = await noBudgetNode(capacity = 4)
    defer:
      (await node.stop()).expect("stop")
    let client = node.messagingClient

    let direct = client.send(MessageEnvelope.init(OrderTopic, "direct"))
    let viaBroker = MessagingSend.request(
      node.waku.brokerCtx, MessageEnvelope.init(OrderTopic, "broker")
    )
    check:
      direct.finished()
      viaBroker.finished()
    if direct.finished() and viaBroker.finished():
      check:
        direct.read().isOk()
        viaBroker.read().isOk()

    # The node has room for 4 tasks, the 2 sends above and 2 more.
    for i in 0 ..< 2:
      check (await client.send(MessageEnvelope.init(OrderTopic, "fill-" & $i))).isOk()
    check:
      client.sendService.isFull()
      (await client.send(MessageEnvelope.init(OrderTopic, "over"))).errorOr("accepted") ==
        "Send queue full, retry later"

  asyncTest "a send that the send service refuses does not subscribe its content topic":
    ## `send` asks the send service before the auto-subscribe. So a refused send
    ## leaves no subscription.
    let node = await noBudgetNode(capacity = 1)
    defer:
      (await node.stop()).expect("stop")
    let client = node.messagingClient
    const fullTopic = ContentTopic("/test/1/send-order-full/proto")
    const stoppedTopic = ContentTopic("/test/1/send-order-stopped/proto")

    check:
      (await client.send(MessageEnvelope.init(OrderTopic, "fill"))).isOk()
      (await client.send(MessageEnvelope.init(fullTopic, "over"))).errorOr("accepted") ==
        "Send queue full, retry later"

    await client.sendService.stopSendService()
    check:
      (await client.send(MessageEnvelope.init(stoppedTopic, "late"))).errorOr(
        "accepted"
      ) == "Send service is stopped"
      node.waku.isSubscribed(OrderTopic).valueOr(false)
      not node.waku.isSubscribed(fullTopic).valueOr(true)
      not node.waku.isSubscribed(stoppedTopic).valueOr(true)

  asyncTest "no event comes before its request id, durable and ephemeral":
    let node = await noBudgetNode(capacity = 100)
    defer:
      (await node.stop()).expect("stop")
    let client = node.messagingClient
    let brokerCtx = node.waku.brokerCtx

    let log = OrderLog()
    let queuedListener = MessageQueuedEvent
      .listen(
        brokerCtx,
        proc(event: MessageQueuedEvent) {.async: (raises: []).} =
          log.record(event.requestId),
      )
      .expect("listen queued")
    let errorListener = MessageErrorEvent
      .listen(
        brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          log.record(event.requestId),
      )
      .expect("listen error")
    defer:
      await MessageQueuedEvent.dropListener(brokerCtx, queuedListener)
      await MessageErrorEvent.dropListener(brokerCtx, errorListener)

    # Each batch sends 4 messages, two through the client and two through the
    # broker, and waits for their events. A durable task parks, and an ephemeral
    # task fails.
    for batch in 0 ..< 3:
      proc envelope(kind: string, ephemeral = false): MessageEnvelope =
        MessageEnvelope.init(OrderTopic, kind & "-" & $batch, ephemeral = ephemeral)

      log.keepId(await client.send(envelope("durable")))
      log.keepId(await client.send(envelope("ephemeral", ephemeral = true)))
      log.keepId(await MessagingSend.request(brokerCtx, envelope("broker-durable")))
      log.keepId(
        await MessagingSend.request(
          brokerCtx, envelope("broker-ephemeral", ephemeral = true)
        )
      )
      checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, chronos.milliseconds(5)):
        log.events.len == log.ids.len

    check:
      log.ids.len == 12
      RequestId("") notin log.ids # each send got an id
      log.early.len == 0
