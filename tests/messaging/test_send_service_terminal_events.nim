{.used.}

import chronos, testutils/unittests, results, stew/byteutils
import brokers/broker_context

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakunodeconf]

## The events a send ends with once it has propagated. A store node confirming
## the message is the durable milestone: `MessageSent`, then `MessageArchived`.
## A propagated message that no store node confirms within `MaxTimeInCache` is
## dropped from the task cache; with reliability on, the drop has to be
## reported, because a reliable channel finalises a send only on
## `MessageSent` or `MessageError`.

type ScriptedProcessor = ref object of BaseSendProcessor
  ## Stamps `outcome` on every task. A propagated outcome is dated before the
  ## store-validation window opened, so the window has elapsed on the first
  ## service tick.
  outcome: DeliveryState

method process(self: ScriptedProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.state = self.outcome
  if self.outcome == DeliveryState.SuccessfullyPropagated:
    task.firstPropagatedTime =
      Opt.some(Moment.now() - MaxTimeInCache - chronos.seconds(1))

type SendEvents = ref object
  ## The send events seen on the node's broker, in the order they arrived.
  sent: seq[MessageSentEvent]
  archived: seq[MessageArchivedEvent]
  errors: seq[MessageErrorEvent]
  order: seq[string]
  sentListener: MessageSentEventListener
  archivedListener: MessageArchivedEventListener
  errorListener: MessageErrorEventListener

proc listenSendEvents(brokerCtx: BrokerContext): SendEvents =
  let events = SendEvents()
  events.sentListener = MessageSentEvent
    .listen(
      brokerCtx,
      proc(event: MessageSentEvent) {.async: (raises: []).} =
        events.sent.add(event)
        events.order.add("sent"),
    )
    .expect("listen MessageSentEvent")
  events.archivedListener = MessageArchivedEvent
    .listen(
      brokerCtx,
      proc(event: MessageArchivedEvent) {.async: (raises: []).} =
        events.archived.add(event)
        events.order.add("archived"),
    )
    .expect("listen MessageArchivedEvent")
  events.errorListener = MessageErrorEvent
    .listen(
      brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        events.errors.add(event)
        events.order.add("error"),
    )
    .expect("listen MessageErrorEvent")
  return events

proc stop(events: SendEvents, brokerCtx: BrokerContext) {.async.} =
  await MessageSentEvent.dropListener(brokerCtx, events.sentListener)
  await MessageArchivedEvent.dropListener(brokerCtx, events.archivedListener)
  await MessageErrorEvent.dropListener(brokerCtx, events.errorListener)

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

suite "SendService - store validation outcomes":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc buildTask(id: string): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/store-validation/proto",
      payload: "hi".toBytes(),
      timestamp: 1_700_000_000_000_000_000,
    )
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    return DeliveryTask(
      requestId: RequestId(id),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.Entry,
    )

  proc newService(preferP2PReliability: bool, outcome: DeliveryState): SendService =
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService
      .new(preferP2PReliability, waku, manager, ScriptedProcessor(outcome: outcome))
      .expect("SendService.new")

  asyncTest "a store-confirmed message reports MessageSent, then MessageArchived":
    ## `MessageSent` is the terminal today's consumers key on; `MessageArchived`
    ## names what the confirmation is: the message is durable.
    let service = newService(true, DeliveryState.SuccessfullyValidated)
    let task = buildTask("store-confirmed")
    let events = listenSendEvents(waku.brokerCtx)
    defer:
      await events.stop(waku.brokerCtx)

    await service.send(task)
    await sleepAsync(chronos.milliseconds(50))

    check:
      events.order == @["sent", "archived"]
      events.errors.len == 0
    if events.sent.len == 1 and events.archived.len == 1:
      check:
        events.sent[0].requestId == task.requestId
        events.archived[0].requestId == task.requestId
        events.archived[0].messageHash == task.msgHash.to0xHex()

  asyncTest "a reliable send that no store node confirms ends with MessageError":
    ## The test node has a store client, so with reliability on the propagated
    ## task waits for store validation. No store peer ever confirms it. Once
    ## the window has elapsed the task is dropped, and the caller is told.
    let service = newService(true, DeliveryState.SuccessfullyPropagated)
    let task = buildTask("never-validated")
    let events = listenSendEvents(waku.brokerCtx)
    defer:
      await events.stop(waku.brokerCtx)

    await service.send(task)
    check task.state == DeliveryState.SuccessfullyPropagated

    # The first service tick evaluates the cache and drops the expired task.
    service.startSendService()
    let deadline = Moment.now() + chronos.seconds(5)
    while Moment.now() < deadline and events.errors.len == 0:
      await sleepAsync(chronos.milliseconds(10))
    await service.stopSendService()

    check:
      events.order == @["error"]
    if events.errors.len == 1:
      check:
        events.errors[0].requestId == task.requestId
        events.errors[0].messageHash == task.msgHash.to0xHex()
        events.errors[0].error.len > 0

  asyncTest "with reliability off a propagated task is dropped without a terminal":
    ## No store confirmation will follow, so the task is dropped as soon as it
    ## propagated, however old the propagation: no `MessageSent`, no
    ## `MessageArchived` and no `MessageError`. The timeout terminal is for
    ## reliable sends only.
    let service = newService(false, DeliveryState.SuccessfullyPropagated)
    let task = buildTask("reliability-off")
    let events = listenSendEvents(waku.brokerCtx)
    defer:
      await events.stop(waku.brokerCtx)

    await service.send(task)
    check task.state == DeliveryState.SuccessfullyPropagated

    service.startSendService()
    await sleepAsync(chronos.milliseconds(500))
    await service.stopSendService()

    check events.order.len == 0
