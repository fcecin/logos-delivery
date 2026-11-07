{.used.}

## A task whose mix request left with no answer never ends with an error, and
## the end of its window ends it with no further request.

import chronos, testutils/unittests, results, stew/byteutils
import
  logos_delivery/waku/[waku, waku_core],
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakucore, wakunodeconf]

type FailingUnansweredProcessor = ref object of BaseSendProcessor
  ## A mix attempt whose request left with no answer, and that then fails, as a
  ## new RLN proof that cannot be made does.

method isPlain(self: FailingUnansweredProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: FailingUnansweredProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: FailingUnansweredProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.anonymized = true
  task.mixUnanswered = true
  task.state = DeliveryState.FailedToDeliver
  task.errorDesc = "no new proof"

type ConfirmedThenHeldProcessor = ref object of BaseSendProcessor
  ## A mix attempt whose confirmation comes, and that a stop then cuts.
  calls: int

method isPlain(self: ConfirmedThenHeldProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: ConfirmedThenHeldProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: ConfirmedThenHeldProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.anonymized = true
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())
  await sleepAsync(chronos.hours(1))

type CountingProcessor = ref object of BaseSendProcessor ## A mix attempt with no answer.
  calls: int

method isPlain(self: CountingProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: CountingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: CountingProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.NextRoundRetry

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc buildTask(id: string, admitted = Moment.now()): DeliveryTask =
  ## An admitted task with no RLN. The id is the payload, so two tasks never
  ## have one hash.
  let msg = WakuMessage(
    contentTopic: "/test/1/unconfirmed/proto",
    payload: id.toBytes(),
    timestamp: 1_700_000_000_000_000_000,
  )
  let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  return DeliveryTask(
    requestId: RequestId(id),
    pubsubTopic: pubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(pubsubTopic, msg),
    state: DeliveryState.Entry,
    firstAdmittedTime: Opt.some(admitted),
  )

type SendEvents = ref object
  propagated, unconfirmed, errors: int
  unconfirmedAnonymous: seq[bool]
  propagatedListener: MessagePropagatedEventListener
  unconfirmedListener: MessageUnconfirmedEventListener
  errorListener: MessageErrorEventListener

suite "SendService - a task whose mix request got no answer":
  var waku {.threadvar.}: Waku
  var events {.threadvar.}: SendEvents
  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    events = SendEvents()
    let sendEvents = events
    events.propagatedListener = MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          sendEvents.propagated.inc(),
      )
      .expect("listen")
    events.unconfirmedListener = MessageUnconfirmedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageUnconfirmedEvent) {.async: (raises: []).} =
          sendEvents.unconfirmed.inc()
          sendEvents.unconfirmedAnonymous.add(event.anonymous),
      )
      .expect("listen")
    events.errorListener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          sendEvents.errors.inc(),
      )
      .expect("listen")
  asyncTeardown:
    await MessagePropagatedEvent.dropListener(waku.brokerCtx, events.propagatedListener)
    await MessageUnconfirmedEvent.dropListener(
      waku.brokerCtx, events.unconfirmedListener
    )
    await MessageErrorEvent.dropListener(waku.brokerCtx, events.errorListener)
    discard await waku.stop()

  proc newService(processor: BaseSendProcessor): SendService =
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(
        false,
        waku,
        manager,
        processor,
        AnonymityLevel.Required,
        retryInterval = chronos.milliseconds(100),
      )
      .expect("SendService.new")
    service.startSendService()
    return service

  asyncTest "an unanswered task that fails ends unconfirmed, not with an error":
    let service = newService(FailingUnansweredProcessor())
    defer:
      await service.stopSendService()

    check service.enqueue(buildTask("fails-unanswered")).isOk()

    checkUntilTimeout:
      events.unconfirmed == 1
    check events.errors == 0

  asyncTest "a confirmation that came before a stop ends the task after the restart":
    let mix = ConfirmedThenHeldProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let task = buildTask("confirmed-then-stopped")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls == 1
    await service.stopSendService()

    service.startSendService()

    checkUntilTimeout:
      events.propagated == 1
    check mix.calls == 1

  asyncTest "an unanswered task past its window ends with no new request":
    let mix = CountingProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let task = buildTask(
      "unanswered-past-its-window", Moment.now() - service.maxDeliveryTime * 2
    )
    task.anonymized = true
    task.mixUnanswered = true

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      events.unconfirmed == 1
    check mix.calls == 0

  asyncTest "the unconfirmed end of a deanonymized send does not say anonymous":
    let service = newService(CountingProcessor())
    defer:
      await service.stopSendService()
    let mixOnly = buildTask("mix-only", Moment.now() - service.maxDeliveryTime * 2)
    let plainFirst =
      buildTask("plain-first", Moment.now() - service.maxDeliveryTime * 2)
    for task in [mixOnly, plainFirst]:
      task.anonymized = true
      task.mixUnanswered = true
    plainFirst.deanonymized = true

    check service.enqueue(mixOnly).isOk()
    checkUntilTimeout:
      events.unconfirmed == 1
    check service.enqueue(plainFirst).isOk()
    checkUntilTimeout:
      events.unconfirmed == 2
    check events.unconfirmedAnonymous == @[true, false]
