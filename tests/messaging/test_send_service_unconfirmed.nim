{.used.}

## A task that the node sent with no answer never ends with an error.
## The end of its window ends it with no further send.

import std/importutils
import chronos, testutils/unittests, results
import
  logos_delivery/waku/waku,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/node/waku_node/lightpush,
  logos_delivery/waku/waku_lightpush/[client, common],
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/events/[kernel_events, messaging_client_events],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakucore, wakunodeconf, send_service_fixtures]

type CountingProcessor = ref object of BaseSendProcessor ## Counts its attempts.
  calls: int

method deanonymizes(self: CountingProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: CountingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: CountingProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.NextRoundRetry

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

type SendEvents = ref object
  propagated, unconfirmed, errors: int
  propagatedListener: MessagePropagatedEventListener
  unconfirmedListener: MessageUnconfirmedEventListener
  errorListener: MessageErrorEventListener

suite "SendService - a task sent with no answer":
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
          sendEvents.unconfirmed.inc(),
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

  proc newService(
      processor: BaseSendProcessor, maxSendsInFlight = MaxSendsInFlight
  ): SendService =
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
        maxSendsInFlight = maxSendsInFlight,
      )
      .expect("SendService.new")
    service.startSendService()
    return service

  asyncTest "an unanswered task whose window ended ends unconfirmed with no send slot":
    ## The task sends no more, so it needs no send slot.
    ## The test keeps the only slot.
    privateAccess(SendService)
    let processor = CountingProcessor()
    let service = newService(processor, maxSendsInFlight = 1)
    defer:
      await service.stopSendService()
    await service.sendSlots.acquire()
    defer:
      service.sendSlots.release2()
    let task = buildTask("ended", admitted = Moment.now() - service.maxDeliveryTime * 2)
    task.anonymized = true
    task.markSendFinished(unanswered = true)

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      events.unconfirmed == 1
    check processor.calls == 0

  asyncTest "an unanswered task whose window ends during the wait for a send slot sends no more":
    privateAccess(SendService)
    privateAccess(AsyncSemaphore)
    let mix = SlotProcessor(finish: newAsyncEvent())
    let service = newService(mix, maxSendsInFlight = 1)
    defer:
      await service.stopSendService()
    check service.enqueue(buildTask("blocker")).isOk()
    checkUntilTimeout:
      mix.blockerStarted
    # The window of this task ends 300 ms from now, while it waits for the slot.
    let late = buildTask(
      "late", admitted = Moment.now() - DeliveryWindow + chronos.milliseconds(300)
    )
    late.anonymized = true
    late.markSendFinished(unanswered = true)
    check service.enqueue(late).isOk()
    checkUntilTimeout:
      service.sendSlots.waiters.len == 1
    checkUntilTimeout:
      late.isDeliveryTimedOut(DeliveryWindow)

    mix.finish.fire()

    checkUntilTimeout:
      events.unconfirmed == 1
    check mix.others == 0

  asyncTest "a copy during the wait for a send slot at the end of the window completes the send":
    ## The copy comes before the window ends, so the send propagates.
    privateAccess(SendService)
    privateAccess(AsyncSemaphore)
    let mix = SlotProcessor(finish: newAsyncEvent())
    let service = newService(mix, maxSendsInFlight = 1)
    defer:
      await service.stopSendService()
    check service.enqueue(buildTask("blocker")).isOk()
    checkUntilTimeout:
      mix.blockerStarted
    # The window of this task ends 300 ms from now, while it waits for the slot.
    let late = buildTask(
      "late", admitted = Moment.now() - DeliveryWindow + chronos.milliseconds(300)
    )
    late.anonymized = true
    late.markSendFinished(unanswered = true)
    check service.enqueue(late).isOk()
    checkUntilTimeout:
      service.sendSlots.waiters.len == 1
    # The copy comes while the task waits for the slot, then the window ends.
    check await waku.receiveFromNetwork(late)
    checkUntilTimeout:
      late.isDeliveryTimedOut(DeliveryWindow)

    mix.finish.fire()

    checkUntilTimeout:
      events.propagated == 1
    check:
      events.unconfirmed == 0
      mix.others == 0

suite "Mix publish results with no answer":
  test "a publish with no answer that the node can read is unanswered":
    ## The network can have the message after each of these results.
    check lighpushErrorResult(LightPushErrorCode.SERVICE_NOT_AVAILABLE, MixTimedOutDesc).error
      .isUnanswered()
    for desc in [
      ReadResponseFailedDesc & ": stream reset", ResponseNotDecodedDesc,
      RequestIdMismatchDesc,
    ]:
      check lightpushResultInternalError(desc).error.isUnanswered()
    check not lighpushErrorResult(LightPushErrorCode.INVALID_MESSAGE, "refused").error
      .isUnanswered()
    # The code of the producer comes with each text.
    check not lighpushErrorResult(
      LightPushErrorCode.INVALID_MESSAGE, RequestIdMismatchDesc
    ).error
      .isUnanswered()
