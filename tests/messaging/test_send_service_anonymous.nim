{.used.}

## Each send event carries `anonymous`, the value of `isAnonymous` at its time.

import chronos, testutils/unittests, results
import
  logos_delivery/waku/waku,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakucore, wakunodeconf, send_service_fixtures]

type PropagatingProcessor = ref object of BaseSendProcessor
  ## An attempt that propagates at once, over mix or in the clear.
  overMix: bool

method deanonymizes(self: PropagatingProcessor): bool {.gcsafe.} =
  return not self.overMix

method isValidProcessor(
    self: PropagatingProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: PropagatingProcessor, task: DeliveryTask
): Future[void] {.async.} =
  if self.overMix:
    task.anonymized = true
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

type FailingProcessor = ref object of BaseSendProcessor
  ## An attempt that the service refuses, over mix or in the clear.
  overMix: bool

method deanonymizes(self: FailingProcessor): bool {.gcsafe.} =
  return not self.overMix

method isValidProcessor(self: FailingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: FailingProcessor, task: DeliveryTask): Future[void] {.async.} =
  if self.overMix:
    task.anonymized = true
  task.errorDesc = "refused"
  task.state = DeliveryState.FailedToDeliver

type UnattemptedFailProcessor = ref object of BaseSendProcessor
  ## A mix processor that fails a task before any attempt, as for a message too large.

method deanonymizes(self: UnattemptedFailProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: UnattemptedFailProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: UnattemptedFailProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.errorDesc = "too large"
  task.state = DeliveryState.FailedToDeliver

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

suite "SendService - the anonymous field of the send events":
  var waku {.threadvar.}: Waku
  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
  asyncTeardown:
    discard await waku.stop()

  proc newService(
      processor: BaseSendProcessor,
      manager =
        RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new"),
  ): SendService =
    return SendService
      .new(false, waku, manager, processor, retryInterval = chronos.milliseconds(100))
      .expect("SendService.new")

  asyncTest "a send over mix propagates as anonymous":
    let service = newService(PropagatingProcessor(overMix: true))
    service.startSendService()
    defer:
      await service.stopSendService()
    var anonymous: seq[bool]
    let listener = MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          anonymous.add(event.anonymous),
      )
      .expect("listen")
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)

    check service.enqueue(buildTask("over-mix")).isOk()

    checkUntilTimeout:
      anonymous.len == 1
    check anonymous == @[true]

  asyncTest "a send that a plain attempt deanonymized after its mix attempt is not anonymous":
    ## The mix attempt anonymized the task, and the plain attempt then deanonymized it.
    ## The event must not say anonymous.
    let mix = MixThenFallbackProcessor()
    mix.chain(PropagatingProcessor(overMix: false))
    let service = newService(mix)
    service.startSendService()
    defer:
      await service.stopSendService()
    var anonymous: seq[bool]
    let listener = MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          anonymous.add(event.anonymous),
      )
      .expect("listen")
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)
    let task = buildTask("mix-then-plain")

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      anonymous.len == 1
    check:
      task.anonymized
      task.deanonymized
      anonymous == @[false]

  asyncTest "the error event keeps the anonymity guarantee of the task":
    ## An app can read the `error` event as "nothing went out".
    ## A plain attempt after the mix attempt makes that wrong, and the field says so.
    ## A task that failed before any attempt has no anonymity guarantee either.
    var anonymous: seq[bool]
    let listener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          anonymous.add(event.anonymous),
      )
      .expect("listen")
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)
    let mixOnly = FailingProcessor(overMix: true)
    let mixThenPlain = MixThenFallbackProcessor()
    mixThenPlain.chain(FailingProcessor(overMix: false))
    for processor in [
      BaseSendProcessor(mixOnly),
      BaseSendProcessor(mixThenPlain),
      BaseSendProcessor(UnattemptedFailProcessor()),
    ]:
      let events = anonymous.len
      let service = newService(processor)
      service.startSendService()
      check service.enqueue(buildTask("refused-" & $events)).isOk()
      checkUntilTimeout:
        anonymous.len == events + 1
      await service.stopSendService()
    check anonymous == @[true, false, false]

  asyncTest "the queued event carries the anonymity of the task":
    ## An RLN proof refresh clears the admission of a task.
    ## A task that went out over mix can then wait for budget again.
    ## The test sets that state on the task, with and without a plain attempt.
    let manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1))
      .expect("RateLimitManager.new")
    let service = newService(PropagatingProcessor(overMix: true), manager)
    service.startSendService()
    defer:
      await service.stopSendService()
    var anonymous: seq[bool]
    let listener = MessageQueuedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageQueuedEvent) {.async: (raises: []).} =
          anonymous.add(event.anonymous),
      )
      .expect("listen")
    defer:
      await MessageQueuedEvent.dropListener(waku.brokerCtx, listener)
    proc unadmittedTask(id: string): DeliveryTask =
      let task = buildTask(id)
      task.firstAdmittedTime = Opt.none(Moment)
      return task

    let spender = unadmittedTask("spender")
    check service.enqueue(spender).isOk()
    checkUntilTimeout:
      spender.state == DeliveryState.SuccessfullyPropagated

    check service.enqueue(unadmittedTask("fresh")).isOk()
    checkUntilTimeout:
      anonymous.len == 1
    let refreshed = unadmittedTask("refreshed")
    refreshed.anonymized = true
    check service.enqueue(refreshed).isOk()
    checkUntilTimeout:
      anonymous.len == 2
    let deanonymized = unadmittedTask("deanonymized")
    deanonymized.anonymized = true
    deanonymized.deanonymized = true
    check service.enqueue(deanonymized).isOk()
    checkUntilTimeout:
      anonymous.len == 3

    check anonymous == @[false, true, false]
