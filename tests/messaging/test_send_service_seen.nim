{.used.}

## A copy of the message from the network completes a mix send whose reply was
## lost. After the task is deanonymized, a copy tells nothing about the mix send.

import std/[deques, importutils]
import chronos, testutils/unittests, results, stew/byteutils
import
  logos_delivery/waku/[waku, waku_core],
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/events/[kernel_events, messaging_client_events],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakucore, wakunodeconf]

type LostReplyProcessor = ref object of BaseSendProcessor
  ## A mix attempt whose reply is lost, each time.
  calls: int

method isPlain(self: LostReplyProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: LostReplyProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: LostReplyProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.anonymized = true
  task.state = DeliveryState.NextRoundRetry

type WaitingReplyProcessor = ref object of BaseSendProcessor
  ## A mix attempt that waits for a reply that never comes.
  calls: int
  cancelled: int

method isPlain(self: WaitingReplyProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: WaitingReplyProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: WaitingReplyProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.anonymized = true
  try:
    await sleepAsync(chronos.hours(1))
  except CancelledError as exc:
    inc self.cancelled
    raise exc
  task.state = DeliveryState.NextRoundRetry

type SlotTestProcessor = ref object of BaseSendProcessor
  ## The task "blocker" holds the send slot until `release` fires. Each other
  ## task gets one mix attempt with no answer, then hands over to the plain path.
  release: AsyncEvent
  blockerStarted: bool

method isPlain(self: SlotTestProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: SlotTestProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: SlotTestProcessor, task: DeliveryTask): Future[void] {.async.} =
  if task.requestId == RequestId("blocker"):
    self.blockerStarted = true
    await self.release.wait()
    task.state = DeliveryState.NextRoundRetry
    return
  task.anonymized = true
  task.state =
    if task.tryCount == 0: DeliveryState.NextRoundRetry else: DeliveryState.FallbackRetry
  task.tryCount.inc()

type HandoverProcessor = ref object of BaseSendProcessor
  ## A mix attempt that hands the task to the plain path at once.

method isPlain(self: HandoverProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: HandoverProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: HandoverProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.anonymized = true
  task.state = DeliveryState.FallbackRetry

type NeverValidProcessor = ref object of BaseSendProcessor
  ## No processor attempts the task, as when no peer serves it. `asked` counts
  ## the passes of the service.
  asked: int

method isValidProcessor(
    self: NeverValidProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  inc self.asked
  return false

type PlainRetryProcessor = ref object of BaseSendProcessor
  ## A plain send that leaves the task for a retry, each time.
  calls: int

method isValidProcessor(
    self: PlainRetryProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(self: PlainRetryProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.NextRoundRetry

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc buildTask(id: string): DeliveryTask =
  ## An admitted task with no RLN. The id is the payload, so two tasks never
  ## have one hash.
  let msg = WakuMessage(
    contentTopic: "/test/1/seen/proto",
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
    firstAdmittedTime: Opt.some(Moment.now()),
  )

proc emitSeen(waku: Waku, task: DeliveryTask) =
  ## The network gives this node the message of `task`.
  MessageSeenEvent.emit(waku.brokerCtx, task.pubsubTopic, task.msg)

suite "SendService - a message seen on the network":
  var waku {.threadvar.}: Waku
  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
  asyncTeardown:
    discard await waku.stop()

  proc newService(
      processor: BaseSendProcessor,
      maxSendsInFlight = MaxSendsInFlight,
      retryInterval = chronos.milliseconds(100),
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
        retryInterval = retryInterval,
        maxSendsInFlight = maxSendsInFlight,
      )
      .expect("SendService.new")
    service.startSendService()
    return service

  asyncTest "a message seen on the network completes a mix send whose reply was lost":
    let mix = LostReplyProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    var propagated = 0
    let listener = MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          propagated.inc(),
      )
      .expect("listen")
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)
    let task = buildTask("lost-reply")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls >= 1

    waku.emitSeen(task)

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      task.firstPropagatedTime.isSome()
    checkUntilTimeout:
      propagated == 1

  asyncTest "a message seen during an attempt ends the attempt at once":
    ## The attempt waits for the reply of the exit. The copy from the network
    ## says that the exit published, so the wait has nothing to add.
    let mix = WaitingReplyProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let task = buildTask("seen-during-attempt")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls == 1

    waku.emitSeen(task)

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      mix.cancelled == 1
      task.seenWaiter.isNil()

  asyncTest "a message seen before any attempt does not complete the task":
    let never = NeverValidProcessor()
    let service = newService(never)
    defer:
      await service.stopSendService()
    let task = buildTask("no-attempt")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry

    waku.emitSeen(task)
    let before = never.asked

    checkUntilTimeout:
      never.asked >= before + 3
    check:
      not task.seenOnNetwork
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a message seen after the task is deanonymized does not complete it as a mix send":
    ## A plain send can publish the message in the clear. The task gets
    ## `deanonymized` before the attempt, whatever the attempt does.
    let mix = HandoverProcessor()
    let plain = PlainRetryProcessor()
    mix.chain(plain)
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let task = buildTask("plain-after-mix")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      plain.calls >= 1
    check:
      task.anonymized
      task.deanonymized

    waku.emitSeen(task)

    checkUntilTimeout:
      plain.calls >= 3
    check:
      not task.seenOnNetwork
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a copy that arrives while the task waits for a send slot stops its hand-over to the plain path":
    ## The plain path would publish in the clear a message that the network has.
    privateAccess(SendService)
    privateAccess(AsyncSemaphore)
    let mix = SlotTestProcessor(release: newAsyncEvent())
    let plain = PlainRetryProcessor()
    mix.chain(plain)
    # The test drives the retries, so that the blocker takes the slot before the
    # second attempt of the task.
    let service =
      newService(mix, maxSendsInFlight = 1, retryInterval = chronos.hours(1))
    defer:
      await service.stopSendService()
    let task = buildTask("waits-for-a-slot")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.tryCount == 1
    check service.enqueue(buildTask("blocker")).isOk()
    checkUntilTimeout:
      mix.blockerStarted
    service.retryTick.fire()
    service.retryTick.clear()
    checkUntilTimeout:
      service.sendSlots.waiters.len == 1

    waku.emitSeen(task)
    mix.release.fire()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      plain.calls == 0
      not task.deanonymized

  asyncTest "a stopped service does not read the network, and a restart reads it again":
    let mix = LostReplyProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let task = buildTask("stopped")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls >= 1
    await service.stopSendService()

    waku.emitSeen(task)
    service.startSendService()

    # A second attempt shows that `completeIfSeen` found no `seenOnNetwork` after
    # the restart.
    checkUntilTimeout:
      mix.calls >= 2
    check not task.seenOnNetwork

    waku.emitSeen(task)

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
