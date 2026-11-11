{.used.}

## A copy of the message of a queued send sets `networkHasMessage` on its task.
## A mix attempt reads it as its answer.

import std/importutils
import chronos, testutils/unittests, results
import
  logos_delivery/waku/waku,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/events/[kernel_events, messaging_client_events],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakucore, wakunodeconf, send_service_fixtures]

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

type WaitingPlainProcessor = ref object of BaseSendProcessor
  ## A plain send that waits until `finish` fires.
  started: bool
  finish: AsyncEvent

method isValidProcessor(
    self: WaitingPlainProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: WaitingPlainProcessor, task: DeliveryTask
): Future[void] {.async.} =
  self.started = true
  await self.finish.wait()
  task.state = DeliveryState.NextRoundRetry

type WaitingMixProcessor = ref object of BaseSendProcessor
  ## A mix attempt that waits for an answer that never comes, until a stop cuts it.
  calls: int

method deanonymizes(self: WaitingMixProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: WaitingMixProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(self: WaitingMixProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.anonymized = true
  await newFuture[void]("an answer that never comes")

type NeverValidProcessor = ref object of BaseSendProcessor
  ## No processor attempts the task, as when no peer serves it.
  ## `asked` counts the passes.
  asked: int

method isValidProcessor(
    self: NeverValidProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  inc self.asked
  return false

type CopyThenFailProcessor = ref object of BaseSendProcessor
  ## A mix attempt that fails just after the network gives this node the message.
  waku: Waku

method deanonymizes(self: CopyThenFailProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: CopyThenFailProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: CopyThenFailProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.anonymized = true
  MessageSeenEvent.emit(self.waku.brokerCtx, task.pubsubTopic, task.msg)
  task.errorDesc = "refused"
  task.state = DeliveryState.FailedToDeliver

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

suite "SendService - the network has the message":
  var waku {.threadvar.}: Waku
  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
  asyncTeardown:
    discard await waku.stop()

  proc newService(
      processor: BaseSendProcessor,
      level = AnonymityLevel.Required,
      storeConfirmation = false,
      retryInterval = chronos.milliseconds(100),
      maxSendsInFlight = MaxSendsInFlight,
  ): SendService =
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService
      .new(
        storeConfirmation,
        waku,
        manager,
        processor,
        level,
        retryInterval = retryInterval,
        maxSendsInFlight = maxSendsInFlight,
      )
      .expect("SendService.new")

  asyncTest "a copy of the message from the network marks its task whatever path sent it":
    ## A copy shows that the network has the message, also for a send in the clear.
    let plain = PlainRetryProcessor()
    let service = newService(plain, AnonymityLevel.None)
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("plain-copy")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      plain.calls >= 1

    check await waku.receiveFromNetwork(task)

    check task.networkHasMessage()

  asyncTest "a copy during an attempt that a stop cuts ends the task at the restart with no send slot":
    ## The task has its answer, so it needs no send slot.
    ## The test keeps the only slot.
    privateAccess(SendService)
    let mix = WaitingMixProcessor()
    let service = newService(mix, maxSendsInFlight = 1)
    service.startSendService()
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
    let task = buildTask("copy-then-stop")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls == 1
    check await waku.receiveFromNetwork(task)
    await service.stopSendService()
    await service.sendSlots.acquire()
    defer:
      service.sendSlots.release2()

    service.startSendService()

    checkUntilTimeout:
      propagated == 1
    check mix.calls == 1

  asyncTest "a cancelled wait for the network leaves the task able to learn that the network has it":
    ## A send that stops its wait must not stop the next wait of the same task.
    let task = buildTask("cancelled-wait")
    let wait = task.waitForNetworkHasMessage()
    await wait.cancelAndWait()

    task.setNetworkHasMessage()

    check:
      task.networkHasMessage()
      task.waitForNetworkHasMessage().completed()

  asyncTest "a plain attempt deanonymizes the task before it runs":
    ## A plain attempt can show the address of this node before it ends.
    let mix = MixThenFallbackProcessor()
    let plain = WaitingPlainProcessor(finish: newAsyncEvent())
    mix.chain(plain)
    let service = newService(mix, AnonymityLevel.Preferred)
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("plain-attempt")
    check service.enqueue(task).isOk()

    checkUntilTimeout:
      plain.started
    check task.deanonymized
    plain.finish.fire()

  asyncTest "a copy after a mix attempt completes the send with no hand-over":
    ## Without the copy, the next attempt would hand the task to the plain path.
    ## The plain path would publish the message again in the clear.
    privateAccess(SendService)
    let mix = MixThenFallbackProcessor()
    let plain = PlainRetryProcessor()
    mix.chain(plain)
    # The test drives the retry, so that the message comes back before the next attempt.
    let service =
      newService(mix, AnonymityLevel.Preferred, retryInterval = chronos.hours(1))
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("after-mix")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.calls == 1

    check await waku.receiveFromNetwork(task)
    service.retryTick.fire()
    service.retryTick.clear()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      mix.calls == 1
      plain.calls == 0

  asyncTest "a copy before any attempt does not complete the send":
    ## No attempt of this node sent the message, so the send has nothing to complete.
    let never = NeverValidProcessor()
    let service = newService(never)
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("no-attempt")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry

    check await waku.receiveFromNetwork(task)
    let before = never.asked

    checkUntilTimeout:
      never.asked >= before + 3
    check:
      task.networkHasMessage()
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a copy after a plain attempt completes the send and starts its wait for a Store confirmation":
    ## The plain attempt can show this node's address, so the task is not anonymous.
    ## A store node confirms it as for a plain send.
    let plain = PlainRetryProcessor()
    let service = newService(plain, AnonymityLevel.None, storeConfirmation = true)
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("after-plain")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      plain.calls >= 1

    check await waku.receiveFromNetwork(task)

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
      not task.storeAnswer.isNil()
    check service.awaitsStoreValidation(task)

  asyncTest "a copy during the wait for a send slot completes the send with no new attempt":
    ## The admission and the wait for a slot can take long.
    ## The network can have the message by then.
    privateAccess(SendService)
    privateAccess(AsyncSemaphore)
    let mix = SlotProcessor(finish: newAsyncEvent())
    # The test drives the retry, so the task waits for the slot when its copy comes.
    let service =
      newService(mix, retryInterval = chronos.hours(1), maxSendsInFlight = 1)
    service.startSendService()
    defer:
      await service.stopSendService()
    let task = buildTask("waits-for-a-slot")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      mix.others == 1
    check service.enqueue(buildTask("blocker")).isOk()
    checkUntilTimeout:
      mix.blockerStarted
    service.retryTick.fire()
    service.retryTick.clear()
    checkUntilTimeout:
      service.sendSlots.waiters.len == 1

    check await waku.receiveFromNetwork(task)
    mix.finish.fire()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check mix.others == 1

  asyncTest "a copy that comes with a failed attempt completes the send":
    ## The copy can come after the processor looked for it.
    ## It can come before the loop reads the failure.
    let service = newService(CopyThenFailProcessor(waku: waku))
    service.startSendService()
    defer:
      await service.stopSendService()
    var errors = 0
    let listener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          errors.inc(),
      )
      .expect("listen")
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)
    let task = buildTask("copy-then-fail")

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check errors == 0
