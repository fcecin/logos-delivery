{.used.}

## A mix send whose exit does not answer is not a failed send. The message left
## this node, so the send waits for a confirmation, sends the message again a
## few times, and ends unconfirmed when none comes.

import chronos, testutils/unittests, results, stew/byteutils
import brokers/broker_context
import
  logos_delivery/waku/[waku, waku_core],
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/conf/modes,
  logos_delivery/api/events/[kernel_events, messaging_client_events],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, mix_processor, delivery_task]
import ../testlib/[futures, testasync, wakucore, wakunodeconf]

type UnconfirmedMixProcessor = ref object of BaseSendProcessor
  ## A mix attempt whose request leaves this node and gets no answer.
  calls: int
  resends: int
  confirmOnResend: bool ## The exit answers the resend.

method isPlain(self: UnconfirmedMixProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: UnconfirmedMixProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: UnconfirmedMixProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.anonymized = true
  task.state = DeliveryState.SuccessfullyPropagated
  task.propagatedAnonymously = true
  task.firstPropagatedTime = Opt.some(Moment.now())

method canResend(self: UnconfirmedMixProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return task.propagatedAnonymously

method resendImpl(
    self: UnconfirmedMixProcessor, task: DeliveryTask
) {.async: (raises: [CancelledError]).} =
  inc self.resends
  task.resendCount.inc()
  if self.confirmOnResend:
    task.confirmMix()

type HeldMixProcessor = ref object of BaseSendProcessor
  ## A mix attempt that waits for the test before it ends with no answer, so the
  ## test can act while the attempt runs.
  started: AsyncEvent
  release: AsyncEvent

method isPlain(self: HeldMixProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: HeldMixProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: HeldMixProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.anonymized = true
  self.started.fire()
  await self.release.wait()
  task.state = DeliveryState.SuccessfullyPropagated
  task.propagatedAnonymously = true
  task.firstPropagatedTime = Opt.some(Moment.now())

method canResend(self: HeldMixProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return task.propagatedAnonymously

type HeldHandoverProcessor = ref object of BaseSendProcessor
  ## A mix attempt that waits for the test, and then hands the task to the plain
  ## path.
  started: AsyncEvent
  release: AsyncEvent

method isPlain(self: HeldHandoverProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: HeldHandoverProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: HeldHandoverProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.anonymized = true
  self.started.fire()
  await self.release.wait()
  task.state = DeliveryState.FallbackRetry

type HandoverProcessor = ref object of BaseSendProcessor
  ## A mix attempt that hands the task to the plain path at once.

method isPlain(self: HandoverProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: HandoverProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: HandoverProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.anonymized = true
  task.state = DeliveryState.FallbackRetry

type PlainCounter = ref object of BaseSendProcessor
  ## A plain processor that counts the resends it gets.
  resends: int

method isValidProcessor(self: PlainCounter, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: PlainCounter, task: DeliveryTask): Future[void] {.async.} =
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

method canResend(self: PlainCounter, task: DeliveryTask): bool {.gcsafe.} =
  return true

method resendImpl(
    self: PlainCounter, task: DeliveryTask
) {.async: (raises: [CancelledError]).} =
  inc self.resends

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc buildTask(id: string): DeliveryTask =
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
    firstAdmittedTime: Opt.some(Moment.now()),
  )

proc emitSeen(waku: Waku, task: DeliveryTask) =
  ## The network gives this node the message of `task`.
  MessageSeenEvent.emit(waku.brokerCtx, task.pubsubTopic, task.msg)

type Events = ref object
  ## The events of the send, one future for each kind, so a test can wait for
  ## the one it expects and check that the others did not come.
  propagated, sent, unconfirmed, error: Future[void]
  anonymous: seq[bool] ## The `anonymous` field of each `propagated` and `sent` event.
  listeners: seq[proc(): Future[void] {.async: (raises: []).}]

proc listen(waku: Waku): Events =
  let events = Events(
    propagated: newFuture[void]("propagated"),
    sent: newFuture[void]("sent"),
    unconfirmed: newFuture[void]("unconfirmed"),
    error: newFuture[void]("error"),
  )
  let ctx = waku.brokerCtx
  let propagated = MessagePropagatedEvent
    .listen(
      ctx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        events.anonymous.add(event.anonymous)
        events.propagated.complete(),
    )
    .expect("listen")
  let sent = MessageSentEvent
    .listen(
      ctx,
      proc(event: MessageSentEvent) {.async: (raises: []).} =
        events.anonymous.add(event.anonymous)
        events.sent.complete(),
    )
    .expect("listen")
  let unconfirmed = MessageUnconfirmedEvent
    .listen(
      ctx,
      proc(event: MessageUnconfirmedEvent) {.async: (raises: []).} =
        events.unconfirmed.complete(),
    )
    .expect("listen")
  let error = MessageErrorEvent
    .listen(
      ctx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        events.error.complete(),
    )
    .expect("listen")
  events.listeners.add(
    proc() {.async: (raises: []).} =
      await MessagePropagatedEvent.dropListener(ctx, propagated)
      await MessageSentEvent.dropListener(ctx, sent)
      await MessageUnconfirmedEvent.dropListener(ctx, unconfirmed)
      await MessageErrorEvent.dropListener(ctx, error)
  )
  return events

proc drop(events: Events) {.async.} =
  for listener in events.listeners:
    await listener()

proc notWithin(fut: Future[void], window: Duration): Future[bool] {.async.} =
  ## True when `fut` does not complete within `window`. `withTimeout` cancels
  ## the future when the time ends, and a test waits for these futures again.
  let timer = sleepAsync(window)
  discard await race(fut, timer)
  await timer.cancelAndWait()
  return not fut.finished()

suite "SendService - a mix send with no answer of the exit":
  var waku {.threadvar.}: Waku
  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
  asyncTeardown:
    discard await waku.stop()

  proc newService(
      processor: BaseSendProcessor,
      maxValidationAge = chronos.minutes(1),
      mixResendInterval = chronos.milliseconds(100),
  ): SendService =
    ## A service with the `reliability` option on, so a plain send would wait
    ## for a store confirmation.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(
        true,
        waku,
        manager,
        processor,
        retryInterval = chronos.milliseconds(100),
        maxValidationAge = maxValidationAge,
        mixResendInterval = mixResendInterval,
      )
      .expect("SendService.new")
    service.startSendService()
    return service

  asyncTest "a copy of the message from the network confirms a mix send with no answer":
    ## The message left this node, so the send propagated. Nothing else needs
    ## to happen for the send to end well, and no second mix attempt is made.
    let mix = UnconfirmedMixProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("seen-confirms")
    check service.enqueue(task).isOk()

    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      task.propagatedAnonymously
      not task.mixConfirmed
    # The send waits. No event ends it before the confirmation.
    check await events.sent.notWithin(chronos.milliseconds(300))

    waku.emitSeen(task)

    check await events.sent.withTimeout(FUTURE_TIMEOUT)
    check:
      task.mixConfirmed
      mix.calls == 1
      events.anonymous == @[true, true]
      not events.unconfirmed.finished()
      not events.error.finished()

  asyncTest "the events of a Preferred send that went to the plain path say so":
    ## The mix attempt marked the task, and the plain path propagated it. The
    ## events report the path that carried the message, not the attempt.
    let mix = HandoverProcessor()
    let plain = PlainCounter()
    mix.chain(plain)
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("plain-after-mix")
    check service.enqueue(task).isOk()

    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    check:
      task.anonymized
      not task.propagatedAnonymously
      events.anonymous == @[false]

  asyncTest "the events of a plain send say that the message went out in the clear":
    let plain = PlainCounter()
    let service = newService(plain)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("plain")
    check service.enqueue(task).isOk()

    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    check events.anonymous == @[false]

  asyncTest "a copy of the message received during the attempt confirms the send when the attempt ends":
    ## The copy arrives while the attempt still waits for the answer of the exit.
    ## The attempt ends with no answer, and the send is confirmed then, with no
    ## wait and no resend.
    let mix = HeldMixProcessor(started: newAsyncEvent(), release: newAsyncEvent())
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("seen-during-attempt")
    check service.enqueue(task).isOk()
    check await mix.started.wait().withTimeout(FUTURE_TIMEOUT)

    waku.emitSeen(task)
    checkUntilTimeout:
      task.seenOnNetwork
    mix.release.fire()

    check await events.sent.withTimeout(FUTURE_TIMEOUT)
    check:
      events.propagated.finished()
      task.mixConfirmed
      task.resendCount == 0

  asyncTest "a copy received during an attempt that goes to the plain path confirms no mix send":
    ## The copy arrives while the mix attempt runs, and the attempt then hands the
    ## task to the plain path, which propagates it in the clear. The copy says
    ## nothing about a mix send, so the task waits for its store confirmation.
    let mix = HeldHandoverProcessor(started: newAsyncEvent(), release: newAsyncEvent())
    let plain = PlainCounter()
    mix.chain(plain)
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("seen-then-plain")
    check service.enqueue(task).isOk()
    check await mix.started.wait().withTimeout(FUTURE_TIMEOUT)

    waku.emitSeen(task)
    checkUntilTimeout:
      task.seenOnNetwork
    mix.release.fire()

    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    check:
      task.plainAttempted
      not task.propagatedAnonymously
      not task.mixConfirmed
      await events.sent.notWithin(chronos.milliseconds(300))

  asyncTest "a confirmation before the restart runs the send again still ends with the sent event":
    ## A stop cancels the wait. A confirmation can come before the restarted
    ## service runs the task again, here the answer of a resend that ended while
    ## the service stopped.
    let mix = UnconfirmedMixProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("confirmed-before-restart")
    check service.enqueue(task).isOk()
    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    await service.stopSendService()

    task.confirmMix()
    service.startSendService()

    check await events.sent.withTimeout(FUTURE_TIMEOUT)
    check:
      mix.calls == 1
      not events.unconfirmed.finished()

  asyncTest "a window shorter than the resend interval ends unconfirmed with no resend":
    let mix = UnconfirmedMixProcessor()
    let service = newService(
      mix,
      maxValidationAge = chronos.milliseconds(300),
      mixResendInterval = chronos.seconds(1),
    )
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("short-window")
    check service.enqueue(task).isOk()

    check await events.unconfirmed.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check mix.resends == 0

  asyncTest "a mix send with no confirmation ends unconfirmed after its window and its resends":
    ## The window of the confirmation is `maxValidationAge`. The send tries the
    ## message again each `mixResendInterval` while it waits, at most
    ## `MaxStoreMissResends` times, and then ends with no error.
    let mix = UnconfirmedMixProcessor()
    let service = newService(mix, maxValidationAge = chronos.milliseconds(600))
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("never-confirmed")
    check service.enqueue(task).isOk()

    check await events.unconfirmed.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      mix.calls == 1
      mix.resends == MaxStoreMissResends
      task.resendCount == MaxStoreMissResends
      not task.mixConfirmed
      events.propagated.finished()
      not events.sent.finished()
      not events.error.finished()

  asyncTest "the answer of the exit to a resend confirms the mix send":
    let mix = UnconfirmedMixProcessor(confirmOnResend: true)
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("resend-confirms")
    check service.enqueue(task).isOk()

    check await events.sent.withTimeout(FUTURE_TIMEOUT)
    check:
      mix.resends == 1
      task.mixConfirmed
      not events.unconfirmed.finished()

  asyncTest "a restart continues the wait for the confirmation":
    ## A stop drops the listener of the network, so a copy that comes while the
    ## service is stopped confirms nothing. The restart waits again.
    let mix = UnconfirmedMixProcessor()
    let service = newService(mix)
    defer:
      await service.stopSendService()
    let events = waku.listen()
    defer:
      await events.drop()
    let task = buildTask("restart")
    check service.enqueue(task).isOk()
    check await events.propagated.withTimeout(FUTURE_TIMEOUT)
    await service.stopSendService()

    waku.emitSeen(task)
    service.startSendService()
    check:
      not task.mixConfirmed
      await events.sent.notWithin(chronos.milliseconds(300))

    waku.emitSeen(task)
    check await events.sent.withTimeout(FUTURE_TIMEOUT)
    check mix.calls == 1

  asyncTest "a message that went out over mix is never sent again in the clear":
    ## The resend of an unconfirmed mix send stays with the mix processor, also
    ## while mix cannot send. A plain copy would carry the address of this node.
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    let plain = PlainCounter()
    mix.chain(plain)
    let task = buildTask("no-plain-resend")
    task.anonymized = true
    task.state = DeliveryState.SuccessfullyPropagated
    task.propagatedAnonymously = true

    await mix.resend(task)

    check:
      plain.resends == 0
      task.resendCount == 1
      not task.mixConfirmed
