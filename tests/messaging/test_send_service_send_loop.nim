{.used.}

import std/[sequtils, tables]
import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/rln/[rln_api, rln_plugin],
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[futures, testasync, wakunodeconf]

## Tests of the send scheduler. A scripted processor stalls, fails or completes
## each task, and records what runs at the same time.

const PollInterval = chronos.milliseconds(5)
  ## The poll interval of `checkUntilTimeoutCustom` in this file.

type ScriptedProcessor = ref object of BaseSendProcessor
  ## The first call parks a task, or fails it when it is in `failing`. A retry of
  ## a `stalled` task waits for its reply, a `raising` task raises, and other
  ## retries end at once in `retryOutcome`.
  stalled: seq[string]
  raising: seq[string]
  failing: seq[string]
  retryOutcome: DeliveryState
  replies: Table[string, Future[void]] ## the reply of the last stalled retry
  started: Table[string, AsyncEvent] ## fired at the first retry
  calls: seq[string] ## the order of all calls
  seen: seq[string] ## the order of the first calls
  retries: seq[string] ## the order of the retry calls
  running: int ## the retry calls that run now
  cancelled: seq[string]

proc startedEvent(self: ScriptedProcessor, id: string): AsyncEvent =
  if id notin self.started:
    self.started[id] = newAsyncEvent()
  return self.started.getOrDefault(id)

proc retryStarted(self: ScriptedProcessor, id: string): Future[void] =
  ## Completes when the first retry of `id` starts. Each call returns a new future.
  return self.startedEvent(id).wait()

proc finish(self: ScriptedProcessor, id: string) =
  ## Gives the reply to the stalled retry of `id`.
  let reply = self.replies.getOrDefault(id)
  if not reply.isNil() and not reply.finished():
    reply.complete()

method process(self: ScriptedProcessor, task: DeliveryTask): Future[void] {.async.} =
  let id = $task.requestId
  self.calls.add(id)
  if id notin self.seen:
    self.seen.add(id)
    if id in self.failing:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = "scripted failure for " & id
    else:
      task.state = DeliveryState.NextRoundRetry
    return

  self.retries.add(id)
  inc self.running
  defer:
    dec self.running
  self.startedEvent(id).fire()

  if id in self.raising:
    raise newException(ValueError, "scripted failure for " & id)
  if id in self.stalled:
    let reply = newFuture[void]("send-loop-reply")
    self.replies[id] = reply
    try:
      await reply
    except CancelledError as exc:
      self.cancelled.add(id)
      raise exc
  else:
    # Suspend, so the sends overlap.
    await sleepAsync(ZeroDuration)

  task.state = self.retryOutcome
  if task.state == DeliveryState.SuccessfullyPropagated and
      task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

proc newScripted(
    stalled: seq[string] = @[],
    raising: seq[string] = @[],
    failing: seq[string] = @[],
    retryOutcome = DeliveryState.SuccessfullyPropagated,
): ScriptedProcessor =
  return ScriptedProcessor(
    stalled: stalled, raising: raising, failing: failing, retryOutcome: retryOutcome
  )

type FakePropagatingProcessor = ref object of BaseSendProcessor
  ## Propagates each task at once.
  calls: int

method isValidProcessor(
    self: FakePropagatingProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: FakePropagatingProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.SuccessfullyPropagated
  if task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

type HandOffProcessor = ref object of BaseSendProcessor
  ## Overrides `sendImpl`, so the base chain runs. The first call parks the task
  ## for a retry. Each later call hands it to the fallback processor, as the mix
  ## processor does for `Preferred`.
  calls: int

method isValidProcessor(self: HandOffProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: HandOffProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state =
    if self.calls == 1: DeliveryState.NextRoundRetry else: DeliveryState.FallbackRetry

type StallingProcessor = ref object of BaseSendProcessor
  ## Overrides `sendImpl`, which the chain calls on each link. Each call waits on
  ## a new reply in `replies`, then propagates the task.
  replies: seq[Future[void]]

method isValidProcessor(self: StallingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: StallingProcessor, task: DeliveryTask): Future[void] {.async.} =
  let reply = newFuture[void]("stalling-reply")
  self.replies.add(reply)
  await reply
  task.state = DeliveryState.SuccessfullyPropagated
  if task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

type RaisingProcessor = ref object of BaseSendProcessor
  calls: int

method isValidProcessor(self: RaisingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: RaisingProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  raise newException(ValueError, "scripted fallback failure")

type OutcomeLog = ref object
  ## The request ids of the propagation and error events, in order.
  brokerCtx: BrokerContext
  propagated: seq[RequestId]
  errors: seq[RequestId]
  propagatedCount: EventCounter
  propagatedListener: MessagePropagatedEventListener
  errorListener: MessageErrorEventListener

proc waitPropagated(log: OutcomeLog, count: int): Future[void] =
  ## Completes when the log holds `count` propagation events. Each call returns a
  ## new future.
  return log.propagatedCount.waitCount(count)

proc newOutcomeLog(brokerCtx: BrokerContext): OutcomeLog =
  let log = OutcomeLog(brokerCtx: brokerCtx, propagatedCount: EventCounter())
  log.propagatedListener = MessagePropagatedEvent
    .listen(
      brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        log.propagated.add(event.requestId)
        log.propagatedCount.inc(),
    )
    .expect("listen propagated")
  log.errorListener = MessageErrorEvent
    .listen(
      brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        log.errors.add(event.requestId),
    )
    .expect("listen error")
  return log

proc teardown(log: OutcomeLog) {.async.} =
  await MessagePropagatedEvent.dropListener(log.brokerCtx, log.propagatedListener)
  await MessageErrorEvent.dropListener(log.brokerCtx, log.errorListener)

type FakeQuotaProvider = ref object
  ## A quota provider whose reads wait for `reply` and never raise a cancel.
  reply: Future[void]
  calls: EventCounter ## the quota reads that started

proc newFakeQuotaProvider(): FakeQuotaProvider =
  return FakeQuotaProvider(reply: newFuture[void]("quota-reply"), calls: EventCounter())

proc waitCalls(quota: FakeQuotaProvider, count: int): Future[void] =
  ## Completes when `count` quota reads started. Each call returns a new future.
  return quota.calls.waitCount(count)

proc provider(quota: FakeQuotaProvider): QuotaProvider =
  return proc(): Future[Opt[EpochQuota]] {.async: (raises: []), gcsafe.} =
    quota.calls.inc()
    try:
      await quota.reply
    except CatchableError:
      discard
    return Opt.none(EpochQuota)

type SendOnErrorHook = ref object
  ## A listener that sends a new message at each error. It starts `timer` at the
  ## first error and stops when the timer ends or after `limit`.
  limit: Duration
  firstErrorAt: Moment
  timer: Future[void] ## a `sleepAsync(ZeroDuration)` from the first error
  stoppedByTimer: bool
  stoppedAt: Moment
  results: seq[Result[void, string]]
  done: Future[void]

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc fixedEpochQuota(epoch: ref uint64, userMessageLimit: uint64): QuotaProvider =
  ## `epoch` is a ref so the test can roll the epoch without taking the
  ## address of a local.
  return proc(): Future[Opt[EpochQuota]] {.async: (raises: []), gcsafe.} =
    return Opt.some(
      EpochQuota(
        epochIndex: epoch[], rateLimit: userMessageLimit, remaining: userMessageLimit
      )
    )

suite "SendService - send scheduler":
  var waku {.threadvar.}: Waku
  var log {.threadvar.}: OutcomeLog

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    log = newOutcomeLog(waku.brokerCtx)

  asyncTeardown:
    await log.teardown()
    discard await waku.stop()

  proc buildTask(
      id: string, timestamp: Timestamp = getNowInNanosecondTime(), ephemeral = false
  ): DeliveryTask =
    ## A message from now, so a task that parks for budget is not too old.
    let msg = WakuMessage(
      contentTopic: "/test/1/send-loop/proto",
      payload: id.toBytes(),
      timestamp: timestamp,
      ephemeral: ephemeral,
    )
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    return DeliveryTask(
      requestId: RequestId(id),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.Entry,
    )

  proc newService(
      processor: BaseSendProcessor,
      interval = ServiceLoopInterval,
      maxTaskCacheSize = DefaultMaxTaskCacheSize,
  ): SendService =
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService
      .new(
        false,
        waku,
        manager,
        processor,
        maxTaskCacheSize = maxTaskCacheSize,
        serviceLoopInterval = interval,
      )
      .expect("SendService.new")

  proc fakeQuotaService(
      processor: BaseSendProcessor,
      quota: FakeQuotaProvider,
      interval = ServiceLoopInterval,
  ): SendService =
    ## A service whose quota reads wait for the reply of `quota`, so its
    ## admissions wait too.
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 100),
        quota.provider(),
      )
      .expect("RateLimitManager.new")
    return SendService
      .new(false, waku, manager, processor, serviceLoopInterval = interval)
      .expect("SendService.new")

  proc names(prefix: string, count: int): seq[string] =
    return (0 ..< count).toSeq().mapIt(prefix & $it)

  proc runFirstAttempts(service: SendService, tasks: seq[DeliveryTask]) {.async.} =
    ## Queues each task, and lets the first call of the processor park it. The
    ## tasks are then retries that are due at once.
    for task in tasks:
      check service.enqueue(task).isOk()
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    for task in tasks:
      check task.state == DeliveryState.NextRoundRetry
      task.nextAttemptTime = Opt.none(Moment)

  proc enqueueAdmittedRetries(
      service: SendService, processor: ScriptedProcessor, tasks: seq[DeliveryTask]
  ) =
    ## Queues each task as an admitted retry that is due at once.
    for task in tasks:
      check service.enqueue(task).isOk()
      task.state = DeliveryState.NextRoundRetry
      task.firstAdmittedTime = Opt.some(Moment.now())
      processor.seen.add($task.requestId)

  asyncTest "a send that ends is reported while another send still waits":
    let processor = newScripted(stalled = @["slow"])
    # The interval is long, so only the end of the send can report it.
    let service = newService(processor, interval = chronos.minutes(10))
    let slow = buildTask("slow")
    let fast = buildTask("fast")
    await service.runFirstAttempts(@[slow, fast])

    service.startSendService()
    defer:
      await service.stopSendService()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      fast.requestId in log.propagated
    check:
      slow.running # the report did not wait for it
      slow.requestId notin log.propagated

  asyncTest "all due retries and a new send run at the same time":
    let ids = names("r", 8)
    let processor = newScripted(stalled = ids)
    let service = newService(processor)
    await service.runFirstAttempts(ids.mapIt(buildTask(it)))

    service.startSendService()
    defer:
      await service.stopSendService()
    for id in ids:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)

    check service.enqueue(buildTask("fresh")).isOk()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      "fresh" in processor.seen
    check processor.running == ids.len # the retries still run

  asyncTest "a due retry that needs no admission sends while a new task waits in admission":
    ## The proof of the new task waits for `reply`. The retry already has its
    ## charge and its proof, so it does not wait for that admission.
    let reply = newFuture[void]("proof-reply")
    let proofCalls = new int
    proc generate(message: WakuMessage): Future[Result[seq[byte], RlnError]] {.async.} =
      inc proofCalls[]
      await reply
      return ok(@[4'u8, 5, 6])

    waku.node.rlnPlugin = Opt.some(RlnPlugin(name: "fake", generateProof: generate))
    let processor = newScripted()
    let service = newService(processor)
    let proven = buildTask("proven-retry")
    proven.msg.proof = @[1'u8, 2, 3]
    check service.enqueue(buildTask("fresh")).isOk()
    check service.enqueue(buildTask("fresh-2")).isOk() # waits for the running admission
    service.enqueueAdmittedRetries(processor, @[proven]) # last in the queue

    service.startSendService()
    defer:
      await service.stopSendService()
    check await processor.retryStarted("proven-retry").withTimeout(
      FUTURE_TIMEOUT_MEDIUM
    )
    check:
      proofCalls[] == 1 # the admission of `fresh` still waits for its proof
      "fresh" notin processor.seen
      "fresh-2" notin processor.seen

  asyncTest "one admission runs at a time":
    ## The quota read of the first admission waits for the reply of the fake
    ## provider. A task added meanwhile waits for that admission to end.
    let quota = newFakeQuotaProvider()
    let processor = newScripted()
    # The interval is long, so only the end of an admission can start the next one.
    let service = fakeQuotaService(processor, quota, interval = chronos.minutes(10))

    check service.enqueue(buildTask("first")).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    check await quota.waitCalls(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)

    check service.enqueue(buildTask("second")).isOk()
    check not await quota.waitCalls(2).withTimeout(FUTURE_TIMEOUT_SHORT)
    check processor.seen.len == 0

    quota.reply.complete()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      processor.seen == @["first", "second"]
    check quota.calls.value == 2

  asyncTest "admission charges the epoch budget in the order of admission":
    ## With a budget of one per epoch, the first task gets the budget, and the
    ## second parks before the processor.
    let epoch = new uint64
    epoch[] = 1'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1),
        fixedEpochQuota(epoch, userMessageLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = newScripted()
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")

    let first = buildTask("in-budget")
    let second = buildTask("over-budget")
    check:
      service.enqueue(first).isOk()
      service.enqueue(second).isOk()
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      first.firstAdmittedTime.isSome()
      second.firstAdmittedTime.isNone()
      manager.sentInCurrentEpoch == 1'u64
      processor.seen == @["in-budget"] # the second parked before the processor
      second.state == DeliveryState.NextRoundRetry

    first.nextAttemptTime = Opt.none(Moment)
    second.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      processor.retries == @["in-budget"]
      first.state == DeliveryState.SuccessfullyPropagated
      second.state == DeliveryState.NextRoundRetry
      manager.sentInCurrentEpoch == 1'u64

  asyncTest "a send that raises does not stop the scheduler":
    let processor = newScripted(raising = @["boom"])
    let service = newService(processor)
    let boom = buildTask("boom")
    let fine = buildTask("fine")
    await service.runFirstAttempts(@[boom, fine])

    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      fine.state == DeliveryState.SuccessfullyPropagated
      boom.state == DeliveryState.NextRoundRetry # left for its next retry
      boom.nextAttemptTime.isSome()
      not boom.running

    # The next retry of the task that raised goes out.
    processor.raising = @[]
    boom.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check boom.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a raise in a first attempt leaves the task for a retry":
    ## A raise stops `process` before it moves the task out of `Entry`. The
    ## scheduler moves it, so the task does not start again at once.
    let processor = RaisingProcessor()
    let service = newService(processor)
    let task = buildTask("raise-first")
    check service.enqueue(task).isOk()

    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      processor.calls == 1
      task.state == DeliveryState.NextRoundRetry
      task.nextAttemptTime.isSome()
      not task.running

  asyncTest "a raise in a fallback processor still leaves the task for a retry":
    ## A raise skips the tail of `process` that moves a hand-off to
    ## `NextRoundRetry`, so the scheduler must move the task out of `FallbackRetry`.
    let handOff = HandOffProcessor()
    let raising = RaisingProcessor()
    handOff.chain(raising)
    let service = newService(handOff)
    let task = buildTask("fallback-raise")
    check service.enqueue(task).isOk()
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
      # parked by the first call
    check task.state == DeliveryState.NextRoundRetry

    task.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
      # the fallback processor gets the task and raises
    check:
      raising.calls == 1
      task.state == DeliveryState.NextRoundRetry # left for its next retry

    task.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check raising.calls == 2 # ... and the next retry went out

  asyncTest "a listener that sends inside its handler adds one task, and the scheduler sends it":
    ## The listener runs inside `emit`, in the cleanup of the round.
    let processor = FakePropagatingProcessor()
    let service = newService(processor)
    let first = buildTask("first")
    let second = buildTask("second")
    let results = new seq[Result[void, string]]
    let listener = MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          if event.requestId == first.requestId:
            results[].add(service.enqueue(second))
        ,
      )
      .expect("listen")
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)

    check service.enqueue(first).isOk()
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      results[].len == 1
      results[].allIt(it.isOk())
      processor.calls == 2
      log.propagated == @[first.requestId, second.requestId]

  asyncTest "the scheduler returns to the event loop while a listener sends at each error":
    ## The budget is 0, so each admission drops its ephemeral message with no
    ## suspension, and the listener sends a new message at each error. The timer
    ## of the first error ends before the listener reaches its limit only when the
    ## scheduler returns to the event loop between rounds.
    let manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 0))
      .expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, FakePropagatingProcessor())
      .expect("SendService.new")
    let hook = SendOnErrorHook(
      limit: chronos.seconds(1), done: newFuture[void]("send-on-error-done")
    )
    let listener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          if hook.done.finished():
            return
          let now = Moment.now()
          if hook.timer.isNil():
            hook.firstErrorAt = now
            hook.timer = sleepAsync(ZeroDuration)
          elif hook.timer.finished() or now - hook.firstErrorAt > hook.limit:
            hook.stoppedByTimer = hook.timer.finished()
            hook.stoppedAt = now
            hook.done.complete()
            return
          hook.results.add(
            service.enqueue(
              buildTask("on-error-" & $hook.results.len, ephemeral = true)
            )
          ),
      )
      .expect("listen")
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)

    check service.enqueue(buildTask("on-error-start", ephemeral = true)).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    check await hook.done.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      hook.stoppedByTimer
      hook.stoppedAt - hook.firstErrorAt < chronos.milliseconds(500)
      hook.results.allIt(it.isOk())

  asyncTest "stopping the service cancels the sends that run":
    let processor = newScripted(stalled = @["stall"])
    let service = newService(processor)
    let task = buildTask("stall")
    await service.runFirstAttempts(@[task])

    service.startSendService()
    check await processor.retryStarted("stall").withTimeout(FUTURE_TIMEOUT_MEDIUM)

    await service.stopSendService()
    # The stop cancels the send, and with it the wait for the reply.
    check:
      processor.cancelled == @["stall"]
      processor.replies.getOrDefault("stall").cancelled()
      not task.running
      task.state == DeliveryState.NextRoundRetry
      log.propagated.len == 0 # no event for a cancelled send
      log.errors.len == 0

  asyncTest "enqueue wakes a scheduler that waits with an empty send queue":
    ## With an empty send queue the scheduler waits with no timer, so only
    ## `enqueue` can wake it.
    let processor = FakePropagatingProcessor()
    let service = newService(processor, interval = chronos.minutes(10))
    service.startSendService()
    defer:
      await service.stopSendService()
    # The first round ran with the empty send queue, and the scheduler waits.
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      service.roundCount() == 1

    check service.enqueue(buildTask("wakes")).isOk()
    check await log.waitPropagated(1).withTimeout(FUTURE_TIMEOUT)

  asyncTest "a round during a stop starts no task":
    ## The cancel of a stop can arrive one turn late, so a round can run during a
    ## stop.
    let processor = FakePropagatingProcessor()
    let service = newService(processor)
    let task = buildTask("queued-at-stop")
    check service.enqueue(task).isOk()

    await service.stopSendService()
    service.runRound()
    check:
      not task.running
      task.state == DeliveryState.Entry
      processor.calls == 0

  asyncTest "a stop normalises a task its cancel left at FallbackRetry":
    ## A cancel stops `process` before it moves the task, so the stop must move
    ## it out of `FallbackRetry`.
    let handOff = HandOffProcessor()
    let stalling = StallingProcessor()
    handOff.chain(stalling)
    let service = newService(handOff)
    let task = buildTask("stop-normalise")
    check service.enqueue(task).isOk()
    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
      # parked by the first call
    task.nextAttemptTime = Opt.none(Moment)

    service.startSendService() # passes the task to the fallback, which waits
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      stalling.replies.len == 1
    check task.state == DeliveryState.FallbackRetry

    await service.stopSendService()
    check:
      task.state == DeliveryState.NextRoundRetry
      not task.running

  asyncTest "a send cancelled by a stop is retried after the restart, with one terminal event":
    let stalling = StallingProcessor()
    let service = newService(stalling, interval = chronos.milliseconds(50))
    let task = buildTask("cancelled-by-stop")
    check service.enqueue(task).isOk()

    service.startSendService()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      stalling.replies.len == 1
    await service.stopSendService()
    check:
      stalling.replies.len == 1 and stalling.replies[0].cancelled()
      task.state == DeliveryState.NextRoundRetry
      log.propagated.len == 0
      log.errors.len == 0

    service.startSendService()
    defer:
      await service.stopSendService()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      stalling.replies.len == 2
    if stalling.replies.len == 2:
      stalling.replies[1].complete()
    check await log.waitPropagated(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check not await log.waitPropagated(2).withTimeout(FUTURE_TIMEOUT_SHORT)
    check:
      log.propagated == @[task.requestId]
      log.errors.len == 0

  asyncTest "enqueue refuses a duplicate hash, a full send queue and a stopped service":
    let processor = FakePropagatingProcessor()
    let service = newService(processor, maxTaskCacheSize = 2)
    let first = buildTask("first")
    let copy = DeliveryTask(
      requestId: RequestId("copy-of-first"),
      pubsubTopic: first.pubsubTopic,
      msg: first.msg, # the same message, so the same hash
      msgHash: first.msgHash,
      state: DeliveryState.Entry,
    )

    check:
      service.enqueue(first).isOk()
      service.enqueue(copy).errorOr("accepted") ==
        "Send queue already has a message with the same hash"
      service.enqueue(buildTask("second")).isOk()
      service.isFull()
      service.enqueue(buildTask("third")).errorOr("accepted") ==
        "Send queue full, retry later"

    check await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check processor.calls == 2 # `first` and `second`

    await service.stopSendService()
    check service.enqueue(buildTask("fourth")).errorOr("accepted") ==
      "Send service is stopped"
