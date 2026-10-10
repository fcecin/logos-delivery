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
import ../testlib/[futures, sendservice, testasync, wakunodeconf]

## The scheduler of the send service. `enqueue` only puts a task in the cache.
## The scheduler admits the tasks one at a time, new tasks first, and runs at
## most `MaxConcurrentRetries` retries at the same time. A scripted processor
## stalls, fails or completes each task on demand, and records what ran at the
## same time.

const PollInterval = chronos.milliseconds(5)
  ## The poll interval of `checkUntilTimeoutCustom` in this file.

type ScriptedProcessor = ref object of BaseSendProcessor
  ## The first `process` of a task parks it at `NextRoundRetry`, or fails it when
  ## it is in `failing`. Later calls follow the script. A retry of a `stalled`
  ## task waits for its reply, a `raising` task raises, and each other retry
  ## ends at once. A retry ends in `retryOutcome`.
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
  peakRunning: int
  cancelled: seq[string]

proc startedEvent(self: ScriptedProcessor, id: string): AsyncEvent =
  if id notin self.started:
    self.started[id] = newAsyncEvent()
  return self.started.getOrDefault(id)

proc retryStarted(self: ScriptedProcessor, id: string): Future[void] =
  ## Completes when the first retry of task `id` started. Each call gives a new
  ## future, so the time limit of a negative check cancels only its own future.
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
  self.peakRunning = max(self.peakRunning, self.running)
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
    # Suspend here too, so `running` stays up while the scheduler starts the
    # next send, and `peakRunning` counts the sends that overlap.
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
  ## Completes when the log holds `count` propagation events. Each call gives a
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
  ## A quota provider whose reads wait for `reply`. A read does not raise the
  ## cancel of a stop, so an admission that waits here ends also during a stop.
  reply: Future[void]
  calls: EventCounter ## the quota reads that started

proc newFakeQuotaProvider(): FakeQuotaProvider =
  return FakeQuotaProvider(reply: newFuture[void]("quota-reply"), calls: EventCounter())

proc waitCalls(quota: FakeQuotaProvider, count: int): Future[void] =
  ## Completes when `count` quota reads started. Each call gives a new future.
  return quota.calls.waitCount(count)

proc provider(quota: FakeQuotaProvider): QuotaProvider =
  return proc(): Future[Opt[EpochQuota]] {.async: (raises: [CancelledError]), gcsafe.} =
    quota.calls.inc()
    try:
      await quota.reply
    except CatchableError:
      discard
    return Opt.none(EpochQuota)

type SendOnErrorHook = ref object
  ## The state of a listener that sends a new message at each error. At the
  ## first error it starts `timer`. It stops when it sees that the timer ended,
  ## or after `limit`.
  limit: Duration
  firstErrorAt: Moment
  timer: Future[void] ## a `sleepAsync(ZeroDuration)` from the first error
  stoppedByTimer: bool
  stoppedAt: Moment
  results: seq[Result[void, string]]
  done: Future[void]

proc waitRoundsAbove(service: SendService, limit: int): Future[void] {.async.} =
  ## Completes when more than `limit` rounds ran. A negative check gives it a
  ## time window.
  while service.roundCount() <= limit:
    await sleepAsync(chronos.milliseconds(1))

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc fixedEpochQuota(epoch: ref uint64, userMessageLimit: uint64): QuotaProvider =
  ## `epoch` is a ref so the test can roll the epoch without taking the
  ## address of a local.
  return proc(): Future[Opt[EpochQuota]] {.async: (raises: [CancelledError]), gcsafe.} =
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
    check await service.runUntilIdleInTime()
    for task in tasks:
      check task.state == DeliveryState.NextRoundRetry
      task.nextAttemptTime = Opt.none(Moment)

  proc enqueueAdmittedRetries(
      service: SendService, processor: ScriptedProcessor, tasks: seq[DeliveryTask]
  ) =
    ## Queues each task as an admitted retry that is due at once, with no first
    ## attempt. A service with a short `serviceLoopInterval` uses it, because a
    ## parked task can be due again while `runUntilIdle` still runs.
    for task in tasks:
      check service.enqueue(task).isOk()
      task.state = DeliveryState.NextRoundRetry
      task.firstAdmittedTime = Opt.some(Moment.now())
      processor.seen.add($task.requestId)

  asyncTest "a retry starts as soon as a retry slot is free":
    ## A slow retry uses only its own slot. When `t1` ends, `t4` starts, and `t0`
    ## still waits for its reply.
    let ids = names("t", MaxConcurrentRetries + 1)
    let processor = newScripted(stalled = ids)
    # Only the end of a send can start `t4` within the test. The cleanup timer
    # does not fire.
    let service = newService(processor, interval = chronos.minutes(10))
    let tasks = ids.mapIt(buildTask(it))
    await service.runFirstAttempts(tasks)

    service.startSendService()
    defer:
      await service.stopSendService()
    for id in ids[0 ..< MaxConcurrentRetries]:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    # `t4` waits for a slot.
    check not await processor.retryStarted("t4").withTimeout(FUTURE_TIMEOUT_SHORT)

    processor.finish("t1")
    check await processor.retryStarted("t4").withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      processor.retries == ids # `t4` started last
      tasks[0].running # still waits for its reply
      tasks[1].state == DeliveryState.SuccessfullyPropagated
      processor.peakRunning == MaxConcurrentRetries

  asyncTest "a send that ends is reported while another send still waits":
    let processor = newScripted(stalled = @["slow"])
    # Only the end of the send can report it within the test. The cleanup timer
    # does not fire.
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

  asyncTest "a new send does not wait for a free retry slot":
    ## Every retry slot is in use. The first attempt of a new task starts right
    ## after its admission.
    let ids = names("r", MaxConcurrentRetries)
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
    check processor.running == MaxConcurrentRetries # every slot is still in use

  asyncTest "at most MaxConcurrentRetries retries run, and each due retry starts once before one starts twice":
    ## Each retry waits for its reply, and leaves the task for a retry that is
    ## due 20 ms later. The first four tasks are due again before the last four
    ## start once. The retries that waited longest go first.
    const count = 3 * MaxConcurrentRetries
    let ids = names("q", count)
    let processor =
      newScripted(stalled = ids, retryOutcome = DeliveryState.NextRoundRetry)
    let service = newService(processor, interval = chronos.milliseconds(20))
    let tasks = ids.mapIt(buildTask(it))
    service.enqueueAdmittedRetries(processor, tasks)
    let first = ids[0 ..< MaxConcurrentRetries]
    let second = ids[MaxConcurrentRetries ..< 2 * MaxConcurrentRetries]
    let third = ids[2 * MaxConcurrentRetries ..< count]

    service.startSendService()
    defer:
      await service.stopSendService()
    for id in first:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check not await processor.retryStarted(second[0]).withTimeout(FUTURE_TIMEOUT_SHORT)

    for id in first:
      processor.finish(id)
    for id in second:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    # The first four tasks are due again.
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      tasks[0 ..< MaxConcurrentRetries].allIt(
        it.nextAttemptTime.isSome() and it.nextAttemptTime.get() <= Moment.now()
      )

    for id in second:
      processor.finish(id)
    for id in third:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      processor.peakRunning == MaxConcurrentRetries
      processor.retries == ids

  asyncTest "new tasks pass admission first, then retries in the order they are due":
    ## A send starts right after its admission, so the order of the calls is the
    ## order of admission. The retries were never admitted, so they need an
    ## admission too.
    let processor = newScripted()
    let service = newService(processor)
    let late = buildTask("late-retry")
    let early = buildTask("early-retry")
    await service.runFirstAttempts(@[late, early])
    late.firstAdmittedTime = Opt.none(Moment)
    early.firstAdmittedTime = Opt.none(Moment)
    late.nextAttemptTime = Opt.some(Moment.now() - chronos.seconds(1))
    early.nextAttemptTime = Opt.some(Moment.now() - chronos.seconds(2))

    check:
      service.enqueue(buildTask("new-1")).isOk()
      service.enqueue(buildTask("new-2")).isOk()
    processor.calls.setLen(0)
    check await service.runUntilIdleInTime()
    check processor.calls == @["new-1", "new-2", "early-retry", "late-retry"]

  asyncTest "a retry that passes admission keeps its retry slot for its send":
    ## The retries were never admitted, so each passes admission and then sends.
    let ids = names("p", MaxConcurrentRetries + 1)
    let processor = newScripted(stalled = ids)
    let service = newService(processor, interval = chronos.minutes(10))
    let tasks = ids.mapIt(buildTask(it))
    service.enqueueAdmittedRetries(processor, tasks)
    for task in tasks:
      task.firstAdmittedTime = Opt.none(Moment)

    service.startSendService()
    defer:
      await service.stopSendService()
    for id in ids[0 ..< MaxConcurrentRetries]:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check not await processor.retryStarted(ids[^1]).withTimeout(FUTURE_TIMEOUT_SHORT)

  asyncTest "a due retry that needs no admission sends while a new task waits in admission":
    ## The proof of the new task waits for `reply`. The retry has its charge and
    ## its proof, so it draws no budget and no RLN message id, and it does not
    ## wait for that admission.
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
    service.enqueueAdmittedRetries(processor, @[proven])
    check service.enqueue(buildTask("fresh")).isOk()
    check service.enqueue(buildTask("fresh-2")).isOk() # waits for the admission slot

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
    # Only the end of an admission can start the next one within the test. The
    # cleanup timer does not fire.
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
    check await service.runUntilIdleInTime()
    check:
      first.firstAdmittedTime.isSome()
      second.firstAdmittedTime.isNone()
      manager.sentInCurrentEpoch == 1'u64
      processor.seen == @["in-budget"] # the second parked before the processor
      second.state == DeliveryState.NextRoundRetry

    first.nextAttemptTime = Opt.none(Moment)
    second.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdleInTime()
    check:
      processor.retries == @["in-budget"]
      first.state == DeliveryState.SuccessfullyPropagated
      second.state == DeliveryState.NextRoundRetry
      manager.sentInCurrentEpoch == 1'u64

  asyncTest "a new task gets its first admission, however old its message is":
    ## The message is from 2023, far past `maxParkedAge`. The cleanup reaps only
    ## a task that had its first admission. The listeners run inside `emit`, so
    ## the log is complete when `runUntilIdle` returns.
    let processor = FakePropagatingProcessor()
    let service = newService(processor)
    let task = buildTask("old-message", timestamp = 1_700_000_000_000_000_000)
    check task.messageAge() > DefaultMaxParkedAge
    check service.enqueue(task).isOk()

    check await service.runUntilIdleInTime()
    check:
      processor.calls == 1
      log.propagated == @[task.requestId]
      log.errors.len == 0

  asyncTest "a send that raises does not stop the scheduler":
    let processor = newScripted(raising = @["boom"])
    let service = newService(processor)
    let boom = buildTask("boom")
    let fine = buildTask("fine")
    await service.runFirstAttempts(@[boom, fine])

    check await service.runUntilIdleInTime()
    check:
      fine.state == DeliveryState.SuccessfullyPropagated
      boom.state == DeliveryState.NextRoundRetry # left for its next retry
      boom.nextAttemptTime.isSome()
      not boom.running

    # The next retry of the task that raised goes out.
    processor.raising = @[]
    boom.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdleInTime()
    check boom.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a raise in a first attempt leaves the task for a retry":
    ## A raise skips the tail of `process` that moves an untried task out of
    ## `Entry`. The scheduler moves it, so the task does not start again at once.
    let processor = RaisingProcessor()
    let service = newService(processor)
    let task = buildTask("raise-first")
    check service.enqueue(task).isOk()

    check await service.runUntilIdleInTime()
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
    check await service.runUntilIdleInTime() # parked by the first call
    check task.state == DeliveryState.NextRoundRetry

    task.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdleInTime() # the hand-off, then the fallback raises
    check:
      raising.calls == 1
      task.state == DeliveryState.NextRoundRetry # left for its next retry

    task.nextAttemptTime = Opt.none(Moment)
    check await service.runUntilIdleInTime()
    check raising.calls == 2 # ... and the next retry went out

  asyncTest "a failed attempt emits one MessageErrorEvent":
    let processor = newScripted(failing = @["bad"])
    let service = newService(processor)
    let bad = buildTask("bad")
    check service.enqueue(bad).isOk()

    check await service.runUntilIdleInTime()
    for _ in 0 ..< 3:
      service.runRound()
    check:
      bad.state == DeliveryState.FailedToDeliver
      log.errors == @[bad.requestId]

  asyncTest "the cleanup fails an expired task while another send waits":
    let processor = newScripted(stalled = @["slow"])
    let service = newService(processor, interval = chronos.milliseconds(100))
    let slow = buildTask("slow")
    let expiring = buildTask("expiring")
    service.enqueueAdmittedRetries(processor, @[slow, expiring])
    expiring.nextAttemptTime = Opt.some(Moment.now() + chronos.hours(1)) # no retry

    service.startSendService()
    defer:
      await service.stopSendService()
    check await processor.retryStarted("slow").withTimeout(FUTURE_TIMEOUT_MEDIUM)

    # Admitted before its delivery window, so the next cleanup fails it.
    expiring.firstAdmittedTime =
      Opt.some(Moment.now() - MaxTimeInCache - chronos.seconds(1))
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      expiring.requestId in log.errors
    check slow.running # the cleanup did not wait for it

  asyncTest "the cleanup does not fail a running retry past its delivery window":
    ## The window ends while the retry waits for its reply. A running task keeps
    ## its state until the round that takes its send.
    let processor = newScripted(stalled = @["late"])
    let service = newService(processor, interval = chronos.milliseconds(10))
    let late = buildTask("late")
    service.enqueueAdmittedRetries(processor, @[late])

    service.startSendService()
    defer:
      await service.stopSendService()
    check await processor.retryStarted("late").withTimeout(FUTURE_TIMEOUT_MEDIUM)
    late.firstAdmittedTime =
      Opt.some(Moment.now() - MaxTimeInCache - chronos.seconds(1))

    # Each round runs a cleanup.
    let before = service.roundCount()
    check await service.waitRoundsAbove(before + 5).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      late.running
      late.state == DeliveryState.NextRoundRetry
      log.errors.len == 0

    processor.finish("late")
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      late.requestId in log.propagated
    check log.errors.len == 0

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
    check await service.runUntilIdleInTime()
    check:
      results[].len == 1
      results[].allIt(it.isOk())
      processor.calls == 2
      log.propagated == @[first.requestId, second.requestId]

  asyncTest "the scheduler returns to the event loop while a listener sends at each error":
    ## The budget is 0, so each admission drops its ephemeral message at once,
    ## with no suspension. The listener runs inside `emit`, and it sends a new
    ## message at each error. The scheduler must return to the event loop between
    ## two rounds, so the timer that the first error starts ends at once. Without
    ## that, the timer ends only when the listener stops at its limit.
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

  asyncTest "one round runs the admission of each new task when no admission suspends":
    ## The budget is 0, so each admission parks its task at once. The round takes
    ## each admission that ended, so the next task passes admission in the same
    ## round.
    let manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 0))
      .expect("RateLimitManager.new")
    let processor = newScripted()
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")
    let tasks = names("parked-", 5).mapIt(buildTask(it))
    for task in tasks:
      check service.enqueue(task).isOk()

    service.runRound()
    check:
      tasks.allIt(it.state == DeliveryState.NextRoundRetry)
      tasks.allIt(not it.running and it.nextAttemptTime.isSome())
      tasks.allIt(it.queuedEventEmitted)
      processor.seen.len == 0

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
      task.nextAttemptTime.isNone() # due at once after a restart
      log.propagated.len == 0 # no event for a cancelled send
      log.errors.len == 0

  asyncTest "an admission that ends during a stop starts no send":
    ## The quota read does not raise the cancel of the stop, so the admission
    ## ends during the stop and admits the task.
    let quota = newFakeQuotaProvider()
    let processor = newScripted()
    let service = fakeQuotaService(processor, quota)
    let task = buildTask("admitted-at-stop")

    check service.enqueue(task).isOk()
    service.startSendService()
    check await quota.waitCalls(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)

    await service.stopSendService()
    check:
      task.firstAdmittedTime.isSome() # the admission ended and charged
      processor.seen.len == 0 # no send started
      not task.running
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a task added before the start gets its first attempt after startSendService returns":
    ## `startSendService` clears the wake-up that the enqueue set. So no round
    ## runs inside the start, and the caller continues first.
    let processor = FakePropagatingProcessor()
    let service = newService(processor)
    let task = buildTask("added-before-start")
    check service.enqueue(task).isOk()

    let roundsBefore = service.roundCount()
    service.startSendService()
    defer:
      await service.stopSendService()
    check:
      service.roundCount() == roundsBefore # no round ran inside the start
      processor.calls == 0
      task.state == DeliveryState.Entry
    check await log.waitPropagated(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check processor.calls == 1

  asyncTest "enqueue wakes a scheduler that waits with an empty cache":
    ## With no task in the cache, the scheduler waits with no timer, so only an
    ## event can wake it. The interval is long, so no cleanup timer can do the
    ## work of the wake-up.
    let processor = FakePropagatingProcessor()
    let service = newService(processor, interval = chronos.minutes(10))
    service.startSendService()
    defer:
      await service.stopSendService()
    # The first round ran with the empty cache, and the scheduler waits.
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      service.roundCount() == 1

    check service.enqueue(buildTask("wakes")).isOk()
    check await log.waitPropagated(1).withTimeout(FUTURE_TIMEOUT)

  asyncTest "a round during a stop starts no task":
    ## Chronos can deliver the cancel of a stop one turn late, so a round can run
    ## during a stop.
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

  asyncTest "a retry in admission counts toward the retry limit":
    ## One retry waits in its admission, and three admitted retries wait for
    ## their replies. A fifth retry that needs no admission finds no free slot.
    let quota = newFakeQuotaProvider()
    let ids = names("s", MaxConcurrentRetries - 1)
    let processor = newScripted(stalled = ids & @["fifth"])
    let service = fakeQuotaService(processor, quota)

    # A retry that was never admitted, first in cache order.
    let parked = buildTask("parked")
    check service.enqueue(parked).isOk()
    parked.state = DeliveryState.NextRoundRetry
    processor.seen.add("parked")
    service.enqueueAdmittedRetries(
      processor, ids.mapIt(buildTask(it)) & @[buildTask("fifth")]
    )

    service.startSendService()
    defer:
      await service.stopSendService()
    check await quota.waitCalls(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    for id in ids:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check not await processor.retryStarted("fifth").withTimeout(FUTURE_TIMEOUT_SHORT)

  asyncTest "a first attempt in admission takes no retry slot":
    ## A new task waits in its admission, and three admitted retries wait for
    ## their replies. First attempts have no limit, so a fourth retry that needs
    ## no admission still finds a free slot.
    let quota = newFakeQuotaProvider()
    let ids = names("s", MaxConcurrentRetries)
    let processor = newScripted(stalled = ids)
    let service = fakeQuotaService(processor, quota)

    # A new task, first in cache order. Its admission waits for the quota.
    check service.enqueue(buildTask("fresh")).isOk()
    service.enqueueAdmittedRetries(processor, ids.mapIt(buildTask(it)))

    service.startSendService()
    defer:
      await service.stopSendService()
    check await quota.waitCalls(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    for id in ids:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)

  asyncTest "SendService.new refuses a serviceLoopInterval that is not positive":
    ## The scheduler waits at most this time between two rounds. A wait of zero
    ## never returns to the event loop.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    check:
      SendService
        .new(
          false,
          waku,
          manager,
          FakePropagatingProcessor(),
          serviceLoopInterval = ZeroDuration,
        )
        .errorOr("accepted") == "serviceLoopInterval must be positive"
      SendService
        .new(
          false,
          waku,
          manager,
          FakePropagatingProcessor(),
          serviceLoopInterval = chronos.milliseconds(1),
        )
        .isOk()

  asyncTest "a stop normalises a task its cancel left at FallbackRetry":
    ## A cancel skips the tail of `process`, so the stop must move the task out
    ## of `FallbackRetry`.
    let handOff = HandOffProcessor()
    let stalling = StallingProcessor()
    handOff.chain(stalling)
    let service = newService(handOff)
    let task = buildTask("stop-normalise")
    check service.enqueue(task).isOk()
    check await service.runUntilIdleInTime() # parked by the first call
    task.nextAttemptTime = Opt.none(Moment)

    service.startSendService() # hands off, then waits in the chain
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      stalling.replies.len == 1
    check task.state == DeliveryState.FallbackRetry

    await service.stopSendService()
    check:
      task.state == DeliveryState.NextRoundRetry
      not task.running

  asyncTest "a retry that comes due during a round gets its timer":
    ## The listener makes the retry due inside the round. The round does not
    ## start it, because it was not due at the start of the round. The scheduler
    ## must still wake for it, well before `serviceLoopInterval`.
    let processor = newScripted(failing = @["bad"])
    let service = newService(processor, interval = chronos.seconds(2))
    let retry = buildTask("retry")
    service.enqueueAdmittedRetries(processor, @[retry])
    retry.nextAttemptTime = Opt.some(Moment.now() + chronos.hours(1))
    let listener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          # Due now. That is after the start of this round.
          retry.nextAttemptTime = Opt.some(Moment.now()),
      )
      .expect("listen")
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)

    check service.enqueue(buildTask("bad")).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    check await processor.retryStarted("retry").withTimeout(FUTURE_TIMEOUT)

  asyncTest "a send cancelled by a stop is retried at once after the restart, with one terminal event":
    let stalling = StallingProcessor()
    # A retry after the interval would come too late for the test.
    let service = newService(stalling, interval = chronos.minutes(1))
    let task = buildTask("cancelled-by-stop")
    check service.enqueue(task).isOk()

    service.startSendService()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      stalling.replies.len == 1
    await service.stopSendService()
    check:
      stalling.replies.len == 1 and stalling.replies[0].cancelled()
      task.state == DeliveryState.NextRoundRetry
      task.nextAttemptTime.isNone()
      log.propagated.len == 0
      log.errors.len == 0

    # Due at once, well before `serviceLoopInterval`.
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

  asyncTest "a send that ends before the stop keeps its state, and the restart reports it":
    ## Nothing in the admission and the send of this task suspends. So the round
    ## takes the admission at once and starts the send, and the send ends inside
    ## that round too.
    let processor = FakePropagatingProcessor()
    let service = newService(processor)
    let task = buildTask("ended-before-stop")
    check service.enqueue(task).isOk()
    service.runRound()
    check:
      processor.calls == 1
      task.state == DeliveryState.SuccessfullyPropagated
      task.running # no round took the send yet

    await service.stopSendService()
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      not task.running
      log.propagated.len == 0

    service.startSendService()
    defer:
      await service.stopSendService()
    check await log.waitPropagated(1).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check not await log.waitPropagated(2).withTimeout(FUTURE_TIMEOUT_SHORT)
    check:
      processor.calls == 1 # not sent again
      log.propagated == @[task.requestId]

  asyncTest "enqueue refuses a duplicate hash, a full cache and a stopped service":
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

    check await service.runUntilIdleInTime()
    check processor.calls == 2 # `first` and `second`

    await service.stopSendService()
    check service.enqueue(buildTask("fourth")).errorOr("accepted") ==
      "Send service is stopped"

  asyncTest "the scheduler returns to the event loop between events":
    ## Every retry slot is in use, and one retry is past due. Only events and the
    ## cleanup time wake the scheduler.
    let ids = names("w", MaxConcurrentRetries + 1)
    let processor = newScripted(stalled = ids)
    let service = newService(processor)
    let tasks = ids.mapIt(buildTask(it))
    await service.runFirstAttempts(tasks)
    tasks[^1].nextAttemptTime = Opt.some(Moment.now() - chronos.seconds(1))

    let startedAt = Moment.now()
    service.startSendService()
    for id in ids[0 ..< MaxConcurrentRetries]:
      check await processor.retryStarted(id).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    # The retries start at once, also while a past due retry waits for a slot.
    check Moment.now() - startedAt < FUTURE_TIMEOUT_MEDIUM

    let before = service.roundCount()
    check service.enqueue(buildTask("fresh")).isOk()
    checkUntilTimeoutCustom(FUTURE_TIMEOUT_MEDIUM, PollInterval):
      "fresh" in processor.seen
    # The enqueue, and the ends of the admission and of the send of `fresh`, each
    # wake the scheduler once. The cleanup time comes at most once.
    let windowStart = Moment.now()
    check not await service.waitRoundsAbove(before + 5).withTimeout(
      chronos.milliseconds(300)
    )
    check Moment.now() - windowStart < chronos.seconds(2) # the event loop keeps time

    # A scheduler that never waits cannot get the cancel of the stop, so the
    # stop must end at once.
    let stopStart = Moment.now()
    await service.stopSendService()
    check Moment.now() - stopStart < FUTURE_TIMEOUT_MEDIUM
