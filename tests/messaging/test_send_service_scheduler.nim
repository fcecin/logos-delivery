{.used.}

import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_node,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/waku_store,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/store,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakunodeconf, wakucore, wakunode]
import ../waku_store/store_utils

## Scheduler-level coverage for the send service's rate-limit seam: a task is
## charged exactly once however many rounds it takes, and an over-budget task
## is parked then released when the epoch rolls. A fake processor scripts the
## delivery outcome so the loop runs without network or sleeps.

type FakeSendProcessor = ref object of BaseSendProcessor
  calls: int
  script: seq[DeliveryState]
    ## State to stamp on the task per invocation; the last entry repeats.

method process(self: FakeSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  let outcome = self.script[min(self.calls, self.script.high)]
  inc self.calls
  task.state = outcome
  if outcome == DeliveryState.SuccessfullyPropagated and
      task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

type SendEventLog = ref object
  ## Record send events and notify tests when the log changes.
  brokerCtx: BrokerContext
  propagated: seq[RequestId]
  sent: seq[RequestId]
  failed: seq[RequestId]
  changed: AsyncEvent
  propagatedListener: MessagePropagatedEventListener
  sentListener: MessageSentEventListener
  errorListener: MessageErrorEventListener

proc newSendEventLog(brokerCtx: BrokerContext): SendEventLog =
  let log = SendEventLog(brokerCtx: brokerCtx, changed: newAsyncEvent())
  let onPropagated = proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
    log.propagated.add(event.requestId)
    log.changed.fire()
  let onSent = proc(event: MessageSentEvent) {.async: (raises: []).} =
    log.sent.add(event.requestId)
    log.changed.fire()
  let onError = proc(event: MessageErrorEvent) {.async: (raises: []).} =
    log.failed.add(event.requestId)
    log.changed.fire()
  log.propagatedListener =
    MessagePropagatedEvent.listen(brokerCtx, onPropagated).expect("listen propagated")
  log.sentListener = MessageSentEvent.listen(brokerCtx, onSent).expect("listen sent")
  log.errorListener =
    MessageErrorEvent.listen(brokerCtx, onError).expect("listen error")
  return log

proc teardown(log: SendEventLog) {.async.} =
  await MessagePropagatedEvent.dropListener(log.brokerCtx, log.propagatedListener)
  await MessageSentEvent.dropListener(log.brokerCtx, log.sentListener)
  await MessageErrorEvent.dropListener(log.brokerCtx, log.errorListener)

proc waitUntil(
    log: SendEventLog, pred: proc(): bool {.gcsafe, raises: [].}, timeout: Duration
): Future[bool] {.async.} =
  ## Recheck pred after each event until it succeeds or the timeout expires.
  let deadline = Moment.now() + timeout
  while true:
    log.changed.clear()
    if pred():
      return true
    let remaining = deadline - Moment.now()
    if remaining <= ZeroDuration:
      return false
    if not await log.changed.wait().withTimeout(remaining):
      return pred()

proc fixedEpochQuota(
    epoch: ptr uint64, rateLimit: uint64, remaining: ptr uint64 = nil
): QuotaProvider =
  ## Quota pinned to whatever `epoch` (and `remaining`, when given) holds, so a
  ## test rolls the epoch or spends RLN budget by writing through the pointer.
  ## Without `remaining` the epoch's budget is reported untouched.
  return proc(): Future[Opt[EpochQuota]] {.async: (raises: []), gcsafe.} =
    let left = if remaining.isNil(): rateLimit else: remaining[]
    return
      Opt.some(EpochQuota(epochIndex: epoch[], rateLimit: rateLimit, remaining: left))

suite "SendService - rate-limit scheduling":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    ## The node is never started, so stop is best-effort cleanup.
    discard await waku.stop()

  proc buildTask(
      id, payload: string,
      timestamp: Timestamp = 1_700_000_000_000_000_000,
      ephemeral = false,
  ): DeliveryTask =
    ## Built directly rather than via `DeliveryTask.new`, which needs a broker
    ## provider only registered once the node starts.
    let msg = WakuMessage(
      contentTopic: "/test/1/scheduler/proto",
      payload: payload.toBytes(),
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

  asyncTest "a task is charged once even when delivery takes several rounds":
    ## First round fails to propagate, second succeeds. The retry must not draw a
    ## second slot: `firstAdmittedTime` guards re-admission.
    var epoch = 5'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 3),
        fixedEpochQuota(addr epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(
      script: @[DeliveryState.NextRoundRetry, DeliveryState.SuccessfullyPropagated]
    )
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")

    let task = buildTask("charge-once", "hi")
    await service.send(task)
    check:
      manager.sentInCurrentEpoch == 1'u64
      task.firstAdmittedTime.isSome()
      task.state == DeliveryState.NextRoundRetry

    await service.trySendMessages()
    check:
      manager.sentInCurrentEpoch == 1'u64 # not re-charged on retry
      processor.calls == 2
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "an over-budget task is parked, then released when the epoch rolls":
    ## Budget of one per epoch. The second send is parked until the epoch rolls,
    ## then admitted and delivered.
    var epoch = 1'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1),
        fixedEpochQuota(addr epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")

    let first = buildTask("in-budget", "one")
    await service.send(first)
    check:
      first.state == DeliveryState.SuccessfullyPropagated
      manager.sentInCurrentEpoch == 1'u64

    let second = buildTask("over-budget", "two")
    await service.send(second)
    check:
      second.state == DeliveryState.NextRoundRetry # parked
      second.firstAdmittedTime.isNone() # never admitted
    let callsWhenParked = processor.calls

    # Same epoch: still over budget, so the parked task is not handed to the
    # processor.
    await service.trySendMessages()
    check:
      second.state == DeliveryState.NextRoundRetry
      second.firstAdmittedTime.isNone()
      processor.calls == callsWhenParked

    # Epoch rolls: budget refills, the parked task is admitted and delivered.
    epoch = 2'u64
    await service.trySendMessages()
    check:
      second.firstAdmittedTime.isSome()
      second.state == DeliveryState.SuccessfullyPropagated

  asyncTest "RLN's remaining budget parks a send until the epoch rolls":
    ## The local cap has room, but RLN reports the epoch's budget spent, so the
    ## task parks; the roll refills RLN's budget and releases it.
    var epoch = 1'u64
    var remaining = 0'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 10),
        fixedEpochQuota(addr epoch, rateLimit = 10, addr remaining),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")

    let task = buildTask("rln-spent", "one")
    await service.send(task)
    check:
      task.state == DeliveryState.NextRoundRetry
      task.firstAdmittedTime.isNone()
      processor.calls == 0

    await service.trySendMessages()
    check:
      task.firstAdmittedTime.isNone()
      processor.calls == 0

    epoch = 2'u64
    remaining = 10'u64
    await service.trySendMessages()
    check:
      task.firstAdmittedTime.isSome()
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a task parked for budget reports itself queued, exactly once":
    ## The park branch is re-entered every retry round; the event must not be.
    var epoch = 1'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1),
        fixedEpochQuota(addr epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")

    var queued: seq[MessageQueuedEvent]
    discard MessageQueuedEvent
      .listen(
        waku.brokerCtx,
        proc(evt: MessageQueuedEvent) {.async: (raises: []).} =
          queued.add(evt),
      )
      .expect("listen MessageQueuedEvent")

    ## Spends the epoch's single slot; admitted, so it reports nothing.
    await service.send(buildTask("queued-in-budget", "one"))
    check queued.len == 0

    let second = buildTask("queued-over-budget", "two")
    await service.send(second)
    check:
      queued.len == 1
      queued[0].requestId == second.requestId
      queued[0].messageHash == second.msgHash.to0xHex()

    ## Still over budget: the task parks again, the event does not repeat.
    await service.trySendMessages()
    check queued.len == 1

    ## Released by the roll, and delivery emits no further queued event.
    epoch = 2'u64
    await service.trySendMessages()
    check:
      second.state == DeliveryState.SuccessfullyPropagated
      queued.len == 1

    await MessageQueuedEvent.dropAllListeners(waku.brokerCtx)

  proc approachedService(
      epoch: ptr uint64, messagesPerEpoch: uint64, processor: FakeSendProcessor
  ): (SendService, RateLimitManager) =
    ## 50% threshold, so half the budget spent is Approached.
    let manager = RateLimitManager
      .new(
        RateLimitConfig(
          enabled: true,
          epochPeriodSec: 600,
          messagesPerEpoch: messagesPerEpoch,
          approachedThresholdPercent: 50,
        ),
        fixedEpochQuota(epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")
    return (service, manager)

  asyncTest "an ephemeral message is dropped when the quota is approached":
    var epoch = 1'u64
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let (service, manager) = approachedService(addr epoch, 4, processor)

    var errors: seq[MessageErrorEvent]
    discard MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(evt: MessageErrorEvent) {.async: (raises: []).} =
          errors.add(evt),
      )
      .expect("listen MessageErrorEvent")
    var queued: seq[MessageQueuedEvent]
    discard MessageQueuedEvent
      .listen(
        waku.brokerCtx,
        proc(evt: MessageQueuedEvent) {.async: (raises: []).} =
          queued.add(evt),
      )
      .expect("listen MessageQueuedEvent")

    await service.send(buildTask("durable-1", "one"))
    await service.send(buildTask("durable-2", "two"))
    check (await manager.quotaState()) == QuotaState.Approached
    let callsBefore = processor.calls

    let eph = buildTask("ephemeral-approached", "eph", ephemeral = true)
    await service.send(eph)
    check:
      eph.state == DeliveryState.FailedToDeliver
      eph.firstAdmittedTime.isNone() # no slot, so no RLN proof either
      manager.sentInCurrentEpoch == 2'u64
      processor.calls == callsBefore
      queued.len == 0
      errors.len == 1
      errors[0].requestId == eph.requestId
      errors[0].messageHash == eph.msgHash.to0xHex()

    ## Not parked: a later round never hands it to the processor.
    await service.trySendMessages()
    check processor.calls == callsBefore

    await MessageErrorEvent.dropAllListeners(waku.brokerCtx)
    await MessageQueuedEvent.dropAllListeners(waku.brokerCtx)

  asyncTest "an ephemeral message is dropped, not parked, when the quota is exhausted":
    var epoch = 1'u64
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let (service, manager) = approachedService(addr epoch, 1, processor)

    var errors: seq[MessageErrorEvent]
    discard MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(evt: MessageErrorEvent) {.async: (raises: []).} =
          errors.add(evt),
      )
      .expect("listen MessageErrorEvent")

    await service.send(buildTask("durable-spends", "one"))
    check (await manager.quotaState()) == QuotaState.Exhausted
    let callsBefore = processor.calls

    let eph = buildTask("ephemeral-exhausted", "eph", ephemeral = true)
    await service.send(eph)
    check:
      eph.state == DeliveryState.FailedToDeliver
      eph.firstAdmittedTime.isNone()
      errors.len == 1

    ## Budget refills, but the dropped message stays dropped.
    epoch = 2'u64
    await service.trySendMessages()
    check:
      processor.calls == callsBefore
      (await manager.quotaState()) == QuotaState.Normal
      manager.sentInCurrentEpoch == 0'u64

    await MessageErrorEvent.dropAllListeners(waku.brokerCtx)

  asyncTest "an ephemeral message below the threshold is charged and sent":
    var epoch = 1'u64
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let (service, manager) = approachedService(addr epoch, 4, processor)

    let eph = buildTask("ephemeral-normal", "eph", ephemeral = true)
    await service.send(eph)
    check:
      eph.state == DeliveryState.SuccessfullyPropagated
      eph.firstAdmittedTime.isSome()
      manager.sentInCurrentEpoch == 1'u64

  asyncTest "a durable message is still admitted when the quota is approached":
    var epoch = 1'u64
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let (service, manager) = approachedService(addr epoch, 4, processor)

    await service.send(buildTask("durable-a", "a"))
    await service.send(buildTask("durable-b", "b"))
    check (await manager.quotaState()) == QuotaState.Approached

    let durable = buildTask("durable-approached", "c")
    await service.send(durable)
    check:
      durable.state == DeliveryState.SuccessfullyPropagated
      manager.sentInCurrentEpoch == 3'u64

  proc listenErrors(
      brokerCtx: BrokerContext,
      errors: ptr seq[MessageErrorEvent],
      seen: AsyncEvent = nil,
  ) =
    discard MessageErrorEvent
      .listen(
        brokerCtx,
        proc(evt: MessageErrorEvent) {.async: (raises: []).} =
          errors[].add(evt)
          if not seen.isNil():
            seen.fire()
        ,
      )
      .expect("listen MessageErrorEvent")

  asyncTest "a parked task past the max parked age fails; admitted and fresh ones stay":
    var epoch = 1'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1),
        fixedEpochQuota(addr epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(
      script: @[DeliveryState.NextRoundRetry, DeliveryState.SuccessfullyPropagated]
    )
    let service = SendService
      .new(false, waku, manager, processor, maxParkedAge = chronos.seconds(30))
      .expect("SendService.new")

    var errors: seq[MessageErrorEvent]
    listenErrors(waku.brokerCtx, addr errors)

    ## Admitted, still retrying, with a timestamp far older than the limit.
    let admitted = buildTask("admitted-old", "one")
    await service.send(admitted)
    let now = getNowInNanosecondTime()
    let stale = buildTask("parked-stale", "two", now - 60_000_000_000)
    let fresh = buildTask("parked-fresh", "three", now)
    await service.send(stale)
    await service.send(fresh)
    check:
      admitted.firstAdmittedTime.isSome()
      stale.firstAdmittedTime.isNone()
      fresh.firstAdmittedTime.isNone()

    service.evaluateAndCleanUp()
    await sleepAsync(chronos.milliseconds(10))
    check:
      stale.state == DeliveryState.FailedToDeliver
      errors.len == 1
      errors[0].requestId == stale.requestId
      admitted.state == DeliveryState.NextRoundRetry
      fresh.state == DeliveryState.NextRoundRetry

    ## The fresh parked task is still released when the epoch rolls.
    epoch = 2'u64
    await service.trySendMessages()
    check:
      fresh.firstAdmittedTime.isSome()
      fresh.state == DeliveryState.SuccessfullyPropagated

    await MessageErrorEvent.dropAllListeners(waku.brokerCtx)

  asyncTest "sends beyond the task cache cap are rejected with an error event":
    var epoch = 1'u64
    let manager = RateLimitManager
      .new(
        RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1),
        fixedEpochQuota(addr epoch, rateLimit = 100),
      )
      .expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.NextRoundRetry])
    let service = SendService
      .new(false, waku, manager, processor, maxTaskCacheSize = 2)
      .expect("SendService.new")

    var errors: seq[MessageErrorEvent]
    listenErrors(waku.brokerCtx, addr errors)

    await service.send(buildTask("cap-admitted", "one"))
    await service.send(buildTask("cap-parked", "two"))
    check:
      service.isFull()
      errors.len == 0

    let rejected = buildTask("cap-rejected", "three")
    await service.send(rejected)
    await sleepAsync(chronos.milliseconds(10))
    check:
      errors.len == 1
      errors[0].requestId == rejected.requestId
      rejected.state == DeliveryState.Entry # never scheduled
      processor.calls == 1

    await MessageErrorEvent.dropAllListeners(waku.brokerCtx)

  asyncTest "a propagated task past the validation window fails; a fresh one stays":
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service = SendService
      .new(true, waku, manager, processor, maxValidationAge = chronos.seconds(30))
      .expect("SendService.new")

    var errors: seq[MessageErrorEvent]
    let errorSeen = newAsyncEvent()
    listenErrors(waku.brokerCtx, addr errors, errorSeen)
    defer:
      await MessageErrorEvent.dropAllListeners(waku.brokerCtx)

    let expired = buildTask("validation-expired", "one")
    let fresh = buildTask("validation-fresh", "two")
    await service.send(expired)
    await service.send(fresh)
    expired.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(60))

    service.evaluateAndCleanUp()
    check:
      expired.state == DeliveryState.FailedToDeliver
      fresh.state == DeliveryState.SuccessfullyPropagated
      await errorSeen.wait().withTimeout(chronos.seconds(1))
      errors.len == 1
    if errors.len == 1:
      check errors[0].requestId == expired.requestId

  asyncTest "with reliability off a propagated task is evicted without MessageError":
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service = SendService
      .new(false, waku, manager, processor, maxValidationAge = chronos.seconds(30))
      .expect("SendService.new")

    let task = buildTask("reliability-off", "one")
    await service.send(task)
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(60))

    service.evaluateAndCleanUp()
    check task.state == DeliveryState.SuccessfullyPropagated

proc propagatedTask(
    i: int, now: Moment, propagatedAgo: Duration, ephemeral = false
): DeliveryTask =
  ## Build a propagated task with a controlled propagation time.
  let msg = WakuMessage(
    contentTopic: "/test/1/batches/proto",
    payload: ("m" & $i).toBytes(),
    timestamp: 1_700_000_000_000_000_000'i64 + int64(i),
    ephemeral: ephemeral,
  )
  let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  return DeliveryTask(
    requestId: RequestId("t" & $i),
    pubsubTopic: pubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(pubsubTopic, msg),
    state: DeliveryState.SuccessfullyPropagated,
    firstPropagatedTime: Opt.some(now - propagatedAgo),
  )

suite "SendService - store validation selection":
  var waku {.threadvar.}: Waku
  var service {.threadvar.}: SendService

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    let manager = RateLimitManager.new(RateLimitConfig(enabled: false), nil).expect(
        "RateLimitManager.new"
      )
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    service = SendService.new(true, waku, manager, processor).expect("SendService.new")

  asyncTeardown:
    discard await waku.stop()

  asyncTest "a page of the tasks that waited longest, in cache order":
    let now = Moment.now()
    var tasks: seq[DeliveryTask]
    for i in 0 ..< 250:
      tasks.add(propagatedTask(i, now, chronos.seconds(10)))

    let first = service.nextStoreValidationBatch(tasks, now)
    check:
      first.len == 100
      first[0].requestId == RequestId("t0")
      first[99].requestId == RequestId("t99")
    for task in first:
      task.lastStoreQueryTime = Opt.some(now)

    let second = service.nextStoreValidationBatch(tasks, now)
    check:
      second.len == 100
      second[0].requestId == RequestId("t100")
    for task in second:
      task.lastStoreQueryTime = Opt.some(now)

    check service.nextStoreValidationBatch(tasks, now).len == 50

  asyncTest "a task never asked outranks the tasks asked since it propagated":
    ## The first 100 tasks are eligible again after five seconds.
    ## The unqueried 101st task must still come first.
    let now = Moment.now()
    var tasks: seq[DeliveryTask]
    for i in 0 ..< 101:
      tasks.add(propagatedTask(i, now, chronos.seconds(10)))
    let page = service.nextStoreValidationBatch(tasks, now)
    check page.len == 100
    for task in page:
      task.lastStoreQueryTime = Opt.some(now)

    let next = service.nextStoreValidationBatch(tasks, now + chronos.seconds(4))
    check:
      next.len == 100
      next[0].requestId == RequestId("t100")
      next[1].requestId == RequestId("t0") # Resume previously queried tasks.

  asyncTest "newer arrivals do not displace a task that already waited":
    let now = Moment.now()
    var tasks: seq[DeliveryTask]
    for i in 0 ..< 100:
      tasks.add(propagatedTask(i, now, chronos.seconds(10)))
    for task in service.nextStoreValidationBatch(tasks, now):
      task.lastStoreQueryTime = Opt.some(now)
    # Add tasks propagated one second after the previous query.
    for i in 100 ..< 250:
      tasks.add(propagatedTask(i, now + chronos.seconds(1), chronos.seconds(0)))

    let later = service.nextStoreValidationBatch(tasks, now + chronos.seconds(5))
    check:
      later.len == 100
      later[0].requestId == RequestId("t0") # Queried before the new tasks propagated.
      later[99].requestId == RequestId("t99")
    for task in later:
      task.lastStoreQueryTime = Opt.some(now + chronos.seconds(5))
    let afterwards = service.nextStoreValidationBatch(tasks, now + chronos.seconds(5))
    check:
      afterwards.len == 100
      afterwards[0].requestId == RequestId("t100")

  asyncTest "young, ephemeral, mixed and non-propagated tasks are not asked":
    let now = Moment.now()
    let retrying = propagatedTask(4, now, chronos.seconds(10))
    retrying.state = DeliveryState.NextRoundRetry
    let mixed = propagatedTask(5, now, chronos.seconds(10))
    mixed.propagatedAnonymously = true
    let tasks = @[
      propagatedTask(1, now, chronos.seconds(1)), # Still within ArchiveTime.
      propagatedTask(2, now, chronos.seconds(10), ephemeral = true),
      propagatedTask(3, now, chronos.seconds(10)),
      retrying,
      mixed,
    ]
    let batch = service.nextStoreValidationBatch(tasks, now)
    check:
      batch.len == 1
      batch[0].requestId == RequestId("t3")

suite "SendService - store validation worker":
  ## Store responses wait for `gate` so tests can observe pending queries.
  ## The test peer mounts metadata so the peer manager accepts its cluster.
  var waku {.threadvar.}: Waku
  var storeNode {.threadvar.}: WakuNode
  var gate {.threadvar.}: Future[void]
  var queryStarted {.threadvar.}: Future[void]
  var answered {.threadvar.}: AsyncEvent
  var storedHashes {.threadvar.}: seq[WakuMessageHash]
  var log {.threadvar.}: SendEventLog

  proc gatedStoreHandler(
      req: StoreQueryRequest
  ): Future[StoreQueryResult] {.async, gcsafe.} =
    if not queryStarted.finished():
      queryStarted.complete()
    await gate
    var resp = StoreQueryResponse(
      requestId: req.requestId, statusCode: uint32(StatusCode.SUCCESS)
    )
    for hash in req.messageHashes:
      if hash in storedHashes:
        resp.messages.add(WakuMessageKeyValue(messageHash: hash))
    answered.fire()
    return ok(resp)

  proc newGatedStoreNode(): Future[WakuNode] {.async.} =
    let node = newTestWakuNode(generateSecp256k1Key())
    node.mountMetadata(TestClusterId, @[0'u16]).isOkOr:
      raiseAssert "mountMetadata: " & error
    discard await newTestWakuStore(node.switch, gatedStoreHandler)
    await node.start()
    return node

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    (await waku.start()).isOkOr:
      raiseAssert "waku.start: " & error
    gate = newFuture[void]("store gate")
    queryStarted = newFuture[void]("query started")
    answered = newAsyncEvent()
    storedHashes = @[]
    log = newSendEventLog(waku.brokerCtx)
    storeNode = await newGatedStoreNode()
    waku.node.peerManager.addServicePeer(
      storeNode.peerInfo.toRemotePeerInfo(), WakuStoreCodec
    )

  asyncTeardown:
    if not gate.finished():
      gate.complete()
    await log.teardown()
    await storeNode.stop()
    discard await waku.stop()

  proc workerTask(id, payload: string): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/scheduler/proto",
      payload: payload.toBytes(),
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

  proc newWorkerService(processor: FakeSendProcessor): SendService =
    let manager = RateLimitManager.new(RateLimitConfig(enabled: false), nil).expect(
        "RateLimitManager.new"
      )
    let service =
      SendService.new(true, waku, manager, processor).expect("SendService.new")
    service.startSendService()
    return service

  asyncTest "a slow Store does not stall the send loop's retries":
    ## Keep A's Store query pending while B retries a failed send.
    let processor = FakeSendProcessor(
      script: @[
        DeliveryState.SuccessfullyPropagated, DeliveryState.NextRoundRetry,
        DeliveryState.SuccessfullyPropagated,
      ]
    )
    let service = newWorkerService(processor)
    defer:
      await service.stopSendService()

    let taskA = workerTask("slow-store-a", "a")
    await service.send(taskA)
    check taskA.state == DeliveryState.SuccessfullyPropagated
    storedHashes.add(taskA.msgHash)
    # Make A eligible for the next Store query.
    taskA.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))

    check await queryStarted.withTimeout(chronos.seconds(15))

    let taskB = workerTask("slow-store-b", "b")
    await service.send(taskB)
    check taskB.state == DeliveryState.NextRoundRetry

    check:
      await log.waitUntil(
        proc(): bool =
          taskB.requestId in log.propagated,
        chronos.seconds(4),
      )
      not gate.finished()
      taskB.state == DeliveryState.SuccessfullyPropagated
      processor.calls == 3

    gate.complete()
    check:
      await log.waitUntil(
        proc(): bool =
          taskA.requestId in log.sent,
        chronos.seconds(5),
      )
      taskA.state == DeliveryState.SuccessfullyValidated

  asyncTest "an answer for a task cleanup already failed is ignored":
    ## Return A's confirmation after the send loop has failed A and removed it
    ## from the cache.
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service = newWorkerService(processor)
    defer:
      await service.stopSendService()

    let taskA = workerTask("expire-in-flight", "a")
    await service.send(taskA)
    storedHashes.add(taskA.msgHash)
    # Make A eligible for the next Store query.
    taskA.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))
    check await queryStarted.withTimeout(chronos.seconds(15))

    # Backdate A so the next cleanup pass fails it.
    taskA.firstPropagatedTime =
      Opt.some(Moment.now() - MaxTimeInCache - chronos.seconds(1))
    check await log.waitUntil(
      proc(): bool =
        taskA.requestId in log.failed,
      chronos.seconds(4),
    )

    # Confirm C in a later query to ensure the worker has processed A's response.
    gate.complete()
    await answered.wait()
    let taskC = workerTask("confirmed-after", "c")
    await service.send(taskC)
    storedHashes.add(taskC.msgHash)
    taskC.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))
    check:
      await log.waitUntil(
        proc(): bool =
          taskC.requestId in log.sent,
        chronos.seconds(15),
      )
      taskA.requestId notin log.sent
      taskA.state == DeliveryState.FailedToDeliver

  asyncTest "stopping the service cancels a pending Store query promptly":
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service = newWorkerService(processor)
    let task = workerTask("stop-in-flight", "s")
    await service.send(task)
    storedHashes.add(task.msgHash)
    # Make the task eligible for the next Store query.
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))
    check await queryStarted.withTimeout(chronos.seconds(15))

    let stopping = service.stopSendService()
    check:
      await stopping.withTimeout(chronos.seconds(3))
      not waku.node.peerManager.hasActiveStoreRequest(storeNode.peerInfo.peerId)

  asyncTest "a hash the Store does not report yet is asked again, never re-sent":
    ## Return an empty response, then confirm the hash in the next query.
    ## Backdate timestamps to make the task eligible without a three-second wait.
    let processor = FakeSendProcessor(script: @[DeliveryState.SuccessfullyPropagated])
    let service = newWorkerService(processor)
    defer:
      await service.stopSendService()
    gate.complete() # Allow immediate Store responses.

    let task = workerTask("no-resend", "n")
    await service.send(task)
    check processor.calls == 1
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))

    # The empty response leaves the task pending without another publish.
    check:
      await queryStarted.withTimeout(chronos.seconds(5))
      await answered.wait().withTimeout(chronos.seconds(5))
      task.state == DeliveryState.SuccessfullyPropagated
      processor.calls == 1

    # Include the hash in the next response.
    answered.clear()
    storedHashes.add(task.msgHash)
    task.lastStoreQueryTime = Opt.some(Moment.now() - chronos.seconds(10))
    check:
      await log.waitUntil(
        proc(): bool =
          task.requestId in log.sent,
        chronos.seconds(5),
      )
      task.state == DeliveryState.SuccessfullyValidated
      processor.calls == 1

  asyncTest "a cancelled Store query is cancelled, not answered with an error":
    ## Both peers hold their responses. Retrying the second peer after
    ## cancellation would leave the query pending and fail the test.
    let otherStore = await newGatedStoreNode()
    defer:
      # Release the request handler before stopping its node.
      if not gate.finished():
        gate.complete()
      await otherStore.stop()
    waku.node.peerManager.addPeer(otherStore.peerInfo.toRemotePeerInfo())
    check:
      waku.node.peerManager.switch.peerStore.getPeersByProtocol(WakuStoreCodec).len == 2

    let query = waku.storeQueryToAny(
      StoreQueryRequest(
        includeData: false,
        messageHashes: @[workerTask("cancel-in-flight", "c").msgHash],
        paginationLimit: Opt.some(1'u64),
      )
    )
    check await queryStarted.withTimeout(chronos.seconds(15))

    # Apply the timeout to join() so it cannot cancel the query under test.
    query.cancelSoon()
    check:
      await query.join().withTimeout(chronos.seconds(3))
      query.cancelled()
