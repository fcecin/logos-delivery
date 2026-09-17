{.used.}

import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
import ../testlib/[testasync, wakunodeconf]

## The service pass sends the queued tasks in batches of `MaxSendsInFlight`
## instead of one at a time, so one send that waits on a mix reply does not
## hold every other queued message. Admission stays sequential. A scripted
## processor stalls, fails or completes each task on demand and records what
## ran concurrently.

type ScriptedProcessor = ref object of BaseSendProcessor
  ## The first `process` of a task parks it (`NextRoundRetry`), which is how
  ## `send()` puts a task in the cache for the pass to retry. Every later call
  ## follows the script: tasks in `stalled` wait on `gate`, tasks in `raising`
  ## raise, the rest propagate at once.
  gate: Future[void]
  stalled: seq[string]
  raising: seq[string]
  seen: seq[string]
  retries: seq[string] ## order of the retry calls
  running: int
  peakRunning: int
  cancelled: seq[string]

method process(self: ScriptedProcessor, task: DeliveryTask): Future[void] {.async.} =
  let id = $task.requestId
  if id notin self.seen:
    self.seen.add(id)
    task.state = DeliveryState.NextRoundRetry
    return

  self.retries.add(id)
  inc self.running
  self.peakRunning = max(self.peakRunning, self.running)
  defer:
    dec self.running

  if id in self.raising:
    raise newException(ValueError, "scripted failure for " & id)
  if id in self.stalled:
    try:
      await self.gate
    except CancelledError as exc:
      self.cancelled.add(id)
      raise exc

  task.state = DeliveryState.SuccessfullyPropagated
  if task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

proc newScripted(
    stalled: seq[string] = @[], raising: seq[string] = @[]
): ScriptedProcessor =
  ScriptedProcessor(
    gate: newFuture[void]("send-loop-gate"), stalled: stalled, raising: raising
  )

type HandOffProcessor = ref object of BaseSendProcessor
  ## Overrides `sendImpl`, not `process`, so the base chain runs. The first
  ## call parks the task (as `send()` needs); every later call hands it off to
  ## the fallback processor, as the mix processor does for `Preferred`.
  calls: int

method isValidProcessor(self: HandOffProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: HandOffProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state =
    if self.calls == 1: DeliveryState.NextRoundRetry else: DeliveryState.FallbackRetry

type RaisingProcessor = ref object of BaseSendProcessor
  calls: int

method isValidProcessor(self: RaisingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: RaisingProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  raise newException(ValueError, "scripted fallback failure")

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc fixedEpochQuota(epoch: ref uint64, userMessageLimit: uint64): QuotaProvider =
  ## `epoch` is a ref so the test can roll the epoch without taking the
  ## address of a local.
  return proc(): Opt[EpochQuota] {.gcsafe, raises: [].} =
    return Opt.some(EpochQuota(epochIndex: epoch[], userMessageLimit: userMessageLimit))

suite "SendService - batched send pass":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc buildTask(id: string): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/send-loop/proto",
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
    )

  proc newService(
      processor: ScriptedProcessor, maxSendsInFlight = MaxSendsInFlight
  ): SendService =
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService
      .new(false, waku, manager, processor, maxSendsInFlight = maxSendsInFlight)
      .expect("SendService.new")

  proc queue(service: SendService, tasks: seq[DeliveryTask]) {.async.} =
    ## `send()` parks each task in the cache (the processor's first call).
    for task in tasks:
      await service.send(task)
      check task.state == DeliveryState.NextRoundRetry

  asyncTest "a send that waits on its reply does not hold the other queued sends":
    let processor = newScripted(stalled = @["a"])
    let service = newService(processor)
    let a = buildTask("a")
    let b = buildTask("b")
    let c = buildTask("c")
    await service.queue(@[a, b, c])

    let pass = service.trySendMessages()
    await sleepAsync(chronos.milliseconds(50))
    check:
      not pass.finished()
      a.state == DeliveryState.NextRoundRetry # still waiting on its reply
      b.state == DeliveryState.SuccessfullyPropagated
      c.state == DeliveryState.SuccessfullyPropagated
      processor.peakRunning >= 2

    processor.gate.complete()
    await pass
    check a.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a pass starts at most maxSendsInFlight sends before waiting for them":
    let processor = newScripted(stalled = @["a", "b", "c"])
    let service = newService(processor, maxSendsInFlight = 2)
    await service.queue(@[buildTask("a"), buildTask("b"), buildTask("c")])

    let pass = service.trySendMessages()
    await sleepAsync(chronos.milliseconds(50))
    check:
      processor.retries == @["a", "b"] # c waits for the first batch
      processor.peakRunning == 2

    processor.gate.complete()
    await pass
    check:
      processor.retries == @["a", "b", "c"]
      processor.peakRunning == 2

  asyncTest "with a batch of one the pass is the old sequential loop":
    let processor = newScripted()
    let service = newService(processor, maxSendsInFlight = 1)
    await service.queue(@[buildTask("a"), buildTask("b"), buildTask("c")])

    await service.trySendMessages()
    check:
      processor.retries == @["a", "b", "c"]
      processor.peakRunning == 1

  asyncTest "admission stays sequential and in order":
    ## Budget of one per epoch: the first task is admitted and sent, the second
    ## is parked and never handed to the processor, whatever the batch size.
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
    await service.send(first) # admitted, parked by the processor's first call
    check:
      first.firstAdmittedTime.isSome()
      manager.sentInCurrentEpoch == 1'u64
    await service.send(second) # over budget, parked before the processor
    check:
      manager.sentInCurrentEpoch == 1'u64
      second.firstAdmittedTime.isNone()

    await service.trySendMessages()
    check:
      processor.retries == @["in-budget"]
      first.state == DeliveryState.SuccessfullyPropagated
      second.state == DeliveryState.NextRoundRetry

  asyncTest "a send that raises does not end the pass":
    let processor = newScripted(raising = @["boom"])
    let service = newService(processor)
    let boom = buildTask("boom")
    let fine = buildTask("fine")
    await service.queue(@[boom, fine])

    await service.trySendMessages()
    check:
      fine.state == DeliveryState.SuccessfullyPropagated
      boom.state == DeliveryState.NextRoundRetry # left for the next round

    # The service keeps working: the next pass retries the failed task.
    processor.raising = @[]
    await service.trySendMessages()
    check boom.state == DeliveryState.SuccessfullyPropagated

  asyncTest "stopping the service cancels the sends still in flight":
    let processor = newScripted(stalled = @["stall"])
    let service = newService(processor)
    await service.queue(@[buildTask("stall")])

    service.startSendService()
    await sleepAsync(chronos.milliseconds(50))
    check processor.retries == @["stall"]

    await service.stopSendService()
    # Cancelling the send cancels the wait it was in, so the gate ends
    # cancelled rather than completed: nobody released it.
    check:
      processor.cancelled == @["stall"]
      processor.gate.cancelled()

  asyncTest "stopping the service cancels a pass started directly":
    ## A pass parked on its batch empties `inFlight` the moment its cancelled
    ## send finishes, which is inside the stop's own wait: the stop must take
    ## the batch before it awaits anything, or it iterates a seq that changes
    ## under it.
    let processor = newScripted(stalled = @["s"])
    let service = newService(processor)
    await service.queue(@[buildTask("s")])

    let pass = service.trySendMessages() # parks on its batch
    await sleepAsync(chronos.milliseconds(50))
    check not pass.finished()

    await service.stopSendService()
    # The stop cancelled the batch, so this one-batch pass ends; bounded, so a
    # stop that leaves the batch running fails this test instead of hanging it.
    check await pass.withTimeout(chronos.seconds(2))
    check processor.cancelled == @["s"]

  asyncTest "a raise in a fallback processor still leaves the task for the next round":
    ## `process` turns a hand-off into `NextRoundRetry` in its tail, which a
    ## raise mid-chain skips. The drain must normalise the state itself, or
    ## the task sits at `FallbackRetry`, a state no pass selects, until the
    ## reaper fails it minutes later.
    let handOff = HandOffProcessor()
    let raising = RaisingProcessor()
    handOff.chain(raising)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service =
      SendService.new(false, waku, manager, handOff).expect("SendService.new")
    let task = buildTask("fallback-raise")
    await service.send(task) # parked by the first call
    check task.state == DeliveryState.NextRoundRetry

    await service.trySendMessages() # the hand-off, then the fallback raises
    check:
      raising.calls == 1
      task.state == DeliveryState.NextRoundRetry # left for the next round

    await service.trySendMessages()
    check raising.calls == 2 # ... and the next round did retry it
