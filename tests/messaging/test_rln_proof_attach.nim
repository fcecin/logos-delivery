{.used.}

import std/[options, osproc, sequtils, times, strutils]
import chronos, testutils/unittests, results, stew/byteutils
import
  logos_delivery/waku/[waku, waku_core, rln],
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/node/waku_node/relay,
  logos_delivery/waku/node/waku_node/lightpush,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/rln/[rln_api, rln_plugin],
  logos_delivery/waku/rln/rln_evm/message_id_store,
  logos_delivery/waku/rln/rln_lez/[rln_lez, transport],
  logos_delivery/waku/persistency/persistency,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service {.all.}, send_processor, delivery_task, mix_processor]
import
  ../testlib/[testasync, wakunodeconf],
  ../waku_rln_relay/utils_onchain,
  ../waku_rln_relay/rln/waku_rln_relay_utils

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

const TestEpochSizeSec = 1'u64
  ## One-second epochs keep epoch-crossing cases inside the generator's
  ## timestamp bound (`MaxClockGapSeconds`).

proc nowSec(): uint64 =
  uint64(getTime().toUnix())

proc messageAt(timestampSec: uint64, payload = "hello"): WakuMessage =
  WakuMessage(
    payload: payload.toBytes(),
    contentTopic: "/test/1/attach/proto",
    timestamp: int64(timestampSec) * 1_000_000_000,
  )

proc testMessage(): WakuMessage =
  messageAt(nowSec())

const LezQuotaReply =
  """{"error":null,"success":true,"value":{"epoch_index":42,"rate_limit":100,"remaining":7}}"""

proc lezGetQuota(
    reqId: uint64, timestamp: uint64, userData: pointer
) {.cdecl, gcsafe, raises: [].} =
  {.cast(gcsafe), cast(raises: []).}:
    discard logosdelivery_rln_response(reqId, LezQuotaReply.cstring)

var lezPlugin = LogosDeliveryRlnPlugin(get_epoch_quota: lezGetQuota)

type FailingProofBackend = ref object
  ## Fails every proof with `failWith`; the test can change it between attempts.
  ## With `succeed` set, it returns a proof instead.
  failWith: RlnErrorKind
  succeed: bool
  attempts: int

proc mountFailingRln(waku: Waku, failWith: RlnErrorKind): FailingProofBackend =
  let backend = FailingProofBackend(failWith: failWith)

  proc generate(message: WakuMessage): Future[Result[seq[byte], RlnError]] {.async.} =
    backend.attempts.inc()
    if backend.succeed:
      return ok(@[1'u8, 2, 3])
    return err(RlnError.init(backend.failWith, "scripted attach failure"))

  waku.node.rlnPlugin = Opt.some(RlnPlugin(name: "stub", generateProof: generate))
  return backend

type CountingProcessor = ref object of BaseSendProcessor
  ## Propagates every task it is given; `calls` shows whether a task got out.
  calls: int

method isValidProcessor(self: CountingProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: CountingProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.SuccessfullyPropagated

type FailFirstProcessor = ref object of BaseSendProcessor
  ## Leaves the task for a retry at its first call, and propagates it after.
  calls: int

method isValidProcessor(self: FailFirstProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: FailFirstProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state =
    if self.calls == 1:
      DeliveryState.NextRoundRetry
    else:
      DeliveryState.SuccessfullyPropagated

proc buildTask(id: string, stamped = false): DeliveryTask =
  ## As a task from `DeliveryTask.new`, the task has no timestamp and no hash:
  ## the first attempt stamps it. A `stamped` task has them from the start.
  var msg = testMessage()
  if not stamped:
    msg.timestamp = 0
  let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  let task = DeliveryTask(
    requestId: RequestId(id),
    pubsubTopic: pubsubTopic,
    msg: msg,
    state: DeliveryState.Entry,
  )
  if stamped:
    task.msgHash = computeMessageHash(pubsubTopic, msg)
  return task

suite "SendService RLN proof attach":
  asyncTest "passes the message through unproven when RLN is not mounted":
    ## The default (no-RLN) configuration must be unaffected: no proof is
    ## attached and the message reaches the send processors unchanged.
    let waku = (await Waku.new(testConf())).expect("Waku.new")
    let msg = testMessage()

    let attached = (await waku.attachRlnProof(msg)).expect("attachRlnProof")

    check:
      attached.proof.len == 0
      attached.payload == msg.payload
      attached.contentTopic == msg.contentTopic

  asyncTest "rlnEpochQuota fails when RLN is not mounted":
    ## The rate limit manager reads the failure as "use the local fallback".
    let waku = (await Waku.new(testConf())).expect("Waku.new")
    check (await waku.rlnEpochQuota(nowSec())).isErr()

  asyncTest "rlnEpochQuota reads the RLN plugin's budget when it is mounted":
    let waku = (await Waku.new(testConf())).expect("Waku.new")
    check logosdelivery_rln_set_plugin(addr lezPlugin, nil) == 0
    defer:
      discard logosdelivery_rln_set_plugin(nil, nil)
    waku.node.rlnPlugin = Opt.some(RlnLez.init().toRlnPlugin())

    let quota = (await waku.rlnEpochQuota(nowSec())).valueOr:
      raiseAssert $error
    check:
      quota.epochIndex == 42
      quota.rateLimit == 100
      quota.remaining == 7

suite "SendService RLN proof attach - failing backend":
  ## A Permanent attach error cannot clear on a later attempt, so the task fails
  ## with one `MessageErrorEvent` and never reaches a send processor.
  const PermanentReason =
    "Failed to attach RLN proof: Permanent: scripted attach failure"

  var
    waku {.threadvar.}: Waku
    errors {.threadvar.}: seq[MessageErrorEvent]
    listener {.threadvar.}: MessageErrorEventListener

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    errors = @[]
    listener = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageErrorEvent) {.async: (raises: []).} =
          errors.add(e),
      )
      .expect("listen")

  asyncTeardown:
    await MessageErrorEvent.dropListener(waku.brokerCtx, listener)
    discard await waku.stop()

  proc newService(processor: BaseSendProcessor): SendService =
    ## A service that holds one task, so `isFull` tells whether it holds the task.
    ## It ticks each 10 minutes, so `retryNow` starts each attempt.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService
      .new(
        false,
        waku,
        manager,
        processor,
        sendQueueCapacity = 1,
        retryInterval = chronos.minutes(10),
      )
      .expect("SendService.new")

  asyncTest "a first attempt fails the task at once, with one event":
    let backend = waku.mountFailingRln(RlnErrorKind.Permanent)
    let processor = CountingProcessor()
    let service = newService(processor)
    let task = buildTask("permanent-in-first-attempt")

    check service.enqueue(task).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    checkUntilTimeout:
      errors.len == 1
    check:
      task.state == DeliveryState.FailedToDeliver
      processor.calls == 0
      errors.mapIt(it.requestId) == @[task.requestId]
      errors.mapIt(it.error) == @[PermanentReason]
      backend.attempts == 1
      not service.isFull() # removed, so it is not retried or reported again

  asyncTest "a retry fails a task whose proof attach turns Permanent":
    ## The task waits for its proof while the backend is not ready. By the retry its
    ## message epoch can be out of reach, which the backend reports as Permanent.
    let backend = waku.mountFailingRln(RlnErrorKind.NotReady)
    let processor = CountingProcessor()
    let service = newService(processor)
    let task = buildTask("permanent-in-retry")

    check service.enqueue(task).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry
    check:
      backend.attempts == 1
      errors.len == 0

    backend.failWith = RlnErrorKind.Permanent
    service.retryNow()
    checkUntilTimeout:
      errors.len == 1
    check:
      task.state == DeliveryState.FailedToDeliver
      processor.calls == 0
      errors.mapIt(it.requestId) == @[task.requestId]
      errors.mapIt(it.error) == @[PermanentReason]
      backend.attempts == 2
      not service.isFull() # removed, so no later attempt reserves an id for it

  asyncTest "a task that waited for the backend is stamped at the attempt that gets its proof":
    ## A NotReady attach leaves no wire object behind. The retry stamps the task
    ## again, so its proof and its timestamp come from the same moment.
    let backend = waku.mountFailingRln(RlnErrorKind.NotReady)
    let processor = CountingProcessor()
    let service = newService(processor)
    let task = buildTask("stamped-with-proof")

    check service.enqueue(task).isOk()
    service.startSendService()
    defer:
      await service.stopSendService()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry
    check:
      backend.attempts == 1
      not task.isStamped()
      task.msg.proof.len == 0

    let beforeRetry = getNowInNanosecondTime()
    backend.succeed = true
    service.retryNow()
    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      backend.attempts == 2
      processor.calls == 1
      task.isStamped()
      task.msg.timestamp >= beforeRetry
      task.msg.proof.len > 0
      errors.len == 0

suite "SendService RLN proof attach - RLN mounted":
  var
    waku {.threadvar.}: Waku
    onchainRln {.threadvar.}: RlnEvm
    anvilProc {.threadvar.}: Process
    manager {.threadvar.}: RlnEvmGroupManager

  asyncSetup:
    anvilProc = runAnvil(stateFile = Opt.some(DEFAULT_ANVIL_STATE_PATH))
    manager = await setupRlnEvm(deployContracts = false)

    waku = (await Waku.new(testConf())).expect("Waku.new")
    onchainRln = await waku.node.mountOnchainRln(
      getWakuRlnConfig(
        manager = manager,
        userMessageLimit = 20,
        index = MembershipIndex(1),
        epochSizeSec = TestEpochSizeSec,
      )
    )

    let credentials = generateCredentials()
    (
      await cast[RlnEvmGroupManager](onchainRln.groupManager).register(
        credentials, UserMessageLimit(20)
      )
    ).isOkOr:
      assert false, "failed to register RLN credentials: " & error

  asyncTeardown:
    ## Stops the on-chain backend's background work (group sync, epoch
    ## monitor) so it does not outlive the test.
    try:
      await onchainRln.stop()
    except Exception:
      assert false, "failed to stop RLN: " & getCurrentExceptionMsg()
    stopAnvil(anvilProc)

  asyncTest "the EVM size preview equals the generated proof length":
    let msg = testMessage()
    let preview = waku.messageForSizeCheck(msg).expect("messageForSizeCheck")
    check:
      onchainRln.nonceManager.nextId == 0
      msg.proof.len == 0
    let proven = (await waku.attachRlnProof(msg)).expect("attachRlnProof")
    check:
      preview.proof.len == proven.proof.len
      mixLightpushSize(PubsubTopic("/waku/2/rs/3/0"), preview).size ==
        mixLightpushSize(PubsubTopic("/waku/2/rs/3/0"), proven).size

  asyncTest "an oversized Required send leaves EVM message ids and admission budget untouched":
    let task = buildTask("oversized-with-evm", stamped = true)
    let limit = mixLightpushSize(task.pubsubTopic, task.msg).limit
    for n in 0 .. limit:
      task.msg.payload = newSeq[byte](n)
      if mixLightpushSize(task.pubsubTopic, task.msg).size == limit:
        break
    check mixLightpushSize(task.pubsubTopic, task.msg).size == limit
    task.msgHash = computeMessageHash(task.pubsubTopic, task.msg)
    let quota = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1))
      .expect("RateLimitManager.new")
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Required, chronos.minutes(1)
    )
    let service = SendService
      .new(false, waku, quota, mix, retryInterval = chronos.hours(1))
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.state == DeliveryState.FailedToDeliver
    check:
      task.errorDesc.startsWith(MixTooLargeReason)
      task.msg.proof.len == 0
      task.firstAdmittedTime.isNone()
      onchainRln.nonceManager.nextId == 0
      quota.sentInCurrentEpoch == 0

  asyncTest "a task parked for budget across an epoch roll gets a proof of its release epoch":
    ## The local cap parks the second send. By its release, a proof drew the
    ## first id of the next epoch. The stamp at the release gives the parked task
    ## a timestamp of that epoch, so its proof draws from that epoch too.
    let processor = CountingProcessor()
    let manager = RateLimitManager
      .new(
        RateLimitConfig(
          enabled: true, epochPeriodSec: TestEpochSizeSec, messagesPerEpoch: 1
        ),
        proc(): Future[Opt[EpochQuota]] {.async: (raises: []), gcsafe.} =
          let res =
            try:
              await waku.rlnEpochQuota(nowSec())
            except CatchableError:
              return Opt.none(EpochQuota)
          let quota = res.valueOr:
            return Opt.none(EpochQuota)
          return Opt.some(quota),
      )
      .expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, processor, retryInterval = chronos.minutes(10))
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()

    # From the start of an epoch, so the first send and the park share it.
    let before = nowSec()
    while nowSec() == before:
      await sleepAsync(chronos.milliseconds(10))
    let epoch = nowSec()
    let first = buildTask("in-budget")
    let parked = buildTask("parked-across-roll")
    check:
      service.enqueue(first).isOk()
      service.enqueue(parked).isOk()
    checkUntilTimeout:
      first.state == DeliveryState.SuccessfullyPropagated and
        parked.state == DeliveryState.NextRoundRetry
    check:
      parked.firstAdmittedTime.isNone()
      not parked.isStamped()
      onchainRln.nonceManager.epochIndex == epoch div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64

    # The epoch rolls, and a proof draws the first id of the new epoch.
    while nowSec() == epoch:
      await sleepAsync(chronos.milliseconds(10))
    discard (await waku.attachRlnProof(messageAt(nowSec(), "next-epoch"))).expect(
      "proof in the next epoch"
    )
    check onchainRln.nonceManager.epochIndex > epoch div TestEpochSizeSec

    service.retryNow()
    checkUntilTimeout:
      parked.state == DeliveryState.SuccessfullyPropagated
    check:
      parked.isStamped()
      uint64(parked.msg.timestamp div 1_000_000_000) > epoch
      parked.msg.proof.len > 0
      processor.calls == 2

  asyncTest "a retry in a later epoch makes a new wire object with a proof of that epoch":
    ## The sender knows the epoch size, so it does not retry a proof of a past
    ## epoch. The retry gets a new timestamp, a new hash and the first id of the
    ## new epoch.
    let processor = FailFirstProcessor()
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, processor, retryInterval = chronos.minutes(10))
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()

    let before = nowSec()
    while nowSec() == before:
      await sleepAsync(chronos.milliseconds(10))
    let epoch = nowSec()
    let task = buildTask("retry-next-epoch")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry
    let firstHash = task.msgHash
    check:
      task.isStamped()
      task.stampTried
      task.msg.proof.len > 0
      onchainRln.nonceManager.epochIndex == epoch div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64

    while nowSec() == epoch:
      await sleepAsync(chronos.milliseconds(10))
    service.retryNow()
    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    let stampSec = uint64(task.msg.timestamp div 1_000_000_000)
    check:
      processor.calls == 2
      task.msgHash != firstHash
      stampSec > epoch
      task.msg.proof.len > 0
      onchainRln.nonceManager.epochIndex == stampSec div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64

  asyncTest "a retry in the same epoch keeps its wire object and its proof":
    let processor = FailFirstProcessor()
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, processor, retryInterval = chronos.minutes(10))
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()

    let before = nowSec()
    while nowSec() == before:
      await sleepAsync(chronos.milliseconds(10))
    let epoch = nowSec()
    let task = buildTask("retry-same-epoch")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      task.state == DeliveryState.NextRoundRetry
    let firstHash = task.msgHash
    let firstProof = task.msg.proof
    service.retryNow()
    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    if nowSec() != epoch:
      # The epoch rolled during the test, so the retry had to make a new object.
      skip()
      return
    check:
      processor.calls == 2
      task.msgHash == firstHash
      task.msg.proof == firstProof
      onchainRln.nonceManager.nextId == 1'u64

  asyncTest "attaches a proof":
    let attached = (await waku.attachRlnProof(testMessage())).expect("attachRlnProof")

    check attached.proof.len > 0

  asyncTest "rlnEpochQuota's remaining budget drops as proofs spend it":
    ## Wires the rate limit manager to RLN: admission stops at
    ## `remaining == 0` and the window rolls on `epochIndex`. The budget is
    ## the one of the epoch the message's timestamp falls in.
    let now = nowSec()
    let before = (await waku.rlnEpochQuota(now)).expect("rlnEpochQuota")
    discard (await waku.attachRlnProof(messageAt(now))).expect("attachRlnProof")
    let after = (await waku.rlnEpochQuota(now)).expect("rlnEpochQuota")
    check:
      before.rateLimit == 20'u64 # the mounted userMessageLimit
      before.remaining == 20'u64
      before.epochIndex == now div TestEpochSizeSec
      after.remaining == 19'u64
      after.epochIndex == before.epochIndex

  asyncTest "draws consecutive message ids from the message timestamp's epoch":
    ## The id is spent in the epoch the proof carries, which is derived from
    ## the message timestamp and not from the wall clock at proof time.
    let now = nowSec()
    discard (await waku.attachRlnProof(messageAt(now, "a"))).expect("first")
    discard (await waku.attachRlnProof(messageAt(now, "b"))).expect("second")

    check:
      onchainRln.nonceManager.epochIndex == now div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 2'u64

  asyncTest "a message in a later epoch starts that epoch's budget":
    let now = nowSec()
    discard (await waku.attachRlnProof(messageAt(now, "a"))).expect("first")
    let later = now + 2 * TestEpochSizeSec
    discard (await waku.attachRlnProof(messageAt(later, "b"))).expect("second")

    check:
      onchainRln.nonceManager.epochIndex == later div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64
      (await waku.rlnEpochQuota(now)).expect("quota").remaining == 20'u64

  asyncTest "refuses a message timestamped in an epoch already left behind":
    ## Drawing again from a passed epoch could repeat an id that was sent, so
    ## the failure is permanent: the send service fails the task instead of
    ## retrying.
    let now = nowSec()
    discard (await waku.attachRlnProof(messageAt(now, "a"))).expect("first")
    let earlier = now - 2 * TestEpochSizeSec

    let res = await waku.attachRlnProof(messageAt(earlier, "b"))
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Permanent
      onchainRln.nonceManager.epochIndex == now div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64

  asyncTest "a far-future timestamp is refused without moving the counter":
    ## Caller-supplied timestamps reach the generator over REST and lightpush.
    ## Reserving for a future epoch would leave every current message with
    ## EpochPassed until that epoch arrives, so the validators' timestamp
    ## bound is applied before the reservation.
    let now = nowSec()
    let farFuture = now + 3600

    let res = await waku.attachRlnProof(messageAt(farFuture, "a"))
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Permanent
      onchainRln.nonceManager.nextId == 0'u64

    discard (await waku.attachRlnProof(messageAt(now, "b"))).expect("current message")
    check:
      onchainRln.nonceManager.epochIndex == now div TestEpochSizeSec
      onchainRln.nonceManager.nextId == 1'u64

  asyncTest "a stale timestamp is refused without spending an id":
    let now = nowSec()
    let res = await waku.attachRlnProof(messageAt(now - 3600))
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Permanent
      onchainRln.nonceManager.nextId == 0'u64

  asyncTest "a failed proof generation returns its message id":
    ## The send service retries a `Transient` failure on its next attempt. The
    ## failed attempt built no proof, so the retry draws the same id instead of
    ## spending a second one on a message the rate limit manager charged once.
    let gm = cast[RlnEvmGroupManager](onchainRln.groupManager)
    # Not a whole number of 32-byte path elements, so generation fails.
    gm.merkleProofCache = @[1'u8]
    let msg = testMessage()

    let res = await waku.attachRlnProof(msg)
    let storedAfterFailure = (
        await onchainRln.messageIdStore.loadMessageIds(onchainRln.messageIdKey)
      )
      .get()
      .get()
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Transient
      onchainRln.nonceManager.nextId == 0'u64
      storedAfterFailure.nextId == 0'u64 # the saved count is lowered again

    gm.invalidateMerkleProofCache()
    let retried = (await waku.attachRlnProof(msg)).expect("retry")
    let storedAfterRetry = (
        await onchainRln.messageIdStore.loadMessageIds(onchainRln.messageIdKey)
      )
      .get()
      .get()
    check:
      retried.proof.len > 0
      onchainRln.nonceManager.nextId == 1'u64
      storedAfterRetry.nextId == 1'u64

  asyncTest "a drawn id's count is saved before its proof is returned":
    let now = nowSec()
    discard (await waku.attachRlnProof(messageAt(now))).expect("attachRlnProof")

    let stored = (
        await onchainRln.messageIdStore.loadMessageIds(onchainRln.messageIdKey)
      )
      .get()
      .get()
    check:
      stored.epochIndex == now div TestEpochSizeSec
      stored.nextId == 1'u64

  asyncTest "a failed save returns no proof and spends no id":
    ## Sending after a failed save is the unsafe direction: the id could be
    ## drawn again after a restart. The draw is undone and the task retried.
    let now = nowSec()
    discard (await waku.attachRlnProof(messageAt(now, "a"))).expect("first")
    GetPersistency.request(waku.node.brokerCtx).expect("persistency").closeJob(RlnJobId)

    let res = await waku.attachRlnProof(messageAt(now, "b"))
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Transient
      onchainRln.nonceManager.nextId == 1'u64

  asyncTest "refuses an untimestamped message":
    let res = await waku.attachRlnProof(messageAt(0))
    check:
      res.isErr()
      res.error.kind == RlnErrorKind.Permanent
      onchainRln.nonceManager.nextId == 0'u64

  asyncTest "is idempotent: a message that already carries a proof is untouched":
    ## Pins the retry contract: the send service re-attaches on every round, so
    ## re-attaching must neither draw a fresh nonce nor change the bytes —
    ## otherwise a retried task would resend under a new nullifier.
    let first = (await waku.attachRlnProof(testMessage())).expect("first attach")
    let second = (await waku.attachRlnProof(first)).expect("second attach")

    check:
      first.proof.len > 0
      second.proof == first.proof
