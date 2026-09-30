{.used.}

import std/[options, osproc, times]
import chronos, testutils/unittests, results, stew/byteutils
import
  logos_delivery/waku/[waku, waku_core, rln],
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/node/waku_node/relay,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/rln/rln_api,
  logos_delivery/waku/rln/rln_lez/[rln_lez, transport],
  logos_delivery/api/types,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task]
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

type PropagatingProcessor = ref object of BaseSendProcessor
  ## Propagates each task at once.

method process(self: PropagatingProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

type DialFailedOnceProcessor = ref object of BaseSendProcessor
  ## The first call ends as a failed dial, which gives the message to no node.
  ## Later calls propagate the task.
  calls: int

method process(
    self: DialFailedOnceProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  if self.calls == 1:
    task.state = DeliveryState.NextRoundRetry
    return
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

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

  asyncTest "a send gives a new proof and a new timestamp together":
    ## The task waited less than `MaxUnsentMessageAge`, but its timestamp is in
    ## an epoch that the message ids already left behind. It gets a new
    ## timestamp before its proof, so the proof is not refused.
    discard (await waku.attachRlnProof(testMessage())).expect("current epoch")
    let rateLimit =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService.new(false, waku, rateLimit, PropagatingProcessor()).expect(
        "SendService.new"
      )
    let msg = messageAt(nowSec() - 5)
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    let task = DeliveryTask(
      requestId: RequestId("rln-epoch"),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.Entry,
    )
    await service.send(task)
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      task.msg.proof.len > 0
      (await onchainRln.validateMessage(task.msg)) == MessageValidationResult.Valid

  asyncTest "a message with a proof keeps its timestamp while it waits":
    ## A wait does not use a new message id. The proof and the timestamp stay
    ## until a validator refuses the proof.
    let rateLimit =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, rateLimit, DialFailedOnceProcessor())
      .expect("SendService.new")
    let msg = testMessage()
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    let task = DeliveryTask(
      requestId: RequestId("rln-wait"),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.Entry,
    )
    await service.send(task)
    check:
      task.state == DeliveryState.NextRoundRetry
      task.msg.proof.len > 0
    let proof = task.msg.proof
    let hash = task.msgHash
    await sleepAsync(MaxUnsentMessageAge + chronos.seconds(1))
    await service.trySendMessages()
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      task.msg.proof == proof
      task.msgHash == hash
