{.used.}

import std/[importutils, sequtils, tables]
import chronos, testutils/unittests, results, stew/byteutils, libp2p/peerid

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/rln/[rln_api, rln_plugin],
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service {.all.}, transports, delivery_task]
import ../testlib/[fake_transport, futures, testasync, wakucore, wakunodeconf]

privateAccess(SendService) # the tests read `deliveryTasks`

## Tests of `sendAttempt`, which makes one attempt of a message over one transport.

const LongInterval = chronos.minutes(1)
  ## The timeout of an attempt that must not time out.

const ShortInterval = chronos.milliseconds(20)

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

type FakeRln = ref object
  proofs: int ## The proofs that the backend made.
  refreshes: int ## The calls of `onProofRejected`.

suite "SendService - sendAttempt":
  var waku {.threadvar.}: Waku
  var manager {.threadvar.}: RateLimitManager
  var sendEvents {.threadvar.}: EventCounter
  var started {.threadvar.}: seq[SendService]

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 5))
      .expect("RateLimitManager.new")
    started = @[]
    sendEvents = EventCounter()
    let events = sendEvents
    discard MessagePropagatedEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
          events.inc(),
      )
      .expect("listen propagated")
    discard MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageSentEvent) {.async: (raises: []).} =
          events.inc(),
      )
      .expect("listen sent")
    discard MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(event: MessageErrorEvent) {.async: (raises: []).} =
          events.inc(),
      )
      .expect("listen error")

  asyncTeardown:
    for service in started:
      await service.stopSendService()
    await MessagePropagatedEvent.dropAllListeners(waku.brokerCtx)
    await MessageSentEvent.dropAllListeners(waku.brokerCtx)
    await MessageErrorEvent.dropAllListeners(waku.brokerCtx)
    discard await waku.stop()

  const Shard = PubsubTopic("/waku/2/rs/3/0")

  proc buildTask(
      id: string, timestamp: Timestamp = getNowInNanosecondTime()
  ): DeliveryTask =
    ## The payload is `id`, so each id gives its own message hash.
    let msg = WakuMessage(
      contentTopic: "/test/1/send-attempt/proto",
      payload: id.toBytes(),
      timestamp: timestamp,
    )
    return DeliveryTask(
      pubsubTopic: Shard,
      msg: msg,
      msgHash: computeMessageHash(Shard, msg),
      state: DeliveryState.Entry,
    )

  proc sameMessage(task: DeliveryTask): DeliveryTask =
    ## A new task of the same message, as each call of the API makes one.
    return buildTask(task.msg.id, task.msg.timestamp)

  proc newService(
      transports: Transports,
      reliability = false,
      attemptInterval = SendAttemptInterval,
      maxValidationAge = StoreValidationWindow,
      maxSendsInFlight = MaxSendsInFlight,
  ): SendService =
    ## A service that runs.
    let service = SendService
      .new(
        reliability,
        waku,
        manager,
        transports,
        attemptInterval = attemptInterval,
        maxValidationAge = maxValidationAge,
        storeValidationInterval = chronos.milliseconds(10),
        maxSendsInFlight = maxSendsInFlight,
      )
      .expect("SendService.new")
    service.startSendService()
    started.add(service)
    return service

  proc mountRln(refreshes = true): FakeRln =
    ## Mounts an RLN backend that gives each proof its own bytes.
    let rln = FakeRln()
    proc generate(message: WakuMessage): Future[Result[seq[byte], RlnError]] {.async.} =
      rln.proofs.inc()
      return ok(@[byte(rln.proofs), 2, 3])

    var plugin = RlnPlugin(name: "fake", generateProof: generate)
    if refreshes:
      plugin.onProofRejected = proc() {.gcsafe, raises: [].} =
        rln.refreshes.inc()
    waku.node.rlnPlugin = Opt.some(plugin)
    return rln

  proc attempt(
      service: SendService,
      task: DeliveryTask,
      transport: Transport,
      timeout = LongInterval,
      lightpushPeers = Opt.none(seq[RemotePeerInfo]),
  ): Future[Result[SendAttempt, SendError]] {.async.} =
    return await service.sendAttempt(task, transport, timeout, lightpushPeers)

  proc unreachablePeer(): RemotePeerInfo =
    return RemotePeerInfo.init(
      PeerId.init(generateSecp256k1Key()).tryGet(),
      @[MultiAddress.init("/ip4/127.0.0.1/tcp/1").tryGet()],
    )

  asyncTest "an attempt uses only the transport of the call, and gives no event":
    ## The caller owns the result, so the events of `send` do not report it.
    let relay = FakeTransport.new()
    let lightpush = FakeTransport.new()
    let service = newService(
      transportsOf((Transport.Relay, relay), (Transport.Lightpush, lightpush))
    )
    let task = buildTask("one-transport")

    let res = await service.attempt(task, Transport.Lightpush)
    check res.isOk()
    if res.isOk():
      check:
        res.get().msgHash == task.msgHash
        res.get().outcome == AttemptOutcome.Propagated
    check:
      lightpush.calls == @["one-transport"]
      relay.calls.len == 0
      relay.checks == 0
    check not await sendEvents.waitCount(1).withTimeout(FUTURE_TIMEOUT_SHORT)

  asyncTest "an attempt that does not propagate gives its reason, and makes no second attempt":
    let lightpush =
      FakeTransport.new(results = @[AttemptResult.notPropagated("scripted")])
    let relay = FakeTransport.new()
    let service = newService(
      transportsOf((Transport.Relay, relay), (Transport.Lightpush, lightpush))
    )

    let res = await service.attempt(buildTask("one-attempt"), Transport.Lightpush)
    check res.isOk()
    if res.isOk():
      check:
        res.get().outcome == AttemptOutcome.NotPropagated
        res.get().desc == "scripted"
        res.get().confirmation.isNil()
    check not await lightpush.attempts.waitCount(2).withTimeout(FUTURE_TIMEOUT_SHORT)
    check relay.calls.len == 0

  asyncTest "a later attempt of a message uses the same proof and gets no new charge":
    ## A new proof for the same message draws one more RLN message id.
    let rln = mountRln()
    let lightpush = FakeTransport.new(
      results = @[AttemptResult.notPropagated("scripted"), AttemptResult.propagated()]
    )
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))
    let task = buildTask("same-proof")

    let first = await service.attempt(task, Transport.Lightpush)
    let second = await service.attempt(task.sameMessage(), Transport.Lightpush)
    check:
      first.isOk() and first.get().outcome == AttemptOutcome.NotPropagated
      second.isOk() and second.get().outcome == AttemptOutcome.Propagated
      rln.proofs == 1
      manager.sentInCurrentEpoch == 1'u64
      lightpush.messages.len == 2
    if lightpush.messages.len == 2:
      check:
        lightpush.messages[0].proof.len > 0
        lightpush.messages[0].proof == lightpush.messages[1].proof

  asyncTest "an attempt over another transport uses the same proof":
    let rln = mountRln()
    let relay = FakeTransport.new(results = @[AttemptResult.notPropagated("scripted")])
    let lightpush = FakeTransport.new()
    let service = newService(
      transportsOf((Transport.Relay, relay), (Transport.Lightpush, lightpush))
    )
    let task = buildTask("two-transports")

    discard await service.attempt(task, Transport.Relay)
    discard await service.attempt(task.sameMessage(), Transport.Lightpush)
    check:
      rln.proofs == 1
      manager.sentInCurrentEpoch == 1'u64
      relay.messages.len == 1 and lightpush.messages.len == 1
    if relay.messages.len == 1 and lightpush.messages.len == 1:
      check relay.messages[0].proof == lightpush.messages[0].proof

  asyncTest "an attempt of a message that has an attempt in progress is refused as Duplicate":
    let relay = FakeTransport.new(stalled = @["in-progress"])
    let service = newService(transportsOf((Transport.Relay, relay)))
    let task = buildTask("in-progress")

    let first = service.attempt(task, Transport.Relay)
    check await relay.attemptStarted("in-progress").withTimeout(FUTURE_TIMEOUT_MEDIUM)
    let second = await service.attempt(task.sameMessage(), Transport.Relay)
    check:
      second.isErr()
      second.error.kind == SendErrorKind.Duplicate
      relay.calls.len == 1
    relay.reply("in-progress")
    check await first.withTimeout(FUTURE_TIMEOUT_MEDIUM)

  asyncTest "an attempt of a message of send is refused as Duplicate":
    ## The attempts of `send` own the message until its last event.
    let relay = FakeTransport.new(stalled = @["of-send"])
    let service = newService(transportsOf((Transport.Relay, relay)))
    let task = buildTask("of-send")
    check service.enqueue(task).isOk()

    let res = await service.attempt(task.sameMessage(), Transport.Relay)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.Duplicate

  asyncTest "an attempt with no answer ends at its timeout":
    let relay = FakeTransport.new(stalled = @["no-answer"])
    let service = newService(transportsOf((Transport.Relay, relay)))

    let res = await service.attempt(
      buildTask("no-answer"), Transport.Relay, timeout = chronos.milliseconds(50)
    )
    check res.isOk()
    if res.isOk():
      check res.get().outcome == AttemptOutcome.NotPropagated
    check relay.cancelled == @["no-answer"]

  asyncTest "an attempt over a transport that the node does not have is Invalid":
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))

    let res = await service.attempt(buildTask("no-transport"), Transport.Lightpush)
    check res.isOk()
    if res.isOk():
      check res.get().outcome == AttemptOutcome.Invalid
    check relay.calls.len == 0

  asyncTest "the lightpush peers of the caller go to the transport":
    let lightpush = FakeTransport.new()
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))
    let peer = unreachablePeer()

    discard await service.attempt(
      buildTask("caller-peer"), Transport.Lightpush, lightpushPeers = Opt.some(@[peer])
    )
    check lightpush.peers.len == 1
    if lightpush.peers.len == 1:
      check lightpush.peers[0].mapIt(it.peerId) == @[peer.peerId]

  asyncTest "a transport that cannot take the message now gives no proof and no transmission":
    let rln = mountRln()
    let lightpush =
      FakeTransport.new(checkResult = Opt.some(AttemptResult.notPropagated()))
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))

    let res = await service.attempt(buildTask("no-peer"), Transport.Lightpush)
    check res.isOk()
    if res.isOk():
      check res.get().outcome == AttemptOutcome.NotPropagated
    check:
      lightpush.calls.len == 0
      rln.proofs == 0

  asyncTest "an attempt with a timeout that is not positive is refused":
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))

    let res = await service.attempt(
      buildTask("no-timeout"), Transport.Relay, timeout = ZeroDuration
    )
    check:
      res.isErr()
      res.error.kind == SendErrorKind.Other
      relay.calls.len == 0
      manager.sentInCurrentEpoch == 0'u64

  asyncTest "a relay attempt after a refused proof validates the message again":
    ## The relay of the node validated the old proof, and the message has a new one.
    discard mountRln()
    let relay = FakeTransport.new(
      results = @[
        AttemptResult(outcome: AttemptOutcome.NotPropagated, validated: true),
        AttemptResult(outcome: AttemptOutcome.NotPropagated, proofRefused: true),
        AttemptResult(outcome: AttemptOutcome.NotPropagated, validated: true),
      ]
    )
    let service = newService(transportsOf((Transport.Relay, relay)))
    let task = buildTask("validated-again")

    for _ in 0 ..< 4:
      discard await service.attempt(task.sameMessage(), Transport.Relay)
    check relay.validated == @[false, true, false, true]

  asyncTest "after a refused RLN proof, the next attempt makes a new proof with a new charge":
    ## A node refuses the proof when the RLN state of this node is old.
    ## The backend then refreshes that state, and a new proof draws a new message id.
    let rln = mountRln()
    let lightpush = FakeTransport.new(
      results = @[
        AttemptResult(
          outcome: AttemptOutcome.NotPropagated, desc: "bad proof", proofRefused: true
        ),
        AttemptResult.propagated(),
      ]
    )
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))
    let task = buildTask("proof-refused")

    let first = await service.attempt(task, Transport.Lightpush)
    check:
      first.isOk() and first.get().outcome == AttemptOutcome.NotPropagated
      rln.refreshes == 1
      rln.proofs == 1
      manager.sentInCurrentEpoch == 1'u64

    let second = await service.attempt(task.sameMessage(), Transport.Lightpush)
    check:
      second.isOk() and second.get().outcome == AttemptOutcome.Propagated
      rln.proofs == 2
      manager.sentInCurrentEpoch == 2'u64
      lightpush.messages.len == 2
    if lightpush.messages.len == 2:
      check lightpush.messages[0].proof != lightpush.messages[1].proof

  asyncTest "a refused RLN proof is Invalid when the backend cannot refresh":
    discard mountRln(refreshes = false)
    let lightpush = FakeTransport.new(
      results = @[
        AttemptResult(
          outcome: AttemptOutcome.NotPropagated, desc: "bad proof", proofRefused: true
        )
      ]
    )
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))

    let res = await service.attempt(buildTask("no-refresh"), Transport.Lightpush)
    check res.isOk()
    if res.isOk():
      check:
        res.get().outcome == AttemptOutcome.Invalid
        res.get().desc == "bad proof"

  asyncTest "a new proof after a refused proof needs budget":
    discard mountRln()
    manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1))
      .expect("RateLimitManager.new")
    let lightpush = FakeTransport.new(
      results = @[
        AttemptResult(
          outcome: AttemptOutcome.NotPropagated, desc: "bad proof", proofRefused: true
        )
      ]
    )
    let service = newService(transportsOf((Transport.Lightpush, lightpush)))
    let task = buildTask("no-budget")

    discard await service.attempt(task, Transport.Lightpush)
    let second = await service.attempt(task.sameMessage(), Transport.Lightpush)
    check second.isOk()
    if second.isOk():
      check:
        second.get().outcome == AttemptOutcome.Invalid
        second.get().desc == NoBudgetForNewProofReason
    check lightpush.calls.len == 1

  asyncTest "a message over the budget is refused as RateLimited":
    manager = RateLimitManager
      .new(RateLimitConfig(enabled: true, epochPeriodSec: 600, messagesPerEpoch: 1))
      .expect("RateLimitManager.new")
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))

    check (await service.attempt(buildTask("in-budget"), Transport.Relay)).isOk()
    let res = await service.attempt(buildTask("over-budget"), Transport.Relay)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.RateLimited
      res.error.retryAfter > ZeroDuration
      relay.calls == @["in-budget"]

  asyncTest "a full service removes its oldest idle message for a new one, and gives back an unused charge":
    ## The service keeps an idle message only for its proof.
    ## A message with no transmission and no proof used nothing of the budget.
    let lightpush =
      FakeTransport.new(checkResult = Opt.some(AttemptResult.notPropagated()))
    let service =
      newService(transportsOf((Transport.Lightpush, lightpush)), maxSendsInFlight = 1)
    let idle = buildTask("idle")

    check (await service.attempt(idle, Transport.Lightpush)).isOk()
    check:
      idle.msgHash in service.deliveryTasks
      manager.sentInCurrentEpoch == 1'u64

    let next = buildTask("next")
    check (await service.attempt(next, Transport.Lightpush)).isOk()
    check:
      idle.msgHash notin service.deliveryTasks
      next.msgHash in service.deliveryTasks
      manager.sentInCurrentEpoch == 1'u64 # the charge of `next` only

  asyncTest "a refused message leaves the idle message of a full service":
    ## The idle message keeps its proof for its next attempt.
    let lightpush =
      FakeTransport.new(checkResult = Opt.some(AttemptResult.notPropagated()))
    let service =
      newService(transportsOf((Transport.Lightpush, lightpush)), maxSendsInFlight = 1)
    let idle = buildTask("idle")
    check (await service.attempt(idle, Transport.Lightpush)).isOk()

    let tooLarge = buildTask("too-large")
    tooLarge.msg.payload = newSeq[byte](int(waku.maxMessageSizeBytes) + 1)
    tooLarge.msgHash = computeMessageHash(tooLarge.pubsubTopic, tooLarge.msg)
    let res = await service.attempt(tooLarge, Transport.Lightpush)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.TooLarge
      idle.msgHash in service.deliveryTasks

  asyncTest "a full service refuses a message that is too large as TooLarge":
    ## `QueueFull` asks the app to send again, and no later send of this message can pass.
    let relay = FakeTransport.new(stalled = @["busy"])
    let service =
      newService(transportsOf((Transport.Relay, relay)), maxSendsInFlight = 1)
    let busy = service.attempt(buildTask("busy"), Transport.Relay)
    check await relay.attemptStarted("busy").withTimeout(FUTURE_TIMEOUT_MEDIUM)

    let tooLarge = buildTask("too-large")
    tooLarge.msg.payload = newSeq[byte](int(waku.maxMessageSizeBytes) + 1)
    tooLarge.msgHash = computeMessageHash(tooLarge.pubsubTopic, tooLarge.msg)
    let res = await service.attempt(tooLarge, Transport.Relay)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.TooLarge
    relay.reply("busy")
    check await busy.withTimeout(FUTURE_TIMEOUT_MEDIUM)

  asyncTest "a message is refused as QueueFull when each kept message has an attempt in progress":
    let relay = FakeTransport.new(stalled = @["busy"])
    let service =
      newService(transportsOf((Transport.Relay, relay)), maxSendsInFlight = 1)

    let busy = service.attempt(buildTask("busy"), Transport.Relay)
    check await relay.attemptStarted("busy").withTimeout(FUTURE_TIMEOUT_MEDIUM)
    let res = await service.attempt(buildTask("over"), Transport.Relay)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.QueueFull
    relay.reply("busy")
    check await busy.withTimeout(FUTURE_TIMEOUT_MEDIUM)

  asyncTest "the service keeps a proof for the delivery window only":
    ## After that time no node accepts the timestamp of the message.
    let rln = mountRln()
    let relay = FakeTransport.new(results = @[AttemptResult.notPropagated("scripted")])
    let service = newService(
      transportsOf((Transport.Relay, relay)), attemptInterval = ShortInterval
    )
    let task = buildTask("window")

    discard await service.attempt(task, Transport.Relay)
    let kept = service.deliveryTasks.getOrDefault(task.msgHash)
    check not kept.isNil()
    if kept.isNil():
      return
    kept.acceptedTime =
      Moment.now() - ShortInterval * MaxSendAttempts - chronos.seconds(1)

    discard await service.attempt(task.sameMessage(), Transport.Relay)
    check:
      rln.proofs == 2
      service.deliveryTasks.getOrDefault(task.msgHash) != kept

  asyncTest "with reliability off the confirmation of a propagated message is NotExpected":
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))

    let res = await service.attempt(buildTask("no-reliability"), Transport.Relay)
    check res.isOk()
    if res.isOk():
      let confirmation = res.get().confirmation
      check:
        confirmation.finished()
        confirmation.read() == Confirmation.NotExpected

  asyncTest "the confirmation of a message that mix propagated is Confirmed, with no Store query":
    ## A Store query would name the message from the address of this node.
    let mix = FakeTransport.new()
    let service = newService(
      transportsOf((Transport.Mix, mix), (Transport.Lightpush, FakeTransport.new())),
      reliability = true,
    )
    let task = buildTask("over-mix")

    let res = await service.attempt(task, Transport.Mix)
    check res.isOk()
    if res.isOk():
      let confirmation = res.get().confirmation
      check:
        confirmation.finished()
        confirmation.read() == Confirmation.Confirmed
    let kept = service.deliveryTasks.getOrDefault(task.msgHash)
    check not kept.isNil()
    if not kept.isNil():
      check:
        kept.anonymized
        not kept.confirmationPending()

  asyncTest "the confirmation of a plain message with no Store answer is NotConfirmed after the validation window":
    let relay = FakeTransport.new()
    let service = newService(
      transportsOf((Transport.Relay, relay)),
      reliability = true,
      maxValidationAge = chronos.milliseconds(50),
    )

    let res = await service.attempt(buildTask("no-store"), Transport.Relay)
    check res.isOk()
    if res.isErr():
      return
    let confirmation = res.get().confirmation
    check not confirmation.finished()
    check await confirmation.join().withTimeout(FUTURE_TIMEOUT_MEDIUM)
    if confirmation.finished():
      check confirmation.read() == Confirmation.NotConfirmed

  asyncTest "a cancel of the confirmation of one attempt leaves the confirmation of another attempt":
    ## Each caller waits for the one confirmation of the message with a future of its own.
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)), reliability = true)
    let task = buildTask("propagated-twice")

    let first = await service.attempt(task, Transport.Relay)
    let second = await service.attempt(task.sameMessage(), Transport.Relay)
    check first.isOk() and second.isOk()
    if first.isErr() or second.isErr():
      return
    await first.get().confirmation.cancelAndWait()
    let confirmation = second.get().confirmation
    check not confirmation.finished()

    await service.stopSendService()
    check await confirmation.join().withTimeout(FUTURE_TIMEOUT)
    if confirmation.finished():
      check confirmation.read() == Confirmation.NotConfirmed

  asyncTest "a stop ends a confirmation in progress as NotConfirmed":
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)), reliability = true)

    let res = await service.attempt(buildTask("stop"), Transport.Relay)
    check res.isOk()
    if res.isErr():
      return
    let confirmation = res.get().confirmation
    check not confirmation.finished()
    await service.stopSendService()
    check service.deliveryTasks.len == 0
    check await confirmation.join().withTimeout(FUTURE_TIMEOUT)
    if confirmation.finished():
      check confirmation.read() == Confirmation.NotConfirmed

  asyncTest "an attempt that propagates after a stop gets a NotConfirmed confirmation":
    ## The service makes no Store query after its stop, so the caller must not wait.
    let relay = FakeTransport.new(stalled = @["during-stop"])
    let service = newService(transportsOf((Transport.Relay, relay)), reliability = true)

    let running = service.attempt(buildTask("during-stop"), Transport.Relay)
    check await relay.attemptStarted("during-stop").withTimeout(FUTURE_TIMEOUT_MEDIUM)
    await service.stopSendService()
    relay.reply("during-stop")
    check await running.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    if not running.finished():
      return
    let res = running.read()
    check res.isOk()
    if res.isOk():
      let confirmation = res.get().confirmation
      check:
        res.get().outcome == AttemptOutcome.Propagated
        confirmation.finished()
      if confirmation.finished():
        check confirmation.read() == Confirmation.NotConfirmed

  asyncTest "a stop keeps the charge of a message whose proof is in progress":
    ## The proof draws its RLN message id also after the stop.
    let proofStarted = newFuture[void]("proof-started")
    let proofReply = newFuture[void]("proof-reply")
    proc generate(message: WakuMessage): Future[Result[seq[byte], RlnError]] {.async.} =
      proofStarted.complete()
      await proofReply
      return ok(@[1'u8, 2, 3])

    waku.node.rlnPlugin = Opt.some(RlnPlugin(name: "fake", generateProof: generate))
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))

    let running = service.attempt(buildTask("proof-in-progress"), Transport.Relay)
    check await proofStarted.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    await service.stopSendService()
    check manager.sentInCurrentEpoch == 1'u64
    proofReply.complete()
    check await running.withTimeout(FUTURE_TIMEOUT_MEDIUM)

  asyncTest "a service that does not run refuses an attempt":
    let relay = FakeTransport.new()
    let service = newService(transportsOf((Transport.Relay, relay)))
    await service.stopSendService()

    let res = await service.attempt(buildTask("after-stop"), Transport.Relay)
    check:
      res.isErr()
      res.error.kind == SendErrorKind.Other
      relay.calls.len == 0
