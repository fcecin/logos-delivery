{.used.}

import std/[net, sequtils, strutils, tables]
import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  libp2p_mix/[curve25519, pool, mix_protocol],
  libp2p/[peerid, multiaddress, peerstore, varint, vbuffer],
  libp2p/stream/connection,
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_mix,
  logos_delivery/waku/node/waku_node,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/peer_manager/waku_peer_store,
  logos_delivery/waku/node/waku_node/lightpush,
  logos_delivery/waku/node/waku_node/store,
  logos_delivery/waku/node/health_monitor/
    [node_health_monitor, protocol_health, health_status],
  logos_delivery/waku/waku_lightpush,
  logos_delivery/waku/waku_store/common,
  logos_delivery/waku/api/store,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/api/conf/messaging_conf,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, mix_processor, delivery_task]
import ../testlib/[testasync, wakucore]

## Tests for the anonymity levels of the send path. The mix processor keeps
## the task for the mix window. Only a `Preferred` chain gives the task to the
## relay and lightpush processors after the window. The node in these tests
## has no mix mounted, so mix cannot deliver. The levels differ in that case.

type PlainSendProcessor = ref object of BaseSendProcessor
  calls: int

type BlockingSendProcessor = ref object of BaseSendProcessor
  ## An attempt that does not complete, as a mix attempt that waits for a
  ## reply.
  started: Future[void]

method isValidProcessor(
    self: BlockingSendProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: BlockingSendProcessor, task: DeliveryTask
): Future[void] {.async.} =
  if not self.started.finished():
    self.started.complete()
  await newFuture[void]("blocking send")

type SlowSendProcessor = ref object of BaseSendProcessor
  ## An attempt that takes `delay`, as a mix attempt that waits for a reply.
  delay: Duration

method isValidProcessor(self: SlowSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: SlowSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  await sleepAsync(self.delay)
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

type MaybeSentProcessor = ref object of BaseSendProcessor
  ## A mix attempt whose request left the node and got no reply: the task
  ## keeps the mark that forbids the plain path, and retries.
  calls: int

method isValidProcessor(
    self: MaybeSentProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: MaybeSentProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.mixRequestLeft = true
  task.errorDesc = "the exit node's reply did not arrive within the time limit"
  task.state = DeliveryState.NextRoundRetry

type RetryProcessor = ref object of BaseSendProcessor
  ## An attempt that did not deliver and left nothing behind: the task
  ## retries.

method isValidProcessor(self: RetryProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: RetryProcessor, task: DeliveryTask): Future[void] {.async.} =
  task.errorDesc = "no exit node"
  task.state = DeliveryState.NextRoundRetry

method isValidProcessor(self: PlainSendProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: PlainSendProcessor, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.SuccessfullyPropagated
  task.firstPropagatedTime = Opt.some(Moment.now())

proc testConf(): WakuConf =
  var conf = MessagingClientConf()
    .toWakuNodeConf(messaging_conf.LogosDeliveryMode.Core).valueOr:
      raiseAssert error
  conf.listenAddress = parseIpAddress("0.0.0.0")
  conf.tcpPort = Port(0)
  conf.discv5UdpPort = Port(0)
  conf.clusterId = Opt.some(3'u16)
  conf.numShardsInNetwork = 1
  conf.rest = false
  return conf.toWakuConf().valueOr:
    raiseAssert error

suite "SendService - anonymity level":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc buildTask(id: string, admittedAgo: Duration): DeliveryTask =
    # One message per id: the cache keeps one task per message hash.
    let msg = WakuMessage(
      contentTopic: "/test/1/anonymity/proto",
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
      firstAdmittedTime: Opt.some(Moment.now() - admittedAgo),
      deadlineStart: Opt.some(Moment.now() - admittedAgo),
    )

  asyncTest "a Required task keeps waiting for mix instead of using the plain path":
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Required, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("required", chronos.minutes(10))
    await mix.process(task)

    check:
      plain.calls == 0 # mix cannot deliver, but the plain path is off limits
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a Preferred task stays on mix while the mix window is open":
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("best-effort-early", chronos.seconds(5))
    await mix.process(task)

    check:
      plain.calls == 0
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a Preferred task falls back to the plain path once the window elapsed":
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("best-effort-late", chronos.minutes(2))
    # Mix had the task since admission and did not deliver it.
    await mix.process(task)

    check:
      plain.calls == 1
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "an RLN proof refresh does not restart the Preferred mix window":
    ## `parkForRlnProofRefresh` clears `firstAdmittedTime` so that the new
    ## proof gets a new nonce. The mix window runs from `deadlineStart`, which
    ## the park does not clear, so the window does not restart.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("rln-park", chronos.minutes(2))
    task.firstAdmittedTime = Opt.none(Moment) # what the RLN park leaves behind

    await mix.process(task)

    check:
      plain.calls == 1
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "the mix window runs from admission, so a wait for budget does not use it":
    ## A task that waits for rate-limit budget has no `deadlineStart`. The
    ## window cannot end before the admission. After the admission, the
    ## window and the delivery deadline run from the same time.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("not-admitted", chronos.minutes(2))
    task.firstAdmittedTime = Opt.none(Moment)
    task.deadlineStart = Opt.none(Moment)

    await mix.process(task)
    check:
      plain.calls == 0 # no admission, so the window did not run
      task.state == DeliveryState.NextRoundRetry

    task.deadlineStart = Opt.some(Moment.now() - chronos.minutes(2))
    await mix.process(task)
    check plain.calls == 1 # admitted two minutes ago, so the window ended

  asyncTest "a Preferred task whose request may have left stays on mix after the window":
    ## A mix attempt wrote its request and got no reply, or the exit node
    ## refused it. The exit node may have published the message, or has seen
    ## it. The task does not go to the plain path, after the window or ever:
    ## a copy in clear text would connect the sender to the message.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("request-left", chronos.minutes(2))
    task.mixRequestLeft = true
    await mix.process(task)
    check:
      plain.calls == 0
      task.state == DeliveryState.NextRoundRetry # mix again, next round

    task.deadlineStart = Opt.some(Moment.now() - chronos.minutes(10))
    await mix.process(task)
    check:
      plain.calls == 0 # the mark never clears
      task.mixRequestLeft

  asyncTest "a task that is no longer waiting for a retry is not sent again":
    ## The pass reads the state of each task when it starts the attempt, and
    ## the attempt reads it again: a store validation can change it between
    ## the two.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let plain = PlainSendProcessor()
    let service = SendService.new(false, waku, manager, sendProcessor = plain).expect(
        "SendService.new"
      )
    let task = buildTask("received-before-retry", chronos.seconds(0))
    service.trackedSend(task) # parked: the service did not start
    task.state = DeliveryState.SuccessfullyPropagated
    await service.trySendMessages()
    check plain.calls == 0

  asyncTest "the deadline error of a task whose request may have left says delivery unconfirmed":
    ## A `Required` task whose mix attempts got no reply reaches the deadline
    ## with a request that may be on the network. The application must not
    ## read the error as "not sent".
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(
        false,
        waku,
        manager,
        sendProcessor = MaybeSentProcessor(),
        anonymityLevel = AnonymityLevel.Required,
      )
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()
    var errorText = ""
    let listener = MessageErrorEvent.listen(
      waku.brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        if event.requestId == RequestId("unconfirmed"):
          errorText = event.error
      ,
    ).valueOr:
      raiseAssert error
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)

    # Admitted two minutes ago: past the deadline of one minute. The first
    # attempt runs, leaves the mark, and the report fails the task.
    let task = buildTask("unconfirmed", chronos.minutes(2))
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(50))
    check:
      task.mixRequestLeft
      task.state == DeliveryState.FailedToDeliver
      "delivery unconfirmed" in errorText

  asyncTest "the deadline error of a task whose request never left says unable to send":
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, sendProcessor = RetryProcessor())
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()
    var errorText = ""
    let listener = MessageErrorEvent.listen(
      waku.brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        if event.requestId == RequestId("never-left"):
          errorText = event.error
      ,
    ).valueOr:
      raiseAssert error
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)

    let task = buildTask("never-left", chronos.minutes(2))
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(50))
    check:
      not task.mixRequestLeft
      task.state == DeliveryState.FailedToDeliver
      "delivery unconfirmed" notin errorText
      "Unable to send" in errorText

  asyncTest "requestLeft is true for each reason after the write, false before it":
    ## The rule that keeps a `Preferred` task off the plain path. An exit node
    ## that refused the request has seen the message all the same.
    proc outcome(reason: MixSendReason): MixSendOutcome =
      MixSendOutcome(reason: reason)

    check:
      outcome(MixSendReason.Accepted).requestLeft()
      outcome(MixSendReason.ReplyTimeout).requestLeft()
      outcome(MixSendReason.ReplyUnreadable).requestLeft()
      outcome(MixSendReason.ExitRefused).requestLeft()
      not outcome(MixSendReason.NoExitNode).requestLeft()
      not outcome(MixSendReason.TooLarge).requestLeft()
      not outcome(MixSendReason.PreWriteFailure).requestLeft()

  asyncTest "a Core node keeps the subscription inside a send at every level":
    ## A gossipsub subscription names the shard, not the topic, and a relay
    ## publish needs the mesh. The Edge claim is in the end-to-end suite.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let anonymous = SendService
      .new(
        false,
        waku,
        manager,
        sendProcessor = PlainSendProcessor(),
        anonymityLevel = AnonymityLevel.Required,
      )
      .expect("SendService.new")
    let plain = SendService
      .new(false, waku, manager, sendProcessor = PlainSendProcessor())
      .expect("SendService.new")
    check:
      waku.hasRelay()
      anonymous.subscribesOnSend()
      plain.subscribesOnSend()

  asyncTest "a Preferred task that already propagated through mix stays on mix after the window":
    ## A copy in clear text would connect the sender to a message that went
    ## out through mix. After the window, the plain path gets only a task that
    ## did not propagate.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("propagated-over-mix", chronos.minutes(2))
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.minutes(1))
    task.propagatedViaMix = true
    await mix.process(task)

    check:
      plain.calls == 0
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a Preferred task that propagated through the plain path falls back again after the window":
    ## A store validation miss sets a propagated task back to retry. A task
    ## that the plain path propagated is not a mixed message, so the plain
    ## path gets it again.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("propagated-in-clear", chronos.minutes(2))
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.minutes(1))
    await mix.process(task)

    check:
      plain.calls == 1
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a level other than None on a node without mix is rejected at construction":
    ## A configuration made by hand can carry a level and no mix. The send
    ## service must not start in that state.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    check:
      SendService
        .new(false, waku, manager, anonymityLevel = AnonymityLevel.Required)
        .isErr()
      SendService
        .new(false, waku, manager, anonymityLevel = AnonymityLevel.Preferred)
        .isErr()
      SendService.new(false, waku, manager, anonymityLevel = AnonymityLevel.None).isOk()

  asyncTest "a message that went through mix is not validated with a store node":
    ## A store query with the message hash would show the sender to the store
    ## node. The send service confirms only messages from the plain path.
    let plain = buildTask("plain", chronos.seconds(1))
    let mixed = buildTask("mixed", chronos.seconds(1))
    mixed.propagatedViaMix = true
    check:
      plain.needsStoreValidation()
      not mixed.needsStoreValidation()

  asyncTest "stopping the service cancels a first attempt that is in progress":
    ## `trackedSend` starts the first attempt outside the service loop. The
    ## stop must cancel it, or the attempt continues on a stopped node.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let blocking = BlockingSendProcessor(started: newFuture[void]("attempt started"))
    let service = SendService.new(false, waku, manager, sendProcessor = blocking).expect(
        "SendService.new"
      )
    service.startSendService()
    var stoppedError = ""
    let listener = MessageErrorEvent.listen(
      waku.brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        if event.requestId == RequestId("in-flight"):
          stoppedError = event.error
      ,
    ).valueOr:
      raiseAssert error
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, listener)

    service.trackedSend(buildTask("in-flight", chronos.seconds(0)))
    check await blocking.started.withTimeout(chronos.seconds(2))
    check service.inFlightSendCount() == 1

    let stopFut = service.stopSendService()
    let guard = sleepAsync(chronos.seconds(5))
    discard await race(FutureBase(stopFut), FutureBase(guard))
    await guard.cancelAndWait()
    if not stopFut.finished():
      raiseAssert "stopSendService did not complete with an attempt in progress"
    check service.inFlightSendCount() == 0

    # The task of the cancelled attempt is not in the cache. It gets its
    # final event all the same.
    await sleepAsync(chronos.milliseconds(50))
    check "stopped" in stoppedError

    # A send after the stop does not start an attempt.
    service.trackedSend(buildTask("after-stop", chronos.seconds(0)))
    check:
      service.isStopped()
      service.inFlightSendCount() == 0

  asyncTest "after a stop and a start, a send runs again":
    ## The messaging client supports a stop and a start. The send service
    ## must not stay stopped after the start.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let plain = PlainSendProcessor()
    let service = SendService.new(false, waku, manager, sendProcessor = plain).expect(
        "SendService.new"
      )
    service.startSendService()
    await service.stopSendService()
    check service.isStopped()

    service.startSendService()
    defer:
      await service.stopSendService()
    check not service.isStopped()
    service.trackedSend(buildTask("after-restart", chronos.seconds(0)))
    await sleepAsync(chronos.milliseconds(50))
    check plain.calls == 1

  asyncTest "retries run as owned attempts: the pass returns at once, each task is reported when its attempt ends":
    ## One retry after the other, a pass over N tasks lasts N attempts, and
    ## the events of the first task wait for the last attempt. The pass
    ## starts the attempts and does not wait for them.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    const attempt = chronos.milliseconds(200)
    let service = SendService
      .new(false, waku, manager, sendProcessor = SlowSendProcessor(delay: attempt))
      .expect("SendService.new")
    var propagatedAt: seq[Moment]
    let listener = MessagePropagatedEvent.listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        propagatedAt.add(Moment.now()),
    ).valueOr:
      raiseAssert error
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)

    # A send before the start waits in the cache for the first pass.
    service.trackedSend(buildTask("retry-1", chronos.seconds(0)))
    service.trackedSend(buildTask("retry-2", chronos.seconds(0)))

    let passStarted = Moment.now()
    await service.trySendMessages()
    check:
      Moment.now() - passStarted < attempt # the pass did not wait for the attempts
      service.inFlightSendCount() == 2
    await sleepAsync(attempt + chronos.milliseconds(100))
    check:
      propagatedAt.len == 2
      propagatedAt[1] - passStarted < attempt * 2 # the two attempts ran together

  asyncTest "at most MaxConcurrentRetries retries are in flight, and a task with an attempt in flight gets no second one":
    ## A mix attempt waits for its reply, and the pass comes each second. A
    ## second attempt for a task that waits would send the message again.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    const attempt = chronos.milliseconds(300)
    let service = SendService
      .new(false, waku, manager, sendProcessor = SlowSendProcessor(delay: attempt))
      .expect("SendService.new")
    for i in 0 ..< MaxConcurrentRetries + 2:
      service.trackedSend(buildTask("limited-" & $i, chronos.seconds(0)))

    await service.trySendMessages()
    check service.inFlightSendCount() == MaxConcurrentRetries
    await service.trySendMessages() # the same tasks: no second attempt for them
    check service.inFlightSendCount() == MaxConcurrentRetries

    await sleepAsync(attempt + chronos.milliseconds(100))
    check service.inFlightSendCount() == 0
    await service.trySendMessages() # the two that waited
    check service.inFlightSendCount() == 2

  asyncTest "stopping the service cancels the retries in flight":
    ## A stop in the middle of a pass must cancel each attempt, or the
    ## attempts run on a stopping node and report their tasks after the stop
    ## did.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    const attempt = chronos.milliseconds(300)
    let service = SendService
      .new(false, waku, manager, sendProcessor = SlowSendProcessor(delay: attempt))
      .expect("SendService.new")
    var propagated, failed: seq[RequestId]
    let propagatedListener = MessagePropagatedEvent.listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        propagated.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, propagatedListener)
    let errorListener = MessageErrorEvent.listen(
      waku.brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        failed.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, errorListener)

    service.trackedSend(buildTask("batch-1", chronos.seconds(0)))
    service.trackedSend(buildTask("batch-2", chronos.seconds(0)))
    service.startSendService() # the first pass starts the two attempts
    await sleepAsync(chronos.milliseconds(50))

    let stopStarted = Moment.now()
    await service.stopSendService()
    check Moment.now() - stopStarted < attempt # the stop did not wait for the attempts

    await sleepAsync(attempt + chronos.milliseconds(50))
    check:
      propagated.len == 0 # the attempts did not complete after the stop
      failed.len == 2 # each task got its "stopped" error

  asyncTest "a send before the service starts waits for the first round":
    ## An attempt before the start runs on a node that did not start: a mix
    ## attempt makes a return path with the address of a node that has no
    ## port yet. The task waits in the cache, and the first round of the loop
    ## sends it.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let plain = PlainSendProcessor()
    let service = SendService.new(false, waku, manager, sendProcessor = plain).expect(
        "SendService.new"
      )
    service.trackedSend(buildTask("before-start", chronos.seconds(0)))
    await sleepAsync(chronos.milliseconds(50))
    check:
      plain.calls == 0
      service.inFlightSendCount() == 0

    service.startSendService()
    defer:
      await service.stopSendService()
    await sleepAsync(chronos.milliseconds(50))
    check plain.calls == 1

  asyncTest "the first attempt starts after the send API returned":
    ## A processor that fails without a network wait reports the result in
    ## the first attempt. The caller must hold the request id before the
    ## event, so the attempt runs one event-loop turn after `trackedSend`.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let plain = PlainSendProcessor()
    let service = SendService.new(false, waku, manager, sendProcessor = plain).expect(
        "SendService.new"
      )
    service.startSendService()
    defer:
      await service.stopSendService()

    service.trackedSend(buildTask("deferred-first-attempt", chronos.seconds(0)))
    check plain.calls == 0 # the caller has the request id and no event yet
    await sleepAsync(chronos.milliseconds(50))
    check plain.calls == 1

  asyncTest "Preferred gets a second delivery window, the other levels do not":
    let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(3'u16, mixPrivKey, newSeq[MixNodePubInfo]())).isOkOr:
      raiseAssert "failed to mount mix: " & error
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")

    let plainService = SendService
      .new(false, waku, manager, anonymityLevel = AnonymityLevel.None)
      .expect("SendService.new")
    let mixOnlyService = SendService
      .new(false, waku, manager, anonymityLevel = AnonymityLevel.Required)
      .expect("SendService.new")
    let bestEffortService = SendService
      .new(false, waku, manager, anonymityLevel = AnonymityLevel.Preferred)
      .expect("SendService.new")

    check:
      plainService.maxDeliveryTime == MaxTimeInCache
      mixOnlyService.maxDeliveryTime == MaxTimeInCache
      bestEffortService.maxDeliveryTime == MaxTimeInCache + MaxTimeInCache

suite "Mix send path - exit peer selection":
  ## With `exit_is_dest` the lightpush server is the last node of the sphinx
  ## path. Mix rejects a destination without a mix public key. The selection
  ## must not give such a peer to mix.
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  const shard = PubsubTopic("/waku/2/rs/3/0")

  proc addLightpushPeer(
      mixCapable: bool, address = "/ip4/127.0.0.1/tcp/60000"
  ): PeerId =
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let keyPair = generateKeyPair().expect("mix key pair")
    let mixPubKey =
      if mixCapable:
        Opt.some(keyPair.publicKey)
      else:
        Opt.none(typeof(keyPair.publicKey))
    waku.node.peerManager.addPeer(
      RemotePeerInfo.init(
        peerId,
        @[MultiAddress.init(address).tryGet()],
        protocols = @[WakuLightPushCodec],
        shards = @[0'u16],
        mixPubKey = mixPubKey,
      )
    )
    waku.node.peerManager.switch.peerStore.setShardInfo(peerId, @[0'u16])
    return peerId

  asyncTest "a plain lightpush peer is never offered as a mix exit":
    discard addLightpushPeer(mixCapable = false)

    check:
      waku.lightpushPeerAvailable(shard) # usable for a plain send
      waku.selectMixLightpushPeer(shard).isNone() # but not as a mix exit

  asyncTest "a mix key alone does not make a peer a usable exit":
    ## Mix routes over IPv4 TCP or QUIC-v1 only. A peer advertising anything
    ## else is not in the pool however good its mix key is, and handing it over
    ## as a destination costs a delivery round and evicts it from the pool.
    discard addLightpushPeer(mixCapable = true, address = "/dns4/node.test/tcp/60000")

    check:
      waku.lightpushPeerAvailable(shard)
      waku.selectMixLightpushPeer(shard).isNone()

  asyncTest "the mix-capable peer is picked out of a mixed set":
    discard addLightpushPeer(mixCapable = false)
    let mixPeer = addLightpushPeer(mixCapable = true)
    discard addLightpushPeer(mixCapable = false)

    let selected = waku.selectMixLightpushPeer(shard).valueOr:
      raiseAssert "expected the mix-capable peer to be selected"
    check selected.peerId == mixPeer

  asyncTest "a statically configured lightpush node is usable as a mix exit":
    ## A `--lightpushnode` peer goes to the service slot of the peer manager
    ## with only its address: no protocols, no shards, no mix key. The two
    ## filters of `selectPeers` reject the peer until identify and
    ## waku-metadata fill those books. `selectPeer` returns the peer from the
    ## slot at once. The mix exit selection must examine the slot too. If it
    ## does not, the plain path works and mix does not.
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let address = MultiAddress.init("/ip4/127.0.0.1/tcp/60000").tryGet()
    waku.node.peerManager.addServicePeer(
      RemotePeerInfo.init(peerId, @[address]), WakuLightPushCodec
    )

    check:
      waku.lightpushPeerAvailable(shard) # the plain path already works
      # ... and the protocol scan with the shard filter does not return the peer
      waku.node.peerManager.selectPeers(WakuLightPushCodec, Opt.some(shard)).len == 0
      waku.selectMixLightpushPeer(shard).isNone() # no mix key learned yet

    # Later, discovery (kademlia or rendezvous) gets the mix key of the peer.
    let keyPair = generateKeyPair().expect("mix key pair")
    waku.node.peerManager.addPeer(
      RemotePeerInfo.init(peerId, @[address], mixPubKey = Opt.some(keyPair.publicKey))
    )

    let selected = waku.selectMixLightpushPeer(shard).valueOr:
      raiseAssert "the slotted lightpush node should be offered as a mix exit"
    check selected.peerId == peerId

  asyncTest "the exit node of a failed attempt is avoided when another exit node exists":
    ## An exit node that lost one reply is not an exit node that is gone, so
    ## the selection returns it when it is the only one.
    let first = addLightpushPeer(mixCapable = true)
    let second = addLightpushPeer(mixCapable = true)

    let other = waku.selectMixLightpushPeer(shard, avoid = Opt.some(first)).valueOr:
      raiseAssert "expected an exit node"
    check other.peerId == second

    waku.node.peerManager.switch.peerStore.delete(second)
    let only = waku.selectMixLightpushPeer(shard, avoid = Opt.some(first)).valueOr:
      raiseAssert "expected the only exit node"
    check only.peerId == first

  asyncTest "configured mix nodes survive the pruning of the peer store":
    ## A configured mix node is not connected, has no ENR and no shards until
    ## the node dials it. That is the shape the peer store prunes first. The
    ## pool is a view over the store, so a pruned bootnode leaves the pool.
    var bootnodes: seq[MixNodePubInfo]
    for i in 0 ..< 4:
      let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
      let keyPair = generateKeyPair().expect("mix key pair")
      bootnodes.add(
        MixNodePubInfo(
          multiAddr: "/ip4/127.0.0.1/tcp/" & $(60100 + i) & "/p2p/" & $peerId,
          pubKey: keyPair.publicKey,
        )
      )
    let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(3'u16, mixPrivKey, bootnodes)).isOkOr:
      raiseAssert "failed to mount mix: " & error
    check waku.node.getMixNodePoolSize() == 4

    # More shardless, not connected peers than the store holds.
    let peerStore = waku.node.peerManager.switch.peerStore
    peerStore.setCapacity(6)
    for i in 0 ..< 10:
      discard addLightpushPeer(mixCapable = false)
    waku.node.peerManager.prunePeerStore()

    check:
      peerStore[AddressBook].book.len <= 6
      waku.node.getMixNodePoolSize() == 4

  asyncTest "the health report says mix is not ready while the pool is below the minimum":
    ## Mounted with a pool that cannot build a path, mix does not deliver.
    ## The health report must not say "ready" for it.
    let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(3'u16, mixPrivKey, newSeq[MixNodePubInfo]())).isOkOr:
      raiseAssert "failed to mount mix: " & error
    let monitor = NodeHealthMonitor.new(waku.node)
    let health = monitor.getSyncProtocolHealthInfo(WakuProtocol.MixProtocol)
    check:
      health.health == HealthStatus.NOT_READY
      "of 4 nodes" in health.desc.get("")

  asyncTest "a statically configured mix node is a usable pool entry, pinned against pruning":
    ## A `--mixnode` entry has the full multiaddr of the peer. Mix compares
    ## pool addresses with its transport patterns, so the stored address must
    ## not have the `/p2p/` part. The address must have the confidence
    ## `Infinite`, so that libp2p does not remove it after 1 hour.
    let peerId = PeerId.init(generateSecp256k1Key()).tryGet()
    let keyPair = generateKeyPair().expect("mix key pair")
    let bootnode = MixNodePubInfo(
      multiAddr: "/ip4/127.0.0.1/tcp/60000/p2p/" & $peerId, pubKey: keyPair.publicKey
    )
    let (mixPrivKey, _) = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(3'u16, mixPrivKey, @[bootnode])).isOkOr:
      raiseAssert "failed to mount mix: " & error

    let peerStore = waku.node.peerManager.switch.peerStore
    check:
      waku.node.getMixNodePoolSize() == 1
      MixNodePool.new(peerStore).get(peerId).isSome()
      peerStore[AddressBook].entries(peerId).anyIt(
        it.confidence == AddressConfidence.Infinite
      )


## A stub for `MixEntryConnection` of `libp2p_mix` without the reply time
## limit of the library. `write` completes (the request left), `readOnce`
## waits for a future that only the reply completes, and `closeImpl` only
## cancels the closure that completes that future. The stub shows that
## `publishOverMix` returns without help from the library time limit, and
## that each outcome comes from the side of the write where the failure fell.
type StubMixConn = ref object of Connection
  incoming: AsyncQueue[seq[byte]]
  incomingFut: Future[void]
  replyReceivedFut: Future[void]
  sendStall: Future[void].Raising([CancelledError])
  stallInSend: bool
  failSend: bool
  failRead: bool
  answer: Opt[LightPushResponse]
    ## The exit node's answer. The stub takes the request id from the frame it
    ## receives and replies with this status after the write.
  cached: seq[byte]

method readOnce(
    s: StubMixConn, pbytes: pointer, nbytes: int
): Future[int] {.async: (raises: [CancelledError, LPStreamError]).} =
  if s.isEof:
    raise newLPStreamEOFError()
  if s.failRead:
    # The request was written. libp2p raises a subclass of `LPStreamError`
    # for a reply it cannot read.
    raise newLPStreamIncompleteError()
  if s.cached.len == 0:
    try:
      await s.replyReceivedFut
      if s.cached.len == 0:
        s.isEof = true
        raise newLPStreamEOFError()
    except CancelledError as exc:
      raise exc
    except LPStreamEOFError as exc:
      raise exc
    except CatchableError as exc:
      raise (ref LPStreamError)(msg: "error in readOnce: " & exc.msg, parent: exc)
  let toRead = min(nbytes, s.cached.len)
  copyMem(pbytes, addr s.cached[0], toRead)
  s.cached = s.cached[toRead ..^ 1]
  return toRead

proc framed(bytes: seq[byte]): seq[byte] =
  ## The length-prefixed frame, as `writeLp` builds it.
  let prefix = PB.toBytes(bytes.len.uint64)
  result = newSeqUninit[byte](prefix.len + bytes.len)
  result[0 ..< prefix.len] = prefix.toOpenArray()
  result[prefix.len ..< result.len] = bytes

method write(
    s: StubMixConn, msg: sink seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  # `stallInSend` models the dial of the first hop in
  # `anonymizeLocalProtocolSend`. Mix dials with `switch.dial` and not with the
  # peer manager, so `DefaultDialTimeout` does not apply. Pool entries do not
  # expire, so a dial to a mix node that went offline is a usual failure.
  if s.stallInSend:
    await s.sendStall
  if s.failSend:
    # `MixEntryConnection.write` raises this error when the first hop refuses
    # the connection. The error is a stream error, not a dial error.
    raise newException(LPStreamError, "Failed to dial to next hop")
  if s.answer.isSome():
    # The request left. The exit node answers it by its request id.
    var vb = initVBuffer(msg)
    var request: seq[byte]
    if vb.readSeq(request) <= 0:
      raise newException(LPStreamError, "stub: no length-prefixed request")
    let req = LightpushRequest.decode(request).valueOr:
      raise newException(LPStreamError, "stub: request does not decode")
    var response = s.answer.get()
    response.requestId = req.requestId
    s.incoming.putNoWait(framed(response.encode().buffer))

method closeImpl(s: StubMixConn): Future[void] {.async: (raises: []).} =
  if not s.incomingFut.isNil():
    s.incomingFut.cancelSoon()

method getWrapped(s: StubMixConn): Connection =
  nil

proc newStubMixConn(
    stallInSend = false,
    failSend = false,
    failRead = false,
    answer = Opt.none(LightPushResponse),
): StubMixConn =
  var inst = StubMixConn(
    stallInSend: stallInSend, failSend: failSend, failRead: failRead, answer: answer
  )
  inst.incoming = newAsyncQueue[seq[byte]]()
  inst.replyReceivedFut = newFuture[void]("stub.replyReceived")
  inst.sendStall = Future[void].Raising([CancelledError]).init("stub.sendStall")
  let checkForIncoming = proc(): Future[void] {.async: (raises: [CancelledError]).} =
    inst.cached = await inst.incoming.get()
    inst.replyReceivedFut.complete()
  inst.incomingFut = checkForIncoming()
  return inst

proc accepted(relayPeerCount: uint32): Opt[LightPushResponse] =
  Opt.some(
    LightPushResponse(
      statusCode: LightPushSuccessCode.SUCCESS,
      relayPeerCount: Opt.some(relayPeerCount),
    )
  )

proc refused(code: LightPushStatusCode, desc: string): Opt[LightPushResponse] =
  Opt.some(LightPushResponse(statusCode: code, statusDesc: Opt.some(desc)))

suite "Mix send path - the typed outcome of one attempt":
  ## `publishOverMix` makes the request and reply exchange itself, so that
  ## the outcome says on which side of the write a failure fell. The library
  ## has a reply time limit in `readOnce`, but the dial of the first hop has no
  ## time limit. Without `publishOverMix`, a stall in the send holds the
  ## attempt for as long as the dial. These tests cover each outcome, a stall
  ## in the read, a stall in the send, and the cancellation.
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  # `publishOverMix` waits `MixReplyTimeout` by default. These tests use a
  # short limit, because the test subject is the mechanism and not the constant.
  const ReplyBudget = chronos.milliseconds(200)
  const TestShard = PubsubTopic("/waku/2/rs/3/0")

  proc testMessage(): WakuMessage =
    fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")

  proc givesUpOn(stallInSend: bool): Future[MixSendOutcome] {.async.} =
    ## Calls the real `publishOverMix` with a mix connection that does not
    ## answer, and returns the outcome. The test does not await the call
    ## directly. It uses `race` with a long timer: when `publishOverMix` does
    ## not return, the test fails one check and the test suite continues.
    let conn = newStubMixConn(stallInSend = stallInSend)
    let publishFut =
      waku.node.publishOverMix(Connection(conn), TestShard, testMessage(), ReplyBudget)
    let guard = sleepAsync(chronos.seconds(5))
    discard await race(FutureBase(publishFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not publishFut.finished():
      publishFut.cancelSoon()
      raiseAssert "publishOverMix never returned; the attempt would hold its slot"
    return await publishFut

  asyncTest "an accepted reply is Confirmed":
    let conn = newStubMixConn(answer = accepted(2))
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), chronos.seconds(5)
    )
    check:
      outcome.certainty == MixSendCertainty.Confirmed
      outcome.reason == MixSendReason.Accepted
      outcome.requestLeft()
      outcome.answer.isOk() and outcome.answer.get() == 2

  asyncTest "a refusal of the exit node is NotSent with the exit node's answer":
    ## The exit node answered that it did not publish. The message is not on
    ## the network, and the exit node has seen it: `requestLeft` is true.
    let conn = newStubMixConn(
      answer = refused(LightPushErrorCode.NO_PEERS_TO_RELAY, "no relay peers")
    )
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), chronos.seconds(5)
    )
    check:
      outcome.certainty == MixSendCertainty.NotSent
      outcome.reason == MixSendReason.ExitRefused
      outcome.requestLeft()
      outcome.answer.isErr()
      outcome.answer.error.code == LightPushErrorCode.NO_PEERS_TO_RELAY

  asyncTest "a success with zero relay peers is a refusal":
    ## The lightpush protocol maps a success with no relay peer to an error
    ## code. The outcome follows that code, not the success status.
    let conn = newStubMixConn(answer = accepted(0))
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), chronos.seconds(5)
    )
    check:
      outcome.reason == MixSendReason.ExitRefused
      outcome.answer.isErr()
      outcome.answer.error.code == LightPushErrorCode.NO_PEERS_TO_RELAY

  asyncTest "a dropped reply is MaybeSent, given up on instead of waited on forever":
    let outcome = await givesUpOn(stallInSend = false)
    check:
      outcome.certainty == MixSendCertainty.MaybeSent
      outcome.reason == MixSendReason.ReplyTimeout
      outcome.requestLeft()

  asyncTest "a reply that cannot be read is MaybeSent":
    ## The request was written, so the exit node may have published the
    ## message. The outcome says so, and the caller keeps its mark.
    let conn = newStubMixConn(failRead = true)
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), ReplyBudget
    )
    check:
      outcome.certainty == MixSendCertainty.MaybeSent
      outcome.reason == MixSendReason.ReplyUnreadable
      outcome.requestLeft()

  asyncTest "a first hop that cannot be dialed is NotSent, and fails the attempt at once":
    ## Mix reports a first hop that refuses the connection as a stream error
    ## on the write. Nothing left the node. The attempt must not wait for the
    ## reply budget after that.
    let conn = newStubMixConn(failSend = true)
    let publishFut = waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), chronos.seconds(5)
    )
    let guard = sleepAsync(chronos.seconds(2))
    discard await race(FutureBase(publishFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not publishFut.finished():
      publishFut.cancelSoon()
      raiseAssert "a failed send waited for the reply budget instead of returning"
    let outcome = await publishFut
    check:
      outcome.certainty == MixSendCertainty.NotSent
      outcome.reason == MixSendReason.PreWriteFailure
      not outcome.requestLeft()

  asyncTest "a message too large for a mix packet is NotSent and TooLarge, before any write":
    ## A sphinx packet has a fixed size, and the mix path does not divide
    ## messages. The check is on the length-prefixed request against the
    ## payload next to one return path, so it is exact, and it runs before
    ## the write: a retry cannot make the message fit.
    let maxPayload = getMaxMessageSizeForCodec(WakuLightPushCodec, MixReplySurbs).expect(
        "max payload"
      )
    let conn = newStubMixConn(failSend = true) # a write would fail the test
    let big = fakeWakuMessage(
      payload = newSeq[byte](maxPayload), contentTopic = "/test/1/anonymity/proto"
    )
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, big, chronos.seconds(5)
    )
    check:
      outcome.certainty == MixSendCertainty.NotSent
      outcome.reason == MixSendReason.TooLarge
      not outcome.requestLeft()
      outcome.answer.isErr()
      outcome.answer.error.code == LightPushErrorCode.PAYLOAD_TOO_LARGE

  asyncTest "a message at the exact limit is written":
    ## The complement of the size check: the largest frame that fits goes to
    ## the connection.
    let maxPayload = getMaxMessageSizeForCodec(WakuLightPushCodec, MixReplySurbs).expect(
        "max payload"
      )
    # The frame is the request (topic, request id, the message) with a
    # varint length. Find the payload size whose frame is exactly the limit.
    var payloadLen = maxPayload
    while true:
      let msg = fakeWakuMessage(
        payload = newSeq[byte](payloadLen), contentTopic = "/test/1/anonymity/proto"
      )
      let req = LightpushRequest(
        requestId: "00000000000000000000", pubSubTopic: Opt.some(TestShard), message: msg
      )
      let frameLen = framed(req.encode().buffer).len
      if frameLen <= maxPayload:
        break
      payloadLen -= frameLen - maxPayload
    let conn = newStubMixConn(answer = accepted(1))
    let fits = fakeWakuMessage(
      payload = newSeq[byte](payloadLen), contentTopic = "/test/1/anonymity/proto"
    )
    let outcome = await waku.node.publishOverMix(
      Connection(conn), TestShard, fits, chronos.seconds(5)
    )
    check outcome.reason == MixSendReason.Accepted

  asyncTest "stopping the send mid-flight lets the cancellation through":
    ## The send service stops with `cancelAndWait` on each attempt. When a
    ## publish converts that cancellation to an outcome, or when its
    ## cancellation does not complete, the stop does not complete.
    let conn = newStubMixConn(stallInSend = true)
    let publishFut = waku.node.publishOverMix(
      Connection(conn), TestShard, testMessage(), chronos.seconds(30)
    )
    await sleepAsync(chronos.milliseconds(50))

    let cancelFut = publishFut.cancelAndWait()
    let guard = sleepAsync(chronos.seconds(5))
    discard await race(FutureBase(cancelFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not cancelFut.finished():
      raiseAssert "cancelling the mix publish never completed"
    check publishFut.cancelled()

  asyncTest "stopping the send during a store query lets the cancellation through":
    ## The service loop confirms a plain-path message with a store query. A
    ## stop cancels the loop while the query dials a store peer. When the dial
    ## or the query converts the cancellation to an error, the loop continues
    ## and the stop does not complete. The store peer here accepts the
    ## connection and does not answer, so the dial waits.
    proc holdOpen(
        server: StreamServer, client: StreamTransport
    ) {.async: (raises: []).} =
      discard

    let silent = createStreamServer(initTAddress("127.0.0.1:0"), holdOpen, {ReuseAddr})
    silent.start()
    defer:
      silent.stop()
      await silent.closeWait()
    (await waku.start()).isOkOr:
      raiseAssert "failed to start the node: " & error
    waku.node.mountStoreClient()
    let storePeer = PeerId.init(generateSecp256k1Key()).tryGet()
    let address = "/ip4/127.0.0.1/tcp/" & $uint16(silent.localAddress().port)
    waku.node.peerManager.addPeer(
      RemotePeerInfo.init(
        storePeer, @[MultiAddress.init(address).tryGet()], protocols = @[WakuStoreCodec]
      )
    )

    let queryFut = waku.storeQueryToAny(StoreQueryRequest())
    await sleepAsync(chronos.milliseconds(100))

    let cancelFut = queryFut.cancelAndWait()
    let guard = sleepAsync(chronos.seconds(5))
    discard await race(FutureBase(cancelFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not cancelFut.finished():
      raiseAssert "cancelling the store query never completed"
    check queryFut.cancelled()

  asyncTest "a stalled first-hop dial is given up on too, as MaybeSent":
    ## The second stall. The write did not complete, and whether bytes left
    ## the node is not known: the outcome says the request may have left.
    ## Without the `reset` of `publishOverMix`, which makes the close return
    ## at once, the cancellation of the stalled write does not complete.
    let outcome = await givesUpOn(stallInSend = true)
    check:
      outcome.certainty == MixSendCertainty.MaybeSent
      outcome.reason == MixSendReason.ReplyTimeout