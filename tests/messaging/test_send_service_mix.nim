{.used.}

import std/[net, sequtils, strutils, tables]
import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  libp2p_mix/[curve25519, pool],
  libp2p/[peerid, multiaddress, peerstore],
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
  logos_delivery/waku/waku_lightpush/common,
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

type FailingAfterProcessor = ref object of BaseSendProcessor
  ## An attempt that ends in a failure the processor does not retry, after
  ## `delay`: the exit node answered that it refused the request.
  delay: Duration

method isValidProcessor(
    self: FailingAfterProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: FailingAfterProcessor, task: DeliveryTask
): Future[void] {.async.} =
  await sleepAsync(self.delay)
  task.state = DeliveryState.FailedToDeliver
  task.errorDesc = "refused by the exit node"

type RetryWithoutMarkProcessor = ref object of BaseSendProcessor
  ## A plain attempt that did not deliver: the task retries, and no mix mark
  ## is set.

method isValidProcessor(
    self: RetryWithoutMarkProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: RetryWithoutMarkProcessor, task: DeliveryTask
): Future[void] {.async.} =
  task.state = DeliveryState.NextRoundRetry

type TimedOutMixProcessor = ref object of BaseSendProcessor
  ## A mix attempt that got no reply: the packet left the node at the start
  ## of the attempt, and the attempt ends after `delay` with no reply.
  delay: Duration
  calls: int

method isValidProcessor(
    self: TimedOutMixProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: TimedOutMixProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  task.lastMixSendTime = Opt.some(Moment.now())
  if self.delay > ZeroDuration:
    await sleepAsync(self.delay)
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

  asyncTest "a mix attempt with no reply holds the plain path for the receipt window":
    ## The packet left the node and the reply did not come. The exit node may
    ## have published the message. The task waits for the sender's own copy
    ## before it goes to the plain path, and starts no new mix attempt while
    ## it waits.
    let plain = PlainSendProcessor()
    let mix = MixSendProcessor.new(
      waku, waku.brokerCtx, AnonymityLevel.Preferred, chronos.minutes(1)
    )
    mix.chain(plain)

    let task = buildTask("lost-reply", chronos.minutes(2))
    task.lastMixSendTime = Opt.some(Moment.now())
    await mix.process(task)
    check:
      plain.calls == 0
      task.state == DeliveryState.NextRoundRetry

    task.lastMixSendTime =
      Opt.some(Moment.now() - MixReceiptWindow - chronos.seconds(1))
    await mix.process(task)
    check:
      plain.calls == 1
      task.lastMixSendTime.isNone() # a copy received from now on is the plain path's

  asyncTest "the sender's own copy of a mixed message counts as propagation":
    ## A mix attempt got no reply, and the node then receives the message
    ## from the network. The message propagated through mix: the events come,
    ## no store validation follows, and the plain path never gets the task.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(
        false,
        waku,
        manager,
        sendProcessor = TimedOutMixProcessor(),
        anonymityLevel = AnonymityLevel.Required,
      )
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()

    var propagated, sent: seq[RequestId]
    let propagatedListener = MessagePropagatedEvent.listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        propagated.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, propagatedListener)
    let sentListener = MessageSentEvent.listen(
      waku.brokerCtx,
      proc(event: MessageSentEvent) {.async: (raises: []).} =
        sent.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, sentListener)

    let task = buildTask("own-copy", chronos.seconds(0))
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(50))
    check:
      task.lastMixSendTime.isSome() # the attempt got no reply
      propagated.len == 0

    MessageReceivedEvent.emit(waku.brokerCtx, task.msgHash.to0xHex(), task.msg)
    await sleepAsync(chronos.milliseconds(50))
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      task.propagatedViaMix
      propagated == @[RequestId("own-copy")]
      sent == @[RequestId("own-copy")]

  asyncTest "a copy that arrives during the first attempt marks the task propagated":
    ## The common timing: the packet leaves at the start of the attempt, the
    ## exit node publishes, and the node's own copy arrives while the attempt
    ## still waits for the reply. The task is not in the cache yet.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let stub = TimedOutMixProcessor(delay: chronos.milliseconds(300))
    let service = SendService
      .new(
        false,
        waku,
        manager,
        sendProcessor = stub,
        anonymityLevel = AnonymityLevel.Required,
      )
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()
    var propagated: seq[RequestId]
    let listener = MessagePropagatedEvent.listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        propagated.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)

    let task = buildTask("early-copy", chronos.seconds(0))
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(100))
    MessageReceivedEvent.emit(waku.brokerCtx, task.msgHash.to0xHex(), task.msg)
    await sleepAsync(chronos.milliseconds(300))
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      task.propagatedViaMix
      propagated == @[RequestId("early-copy")]
    # The next pass of the loop does not send the message again.
    await sleepAsync(ServiceLoopInterval + chronos.milliseconds(200))
    check stub.calls == 1

  asyncTest "a copy without the mix mark is not propagation evidence":
    ## A node that publishes on relay gets its own publish back from its
    ## local handlers, and a filter service node that publishes pushes the
    ## copy back to its Edge client, before any peer sees the message. A
    ## receipt counts for a mix attempt only.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, sendProcessor = RetryWithoutMarkProcessor())
      .expect("SendService.new")
    # A started service: the receipt listener exists from the start on.
    service.startSendService()
    defer:
      await service.stopSendService()
    var propagated: seq[RequestId]
    let listener = MessagePropagatedEvent.listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        propagated.add(event.requestId),
    ).valueOr:
      raiseAssert error
    defer:
      await MessagePropagatedEvent.dropListener(waku.brokerCtx, listener)

    let task = buildTask("local-echo", chronos.seconds(0))
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(50)) # the plain attempt ran, no mark
    check task.lastMixSendTime.isNone()
    MessageReceivedEvent.emit(waku.brokerCtx, task.msgHash.to0xHex(), task.msg)
    await sleepAsync(chronos.milliseconds(50))
    check:
      task.state == DeliveryState.NextRoundRetry
      not task.receivedByNode
      propagated.len == 0

  asyncTest "a task marked propagated before its retry is not sent again":
    ## The pass snapshots the tasks to retry, then runs them in batches. A
    ## receipt that lands between the snapshot and the attempt marks the
    ## task propagated; the attempt must not send the message again.
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

  asyncTest "a receipt during an attempt that ends in a failure keeps the task propagated":
    ## The copy shows that the message is on the network, whatever the exit
    ## node answered afterwards. The application gets sent, not sent then
    ## failed.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(
        false,
        waku,
        manager,
        sendProcessor = FailingAfterProcessor(delay: chronos.milliseconds(300)),
        anonymityLevel = AnonymityLevel.Required,
      )
      .expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()
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

    let task = buildTask("received-then-refused", chronos.seconds(0))
    task.lastMixSendTime = Opt.some(Moment.now()) # a mix attempt in progress
    service.trackedSend(task)
    await sleepAsync(chronos.milliseconds(100))
    MessageReceivedEvent.emit(waku.brokerCtx, task.msgHash.to0xHex(), task.msg)
    await sleepAsync(chronos.milliseconds(400))
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      propagated == @[RequestId("received-then-refused")]
      failed.len == 0

  asyncTest "the mix mark stays after a failure that may have come after the write":
    ## The mark is cleared for a failure before the write only. Unsure means
    ## kept: a cleared mark costs a store query for a mixed message.
    let exit = Opt.some(PeerId.init(generateSecp256k1Key()).tryGet())
    check:
      mixPacketMayHaveLeft(
        (
          code: LightPushErrorCode.INTERNAL_SERVER_ERROR,
          desc: Opt.some("reply garbled"),
        ),
        exit,
      )
      mixPacketMayHaveLeft(
        (
          code: LightPushErrorCode.SERVICE_NOT_AVAILABLE,
          desc: Opt.some(lightpush.MixReplyTimeoutDesc),
        ),
        exit,
      )
      not mixPacketMayHaveLeft(
        (
          code: LightPushErrorCode.SERVICE_NOT_AVAILABLE,
          desc: Opt.some("Failed to dial to next hop"),
        ),
        exit,
      )
      not mixPacketMayHaveLeft(
        (code: LightPushErrorCode.PAYLOAD_TOO_LARGE, desc: Opt.some("too large")), exit
      )
      mixPacketMayHaveLeft(
        (
          code: LightPushErrorCode.SERVICE_NOT_AVAILABLE,
          desc: Opt.some(lightpush.MixReplyUnreadableDesc & ": incomplete"),
        ),
        exit,
      )
      not mixPacketMayHaveLeft(
        (code: LightPushErrorCode.INTERNAL_SERVER_ERROR, desc: Opt.some("no exit")),
        Opt.none(PeerId),
      )
      not mixPacketMayHaveLeft(
        (code: LightPushErrorCode.NO_PEERS_TO_RELAY, desc: Opt.some("no peers")), exit
      )
      not mixPacketMayHaveLeft(
        (code: LightPushErrorCode.TOO_MANY_REQUESTS, desc: Opt.some("rate limited")),
        exit,
      )

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

  asyncTest "retries run together and each one is reported at once":
    ## One retry after the other, a pass over N tasks lasts N attempts, and
    ## the events of the first task wait for the last attempt. The attempts
    ## run together, and each task gets its events when its attempt ends.
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
    let passTook = Moment.now() - passStarted
    await sleepAsync(chronos.milliseconds(20))
    check:
      passTook < attempt * 2 # the two attempts ran together
      propagatedAt.len == 2
      propagatedAt[0] < passStarted + passTook # reported inside the pass

  asyncTest "stopping the service cancels the retries of a batch":
    ## `allFutures` does not cancel its children when it is cancelled. A stop
    ## in the middle of a batch must cancel each attempt, or the attempts
    ## run on a stopping node and report their tasks after the stop did.
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
## limit of the library. `write` completes (the packet left), `readOnce` waits
## for a future that only the reply completes, and `closeImpl` only cancels
## the closure that completes that future. The stub shows that
## `publishOverMix` returns without help from the library time limit.
type StubMixConn = ref object of Connection
  incoming: AsyncQueue[seq[byte]]
  incomingFut: Future[void]
  replyReceivedFut: Future[void]
  sendStall: Future[void].Raising([CancelledError])
  stallInSend: bool
  failSend: bool
  oversized: bool
  failRead: bool
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
  if s.oversized:
    # `MixEntryConnection.write` raises this error for a frame that is larger
    # than one sphinx packet.
    raise newException(LPStreamError, "exceeds max msg size of 4000 bytes")

method closeImpl(s: StubMixConn): Future[void] {.async: (raises: []).} =
  if not s.incomingFut.isNil():
    s.incomingFut.cancelSoon()

method getWrapped(s: StubMixConn): Connection =
  nil

proc newStubMixConn(
    stallInSend = false, failSend = false, oversized = false, failRead = false
): StubMixConn =
  var inst = StubMixConn(
    stallInSend: stallInSend,
    failSend: failSend,
    oversized: oversized,
    failRead: failRead,
  )
  inst.incoming = newAsyncQueue[seq[byte]]()
  inst.replyReceivedFut = newFuture[void]("stub.replyReceived")
  inst.sendStall = Future[void].Raising([CancelledError]).init("stub.sendStall")
  let checkForIncoming = proc(): Future[void] {.async: (raises: [CancelledError]).} =
    inst.cached = await inst.incoming.get()
    inst.replyReceivedFut.complete()
  inst.incomingFut = checkForIncoming()
  return inst

suite "Mix send path - the reply budget":
  ## `publishOverMix` has the time limit for a mix-routed lightpush. The
  ## library has a reply time limit in `readOnce`, but the dial of the first
  ## hop has no time limit. Without `publishOverMix`, a stall in the send
  ## stops the send-service loop for all messages of the node. These tests
  ## cover a stall in the read and a stall in the send. They check that the
  ## wait ends and that the cancellation returns.
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  # `publishOverMix` waits `MixReplyTimeout` by default. These tests use a
  # short limit, because the test subject is the mechanism and not the constant.
  const ReplyBudget = chronos.milliseconds(200)

  proc givesUpOn(stallInSend: bool): Future[WakuLightPushResult] {.async.} =
    ## Calls the real `publishOverMix` with a mix connection that does not
    ## answer, and returns the result. The test does not await the call
    ## directly. It uses `race` with a long timer: when `publishOverMix` does
    ## not return, the test fails one check and the test suite continues.
    let conn = newStubMixConn(stallInSend = stallInSend)
    let msg = fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")

    let publishFut = waku.node.publishOverMix(
      Connection(conn), PubsubTopic("/waku/2/rs/3/0"), msg, ReplyBudget
    )
    let guard = sleepAsync(chronos.seconds(5))
    discard await race(FutureBase(publishFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not publishFut.finished():
      publishFut.cancelSoon()
      raiseAssert "publishOverMix never returned; the send-service loop would wedge"
    return await publishFut

  asyncTest "a dropped reply is given up on instead of waited on forever":
    let res = await givesUpOn(stallInSend = false)
    check:
      res.isErr()
      res.error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE
      # The mix processor keys the lost-reply rules on this description.
      res.error.desc == Opt.some(lightpush.MixReplyTimeoutDesc)

  asyncTest "a reply that cannot be read is reported as a failure after the write":
    ## The request was written, so the exit node may have published the
    ## message. The description says so, and the mix processor keeps its
    ## mark for the sender's own copy.
    let conn = newStubMixConn(failRead = true)
    let msg = fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")
    let res = await waku.node.publishOverMix(
      Connection(conn), PubsubTopic("/waku/2/rs/3/0"), msg, ReplyBudget
    )
    check:
      res.isErr()
      res.error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE
      res.error.desc.get("").startsWith(lightpush.MixReplyUnreadableDesc)

  asyncTest "a first hop that cannot be dialed fails the attempt at once":
    ## Mix reports a first hop that refuses the connection as a stream error
    ## on the write. The lightpush client must not read the mix connection
    ## again after that: the read waits for a reply that does not come, and
    ## the failure becomes a wait for the full time limit. The test examines
    ## the text of the result, not the time, to tell the failure from a time
    ## limit.
    let conn = newStubMixConn(failSend = true)
    let msg = fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")

    let publishFut = waku.node.publishOverMix(
      Connection(conn), PubsubTopic("/waku/2/rs/3/0"), msg, chronos.seconds(5)
    )
    let guard = sleepAsync(chronos.seconds(2))
    discard await race(FutureBase(publishFut), FutureBase(guard))
    await guard.cancelAndWait()

    if not publishFut.finished():
      publishFut.cancelSoon()
      raiseAssert "a failed send waited for the reply budget instead of returning"
    let res = await publishFut
    check:
      res.isErr()
      res.error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE
      "timed out" notin res.error.desc.get("")

  asyncTest "a message too large for a mix packet is reported as such, not as transient":
    ## A sphinx packet has a fixed size, and the entry connection does not
    ## divide messages. A retry cannot make the message fit. The send
    ## processors retry SERVICE_NOT_AVAILABLE, so the result must not be that
    ## status.
    let conn = newStubMixConn(oversized = true)
    let msg = fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")

    let res = await waku.node.publishOverMix(
      Connection(conn), PubsubTopic("/waku/2/rs/3/0"), msg, chronos.seconds(5)
    )
    check:
      res.isErr()
      res.error.code == LightPushErrorCode.PAYLOAD_TOO_LARGE

  asyncTest "stopping the send mid-flight lets the cancellation through":
    ## The send service stops with `cancelAndWait` on its loop. When a publish
    ## converts that cancellation to an error, or when its cancellation does
    ## not complete, the stop does not complete.
    let conn = newStubMixConn(stallInSend = true)
    let msg = fakeWakuMessage(contentTopic = "/test/1/anonymity/proto")

    let publishFut = waku.node.publishOverMix(
      Connection(conn), PubsubTopic("/waku/2/rs/3/0"), msg, chronos.seconds(30)
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

  asyncTest "a stalled first-hop dial is given up on too":
    ## The second stall. When the stall is in the send, the reply future is
    ## pending when the client closes the connection after the failed send.
    ## The close cancels the closure that completes that future. Without the
    ## `reset` of `publishOverMix`, which makes the close return at once, the
    ## cancellation does not complete, and the send-service loop stops.
    let res = await givesUpOn(stallInSend = true)
    check:
      res.isErr()
      res.error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE
