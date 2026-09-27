{.used.}

import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_node,
  logos_delivery/waku/waku_store,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/store,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task],
  logos_delivery/api/events/messaging_client_events
import ../testlib/[testasync, wakunodeconf, wakucore, wakunode]
import ../waku_store/store_utils

## Store-based reliability asks a store node for a propagated message's hash, in
## clear from this node's own address. It must never ask for a mixed message.

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

suite "SendService - store validation and mix":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc service(reliability: bool): SendService =
    ## The policy under test never sends, so the plain chain is enough.
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let chain = setupSendProcessorChain(waku, AnonymityLevel.None).expect("chain")
    return SendService.new(reliability, waku, manager, chain).expect("SendService.new")

  proc propagatedTask(overMix: bool, ephemeral = false): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/store-validation/proto",
      payload: "hi".toBytes(),
      timestamp: 1_700_000_000_000_000_000,
      ephemeral: ephemeral,
    )
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    return DeliveryTask(
      requestId: RequestId("t"),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.SuccessfullyPropagated,
      propagatedAnonymously: overMix,
      firstPropagatedTime: Opt.some(Moment.now()),
    )

  asyncTest "the store client is mounted even on a node that serves no store":
    ## `checkStoreForMessages` is `preferP2PReliability and isStoreMounted()`,
    ## and every node mounts the store client, so reliability alone decides.
    check waku.isStoreMounted()

  asyncTest "a plainly propagated message is confirmed against a store node":
    check service(reliability = true).awaitsStoreValidation(
      propagatedTask(overMix = false)
    )

  asyncTest "a message that went out over mix is never confirmed against a store node":
    ## The query would carry its hash, in clear, from this node's own address.
    check not service(reliability = true).awaitsStoreValidation(
      propagatedTask(overMix = true)
    )

  asyncTest "an ephemeral message is not confirmed either, mixed or not":
    let reliable = service(reliability = true)
    check:
      not reliable.awaitsStoreValidation(
        propagatedTask(overMix = false, ephemeral = true)
      )
      not reliable.awaitsStoreValidation(
        propagatedTask(overMix = true, ephemeral = true)
      )

  asyncTest "nothing is confirmed when reliability is off":
    check not service(reliability = false).awaitsStoreValidation(
      propagatedTask(overMix = false)
    )

  asyncTest "a task that has not propagated is not confirmed yet":
    let task = propagatedTask(overMix = false)
    task.state = DeliveryState.NextRoundRetry
    check not service(reliability = true).awaitsStoreValidation(task)

## A scripted processor sets the outcome that a real processor would, so these
## tests drive the completion events with no live mixnet.
type ScriptedProc = ref object of BaseSendProcessor
  overMix: bool
  calls: int

method process(self: ScriptedProc, task: DeliveryTask): Future[void] {.async.} =
  inc self.calls
  task.state = DeliveryState.SuccessfullyPropagated
  task.propagatedAnonymously = self.overMix
  task.deliveryTime = Moment.now()
  if task.firstPropagatedTime.isNone():
    task.firstPropagatedTime = Opt.some(Moment.now())

suite "SendService - mix completion":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  proc mkTask(id: string): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/completion/proto",
      payload: "hi".toBytes(),
      timestamp: 1_700_000_000_000_000_000,
    )
    let shard = PubsubTopic("/waku/2/rs/3/0")
    DeliveryTask(
      requestId: RequestId(id),
      pubsubTopic: shard,
      msg: msg,
      msgHash: computeMessageHash(shard, msg),
      state: DeliveryState.Entry,
    )

  asyncTest "a mixed send that follows a prior plain propagation still emits MessageSent":
    ## A task that emitted `MessagePropagated` on a plain attempt still emits
    ## `MessageSent` when a mix retry succeeds.
    var sent = 0
    let listener = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          inc sent
        ,
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, listener)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let scripted = ScriptedProc(overMix: false) # first attempt: plain
    let service = SendService
      .new(true, waku, manager, scripted, AnonymityLevel.Preferred)
      .expect("SendService.new")
    let task = mkTask("plain-then-mix")
    await service.send(task)
    await sleepAsync(chronos.milliseconds(10))
    check task.propagateEventEmitted # plain propagation reported
    check sent == 0 # ... but not a mixed completion
    # then a mix retry of the same task succeeds
    task.state = DeliveryState.NextRoundRetry
    scripted.overMix = true
    await service.trySendMessages()
    service.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await service.stopSendService()
    check sent == 1 # MessageSent must still fire

  asyncTest "a mixed send reports MessageSent once, though it is reported twice":
    ## `send()` reports the task and caches it, and the next pass reports it again
    ## before it drops it. With reliability on, `sentEventEmitted` keeps
    ## `MessageSent` to one event.
    var sent = 0
    let listener = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          inc sent
        ,
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, listener)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(true, waku, manager, ScriptedProc(overMix: true), AnonymityLevel.Preferred)
      .expect("SendService.new") # reliability = on
    let task = mkTask("mixed-once")

    await service.send(task)
    await sleepAsync(chronos.milliseconds(10))
    check sent == 1 # reported by send()

    service.startSendService()
    await sleepAsync(chronos.milliseconds(50))
    await service.stopSendService()
    check sent == 1 # ... and not again by the pass that drops it

  asyncTest "reliability off: a mixed send ends the same as a plain one (no MessageSent)":
    ## With store reliability off, a plain and a mixed send both end at
    ## `MessagePropagated`.
    var sent = 0
    let listener = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          inc sent
        ,
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, listener)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let service = SendService
      .new(false, waku, manager, ScriptedProc(overMix: true), AnonymityLevel.Preferred)
      .expect("SendService.new") # reliability = false
    let task = mkTask("mixed-no-reliability")
    await service.send(task)
    service.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await service.stopSendService()
    check sent == 0

suite "SendService - store validation answers":
  ## The Store node answers with the hashes listed in `storedHashes`.
  var waku {.threadvar.}: Waku
  var storeNode {.threadvar.}: WakuNode
  var storedHashes {.threadvar.}: seq[WakuMessageHash]
  var answered {.threadvar.}: AsyncEvent

  proc storeHandler(
      req: StoreQueryRequest
  ): Future[StoreQueryResult] {.async, gcsafe.} =
    var resp = StoreQueryResponse(
      requestId: req.requestId, statusCode: uint32(StatusCode.SUCCESS)
    )
    for hash in req.messageHashes:
      if hash in storedHashes:
        resp.messages.add(WakuMessageKeyValue(messageHash: hash))
    answered.fire()
    return ok(resp)

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    (await waku.start()).isOkOr:
      raiseAssert "waku.start: " & error
    storedHashes = @[]
    answered = newAsyncEvent()
    storeNode = newTestWakuNode(generateSecp256k1Key())
    storeNode.mountMetadata(TestClusterId, @[0'u16]).isOkOr:
      raiseAssert "mountMetadata: " & error
    discard await newTestWakuStore(storeNode.switch, storeHandler)
    await storeNode.start()
    waku.node.peerManager.addServicePeer(
      storeNode.peerInfo.toRemotePeerInfo(), WakuStoreCodec
    )

  asyncTeardown:
    await storeNode.stop()
    discard await waku.stop()

  asyncTest "a message the Store does not report yet is asked again, never sent again":
    let sentEvent = newAsyncEvent()
    let listener = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          sentEvent.fire(),
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, listener)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let processor = ScriptedProc(overMix: false)
    let service =
      SendService.new(true, waku, manager, processor).expect("SendService.new")
    service.startSendService()
    defer:
      await service.stopSendService()

    let msg = WakuMessage(
      contentTopic: "/test/1/store-answers/proto",
      payload: "hi".toBytes(),
      timestamp: 1_700_000_000_000_000_000,
    )
    let shard = PubsubTopic("/waku/2/rs/3/0")
    let task = DeliveryTask(
      requestId: RequestId("not-yet-stored"),
      pubsubTopic: shard,
      msg: msg,
      msgHash: computeMessageHash(shard, msg),
      state: DeliveryState.Entry,
    )
    await service.send(task)
    check processor.calls == 1
    # Make the task old enough for the next Store check.
    task.firstPropagatedTime = Opt.some(Moment.now() - chronos.seconds(10))

    # The first answer does not list the hash.
    check await answered.wait().withTimeout(chronos.seconds(10))

    # The next answer lists it.
    storedHashes.add(task.msgHash)
    check:
      await sentEvent.wait().withTimeout(chronos.seconds(10))
      task.state == DeliveryState.SuccessfullyValidated
      processor.calls == 1
