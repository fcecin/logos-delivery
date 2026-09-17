{.used.}

import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/api/store,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task],
  logos_delivery/api/events/[messaging_client_events, kernel_events]
import ../testlib/[testasync, wakunodeconf]

## Store-based reliability confirms a propagated message by asking a store node
## whether it holds that message's hash. The query goes in clear text from this
## node's own address, so it must never be made for a message that went out over
## mix: it would hand an observer the link the mixed send paid to break.

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
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    return SendService.new(reliability, waku, manager).expect("SendService.new")

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
      propagatedOverMix: overMix,
      firstPropagatedTime: Opt.some(Moment.now()),
    )

  asyncTest "the store client is mounted even on a node that serves no store":
    ## The premise of the whole test: `checkStoreForMessages` is
    ## `preferP2PReliability and isStoreMounted()`, and the client is mounted
    ## unconditionally, so reliability alone decides.
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

## A scripted processor lets these drive the completion-event path without a
## live mixnet: it just stamps the outcome the real processors would.
type ScriptedProc = ref object of BaseSendProcessor
  overMix: bool

method process(self: ScriptedProc, task: DeliveryTask): Future[void] {.async.} =
  task.state = DeliveryState.SuccessfullyPropagated
  task.propagatedOverMix = self.overMix
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
    ## Regression: MessageSent for a mix propagation was nested under the
    ## `propagateEventEmitted` guard, so a task that had already emitted
    ## MessagePropagated on a plain attempt lost its completion on the mix retry.
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
    # now a mix retry succeeds for the same task
    task.state = DeliveryState.NextRoundRetry
    scripted.overMix = true
    await service.trySendMessages()
    service.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await service.stopSendService()
    check sent == 1 # MessageSent must still fire

  asyncTest "reliability off: a mixed send ends the same as a plain one (no MessageSent)":
    ## The terminal event must not depend on the path: with store reliability
    ## off, neither a plain nor a mixed send has a confirmation to report, so
    ## both end at MessagePropagated only.
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

  asyncTest "a mixed send is marked seen, so the backfill never asks a store for it by hash":
    ## The receive service fetches, by hash and in clear, every message a store
    ## holds that this node has not seen. A relay publish is seen through the
    ## relay's own delivery; a mixed one leaves through the exit, so the send
    ## service marks it seen itself, once, whatever the reliability setting.
    var seen: seq[WakuMessageHash]
    let listener = MessageSeenEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSeenEvent) {.async: (raises: []).} =
          seen.add(computeMessageHash(e.topic, e.message)),
      )
      .expect("listen")
    defer:
      await MessageSeenEvent.dropListener(waku.brokerCtx, listener)
    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")

    let plain = SendService
      .new(false, waku, manager, ScriptedProc(overMix: false), AnonymityLevel.Preferred)
      .expect("SendService.new")
    await plain.send(mkTask("plain"))
    check seen.len == 0 # a plain send is seen through the relay, not here

    let mixed = SendService
      .new(false, waku, manager, ScriptedProc(overMix: true), AnonymityLevel.Preferred)
      .expect("SendService.new")
    let task = mkTask("mixed")
    await mixed.send(task)
    check seen == @[task.msgHash]

    # the loop reports the task once more before dropping it: still once
    mixed.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await mixed.stopSendService()
    check seen.len == 1
