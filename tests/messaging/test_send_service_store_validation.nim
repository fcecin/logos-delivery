{.used.}

import std/net
import chronos, chronicles, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/api/store,
  logos_delivery/waku/waku_core,
  logos_delivery/api/types,
  logos_delivery/api/conf/messaging_conf,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, delivery_task],
  logos_delivery/api/events/messaging_client_events
import ../testlib/[testasync, wakucore]

## Store-based reliability confirms a propagated message by asking a store node
## whether it holds that message's hash. The query goes in clear text from this
## node's own address, so it must never be made for a message that went out over
## mix: it would hand an observer the link the mixed send paid to break.

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
    let svc = service(reliability = true)
    check:
      not svc.awaitsStoreValidation(propagatedTask(overMix = false, ephemeral = true))
      not svc.awaitsStoreValidation(propagatedTask(overMix = true, ephemeral = true))

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

type FailProc = ref object of BaseSendProcessor
method process(self: FailProc, task: DeliveryTask): Future[void] {.async.} =
  ## Terminal, synchronous failure -- the shape a Required fail-fast takes.
  task.state = DeliveryState.FailedToDeliver
  task.errorDesc = "review: no path"

suite "SendService - mix completion and terminal-event ordering":
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
    let l = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          inc sent
        ,
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, l)
    let mgr = RateLimitManager.new(DefaultRateLimitConfig).expect("rlm")
    let proc0 = ScriptedProc(overMix: false) # first attempt: plain
    let svc =
      SendService.new(true, waku, mgr, proc0, AnonymityLevel.Preferred).expect("svc")
    let task = mkTask("plain-then-mix")
    await svc.send(task)
    await sleepAsync(chronos.milliseconds(10))
    check task.propagateEventEmitted # plain propagation reported
    check sent == 0 # ... but not a mixed completion
    # now a mix retry succeeds for the same task
    task.state = DeliveryState.NextRoundRetry
    proc0.overMix = true
    await svc.trySendMessages()
    svc.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await svc.stopSendService()
    check sent == 1 # MessageSent must still fire

  asyncTest "reliability off: a mixed send ends the same as a plain one (no MessageSent)":
    ## The terminal event must not depend on the path: with store reliability
    ## off, neither a plain nor a mixed send has a confirmation to report, so
    ## both end at MessagePropagated only (R3-4).
    var sent = 0
    let l = MessageSentEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageSentEvent) {.async: (raises: []).} =
          inc sent
        ,
      )
      .expect("listen")
    defer:
      await MessageSentEvent.dropListener(waku.brokerCtx, l)
    let mgr = RateLimitManager.new(DefaultRateLimitConfig).expect("rlm")
    let svc = SendService
      .new(false, waku, mgr, ScriptedProc(overMix: true), AnonymityLevel.Preferred)
      .expect("svc") # reliability = false
    let task = mkTask("mixed-no-reliability")
    await svc.send(task)
    svc.startSendService()
    await sleepAsync(chronos.milliseconds(20))
    await svc.stopSendService()
    check sent == 0

  asyncTest "a terminal outcome is not emitted before send() yields to its caller":
    ## Regression: a Required fail-fast reported synchronously inside `send`,
    ## which the messaging API asyncSpawns and whose id the caller records only
    ## after it yields -- so the event fired before anyone was listening.
    var errors = 0
    let l = MessageErrorEvent
      .listen(
        waku.brokerCtx,
        proc(e: MessageErrorEvent) {.async: (raises: []).} =
          inc errors
        ,
      )
      .expect("listen")
    defer:
      await MessageErrorEvent.dropListener(waku.brokerCtx, l)
    let mgr = RateLimitManager.new(DefaultRateLimitConfig).expect("rlm")
    let svc = SendService
      .new(true, waku, mgr, FailProc(), AnonymityLevel.Required)
      .expect("svc")
    let task = mkTask("failfast")
    let fut = svc.send(task)
    # the emit must not have happened during the synchronous run up to the first yield
    check errors == 0
    await fut
    await sleepAsync(chronos.milliseconds(10))
    check errors == 1 # ... but it does happen, promptly
