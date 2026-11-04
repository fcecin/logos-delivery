{.used.}

## A mix send whose reply is lost, against the real mix library and relay.

import chronos, chronicles, testutils/unittests, results
import brokers/broker_context
import libp2p/[peerid, peerinfo, switch]
import libp2p_mix/curve25519
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/conf/messaging_conf,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/[send_service, delivery_task]
import
  ../testlib/[futures, testasync, wakucore, wakunode, mix_size_network, wakunodeconf]

proc senderConf(mode: LogosDeliveryMode): WakuConf =
  var nodeConf = defaultTestWakuNodeConf(mode)
  nodeConf.clusterId = Opt.some(DefaultClusterId)
  return nodeConf.toWakuConf().valueOr:
    raiseAssert error

type Events = ref object
  propagated, sent, unconfirmed, error: Future[void]
  listeners: seq[proc(): Future[void] {.async: (raises: []).}]

proc listen(waku: Waku): Events =
  ## One future for each event of the send, so the test waits for the one it
  ## expects and checks that the others did not come.
  let events = Events(
    propagated: newFuture[void]("propagated"),
    sent: newFuture[void]("sent"),
    unconfirmed: newFuture[void]("unconfirmed"),
    error: newFuture[void]("error"),
  )
  let ctx = waku.brokerCtx
  let propagated = MessagePropagatedEvent
    .listen(
      ctx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        events.propagated.complete(),
    )
    .expect("listen")
  let sent = MessageSentEvent
    .listen(
      ctx,
      proc(event: MessageSentEvent) {.async: (raises: []).} =
        events.sent.complete(),
    )
    .expect("listen")
  let unconfirmed = MessageUnconfirmedEvent
    .listen(
      ctx,
      proc(event: MessageUnconfirmedEvent) {.async: (raises: []).} =
        events.unconfirmed.complete(),
    )
    .expect("listen")
  let error = MessageErrorEvent
    .listen(
      ctx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        events.error.complete(),
    )
    .expect("listen")
  events.listeners.add(
    proc() {.async: (raises: []).} =
      await MessagePropagatedEvent.dropListener(ctx, propagated)
      await MessageSentEvent.dropListener(ctx, sent)
      await MessageUnconfirmedEvent.dropListener(ctx, unconfirmed)
      await MessageErrorEvent.dropListener(ctx, error)
  )
  return events

proc drop(events: Events) {.async.} =
  for listener in events.listeners:
    await listener()

type LostReplyCase = ref object
  ## A sender whose exit holds its answer, so the reply of the first mix send
  ## never comes.
  net: MixSizeNetwork
  hook: ExitHook
  waku: Waku
  service: SendService
  events: Events
  task: DeliveryTask

proc senderBehindAnExitThatHoldsItsAnswer(
    mixResendInterval = chronos.minutes(1), mode = LogosDeliveryMode.Core
): Future[LostReplyCase] {.async.} =
  ## A Core sender reads the shard over relay, so a copy of its message reaches
  ## it. An Edge sender has no relay and no filter subscription, so no copy does.
  let hook = ExitHook.new()
  let net = await startMixNodes(hook)
  # The mix nodes also receive the message. Their events must not reach the
  # send service of the sender.
  var waku: Waku
  lockNewGlobalBrokerContext:
    waku = (await Waku.new(senderConf(mode))).expect("Waku.new")
    let keys = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(DefaultClusterId, keys.privateKey, net.infos)).isOkOr:
      raiseAssert "mountMix: " & $error
    (await waku.start()).expect("start")
  # The answer of the exit is the timer under test. A short one keeps the test
  # short.
  waku.node.wakuMix.replyTimeout = chronos.seconds(2)
  for node in net.nodes:
    await waku.node.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)
  waku.node.peerManager.addServicePeer(
    net.exit.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
  )
  checkUntilTimeout:
    waku.mixReady()
    waku.selectMixLightpushPeer(DefaultPubsubTopic).isSome()
  if mode == LogosDeliveryMode.Core:
    checkUntilTimeout:
      waku.node.hasMeshPeer(DefaultPubsubTopic, net.nodes[1].peerInfo.peerId)

  let manager =
    RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
  let chain = setupSendProcessorChain(waku, AnonymityLevel.Required).expect("chain")
  # The `reliability` option is on, so a plain send would wait for a store
  # confirmation and a mix send waits for its confirmation.
  let service = SendService
    .new(
      true,
      waku,
      manager,
      chain,
      AnonymityLevel.Required,
      retryInterval = chronos.hours(1),
      mixResendInterval = mixResendInterval,
    )
    .expect("SendService.new")
  service.startSendService()
  let msg =
    fakeWakuMessage(payload = "lost-reply", contentTopic = "/mix-seen/1/probe/proto")
  let task = DeliveryTask(
    requestId: RequestId("lost-reply"),
    pubsubTopic: DefaultPubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(DefaultPubsubTopic, msg),
    state: DeliveryState.Entry,
    firstAdmittedTime: Opt.some(Moment.now()), # no budget and no proof
  )
  return LostReplyCase(
    net: net,
    hook: hook,
    waku: waku,
    service: service,
    events: waku.listen(),
    task: task,
  )

proc stop(c: LostReplyCase) {.async.} =
  # Let a held request end before its exit stops.
  c.hook.reply.fire()
  await c.events.drop()
  await c.service.stopSendService()
  discard await c.waku.stop()
  await c.net.stop(@[])

suite "Waku Mix - a send whose reply is lost":
  asyncTest "a send whose exit does not answer propagates, and a copy from the network confirms it":
    let c = await senderBehindAnExitThatHoldsItsAnswer()
    defer:
      await c.stop()

    check c.service.enqueue(c.task).isOk()
    check await c.hook.requested.wait().withTimeout(chronos.seconds(10))
    # The request reached the exit and the exit holds its answer. The send
    # propagated with no confirmation, and no retry reaches the exit.
    check await c.events.propagated.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      c.task.state == DeliveryState.SuccessfullyPropagated
      c.task.propagatedAnonymously
      not c.task.mixConfirmed
      not c.events.sent.finished()
      not c.events.error.finished()

    # `net.nodes[1]` publishes the message, as the exit of an earlier attempt
    # does. The sender receives it over relay, and that confirms the send.
    check (await c.net.nodes[1].publish(Opt.some(DefaultPubsubTopic), c.task.msg)).isOk()
    check await c.events.sent.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      c.task.mixConfirmed
      c.hook.requests == 1
      not c.events.unconfirmed.finished()
      not c.events.error.finished()

  asyncTest "the refusal of a message that the network has confirms a resend":
    ## The exit published the first request and its answer was lost. The resend
    ## reaches the exit again, and its relay already has the message. The
    ## refusal tells the sender that the network has it. The sender is an Edge
    ## node, so no copy of the message confirms the send first.
    let c = await senderBehindAnExitThatHoldsItsAnswer(
      mixResendInterval = chronos.milliseconds(500), mode = LogosDeliveryMode.Edge
    )
    defer:
      await c.stop()

    check c.service.enqueue(c.task).isOk()
    check await c.hook.requested.wait().withTimeout(chronos.seconds(10))
    check await c.events.propagated.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    # The exit publishes the first request now. Its answer goes to a connection
    # that the sender has reset.
    c.hook.reply.fire()
    checkUntilTimeout:
      c.hook.publishResult.isSome()

    check await c.events.sent.withTimeout(FUTURE_TIMEOUT_LONG)
    check:
      c.task.mixConfirmed
      not c.task.seenOnNetwork
      c.task.resendCount == 1
      c.hook.requests == 2
      c.hook.laterResults.len == 1
      not c.events.unconfirmed.finished()
    if c.hook.laterResults.len == 1:
      check:
        c.hook.laterResults[0].isErr()
        c.hook.laterResults[0].error.code == LightPushErrorCode.INVALID_MESSAGE

  asyncTest "a stop during the wait for the answer leaves the send propagated, and the restart confirms it":
    ## The request reached the exit before the stop. A restart makes no new
    ## attempt, emits the propagated event, and a copy from the network confirms
    ## the send.
    let c = await senderBehindAnExitThatHoldsItsAnswer()
    defer:
      await c.stop()

    check c.service.enqueue(c.task).isOk()
    check await c.hook.requested.wait().withTimeout(chronos.seconds(10))
    # The attempt waits for the answer of the exit. The stop cuts it there.
    await c.service.stopSendService()
    check:
      c.task.mixRequestLeft
      c.task.state == DeliveryState.SuccessfullyPropagated
      not c.task.mixAttemptCut
      not c.events.propagated.finished()

    c.service.startSendService()
    check await c.events.propagated.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check (await c.net.nodes[1].publish(Opt.some(DefaultPubsubTopic), c.task.msg)).isOk()
    check await c.events.sent.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      c.hook.requests == 1
      c.task.mixConfirmed
      not c.events.error.finished()
