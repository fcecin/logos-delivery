{.used.}

## A mix send whose answers are lost, against the real mix library and relay.
## The exit publishes the message and does not answer, as when a NAT drops the reply.

import chronos, testutils/unittests, results
import brokers/broker_context
import libp2p/[peerid, peerinfo, switch]
import libp2p_mix/curve25519
from metrics import value
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/subscription_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/messaging/messaging_metrics,
  logos_delivery/waku/rln/[markers, rln_plugin],
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, mix_processor, lightpush_processor, delivery_task]
import ../testlib/[testasync, wakucore, mix_size_network, wakunodeconf]

const
  ResendInterval = chronos.milliseconds(500)
    ## Shorter than `MixResendInterval`, so that the resend fits in a test.
  ReplyTimeout = chronos.seconds(1)
    ## Shorter than `MixReplyTimeout`, so that a lost answer ends the wait soon.
  Window = chronos.seconds(5)
    ## The delivery window, for one send and its resend with their waits.

proc senderConf(): WakuConf =
  var nodeConf = defaultTestWakuNodeConf()
  nodeConf.clusterId = Opt.some(DefaultClusterId)
  return nodeConf.toWakuConf().valueOr:
    raiseAssert error

proc startSender(
    net: MixSizeNetwork, replyTimeout = ReplyTimeout
): Future[Waku] {.async.} =
  ## A sender with the mix nodes of `net` in its pool and the exit as lightpush peer.
  ## It leaves the shard of its mode, so no copy of its message reaches it.
  # The mix nodes also receive the message.
  # Their events must not reach the send service of the sender.
  var waku: Waku
  lockNewGlobalBrokerContext:
    waku = (await Waku.new(senderConf())).expect("Waku.new")
    let keys = generateKeyPair().expect("mix key pair")
    (
      await waku.node.mountMix(
        DefaultClusterId, keys.privateKey, net.infos, allowAllAddresses = true
      )
    ).isOkOr:
      raiseAssert "mountMix: " & $error
    (await waku.start()).expect("start")
  waku.node.wakuMix.replyTimeout = replyTimeout
  waku.node.subscriptionManager.unsubscribeShard(DefaultPubsubTopic).expect(
    "unsubscribe"
  )
  for node in net.nodes:
    await waku.node.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)
  waku.node.peerManager.addServicePeer(
    net.exit.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
  )
  checkUntilTimeout:
    waku.mixReady()
    waku.selectMixLightpushPeer(DefaultPubsubTopic).isSome()
  return waku

proc newService(
    waku: Waku, level: AnonymityLevel, chain: BaseSendProcessor, window = Window
): SendService =
  let manager =
    RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
  let service = SendService
    .new(false, waku, manager, chain, level, retryInterval = chronos.milliseconds(200))
    .expect("SendService.new")
  service.maxDeliveryTime = window
  service.startSendService()
  return service

proc buildChain(
    waku: Waku, level: AnonymityLevel, mixWindow: Duration
): BaseSendProcessor =
  ## The chain of `level` with `mixWindow` and a short resend interval.
  let mix = MixSendProcessor.new(
    waku, waku.brokerCtx, level, mixWindow, resendInterval = ResendInterval
  )
  if level == AnonymityLevel.Preferred:
    mix.chain(LightpushSendProcessor.new(waku, waku.brokerCtx))
  return mix

proc lostAnswerTask(id: string): DeliveryTask =
  let msg =
    fakeWakuMessage(payload = id, contentTopic = "/mix-unconfirmed/1/probe/proto")
  DeliveryTask(
    requestId: RequestId(id),
    pubsubTopic: DefaultPubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(DefaultPubsubTopic, msg),
    state: DeliveryState.Entry,
    firstAdmittedTime: Opt.some(Moment.now()), # no budget and no proof
  )

type SendEvents = ref object ## The send events of one sender.
  propagated, unconfirmed, errors: int
  propagatedListener: MessagePropagatedEventListener
  unconfirmedListener: MessageUnconfirmedEventListener
  errorListener: MessageErrorEventListener

proc countSendEvents(waku: Waku): SendEvents =
  let events = SendEvents()
  events.propagatedListener = MessagePropagatedEvent
    .listen(
      waku.brokerCtx,
      proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
        events.propagated.inc(),
    )
    .expect("listen")
  events.unconfirmedListener = MessageUnconfirmedEvent
    .listen(
      waku.brokerCtx,
      proc(event: MessageUnconfirmedEvent) {.async: (raises: []).} =
        events.unconfirmed.inc(),
    )
    .expect("listen")
  events.errorListener = MessageErrorEvent
    .listen(
      waku.brokerCtx,
      proc(event: MessageErrorEvent) {.async: (raises: []).} =
        events.errors.inc(),
    )
    .expect("listen")
  return events

proc dropListeners(events: SendEvents, waku: Waku) {.async.} =
  await MessagePropagatedEvent.dropListener(waku.brokerCtx, events.propagatedListener)
  await MessageUnconfirmedEvent.dropListener(waku.brokerCtx, events.unconfirmedListener)
  await MessageErrorEvent.dropListener(waku.brokerCtx, events.errorListener)

suite "Waku Mix - a send whose answers are lost":
  asyncTest "a send whose answers are all lost ends unconfirmed after its resend":
    let hook = ExitHook.new(answersToDrop = high(int))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.startSender()
    defer:
      discard await waku.stop()
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    let service = waku.newService(
      AnonymityLevel.Required, waku.buildChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("all-lost")
    let unconfirmedBefore = logos_delivery_send_unconfirmed.value()

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      events.propagated == 0
      hook.requests == 1 + MaxMixResends
      task.resendsAfterNoAnswer == MaxMixResends
      logos_delivery_send_unconfirmed.value() == unconfirmedBefore + 1

  asyncTest "a refused resend does not fail a send whose first answer was lost":
    ## The first request can have delivered the message, so a refusal says nothing.
    let hook = ExitHook.new(answersToDrop = 1, refusal = Opt.some("refused"))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.startSender()
    defer:
      discard await waku.stop()
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    let service = waku.newService(
      AnonymityLevel.Required, waku.buildChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("resend-refused")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      hook.requests == 1 + MaxMixResends

  asyncTest "an RLN refusal of a resend keeps the first admission and does not fail the send":
    ## A new proof makes a new relay message, so the task keeps its proof and admission.
    ## The refusal does not end the task, because an earlier send can have delivered it.
    let hook = ExitHook.new(answersToDrop = 1, refusal = Opt.some(RlnValidatorErrorMsg))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.startSender()
    defer:
      discard await waku.stop()
    # A backend that can refresh, so that the task could drop its proof and admission.
    waku.node.rlnPlugin = Opt.some(
      RlnPlugin(
        onProofRejected: proc() {.gcsafe, raises: [].} =
          discard
      )
    )
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    let service = waku.newService(
      AnonymityLevel.Required, waku.buildChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("rln-refused")
    let admitted = task.firstAdmittedTime

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      hook.requests == 1 + MaxMixResends
      # A new proof needs a new admission.
      task.firstAdmittedTime == admitted

  asyncTest "a resend that the exit fails before it publishes does not use up the resend":
    ## The exit drops the answer of the first request.
    ## It answers each later request with `INTERNAL_SERVER_ERROR`, before it publishes.
    ## So no later request puts the message on the network or counts as the resend.
    let hook = ExitHook.new(
      answersToDrop = 1,
      refusal = Opt.some("Error generating message id, skipping publish"),
      refusalCode = LightPushErrorCode.INTERNAL_SERVER_ERROR,
    )
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    # A longer time limit, so that each answer of the exit comes before it.
    let waku = await net.startSender(replyTimeout = chronos.seconds(3))
    defer:
      discard await waku.stop()
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    let window = chronos.seconds(8)
    let service = waku.newService(
      AnonymityLevel.Required,
      waku.buildChain(AnonymityLevel.Required, DeliveryWindow),
      window,
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("exit-fault")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      task.resendsAfterNoAnswer == 0
      hook.requests > 1 + MaxMixResends

  asyncTest "a resend that mix cannot write does not use up the resend":
    ## The exit drops the answer of the first request.
    ## Then the other mix nodes stop, so each later dial of a first hop fails.
    ## A write that the Mix library refuses puts nothing on the network.
    ## So no later attempt counts as the resend, and the exit gets one request.
    let hook = ExitHook.new(answersToDrop = 1)
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.startSender()
    defer:
      discard await waku.stop()
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    # A longer resend interval, so that the mix nodes stop before the resend.
    let mix = MixSendProcessor.new(
      waku,
      waku.brokerCtx,
      AnonymityLevel.Required,
      DeliveryWindow,
      resendInterval = chronos.seconds(2),
    )
    let service = waku.newService(AnonymityLevel.Required, mix)
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("unwritten-resend")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window, chronos.milliseconds(50)):
      hook.requests == 1
    for node in net.nodes[1 ..^ 1]:
      await node.stop()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      hook.requests == 1
      task.resendsAfterNoAnswer == 0
      task.tryCount > 1 + MaxMixResends

  asyncTest "a Preferred send whose answers are lost stays on mix past its mix window and a restart":
    ## The exit can have published the message, so the plain path must not get it.
    let hook = ExitHook.new(answersToDrop = high(int))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    # A longer time limit keeps the check before the stop clear of its end.
    let waku = await net.startSender(replyTimeout = chronos.seconds(3))
    defer:
      discard await waku.stop()
    let events = waku.countSendEvents()
    defer:
      await events.dropListeners(waku)
    let window = chronos.seconds(7)
    let service = waku.newService(
      AnonymityLevel.Preferred,
      waku.buildChain(AnonymityLevel.Preferred, chronos.seconds(1)),
      window,
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("preferred-lost")

    check service.enqueue(task).isOk()
    # The stop comes while the first request waits for its answer.
    checkUntilTimeoutCustom(window, chronos.milliseconds(50)):
      hook.requests == 1
    check not task.sentWithNoAnswer
    await service.stopSendService()
    check task.sentWithNoAnswer
    service.startSendService()

    checkUntilTimeoutCustom(window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      not task.deanonymized
      events.errors == 0
      hook.requests == 1 + MaxMixResends
