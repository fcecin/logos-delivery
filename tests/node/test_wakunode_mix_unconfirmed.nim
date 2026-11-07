{.used.}

## A mix send whose answers are lost, against the real mix library and relay.
## The exit publishes the message and does not answer, as when a NAT eats the
## reply.

import chronos, testutils/unittests, results
import brokers/broker_context
import libp2p/[peerid, peerinfo, switch]
import libp2p_mix/curve25519
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/subscription_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/events/messaging_client_events,
  logos_delivery/waku/rln/markers,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, send_processor, mix_processor, lightpush_processor, delivery_task]
import ../testlib/[testasync, wakucore, mix_size_network, wakunodeconf]

const
  ResendInterval = chronos.milliseconds(500)
    ## Shorter than `MixResendInterval`, so that the resends fit in a test.
  Window = chronos.seconds(35)
    ## The delivery window: one request and `MaxMixResends` resends, each with a
    ## wait of `MixReplyTimeout` for its answer.

proc senderConf(): WakuConf =
  var nodeConf = defaultTestWakuNodeConf()
  nodeConf.clusterId = Opt.some(DefaultClusterId)
  return nodeConf.toWakuConf().valueOr:
    raiseAssert error

proc senderOf(net: MixSizeNetwork): Future[Waku] {.async.} =
  ## A sender with the mix nodes of `net` in its pool and the exit as its
  ## lightpush peer. It leaves the shard that its mode subscribes, so that no
  ## copy of its message reaches it.
  # The mix nodes also receive the message. Their events must not reach the send
  # service of the sender.
  var waku: Waku
  lockNewGlobalBrokerContext:
    waku = (await Waku.new(senderConf())).expect("Waku.new")
    let keys = generateKeyPair().expect("mix key pair")
    (await waku.node.mountMix(DefaultClusterId, keys.privateKey, net.infos)).isOkOr:
      raiseAssert "mountMix: " & $error
    (await waku.start()).expect("start")
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
    waku: Waku, level: AnonymityLevel, chain: BaseSendProcessor
): SendService =
  let manager =
    RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
  let service = SendService
    .new(false, waku, manager, chain, level, retryInterval = chronos.milliseconds(200))
    .expect("SendService.new")
  service.maxDeliveryTime = Window
  service.startSendService()
  return service

proc mixChain(
    waku: Waku, level: AnonymityLevel, mixWindow: Duration
): BaseSendProcessor =
  ## The chain of the level, with a short resend interval and `mixWindow`.
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

proc listen(waku: Waku): SendEvents =
  let events = SendEvents()
  discard MessagePropagatedEvent.listen(
    waku.brokerCtx,
    proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
      events.propagated.inc(),
  )
  discard MessageUnconfirmedEvent.listen(
    waku.brokerCtx,
    proc(event: MessageUnconfirmedEvent) {.async: (raises: []).} =
      events.unconfirmed.inc(),
  )
  discard MessageErrorEvent.listen(
    waku.brokerCtx,
    proc(event: MessageErrorEvent) {.async: (raises: []).} =
      events.errors.inc(),
  )
  return events

suite "Waku Mix - a send whose answers are lost":
  asyncTest "a send whose answers are all lost ends unconfirmed after its resends":
    let hook = ExitHook.new(answersToDrop = high(int))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOf()
    defer:
      discard await waku.stop()
    let events = waku.listen()
    let service = waku.newService(
      AnonymityLevel.Required, waku.mixChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("all-lost")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      events.propagated == 0
      hook.requests == 1 + MaxMixResends
      task.mixResends == MaxMixResends

  asyncTest "a resend after a lost answer gets the refusal of the exit and confirms the send":
    ## The exit published the first copy and its answer was lost. Its relay has
    ## the message, so it refuses the resend, and the refusal says that the
    ## network has the message.
    let hook = ExitHook.new(answersToDrop = 1)
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOf()
    defer:
      discard await waku.stop()
    let events = waku.listen()
    let service = waku.newService(
      AnonymityLevel.Required, waku.mixChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("resend-confirms")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window, chronos.milliseconds(200)):
      events.propagated == 1
    check:
      task.state == DeliveryState.SuccessfullyPropagated
      hook.requests == 2
      events.unconfirmed == 0
      events.errors == 0

  asyncTest "a refused resend does not fail a send whose first answer was lost":
    ## The first request can have delivered the message, so a refusal of a
    ## resend says nothing about it.
    let hook = ExitHook.new(answersToDrop = 1, refusal = Opt.some("refused"))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOf()
    defer:
      discard await waku.stop()
    let events = waku.listen()
    let service = waku.newService(
      AnonymityLevel.Required, waku.mixChain(AnonymityLevel.Required, DeliveryWindow)
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

  asyncTest "an RLN refusal of a resend that the sender cannot answer with a new proof does not fail the send":
    ## The sender has no RLN backend to refresh its proof, so on master the
    ## refusal fails the task.
    let hook = ExitHook.new(answersToDrop = 1, refusal = Opt.some(RlnValidatorErrorMsg))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOf()
    defer:
      discard await waku.stop()
    let events = waku.listen()
    let service = waku.newService(
      AnonymityLevel.Required, waku.mixChain(AnonymityLevel.Required, DeliveryWindow)
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("rln-refused")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      events.errors == 0
      hook.requests == 1 + MaxMixResends

  asyncTest "a Preferred send whose answers are lost stays on mix past its mix window and a restart":
    ## The plain path would publish from the address of this node a message that
    ## the network can have from the mix exit.
    let hook = ExitHook.new(answersToDrop = high(int))
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOf()
    defer:
      discard await waku.stop()
    let events = waku.listen()
    let service = waku.newService(
      AnonymityLevel.Preferred,
      waku.mixChain(AnonymityLevel.Preferred, chronos.seconds(1)),
    )
    defer:
      await service.stopSendService()
    let task = lostAnswerTask("preferred-lost")

    check service.enqueue(task).isOk()
    # The stop comes while the first request waits for its answer.
    checkUntilTimeoutCustom(Window, chronos.milliseconds(50)):
      hook.requests == 1
    check not task.mixUnanswered
    await service.stopSendService()
    check task.mixUnanswered
    service.startSendService()

    checkUntilTimeoutCustom(Window + chronos.seconds(15), chronos.milliseconds(200)):
      events.unconfirmed == 1
    check:
      not task.deanonymized
      events.errors == 0
      hook.requests == 1 + MaxMixResends
