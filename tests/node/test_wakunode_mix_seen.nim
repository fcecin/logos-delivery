{.used.}

## Sends that the network already has, with real relay, lightpush and mix nodes.

import chronos, testutils/unittests, results, metrics
import brokers/broker_context
import libp2p/[peerid, peerinfo, switch, protocols/pubsub/pubsub]
import libp2p_mix/[curve25519, mix_protocol, mix_metrics]
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_lightpush, waku_mix, waku_relay],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/subscription_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/[send_service, delivery_task]
import
  ../testlib/[futures, testasync, wakucore, wakunode, mix_size_network, wakunodeconf]

const ReplyTimeout = chronos.seconds(1)
  ## Shorter than `MixReplyTimeout`, so that a lost answer ends the wait soon.

proc mixSendsStarted(): float64 {.gcsafe.} =
  ## The number of sends that the Mix library got from the nodes of this process.
  ## The counter has no value before its first increment, and a read then raises.
  {.cast(gcsafe).}:
    try:
      mix_messages_recvd.value(["Entry"])
    except CatchableError:
      0.0

proc senderConf(): WakuConf =
  var nodeConf = defaultTestWakuNodeConf()
  nodeConf.clusterId = Opt.some(DefaultClusterId)
  return nodeConf.toWakuConf().valueOr:
    raiseAssert error

proc newService(
    waku: Waku, level: AnonymityLevel, retryInterval = chronos.hours(1)
): SendService =
  let manager =
    RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
  let chain = setupSendProcessorChain(waku, level).expect("chain")
  let service = SendService
    .new(false, waku, manager, chain, level, retryInterval = retryInterval)
    .expect("SendService.new")
  service.startSendService()
  return service

suite "Waku Send - a send that the network already has":
  proc senderOn(net: MixSizeNetwork, readsShard: bool): Future[Waku] {.async.} =
    ## A sender with the mix nodes of `net` in its pool and the exit as lightpush peer.
    ## With `readsShard` it reads the shard, as an app that receives the topic does.
    ## Without it, it leaves the shard of its mode, so no copy of its message comes.
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
    if readsShard:
      waku.node.subscriptionManager.subscribeShard(DefaultPubsubTopic).expect(
        "subscribe"
      )
    else:
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
      not readsShard or
        waku.node.hasMeshPeer(DefaultPubsubTopic, net.nodes[1].peerInfo.peerId)
    return waku

  proc edgeSender(net: MixSizeNetwork): Future[Waku] {.async.} =
    ## A sender with no relay and the exit as its lightpush peer, as an edge node.
    var nodeConf = defaultTestWakuNodeConf(LogosDeliveryMode.Edge)
    nodeConf.clusterId = Opt.some(DefaultClusterId)
    var waku: Waku
    lockNewGlobalBrokerContext:
      waku = (await Waku.new(nodeConf.toWakuConf().expect("conf"))).expect("Waku.new")
      (await waku.start()).expect("start")
    await waku.node.switch.connect(net.exit.peerInfo.peerId, net.exit.peerInfo.addrs)
    waku.node.peerManager.addServicePeer(
      net.exit.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
    )
    checkUntilTimeout:
      waku.lightpushPeerAvailable(DefaultPubsubTopic)
    return waku

  proc probeTask(id: string): DeliveryTask =
    let msg = fakeWakuMessage(payload = id, contentTopic = "/mix-seen/1/probe/proto")
    DeliveryTask(
      requestId: RequestId(id),
      pubsubTopic: DefaultPubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(DefaultPubsubTopic, msg),
      state: DeliveryState.Entry,
      firstAdmittedTime: Opt.some(Moment.now()), # no budget and no proof
    )

  asyncTest "a copy from the network completes the send before the exit answers":
    ## A mix node publishes the message, and the copy reaches the sender on its shard.
    let hook = ExitHook.new()
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOn(readsShard = true)
    defer:
      discard await waku.stop()
    let service = newService(waku, AnonymityLevel.Required)
    defer:
      await service.stopSendService()
    let task = probeTask("copy-first")

    let started = Moment.now()
    check service.enqueue(task).isOk()
    check await hook.requested.wait().withTimeout(FUTURE_TIMEOUT_LONG)
    # `net.nodes[1]` publishes the message, as the exit of an earlier attempt does.
    # The sender receives its copy before the exit answers.
    check (await net.nodes[1].publish(Opt.some(DefaultPubsubTopic), task.msg)).isOk()
    checkUntilTimeout:
      task.networkHasMessage()
      task.msgHash in hook.arrivals
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      # Sooner than the attempt would give up on the answer.
      Moment.now() - started < MixReplyTimeout
      # The end of the attempt gave back the reply credentials of the cut publish.
      waku.node.wakuMix.surbCredsLen() == 0

  asyncTest "a mix attempt sends nothing when a copy came before it":
    ## A copy can come while the task waits for its admission.
    ## The attempt then starts no publish, so the Mix library gets no message.
    let hook = ExitHook.new()
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOn(readsShard = false)
    defer:
      discard await waku.stop()
    let service = newService(waku, AnonymityLevel.Required)
    defer:
      await service.stopSendService()
    let task = probeTask("copy-before")
    task.setNetworkHasMessage()
    let mixSendsBefore = mixSendsStarted()

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      mixSendsStarted() == mixSendsBefore
      hook.requests == 0
      waku.node.wakuMix.surbCredsLen() == 0

  asyncTest "a mix attempt whose answer is lost is confirmed by the refusal of the next attempt":
    ## The exit publishes the first copy, and its answer is lost.
    ## The relay of the exit then has the message, so it refuses the next attempt.
    ## The sender reads no topic, so no copy comes.
    let hook = ExitHook.new(answersToDrop = 1)
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOn(readsShard = false)
    defer:
      discard await waku.stop()
    waku.node.wakuMix.replyTimeout = ReplyTimeout
    let service = newService(
      waku, AnonymityLevel.Required, retryInterval = chronos.milliseconds(200)
    )
    defer:
      await service.stopSendService()
    let task = probeTask("refusal")

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      # A refusal is an answer, not a copy.
      not task.networkHasMessage()
      hook.requests == 2

  asyncTest "a relay send that this node already published propagates with no copy":
    ## An earlier publish of this node puts the message in the seen cache of its relay.
    ## An attempt that a stop cut does the same.
    ## The refusal of the next attempt must not end the send with an error.
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    let waku = await net.senderOn(readsShard = true)
    defer:
      discard await waku.stop()
    let service = newService(waku, AnonymityLevel.None)
    defer:
      await service.stopSendService()
    let task = probeTask("relay-repeat")
    check (await waku.node.publish(Opt.some(DefaultPubsubTopic), task.msg)).isOk()

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      # A send of this node is not a copy.
      not task.networkHasMessage()
      task.tryCount == 1

  asyncTest "a lightpush send whose service already has the message propagates with no copy":
    ## The relay of the service has the message after a request whose answer was lost.
    ## The refusal of the next request must not end the send with an error.
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    let waku = await net.edgeSender()
    defer:
      discard await waku.stop()
    let service = newService(waku, AnonymityLevel.None)
    defer:
      await service.stopSendService()
    let task = probeTask("lightpush-repeat")
    # The exit relays the message, as it does before an answer that gets lost.
    check (await net.exit.publish(Opt.some(DefaultPubsubTopic), task.msg)).isOk()

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      not task.networkHasMessage()
      task.tryCount == 1

  asyncTest "a lightpush send that the service refuses for another reason fails":
    ## Only the already-seen refusal means what a success answer means.
    ## Each other refusal of the message still ends the send with an error.
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    proc refuseAll(
        topic: PubsubTopic, msg: WakuMessage
    ): Future[pubsub.ValidationResult] {.async.} =
      return pubsub.ValidationResult.Reject

    net.exit.wakuRelay.addValidator(refuseAll, "Refused by the test")
    let waku = await net.edgeSender()
    defer:
      discard await waku.stop()
    let service = newService(waku, AnonymityLevel.None)
    defer:
      await service.stopSendService()
    let task = probeTask("lightpush-refused")

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.FailedToDeliver
    check task.errorDesc == "Refused by the test"
