{.used.}

## A mix send whose reply is lost, against the real mix library and relay.

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
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/[send_service, delivery_task]
import ../testlib/[testasync, wakucore, wakunode, mix_size_network, wakunodeconf]

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

suite "Waku Mix - a send whose reply is lost":
  proc senderOn(net: MixSizeNetwork, readsShard: bool): Future[Waku] {.async.} =
    ## A sender with the mix nodes of `net` in its pool and the exit as its
    ## lightpush peer. With `readsShard` it reads the shard, as an application
    ## that receives the topic does. Without it, it leaves the shard that its
    ## mode subscribes, so that no copy of its message reaches it.
    # The mix nodes also receive the message. Their events must not reach the
    # send service of the sender.
    var waku: Waku
    lockNewGlobalBrokerContext:
      waku = (await Waku.new(senderConf())).expect("Waku.new")
      let keys = generateKeyPair().expect("mix key pair")
      (await waku.node.mountMix(DefaultClusterId, keys.privateKey, net.infos)).isOkOr:
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

  proc lostReplyTask(id: string): DeliveryTask =
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
    let task = lostReplyTask("copy-first")

    let started = Moment.now()
    check service.enqueue(task).isOk()
    check await hook.requested.wait().withTimeout(chronos.seconds(10))
    # `net.nodes[1]` publishes the message, as the exit of an earlier attempt
    # does. The sender receives its copy while the exit holds the answer.
    check (await net.nodes[1].publish(Opt.some(DefaultPubsubTopic), task.msg)).isOk()
    checkUntilTimeout:
      task.seenOnNetwork
      task.msgHash in hook.arrivals
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      # Sooner than the attempt would give up on the answer.
      Moment.now() - started < MixReplyTimeout

  asyncTest "a mix attempt whose answer is lost is confirmed by the refusal of the next attempt":
    ## The exit publishes the first copy, and its answer is lost. The relay of the
    ## exit then has the message, so it refuses the copy of the next attempt. The
    ## sender reads no topic, so no copy comes.
    let hook = ExitHook.new(answersToDrop = 1)
    let net = await startMixNodes(hook)
    defer:
      await net.stop(@[])
    let waku = await net.senderOn(readsShard = false)
    defer:
      discard await waku.stop()
    let service = newService(
      waku, AnonymityLevel.Required, retryInterval = chronos.milliseconds(200)
    )
    defer:
      await service.stopSendService()
    let task = lostReplyTask("refusal")

    check service.enqueue(task).isOk()

    checkUntilTimeoutCustom(chronos.seconds(30), chronos.milliseconds(200)):
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      not task.seenOnNetwork
      hook.requests == 2
