{.used.}

## A copy of a sent message that comes back from the network completes the send.
## The relay, filter, lightpush and mix nodes are real.

import std/sequtils
import chronos, testutils/unittests, results, brokers/broker_context
import libp2p/[peerid, peerinfo, switch]
import libp2p_mix/[curve25519, mix_protocol]
import
  logos_delivery/waku/[waku, waku_core, waku_node, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/node/subscription_manager,
  logos_delivery/waku/node/waku_node/[filter, lightpush, relay],
  logos_delivery/waku/waku_archive,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/api/types,
  logos_delivery/api/events/kernel_events,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/[send_service, delivery_task]
import
  ../testlib/[futures, testasync, wakucore, wakunode, mix_size_network, wakunodeconf]

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
    .new(false, waku, manager, chain, retryInterval = retryInterval)
    .expect("SendService.new")
  service.startSendService()
  return service

proc newTask(id: string): DeliveryTask =
  let msg = fakeWakuMessage(payload = id, contentTopic = "/send-copy/1/probe/proto")
  return DeliveryTask(
    pubsubTopic: DefaultPubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(DefaultPubsubTopic, msg),
    state: DeliveryState.Entry,
  )

proc dropAnswers(exit: WakuNode) =
  ## The exit publishes each message and never answers.
  ## A NAT in front of the sender that drops the answer does the same.
  let publish = exit.wakuLightPush.pushHandler
  exit.wakuLightPush.pushHandler = proc(
      peerId: PeerId, pubsubTopic: PubsubTopic, message: WakuMessage
  ): Future[WakuLightPushResult] {.async.} =
    let published = await publish(peerId, pubsubTopic, message)
    await newFuture[void]("an answer that the exit never sends")
    return published

proc newSender(): Future[Waku] {.async.} =
  ## Its own broker context keeps the events of the mix nodes from its send service.
  var waku: Waku
  lockNewGlobalBrokerContext:
    waku = (await Waku.new(senderConf())).expect("Waku.new")
  return waku

proc connectTo(waku: Waku, net: MixSizeNetwork) {.async.} =
  ## Puts the mix nodes of `net` in the pool, with the exit as the lightpush peer.
  let keys = generateKeyPair().expect("mix key pair")
  (
    await waku.node.mountMix(
      DefaultClusterId, keys.privateKey, net.infos, allowAllAddresses = true
    )
  ).expect("mount mix")
  (await waku.start()).expect("start")
  for node in net.nodes:
    await waku.node.switch.connect(node.peerInfo.peerId, node.peerInfo.addrs)
  waku.node.peerManager.addServicePeer(
    net.exit.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
  )
  checkUntilTimeout:
    waku.mixReady()
    waku.selectMixLightpushPeer(DefaultPubsubTopic).isSome()

type HeldPutDriver = ref object of ArchiveDriver
  release: Future[void]

method put(
    driver: HeldPutDriver,
    messageHash: WakuMessageHash,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
): Future[ArchiveDriverResult[bool]] {.async.} =
  await driver.release
  return ok(true)

suite "Copies of sent messages":
  asyncTest "a copy completes a mix send whose answer is lost":
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    net.exit.dropAnswers()
    let waku = await newSender()
    defer:
      discard await waku.stop()
    await waku.connectTo(net)
    waku.node.subscriptionManager.subscribeShard(DefaultPubsubTopic).expect("subscribe")
    checkUntilTimeout:
      waku.node.hasMeshPeer(DefaultPubsubTopic, net.exit.peerInfo.peerId)
    let service = newService(waku, AnonymityLevel.Required)
    defer:
      await service.stopSendService()
    let task = newTask("lost-answer")

    let started = Moment.now()
    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.state == DeliveryState.SuccessfullyPropagated
    check:
      task.networkHasMessage()
      task.propagatedAnonymously
      # The attempt ended when the copy came, not when its time limit ended.
      Moment.now() - started < MixReplyTimeout
      waku.node.wakuMix.surbCredsLen() == 0

  asyncTest "a filter push completes a mix send whose answer is lost":
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    net.exit.dropAnswers()
    await net.exit.mountFilter()
    let waku = await newSender()
    defer:
      discard await waku.stop()
    await waku.connectTo(net)
    waku.node.subscriptionManager.unsubscribeShard(DefaultPubsubTopic).expect(
      "unsubscribe"
    )
    let task = newTask("filter-copy")
    let subscribed = await waku.node.filterSubscribe(
      Opt.some(DefaultPubsubTopic),
      task.msg.contentTopic,
      net.exit.peerInfo.toRemotePeerInfo(),
    )
    check subscribed.isOk()
    let service = newService(waku, AnonymityLevel.Required)
    defer:
      await service.stopSendService()

    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.networkHasMessage()
      task.state == DeliveryState.SuccessfullyPropagated

  asyncTest "a slow archive write does not delay a copy":
    ## A copy that comes after `MixWindow` does not keep the message from the clear.
    var receiver: WakuNode
    lockNewGlobalBrokerContext:
      receiver = newTestWakuNode(generateSecp256k1Key())
    let publisher = newTestWakuNode(generateSecp256k1Key())
    let nodes = @[receiver, publisher]
    for node in nodes:
      (await node.mountRelay()).expect("mount relay")
      await node.start()
    defer:
      await allFutures(nodes.mapIt(it.stop()))
    let archive = HeldPutDriver(release: newFuture[void]("archive write"))
    receiver.wakuArchive = WakuArchive.new(driver = archive).expect("archive")
    proc handle(topic: PubsubTopic, message: WakuMessage) {.async.} =
      discard

    for node in nodes:
      node.subscribe((kind: PubsubSub, topic: DefaultPubsubTopic), handle).expect(
        "subscribe"
      )
    await receiver.switch.connect(
      publisher.peerInfo.peerId, publisher.peerInfo.listenAddrs
    )
    checkUntilTimeout:
      publisher.hasMeshPeer(DefaultPubsubTopic, receiver.peerInfo.peerId)
    let seen = newFuture[void]("seen")
    let listener = MessageSeenEvent
      .listen(
        receiver.brokerCtx,
        proc(event: MessageSeenEvent) {.async: (raises: []).} =
          if not seen.finished():
            seen.complete()
        ,
      )
      .expect("listen")
    defer:
      await MessageSeenEvent.dropListener(receiver.brokerCtx, listener)

    let published =
      await publisher.publish(Opt.some(DefaultPubsubTopic), fakeWakuMessage())

    check:
      published.isOk()
      await seen.withTimeout(FUTURE_TIMEOUT_MEDIUM)
    archive.release.complete()

  asyncTest "a mix attempt whose answer is lost marks the task":
    ## The exit published the message, so a plain send of it would show the sender.
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    net.exit.dropAnswers()
    let waku = await newSender()
    defer:
      discard await waku.stop()
    await waku.connectTo(net)
    # No copy comes, so the attempt ends at `MixReplyTimeout`.
    waku.node.subscriptionManager.unsubscribeShard(DefaultPubsubTopic).expect(
      "unsubscribe"
    )
    let service = newService(waku, AnonymityLevel.Preferred)
    defer:
      await service.stopSendService()
    let task = newTask("lost-answer-no-copy")
    check service.enqueue(task).isOk()

    checkUntilTimeout:
      task.mixMayHavePublished
    check:
      task.tryCount == 1
      task.state == DeliveryState.NextRoundRetry

  asyncTest "a stop during a mix attempt ends its publish":
    ## A publish that continues keeps its reply credentials until its time limit.
    let net = await startMixNodes()
    defer:
      await net.stop(@[])
    net.exit.dropAnswers()
    let waku = await newSender()
    defer:
      discard await waku.stop()
    await waku.connectTo(net)
    # No copy comes, so the attempt waits for the answer.
    waku.node.subscriptionManager.unsubscribeShard(DefaultPubsubTopic).expect(
      "unsubscribe"
    )
    let service = newService(waku, AnonymityLevel.Required)
    let task = newTask("stop-in-attempt")
    check service.enqueue(task).isOk()
    checkUntilTimeout:
      waku.node.wakuMix.surbCredsLen() == 1

    await service.stopSendService()

    check:
      waku.node.wakuMix.surbCredsLen() == 0
      task.state == DeliveryState.NextRoundRetry
