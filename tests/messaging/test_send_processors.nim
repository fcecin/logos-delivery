{.used.}

import chronos, testutils/unittests, results, stew/byteutils

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_node,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/waku_relay,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/api/publish,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/waku/waku_lightpush/callbacks,
  logos_delivery/messaging/delivery_service/send_service/
    [send_processor, relay_processor, lightpush_processor, delivery_task]
import ../testlib/[futures, testasync, wakucore, wakunode, wakunodeconf]
import ../waku_lightpush/lightpush_utils
import ../waku_relay/utils

## A relay rejects a message that it has as `DuplicateMessage`. A retry after an
## attempt with no response can get this result, from the relay of a service
## node or from the relay of this node. The plain send processors accept it as a
## propagation. `test_wakunode_mix_e2e` has the test of the mix processor.

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

proc newTask(id: string): DeliveryTask =
  ## A message from now. The id is the payload, so two tasks never have one hash.
  let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  let msg = WakuMessage(
    contentTopic: "/test/1/send-processors/proto",
    payload: id.toBytes(),
    timestamp: getNowInNanosecondTime(),
  )
  return DeliveryTask(
    pubsubTopic: pubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(pubsubTopic, msg),
    state: DeliveryState.Entry,
  )

proc duplicateAnswer(): WakuLightPushResult =
  ## The result that a relay gives for a message that it has.
  return mapPubishingErrorToPushResult(
    PublishOutcome.DuplicateMessage, int(DefaultMaxWakuMessageSize)
  )

proc sendAfterPeerPublish(
    waku: Waku, task: DeliveryTask
): Future[Result[seq[WakuLightPushResult], string]] {.async.} =
  ## Connects a relay of this node to a peer relay on the shard of `task`. The
  ## peer publishes the message of `task`, as a mix exit or a lightpush service
  ## does for a previous attempt, and the relay of this node receives it. Then
  ## `RelaySendProcessor` sends `task` through the relay of this node. Gives the
  ## publish results that the processor got. The processor uses `waku` only for
  ## an RLN rejection.
  let switch = newTestSwitch()
  let relay = await newTestWakuRelay(switch)
  let peerSwitch = newTestSwitch()
  let peerRelay = await newTestWakuRelay(peerSwitch)
  await allFutures(switch.start(), peerSwitch.start())
  defer:
    await allFutures(switch.stop(), peerSwitch.stop())
  if not await PeerManager.new(switch).connectPeer(
    peerSwitch.peerInfo.toRemotePeerInfo()
  ):
    return err("the relays did not connect")

  let received = newPushHandlerFuture()
  proc handler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if not received.finished():
      received.complete((topic, msg))

  relay.subscribe(task.pubsubTopic, handler)
  peerRelay.subscribe(task.pubsubTopic, noopRawHandler())
  let meshDeadline = Moment.now() + FUTURE_TIMEOUT_MEDIUM
  while not (
    relay.hasMeshPeer(task.pubsubTopic, peerSwitch.peerInfo.peerId) and
    peerRelay.hasMeshPeer(task.pubsubTopic, switch.peerInfo.peerId)
  )
  :
    if Moment.now() > meshDeadline:
      return err("the relays made no mesh")
    await sleepAsync(chronos.milliseconds(10))
  (await peerRelay.publish(task.pubsubTopic, task.msg)).isOkOr:
    return err("the peer did not publish: " & $error)
  if not await received.withTimeout(FUTURE_TIMEOUT):
    return err("the relay of this node did not receive the message")

  var answers: seq[WakuLightPushResult]
  let relayPublish = getRelayPushHandler(relay)
  proc recordedPublish(
      pubsubTopic: PubsubTopic, message: WakuMessage
  ): Future[WakuLightPushResult] {.async.} =
    let answer = await relayPublish(pubsubTopic, message)
    answers.add(answer)
    return answer

  let processor = RelaySendProcessor.new(false, recordedPublish, waku, waku.brokerCtx)
  if not await processor.process(task).withTimeout(FUTURE_TIMEOUT_MEDIUM):
    return err("the relay attempt did not end")
  return ok(answers)

suite "Send processors - a message that the relay has":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")

  asyncTeardown:
    discard await waku.stop()

  asyncTest "a lightpush attempt that the service rejects as a duplicate propagates the task":
    ## The push handler gives the result that the relay of a service node gives.
    ## It gives `DuplicateMessage` for each task, but `InvalidMessage` for the
    ## task "invalid", which has the same code and a different text.
    let lightpushNode = newTestWakuNode(generateSecp256k1Key())
    lightpushNode.mountMetadata(TestClusterId, @[0'u16]).isOkOr:
      raiseAssert "mountMetadata: " & error
    proc pushHandler(
        pubsubTopic: PubsubTopic, message: WakuMessage
    ): Future[WakuLightPushResult] {.async.} =
      let outcome =
        if string.fromBytes(message.payload) == "invalid":
          PublishOutcome.InvalidMessage
        else:
          PublishOutcome.DuplicateMessage
      return mapPubishingErrorToPushResult(outcome, int(DefaultMaxWakuMessageSize))

    discard await newTestWakuLightpushNode(lightpushNode.switch, pushHandler)
    await lightpushNode.start()
    defer:
      await lightpushNode.stop()
    waku.node.mountLightPushClient()
    waku.node.peerManager.addServicePeer(
      lightpushNode.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
    )
    let processor = LightpushSendProcessor.new(waku, waku.brokerCtx)

    let task = newTask("already-seen")
    check await processor.process(task).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      task.tryCount == 1
      task.state == DeliveryState.SuccessfullyPropagated
      task.firstPropagatedTime.isSome()

    # The request of an anonymized task also went to the service node without
    # mix. Thus its propagation stays plain.
    let anonymized = newTask("already-seen-anonymized")
    anonymized.anonymized = true
    check await processor.process(anonymized).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      anonymized.state == DeliveryState.SuccessfullyPropagated
      not anonymized.propagatedAnonymously

    let invalid = newTask("invalid")
    check await processor.process(invalid).withTimeout(FUTURE_TIMEOUT_MEDIUM)
    check:
      invalid.tryCount == 1
      invalid.state == DeliveryState.FailedToDeliver
      invalid.firstPropagatedTime.isNone()

  asyncTest "a relay attempt that the relay of this node rejects as a duplicate propagates the task":
    let task = newTask("from-a-peer")
    let answers = (await waku.sendAfterPeerPublish(task)).valueOr:
      raiseAssert error
    check:
      answers == @[duplicateAnswer()]
      task.tryCount == 1
      task.state == DeliveryState.SuccessfullyPropagated
      task.firstPropagatedTime.isSome()
      not task.propagatedAnonymously

  asyncTest "an anonymized task that the relay of this node rejects as a duplicate propagates anonymously":
    ## A mix attempt anonymized the task, and the relay of this node got its
    ## copy from the network. This node sent nothing. Thus a Store query must not
    ## name the message from the address of this node.
    let task = newTask("from-a-mix-exit")
    task.anonymized = true
    let answers = (await waku.sendAfterPeerPublish(task)).valueOr:
      raiseAssert error
    check:
      answers == @[duplicateAnswer()]
      task.tryCount == 1
      task.state == DeliveryState.SuccessfullyPropagated
      task.propagatedAnonymously
