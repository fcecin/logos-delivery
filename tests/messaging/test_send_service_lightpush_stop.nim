{.used.}

import chronos, testutils/unittests, results, stew/byteutils
import libp2p/crypto/crypto

import
  logos_delivery/waku/waku,
  logos_delivery/waku/waku_node,
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/waku_core,
  logos_delivery/waku/waku_lightpush/common,
  logos_delivery/api/types,
  logos_delivery/waku/factory/waku_conf,
  logos_delivery/messaging/rate_limit_manager/rate_limit_manager,
  logos_delivery/messaging/delivery_service/send_service/
    [send_service, lightpush_processor, delivery_task]
import ../testlib/[futures, testasync, wakunodeconf, wakucore, wakunode]
import ../waku_lightpush/lightpush_utils

## A stop of the send service cancels each send that runs, and waits for it. A
## lightpush send to a service peer that does not answer must end when the stop
## cancels it, and not wait for the peer to close the stream.

proc testConf(): WakuConf =
  defaultTestWakuNodeConf().toWakuConf().valueOr:
    raiseAssert error

suite "SendService - stop during a lightpush send":
  var waku {.threadvar.}: Waku

  asyncSetup:
    waku = (await Waku.new(testConf())).expect("Waku.new")
    (await waku.start()).isOkOr:
      raiseAssert "waku.start: " & error

  asyncTeardown:
    discard await waku.stop()

  asyncTest "stopping the service ends a lightpush send to a peer that does not answer":
    ## Stop the service while its send waits for a lightpush answer. The peer
    ## does not answer until the test allows it.
    let requested = newAsyncEvent()
    let allowAnswer = newAsyncEvent()
    let unresponsivePush = proc(
        pubsubTopic: PubsubTopic, message: WakuMessage
    ): Future[WakuLightPushResult] {.async.} =
      requested.fire()
      await allowAnswer.wait()
      return ok(1)
    # The server mounts metadata, so the peer manager accepts its cluster.
    let lightpushNode = newTestWakuNode(generateSecp256k1Key())
    lightpushNode.mountMetadata(TestClusterId, @[0'u16]).isOkOr:
      raiseAssert "mountMetadata: " & error
    discard await newTestWakuLightpushNode(lightpushNode.switch, unresponsivePush)
    await lightpushNode.start()
    defer:
      allowAnswer.fire()
      await lightpushNode.stop()
    waku.node.peerManager.addServicePeer(
      lightpushNode.peerInfo.toRemotePeerInfo(), WakuLightPushCodec
    )

    let manager =
      RateLimitManager.new(DefaultRateLimitConfig).expect("RateLimitManager.new")
    let processor = LightpushSendProcessor.new(waku, waku.brokerCtx)
    let service =
      SendService.new(false, waku, manager, processor).expect("SendService.new")
    let msg = WakuMessage(
      contentTopic: "/test/1/lightpush-stop/proto",
      payload: "lightpush-stop".toBytes(),
      timestamp: getNowInNanosecondTime(),
    )
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    let task = DeliveryTask(
      requestId: RequestId("lightpush-stop"),
      pubsubTopic: pubsubTopic,
      msg: msg,
      msgHash: computeMessageHash(pubsubTopic, msg),
      state: DeliveryState.Entry,
    )

    check service.enqueue(task).isOk()
    service.startSendService()
    check await requested.wait().withTimeout(FUTURE_TIMEOUT_MEDIUM)

    # Apply the time limit to `join()`, so that it cannot cancel the stop.
    let stopping = service.stopSendService()
    let stoppedInTime = await stopping.join().withTimeout(FUTURE_TIMEOUT)
    allowAnswer.fire() # lets a stop that waits for the peer end
    await stopping
    check:
      stoppedInTime
      not task.running
      task.state == DeliveryState.NextRoundRetry
