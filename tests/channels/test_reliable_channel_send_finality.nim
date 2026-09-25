{.used.}

## Channel send finality through the real send service. The peer is
## relay-only, so no store node confirms a message.

import results
import chronos, testutils/unittests, stew/byteutils, libp2p/peerinfo
import brokers/broker_context

import ../testlib/[wakucore, wakunode, wakunodeconf, testasync]

import logos_delivery
import logos_delivery/waku/[waku_node, waku_core]
import logos_delivery/api/conf/logos_delivery_conf
import logos_delivery/channels/reliable_channel_manager

const
  FinalityTimeout = chronos.seconds(10)
  ShardTopic = PubsubTopic("/waku/2/rs/3/0")
  ChannelContentTopic = ContentTopic("/reliable-channel/1/send-finality/proto")

type Finality {.pure.} = enum
  Pending
  Sent
  Failed

proc newChannelNode(reliability: bool): Future[LogosDelivery] {.async.} =
  ## Builds a channels node with the given store-based send reliability.
  ## `LogosDelivery.new(WakuNodeConf)` always enables reliability.
  var kernelConf = defaultTestWakuNodeConf()
  applyMode(kernelConf, kernelConf.mode).expect("applyMode")
  return (
    await LogosDelivery.new(
      LogosDeliveryConf(
        kernelConf: KernelConf(kernelConf),
        messagingConf:
          Opt.some(MessagingClientConf(reliabilityEnabled: Opt.some(reliability))),
        channelsConf: Opt.some(ReliableChannelManagerConf()),
      )
    )
  ).expect("LogosDelivery.new")

proc sendOnChannel(
    peerInfo: RemotePeerInfo, channelId: ChannelId, reliability, ephemeral: bool
): Future[Finality] {.async.} =
  ## Sends one message on a new channel of a new node that connects to
  ## `peerInfo`. Returns the channel final event that occurs within
  ## `FinalityTimeout`, or `Pending`.
  var node: LogosDelivery
  lockNewGlobalBrokerContext:
    node = await newChannelNode(reliability)
    (await node.start()).expect("start")
    await node.waku.node.connectToNodes(@[peerInfo])
  defer:
    (await node.stop()).expect("stop")

  let brokerCtx = node.waku.brokerCtx
  let sent = newFuture[RequestId]("channel-sent")
  let failed = newFuture[RequestId]("channel-error")
  discard ChannelMessageSentEvent
    .listen(
      brokerCtx,
      proc(evt: ChannelMessageSentEvent) {.async: (raises: []).} =
        if evt.channelId == channelId and not sent.finished():
          sent.complete(evt.requestId)
      ,
    )
    .expect("listen ChannelMessageSentEvent")
  discard ChannelMessageErrorEvent
    .listen(
      brokerCtx,
      proc(evt: ChannelMessageErrorEvent) {.async: (raises: []).} =
        if evt.channelId == channelId and not failed.finished():
          failed.complete(evt.requestId)
      ,
    )
    .expect("listen ChannelMessageErrorEvent")
  defer:
    await ChannelMessageSentEvent.dropAllListeners(brokerCtx)
    await ChannelMessageErrorEvent.dropAllListeners(brokerCtx)

  let manager = node.reliableChannelManager
  discard manager
    .createReliableChannel(channelId, ChannelContentTopic, SdsParticipantID("sender"))
    .expect("createReliableChannel")
  let channelReqId =
    (await manager.send(channelId, "finality".toBytes(), ephemeral)).expect("send")

  let deadline = Moment.now() + FinalityTimeout
  while Moment.now() < deadline and not sent.finished() and not failed.finished():
    await sleepAsync(10.milliseconds)

  if failed.finished():
    check failed.read() == channelReqId
    return Finality.Failed
  if sent.finished():
    check sent.read() == channelReqId
    return Finality.Sent
  return Finality.Pending

suite "Reliable Channel - send finality over the send service":
  var
    relayPeer {.threadvar.}: WakuNode
    relayPeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    lockNewGlobalBrokerContext:
      relayPeer = newTestWakuNode(generateSecp256k1Key())
      relayPeer.mountMetadata(TestClusterId, @[0'u16]).isOkOr:
        raiseAssert "Failed to mount metadata: " & error
      (await relayPeer.mountRelay()).isOkOr:
        raiseAssert "Failed to mount relay"
      await relayPeer.mountLibp2pPing()
      await relayPeer.start()

    relayPeerInfo = relayPeer.peerInfo.toRemotePeerInfo()

    proc dummyHandler(
        topic: PubsubTopic, msg: WakuMessage
    ): Future[void] {.async, gcsafe.} =
      discard

    relayPeer.subscribe((kind: PubsubSub, topic: ShardTopic), dummyHandler).isOkOr:
      raiseAssert "Failed to subscribe relayPeer: " & error

  asyncTeardown:
    await relayPeer.stop()

  asyncTest "with reliability off, a channel send finalises as sent once it propagates":
    ## With reliability off, the send service emits no `MessageSentEvent`.
    let finality = await sendOnChannel(
      relayPeerInfo, ChannelId("finality-reliability-off"), false, false
    )
    check finality == Finality.Sent

  asyncTest "with reliability on, an ephemeral channel send finalises as sent once it propagates":
    ## The send service emits no `MessageSentEvent` for an ephemeral message.
    let finality =
      await sendOnChannel(relayPeerInfo, ChannelId("finality-ephemeral"), true, true)
    check finality == Finality.Sent

  asyncTest "with reliability on and no store node, a channel send finalises as sent once it propagates":
    ## No store node confirms the message. The send finalises as Sent when the
    ## message propagates.
    let finality =
      await sendOnChannel(relayPeerInfo, ChannelId("finality-no-store"), true, false)
    check finality == Finality.Sent
