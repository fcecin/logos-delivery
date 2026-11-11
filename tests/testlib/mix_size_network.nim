{.used.}

import std/[net, sequtils]
import chronos, results, testutils/unittests
import libp2p/[crypto/crypto, peerinfo, switch]
import libp2p_mix/curve25519
import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  logos_delivery/waku/common/rate_limit/setting,
  ./[wakucore, wakunode, testasync]

type
  MixSizeNetwork* = object
    nodes*: seq[WakuNode]
    infos*: seq[MixNodePubInfo]

  ExitHook* = ref object
    ## Makes the first lightpush request at the exit wait forever.
    ## The exit then neither publishes nor answers that request.
    ## With `answersToDrop`, the exit publishes that many requests with no answer.
    ## A NAT that drops the reply does the same.
    enabled: bool ## True until the first request, when there is no `answersToDrop`.
    answersToDrop: int
    refusal: Opt[string]
      ## After the dropped answers, the exit refuses each request with this text.
    refusalCode: LightPushStatusCode ## The code of that refusal.
    requests*: int ## The lightpush requests that the exit received.
    requested*: AsyncEvent
    arrivals*: seq[WakuMessageHash]
      ## The hashes of the messages that the exit received over relay.

proc new*(
    T: type ExitHook,
    answersToDrop = 0,
    refusal = Opt.none(string),
    refusalCode = LightPushErrorCode.INVALID_MESSAGE,
): ExitHook =
  ExitHook(
    enabled: answersToDrop == 0,
    answersToDrop: answersToDrop,
    refusal: refusal,
    refusalCode: refusalCode,
    requested: newAsyncEvent(),
  )

proc mountHookedLightpush(node: WakuNode, hook: ExitHook) =
  ## Lightpush over relay, whose first request waits for the test.
  let relayHandler = getRelayPushHandler(node.wakuRelay)
  let handler: PushMessageHandler = proc(
      pubsubTopic: PubsubTopic, message: WakuMessage
  ): Future[WakuLightPushResult] {.async.} =
    hook.requests.inc()
    if hook.answersToDrop > 0:
      hook.answersToDrop.dec()
      let published = await relayHandler(pubsubTopic, message)
      # The answer never leaves the exit.
      await newFuture[void]("an answer that the exit never sends")
      return published
    if hook.refusal.isSome():
      return lighpushErrorResult(hook.refusalCode, hook.refusal.get())
    if not hook.enabled:
      return await relayHandler(pubsubTopic, message)
    hook.enabled = false
    hook.requested.fire()
    await newFuture[void]("a request that the exit never serves")
    return await relayHandler(pubsubTopic, message)
  node.wakuLightPush = WakuLightPush.new(
    node.peerManager,
    node.rng,
    handler,
    node.wakuAutoSharding,
    Opt.none(RateLimitSetting),
    node.wakuRelay.maxEncodedMessageSize,
  )
  node.switch.mount(node.wakuLightPush, protocolMatcher(WakuLightPushCodec))

proc exit*(net: MixSizeNetwork): WakuNode =
  net.nodes[0]

proc meshFormed(net: MixSizeNetwork): bool =
  for node in net.nodes:
    for other in net.nodes:
      if node != other and
          not node.hasMeshPeer(DefaultPubsubTopic, other.peerInfo.peerId):
        return false
  return true

proc startMixNodes*(hook: ExitHook = nil): Future[MixSizeNetwork] {.async.} =
  ## Starts four connected mix nodes on ephemeral loopback ports.
  ## `hook` changes the lightpush answers of the exit.
  var net: MixSizeNetwork
  for i in 0 ..< 4:
    let node = newTestWakuNode(
      generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0), quicEnabled = false
    )
    (await node.mountRelay()).expect("mount relay")
    if i == 0 and not hook.isNil():
      node.mountHookedLightpush(hook)
    else:
      (await node.mountLightpush()).expect("mount lightpush")
    node.mountMetadata(uint32(DefaultClusterId), @[0'u16]).expect("mount metadata")
    let keys = generateKeyPair().expect("mix keys")
    (
      await node.mountMix(
        DefaultClusterId, keys.privateKey, @[], allowAllAddresses = true
      )
    ).expect("mount mix")
    await node.start()
    net.nodes.add(node)
    net.infos.add(
      MixNodePubInfo(
        multiAddr:
          "/ip4/127.0.0.1/tcp/" & $node.boundTcpPort() & "/p2p/" & $node.peerInfo.peerId,
        pubKey: keys.publicKey,
      )
    )
  # Defined before the loop, so that no handler captures the loop index.
  proc ignore(topic: PubsubTopic, message: WakuMessage) {.async.} =
    discard

  proc record(topic: PubsubTopic, message: WakuMessage) {.async.} =
    hook.arrivals.add(computeMessageHash(topic, message))

  for i, node in net.nodes:
    node.wakuMix.addBootNodes(
      (0 ..< net.nodes.len).toSeq().filterIt(it != i).mapIt(net.infos[it])
    )
    let receive = if i == 0 and not hook.isNil(): record else: ignore
    node.subscribe((kind: PubsubSub, topic: DefaultPubsubTopic), receive).expect(
      "subscribe"
    )
  for i in 0 ..< net.nodes.len:
    for j in i + 1 ..< net.nodes.len:
      await net.nodes[i].switch.connect(
        net.nodes[j].peerInfo.peerId, net.nodes[j].peerInfo.addrs
      )
  checkUntilTimeout:
    net.meshFormed()
  return net

proc stop*(net: MixSizeNetwork, senders: seq[WakuNode]) {.async.} =
  for node in senders:
    await node.stop()
  for node in net.nodes:
    await node.stop()
