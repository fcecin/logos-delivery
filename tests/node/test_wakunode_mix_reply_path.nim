{.used.}

## `exitConnection` connects a reply hop before a send that expects a reply,
## so the reply of a mixed send reaches a sender behind NAT.

import std/sequtils
import testutils/unittests, chronos, results, metrics, stew/byteutils
import libp2p/[crypto/crypto, crypto/curve25519, multiaddress, peerid, switch]
import libp2p/stream/connection
import libp2p_mix, libp2p_mix/[curve25519, mix_protocol]

import
  logos_delivery/waku/[waku_core, waku_node, waku_mix, waku_lightpush],
  logos_delivery/waku/node/peer_manager,
  ../testlib/[wakucore, wakunode]

const DeadAddress = "/ip4/127.0.0.1/tcp/1"
  ## A reserved port that refuses each connection at once.

const ReplyParams =
  MixParameters(expectReply: Opt.some(true), numSurbs: Opt.some(MixLightpushSurbs))

type MixPeer = object
  node: WakuNode
  pubKey: Curve25519Key

proc startMixNode(replyHopTimeout = MixReplyHopTimeout): Future[MixPeer] {.async.} =
  let node = newTestWakuNode(
    generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(0), quicEnabled = false
  )
  let mixKeys = generateKeyPair().expect("mix keys")
  (
    await node.mountMix(
      DefaultClusterId,
      mixKeys.privateKey,
      @[],
      addressPolicy = defaultAddressPolicy,
      replyHopTimeout = replyHopTimeout,
    )
  ).expect("mount mix")
  node.mountLightPushClient()
  await node.start()
  return MixPeer(node: node, pubKey: mixKeys.publicKey)

proc discover(sender: WakuNode, peer: MixPeer): PeerId =
  ## A record of a live mix node, as discovery writes it.
  sender.peerManager.addPeer(
    RemotePeerInfo.init(
      peer.node.peerInfo.peerId,
      @[peer.node.announcedAddresses[0]],
      mixPubKey = Opt.some(peer.pubKey),
    )
  )
  return peer.node.peerInfo.peerId

proc discoverDeadPeer(sender: WakuNode): PeerId =
  let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
  let hopKeys = generateKeyPair().expect("hop keys")
  sender.peerManager.addPeer(
    RemotePeerInfo.init(
      peerId,
      @[MultiAddress.init(DeadAddress).expect("address")],
      mixPubKey = Opt.some(hopKeys.publicKey),
    )
  )
  return peerId

proc stopAll(peers: seq[MixPeer]) {.async.} =
  for peer in peers:
    await peer.node.stop()

suite "Waku Mix - a connected reply hop before a send":
  asyncTest "a mounted mix ends its reply paths on a connected pool member":
    var peers: seq[MixPeer]
    for _ in 0 ..< 3:
      peers.add(await startMixNode())
    defer:
      await peers.stopAll()
    let sender = peers[0].node
    let connectedId = sender.discover(peers[1])
    let otherId = sender.discover(peers[2])
    await sender.switch.connect(connectedId, peers[1].node.announcedAddresses)
    check not sender.wakuMix.replyHopPicker.isNil
    for _ in 0 ..< 10:
      check sender.wakuMix.replyHopPicker(@[otherId, connectedId]).get() == connectedId

  asyncTest "a send that expects a reply connects two reply hops first":
    var peers: seq[MixPeer]
    for _ in 0 ..< 4:
      peers.add(await startMixNode())
    defer:
      await peers.stopAll()
    let sender = peers[0].node
    var memberIds: seq[PeerId]
    for peer in peers[1 ..^ 1]:
      memberIds.add(sender.discover(peer))
    let exitId = memberIds[0]
    check sender.switch.connectedPeers().len == 0

    let conn = (
      await sender.wakuMix.exitConnection(exitId, WakuLightPushCodec, ReplyParams)
    ).expect("mix connection")
    defer:
      await conn.close()

    let connected = sender.switch.connectedPeers()
    check:
      connected.len == MixReplyHopMaxDials
      exitId notin connected
      connected.allIt(it in memberIds)

  asyncTest "a send without a reply connects no reply hop":
    var peers: seq[MixPeer]
    for _ in 0 ..< 4:
      peers.add(await startMixNode())
    defer:
      await peers.stopAll()
    let sender = peers[0].node
    var memberIds: seq[PeerId]
    for peer in peers[1 ..^ 1]:
      memberIds.add(sender.discover(peer))

    let conn = (
      await sender.wakuMix.exitConnection(
        memberIds[0], WakuLightPushCodec, MixParameters(expectReply: Opt.some(false))
      )
    ).expect("mix connection")
    defer:
      await conn.close()

    check sender.switch.connectedPeers().len == 0

  asyncTest "a send fails at once when every reply hop candidate refuses the dial":
    let peer = await startMixNode()
    defer:
      await peer.node.stop()
    let sender = peer.node
    var deadIds: seq[PeerId]
    for _ in 0 ..< 5:
      deadIds.add(sender.discoverDeadPeer())
    let failures = logos_delivery_mix_reply_hop_failures.value()
    let started = Moment.now()

    let res =
      await sender.wakuMix.exitConnection(deadIds[0], WakuLightPushCodec, ReplyParams)

    check:
      res.isErr()
      Moment.now() - started < MixReplyHopTimeout
      logos_delivery_mix_reply_hop_failures.value() == failures + 1
      sender.switch.connectedPeers().len == 0

  asyncTest "reply hop candidates that refuse the dial leave mix paths":
    ## `connectReplyHop` reports its failed dials to `recordHopDial`. They count
    ## because this node has another connection.
    let peer = await startMixNode()
    let other = await startMixNode()
    defer:
      await peer.node.stop()
      await other.node.stop()
    let sender = peer.node
    await sender.connectToNodes(@[other.node.peerInfo.toRemotePeerInfo()])
    var deadIds: seq[PeerId]
    for _ in 0 ..< 5:
      deadIds.add(sender.discoverDeadPeer())
    let failures = logos_delivery_mix_dial_failures.value()

    let res =
      await sender.wakuMix.exitConnection(deadIds[0], WakuLightPushCodec, ReplyParams)

    check:
      res.isErr()
      deadIds.filterIt(sender.wakuMix.nodePool.get(it).isNone()).len ==
        MixReplyHopMaxDials
      logos_delivery_mix_dial_failures.value() == failures + MixReplyHopMaxDials.float64

  asyncTest "a send fails at the reply hop timeout when no candidate answers":
    ## The candidates accept the TCP connection and never answer the handshake.
    let peer = await startMixNode(replyHopTimeout = chronos.milliseconds(300))
    defer:
      await peer.node.stop()
    let sender = peer.node
    var held: seq[StreamTransport]
    proc mute(server: StreamServer, client: StreamTransport) {.async: (raises: []).} =
      held.add(client)

    let listener = createStreamServer(initTAddress("127.0.0.1:0"), mute, {ReuseAddr})
    listener.start()
    defer:
      for client in held:
        await client.closeWait()
      listener.stop()
      await listener.closeWait()
    let address = "/ip4/127.0.0.1/tcp/" & $listener.localAddress().port
    var memberIds: seq[PeerId]
    for _ in 0 ..< 5:
      let peerId = PeerId.init(generateSecp256k1Key()).expect("peer id")
      sender.peerManager.addPeer(
        RemotePeerInfo.init(
          peerId,
          @[MultiAddress.init(address).expect("address")],
          mixPubKey = Opt.some(generateKeyPair().expect("hop keys").publicKey),
        )
      )
      memberIds.add(peerId)
    let failures = logos_delivery_mix_reply_hop_failures.value()
    let started = Moment.now()

    let res =
      await sender.wakuMix.exitConnection(memberIds[0], WakuLightPushCodec, ReplyParams)

    let elapsed = Moment.now() - started
    check:
      res.isErr()
      elapsed >= chronos.milliseconds(300)
      elapsed < chronos.seconds(3)
      logos_delivery_mix_reply_hop_failures.value() == failures + 1

  asyncTest "a mixed lightpush send reports the service as not available when no reply hop connects":
    let peer = await startMixNode()
    defer:
      await peer.node.stop()
    let sender = peer.node
    var deadIds: seq[PeerId]
    for _ in 0 ..< 5:
      deadIds.add(sender.discoverDeadPeer())
    let msg = WakuMessage(
      payload: toBytes("no reply hop"),
      contentTopic: "/mix/1/reply-hop/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )

    let failures = logos_delivery_mix_reply_hop_failures.value()

    let res = await sender.lightpushPublish(
      Opt.some(DefaultPubsubTopic),
      msg,
      Opt.some(RemotePeerInfo.init(deadIds[0])),
      mixify = true,
    )

    check:
      res.isErr()
      res.error.code == LightPushErrorCode.SERVICE_NOT_AVAILABLE
      logos_delivery_mix_reply_hop_failures.value() == failures + 1
