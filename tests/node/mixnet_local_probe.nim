## mixnet_local_probe.nim -- proves (or refutes) master's mix path end-to-end
## with every external variable controlled:
##   - all nodes run THIS build (no fleet version skew)
##   - all nodes bind 127.0.0.1 with fixed ports (no NAT, SURB last hop dialable)
##
## Topology: 4 core nodes (relay + lightpush service + mix) + 1 sender
## (relay + lightpush client + mix). The sender publishes over mix with a core
## node as the exit/destination (exit_is_dest).
##
## AXIOM-FORWARD: the message appears on relay (exit really published it).
## AXIOM-REPLY:   the lightpush-over-mix call returns Ok (SURB reply arrived).
##
## Exit code 0 iff both hold at least once within the attempt budget.

import std/[os, net, strutils, sequtils]
import chronos, results
import stew/byteutils
import libp2p/crypto/crypto, libp2p/peerid, libp2p/multiaddress
import libp2p_mix/curve25519

import
  logos_delivery/waku/
    [waku_core, node/peer_manager, waku_node, waku_mix, waku_lightpush],
  ../testlib/[wakucore, wakunode]

const
  BasePort = 61300
  NumCore = 4

var forwardSeen = 0
var replyOk = 0 # module-level: the isMainModule exit code reads it

proc ts(): string =
  $Moment.now()

proc main() {.async.} =
  let runId = "MIXNET-LOCAL-" & $Moment.now()
  let shard = DefaultPubsubTopic
  echo ts(), " local mixnet probe, shard=", shard

  # --- build the five nodes on fixed local ports ------------------------------
  var nodes: seq[WakuNode] = @[]
  var mixPrivs: seq[FieldElement] = @[]
  var pubInfos: seq[MixNodePubInfo] = @[]

  # natsim mode: the sender announces an address nobody listens on, which is
  # what a NATed node effectively does. buildSurb embeds the sender's own
  # announced multiaddr as the reply's last hop, so the prediction is
  # FORWARD-ONLY: delivery works, every reply dies.
  let natsim = paramCount() >= 1 and paramStr(1) in ["natsim", "isolated"]
  # isolated: additionally, the sender pre-connects to nobody. The only
  # connections to the sender are the ones its own sphinx entry dials create,
  # so a reply succeeds only when the delivering SURB hop happens to hold one
  # -- the dynamic proof that reply delivery rides connection reuse.
  let isolated = paramCount() >= 1 and paramStr(1) == "isolated"

  for i in 0 .. NumCore: # 0..3 = core, 4 = sender
    let port = BasePort + i
    let n =
      if natsim and i == NumCore:
        newTestWakuNode(
          generateSecp256k1Key(),
          parseIpAddress("127.0.0.1"),
          Port(port),
          extMultiAddrs = @[MultiAddress.init("/ip4/127.0.0.1/tcp/61999").tryGet()],
          extMultiAddrsOnly = true,
        )
      else:
        newTestWakuNode(generateSecp256k1Key(), parseIpAddress("127.0.0.1"), Port(port))
    let kp = generateKeyPair().valueOr:
      echo "FATAL mix keypair: ", error
      quit(QuitFailure)
    nodes.add(n)
    mixPrivs.add(kp.privateKey)
    pubInfos.add(
      MixNodePubInfo(
        multiAddr: "/ip4/127.0.0.1/tcp/" & $port & "/p2p/" & $n.peerInfo.peerId,
        pubKey: kp.publicKey,
      )
    )

  let sender = nodes[NumCore]

  # --- mount protocols (mix before start, as the factory does) ----------------
  for i in 0 ..< NumCore:
    let others = (0 .. NumCore).toSeq().filterIt(it != i).mapIt(pubInfos[it])
    (await nodes[i].mountRelay()).isOkOr:
      echo "FATAL mountRelay core ", i, ": ", error
      quit(QuitFailure)
    (await nodes[i].mountLightpush()).isOkOr:
      echo "FATAL mountLightpush core ", i, ": ", error
      quit(QuitFailure)
    (await nodes[i].mountMix(DefaultClusterId, mixPrivs[i], others)).isOkOr:
      echo "FATAL mountMix core ", i, ": ", error
      quit(QuitFailure)

  (await sender.mountRelay()).isOkOr:
    echo "FATAL mountRelay sender: ", error
    quit(QuitFailure)
  sender.mountLightpushClient()
  block:
    let cores = (0 ..< NumCore).toSeq().mapIt(pubInfos[it])
    (await sender.mountMix(DefaultClusterId, mixPrivs[NumCore], cores)).isOkOr:
      echo "FATAL mountMix sender: ", error
      quit(QuitFailure)

  # --- start + interconnect ---------------------------------------------------
  for n in nodes:
    await n.start()
  echo ts(), " all nodes started; sender pool=", sender.getMixNodePoolSize()

  let last =
    if isolated:
      NumCore - 1
    else:
      NumCore
  for i in 0 .. last:
    let peers =
      (0 .. last).toSeq().filterIt(it != i).mapIt(nodes[it].peerInfo.toRemotePeerInfo())
    await nodes[i].connectToNodes(peers)

  # --- relay subscriptions ----------------------------------------------------
  proc handler(topic: PubsubTopic, msg: WakuMessage): Future[void] {.async.} =
    let p = string.fromBytes(msg.payload)
    if p.startsWith(runId):
      forwardSeen.inc()
      echo ts(), " AXIOM-FORWARD: message on relay at sender, payload=", p

  # Cores observe the forward axiom too: in isolated mode the sender is not
  # in the relay mesh, so arrival at any core is the proof the exit published.
  for i in 0 ..< NumCore:
    nodes[i].subscribe((kind: PubsubSub, topic: shard), handler).isOkOr:
      echo "FATAL subscribe core ", i, ": ", error
      quit(QuitFailure)
  sender.subscribe((kind: PubsubSub, topic: shard), handler).isOkOr:
    echo "FATAL subscribe sender: ", error
    quit(QuitFailure)

  await sleepAsync(chronos.seconds(3)) # let gossipsub mesh form

  # --- publish over mix, attempt budget ---------------------------------------
  var attempts = 0
  for attempt in 1 .. 6:
    attempts = attempt
    let msg = WakuMessage(
      payload: toBytes(runId & " attempt " & $attempt),
      contentTopic: "/mixnet/1/probe/proto",
      version: 2,
      timestamp: getNowInNanosecondTime(),
    )
    let dest = nodes[attempt mod NumCore].peerInfo.toRemotePeerInfo()
    echo ts(), " attempt ", attempt, " via exit ", dest.peerId
    let res =
      await sender.lightpushPublish(Opt.some(shard), msg, Opt.some(dest), mixify = true)
    if res.isOk():
      replyOk.inc()
      echo ts(), " AXIOM-REPLY: lightpush-over-mix Ok, relayedTo=", res.get()
    else:
      echo ts(),
        " attempt ", attempt, " error: ", res.error.code, " ", res.error.desc.get("")
    # isolated mode runs the full budget: the per-attempt pattern is the data.
    if not isolated and replyOk > 0 and forwardSeen > 0:
      break
    await sleepAsync(chronos.seconds(2))

  await sleepAsync(chronos.seconds(5)) # grace for late relay arrival

  echo ts(), " ---- summary ----"
  echo "attempts:      ", attempts
  echo "reply ok:      ", replyOk
  echo "forward seen:  ", forwardSeen
  if replyOk > 0 and forwardSeen > 0:
    echo "MIXNET-LOCAL VERDICT: OK (forward + reply both proven)"
  elif forwardSeen > 0:
    echo "MIXNET-LOCAL VERDICT: FORWARD-ONLY (delivery works, replies broken)"
  elif replyOk > 0:
    echo "MIXNET-LOCAL VERDICT: REPLY-ONLY (odd: ack without relay arrival)"
  else:
    echo "MIXNET-LOCAL VERDICT: FAIL (mix transport broken in this build)"

  for n in nodes:
    await n.stop()

when isMainModule:
  waitFor main()
  # match the printed verdict: OK requires BOTH forward delivery and a reply
  quit(if replyOk > 0 and forwardSeen > 0: QuitSuccess else: QuitFailure)
