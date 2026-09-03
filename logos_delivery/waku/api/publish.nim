## Waku layer API — message publish primitives used by the messaging send
## pipeline.
##
## Unlike `relay.nim`/`lightpush.nim`, these preserve the rich
## `WakuLightPushResult` (status code + description) that the send processors
## branch on for their retry decisions, and expose relay/lightpush availability
## so the messaging layer never inspects `waku.node` directly.
{.push raises: [].}

import std/[tables, times, strutils]
import results, chronos, libp2p/crypto/rng, libp2p_mix/[pool, mix_metrics]

import logos_delivery/waku/waku
import
  logos_delivery/waku/[
    waku_core,
    node/waku_node,
    node/waku_node/lightpush,
    node/peer_manager,
    waku_relay/protocol,
    rln,
    waku_lightpush/common,
    waku_lightpush/rpc,
    waku_lightpush/client,
    waku_lightpush/callbacks,
    waku_mix,
  ]

# WakuLightPushResult, PushMessageHandler, LightPushErrorCode (common) plus the
# LightPushStatusCode `$`/`==` the send processors branch on (rpc).
export common, rpc

proc hasRelay*(self: Waku): bool =
  ## True if relay (gossipsub publishing) is mounted.
  return not self.node.wakuRelay.isNil()

proc hasLightpush*(self: Waku): bool =
  ## True if a lightpush client is mounted.
  return not self.node.wakuLightpushClient.isNil()

proc relayPushHandler*(self: Waku): PushMessageHandler =
  ## Builds the relay publish handler used by the send pipeline. Caller
  ## ensures relay is mounted. The handler validates and republishes; the
  ## proof is attached by the messaging layer via `attachRlnProof`.
  return getRelayPushHandler(self.node.wakuRelay)

proc currentRlnEpochQuota*(self: Waku): Opt[tuple[epochIndex, messageLimit: uint64]] =
  ## RLN's current epoch index and user message limit, read together so the
  ## pair cannot straddle an epoch boundary.
  if self.node.rln.isNil():
    return Opt.none(tuple[epochIndex, messageLimit: uint64])

  let limit = self.node.rln.groupManager.userMessageLimit.valueOr:
    return Opt.none(tuple[epochIndex, messageLimit: uint64])

  return Opt.some((fromEpoch(self.node.rln.getCurrentEpoch()), uint64(limit)))

proc attachRlnProof*(
    self: Waku, message: WakuMessage
): Future[Result[WakuMessage, string]] {.async.} =
  ## Returns `message` carrying an RLN proof. A message that already has one is
  ## returned untouched, so retrying a task neither redraws a nonce nor changes
  ## the bytes. Without RLN mounted the message passes through unproven.
  ##
  ## Uses the root-refreshing generator: a message can wait in the send
  ## service's task cache while the group root moves on chain, so the proof is
  ## validated against the acceptable-root window and regenerated once against a
  ## refetched merkle path if it went stale.
  if self.node.rln.isNil() or message.proof.len > 0:
    return ok(message)

  var msgWithProof = message
  msgWithProof.proof = (
    await self.node.rln.generateRLNProofWithRootRefresh(
      message.toRLNSignal(), float64(getTime().toUnix())
    )
  ).valueOr:
    return err("failed to attach RLN proof: " & error)

  return ok(msgWithProof)

func isRlnRejection*(error: ErrorStatus): bool =
  ## True when a publish failure means "the RLN proof was not accepted", so the
  ## message is worth retrying with a freshly generated proof rather than being
  ## failed outright.
  ##
  ## OUT_OF_RLN_PROOF is always RLN. INVALID_MESSAGE also covers non-RLN
  ## rejections (an oversized message, say), so it additionally has to carry the
  ## validator's error marker — this is the same gate the kernel lightpush path
  ## applies before scheduling a refresh.
  return
    error.code == LightPushErrorCode.OUT_OF_RLN_PROOF or (
      error.code == LightPushErrorCode.INVALID_MESSAGE and
      error.desc.get("").contains(RlnValidatorErrorMsg)
    )

proc onRlnProofRejected*(self: Waku) =
  ## Called when a publish was rejected as RLN-invalid. Starts refetching the
  ## merkle path in the background, so the next proof generated for the message
  ## is built against a fresh one. Non-blocking: the send service's own loop is
  ## what retries, and it must not stall waiting on an RPC round trip.
  if self.node.rln.isNil():
    return

  self.node.rln.groupManager.scheduleMerkleProofRefresh()

proc lightpushPeerAvailable*(self: Waku, shard: PubsubTopic): bool =
  ## True if a lightpush service peer is available for `shard`.
  return self.node.peerManager.selectPeer(WakuLightPushCodec, Opt.some(shard)).isSome()

proc selectMixLightpushPeer*(
    self: Waku, shard: PubsubTopic, avoid = Opt.none(PeerId)
): Opt[RemotePeerInfo] =
  ## Picks a lightpush service peer for `shard` that mix can route to. With
  ## `exit_is_dest` the lightpush server terminates the sphinx path, so it has
  ## to be one mix can build a `MixPubInfo` for: a peer carrying a mix key but
  ## no mix-routable address passes mix's own destination gate and only fails
  ## deep inside path construction, which evicts it from the pool on the way out.
  ##
  ## Walks the mix pool rather than the lightpush peers. Both orders answer the
  ## same question, but `selectPeers` reaches it through `peerStore.peers`, which
  ## materialises a full `RemotePeerInfo` - addresses, protocols, shards, raw ENR
  ## - for every peer in the store before filtering. The pool is a handful of
  ## entries and the filters here are direct book reads, so the work scales with
  ## the mix pool instead of the peer store. Shuffled for the same reason
  ## `selectPeers` shuffles: without it every message leaves by the same exit.
  ##
  ## Service slot first, following `selectPeer`. A statically configured
  ## `--lightpushnode` lives in the slot and carries no protocols and no shards
  ## until identify and waku-metadata have filled those books, so the two filters
  ## below would drop it on exactly the setup a mix deployment uses.
  ## `avoid` is the exit node of a failed attempt. The selection skips it when
  ## a different exit node exists, and returns it when it is the only one: an
  ## exit node that lost one reply is not an exit node that is gone.
  let peerStore = self.node.peerManager.switch.peerStore
  let pool = MixNodePool.new(peerStore)
  var first = Opt.none(PeerId)

  proc consider(peerId: PeerId): bool =
    ## Records the first usable exit node. Returns true when `peerId` is
    ## usable and not the one to avoid.
    if first.isNone():
      first = Opt.some(peerId)
    return avoid.isNone() or avoid.get() != peerId

  let slotted = self.node.peerManager.serviceSlots.getOrDefault(WakuLightPushCodec)
  if not slotted.isNil() and pool.get(slotted.peerId).isSome():
    if consider(slotted.peerId):
      return Opt.some(peerStore.getPeer(slotted.peerId))

  let shardInfo = RelayShard.parse(shard).valueOr:
    return Opt.none(RemotePeerInfo)

  var mixPeers = pool.peerIds()
  # The node's RNG, not the unseeded one of `std/random`: the exit order
  # must not repeat across process starts.
  self.node.rng.shuffle(mixPeers)
  for peerId in mixPeers:
    if not peerStore[ProtoBook][peerId].contains(WakuLightPushCodec):
      continue
    if not peerStore.hasShard(peerId, shardInfo.clusterId, shardInfo.shardId):
      continue
    if pool.get(peerId).isSome() and consider(peerId):
      return Opt.some(peerStore.getPeer(peerId))

  if first.isSome():
    return Opt.some(peerStore.getPeer(first.get()))
  return Opt.none(RemotePeerInfo)

const MixPoolSizeRequired* = MinMixPoolSize
  ## Number of mix nodes a node must know before it can send through mix.

proc mixMounted*(self: Waku): bool =
  ## True if the mix protocol is mounted on the node.
  return not self.node.wakuMix.isNil()

proc mixPoolSize*(self: Waku): int =
  ## Number of mix nodes the node knows. 0 when mix is not mounted. Each read
  ## sets the gauge `mix_pool_size`: the mix protocol sets it at mount only,
  ## and discovery and path construction change the pool after that.
  if self.node.wakuMix.isNil():
    return 0
  let size = self.node.getMixNodePoolSize()
  mix_pool_size.set(size.int64)
  return size

proc mixReady*(self: Waku): bool =
  ## True when mix can carry a publish: mounted, and with enough nodes for a
  ## path. The two checks are O(1) reads.
  ##
  ## Does not look for an exit node. `lightpushPublishViaMix` selects one
  ## itself, and answers SERVICE_NOT_AVAILABLE when it finds none, which the
  ## mix processor turns into the same `NextRoundRetry` this check would have
  ## produced. A check here would be a second scan per task per round.
  if self.node.wakuMix.isNil():
    return false
  return self.node.getMixNodePoolSize() >= MixPoolSizeRequired

const MixReplyTimeoutDesc* = lightpush.MixReplyTimeoutDesc
  ## Reader for the messaging layer, which does not import node internals.
const MixReplyUnreadableDesc* = lightpush.MixReplyUnreadableDesc

proc lightpushPublishViaMix*(
    self: Waku, shard: PubsubTopic, message: WakuMessage, avoid = Opt.none(PeerId)
): Future[tuple[res: WakuLightPushResult, exit: Opt[PeerId]]] {.async.} =
  ## Publishes `message` through mix to an exit node for `shard`, and returns
  ## the result with the peer id of the exit node. The caller keeps the exit
  ## node of a failed attempt and gives it as `avoid` for the next attempt.
  let peer = self.selectMixLightpushPeer(shard, avoid).valueOr:
    return (
      lightpushResultServiceUnavailable(
        "no mix-capable lightpush peer available for shard"
      ),
      Opt.none(PeerId),
    )
  try:
    let res = await self.node.lightpushPublish(
      Opt.some(shard), message, Opt.some(peer), mixify = true
    )
    return (res, Opt.some(peer.peerId))
  except CancelledError as exc:
    raise exc
  except CatchableError as e:
    return (lightpushResultInternalError(e.msg), Opt.some(peer.peerId))

proc lightpushPublishToAny*(
    self: Waku, shard: PubsubTopic, message: WakuMessage
): Future[WakuLightPushResult] {.async.} =
  ## Selects a lightpush service peer for `shard` and publishes `message`
  ## through the node's lightpush flow. With RLN mounted the flow proves
  ## `message` only if it carries no proof, so an already-proven task reuses its
  ## nonce. Returns SERVICE_NOT_AVAILABLE when no peer is available.
  let peer = self.node.peerManager.selectPeer(WakuLightPushCodec, Opt.some(shard)).valueOr:
    return lightpushResultServiceUnavailable("no lightpush peer available for shard")
  try:
    return await self.node.lightpushPublish(Opt.some(shard), message, Opt.some(peer))
  except CancelledError as exc:
    # This is not a publish failure: the send service is in its stop sequence.
    # An error result here keeps the service loop alive, and the
    # `cancelAndWait` of the stop does not complete.
    raise exc
  except CatchableError as e:
    return lightpushResultInternalError(e.msg)
