{.push raises: [].}

import logos_delivery/waku/common/option_shims

import std/options, results, chronicles, chronos, bearssl/rand
import libp2p/stream/connection
import
  ../../node/peer_manager,
  ../../waku_core,
  ../../utils/requests,
  ./rpc,
  ./rpc_codec

logScope:
  topics = "waku rln-gifter client"

type
  RlnGifterResult* = Result[MembershipAllocationSuccess, string]

  WakuRlnGifterClient* = ref object
    rng*: ref rand.HmacDrbgContext
    peerManager*: PeerManager

proc new*(
    T: type WakuRlnGifterClient, peerManager: PeerManager, rng: ref rand.HmacDrbgContext
): T =
  WakuRlnGifterClient(peerManager: peerManager, rng: rng)

proc requestMembership*(
    wc: WakuRlnGifterClient,
    identityCommitment: seq[byte],
    rateLimit: Option[uint64],
    peer: RemotePeerInfo,
    authenticationType: seq[byte] = @[],
    authenticationPayload: seq[byte] = @[],
): Future[RlnGifterResult] {.async.} =
  let request = RlnGifterRequest(
    requestId: generateRequestId(wc.rng),
    authenticationType: authenticationType,
    authenticationPayload: authenticationPayload,
    identityCommitment: identityCommitment,
    rateLimit: rateLimit,
  )

  info "requesting RLN membership from gifter",
    requestId = request.requestId,
    identityCommitmentLen = identityCommitment.len

  # Retry dial with backoff (gifter node may still be initializing)
  var connection: Connection
  var dialAttempts = 0
  while true:
    let connOpt = await wc.peerManager.dialPeer(peer, WakuRlnGifterCodec)
    if connOpt.isSome:
      connection = connOpt.get()
      break
    dialAttempts += 1
    if dialAttempts >= 5:
      return err("failed to dial gifter peer after " & $dialAttempts & " attempts")
    warn "gifter dial failed, retrying", attempt = dialAttempts
    await sleepAsync(seconds(5))

  try:
    await connection.writeLP(request.encode().buffer)
  except LPStreamError:
    return err("failed to write request: " & getCurrentExceptionMsg())

  var buffer: seq[byte]
  try:
    buffer = await connection.readLp(DefaultMaxRpcSize)
  except LPStreamError:
    return err("failed to read response: " & getCurrentExceptionMsg())

  # Do NOT close the connection here. Let it leak and be cleaned up by GC.
  # Calling closeWithEOF triggers yamux cleanup that crashes the delivery module
  # process when the FFI boundary returns to C++ before yamux completes.

  let response = RlnGifterResponse.decode(buffer).valueOr:
    return err("failed to decode response: " & $error)

  if response.requestId != request.requestId:
    return err("requestId mismatch")

  if not response.authSuccess:
    let desc = response.error.get(
      if response.failure.isSome: response.failure.get().errorMessage
      else: "authentication failed"
    )
    return err("authentication failed: " & desc)

  if response.failure.isSome:
    return err("registration failed: " & response.failure.get().errorMessage)

  let success = response.success.valueOr:
    return err("response missing success/failure result")

  info "RLN membership granted", leafIndex = success.leafIndex

  return ok(success)

proc queryMembershipStatus*(
    wc: WakuRlnGifterClient,
    peer: RemotePeerInfo,
    configAccountId: string,
    identityCommitment: seq[byte],
): Future[Result[MembershipStatusResponse, string]] {.async.} =
  ## Short-lived RPC: each call opens its own stream so pollers don't hold
  ## a stream open past the libp2p timeout.
  let connOpt = await wc.peerManager.dialPeer(peer, WakuRlnGifterStatusCodec)
  if connOpt.isNone:
    return err("failed to dial gifter status peer")
  let connection = connOpt.get()

  let req = MembershipStatusRequest(
    configAccountId: configAccountId,
    identityCommitment: identityCommitment,
  )
  try:
    await connection.writeLP(req.encode().buffer)
  except LPStreamError:
    return err("failed to write status request: " & getCurrentExceptionMsg())

  var buffer: seq[byte]
  try:
    buffer = await connection.readLp(DefaultMaxRpcSize)
  except LPStreamError:
    return err("failed to read status response: " & getCurrentExceptionMsg())

  let resp = MembershipStatusResponse.decode(buffer).valueOr:
    return err("failed to decode status response: " & $error)

  return ok(resp)

proc watchMembershipConfirmation*(
    wc: WakuRlnGifterClient,
    peer: RemotePeerInfo,
    configAccountId: string,
    identityCommitment: seq[byte],
    optimisticLeaf: uint64,
    label: string,
    onConfirmed: proc(authLeaf: uint64) {.gcsafe, raises: [].},
): Future[void] {.async.} =
  ## Poll the status codec until the membership PDA exists, then call
  ## `onConfirmed` with the authoritative leaf — whether or not it differs
  ## from `optimisticLeaf`. Returns on first confirmation or after the
  ## deadline elapses.
  const pollEveryMs = 30_000
  const deadlineMs = 1_800_000
  let deadline = Moment.now() + chronos.milliseconds(deadlineMs)
  while Moment.now() < deadline:
    try:
      await sleepAsync(chronos.milliseconds(pollEveryMs))
    except CancelledError:
      return
    let qr =
      try:
        await wc.queryMembershipStatus(peer, configAccountId, identityCommitment)
      except CancelledError:
        return
      except CatchableError as e:
        Result[MembershipStatusResponse, string].err(
          "queryMembershipStatus raised: " & e.msg)
    if qr.isErr: continue
    let resp = qr.get()
    if resp.errorMessage.isSome: continue
    if not resp.registered: continue
    if resp.leafIndex.isSome:
      let authLeaf = resp.leafIndex.get()
      if authLeaf != optimisticLeaf:
        info "membership leaf corrected from optimistic",
          label = label, optimistic = optimisticLeaf, authoritative = authLeaf
      else:
        info "membership confirmed on-chain",
          label = label, leafIndex = authLeaf
      onConfirmed(authLeaf)
      return
  warn "membership confirmation timed out",
    label = label, optimisticLeaf = optimisticLeaf
