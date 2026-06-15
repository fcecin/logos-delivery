{.push raises: [].}

import logos_delivery/waku/common/option_shims

import
  std/[options, sets],
  results,
  chronicles,
  chronos,
  bearssl/rand,
  eth/common/[addresses, keys]
import
  ../../node/peer_manager/peer_manager,
  ../../waku_core,
  ./rpc,
  ./rpc_codec
export rpc

logScope:
  topics = "waku rln-gifter"

type
  RegisterMemberHandler* = proc(
    identityCommitment: seq[byte], rateLimit: uint64
  ): Future[Result[MembershipAllocationSuccess, string]] {.async, gcsafe.}

  MembershipStatusHandler* = proc(
    configAccountId: string, identityCommitment: seq[byte]
  ): Future[Result[MembershipStatusResponse, string]] {.async, gcsafe.}

  EthAllowlistAuth* = ref object
    addresses*: HashSet[Address]
    consumed*: HashSet[Address]

  WakuRlnGifter* = ref object of LPProtocol
    rng*: ref rand.HmacDrbgContext
    peerManager*: PeerManager
    registerHandler*: RegisterMemberHandler
    statusHandler*: MembershipStatusHandler
    auth*: Option[EthAllowlistAuth]

  WakuRlnGifterStatus* = ref object of LPProtocol
    statusHandler*: MembershipStatusHandler

proc toHexLower(b: openArray[byte]): string =
  result = newStringOfCap(b.len * 2)
  const digits = "0123456789abcdef"
  for x in b:
    result.add(digits[int(x shr 4)])
    result.add(digits[int(x and 0x0f)])

proc eip191Message*(idCommitment: openArray[byte]): seq[byte] =
  ## The EIP-191 personal_sign envelope wraps the lowercase hex representation
  ## of the 32-byte identity commitment. Hex is used (rather than raw bytes)
  ## so the signed message is human-readable in wallets that surface it.
  let hex = toHexLower(idCommitment)
  let prefix = "\x19Ethereum Signed Message:\n" & $hex.len
  result = newSeqOfCap[byte](prefix.len + hex.len)
  for c in prefix:
    result.add(byte(c))
  for c in hex:
    result.add(byte(c))

proc verifyEip191*(
    idCommitment: openArray[byte], sigBytes: openArray[byte]
): Result[Address, string] =
  if sigBytes.len != 65:
    return err("signature must be 65 bytes, got " & $sigBytes.len)
  let sig = Signature.fromRaw(sigBytes).valueOr:
    return err("invalid signature encoding: " & $error)
  let pub = sig.recover(eip191Message(idCommitment)).valueOr:
    return err("signature recovery failed: " & $error)
  ok(pub.to(Address))

proc failureResponse(
    requestId: string, authSuccess: bool, message: string
): RlnGifterResponse =
  RlnGifterResponse(
    requestId: requestId,
    authSuccess: authSuccess,
    error: some(message),
    failure: some(MembershipAllocationFailure(errorMessage: message)),
  )

proc handleRequest(
    wg: WakuRlnGifter, peerId: PeerId, buffer: seq[byte]
): Future[RlnGifterResponse] {.async.} =
  let request = RlnGifterRequest.decode(buffer).valueOr:
    error "failed to decode RLN gifter request", error = $error
    return failureResponse("N/A", false, "decode error: " & $error)

  info "handling RLN gifter request",
    peerId = peerId,
    requestId = request.requestId,
    identityCommitment = toHexLower(request.identityCommitment)[0 .. min(15, request.identityCommitment.len * 2 - 1)] & "..."

  if request.identityCommitment.len != 32:
    return failureResponse(
      request.requestId, true, "identity_commitment must be 32 bytes"
    )

  var authorizedSigner: Option[Address]
  if wg.auth.isSome:
    let auth = wg.auth.get()
    let authType =
      block:
        var s = newStringOfCap(request.authenticationType.len)
        for b in request.authenticationType: s.add(char(b))
        s
    if authType != EthAllowlistAuthType:
      return failureResponse(
        request.requestId, false,
        "unsupported authentication_type: '" & authType & "'",
      )
    if request.authenticationPayload.len == 0:
      return failureResponse(
        request.requestId, false, "missing authentication_payload"
      )
    let signer = verifyEip191(
      request.identityCommitment, request.authenticationPayload
    ).valueOr:
      return failureResponse(
        request.requestId, false, "signature verification failed: " & error
      )
    if signer notin auth.addresses:
      return failureResponse(
        request.requestId, false, "address not allowlisted: " & signer.to0xHex()
      )
    if signer in auth.consumed:
      return failureResponse(
        request.requestId, false, "address already used: " & signer.to0xHex()
      )
    authorizedSigner = some(signer)

  let effectiveRateLimit = request.rateLimit.get(100'u64)
  let success = (await wg.registerHandler(request.identityCommitment, effectiveRateLimit)).valueOr:
    error "RLN gifter registration failed", error = error
    return RlnGifterResponse(
      requestId: request.requestId,
      authSuccess: true,
      failure: some(MembershipAllocationFailure(errorMessage: error)),
    )

  if authorizedSigner.isSome and wg.auth.isSome:
    wg.auth.get().consumed.incl(authorizedSigner.get())

  info "RLN gifter registration succeeded",
    leafIndex = success.leafIndex,
    requestId = request.requestId

  return RlnGifterResponse(
    requestId: request.requestId,
    authSuccess: true,
    success: some(success),
  )

proc initProtocolHandler(wg: WakuRlnGifter) =
  proc handler(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    var rpc: RlnGifterResponse
    # NOTE: Do NOT close the connection from the server side. The client closes
    # its side after reading the response. If the server closes first, the remote
    # FIN triggers yamux cleanup on the client side after createNode returns,
    # causing a use-after-free crash in the delivery module process.

    var buffer: seq[byte]
    try:
      buffer = await conn.readLp(DefaultMaxRpcSize)
    except LPStreamError:
      error "rln-gifter read stream failed", error = getCurrentExceptionMsg()
      return

    try:
      rpc = await wg.handleRequest(conn.peerId, buffer)
    except CatchableError:
      error "rln-gifter handleRequest failed", error = getCurrentExceptionMsg()
      rpc = failureResponse("N/A", true, "internal error")

    try:
      await conn.writeLp(rpc.encode().buffer)
    except LPStreamError:
      error "rln-gifter write stream failed", error = getCurrentExceptionMsg()

  wg.handler = handler
  wg.codec = WakuRlnGifterCodec

proc new*(
    T: type WakuRlnGifter,
    peerManager: PeerManager,
    rng: ref rand.HmacDrbgContext,
    registerHandler: RegisterMemberHandler,
    auth: Option[EthAllowlistAuth] = none(EthAllowlistAuth),
    statusHandler: MembershipStatusHandler = nil,
): T =
  let wg = WakuRlnGifter(
    rng: rng,
    peerManager: peerManager,
    registerHandler: registerHandler,
    statusHandler: statusHandler,
    auth: auth,
  )
  wg.initProtocolHandler()
  return wg

proc initStatusProtocolHandler(ws: WakuRlnGifterStatus) =
  proc handler(conn: Connection, proto: string) {.async: (raises: [CancelledError]).} =
    var buffer: seq[byte]
    try:
      buffer = await conn.readLp(DefaultMaxRpcSize)
    except LPStreamError:
      error "rln-gifter-status read stream failed",
        error = getCurrentExceptionMsg()
      return

    var resp = MembershipStatusResponse(registered: false)
    let req = MembershipStatusRequest.decode(buffer).valueOr:
      resp.errorMessage = some("decode error: " & $error)
      try:
        await conn.writeLp(resp.encode().buffer)
      except LPStreamError:
        discard
      return

    if ws.statusHandler.isNil:
      resp.errorMessage = some("status handler not wired")
    else:
      try:
        let r = await ws.statusHandler(req.configAccountId, req.identityCommitment)
        if r.isErr:
          resp.errorMessage = some(r.error)
        else:
          resp = r.get()
      except CatchableError:
        resp.errorMessage = some(
          "status handler raised: " & getCurrentExceptionMsg())

    try:
      await conn.writeLp(resp.encode().buffer)
    except LPStreamError:
      error "rln-gifter-status write stream failed",
        error = getCurrentExceptionMsg()

  ws.handler = handler
  ws.codec = WakuRlnGifterStatusCodec

proc new*(
    T: type WakuRlnGifterStatus,
    statusHandler: MembershipStatusHandler,
): T =
  let ws = WakuRlnGifterStatus(statusHandler: statusHandler)
  ws.initStatusProtocolHandler()
  return ws
