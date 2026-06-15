{.push raises: [].}

import std/options
import ../../common/protobuf, ./rpc

const DefaultMaxRpcSize* = 4096

proc encode*(rpc: MembershipAllocationSuccess): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, rpc.leafIndex)
  pb.write3(2, rpc.merkleRoot)
  pb.write3(3, rpc.blockNumber)
  pb.write3(4, rpc.transactionHash)
  if rpc.configAccountId.isSome:
    pb.write3(100, rpc.configAccountId.get())
  pb.finish3()
  return pb

proc decode*(T: type MembershipAllocationSuccess, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var msg = MembershipAllocationSuccess()

  var leafIndex: uint64
  if ?pb.getField(1, leafIndex):
    msg.leafIndex = leafIndex

  var merkleRoot: seq[byte]
  if ?pb.getField(2, merkleRoot):
    msg.merkleRoot = merkleRoot

  var blockNumber: uint64
  if ?pb.getField(3, blockNumber):
    msg.blockNumber = blockNumber

  var transactionHash: seq[byte]
  if ?pb.getField(4, transactionHash):
    msg.transactionHash = transactionHash

  var configAccountId: string
  if ?pb.getField(100, configAccountId):
    msg.configAccountId = some(configAccountId)

  return ok(msg)

proc encode*(rpc: MembershipAllocationFailure): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, rpc.errorMessage)
  pb.finish3()
  return pb

proc decode*(T: type MembershipAllocationFailure, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var msg = MembershipAllocationFailure()
  var errorMessage: string
  if ?pb.getField(1, errorMessage):
    msg.errorMessage = errorMessage
  return ok(msg)

proc encode*(rpc: RlnGifterRequest): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, rpc.requestId)
  pb.write3(2, rpc.authenticationType)
  pb.write3(3, rpc.authenticationPayload)
  pb.write3(4, rpc.identityCommitment)
  if rpc.rateLimit.isSome:
    pb.write3(5, rpc.rateLimit.get())
  pb.finish3()
  return pb

proc decode*(T: type RlnGifterRequest, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var rpc = RlnGifterRequest()

  var requestId: string
  if not ?pb.getField(1, requestId):
    return err(ProtobufError.missingRequiredField("request_id"))
  rpc.requestId = requestId

  var authenticationType: seq[byte]
  discard ?pb.getField(2, authenticationType)
  rpc.authenticationType = authenticationType

  var authenticationPayload: seq[byte]
  discard ?pb.getField(3, authenticationPayload)
  rpc.authenticationPayload = authenticationPayload

  var identityCommitment: seq[byte]
  if not ?pb.getField(4, identityCommitment):
    return err(ProtobufError.missingRequiredField("identity_commitment"))
  rpc.identityCommitment = identityCommitment

  var rateLimit: uint64
  if ?pb.getField(5, rateLimit):
    rpc.rateLimit = some(rateLimit)

  return ok(rpc)

proc encode*(rpc: RlnGifterResponse): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, rpc.requestId)
  pb.write3(2, rpc.authSuccess)
  if rpc.error.isSome:
    pb.write3(3, rpc.error.get())
  if rpc.success.isSome:
    pb.write3(4, rpc.success.get().encode().buffer)
  if rpc.failure.isSome:
    pb.write3(5, rpc.failure.get().encode().buffer)
  pb.finish3()
  return pb

proc decode*(T: type RlnGifterResponse, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var rpc = RlnGifterResponse()

  var requestId: string
  if not ?pb.getField(1, requestId):
    return err(ProtobufError.missingRequiredField("request_id"))
  rpc.requestId = requestId

  var authSuccess: bool
  if not ?pb.getField(2, authSuccess):
    return err(ProtobufError.missingRequiredField("auth_success"))
  rpc.authSuccess = authSuccess

  var error: string
  if ?pb.getField(3, error):
    rpc.error = some(error)

  var successBuf: seq[byte]
  if ?pb.getField(4, successBuf):
    rpc.success = some(?MembershipAllocationSuccess.decode(successBuf))

  var failureBuf: seq[byte]
  if ?pb.getField(5, failureBuf):
    rpc.failure = some(?MembershipAllocationFailure.decode(failureBuf))

  return ok(rpc)

proc encode*(req: MembershipStatusRequest): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, req.configAccountId)
  pb.write3(2, req.identityCommitment)
  pb.finish3()
  return pb

proc decode*(T: type MembershipStatusRequest, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var req = MembershipStatusRequest()
  var configAccountId: string
  if not ?pb.getField(1, configAccountId):
    return err(ProtobufError.missingRequiredField("config_account_id"))
  req.configAccountId = configAccountId
  var identityCommitment: seq[byte]
  if not ?pb.getField(2, identityCommitment):
    return err(ProtobufError.missingRequiredField("identity_commitment"))
  req.identityCommitment = identityCommitment
  return ok(req)

proc encode*(resp: MembershipStatusResponse): ProtoBuffer =
  var pb = initProtoBuffer()
  pb.write3(1, resp.registered)
  if resp.leafIndex.isSome:
    pb.write3(2, resp.leafIndex.get())
  if resp.errorMessage.isSome:
    pb.write3(3, resp.errorMessage.get())
  pb.finish3()
  return pb

proc decode*(T: type MembershipStatusResponse, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var resp = MembershipStatusResponse()
  var registered: bool
  if not ?pb.getField(1, registered):
    return err(ProtobufError.missingRequiredField("registered"))
  resp.registered = registered
  var leafIndex: uint64
  if ?pb.getField(2, leafIndex):
    resp.leafIndex = some(leafIndex)
  var errorMessage: string
  if ?pb.getField(3, errorMessage):
    resp.errorMessage = some(errorMessage)
  return ok(resp)
