{.used.}

import std/options, results
import testutils/unittests
import eth/common/[addresses, keys]
import waku/waku_rln_relay/rln_gifter/rpc
import waku/waku_rln_relay/rln_gifter/rpc_codec
import waku/waku_rln_relay/rln_gifter/protocol as rln_gifter_protocol

proc eip191Sign(seckey: PrivateKey, idCommitment: openArray[byte]): seq[byte] =
  @(seckey.sign(eip191Message(idCommitment)).toRaw())

proc bytesOf(s: string): seq[byte] =
  result = newSeqOfCap[byte](s.len)
  for c in s:
    result.add(byte(c))

const
  TestSecretHex =
    "1111111111111111111111111111111111111111111111111111111111111111"

let TestCommitment = @[
  byte 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
  0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
  0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
  0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
]

let TamperedCommitment = @[
  byte 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
  0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
  0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
  0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd, 0xcd,
]

suite "RLN gifter EIP-191 auth":
  test "verifyEip191 recovers the signer address":
    let sk = PrivateKey.fromHex(TestSecretHex).expect("valid key")
    let expected = sk.toPublicKey().to(Address)
    let sig = eip191Sign(sk, TestCommitment)

    let recovered = verifyEip191(TestCommitment, sig).expect("recoverable")
    check recovered == expected

  test "verifyEip191 rejects wrong-length payload":
    let bad = newSeq[byte](64)
    let res = verifyEip191(TestCommitment, bad)
    check res.isErr

  test "verifyEip191 produces a different address when the message differs":
    let sk = PrivateKey.fromHex(TestSecretHex).expect("valid key")
    let expected = sk.toPublicKey().to(Address)
    let sig = eip191Sign(sk, TestCommitment)

    let recovered = verifyEip191(TamperedCommitment, sig).expect("recoverable")
    check recovered != expected

  test "verifyEip191 rejects malformed signature bytes":
    let bogus = newSeq[byte](65)
    let res = verifyEip191(TestCommitment, bogus)
    check res.isErr

suite "RLN gifter request codec":
  test "round-trips all fields when populated":
    let req = RlnGifterRequest(
      requestId: "req-1",
      authenticationType: bytesOf("eth-allowlist"),
      authenticationPayload: @[byte 0xde, 0xad, 0xbe, 0xef],
      identityCommitment: TestCommitment,
      rateLimit: some(42'u64),
    )
    let decoded = RlnGifterRequest.decode(req.encode().buffer).expect("decodes")
    check:
      decoded.requestId == "req-1"
      decoded.authenticationType == bytesOf("eth-allowlist")
      decoded.authenticationPayload == @[byte 0xde, 0xad, 0xbe, 0xef]
      decoded.identityCommitment == TestCommitment
      decoded.rateLimit == some(42'u64)

  test "rate_limit is optional on the wire":
    let req = RlnGifterRequest(
      requestId: "req-2",
      authenticationType: @[],
      authenticationPayload: @[],
      identityCommitment: TestCommitment,
      rateLimit: none(uint64),
    )
    let decoded = RlnGifterRequest.decode(req.encode().buffer).expect("decodes")
    check:
      decoded.requestId == "req-2"
      decoded.identityCommitment == TestCommitment
      decoded.rateLimit.isNone

suite "RLN gifter response codec":
  test "encodes/decodes a success result":
    let resp = RlnGifterResponse(
      requestId: "req-1",
      authSuccess: true,
      error: none(string),
      success: some(MembershipAllocationSuccess(
        leafIndex: 7'u64,
        merkleRoot: @[byte 0x01, 0x02, 0x03],
        blockNumber: 42'u64,
        transactionHash: @[byte 0xaa, 0xbb],
        configAccountId: some("acct-123"),
      )),
      failure: none(MembershipAllocationFailure),
    )
    let decoded = RlnGifterResponse.decode(resp.encode().buffer).expect("decodes")
    check:
      decoded.requestId == "req-1"
      decoded.authSuccess == true
      decoded.success.isSome
      decoded.success.get().leafIndex == 7'u64
      decoded.success.get().merkleRoot == @[byte 0x01, 0x02, 0x03]
      decoded.success.get().blockNumber == 42'u64
      decoded.success.get().transactionHash == @[byte 0xaa, 0xbb]
      decoded.success.get().configAccountId == some("acct-123")
      decoded.failure.isNone

  test "encodes/decodes a failure result":
    let resp = RlnGifterResponse(
      requestId: "req-2",
      authSuccess: false,
      error: some("address not allowlisted"),
      success: none(MembershipAllocationSuccess),
      failure: some(MembershipAllocationFailure(errorMessage: "address not allowlisted")),
    )
    let decoded = RlnGifterResponse.decode(resp.encode().buffer).expect("decodes")
    check:
      decoded.requestId == "req-2"
      decoded.authSuccess == false
      decoded.error == some("address not allowlisted")
      decoded.failure.isSome
      decoded.failure.get().errorMessage == "address not allowlisted"
      decoded.success.isNone
