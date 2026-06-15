import std/options

type
  MembershipAllocationSuccess* = object
    leafIndex*: uint64
    merkleRoot*: seq[byte]
    blockNumber*: uint64
    transactionHash*: seq[byte]
    # Non-spec extension (high tag): LEZ config account that owns the
    # membership. Required so the client can route on-chain queries.
    configAccountId*: Option[string]

  MembershipAllocationFailure* = object
    errorMessage*: string

  RlnGifterRequest* = object
    requestId*: string
    authenticationType*: seq[byte]
    authenticationPayload*: seq[byte]
    identityCommitment*: seq[byte]
    rateLimit*: Option[uint64]

  RlnGifterResponse* = object
    requestId*: string
    authSuccess*: bool
    error*: Option[string]
    success*: Option[MembershipAllocationSuccess]
    failure*: Option[MembershipAllocationFailure]

  MembershipStatusRequest* = object
    configAccountId*: string
    identityCommitment*: seq[byte]

  MembershipStatusResponse* = object
    registered*: bool
    leafIndex*: Option[uint64]
    # Set on lookup failure (e.g. wallet RPC error); distinct from a
    # successful registered=false answer.
    errorMessage*: Option[string]

const
  EthAllowlistAuthType* = "eth-allowlist"
