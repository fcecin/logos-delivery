import stint, chronos

import ./protocol_types

import ../../waku_keystore
import ../../waku_core/message/default_values

# Acceptable roots for merkle root validation of incoming messages
const AcceptableRootWindowSize* = 50

#Size if RLN contract root cache
const RlnContractRootCacheSize* = 5

# Minimum time between two consecutive root refreshes, to avoid refreshing the roots too often when receiving messages with old roots
# Using Linea block generation time as reference, which is around 2 seconds
const RootsRefreshMinInterval* = 2.seconds

# RLN membership key and index files path
const RlnCredentialsFilename* = "rlnCredentials.txt"

# inputs of the membership contract constructor
# TODO may be able to make these constants private and put them inside the waku_rln_utils
const
  # in wei
  MembershipFee* = 0.u256
  #  the current implementation of the rln lib supports a circuit for Merkle tree with depth 20
  MerkleTreeDepth* = 20
  EthClient* = "http://127.0.0.1:8540"

const
  # the size of poseidon hash output in bits
  HashBitSize* = 256
  # the size of poseidon hash output as the number hex digits
  HashHexSize* = int(HashBitSize / 4)

const
  # pre-processed "rln/waku-rln-relay/v2.0.0" to array[32, byte]
  DefaultRlnIdentifier*: RlnIdentifier = [
    114, 108, 110, 47, 119, 97, 107, 117, 45, 114, 108, 110, 45, 114, 101, 108, 97, 121,
    47, 118, 50, 46, 48, 46, 48, 0, 0, 0, 0, 0, 0, 0,
  ]
  DefaultUserMessageLimit* = UserMessageLimit(20)

const MaxTimestampGapPastSeconds* = float64(MaxMessageTimestampSkewPastSeconds)
  ## The time the timestamp of a message may lie behind the clock of a
  ## validator, see `MaxMessageTimestampSkewPastSeconds`. The nullifier log
  ## keeps the epochs it spans.
const MaxTimestampGapFutureSeconds* = float64(MaxMessageTimestampSkewFutureSeconds)
  ## The time the timestamp of a message may lie ahead of the clock of a
  ## validator, see `MaxMessageTimestampSkewFutureSeconds`.

# RLN Keystore defaults
const RLNAppInfo* = AppInfo(
  application: "waku-rln-relay", appIdentifier: "01234567890abcdef", version: "0.2"
)
