{.used.}

import std/strutils, results, stew/byteutils, testutils/unittests
import
  logos_delivery/waku/[common/protobuf, waku_filter_v2/rpc, waku_filter_v2/rpc_codec],
  ../testlib/protobuf_errors

suite "Waku Filter - RPC codec":
  test "a message push without a pubsub topic is refused":
    let res = MessagePush.decode(hexToSeqByte("0a070a010112022f74"))
    check:
      res.isErr()
      res.error == ProtobufError.missingRequiredField("pubsub_topic")

  test "a message push with a nested meta of MaxMetaAttrLength + 1 bytes is refused":
    let res = MessagePush.decode(
      hexToSeqByte("0a4a0a010112022f745a41" & "00".repeat(65) & "12022f73")
    )
    check:
      res.isErr()
      res.error == ProtobufError.invalidLengthField("meta")

  test "a message push with a nested message without a content topic is refused":
    let res = MessagePush.decode(hexToSeqByte("0a030a010112022f73"))
    check:
      res.isErr()
      res.error == ProtobufError.missingRequiredField("content_topic")
