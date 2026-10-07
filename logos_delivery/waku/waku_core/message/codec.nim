## Waku Message module: encoding and decoding
# See:
# - RFC 14: https://rfc.vac.dev/spec/14/
# - Proto definition: https://github.com/vacp2p/waku/blob/main/waku/message/v1/message.proto
{.push raises: [].}

import std/unicode
import ../../common/protobuf, ./message

proc validateWakuMessageFields*(msg: WakuMessage): ProtobufResult[void] =
  ## Refuses a message that each peer refuses at decode. Codecs call it for
  ## a nested `WakuMessage`, and the relay calls it before a send. At decode
  ## the library refuses a `string` that is not valid UTF-8 before this proc
  ## runs, so the UTF-8 check here acts only on the send side.
  if msg.contentTopic.len == 0:
    return err(ProtobufError.missingRequiredField("content_topic"))
  if validateUtf8(msg.contentTopic) != -1:
    return err(ProtobufError.decodeFailure("content_topic is not valid UTF-8"))
  if msg.meta.len > MaxMetaAttrLength:
    return err(ProtobufError.invalidLengthField("meta"))
  ok()

protobufCodec(WakuMessage, validateWakuMessageFields)
