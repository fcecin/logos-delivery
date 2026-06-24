{.push raises: [].}
import std/hashes
import bearssl/rand, std/times, chronos
import stew/byteutils
import libp2p/crypto/crypto

import logos_delivery/waku/compat/option_valueor

import logos_delivery/waku/waku_core/[topics/content_topic, message/message, time]

export content_topic, message

import types/sds_message_id

export sds_message_id

type
  MessageEnvelope* = object
    contentTopic*: ContentTopic
    payload*: seq[byte]
    ephemeral*: bool
    meta*: seq[byte]
      ## Opaque wire-format marker carried on the underlying WakuMessage.
      ## Higher layers (e.g. Reliable Channel) stamp this so peers can route
      ## ingress traffic to their corresponding layer. Empty by default.

  RequestId* = distinct string

  ConnectionStatus* {.pure.} = enum
    Disconnected
    PartiallyConnected
    Connected

  ChannelId* = SdsChannelID

proc generateRequestId*(rng: crypto.Rng): string =
  var bytes: array[10, byte]
  rng.generate(bytes)
  return byteutils.toHex(bytes)

proc new*(T: typedesc[RequestId], rng: crypto.Rng): T =
  ## Generate a new RequestId using the provided RNG.
  RequestId(generateRequestId(rng))

proc `$`*(r: RequestId): string {.inline.} =
  string(r)

proc `==`*(a, b: RequestId): bool {.inline.} =
  string(a) == string(b)

proc hash*(r: RequestId): Hash =
  ## Allows `RequestId` to be used as a `Table` key.
  hash(string(r))

proc init*(
    T: type MessageEnvelope,
    contentTopic: ContentTopic,
    payload: seq[byte] | string,
    ephemeral: bool = false,
    meta: seq[byte] = @[],
): MessageEnvelope =
  when payload is seq[byte]:
    MessageEnvelope(
      contentTopic: contentTopic, payload: payload, ephemeral: ephemeral, meta: meta
    )
  else:
    MessageEnvelope(
      contentTopic: contentTopic,
      payload: payload.toBytes(),
      ephemeral: ephemeral,
      meta: meta,
    )

proc toWakuMessage*(envelope: MessageEnvelope): WakuMessage =
  ## Convert a MessageEnvelope to a WakuMessage.
  var wm = WakuMessage(
    contentTopic: envelope.contentTopic,
    payload: envelope.payload,
    ephemeral: envelope.ephemeral,
    meta: envelope.meta,
    timestamp: getNowInNanosecondTime(),
  )

  ## TODO: First find out if proof is needed at all
  ## Follow up: left it to the send logic to add RLN proof if needed and possible
  # let requestedProof = (
  #   waitFor RequestGenerateRlnProof.request(wm, getTime().toUnixFloat())
  # ).valueOr:
  #   warn "Failed to add RLN proof to WakuMessage: ", error = error
  #   return wm

  # wm.proof = requestedProof.proof
  return wm

{.pop.}
