{.push raises: [].}

import
  ../waku_core,
  ../waku_relay,
  ./common,
  ./protocol_metrics,
  ../rln,
  ../rln/protocol_types

import std/times, libp2p/peerid, stew/byteutils

proc checkAndGenerateRLNProof*(
    rlnPeer: Option[Rln], message: WakuMessage
): Future[Result[WakuMessage, string]] {.async.} =
  # check if the message already has RLN proof
  if message.proof.len > 0:
    return ok(message)

  if rlnPeer.isNone():
    notice "Publishing message without RLN proof"
    return ok(message)
  # generate and append RLN proof
  let
    time = getTime().toUnix()
    senderEpochTime = float64(time)
  var msgWithProof = message
  msgWithProof.proof = (
    await rlnPeer.get().generateRLNProof(msgWithProof.toRLNSignal(), senderEpochTime)
  ).valueOr:
    return err($error)
  return ok(msgWithProof)

proc getNilPushHandler*(): PushMessageHandler =
  return proc(
      pubsubTopic: string, message: WakuMessage
  ): Future[WakuLightPushResult[void]] {.async.} =
    return err("no waku relay found")

proc getRelayPushHandler*(
    wakuRelay: Option[WakuRelay], rlnPeer: Option[Rln] = none[Rln]()
): PushMessageHandler =
  return proc(
      pubsubTopic: string, message: WakuMessage
  ): Future[WakuLightPushResult[void]] {.async.} =
    # append RLN proof
    let msgWithProof = ?(await checkAndGenerateRLNProof(rlnPeer, message))

    # Prefer the relay validator chain when available (preserves the full
    # chain, including any non-RLN validators). Fall back to RLN-direct
    # when relay is not mounted.
    if wakuRelay.isSome():
      ?(await wakuRelay.get().validateMessage(pubSubTopic, msgWithProof))
    elif rlnPeer.isSome():
      let validationRes = await rlnPeer.get().validateMessageAndUpdateLog(msgWithProof)
      if validationRes != MessageValidationResult.Valid:
        return err($validationRes)

    if wakuRelay.isNone():
      return err(protocol_metrics.notPublishedAnyPeer)

    (await wakuRelay.get().publish(pubsubTopic, msgWithProof)).isOkOr:
      ## Agreed change expected to the lightpush protocol to better handle such case. https://github.com/waku-org/pm/issues/93
      let msgHash = computeMessageHash(pubsubTopic, message).to0xHex()
      notice "Lightpush request has not been published to any peers",
        msg_hash = msgHash, reason = $error
      # for legacy lightpush we do not detail the reason towards clients. All error during publish result in not-published-to-any-peer
      # this let client of the legacy protocol to react as they did so far.
      return err(protocol_metrics.notPublishedAnyPeer)

    return ok()
