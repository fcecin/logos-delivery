{.push raises: [].}

## Relay broker provider wiring. Binds the kernel relay publish broker to the
## live relay protocol (RLN proof + validation + publish).

import std/options
import chronos, results
import ../waku_node
import ../../waku_core
import ../../waku_relay
import ../../waku_rln_relay
import ../../waku_lightpush/callbacks
import ../../api/requests/relay as relay_api

proc registerRelayProviders*(node: WakuNode): Result[void, string] =
  ## Bind the relay publish broker provider to the live relay protocol.
  relay_api.RequestRelayPublish.setProvider(
    node.brokerCtx,
    proc(
        pubsubTopic: PubsubTopic, wakuMessage: WakuMessage
    ): Future[Result[relay_api.RequestRelayPublish, string]] {.async.} =
      # Publish-path exceptions propagate; the broker turns them into a request
      # error, which the caller maps to a permanent failure.
      let rlnPeer =
        if node.wakuRlnRelay.isNil():
          none(WakuRLNRelay)
        else:
          some(node.wakuRlnRelay)
      let msgWithProof = checkAndGenerateRLNProof(rlnPeer, wakuMessage).valueOr:
        return ok(
          relay_api.RequestRelayPublish(
            relayedPeerCount: 0'u32,
            publishError: none(PublishOutcome),
            rlnProofFailed: true,
            validationFailed: false,
            errorDesc: error,
          )
        )

      (await node.wakuRelay.validateMessage(pubsubTopic, msgWithProof)).isOkOr:
        return ok(
          relay_api.RequestRelayPublish(
            relayedPeerCount: 0'u32,
            publishError: none(PublishOutcome),
            rlnProofFailed: false,
            validationFailed: true,
            errorDesc: error,
          )
        )

      let res = await node.wakuRelay.publish(pubsubTopic, msgWithProof)
      if res.isOk():
        return ok(
          relay_api.RequestRelayPublish(
            relayedPeerCount: res.value.uint32,
            publishError: none(PublishOutcome),
            rlnProofFailed: false,
            validationFailed: false,
            errorDesc: "",
          )
        )
      return ok(
        relay_api.RequestRelayPublish(
          relayedPeerCount: 0'u32,
          publishError: some(res.error),
          rlnProofFailed: false,
          validationFailed: false,
          errorDesc: $res.error,
        )
      ),
  ).isOkOr:
    return err("registerRelayProviders: RequestRelayPublish: " & error)

  ok()

{.pop.}
