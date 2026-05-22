{.push raises: [].}

## Lightpush broker provider wiring. Binds the kernel lightpush broker request
## to the live lightpush client.

import std/options
import chronos, results
import ../waku_node
import ../../waku_core
import ../../waku_lightpush/client as lightpush_client
import ../../waku_lightpush as lightpush_protocol
import ../../api/requests/lightpush

proc registerLightpushProviders*(node: WakuNode): Result[void, string] =
  ## Bind the lightpush broker provider to the live lightpush client.
  RequestLightpushPublish.setProvider(
    node.brokerCtx,
    proc(
        peer: RemotePeerInfo,
        pubsubTopic: PubsubTopic,
        wakuMessage: WakuMessage,
    ): Future[Result[RequestLightpushPublish, string]] {.async.} =
      try:
        let res =
          await node.wakuLightpushClient.publish(some(pubsubTopic), wakuMessage, peer)
        if res.isOk():
          return ok(
            RequestLightpushPublish(
              relayedPeerCount: res.value,
              publishError: none(LightPushStatusCode),
              errorDesc: "",
            )
          )
        return ok(
          RequestLightpushPublish(
            relayedPeerCount: 0'u32,
            publishError: some(res.error.code),
            errorDesc: res.error.desc.get(""),
          )
        )
      except CatchableError as e:
        return ok(
          RequestLightpushPublish(
            relayedPeerCount: 0'u32,
            publishError: some(LightPushErrorCode.INTERNAL_SERVER_ERROR),
            errorDesc: "lightpush.publish raised: " & e.msg,
          )
        ),
  ).isOkOr:
    return err("registerLightpushProviders: RequestLightpushPublish: " & error)

  ok()

{.pop.}
