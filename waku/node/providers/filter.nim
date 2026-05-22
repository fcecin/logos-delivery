{.push raises: [].}

## Filter broker provider wiring. Binds the kernel filter broker requests to
## the live filter client.

import std/options
import chronos, results
import ../waku_node
import ../../waku_core
import ../../waku_filter_v2
import ../../waku_filter_v2/client as filter_client
import ../../api/requests/filter

# Collapse FilterSubscribeError (case object) to a description string.
proc filterErrDesc(e: FilterSubscribeError): string =
  result = $e.kind
  case e.kind
  of FilterSubscribeErrorKind.PEER_DIAL_FAILURE:
    result.add(": " & e.address)
  of FilterSubscribeErrorKind.BAD_RESPONSE,
      FilterSubscribeErrorKind.BAD_REQUEST,
      FilterSubscribeErrorKind.NOT_FOUND,
      FilterSubscribeErrorKind.TOO_MANY_REQUESTS,
      FilterSubscribeErrorKind.SERVICE_UNAVAILABLE:
    result.add(": " & e.cause)
  else:
    discard

proc registerFilterProviders*(node: WakuNode): Result[void, string] =
  ## Bind the filter broker providers to the live filter client.
  RequestFilterSubscribe.setProvider(
    node.brokerCtx,
    proc(
        servicePeer: RemotePeerInfo,
        pubsubTopic: PubsubTopic,
        contentTopics: seq[ContentTopic],
    ): Future[Result[RequestFilterSubscribe, string]] {.async.} =
      var res: FilterSubscribeResult
      try:
        res =
          await node.wakuFilterClient.subscribe(servicePeer, pubsubTopic, contentTopics)
      except CatchableError as e:
        res = FilterSubscribeResult.err(FilterSubscribeError.badResponse(e.msg))
      if res.isOk():
        return ok(
          RequestFilterSubscribe(
            subscribed: true,
            subscribeError: none(FilterSubscribeErrorKind),
            errorDesc: "",
          )
        )
      return ok(
        RequestFilterSubscribe(
          subscribed: false,
          subscribeError: some(res.error.kind),
          errorDesc: filterErrDesc(res.error),
        )
      ),
  ).isOkOr:
    return err("registerFilterProviders: RequestFilterSubscribe: " & error)

  RequestFilterUnsubscribe.setProvider(
    node.brokerCtx,
    proc(
        servicePeer: RemotePeerInfo,
        pubsubTopic: PubsubTopic,
        contentTopics: seq[ContentTopic],
    ): Future[Result[RequestFilterUnsubscribe, string]] {.async.} =
      var res: FilterSubscribeResult
      try:
        res = await node.wakuFilterClient.unsubscribe(
          servicePeer, pubsubTopic, contentTopics
        )
      except CatchableError as e:
        res = FilterSubscribeResult.err(FilterSubscribeError.badResponse(e.msg))
      if res.isOk():
        return ok(
          RequestFilterUnsubscribe(
            unsubscribed: true,
            subscribeError: none(FilterSubscribeErrorKind),
            errorDesc: "",
          )
        )
      return ok(
        RequestFilterUnsubscribe(
          unsubscribed: false,
          subscribeError: some(res.error.kind),
          errorDesc: filterErrDesc(res.error),
        )
      ),
  ).isOkOr:
    return err("registerFilterProviders: RequestFilterUnsubscribe: " & error)

  RequestFilterPing.setProvider(
    node.brokerCtx,
    proc(
        servicePeer: RemotePeerInfo, timeout: Duration
    ): Future[Result[RequestFilterPing, string]] {.async.} =
      var res: FilterSubscribeResult
      try:
        res = await node.wakuFilterClient.ping(servicePeer, timeout)
      except CatchableError as e:
        res = FilterSubscribeResult.err(FilterSubscribeError.badResponse(e.msg))
      if res.isOk():
        return ok(
          RequestFilterPing(
            pingOk: true,
            subscribeError: none(FilterSubscribeErrorKind),
            errorDesc: "",
          )
        )
      return ok(
        RequestFilterPing(
          pingOk: false,
          subscribeError: some(res.error.kind),
          errorDesc: filterErrDesc(res.error),
        )
      ),
  ).isOkOr:
    return err("registerFilterProviders: RequestFilterPing: " & error)

  ok()

{.pop.}
