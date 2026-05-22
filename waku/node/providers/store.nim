{.push raises: [].}

## Store broker provider wiring. Binds the kernel store broker request to the
## live store client.

import std/options
import chronos, results
import ../waku_node
import ../../waku_store/client as store_client
import ../../waku_store/common as store_common
import ../../api/requests/store as store_api

proc registerStoreProviders*(node: WakuNode): Result[void, string] =
  ## Bind the store broker provider to the live store client.
  store_api.RequestStoreQueryToAny.setProvider(
    node.brokerCtx,
    proc(
        request: store_common.StoreQueryRequest
    ): Future[Result[store_api.RequestStoreQueryToAny, string]] {.async.} =
      var res: store_common.StoreQueryResult
      try:
        res = await node.wakuStoreClient.queryToAny(request)
      except CatchableError:
        res = store_common.StoreQueryResult.err(
          store_common.StoreError(kind: store_common.ErrorCode.UNKNOWN)
        )
      if res.isOk():
        return ok(
          store_api.RequestStoreQueryToAny(
            response: res.value,
            queryError: none(store_common.ErrorCode),
            errorDesc: "",
          )
        )
      let storeErr = res.error
      var desc = $storeErr.kind
      case storeErr.kind
      of store_common.ErrorCode.PEER_DIAL_FAILURE:
        desc.add(": " & storeErr.address)
      of store_common.ErrorCode.BAD_RESPONSE, store_common.ErrorCode.BAD_REQUEST:
        desc.add(": " & storeErr.cause)
      else:
        discard
      return ok(
        store_api.RequestStoreQueryToAny(
          response: default(store_common.StoreQueryResponse),
          queryError: some(storeErr.kind),
          errorDesc: desc,
        )
      ),
  ).isOkOr:
    return err("registerStoreProviders: RequestStoreQueryToAny: " & error)

  ok()

{.pop.}
