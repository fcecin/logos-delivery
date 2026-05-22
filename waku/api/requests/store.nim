{.push raises: [].}

## Waku API: Store broker request types.

import std/options
import chronos
import brokers/[broker_context, request_broker]
import waku/waku_store/common  # StoreQueryRequest/Response, ErrorCode, StoreError, StoreQueryResult

export StoreQueryRequest, StoreQueryResponse, ErrorCode

RequestBroker:
  type RequestStoreQueryToAny* = object
    response*: StoreQueryResponse
    queryError*: Option[ErrorCode]
    errorDesc*: string

  proc signature(
    request: StoreQueryRequest
  ): Future[Result[RequestStoreQueryToAny, string]]

{.pop.}
