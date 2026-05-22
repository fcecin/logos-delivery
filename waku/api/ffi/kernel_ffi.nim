## Kernel-tier FFI surface for `liblogosdelivery.so`. Exposes raw `Waku`
## lifecycle for fleet/operator callers: `waku_new`, `waku_start`,
## `waku_stop`, `waku_destroy`. C declarations live in
## `liblogosdelivery_kernel.h`.

import std/[atomics, options]
import chronos, chronicles, results, ffi
import brokers/broker_context
# Imported ahead of the kernel/sequtils-heavy block to keep the messaging
# broker-macro instantiations first (gensym-order workaround).
import layers/logos_delivery
import waku/factory/waku
import waku/node/waku_node
import waku/api/requests/subscription
import waku/waku_core/[topics/content_topic, topics/pubsub_topic]
import waku/api/ffi/kernel_helpers

template requireInitializedKernel(
    ctx: ptr FFIContext[LogosDelivery], opName: string, onError: untyped
) =
  if isNil(ctx):
    let errMsg {.inject.} = opName & " failed: invalid context"
    onError
  elif isNil(ctx.myLib) or isNil(ctx.myLib[]):
    let errMsg {.inject.} = opName & " failed: node is not initialized"
    onError

registerReqFFI(CreateWakuRequest, ctx: ptr FFIContext[LogosDelivery]):
  proc(configJson: cstring): Future[Result[string, string]] {.async.} =
    let waku = (await createWakuFromJson(configJson)).valueOr:
      chronicles.error "CreateWakuRequest: createWakuFromJson failed", err = error
      return err(error)
    ctx.myLib[] = LogosDelivery.new(Waku, waku).valueOr:
      chronicles.error "CreateWakuRequest: LogosDelivery.new(Waku) failed", err = error
      return err(error)
    return ok("")

proc waku_new(
    configJson: cstring, callback: FFICallback, userData: pointer
): pointer {.dynlib, exportc, cdecl.} =
  initializeLibrary()

  if isNil(callback):
    echo "error: missing callback in waku_new"
    return nil

  var ctx = ffi.createFFIContext[LogosDelivery]().valueOr:
    let msg = "Error in createFFIContext: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    return nil

  ctx.userData = userData

  ffi.sendRequestToFFIThread(
    ctx, CreateWakuRequest.ffiNewReq(callback, userData, configJson)
  ).isOkOr:
    let msg = "error in sendRequestToFFIThread: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    ffi.destroyFFIContext(ctx).isOkOr:
      chronicles.error "destroyFFIContext failed after sendRequestToFFIThread error",
        err = $error
    return nil

  return ctx

proc waku_start(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  requireInitializedKernel(ctx, "waku_start"):
    return err(errMsg)
  (await ctx.myLib[].start()).isOkOr:
    chronicles.error "waku_start failed", err = error
    return err("failed to start: " & error)
  return ok("")

proc waku_stop(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  requireInitializedKernel(ctx, "waku_stop"):
    return err(errMsg)
  (await ctx.myLib[].stop()).isOkOr:
    chronicles.error "waku_stop failed", err = error
    return err("failed to stop: " & $error)
  return ok("")

proc waku_relay_subscribe_shard(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    pubsubTopic: cstring,
) {.ffi.} =
  requireInitializedKernel(ctx, "waku_relay_subscribe_shard"):
    return err(errMsg)
  RequestRelaySubscribeShard.request(
    ctx.myLib[].brokerCtx, PubsubTopic($pubsubTopic)
  ).isOkOr:
    chronicles.error "waku_relay_subscribe_shard failed", err = error
    return err(error)
  return ok("")

proc waku_relay_unsubscribe_shard(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    pubsubTopic: cstring,
) {.ffi.} =
  requireInitializedKernel(ctx, "waku_relay_unsubscribe_shard"):
    return err(errMsg)
  RequestRelayUnsubscribeShard.request(
    ctx.myLib[].brokerCtx, PubsubTopic($pubsubTopic)
  ).isOkOr:
    chronicles.error "waku_relay_unsubscribe_shard failed", err = error
    return err(error)
  return ok("")

proc waku_relay_subscribe_content_topic(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    contentTopic: cstring,
    pubsubTopic: cstring,
) {.ffi.} =
  ## Subscribe to a content topic. `pubsubTopic` is the optional shard: pass an
  ## empty string to derive it via auto-sharding; under static/manual sharding
  ## a non-empty shard must be supplied.
  requireInitializedKernel(ctx, "waku_relay_subscribe_content_topic"):
    return err(errMsg)
  let shardOp =
    if len(pubsubTopic) == 0:
      none[PubsubTopic]()
    else:
      some(PubsubTopic($pubsubTopic))
  RequestRelaySubscribeContentTopic.request(
    ctx.myLib[].brokerCtx, ContentTopic($contentTopic), shardOp
  ).isOkOr:
    chronicles.error "waku_relay_subscribe_content_topic failed", err = error
    return err(error)
  return ok("")

proc waku_relay_unsubscribe_content_topic(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    contentTopic: cstring,
    pubsubTopic: cstring,
) {.ffi.} =
  ## Unsubscribe from a content topic. `pubsubTopic` is the optional shard, same
  ## convention as `waku_relay_subscribe_content_topic`.
  requireInitializedKernel(ctx, "waku_relay_unsubscribe_content_topic"):
    return err(errMsg)
  let shardOp =
    if len(pubsubTopic) == 0:
      none[PubsubTopic]()
    else:
      some(PubsubTopic($pubsubTopic))
  RequestRelayUnsubscribeContentTopic.request(
    ctx.myLib[].brokerCtx, ContentTopic($contentTopic), shardOp
  ).isOkOr:
    chronicles.error "waku_relay_unsubscribe_content_topic failed", err = error
    return err(error)
  return ok("")

proc waku_destroy(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
): cint {.dynlib, exportc, cdecl.} =
  initializeLibrary()
  checkParams(ctx, callback, userData)

  ffi.destroyFFIContext(ctx).isOkOr:
    let msg = "waku_destroy error: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    return RET_ERR

  callback(RET_OK, nil, 0, userData)
  return RET_OK
