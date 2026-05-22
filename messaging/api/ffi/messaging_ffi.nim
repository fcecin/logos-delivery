## FFI surface for `liblogosdelivery.so`. Exported C functions use the
## `logosdelivery_*` prefix; C declarations live in `liblogosdelivery.h`.

import std/[json, locks, strutils, tables]
import chronos, chronicles, results, ffi
import stew/byteutils
import waku/common/base64
import waku/factory/waku
import waku/factory/waku_state_info
import waku/api/ffi/kernel_helpers
import waku/waku_core/topics/content_topic
import layers/logos_delivery
import messaging/api/types
import messaging/api/events
import messaging/api/messaging as messaging_brokers
import tools/confutils/cli_args
import tools/confutils/config_option_meta
import messaging/api/ffi/json_event

# `RequestId` is rendered via `$`.
proc `%`*(id: RequestId): JsonNode =
  %($id)

var eventCallbackLock: Lock
initLock(eventCallbackLock)

# Event listener handles registered at start, kept per broker context so stop
# drops exactly these.
type MessagingFFIListeners = object
  sent: MessageSentEventListener
  error: MessageErrorEventListener
  propagated: MessagePropagatedEventListener
  received: MessageReceivedEventListener
  connStatus: EventConnectionStatusChangeListener

var ffiListeners {.threadvar.}: Table[uint32, MessagingFFIListeners]

template requireInitializedMessaging(
    ctx: ptr FFIContext[LogosDelivery], opName: string, onError: untyped
) =
  if isNil(ctx):
    let errMsg {.inject.} = opName & " failed: invalid context"
    onError
  elif isNil(ctx.myLib) or isNil(ctx.myLib[]):
    let errMsg {.inject.} = opName & " failed: client is not initialized"
    onError

# ---- Construction requests (run on the FFI worker thread) ----

registerReqFFI(CreateMessagingClientByPresetMode, ctx: ptr FFIContext[LogosDelivery]):
  proc(preset: cstring, mode: cstring): Future[Result[string, string]] {.async.} =
    let modeEnum =
      try:
        parseEnum[WakuMode]($mode)
      except ValueError:
        return err("Invalid mode value: " & $mode)
    ctx.myLib[] = (await LogosDelivery.new(MessagingClient, $preset, modeEnum)).valueOr:
      chronicles.error "CreateMessagingClientByPresetMode failed", err = error
      return err(error)
    return ok("")

registerReqFFI(CreateMessagingClientByConf, ctx: ptr FFIContext[LogosDelivery]):
  proc(configJson: cstring): Future[Result[string, string]] {.async.} =
    let waku = (await createWakuFromJson(configJson)).valueOr:
      chronicles.error "CreateMessagingClientByConf: createWakuFromJson failed",
        err = error
      return err(error)
    ctx.myLib[] = LogosDelivery.new(MessagingClient, waku).valueOr:
      chronicles.error "CreateMessagingClientByConf: LogosDelivery.new failed",
        err = error
      return err(error)
    return ok("")

# ---- C exports ----

proc logosdelivery_create_node_preset_mode(
    preset: cstring, mode: cstring, callback: FFICallback, userData: pointer
): pointer {.dynlib, exportc, cdecl.} =
  ## Create a node from a preset name and mode string.
  initializeLibrary()

  if isNil(callback):
    echo "error: missing callback in logosdelivery_create_node_preset_mode"
    return nil

  var ctx = ffi.createFFIContext[LogosDelivery]().valueOr:
    let msg = "Error in createFFIContext: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    return nil

  ctx.userData = userData

  ffi.sendRequestToFFIThread(
    ctx,
    CreateMessagingClientByPresetMode.ffiNewReq(callback, userData, preset, mode),
  ).isOkOr:
    let msg = "error in sendRequestToFFIThread: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    ffi.destroyFFIContext(ctx).isOkOr:
      chronicles.error "destroyFFIContext failed after sendRequestToFFIThread error",
        err = $error
    return nil

  return ctx

proc logosdelivery_create_node(
    configJson: cstring, callback: FFICallback, userData: pointer
): pointer {.dynlib, exportc, cdecl.} =
  initializeLibrary()

  if isNil(callback):
    echo "error: missing callback in logosdelivery_create_node"
    return nil

  var ctx = ffi.createFFIContext[LogosDelivery]().valueOr:
    let msg = "Error in createFFIContext: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    return nil

  ctx.userData = userData

  ffi.sendRequestToFFIThread(
    ctx, CreateMessagingClientByConf.ffiNewReq(callback, userData, configJson)
  ).isOkOr:
    let msg = "error in sendRequestToFFIThread: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    ffi.destroyFFIContext(ctx).isOkOr:
      chronicles.error "destroyFFIContext failed after sendRequestToFFIThread error",
        err = $error
    return nil

  return ctx

proc logosdelivery_destroy(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
): cint {.dynlib, exportc, cdecl.} =
  initializeLibrary()
  checkParams(ctx, callback, userData)

  ffi.destroyFFIContext(ctx).isOkOr:
    let msg = "logosdelivery_destroy error: " & $error
    callback(RET_ERR, unsafeAddr msg[0], cast[csize_t](len(msg)), userData)
    return RET_ERR

  callback(RET_OK, nil, 0, userData)
  return RET_OK

proc logosdelivery_set_event_callback(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.dynlib, exportc, cdecl.} =
  if isNil(ctx):
    echo "error: invalid context in logosdelivery_set_event_callback"
    return
  eventCallbackLock.acquire()
  defer:
    eventCallbackLock.release()
  ctx[].eventCallback = cast[pointer](callback)
  ctx[].eventUserData = userData

# ---- Lifecycle: start (register event listeners + MessagingClient.start) ----

proc logosdelivery_start_node(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  requireInitializedMessaging(ctx, "logosdelivery_start_node"):
    return err(errMsg)

  let brokerCtx = ctx.myLib[].brokerCtx

  let sentListener = MessageSentEvent.listen(
    brokerCtx,
    proc(event: MessageSentEvent) {.async: (raises: []).} =
      callEventCallback(ctx, "onMessageSent"):
        $newJsonEvent("message_sent", event),
  ).valueOr:
    chronicles.error "MessageSentEvent.listen failed", err = $error
    return err("MessageSentEvent.listen failed: " & $error)

  let errorListener = MessageErrorEvent.listen(
    brokerCtx,
    proc(event: MessageErrorEvent) {.async: (raises: []).} =
      callEventCallback(ctx, "onMessageError"):
        $newJsonEvent("message_error", event),
  ).valueOr:
    chronicles.error "MessageErrorEvent.listen failed", err = $error
    return err("MessageErrorEvent.listen failed: " & $error)

  let propagatedListener = MessagePropagatedEvent.listen(
    brokerCtx,
    proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
      callEventCallback(ctx, "onMessagePropagated"):
        $newJsonEvent("message_propagated", event),
  ).valueOr:
    chronicles.error "MessagePropagatedEvent.listen failed", err = $error
    return err("MessagePropagatedEvent.listen failed: " & $error)

  let receivedListener = MessageReceivedEvent.listen(
    brokerCtx,
    proc(event: MessageReceivedEvent) {.async: (raises: []).} =
      callEventCallback(ctx, "onMessageReceived"):
        $newJsonEvent("message_received", event),
  ).valueOr:
    chronicles.error "MessageReceivedEvent.listen failed", err = $error
    return err("MessageReceivedEvent.listen failed: " & $error)

  let connStatusListener = EventConnectionStatusChange.listen(
    brokerCtx,
    proc(event: EventConnectionStatusChange) {.async: (raises: []).} =
      callEventCallback(ctx, "onConnectionStatusChange"):
        $newJsonEvent("connection_status_change", event),
  ).valueOr:
    chronicles.error "EventConnectionStatusChange.listen failed", err = $error
    return err("EventConnectionStatusChange.listen failed: " & $error)

  ffiListeners[brokerCtx.uint32] = MessagingFFIListeners(
    sent: sentListener,
    error: errorListener,
    propagated: propagatedListener,
    received: receivedListener,
    connStatus: connStatusListener,
  )

  (await ctx.myLib[].start()).isOkOr:
    chronicles.error "logosdelivery_start_node failed", err = error
    return err("failed to start: " & error)
  return ok("")

# ---- Lifecycle: stop (drop listeners + MessagingClient.stop) ----

proc logosdelivery_stop_node(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  requireInitializedMessaging(ctx, "logosdelivery_stop_node"):
    return err(errMsg)

  let brokerCtx = ctx.myLib[].brokerCtx
  var listeners: MessagingFFIListeners
  if ffiListeners.pop(brokerCtx.uint32, listeners):
    await MessageSentEvent.dropListener(brokerCtx, listeners.sent)
    await MessageErrorEvent.dropListener(brokerCtx, listeners.error)
    await MessagePropagatedEvent.dropListener(brokerCtx, listeners.propagated)
    await MessageReceivedEvent.dropListener(brokerCtx, listeners.received)
    await EventConnectionStatusChange.dropListener(brokerCtx, listeners.connStatus)

  (await ctx.myLib[].stop()).isOkOr:
    chronicles.error "logosdelivery_stop_node failed", err = error
    return err("failed to stop: " & error)
  return ok("")

# ---- Messaging operations ----

proc logosdelivery_subscribe(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    contentTopicStr: cstring,
) {.ffi.} =
  requireInitializedMessaging(ctx, "logosdelivery_subscribe"):
    return err(errMsg)

  let contentTopic = ContentTopic($contentTopicStr)

  (
    await messaging_brokers.RequestMessagingSubscribe.request(
      ctx.myLib[].brokerCtx, contentTopic
    )
  ).isOkOr:
    return err("subscribe failed: " & error)

  return ok("")

proc logosdelivery_unsubscribe(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    contentTopicStr: cstring,
) {.ffi.} =
  requireInitializedMessaging(ctx, "logosdelivery_unsubscribe"):
    return err(errMsg)

  let contentTopic = ContentTopic($contentTopicStr)

  messaging_brokers.RequestMessagingUnsubscribe.request(
    ctx.myLib[].brokerCtx, contentTopic
  ).isOkOr:
    return err("unsubscribe failed: " & error)

  return ok("")

proc logosdelivery_send(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    messageJson: cstring,
) {.ffi.} =
  requireInitializedMessaging(ctx, "logosdelivery_send"):
    return err(errMsg)

  var jsonNode: JsonNode
  try:
    jsonNode = parseJson($messageJson)
  except Exception as e:
    return err("Failed to parse message JSON: " & e.msg)

  if not jsonNode.hasKey("contentTopic"):
    return err("Missing contentTopic field")

  let contentTopic = ContentTopic(jsonNode["contentTopic"].getStr())

  if not jsonNode.hasKey("payload"):
    return err("Missing payload field")

  let payloadStr = jsonNode["payload"].getStr()
  let payload = base64.decode(Base64String(payloadStr)).valueOr:
    return err("invalid payload format: " & error)

  let ephemeral = jsonNode.getOrDefault("ephemeral").getBool(false)

  let envelope = MessageEnvelope.init(
    contentTopic = contentTopic, payload = payload, ephemeral = ephemeral
  )

  let sendResp = (
    await messaging_brokers.RequestMessagingSend.request(
      ctx.myLib[].brokerCtx, envelope
    )
  ).valueOr:
    return err("send failed: " & error)

  return ok($sendResp.requestId)

# ---- Debug / introspection ----

proc logosdelivery_get_available_node_info_ids(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  ## List of all available node info item ids queryable via get_node_info.
  requireInitializedMessaging(ctx, "logosdelivery_get_available_node_info_ids"):
    return err(errMsg)
  return ok($ctx.myLib[].waku.stateInfo.getAllPossibleInfoItemIds())

proc logosdelivery_get_node_info(
    ctx: ptr FFIContext[LogosDelivery],
    callback: FFICallBack,
    userData: pointer,
    nodeInfoId: cstring,
) {.ffi.} =
  ## Content of the node info item with the given id, if it exists.
  requireInitializedMessaging(ctx, "logosdelivery_get_node_info"):
    return err(errMsg)
  let infoItemIdEnum =
    try:
      parseEnum[NodeInfoId]($nodeInfoId)
    except ValueError:
      return err("Invalid node info id: " & $nodeInfoId)
  return ok(ctx.myLib[].waku.stateInfo.getNodeInfoItem(infoItemIdEnum))

proc logosdelivery_get_available_configs(
    ctx: ptr FFIContext[LogosDelivery], callback: FFICallBack, userData: pointer
) {.ffi.} =
  ## Information about the accepted config items.
  requireInitializedMessaging(ctx, "logosdelivery_get_available_configs"):
    return err(errMsg)
  let optionMetas: seq[ConfigOptionMeta] = extractConfigOptionMeta(WakuNodeConf)
  var configOptionDetails = newJArray()
  for meta in optionMetas:
    configOptionDetails.add(
      %*{
        meta.fieldName: meta.typeName & "(" & meta.defaultValue & ")", "desc": meta.desc
      }
    )
  var jsonNode = newJObject()
  jsonNode["configOptions"] = configOptionDetails
  return ok(pretty(jsonNode))
