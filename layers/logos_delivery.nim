{.push raises: [].}

import chronos, chronicles, results
import brokers/broker_context

import waku/factory/waku
import layers/mounts
import messaging/messaging_client
import tools/confutils/cli_args

export messaging_client

logScope:
  topics = "logos-delivery"

type LogosDelivery* = ref object
  brokerCtx*: BrokerContext
  waku*: Waku
    ## Kernel layer. Always present.
  messaging*: MessagingClient
    ## Messaging layer. `nil` in the kernel-only composition.

# The composition is selected by the primary layer typedesc:
# `new(Waku, ...)` is kernel-only, `new(MessagingClient, ...)` is the full stack.

proc new*(
    T: type LogosDelivery, primary: typedesc[Waku], node: Waku
): Result[LogosDelivery, string] =
  ## Kernel-only. Waku is the primary.
  if node.isNil():
    return err("LogosDelivery.new(Waku): node is nil")
  mountLayer(Waku, node.brokerCtx).isOkOr:
    return err("mount Waku layer failed: " & error)
  ok(LogosDelivery(brokerCtx: node.brokerCtx, waku: node, messaging: nil))

proc new*(
    T: type LogosDelivery, primary: typedesc[MessagingClient], node: Waku
): Result[LogosDelivery, string] =
  ## Messaging primary. Mounts the kernel, then the messaging layer on top;
  ## rolls back the kernel gate if messaging fails to mount.
  if node.isNil():
    return err("LogosDelivery.new(MessagingClient): node is nil")
  mountLayer(Waku, node.brokerCtx).isOkOr:
    return err("mount Waku layer failed: " & error)
  let messaging = MessagingClient.new(node.brokerCtx, node.conf.p2pReliability)
  mountLayer(MessagingClient, messaging.brokerCtx).isOkOr:
    discard unmountLayer(Waku, node.brokerCtx)
    return err("mount MessagingClient layer failed: " & error)
  ok(LogosDelivery(brokerCtx: node.brokerCtx, waku: node, messaging: messaging))

proc new*(
    T: type LogosDelivery,
    primary: typedesc[MessagingClient],
    preset: string,
    mode: WakuMode,
): Future[Result[LogosDelivery, string]] {.async: (raises: []).} =
  ## Messaging primary, kernel built from `(preset, mode)` defaults.
  var conf = defaultWakuNodeConf().valueOr:
    return err("defaultWakuNodeConf failed: " & error)
  conf.preset = preset
  conf.mode = mode

  let wakuConf = conf.toWakuConf().valueOr:
    return err("toWakuConf failed: " & error)

  let w =
    try:
      (await Waku.new(wakuConf)).valueOr:
        return err("Waku.new failed: " & $error)
    except CatchableError as e:
      return err("Waku.new raised: " & e.msg)

  return LogosDelivery.new(MessagingClient, w)

proc start*(self: LogosDelivery): Future[Result[void, string]] {.async: (raises: []).} =
  ## Kernel first (its broker providers must be live before messaging queries
  ## protocol-mount status), then the messaging layer if present.
  if self.isNil() or self.waku.isNil():
    return err("LogosDelivery.start: delivery/waku is nil")

  (await startWaku(addr self.waku)).isOkOr:
    return err("startWaku failed: " & error)

  if not self.messaging.isNil():
    (await self.messaging.start()).isOkOr:
      return err("MessagingClient.start failed: " & error)

  ok()

proc stop*(self: LogosDelivery): Future[Result[void, string]] {.async: (raises: []).} =
  ## Tear down in reverse: messaging (if present) then the kernel, releasing
  ## each layer's gate. Best-effort: reports the first error.
  if self.isNil():
    return err("LogosDelivery.stop: delivery is nil")

  var firstErr = ""

  if not self.messaging.isNil():
    (await self.messaging.stop()).isOkOr:
      firstErr = "MessagingClient.stop failed: " & error
    let unmountMsgRes = unmountLayer(MessagingClient, self.messaging.brokerCtx)
    if unmountMsgRes.isErr() and firstErr.len == 0:
      firstErr = "unmount MessagingClient layer failed: " & unmountMsgRes.error

  if not self.waku.isNil():
    let stopRes = await self.waku.stop()
    if stopRes.isErr() and firstErr.len == 0:
      firstErr = "Waku.stop failed: " & stopRes.error
    let unmountRes = unmountLayer(Waku, self.waku.brokerCtx)
    if unmountRes.isErr() and firstErr.len == 0:
      firstErr = "unmount Waku layer failed: " & unmountRes.error

  if firstErr.len > 0:
    return err(firstErr)
  ok()

{.pop.}
