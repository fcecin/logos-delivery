## Opaque handle to a live reliable channel. Holds the owning manager + the
## channel id so the channel ops (send / close) need no other context. Only its
## uint64 id crosses the FFI boundary; the object stays in the ctx registry.
type ReliableChannelHandle {.ffiHandle.} = ref object
  manager: ReliableChannelManager
  channelId: ChannelId

proc channel_create*(
    self: LogosDelivery, channelId: string, contentTopic: string, senderId: string
): Future[Result[ReliableChannelHandle, string]] {.ffi.} =
  ## Creates a reliable channel and returns a handle to it. The send handler and
  ## rng come from the manager; encryption providers are installed separately.
  let id = self.reliableChannelManager.createReliableChannel(
    ChannelId(channelId), ContentTopic(contentTopic), SdsParticipantID(senderId)
  ).valueOr:
    return err(error)
  return ok(ReliableChannelHandle(manager: self.reliableChannelManager, channelId: id))

proc channel_close*(ch: ReliableChannelHandle): Future[Result[string, string]] {.ffi.} =
  ## Stops the channel's SDS loops and deregisters it from the manager.
  ## Persisted SDS state survives, so re-creating the channel restores it.
  (await ch.manager.closeChannel(ch.channelId)).isOkOr:
    return err(error)
  return ok("")
