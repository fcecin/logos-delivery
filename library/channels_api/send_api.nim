proc channel_send*(
    ch: ReliableChannelHandle, payload: seq[byte], ephemeral: bool
): Future[Result[string, string]] {.ffi.} =
  ## Sends `payload` on the reliable channel. Routes through the messaging
  ## layer (ReliableChannelManager.send -> MessagingClient.send); returns the
  ## channel-layer request id.
  let requestId = (await ch.manager.send(ch.channelId, payload, ephemeral)).valueOr:
    return err(error)
  return ok($requestId)
