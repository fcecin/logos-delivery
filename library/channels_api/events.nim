## Reliable-channel events: per-channel message received / sent / errored,
## fed by the channel-layer broker events.

proc onChannelMessageReceived*(
  channelId: string, senderId: string, payload: seq[byte]
) {.ffiEvent: "on_channel_message_received".}

proc onChannelMessageSent*(
  channelId: string, requestId: string
) {.ffiEvent: "on_channel_message_sent".}

proc onChannelMessageError*(
  channelId: string, requestId: string, error: string
) {.ffiEvent: "on_channel_message_error".}

proc listenChannelEvents(self: LogosDelivery) =
  let brokerCtx = self.waku.brokerCtx

  discard ChannelMessageReceivedEvent.listen(
    brokerCtx,
    proc(e: ChannelMessageReceivedEvent) {.async: (raises: []).} =
      onChannelMessageReceived(string(e.channelId), $e.senderId, e.payload),
  )
  discard ChannelMessageSentEvent.listen(
    brokerCtx,
    proc(e: ChannelMessageSentEvent) {.async: (raises: []).} =
      onChannelMessageSent(string(e.channelId), $e.requestId),
  )
  discard ChannelMessageErrorEvent.listen(
    brokerCtx,
    proc(e: ChannelMessageErrorEvent) {.async: (raises: []).} =
      onChannelMessageError(string(e.channelId), $e.requestId, e.error),
  )
