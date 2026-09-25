import brokers/event_broker

import logos_delivery/channels/types as channel_types

export event_broker, channel_types

EventBroker:
  type ChannelMessageReceivedEvent* = object
    channelId*: ChannelId
    senderId*: SdsParticipantID
    payload*: seq[byte]

EventBroker:
  ## The channel emits this event when all segments of a `send()` propagate.
  ## `requestId` is the value that `send()` returns.
  type ChannelMessageSentEvent* = object
    channelId*: ChannelId
    requestId*: RequestId

EventBroker:
  ## The channel emits this event when all segments of a `send()` are final and
  ## at least one segment failed before it propagated. An error for a segment
  ## that already propagated does not count.
  type ChannelMessageErrorEvent* = object
    channelId*: ChannelId
    requestId*: RequestId
    error*: string

EventBroker:
  ## Emitted when an inbound payload can no longer be reassembled: its segment
  ## set expired, was evicted under memory pressure, or failed its integrity
  ## check. There is no `requestId` -- the send was a peer's.
  type ChannelMessageLostEvent* = object
    channelId*: ChannelId
    payloadHash*: seq[byte]
    reason*: string
