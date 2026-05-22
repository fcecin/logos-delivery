{.push raises: [].}

## Messaging API event types. Re-exports the waku-tier event types too.

import brokers/event_broker
import waku/waku_core
import waku/api/events/message
import waku/api/events/health
import ./types

export message
export health
export types

EventBroker:
  # Emitted when a message is sent to the network.
  type MessageSentEvent* = object
    requestId*: RequestId
    messageHash*: string

EventBroker:
  # Emitted when a message send operation fails.
  type MessageErrorEvent* = object
    requestId*: RequestId
    messageHash*: string
    error*: string

EventBroker:
  # Emitted when a message is delivered to neighbouring nodes.
  type MessagePropagatedEvent* = object
    requestId*: RequestId
    messageHash*: string

EventBroker:
  # Emitted when a message is received via Waku.
  type MessageReceivedEvent* = object
    messageHash*: string
    message*: WakuMessage

{.pop.}
