import brokers/event_broker

import logos_delivery/api/types as api_types

export event_broker, api_types

EventBroker:
  # Event emitted when a store node, or the mix exit for a send over mix, confirms
  # a message. Only with the `reliability` option on, and never for an ephemeral
  # message. `anonymous` is true when the message went out over mix and is not
  # deanonymized: the node never attempted to send it in the clear.
  type MessageSentEvent* = object
    requestId*: RequestId
    messageHash*: string
    anonymous*: bool

EventBroker:
  # Event emitted when a send over mix ends with no confirmation in its window.
  # Its request left this node, and the message can be on the network. The node
  # does not send it again. `anonymous` is false when the message is
  # deanonymized: the node attempted to send it in the clear.
  type MessageUnconfirmedEvent* = object
    requestId*: RequestId
    messageHash*: string
    anonymous*: bool

EventBroker:
  # Event emitted when a message send operation fails
  type MessageErrorEvent* = object
    requestId*: RequestId
    messageHash*: string
    error*: string

EventBroker:
  # Event emitted when a send is held back because the epoch's rate-limit budget
  # is spent. The message stays queued and is sent once the budget refills.
  type MessageQueuedEvent* = object
    requestId*: RequestId
    messageHash*: string

EventBroker:
  # Confirmation that a message has been correctly delivered to some neighbouring nodes.
  # `anonymous` is true when the message went out over mix and is not
  # deanonymized: the node never attempted to send it in the clear.
  type MessagePropagatedEvent* = object
    requestId*: RequestId
    messageHash*: string
    anonymous*: bool

EventBroker:
  # Event emitted when either a message belongs to Live communication or
  # recovered from Store. The source field has this information.
  type MessageReceivedEvent* = object
    messageHash*: string
    message*: WakuMessage
    source*: MessageSource
