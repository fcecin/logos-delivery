import brokers/event_broker
import logos_delivery/waku/[api/types, waku_core/message, waku_core/topics]
from logos_delivery/api/messaging_client_interface import
  MessageSentEvent, MessageErrorEvent, MessagePropagatedEvent, MessageReceivedEvent
export
  types, MessageSentEvent, MessageErrorEvent, MessagePropagatedEvent,
  MessageReceivedEvent

EventBroker:
  # Internal event emitted when a message arrives from the network via any protocol
  type MessageSeenEvent* = object
    topic*: PubsubTopic
    message*: WakuMessage
