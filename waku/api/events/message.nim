import brokers/event_broker
import waku/[waku_core/message, waku_core/topics]

EventBroker:
  # Emitted when a message arrives from the network via any protocol
  type MessageSeenEvent* = object
    topic*: PubsubTopic
    message*: WakuMessage
