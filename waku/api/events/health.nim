import brokers/event_broker

import waku/node/health_monitor/connection_status  # ConnectionStatus
import waku/node/health_monitor/[protocol_health, topic_health]
import waku/waku_core/topics

export protocol_health, topic_health

# Notify health changes to node connectivity
EventBroker:
  type EventConnectionStatusChange* = object
    connectionStatus*: ConnectionStatus

# Notify health changes to a subscribed content topic. A content topic's health
# is its shard's health.
EventBroker:
  type EventContentTopicHealthChange* = object
    contentTopic*: ContentTopic
    health*: TopicHealth

# Notify health changes to a shard (pubsub topic)
EventBroker:
  type EventShardTopicHealthChange* = object
    topic*: PubsubTopic
    health*: TopicHealth
