## Waku API: node health and connectivity broker request types.

import brokers/request_broker
import
  waku/node/health_monitor/[
    protocol_health, topic_health, health_report, connection_status
  ]
import waku/waku_core/topics
import waku/common/waku_protocol

export protocol_health, topic_health, connection_status

# Overall node connectivity status.
RequestBroker(sync):
  type RequestConnectionStatus* = object
    connectionStatus*: ConnectionStatus

# Health of a set of content topics.
RequestBroker(sync):
  type RequestContentTopicsHealth* = object
    contentTopicHealth*: seq[tuple[topic: ContentTopic, health: TopicHealth]]

  proc signature(topics: seq[ContentTopic]): Result[RequestContentTopicsHealth, string]

# Consolidated node health report.
RequestBroker:
  type RequestHealthReport* = object
    healthReport*: HealthReport

# Health of a set of shards (pubsub topics).
RequestBroker(sync):
  type RequestShardTopicsHealth* = object
    topicHealth*: seq[tuple[topic: PubsubTopic, health: TopicHealth]]

  proc signature(topics: seq[PubsubTopic]): Result[RequestShardTopicsHealth, string]

# Health of a mounted protocol.
RequestBroker:
  type RequestProtocolHealth* = object
    healthStatus*: ProtocolHealth

  proc signature(protocol: WakuProtocol): Future[Result[RequestProtocolHealth, string]]
