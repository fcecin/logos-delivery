import brokers/request_broker
import waku/node/health_monitor/topic_health
import waku/waku_core/topics

export topic_health

# Edge filter health for a single shard, folded into RequestShardTopicsHealth by its provider.
RequestBroker(sync):
  type RequestEdgeShardHealth* = object
    health*: TopicHealth

  proc signature(shard: PubsubTopic): Result[RequestEdgeShardHealth, string]

# Edge filter confirmed peer count. WakuSubscriptionManager sets it; health_monitor reads it.
RequestBroker(sync):
  type RequestEdgeFilterPeerCount* = object
    peerCount*: int
