import results, chronos, chronicles
import brokers/broker_context
import logos_delivery/waku/[waku_core], logos_delivery/waku/waku_lightpush/[common, rpc]
import logos_delivery/waku/waku, logos_delivery/waku/api/publish
import logos_delivery/waku/requests/health_requests
import logos_delivery/api/types
import ./[delivery_task, send_processor]

logScope:
  topics = "send service relay processor"

type RelaySendProcessor* = ref object of BaseSendProcessor
  waku: Waku
  publishProc: PushMessageHandler
  fallbackStateToSet: DeliveryState

proc new*(
    T: typedesc[RelaySendProcessor],
    lightpushAvailable: bool,
    publishProc: PushMessageHandler,
    waku: Waku,
    brokerCtx: BrokerContext,
): RelaySendProcessor =
  let fallbackStateToSet =
    if lightpushAvailable:
      DeliveryState.FallbackRetry
    else:
      DeliveryState.FailedToDeliver

  return RelaySendProcessor(
    waku: waku,
    publishProc: publishProc,
    fallbackStateToSet: fallbackStateToSet,
    brokerCtx: brokerCtx,
  )

proc isTopicHealthy(self: RelaySendProcessor, topic: PubsubTopic): bool {.gcsafe.} =
  let healthReport = RequestShardTopicsHealth.request(self.brokerCtx, @[topic]).valueOr:
    debug "isTopicHealthy: failed to get health report", topic = topic, error = error
    return false

  if healthReport.topicHealth.len() < 1:
    debug "isTopicHealthy: no topic health entries", topic = topic
    return false
  let health = healthReport.topicHealth[0].health
  debug "isTopicHealthy: topic health is ", topic = topic, health = health
  return health == MINIMALLY_HEALTHY or health == SUFFICIENTLY_HEALTHY

method isValidProcessor*(
    self: RelaySendProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  # Topic health query is not reliable enough after a fresh subscribe...
  # return self.isTopicHealthy(task.pubsubTopic)
  return true

method canAttempt*(
    self: RelaySendProcessor, task: DeliveryTask
): bool {.gcsafe, raises: [].} =
  return self.waku.relayHasPeers(task.pubsubTopic)

method sendImpl*(self: RelaySendProcessor, task: DeliveryTask) {.async.} =
  # GossipSub gives a published message to the local handlers before it looks
  # for peers. Without a peer, a publish gives the message to this node only.
  if not self.waku.relayHasPeers(task.pubsubTopic):
    debug "No relay peer for the shard, relay does not publish",
      requestId = task.requestId, shard = task.pubsubTopic
    task.state = self.fallbackStateToSet
    return

  task.tryCount.inc()
  debug "Trying message delivery via Relay",
    requestId = task.requestId,
    msgHash = task.msgHash.to0xHex(),
    tryCount = task.tryCount

  # Relay also gives a published message to the local handlers of this node.
  task.relayPublished = true

  let noOfPublishedPeers = (await self.publishProc(task.pubsubTopic, task.msg)).valueOr:
    let errorMessage = error.desc.get($error.code)
    debug "Failed to publish message with relay",
      request = task.requestId, msgHash = task.msgHash.to0xHex(), error = errorMessage

    if error.isRlnRejection():
      task.parkForRlnProofRefresh(self.waku, errorMessage)
      return

    if error.code != LightPushErrorCode.NO_PEERS_TO_RELAY:
      task.state = DeliveryState.FailedToDeliver
      task.errorDesc = errorMessage
    else:
      # The publish gave the message to the local handlers.
      task.timestampFixed = true
      task.state = self.fallbackStateToSet
    return

  if noOfPublishedPeers > 0:
    debug "Message propagated via Relay",
      requestId = task.requestId,
      msgHash = task.msgHash.to0xHex(),
      noOfPeers = noOfPublishedPeers
    task.state = DeliveryState.SuccessfullyPropagated
    task.deliveryTime = Moment.now()
    if task.firstPropagatedTime.isNone():
      task.firstPropagatedTime = Opt.some(Moment.now())
  else:
    # It shall not happen, but still covering it
    task.timestampFixed = true
    task.state = self.fallbackStateToSet
