{.push raises: [].}

## Waku API: Relay broker request types.

import std/options
import chronos
import brokers/[broker_context, request_broker]
import waku/waku_core/[message/message, topics/pubsub_topic]
import waku/waku_relay/protocol

export PublishOutcome

# RequestRelayPublish status fields:
# - publishError: wakuRelay.publish failure mode.
# - rlnProofFailed: RLN proof step refused to attach a proof.
# - validationFailed: validateMessage rejected the message pre-publish.
# - errorDesc: error description.
RequestBroker:
  type RequestRelayPublish* = object
    relayedPeerCount*: uint32
    publishError*: Option[PublishOutcome]
    rlnProofFailed*: bool
    validationFailed*: bool
    errorDesc*: string

  proc signature(
    pubsubTopic: PubsubTopic, wakuMessage: WakuMessage
  ): Future[Result[RequestRelayPublish, string]]

{.pop.}
