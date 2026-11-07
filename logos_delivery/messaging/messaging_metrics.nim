## Messaging layer counters.
##
## Label cardinality is bounded by construction: `recordReceived` takes the
## `MessageSource` enum rather than a string, so `source` can only ever be
## `live` or `history`. Four receive series in total, plus the send counters.
##
## nim-metrics appends `_total` to each counter name on export.

{.push raises: [].}

import metrics
import logos_delivery/api/types

declarePublicCounter logos_delivery_send_mix_unconfirmed,
  "mix sends that ended with no confirmation in their window"
declarePublicCounter logos_delivery_send_store_validation_timeouts,
  "messages propagated but failed without store-node validation within the retry window"
declarePublicCounter logos_delivery_send_mix_fallbacks,
  "Preferred sends that the node deanonymized, because mix could not carry them or its window ended"
declarePublicCounter logos_delivery_recv_messages,
  "messages delivered to the application, live from the network or history from Store",
  ["source"]
declarePublicCounter logos_delivery_recv_message_bytes,
  "payload bytes of the messages delivered to the application, by source", ["source"]

# A labelled series exists from its first increment. Start every source at zero,
# so the series are scraped before the first message and can be read at any time.
for source in MessageSource:
  logos_delivery_recv_messages.inc(0, labelValues = [$source])
  logos_delivery_recv_message_bytes.inc(0, labelValues = [$source])

proc recordMixUnconfirmed*() =
  logos_delivery_send_mix_unconfirmed.inc()

proc recordStoreValidationTimeout*() =
  logos_delivery_send_store_validation_timeouts.inc()

proc recordMixFallback*() =
  logos_delivery_send_mix_fallbacks.inc()

proc recordReceived*(source: MessageSource, payloadBytes: int) =
  logos_delivery_recv_messages.inc(labelValues = [$source])
  logos_delivery_recv_message_bytes.inc(payloadBytes.int64, labelValues = [$source])

{.pop.}
