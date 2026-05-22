{.push raises: [].}

## Shared declarations for node telemetry metrics. Both relay-handler paths
## observe these collectors; declaring them once avoids a duplicate Prometheus
## registration.

import metrics

declarePublicCounter waku_node_messages, "number of messages received", ["type"]

declarePublicHistogram waku_histogram_message_size,
  "message size histogram in kB",
  buckets = [
    0.0, 1.0, 3.0, 5.0, 15.0, 50.0, 75.0, 100.0, 125.0, 150.0, 500.0, 700.0, 1000.0, Inf
  ]

{.pop.}
