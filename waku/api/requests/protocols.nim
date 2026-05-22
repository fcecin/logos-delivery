{.push raises: [].}

## Waku API: kernel node-introspection broker types.
##
## Reports which optional protocol clients are mounted. Pure declarations; the
## provider is wired by WakuNode.startProvidersAndListeners.

import brokers/[broker_context, request_broker]

# Which optional protocol clients are currently mounted on the node.
RequestBroker(sync):
  type RequestProtocolMountStatus* = object
    relayMounted*: bool
    lightpushMounted*: bool
    filterMounted*: bool
    storeMounted*: bool

{.pop.}
