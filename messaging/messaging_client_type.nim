{.push raises: [].}

## MessagingClient type definition.
## Addressed by its broker context; all kernel interaction goes through the
## broker surface (waku/api).

import bearssl/rand
import brokers/broker_context
import messaging/delivery_service/delivery_service

type MessagingClient* = ref object
  brokerCtx*: BrokerContext
  rng*: ref HmacDrbgContext
    ## RNG for request-id generation
  preferP2PReliability*: bool
  deliveryService*: DeliveryService
  relayMounted*: bool
    ## Cached at `start`: is the relay protocol mounted?
  filterMounted*: bool
    ## Cached at `start`: is the filter client mounted?

{.pop.}
