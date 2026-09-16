## HTTP REST API

The HTTP REST API consists of a set of methods operating on the Waku Node remotely over HTTP.

This API is divided in different _namespaces_ which group a set of resources:

| Namespace | Description |
------------|--------------
| `/debug` | Information about a Waku v2 node. |
| `/relay` | Control of the relaying of messages. See [11/WAKU2-RELAY](https://rfc.vac.dev/spec/11/) RFC |
| `/store` | Retrieve the message history. See [13/WAKU2-STORE](https://rfc.vac.dev/spec/13/) RFC |
| `/filter` | Control of the content filtering. See [12/WAKU2-FILTER](https://rfc.vac.dev/spec/12/) RFC |
| `/admin` | Privileged access to the internal operations of the node. |
| `/private` | Provides functionality to encrypt/decrypt `WakuMessage` payloads using either symmetric or asymmetric cryptography. This allows backwards compatibility with Waku v1 nodes. |
| `/messaging` | The Messaging API: subscribe and send by content topic, and poll the send and received events. See [Messaging API](#messaging-api). |


### API Specification

The HTTP REST API has been designed following the OpenAPI 3.0.3 standard specification format.
The OpenAPI specification files can be found in the [Logos Delivery REST API Reference](https://github.com/logos-messaging/logos-delivery-rest-api) repository.

You can also use the [hosted OpenAPI UI](https://logos-messaging.github.io/logos-delivery-rest-api/) to explore and execute the calls locally.

Check the [OpenAPI Tools](https://openapi.tools/) site for the right tool for you (e.g. REST API client generator)

A particular OpenAPI spec can be easily imported into [Postman](https://www.postman.com/downloads/)
  1. Open Postman.
  2. Click on File -> Import...
  2. Load the openapi.yaml of interest, stored in your computer.
  3. Then, requests can be made from within the 'Collections' section.


### Usage example

#### [`get_waku_v2_debug_v1_info`](https://rfc.vac.dev/spec/16/#get_waku_v2_debug_v1_info)

```bash
curl http://localhost:8645/debug/v1/info -s | jq
```

### Store API

The `page_size` flag in the Store API has a default value of 20 and a max value of 100.

### Messaging API

The `/messaging/v1` routes expose the Messaging API layer over REST. They are mounted only
when the node runs the messaging layer (`--entry-layer=messaging` or `--entry-layer=channels`;
a kernel-only node answers 404 with that hint), and content topics resolve to shards through
autosharding, so the node needs a network preset (`--preset=logos.test`, ...) or
`--cluster-id` with `--num-shards-in-network`.

```bash
# a service node: runs the message archive that confirms sends and serves backfill
logosdeliverynode --entry-layer=messaging --mode=core --preset=logos.test \
  --rest=true --rest-address=0.0.0.0 --rest-port=8645 --store=true
# a client node: names the Store peer that confirms its sends (or relies on discovery)
logosdeliverynode --entry-layer=messaging --mode=core --preset=logos.test \
  --rest=true --rest-address=0.0.0.0 --rest-port=8645 --storenode=<multiaddr>
```

| Method and route | Body | Response |
|---|---|---|
| `POST /messaging/v1/subscriptions` | `["/app/1/topic/proto", ...]` | `200 OK` |
| `DELETE /messaging/v1/subscriptions` | `["/app/1/topic/proto", ...]` | `200 OK` |
| `POST /messaging/v1/messages` | `{"payload":"<base64>","contentTopic":"/app/1/topic/proto","ephemeral":false,"meta":"<base64>"}` | `{"requestId":"..."}` |
| `GET /messaging/v1/events/send` | | every buffered send status, then cleared |
| `GET /messaging/v1/events/send/{requestId}` | | the send status of one request, then cleared; `404` while nothing is buffered for it |
| `GET /messaging/v1/events/received` | | the buffered received messages, oldest first, then cleared; each record carries `seq`; the `X-Messaging-Dropped` header counts what was evicted since the previous poll |

A send is asynchronous: `200` means accepted, and the outcome arrives as send events
correlated by `requestId`. `propagated` means the message reached at least one peer,
`sent` that a Store peer confirmed it holds the message (needs a Store peer and
store-based reliability, which the network preset decides: on for `logos.dev` and
`logos.test`, off for `twn` and `status.prod`, on by default without a preset; the CLI has
no switch for it, library and JSON configs use `reliability`; never emitted for ephemeral
messages), and `error` is terminal (rejected message, no peers within the retry window, ...).
Clients must ignore kinds they do not know. `propagated` can be the last event: for an
ephemeral message, on a node whose preset has store-based reliability off, and for a
message no Store peer confirms within about 60 s (dropped with a metric only), so a client
waiting for `sent` must apply a deadline of its own and count what is still unresolved as
unconfirmed. A `404` on `GET /events/send/{requestId}` means nothing is buffered for that
id right now: unknown, already polled, or not fired yet, so keep polling until `sent` or
`error`, or until that deadline.

Received records carry the message hash, the full `WakuMessage` and a `source`: `live`
for a message that arrived as it was published, `history` for one recovered from a Store
peer at startup or after a connectivity gap. Only content topics subscribed through
`/messaging/v1/subscriptions` are buffered; relaying a shard is not enough. Sending
auto-subscribes the node to the content topic, so a sender also sees its own messages.

Both observation surfaces are evict-after-poll. The received buffer keeps the newest
`--rest-messaging-cache-capacity` messages (default 50) between polls and drops the
oldest when full. Overflow is never silent: every received record carries a monotonic
`seq` (from 1, without gaps, so a gap between two polls is the number lost), both
poll-all responses carry an `X-Messaging-Dropped` header with the count evicted since
the previous poll, the node counts evictions in
`logos_delivery_rest_received_dropped_total` and
`logos_delivery_rest_send_status_dropped_total`, and it logs a warning. A harness must
treat a non-zero count as its own observation loss, not the network's: poll faster or
raise the capacity, or count from the node's `Message received` log line or its
`logos_delivery_recv_messages_total{source=...}` metric, which lose nothing. Malformed bodies and content topics answer `400`; a node
without autosharding answers `503`.

### Node configuration
Find details [here](../operators/how-to/configure-rest-api.md)
