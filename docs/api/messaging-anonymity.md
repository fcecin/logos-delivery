# Sender anonymity in the Messaging API

The Messaging API can hide which node a message came from by sending it through
the mix network instead of publishing it directly. The behaviour is selected per
node with the `anonymityLevel` setting of the messaging configuration.

## Setting the level

| Route | Form |
|---|---|
| Programmatic (Nim) | `MessagingClientConf(anonymityLevel: Opt.some(AnonymityLevel.Required))` passed as `messagingOverrides` to `LogosDelivery.new` |
| Structured JSON | `{"messagingOverrides": {"anonymityLevel": "Required"}}` (`"anonymity-level"` is accepted too) |
| Legacy flat JSON | `{"anonymityLevel": "Required"}` |

There is deliberately no command-line flag: the setting belongs to the
Messaging API, and the node application does not expose it.

Any level other than `None` also mounts the mix protocol on the node, since the
send path can only route through a mix the node runs. Setting `mix: false`
alongside a level that needs mix is a configuration error.

## Levels

| Level | Send path | On failure | Reports failure after |
|---|---|---|---|
| `None` (default) | relay, then lightpush | as before this setting existed | 60 s |
| `BestEffort` | mix only for the first 60 s, then relay and lightpush | falls back to the plain path once the mix window is over, and never tries mix again for that message | 120 s |
| `Required` | mix only | keeps retrying mix; the plain path is never built for this node | 60 s |

Delivery is reported through the same `MessageSent`, `MessagePropagated` and
`MessageError` events as a plain send. A `Required` node whose mix path never
works fails every message with `MessageError` at the deadline rather than
sending it unmixed. A `BestEffort` node may put the same message on the wire
twice, once mixed and once in the clear, if the mixed attempt propagated but
its store validation kept failing into the fallback window.

## What a mixed send needs

A mixed send is a lightpush request routed over three mix hops to a lightpush
server that is itself a mix node and terminates the path. So the node needs:

- **Mix mounted locally.** Done automatically by the level. Mounting mix
  requires the node to have an announced address, otherwise it refuses to start.
- **At least four known mix nodes**: three hops plus the exit. Mix nodes are
  learned from static `mixnode` entries (`multiaddr:mixPublicKey`), from
  Kademlia service discovery and from the rendezvous `/mix` namespace. Until the
  pool is large enough, every round is retried and nothing is sent.
- **A lightpush service peer that is in the mix pool.** A lightpush server
  without a mix key is used by the plain path but never as a mix exit. A
  statically configured `lightpushnode` qualifies as soon as its mix key has
  been learned.

A sphinx packet is fixed-size and the mix path does not split messages, so a
message of more than a few KiB cannot go through mix at all: `BestEffort`
hands it to the plain path at once, `Required` fails it at once.

Each mix attempt waits at most 5 s for the exit's reply before it is retried on
the next round. That budget is short on purpose: the send service walks its
queued messages one at a time, so every other message on the node waits behind
a stalled one.

## What it does and does not hide

The lightpush server (the exit) and the relay network learn the message but not
the node that sent it: the sender only ever opens a connection to the first hop,
and the exit's reply travels back over a single-use return path. There is no
cover traffic, so an observer who can watch the whole network can still attempt
timing correlation on a quiet network.

Mounting mix also makes the node a mix hop and exit for others: it advertises
its mix key and forwards other nodes' packets. The mix key is regenerated on
every restart unless `mixkey` is set, so peers holding the old key will fail to
route through the node until discovery refreshes it.
