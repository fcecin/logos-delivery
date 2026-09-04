# Sender anonymity in the Messaging API

The Messaging API can send a message through the mix network. The mix network hides the address of the sender from the node that publishes the message. The setting `anonymityLevel` of the messaging configuration selects this behavior for a node.

## The setting

| Route | Form |
|---|---|
| Nim API | `MessagingClientConf(anonymityLevel: Opt.some(AnonymityLevel.Required))`, given as `messagingOverrides` to `LogosDelivery.new` |
| Structured JSON configuration | `{"messagingOverrides": {"anonymityLevel": "Required"}}`. The key `"anonymity-level"` is also permitted. |
| Flat JSON configuration | `{"anonymityLevel": "Required"}` |

There is no command-line flag. The setting is part of the Messaging API, and the node application does not have it.

One rule connects the setting to the node: a level above `None` requires the mix protocol. Every input route applies it, and every constructor applies it again: the node mounts mix, a configuration that sets `mix: false` next to such a level is an error, and so is an IPv6 listen address, because mix routes IPv4 addresses only. The kernel entry layer has no send path, so a level above `None` on that layer is an error too.

## The levels

| Level | Send path | When mix cannot deliver | Time to a failure report |
|---|---|---|---|
| `None` (default) | Relay, then lightpush | No change from a node without the setting | 60 s |
| `Preferred` | Mix for the first 60 s, then relay and lightpush | Clear text only for a message that mix never got out of the node. See the rule below. | 120 s |
| `Required` | Mix only | The send fails at the deadline. The plain path is not built for this node. | 60 s |

The rule of `Preferred`: the message goes in clear text only when no mix request for it left the node. That is the case when there is no exit node, when the pool is short, when the message is too large for a mix packet, and when an attempt failed before its write (the path could not be built, the first hop refused the connection). Once a request left the node, or may have left it, the message completes through mix or fails at the deadline; it is never sent in clear text. An exit node that refused the request has seen the message, so a refusal counts as a request that left. A copy in clear text would connect the sender to a message that a mix node has seen.

## What one attempt says

Each mix attempt ends with a certainty and a reason. The certainty is what the node may conclude about the message. The reason is what happened.

| Certainty | Meaning |
|---|---|
| `NotSent` | The message is not on the network: no request left the node, or the exit node answered that it did not publish. |
| `MaybeSent` | The request was written to the mix connection, or may have been, and no readable answer came back. The exit node may have published the message. |
| `Confirmed` | The exit node answered that it published the message. |

| Reason | Certainty | What the send service does |
|---|---|---|
| no exit node; pool below the minimum | `NotSent` | retries each round |
| message too large for a packet next to one return path | `NotSent` | `Preferred` sends in clear text at once; `Required` fails at once |
| the path could not be built, the first hop refused, the write failed | `NotSent` | retries with another exit node |
| the write happened, no reply within the limit | `MaybeSent` | retries with another exit node; never clear text |
| the write happened, the reply unreadable | `MaybeSent` | the same |
| the exit node answered and did not publish (an RLN rejection, no relay peer, too many requests) | `NotSent` | an RLN rejection waits for a proof refresh; the others retry with another exit node; never clear text |
| the exit node accepted | `Confirmed` | the message is propagated |

The certainty comes from the side of the write where a failure fell, which the node observes itself. No error text is read to decide it.

The send path reports delivery with the events `MessagePropagated`, `MessageSent` and `MessageError`. For a message that the exit node confirmed, `MessageSent` follows `MessagePropagated` at once, without a store confirmation: the send service does not confirm such a message with a store node, because a store query with the message hash would show the sender to the store node. Store-based reliability does not apply to a mixed send. The events do not name the path that delivered a message; the INFO log of the node does.

A `Required` node reports `MessageError` at the deadline when mix does not deliver. It does not send the message without mix. The error says "delivery unconfirmed: the message may have left this node, but no confirmation arrived before the deadline" when a request left the node or may have, followed by the reason of the last attempt. It says "Unable to send within retry time window" followed by the reason when no request ever left. A reliable channel cannot send through mix until the mix path divides messages: the SDS envelope of a channel message is about 19 KB whatever the payload, and a mix packet holds about 3 KB. A `Required` node fails a channel send at once, and a `Preferred` node sends it in clear text at once.

A stop of the messaging client cancels each attempt in flight and reports `MessageError` ("send service stopped") for each message that did not propagate, and no event for a message that propagated and waited for a store confirmation. A message whose attempt was cancelled keeps the rule of the levels: it is never sent in clear text by a later start. A send before the start waits in the cache, and the first round of the send service sends it after the start.

An Edge node with a level above `None` does not subscribe to the content topic inside `send`. A filter subscribe request goes in clear text from the node's own address to a filter service node, and a request one second before the first message on that topic connects the node to the message. The application subscribes before it sends, with a delay of its choice. A Core node keeps the subscription inside `send`: a relay publish needs the mesh, and a gossipsub subscription names the shard, not the topic.

The setting applies to the Messaging API send path only. The kernel API has relay and lightpush publish procedures that do not go through the send service. A message published through them goes in clear text, whatever the level.

## What a mixed send needs

A mixed send is a lightpush request that goes through two mix nodes (hops) to a lightpush server. The lightpush server is itself a mix node and is the third and last node of the path (the exit node). The node needs these conditions:

- The mix protocol is mounted on the node. The level does this. A node without an announced address cannot mount mix and does not start.
- The node knows four or more mix nodes: the exit node and three other nodes. The path takes two of the three, and the return path takes two of the three. The node learns mix nodes from the `mixnode` entries in its configuration (`multiaddr:mixPublicKey`; the field `mixnodes` of `MessagingClientConf`, the JSON key `mixnode`), from Kademlia service discovery, and from the rendezvous namespace `/mix`. Until the node knows four mix nodes, the send path retries each round and does not send.
- The `mixnode` and `lightpushnode` entries may name a TCP or a QUIC-v1 address; the node dials QUIC-v1 when its QUIC transport is enabled. A node without the QUIC transport accepts a QUIC-only entry at configuration and fails to dial it, with the reason in the log.
- The node has a lightpush service peer that is in the mix pool. The send path does not use a lightpush server without a mix key as an exit node. A `lightpushnode` from the configuration (the field `lightpushnode` of `MessagingClientConf`) is usable as soon as the node knows its mix key.

Each mix attempt waits a maximum of 5 s for the write of its request and the reply of the exit node. The mix connection gets a limit twice as long, so the send path's limit is the one that ends an attempt. After a failed attempt, the next attempt uses a different exit node when the node knows one, and the same one when it is the only one. The exit node to avoid is a fact of the task; nothing about an attempt persists across tasks or on the protocol. The send service retries each round; a retry runs as an attempt of its own, so an attempt that waits for a reply delays no other message and no deadline. At most four retries are in flight at a time.

A sphinx packet has a fixed size, and the mix path does not divide messages. With one return path, a message of more than about 3 KiB cannot go through mix. The check is exact, on the bytes that go to the mix connection, and runs before any write. A `Preferred` node gives such a message to the plain path at once. A `Required` node fails such a message at once.

## The node's role

A Core node with a level mounts relay and mix and serves the mix network as an intermediary and exit node, the default that the WAKU-MIX specification gives a relay node. It advertises its mix key in its ENR, its peer record and its service discovery record. An Edge node with a level is a sender only: it mounts the mix protocol, because the reply of the exit node arrives over that protocol, and it advertises nothing. No node learns its mix key through discovery, so no path goes through it. The specification says the same: an Edge node with short connection windows acts as a sender only. A node that mounts nothing sets no mix bit in its ENR, whatever a network preset says.

Until the mix library ships a sender-only mode, an Edge node with a level still processes intermediate and exit packets from a peer that holds its key by other means. The gate on this side is advertising only. The mode that drops such packets in the protocol is a change to the library.

A serving node sets `mixkey` in its kernel configuration, next to `nodekey`. Without it the node generates a new mix key at each start, and a peer that cached the old key through discovery builds paths the node cannot open until discovery refreshes the key; the sender of such a path sees a lost reply and nothing else. An Edge node with a level needs no `mixkey`: it advertises no key.

## A sender that nothing can dial

The reply of the exit node travels a return path that ends at the sender. The mix node before the sender delivers the reply by dialing the sender's address, and libp2p reuses an existing connection before it dials. A node that nothing can dial (a phone behind a carrier NAT) receives a reply only when the hop before it already holds a connection to it, which is the case when that hop was a first hop of an earlier send. Such a sender may deliver a message and still report it unconfirmed at the deadline. This is a stated limitation of this version, not a defect in it: the choice of the node that delivers the reply is a design decision of the mix protocol, with a privacy trade-off of its own, and it is proposed to the library and the specification separately.

## What the mix network hides

The exit node and the relay network get the message, but they do not get the address of the sender. The sender opens a connection to the first hop only. The reply of the exit node goes back on a single-use return path. There is no cover traffic. A party that sees the full network can try to correlate the times of packets on a network with little traffic. Four mix nodes are the minimum for a path, not an anonymity floor: an adversary who runs two of the three nodes that are not the exit node sees both hops of a third of the packets. The anonymity set is the pool minus the adversary's nodes. Mix hides the transport identity only. The message carries what the application puts in it: a reliable channel puts the participant id in its envelope, the timestamp is the sender's clock at nanosecond precision, and the meta field carries the wire-format marker. A node that mounts mix puts its mix key into its rendezvous record, and the ENR of a Core node carries the mix capability, so a Core node that uses the setting is visible as a mix node. A sender that gets no replies (a sender behind NAT, for example) sees an RLN rejection by the exit node only as a missed deadline: the exit node publishes nothing, and the proof refresh does not run. A hop that is dead or holds a stale mix key drops the packet without a reply, and the sender cannot name it; with the minimum pool of four nodes, one such hop is in two paths of three, and each affected attempt costs the reply time limit.

A Core node that mounts mix also forwards packets for other nodes. It shows its mix key to the network. The mix key changes at each restart unless `mixkey` is set. Peers that have the previous key cannot route packets through the node until discovery gives them the new key.
