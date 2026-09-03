# Sender anonymity in the Messaging API

The Messaging API can send a message through the mix network. The mix network hides the address of the sender from the node that publishes the message. The setting `anonymityLevel` of the messaging configuration selects this behavior for a node.

## The setting

| Route | Form |
|---|---|
| Nim API | `MessagingClientConf(anonymityLevel: Opt.some(AnonymityLevel.Required))`, given as `messagingOverrides` to `LogosDelivery.new` |
| Structured JSON configuration | `{"messagingOverrides": {"anonymityLevel": "Required"}}`. The key `"anonymity-level"` is also permitted. |
| Flat JSON configuration | `{"anonymityLevel": "Required"}` |

There is no command-line flag. The setting is part of the Messaging API, and the node application does not have it.

A value other than `None` also mounts the mix protocol on the node. The send path can only use a mix that the node runs. Every constructor mounts mix for a level above `None`. A configuration that sets `mix: false` next to such a level is an error.

## The levels

| Level | Send path | When mix cannot deliver | Time to a failure report |
|---|---|---|---|
| `None` (default) | Relay, then lightpush | No change from a node without the setting | 60 s |
| `Preferred` | Mix for the first 60 s, then relay and lightpush | A message that did not propagate goes to the plain path after 60 s. Mix makes no attempt for that message again. | 120 s |
| `Required` | Mix only | The send fails at the deadline. The plain path is not built for this node. | 60 s |

The send path reports delivery with the events `MessagePropagated`, `MessageSent` and `MessageError`. For a message that went through mix, `MessageSent` follows `MessagePropagated` at once, without a store confirmation: the send service does not confirm such a message with a store node, because a store query with the message hash would show the sender to the store node. Store-based reliability does not apply to a mixed send. A reliable channel cannot send through mix until the mix path divides messages: the SDS envelope of a channel message is about 19 KB whatever the payload, and a mix packet holds about 3 KB. A `Required` node fails a channel send at once, and a `Preferred` node sends it in clear text at once. The events do not name the path that delivered a message; the INFO log of the node does.

A `Required` node reports `MessageError` at the deadline when mix does not deliver. It does not send the message without mix. A `Preferred` node does not send in clear text a message that it knows went out through mix. The evidence is the reply of the exit node, or, when the reply is lost, the sender's own copy of the message: a node that is subscribed to the content topic receives its own message from the network (an Edge node from its filter service node, a Core node from the relay mesh), and the send service marks the message propagated. After a mix attempt with no reply, a `Preferred` node waits 5 s for that copy before it uses the plain path. Without either evidence (a lost reply and no subscription, or a copy that arrives later than 5 s), a `Preferred` node sends the message in clear text after the window, with the same hash. The copy counts for a mix attempt only: a node that publishes on relay gets its own publish back from its local handlers before any peer sees it, and a filter service node that publishes pushes the copy back to its Edge client the same way. A mixed message leaves the send cache after the propagation.

A stop of the messaging client reports `MessageError` ("send service stopped") for each message that did not propagate, and no event for a message that propagated and waited for a store confirmation. The channels layer stops before the messaging client, so a channel request that waits at stop gets no channel-level event. A send before the start waits in the cache, and the first round of the send service sends it after the start.

An Edge node with a level above `None` does not subscribe to the content topic inside `send`. A filter subscribe request goes in clear text from the node's own address to a filter service node, and a request one second before the first message on that topic connects the node to the message. The application subscribes before it sends, with a delay of its choice. A Core node keeps the subscription inside `send`: a relay publish needs the mesh, and a gossipsub subscription names the shard, not the topic.

The setting applies to the Messaging API send path only. The kernel API has relay and lightpush publish procedures that do not go through the send service. A message published through them goes in clear text, whatever the level. The kernel entry layer has no send path, so a level above `None` on that layer is a configuration error. Mix routes IPv4 addresses only, so a level above `None` with an IPv6 listen address is a configuration error.

## What a mixed send needs

A mixed send is a lightpush request that goes through two mix nodes (hops) to a lightpush server. The lightpush server is itself a mix node and is the third and last node of the path (the exit node). The node needs these conditions:

- The mix protocol is mounted on the node. The level does this. A node without an announced address cannot mount mix and does not start.
- The node knows four or more mix nodes: the exit node and three other nodes. The path takes two of the three, and the return path takes two of the three. The node learns mix nodes from the `mixnode` entries in its configuration (`multiaddr:mixPublicKey`; the field `mixnodes` of `MessagingClientConf`, the JSON key `mixnode`), from Kademlia service discovery, and from the rendezvous namespace `/mix`. Until the node knows four mix nodes, the send path retries each round and does not send.
- The node has a lightpush service peer that is in the mix pool. The send path does not use a lightpush server without a mix key as an exit node. A `lightpushnode` from the configuration (the field `lightpushnode` of `MessagingClientConf`) is usable as soon as the node knows its mix key.

Each mix attempt waits a maximum of 5 s for the reply of the exit node. The mix connection gets a limit twice as long, so the send path's limit is the one that ends an attempt. After that, the send path retries in the next round. The next attempt uses a different exit node when the node knows one. The limit is short so that an attempt with no reply does not hold a message for long: first attempts run one future each, and retries run in batches of four, so an attempt with no reply delays the other retries of its batch only.

A sphinx packet has a fixed size, and the mix path does not divide messages. With one return path, a message of more than about 3 KiB cannot go through mix. A `Preferred` node gives such a message to the plain path at once. A `Required` node fails such a message at once.

## What the mix network hides

The exit node and the relay network get the message, but they do not get the address of the sender. The sender opens a connection to the first hop only. The reply of the exit node goes back on a single-use return path. There is no cover traffic. A party that sees the full network can try to correlate the times of packets on a network with little traffic. Four mix nodes are the minimum for a path, not an anonymity floor: an adversary who runs two of the three nodes that are not the exit node sees both hops of a third of the packets. The anonymity set is the pool minus the adversary's nodes. Mix hides the transport identity only. The message carries what the application puts in it: a reliable channel puts the participant id in its envelope, the timestamp is the sender's clock at nanosecond precision, and the meta field carries the wire-format marker. A node that mounts mix puts its mix key into its rendezvous record, and the ENR of a Core node carries the mix capability, so a node that uses the setting is visible as a mix node. A mode that mounts mix without advertising it is future work. A sender that gets no replies (a sender behind NAT, for example) sees an RLN rejection by the exit node only as a missed deadline: the exit node publishes nothing, so no copy comes back, and the proof refresh does not run. A hop that is dead or holds a stale mix key drops the packet without a reply, and the sender cannot name it; with the minimum pool of four nodes, one such hop is in two paths of three, and each affected attempt costs the reply time limit.

A node that mounts mix also forwards packets for other nodes. It shows its mix key to the network. The mix key changes at each restart unless `mixkey` is set. Peers that have the previous key cannot route packets through the node until discovery gives them the new key.
