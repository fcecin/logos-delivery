# Message Event Handling in LMAPI

## Overview

The liblogosdelivery library emits five types of message delivery events and one message receipt event that clients can listen to by registering a per-event callback with `logosdelivery_add_event_listener()`. The events are delivered under the wire names `onMessageQueued`, `onMessageSent`, `onMessageUnconfirmed`, `onMessagePropagated`, `onMessageError` and `onMessageReceived` (the JSON `eventType` inside each payload is `message_queued` / `message_sent` / `message_unconfirmed` / `message_propagated` / `message_error` / `message_received`).

## The `anonymous` field

Each send event (`message_queued`, `message_propagated`, `message_sent`, `message_unconfirmed` and `message_error`) has the field `anonymous`, with its value at the time of the event:

- `true`: the node attempted to send the message over mix, and it never attempted to send the message in the clear. This is a guarantee.
- `false`: no guarantee. The node attempted to send the message in the clear, also when that attempt sent nothing, or it attempted no send yet.

At the `Preferred` anonymity level, the node sends the message in the clear when mix cannot carry it, when it is too large for mix, or when its time for mix ends. A message that the node already sent over mix with no answer stays on mix. The field is `false` before the first send attempt, and it becomes `true` when the node attempts to send the message over mix. When an attempt in the clear starts, the field becomes `false` and stays `false`. A `message_error` can come after the node sent the message in the clear, and receivers can have that message.

## Event Types

### 1. message_sent
Emitted when the node confirms the delivery of a message that is not ephemeral, with the `reliability` option on. For a normal send, a store node confirms the delivery. For a send over mix, the reply of the mix exit confirms it. In that case, `message_sent` means that the network has the message, not that a store node has it. A copy of the message that this node receives also confirms it, and so does the refusal of a later copy as already seen. A message is deanonymized from the moment that the node attempts to send it in the clear, and a store node confirms a deanonymized message as for a normal send. For an ephemeral message, or with the `reliability` option off, `message_sent` does not come, and a successful send ends at `message_propagated`.

**JSON Structure:**
```json
{
  "eventType": "message_sent",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "anonymous": false
}
```

**Fields:**
- `eventType`: Always "message_sent"
- `requestId`: Request ID returned from the send operation
- `messageHash`: Hash of the message that was sent
- `anonymous`: see [The `anonymous` field](#the-anonymous-field)

### 2. message_propagated
Emitted when a message has been successfully propagated to neighboring nodes on the network.

**JSON Structure:**
```json
{
  "eventType": "message_propagated",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "anonymous": false
}
```

**Fields:**
- `eventType`: Always "message_propagated"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was propagated
- `anonymous`: see [The `anonymous` field](#the-anonymous-field)

### 3. message_error
Emitted when a send fails: rejected, retry window elapsed, parked too long, ephemeral dropped near the rate limit, or store validation timed out. A send over mix that got no answer ends with `message_unconfirmed` instead.

**JSON Structure:**
```json
{
  "eventType": "message_error",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "error": "error description",
  "anonymous": false
}
```

**Fields:**
- `eventType`: Always "message_error"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that failed
- `error`: Description of what went wrong
- `anonymous`: see [The `anonymous` field](#the-anonymous-field)

### 4. message_queued
Emitted when a send is held back because the current epoch's rate-limit budget is spent. The message is not rejected: it stays queued and goes out once the budget refills. Emitted at most once per send, on the first time the message is held back.

**JSON Structure:**
```json
{
  "eventType": "message_queued",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "anonymous": false
}
```

**Fields:**
- `eventType`: Always "message_queued"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was held back
- `anonymous`: see [The `anonymous` field](#the-anonymous-field)

### 5. message_received
Emitted once for every message accepted on a subscribed content topic, whether it arrived live from the network or was recovered from a Store peer (at startup, or after a connectivity gap). The `source` field tells the two apart.

**JSON Structure:**
```json
{
  "eventType": "message_received",
  "messageHash": "0x...",
  "message": {
    "payload": "base64...",
    "contentTopic": "/myapp/1/chat/proto",
    "version": 0,
    "timestamp": 1700000000000000000,
    "ephemeral": false,
    "meta": "",
    "proof": ""
  },
  "source": "live"
}
```

**Fields:**
- `eventType`: Always "message_received"
- `messageHash`: Hash of the received message
- `message`: The received message; `payload`, `meta` and `proof` are base64-encoded
- `source`: `"live"` when the message was delivered as it was published (relay or filter), `"history"` when it was recovered from Store

Duplicate suppression is best effort. The node remembers the hashes it has delivered for a few minutes, in memory only, so a live message that a Store check returns within that window is not reported again. After a restart, or when a Store recovery returns a message later than that, the same message can be reported a second time as `history`. Consumers that need exactly-once delivery should deduplicate by `messageHash`.

### 6. message_unconfirmed
Emitted when a send over mix ends with no confirmation. The node sent the message over mix, and the message can be on the network. The node can send it again over mix one time. Then it does not send it again, also not in the clear. Like `message_sent` and `message_error`, it is the last event of the send. It comes also with the `reliability` option off and for an ephemeral message. A send over mix gets `message_propagated` only with its confirmation, so an unconfirmed send gets no `message_propagated`.

**JSON Structure:**
```json
{
  "eventType": "message_unconfirmed",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "anonymous": true
}
```

**Fields:**
- `eventType`: Always "message_unconfirmed"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was sent
- `anonymous`: see [The `anonymous` field](#the-anonymous-field)

## Usage

### 1. Define an Event Callback

```c
void event_callback(int ret, const char *msg, size_t len, void *userData) {
    if (ret != RET_OK || msg == NULL || len == 0) {
        return;
    }

    // Parse the JSON message
    // Extract eventType field
    // Handle based on event type

    if (eventType == "message_queued") {
        // Handle message held back for rate-limit budget
    } else if (eventType == "message_sent") {
        // Handle message sent
    } else if (eventType == "message_propagated") {
        // Handle message propagated
    } else if (eventType == "message_error") {
        // Handle message error
    } else if (eventType == "message_unconfirmed") {
        // Handle a send over mix that ended with no confirmation
    } else if (eventType == "message_received") {
        // Handle message received; check "source" for "live" or "history"
    }
}
```

### 2. Register the Callback

Register the callback once per event name you want to receive. Each call returns a
listener id you can later pass to `logosdelivery_remove_event_listener(rawCtx, id)`.

The event API takes the raw context, which is the `ptr` field of the
`LogosDeliveryCtx` that `logosdelivery_ctx_create` hands to its callback.

```c
// ctx comes from the logosdelivery_ctx_create callback; see the README.
void *rawCtx = ctx->ptr;
logosdelivery_add_event_listener(rawCtx, "onMessageQueued", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageSent", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessagePropagated", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageError", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageUnconfirmed", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageReceived", event_callback, NULL);
```

### 3. Start the Node

Once the node is started, events will be delivered to your callback:

```c
logosdelivery_ctx_start_node(ctx, on_reply, userData);
```

## Event Flow

For a typical successful message send:

1. **send** → Returns request ID
2. **message_propagated** → Message delivered to peers
3. **message_sent** → A store node confirms the message, or, for a send over mix, the exit, a copy of the message or an already-seen refusal confirms it (only with the `reliability` option on)

For a send over mix that gets no confirmation:

1. **send** → Returns request ID
2. **message_unconfirmed** → The node sent the message over mix, and no confirmation came in the window

For a failed message send:

1. **send** → Returns request ID
2. **message_error** → Delivery failed with error description

For a send that no store node confirms (only with the `reliability` option on):

1. **send** → Returns request ID
2. **message_propagated** → Message delivered to peers
3. **message_error** → No store node confirmed the message within the validation window

The `on_reply` callback of `logosdelivery_ctx_send` gets the request ID before the event thread gets an event of that request. When the send queue is full, `logosdelivery_ctx_send` returns an error and no request ID. A send of a message that the send queue already has gets the request ID of the queued send.

## Important Notes

1. **Thread Safety**: The event callback is invoked from a dedicated event thread (separate from the FFI worker thread). Ensure your callback is thread-safe if it accesses shared state.

2. **Non-Blocking**: Keep the callback fast and non-blocking. Do not perform long-running operations in the callback.

3. **JSON Parsing**: The example uses a simple string-based parser. For production, use a proper JSON library like:
   - [cJSON](https://github.com/DaveGamble/cJSON)
   - [json-c](https://github.com/json-c/json-c)
   - [Jansson](https://github.com/akheron/jansson)

4. **Memory Management**: The message buffer is owned by the library. Copy any data you need to retain.

5. **Event Order**: Events are delivered in the order they occur, but timing depends on network conditions.

## Example Implementation

See `examples/liblogosdelivery_example.c` for a complete working example that:
- Registers an event callback
- Sends a message
- Receives and prints all five event types
- Properly parses the JSON event structure

## Debugging Events

To see all events during development:

```c
void debug_event_callback(int ret, const char *msg, size_t len, void *userData) {
    printf("Event received: %.*s\n", (int)len, msg);
}
```

This will print the raw JSON for all events, helping you understand the event structure.
