# Message Event Handling in LMAPI

## Overview

The liblogosdelivery library emits five types of message delivery events and one message receipt event that clients can listen to by registering a per-event callback with `logosdelivery_add_event_listener()`. The events are delivered under the wire names `onMessageQueued`, `onMessageSent`, `onMessagePropagated`, `onMessageArchived`, `onMessageError` and `onMessageReceived` (the JSON `eventType` inside each payload is `message_queued` / `message_sent` / `message_propagated` / `message_error` / `message_received`).

A send has three milestones and one failure. `message_propagated` means the message reached neighbouring peers. `message_sent` means the send service considers it sent, which today requires a store node to confirm it. `message_archived` means a store node holds it, so an offline recipient can recover it later. `message_error` means the send failed.

## Event Types

### 1. message_sent
Emitted when the send service considers the message sent. Today this is when a store node confirms it holds the message, which requires reliability to be enabled and a non-ephemeral message. When no store confirmation will follow (reliability disabled, or an ephemeral message) the send ends at `message_propagated` and no `message_sent` is emitted.

**JSON Structure:**
```json
{
  "eventType": "message_sent",
  "requestId": "unique-request-id",
  "messageHash": "0x..."
}
```

**Fields:**
- `eventType`: Always "message_sent"
- `requestId`: Request ID returned from the send operation
- `messageHash`: Hash of the message that was sent

### 2. message_propagated
Emitted when a message has been successfully propagated to neighboring nodes on the network.

**JSON Structure:**
```json
{
  "eventType": "message_propagated",
  "requestId": "unique-request-id",
  "messageHash": "0x..."
}
```

**Fields:**
- `eventType`: Always "message_propagated"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was propagated

### 3. message_archived
Emitted when a store node confirms it holds the message: the durable milestone of a reliable send, after which an offline recipient can recover the message from Store. Today it follows `message_sent` for the same request.

**JSON Structure:**
```json
{
  "eventType": "message_archived",
  "requestId": "unique-request-id",
  "messageHash": "0x..."
}
```

**Fields:**
- `eventType`: Always "message_archived"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was archived

### 4. message_error
Emitted when the send fails: the message was rejected or could not be propagated within the retry window, or, with reliability enabled, a propagated non-ephemeral message was not confirmed by a store node within the store validation window (one minute from propagation). In the second case the message did reach the network and may have been received live; only its archival is unconfirmed. Do not read that error as "not delivered": resending would duplicate the message for online recipients.

**JSON Structure:**
```json
{
  "eventType": "message_error",
  "requestId": "unique-request-id",
  "messageHash": "0x...",
  "error": "error description"
}
```

**Fields:**
- `eventType`: Always "message_error"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that failed
- `error`: Description of what went wrong

### 5. message_queued
Emitted when a send is held back because the current epoch's rate-limit budget is spent. The message is not rejected: it stays queued and goes out once the budget refills. Emitted at most once per send, on the first time the message is held back.

**JSON Structure:**
```json
{
  "eventType": "message_queued",
  "requestId": "unique-request-id",
  "messageHash": "0x..."
}
```

**Fields:**
- `eventType`: Always "message_queued"
- `requestId`: Request ID from the send operation
- `messageHash`: Hash of the message that was held back

### 6. message_received
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
    } else if (eventType == "message_archived") {
        // Handle message archived by a store node
    } else if (eventType == "message_error") {
        // Handle message error
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
logosdelivery_add_event_listener(rawCtx, "onMessageArchived", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageError", event_callback, NULL);
logosdelivery_add_event_listener(rawCtx, "onMessageReceived", event_callback, NULL);
```

### 3. Start the Node

Once the node is started, events will be delivered to your callback:

```c
logosdelivery_ctx_start_node(ctx, on_reply, userData);
```

## Event Flow

For a send with reliability enabled (non-ephemeral message):

1. **send** → Returns request ID
2. **message_propagated** → Message delivered to neighbouring peers
3. **message_sent** → A store node confirmed it holds the message
4. **message_archived** → The same confirmation, as the durable milestone

For a send with reliability disabled, or an ephemeral message:

1. **send** → Returns request ID
2. **message_propagated** → Message delivered to neighbouring peers; no further event follows

For a failed send:

1. **send** → Returns request ID
2. **message_error** → The message could not be propagated, or (after **message_propagated**) no store node confirmed it within the store validation window

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
- Receives and prints all message event types
- Properly parses the JSON event structure

## Debugging Events

To see all events during development:

```c
void debug_event_callback(int ret, const char *msg, size_t len, void *userData) {
    printf("Event received: %.*s\n", (int)len, msg);
}
```

This will print the raw JSON for all events, helping you understand the event structure.
