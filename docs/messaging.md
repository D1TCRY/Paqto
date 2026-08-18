# Messaging, request/reply, and acknowledgements

Paqto messages are volatile application envelopes sent over a READY protocol
session. Paqto validates the envelope and session identities, but it does not
interpret the payload or define what application success means.

## `Message`

`Message` is a mutable, slotted dataclass with these fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `payload: Any` | required | Application-defined content. |
| `type: str` | `"message"` | Routing key; must be a non-empty string when sent or received. |
| `sender: str \| None` | `None` | Logical sender. Node send/reply methods populate it. |
| `recipient: str \| None` | `None` | Logical destination; incoming non-null values must match the local peer. |
| `headers: dict[str, str]` | empty | Generic string-to-string metadata. |
| `id: str` | random UUID hex | Unique correlation identifier; must be non-empty. |
| `created_at: datetime` | current UTC time | Local construction time. The example serializer does not transmit it. |
| `reply_to: str \| None` | `None` | Request id answered by this message. |

The serializer owns the entire wire representation. Paqto requires
`serialize()` to return `bytes` and `deserialize()` to return a `Message`.
Unexpected serializer errors are wrapped as `SerializationError`.

## Sending

```python
sent = await node.send(
    target,
    {"reading": 12.5},
    type="measurement",
    headers={"schema": "v1"},
    timeout=5,
)
print(sent.id)
```

`target` is a `DiscoveredPeer` or a known `Peer`. The node establishes or
reuses the canonical READY session, builds the envelope, serializes it, applies
the negotiated message-size limit, and queues the frame on that connection's
writer.

For `send()`, an explicit `timeout` is used for connection establishment and
for waiting until the writer has handed the frame to the transport. If omitted,
`connect_timeout` and `send_timeout` apply separately.

A send timeout or cancellation does not prove that no bytes were transmitted.
The frame may already have reached the writer or operating-system stream.

## Handler registration and routing

```python
@node.on_message("measurement")
async def store_in_application_memory(message: Message) -> None:
    ...


@node.on_message()  # every ordinary, non-reply application message
def observe(message: Message) -> None:
    ...
```

For each message, handlers registered for its exact type run first, followed by
catch-all handlers. Each list retains registration order. A handler exception
becomes `MessageRoutingError`, emits `HANDLER_ERROR`, and follows the configured
`HandlerErrorPolicy`.

Messages carrying `reply_to` are correlation traffic and are consumed before
the router, including unknown or late replies. ACK, PING, and PONG frames are
protocol controls and never become `Message` objects.

## Request/reply

The responder uses `reply()` on the inbound `Message`:

```python
@responder.on_message("query")
async def query(message: Message) -> None:
    await responder.reply(
        message,
        {"value": 42},
        type="query-result",
    )
```

The requester waits for the complete response envelope:

```python
try:
    response = await requester.request(
        target,
        {"key": "example"},
        type="query",
        timeout=5,
    )
except RequestTimeoutError:
    ...
else:
    print(response.payload, response.reply_to)
```

The request lifecycle is:

1. create a normal `Message` with a unique id;
2. create a Future and register it by that id before sending;
3. bind the pending entry to the intended peer and exact `Connection`;
4. send the request;
5. complete only when a reply's `reply_to`, connection, and READY peer all
   match;
6. remove the pending entry in every terminal path.

The `request(timeout=...)` value controls the reply wait. Connection and send
stages use `connect_timeout` and `send_timeout`. If `timeout` is omitted,
`request_timeout` controls the reply wait.

`reply()` normally uses the exact connection stored in the current handler
context. Outside a handler, it can use the unique READY connection for
`message.sender`; it raises `RequestError` if none exists or if the selection is
ambiguous.

Pending requests are removed after success, timeout, caller cancellation,
send/serialization failure, disconnect, or node shutdown. A timeout raises
`RequestTimeoutError`; connection loss or shutdown raises `RequestError`.
Well-formed late, duplicate, unknown, wrong-peer, or wrong-connection replies
are ignored and are not dispatched as new application work.

Cancellation is local. Paqto does not send a cancellation control message to
the responder.

## Delivery and acknowledgement semantics

Technical ACK support is negotiated with capability `paqto.ack.v1` and is
enabled by default. Waiting for it is opt-in:

```python
await node.send(
    target,
    payload,
    type="event",
    require_ack=True,
    acknowledgement_timeout=2,
)
```

An ACK is sent after the remote reader has:

1. received an application frame within the negotiated size limit;
2. deserialized it to `Message`;
3. validated envelope shape, sender/session identity, and recipient.

The ACK is sent before application dispatch and may be emitted before the
bounded inbound queue admits the message. Later queue rejection, cancellation,
shutdown, or handler failure does not retract it.

These stages are distinct:

| Stage | What it means | What it does not mean |
| --- | --- | --- |
| Transport write | Bytes were handed to the local transport. | Remote receipt. |
| Paqto ACK | Remote Paqto received, deserialized, and validated the envelope. | Handler completion, persistence, or application success. |
| Application response | Whatever success contract the application explicitly defines. | A framework-level transaction or durability guarantee. |

If ACK support was not negotiated, `require_ack=True` raises
`AcknowledgementUnavailableError` before sending the application frame. A
deadline raises `AcknowledgementTimeoutError`; disconnect or shutdown raises
`AcknowledgementError`. Pending ACK state is exact-connection scoped and always
cleaned up.

Paqto does not deduplicate acknowledged messages and does not provide
exactly-once, at-least-once, durable, transactional, or automatically retried
delivery. Applications must design their own idempotency, durable state, retry,
and success response when needed.

See [Reliability](reliability.md) for queue and reconnect behavior.

