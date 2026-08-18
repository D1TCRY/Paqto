# Events, logging, and error handling

Paqto exposes best-effort diagnostic events and structured standard-library
logging. Neither mechanism is a durable audit log, and neither carries
application payloads by default.

## Node events

Register a sync or async listener through the node:

```python
from paqto import NodeEvent, NodeEventType


@node.on_event(NodeEventType.CONNECTED)
async def connected(event: NodeEvent) -> None:
    print(event.peer_id, event.connection_id)


@node.on_event()
def all_events(event: NodeEvent) -> None:
    ...
```

`NodeEvent` is immutable and contains:

- `type`;
- `local_peer_id`;
- optional `peer_id` and opaque process-local `connection_id`;
- optional exception object in `error`;
- immutable copied generic `metadata`;
- timezone-aware UTC `occurred_at`.

Event metadata is a shallow immutable snapshot; nested mutable objects are not
deep-copied.

### Event types

| Event | When it is emitted |
| --- | --- |
| `PEER_DISCOVERED` | A newly remembered fresh peer is returned by node discovery. |
| `PEER_EXPIRED` | Node activity rechecks and removes an expired discovery observation. There is no background expiry loop. |
| `CONNECTING` | An explicit outgoing session attempt begins. |
| `CONNECTED` | A connection becomes the selected READY session; metadata includes `peer_id_authenticated`. |
| `AUTHENTICATED` | A READY connection's transport reports authentication; metadata separates `authenticated_peer_id`, mechanism, and logical-id binding. |
| `DISCONNECTED` | The canonical session ends. Explicit disconnect includes reason metadata. |
| `RECONNECTING` | A reconnect attempt begins. |
| `TRANSPORT_ERROR` | The listener/accept adapter reports an error; transient accept errors are retried while running. |
| `PROTOCOL_ERROR` | Handshake or READY session processing fails. |
| `HANDLER_ERROR` | Application routing fails; metadata includes message id/type, never payload. |
| `RESOURCE_LIMIT` | A configured queue, correlation, or connection limit rejects work. |

Events are placed on a bounded node queue and dispatched by one event worker.
They never backpressure network processing. If `max_event_queue` is full, the
event is dropped and Paqto logs a warning. A failing listener is isolated,
logged, and does not stop later listeners or the node.

Because events can be dropped and disappear at shutdown, applications needing
a durable audit trail must implement and validate their own sink.

`EventRouter` is public for standalone composition, but `node.on_event()` is
the normal interface. Its `dispatch()` returns a tuple of listener exceptions
instead of raising them.

## Logging

Paqto uses Python's `logging` hierarchy, primarily `paqto.core.node`. It does
not configure handlers or logging levels for the application.

Relevant records attach structured `extra` fields such as:

- `paqto_local_peer_id` and `paqto_peer_id`;
- `paqto_connection_id`;
- `paqto_message_id` and `paqto_message_type`;
- `paqto_error_type` and `paqto_event_type`;
- reconnect and authentication state where relevant.

Default Paqto log messages do not include payloads, headers, serialized bytes,
certificate bodies, credentials, or exception text. The event object may carry
the actual exception for an explicit observer. Identifiers and message types
can still be sensitive metadata: do not embed secrets in them, and protect log
access and retention accordingly.

## Public exception hierarchy

Catch the most specific error when behavior differs, or `PaqtoError` for a
framework-wide boundary. Async cancellation remains `asyncio.CancelledError`,
not a Paqto exception.

### Lifecycle and resource errors

- `AlreadyStartedError`: a component/node is started more than once where that
  lifecycle rejects it.
- `NotStartedError`: a node operation requires a running node.
- `ResourceLimitError`: a configured pending-operation, queue, or connection
  capacity is reached.

### Transport and discovery

- `TransportError`: adapter setup, endpoint, frame, or network operation fails.
- `ConnectionClosedError(TransportError)`: a closed connection is used or EOF
  prevents completion.
- `DiscoveryError`: discovery lifecycle, serialization, socket, or send fails.
- `NoEndpointError`: a peer has no endpoint compatible with the node transport.

### Serialization and routing

- `SerializationError`: serializer invocation fails, returns the wrong type, or
  cannot produce/consume a message.
- `MessageRoutingError`: an application handler raises. The original exception
  is preserved as `__cause__`.

### Timeouts

- `PaqtoTimeoutError`: common timeout boundary for generic Paqto operations.
- `ConnectionIdleTimeoutError(PaqtoTimeoutError)`: public type used internally
  when a READY receive exceeds `idle_timeout`; the reader normally consumes it
  through session cleanup rather than raising it to a caller.
- `ProtocolHandshakeTimeoutError`: both `ProtocolHandshakeError` and
  `PaqtoTimeoutError`.
- `RequestTimeoutError`: both `RequestError` and `PaqtoTimeoutError`.
- `AcknowledgementTimeoutError`: both `AcknowledgementError` and
  `PaqtoTimeoutError`.

Built-in `TimeoutError` can still appear from direct low-level transport use;
the node/manager normalizes its documented high-level boundaries.

### Peer identity and lookup

- `PeerNotFoundError`: a `Peer` is unknown to the node; discover it or pass a
  `DiscoveredPeer`.
- `PeerExpiredError(PeerNotFoundError)`: the discovery observation is too old
  to drive a new connection.
- `PeerAuthenticationError`: strict policy requires identity proof that the
  connection did not provide.
- `PeerIdentityMismatchError(PeerAuthenticationError)`: discovered,
  handshake, authenticated, or message/session identities disagree.

### Protocol

- `ProtocolError`: base for protocol-session failures.
- `ProtocolHandshakeError(ProtocolError)`: hello establishment fails.
- `ProtocolVersionError(ProtocolHandshakeError)`: exact versions differ.
- `ProtocolFrameError(ProtocolError)`: a control/application frame or message
  envelope is malformed, unexpected, or over its negotiated limit.

### Request/reply and acknowledgements

- `RequestError`: pending correlation fails because of connection loss,
  shutdown, invalid reply context, or another non-timeout request condition.
- `RequestTimeoutError`: no matching reply arrives before its deadline.
- `AcknowledgementError(ProtocolError)`: ACK wait fails, commonly because the
  exact connection closes or the node stops.
- `AcknowledgementTimeoutError`: no matching ACK arrives before its deadline.
- `AcknowledgementUnavailableError`: the READY session did not negotiate ACK
  support; the application frame is not sent for that operation.

Malformed peer traffic is generally handled by closing its connection and
emitting/logging an event rather than raising into an unrelated application
task. Direct calls such as `connect()`, `send()`, and `request()` expose their
own setup/correlation failures to the caller.

