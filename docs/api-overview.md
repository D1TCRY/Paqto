# Public API overview

This page maps common tasks to public components. It is an orientation guide,
not a duplicate of every signature or docstring.

## Primary application API

Import these from `paqto`:

| Component | Use it for |
| --- | --- |
| `PaqtoNode` | Start/stop/restart network resources, refresh after host-observed network changes, discover/connect/disconnect peers, send/request/reply, register handlers/events, and inspect session state. |
| `PaqtoConfig` | Configure protocol, timeouts, limits, liveness, backpressure, handler failure, and strict identity binding. |
| `Message` | Represent a complete application envelope. |
| `Peer` | Represent a logical identity; `Peer.create()` generates a random id. |
| `Endpoint` | Represent one transport-specific address. |
| `DiscoveredPeer` | Combine a peer claim, reachable endpoints, metadata, and freshness. |
| `NoDiscovery` | Run with only host-provisioned endpoints and no discovery sockets. |
| `PeerFreshness` | Interpret a discovery observation as `FRESH` or `EXPIRED`. |
| `Serializer` | Implement safe conversion between complete `Message` objects and bytes. |
| `NodeEvent`, `NodeEventType` | Observe best-effort lifecycle and failure events. |
| `ConnectionState` | Inspect logical peer orchestration state. |
| `SecurityInfo` | Inspect established transport security guarantees. |
| `ReconnectPolicy` | Enable and tune session reconnect. |
| `BackpressurePolicy`, `HandlerErrorPolicy` | Select queue-full and handler-failure behavior. |

### `PaqtoNode` operations

Construction requires keyword arguments `name`, `transport`, `discovery`, and
`serializer`; `config` and `peer_id` are optional. Use a stable explicit
`peer_id` when certificate identity mapping or long-lived logical identity
matters.

- `start()` / `stop()` own the full adapter/task lifecycle.
- `network_changed()` atomically rebuilds network resources, invalidates stale
  endpoints, rediscovers, and schedules eligible prior peers for reconnect.
- `discover(timeout=None)` returns fresh `DiscoveredPeer` observations.
- `connect(target, timeout=None)` returns only after READY.
- `disconnect(target)` intentionally closes and suppresses reconnect.
- `send(...)` returns the constructed outgoing `Message`.
- `request(...)` returns the correlated reply `Message`.
- `reply(message, ...)` creates and sends a response with `reply_to=message.id`.
- `on_message(type=None)` and `on_event(type=None)` are decorators.
- `session_for(connection)`, `connection_for_peer(peer)`, and
  `connection_state(peer)` expose current volatile session state.
- `last_reconnect_error(peer)` exposes reconnect diagnostics.
- `is_running`, pending counts, task counts, queue sizes, and
  `active_connection_count` expose lightweight diagnostics, not durable
  metrics.

## Extension contracts

Custom adapters implement these abstract base classes from `paqto`:

- `Transport`: stable `name`; async `start()`, `stop()`, `connect()`, and
  `create_listener()`.
- `Connection`: endpoint/closed properties; async complete-frame
  `send_frame()`, `receive_frame()`, and `close()`; optionally override
  `security_info`.
- `Listener`: advertised `local_endpoint`; async `start()`, `accept()`, and
  `close()`.
- `DiscoveryService`: async `start(local_peer, endpoints)`, `discover()`, and
  `stop()`.
- `Serializer`: `serialize(Message) -> bytes`, `deserialize(bytes) -> Message`,
  and optionally a stable `protocol_id`.

Adapter lifecycle methods must cooperate with cancellation and return from
close/stop; the core has no hard adapter shutdown deadline. Paqto does not
install process signal handlers or create/configure the host event loop.

`MessageRouter`, `EventRouter`, and `ConnectionManager` are also public and
useful for lower-level composition or adapter testing. Applications normally
use their `PaqtoNode` wrappers.

`MessageHandler` and `EventHandler` are public callable type aliases for sync
or async callbacks.

## LAN API

Import the built-in adapters from `paqto.lan`:

| Component | Use it for |
| --- | --- |
| `LanTransport` | Framed TCP, optionally TLS-protected. |
| `LanDiscovery` | IPv4 UDP broadcast reachability discovery. |
| `TlsConfig` | TLS trust, verification, mTLS, identity resolver, and handshake timeout. |
| `TlsContextConfig` | Caller-prepared client/server `SSLContext` injection and connection policy. |
| `TlsPeerIdentityResolver` | Type alias for the certificate-to-logical-id callback. |
| `TcpConnection` | Direct low-level access to a framed asyncio TCP connection. |
| `TcpListener` | Direct low-level access to a TCP listener. |

`paqto.lan.address` additionally provides the `TcpAddress` value object and
`parse_tcp_address()`, `build_tcp_address()`, `endpoint_from_host_port()`,
`endpoint_from_sockname()`, `parse_sockname()`, `choose_advertised_host()`,
`validate_max_frame_size()`, and `validate_frame_payload_timeout()`. These
lower-level helpers are not re-exported by `paqto.lan`.

## Protocol API

Top-level `paqto` exports protocol constants and models:

- `PROTOCOL_MAGIC`, `PROTOCOL_VERSION`;
- `TECHNICAL_ACK_CAPABILITY`, `HEARTBEAT_CAPABILITY`;
- `HandshakeOffer`, `ProtocolSession`;
- `TechnicalAcknowledgement`, `HeartbeatPing`, `HeartbeatPong`.

Advanced adapter/protocol work can import `negotiate_protocol`, application
frame encode/decode helpers, control-frame encoders, and `decode_session_frame`
from `paqto.core.protocol`. Normal applications should let `PaqtoNode` own this
state machine; bypassing it can violate READY and cleanup assumptions.

## Security API

`SecurityInfo` is the generic immutable established-connection snapshot.
`TlsConfig` and `TlsContextConfig` are LAN-specific requested configuration.
Do not use requested TLS settings as proof; inspect the established
`Connection.security_info` and `ProtocolSession.peer_id_authenticated` when
diagnostics need the actual result.

## Models and mutability

- Immutable: `SecurityInfo`, `HandshakeOffer`, `ProtocolSession`, heartbeat/ACK
  models, `NodeEvent`, `ReconnectPolicy`, `TlsConfig`, `TlsContextConfig`, and
  `TcpAddress`.
- Mutable: `PaqtoConfig`, `Peer`, `Endpoint`, `DiscoveredPeer`, and `Message`.
- `SecurityInfo.metadata`, `ProtocolSession.metadata`, and `NodeEvent.metadata`
  are read-only shallow copies.

Treat node configuration and peer identity as startup state even where the
Python object is mutable.

## Compatibility import

`from paqto.connection import Connection` resolves to the same abstract async
type as `from paqto import Connection`. It does not emulate the retired threaded
`Connection(host, port)` constructor or blocking send/receive API.

See [Configuration](configuration.md) and
[Events and errors](events-and-errors.md) for detailed options and exceptions.
