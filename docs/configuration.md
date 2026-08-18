# Configuration reference

Configuration is split by responsibility. `PaqtoConfig` controls the generic
node and protocol, `ReconnectPolicy` controls restoration of lost sessions,
and LAN adapter constructors control TCP, UDP discovery, and TLS.

Defaults are intentionally usable for development, not a production sizing or
security recommendation.

## `PaqtoConfig`

### Operation and liveness timeouts

Except where noted, timeout values are seconds and accept `None` to remove the
deadline. Finite timeouts must be greater than zero. Removing a deadline can
allow faulty or hostile peers/adapters to retain tasks or resources
indefinitely.

| Option | Default | Purpose and operational/security implications |
| --- | ---: | --- |
| `connect_timeout` | `10.0` | Default outgoing transport-connect deadline. `None` permits an unbounded adapter connect. |
| `send_timeout` | `10.0` | Default wait for an outbound frame writer. It does not prove remote receipt and expiry does not prove the frame was unsent. |
| `discover_timeout` | `3.0` | Node discovery budget; accepts `None` or a finite value greater than or equal to zero. `LanDiscovery` uses a small guard inside the budget. |
| `handshake_timeout` | `10.0` | Paqto hello exchange deadline. `None` removes the protocol-handshake bound; this is separate from TLS handshake time. |
| `request_timeout` | `10.0` | Default correlated reply wait. `None` waits until completion, cancellation, disconnect, or shutdown. |
| `acknowledgement_timeout` | `10.0` | Default technical ACK wait. `None` can leave a wait pending until connection loss or shutdown. |
| `heartbeat_interval` | `None` | `None` disables initiated PING checks. A positive value starts one heartbeat task per canonical READY connection when the capability is negotiated. |
| `heartbeat_timeout` | `10.0` | PONG deadline. It may be `None` only while heartbeat initiation is disabled. |
| `idle_timeout` | `None` | Maximum wait for any frame on a READY session. `None` disables idle close. Heartbeat traffic counts as activity. |
| `peer_ttl` | `60.0` | Node-level lifetime of a discovery observation for new connect/reconnect. `None` disables node expiry; TTL is not an authentication rule. |

An explicit `send(timeout=x)` uses `x` for both connect and writer waits.
`request(timeout=x)` uses `x` only for the reply wait; its connect and send
stages use `connect_timeout` and `send_timeout`.

`reconnect` defaults to a disabled `ReconnectPolicy()` and must be a
`ReconnectPolicy` instance. Its fields are documented below.

### Protocol and identity options

| Option | Default | Allowed values and implications |
| --- | --- | --- |
| `enable_acknowledgements` | `True` | Boolean. Offers `paqto.ack.v1`; waiting remains opt-in per operation. Disabling it does not change ordinary sends. |
| `protocol_version` | `1` | Positive non-boolean integer. Peers must match exactly; changing it makes sessions incompatible with v1 defaults. |
| `capabilities` | `()` | Tuple of unique, non-empty strings. Negotiated by intersection; reserved ACK/heartbeat capabilities are added by node policy. |
| `serializer_id` | `None` | Non-empty string or `None`. `None` uses `Serializer.protocol_id`. Set an explicit stable id for cross-process/language compatibility. |
| `max_message_size` | `16 * 1024 * 1024` | Positive integer serialized-byte limit offered in the hello. The negotiated value is the lower offer. It is not an object-memory budget. |
| `protocol_metadata` | `None` | Dictionary or `None`, shallow-copied to a new dict. It must encode as safe bounded JSON at handshake time; do not place secrets in unauthenticated hello metadata. |
| `require_authenticated_peer_id_match` | `False` | Boolean. When true, every READY connection must be authenticated and expose an authenticated id matching the hello. Requires a suitable transport identity resolver. |

### Capacity and handler options

Every capacity is a positive, non-boolean integer.

| Option | Default | Scope and implications |
| --- | ---: | --- |
| `max_pending_requests` | `1024` | Node-wide pending request Futures. Excess requests raise `ResourceLimitError`. |
| `max_pending_acknowledgements` | `1024` | Node-wide pending ACK Futures. Excess ACK-waiting sends raise `ResourceLimitError`. |
| `max_inbound_queue` | `256` | Node-wide ordinary-message queue item count. It is not a byte limit or per-peer quota. |
| `max_outbound_queue` | `256` | Frame item count per READY connection, including application and control frames. |
| `max_event_queue` | `256` | Best-effort event item count. Full queues drop events and log a warning rather than blocking network work. |
| `max_connections` | `128` | Admitted physical node connections, including incoming protocol handshakes. It runs after TCP/TLS acceptance and may need room for duplicate resolution. |
| `handler_concurrency` | `4` | Fixed dispatch worker count. Higher values increase concurrency and possible out-of-order completion. |
| `inbound_backpressure` | `BackpressurePolicy.WAIT` | `WAIT` pauses the reader for capacity; `REJECT` reports a limit and closes the overflowing session. Enum instances are required, not strings. |
| `outbound_backpressure` | `BackpressurePolicy.WAIT` | `WAIT` pauses the sender/control producer; `REJECT` raises `ResourceLimitError`. Enum instances are required. |
| `handler_error_policy` | `HandlerErrorPolicy.CONTINUE` | `CONTINUE` retains the session after a handler failure; `CLOSE_CONNECTION` closes only that message's connection. |

These limits bound stored item counts, not aggregate bytes, expanded Python
objects, or application-created waiting tasks.

## `ReconnectPolicy`

`ReconnectPolicy` is immutable.

| Option | Default | Validation and effect |
| --- | ---: | --- |
| `enabled` | `False` | Boolean; no unexpected-loss reconnect occurs unless true. |
| `initial_delay` | `0.5` | Finite positive seconds before attempt zero. |
| `multiplier` | `2.0` | Finite number at least `1`; exponential growth factor. |
| `maximum_delay` | `30.0` | Finite positive cap, not less than `initial_delay`. |
| `jitter` | `0.0` | Finite fraction from `0` through `1`; randomizes each bounded delay by ± this fraction. |
| `max_attempts` | `None` | Positive integer or `None`; `None` continues while the node runs and discovery stays fresh. |

Reconnect restores only a session. It never resends an application message or
reattaches an old request/ACK Future.

## `LanTransport`

| Option | Default | Purpose and implications |
| --- | ---: | --- |
| `host` | `"0.0.0.0"` | Listener bind host. Asyncio validates usable host values when the listener starts. Use an explicit interface when address advertisement matters. |
| `port` | `0` | Listener port; `0` requests an OS-assigned port. Asyncio validates it at start. |
| `max_frame_size` | `16 * 1024 * 1024 + 1` | Complete TCP-frame payload limit, positive integer up to `2**32 - 1`. Include Paqto's one-byte kind marker above the application limit. |
| `tls` | `None` | `TlsConfig` enables TLS; `None` is unauthenticated plain TCP. |
| `max_pending_accepts` | `128` | Positive integer cap on established connections waiting for `accept()`. This is before node admission. |
| `frame_payload_timeout` | `30.0` | Finite positive seconds to complete a payload after its length is declared, or `None` for no deadline. Do not disable on hostile networks. |

## `LanDiscovery`

| Option | Default | Purpose and implications |
| --- | ---: | --- |
| `discovery_port` | `37020` | Integer from `0` through `65535`. Peers must use compatible ports; `0` is mainly useful for isolated tests. |
| `bind_host` | `"0.0.0.0"` | IPv4 UDP bind host. Socket startup reports invalid values as `DiscoveryError`. |
| `broadcast_host` | `"255.255.255.255"` | UDP destination for discover/periodic announce broadcasts. Tests may use loopback. |
| `announce_interval` | `5.0` | Finite positive seconds between announcements. Smaller values increase network traffic. |
| `default_discover_timeout` | `1.0` | Finite non-negative collection budget when the service is called directly with no timeout. |
| `metadata` | `None` | Generic discovery-announcement mapping, copied at construction and required to encode as safe JSON at start. It is public and untrusted. |
| `max_datagram_size` | `65_507` | Integer from `1` through the maximum IPv4 UDP payload. Applies to outgoing and incoming discovery data. |
| `peer_ttl` | `60.0` | Finite positive cache TTL or `None`. Separate from `PaqtoConfig.peer_ttl`. |
| `max_discovered_peers` | `1024` | Positive integer cache-admission bound; existing entries may refresh at capacity. |

`default_discover_timeout=0` is valid and useful in deterministic tests. The
node-level `discover_timeout` is passed to the service and also wraps it.

## `TlsConfig`

| Option | Default | Purpose and security implications |
| --- | --- | --- |
| `certfile` | required | Non-empty string or path-like certificate-chain file. Loaded when the transport starts. Never use repository test fixtures in deployment. |
| `keyfile` | required | Non-empty string or path-like private key. Paqto has no key-password callback or key-storage manager. |
| `cafile` | `None` | Optional trust-root file. `None` uses system roots for the relevant client/server purpose. |
| `verify_peer` | `True` | Boolean controlling outgoing certificate verification. Disabling it makes outgoing peer authentication false. |
| `check_hostname` | `True` | Boolean controlling outgoing hostname/IP verification. Cannot be true when `verify_peer` is false. |
| `require_client_certificate` | `False` | Boolean. When true, incoming TLS requires and verifies a client certificate (mTLS). |
| `peer_identity_resolver` | `None` | Callable from an already verified certificate mapping to a non-empty logical id or `None`. It defines the deployment's X.509-to-`Peer.id` convention. |
| `handshake_timeout` | `10.0` | Finite positive TLS handshake deadline applied in both directions. It does not cap the later Paqto hello. |

TLS always sets a minimum version of TLS 1.2. See [Security](security.md) for
identity-binding requirements and residual risks.

## Mutability and deployment consistency

`PaqtoConfig` is mutable, but changing it while a node runs does not rebuild
already-created queues, workers, handshakes, or sessions. Treat configuration as
startup state. `ReconnectPolicy`, `TlsConfig`, `SecurityInfo`, and
`ProtocolSession` are immutable snapshots.

Peers must agree on protocol version and serializer id. Transport frame limits
must accommodate the negotiated application limit plus protocol overhead.
Strict TLS deployments must provision matching certificate identities and
enable client-certificate verification in every incoming direction that must
be authenticated.
