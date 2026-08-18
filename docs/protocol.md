# Paqto protocol and READY sessions

Paqto version 1 adds a session protocol above a transport's complete byte
frames. It prevents raw transport connections or early application data from
being treated as trusted application sessions.

## Frame layers

On the LAN transport, one TCP frame is a four-byte length plus a Paqto frame.
The first byte of the Paqto frame identifies its kind:

```text
TCP:   [4-byte unsigned big-endian length][Paqto frame bytes]
Paqto: [1-byte kind][control JSON or serializer-produced bytes]
```

- `0x00` is a control frame.
- `0x01` is a serialized application-message frame.

Control JSON is internal to the protocol and never goes through the
application serializer. Application bytes are never interpreted as control
JSON.

## Hello exchange

Both peers send one `hello` control frame and receive one from the other side.
The hello contains:

| Field | Semantics |
| --- | --- |
| `magic` | Exact protocol identifier `PAQTO`. |
| `type` | `hello`. |
| `version` | Positive integer protocol version; current default is `1`. |
| `peer_id` | Logical identity declared for this session. |
| `serializer` | Application encoding identity. |
| `capabilities` | Unique non-empty strings offered by this peer. |
| `max_message_size` | Maximum serialized application bytes accepted. |
| `metadata` | Optional generic JSON object, exposed as remote session metadata. |

Version and serializer identity must match exactly; version 1 performs no
downgrade or alternative-encoding selection. A mismatch fails the handshake,
closes the connection, and creates no `ProtocolSession`.

Capabilities are negotiated as the intersection of both offers in local offer
order. The negotiated application limit is the lower of the two offered
`max_message_size` values.

Current reserved capabilities are:

- `paqto.ack.v1` for technical acknowledgements;
- `paqto.heartbeat.v1` for PING/PONG.

Nodes advertise heartbeat support even when they do not initiate heartbeats,
so they can answer negotiated PING frames. ACK support is offered only when
`enable_acknowledgements` is true. User-configured capability strings remain
opaque to the core.

## Identity consistency

Three identities must not be confused:

```text
discovered peer id       = untrusted intended destination
handshake peer id        = session claim
authenticated peer id    = identity proved by transport security
```

For outgoing connections, the handshake id must equal the intended discovered
peer id. Whenever `SecurityInfo.authenticated_peer_id` exists, it must equal
the handshake id for incoming and outgoing sessions. A contradiction raises
`PeerIdentityMismatchError` and closes the connection.

If `require_authenticated_peer_id_match=True`, the connection must be marked
authenticated and must supply an authenticated peer id. The session records
`peer_id_authenticated=True` only when that authenticated id equals the hello
id. On plain TCP, a consistent hello can reach READY, but the flag is false and
the identity is not cryptographic proof.

After READY, every incoming `Message.sender` must match the session peer id. A
non-null recipient must match the local node id.

## `ProtocolSession`

`node.session_for(connection)` returns the immutable negotiated session while
the connection is READY. It exposes:

- remote `peer_id`;
- negotiated `version` and `serializer_id`;
- negotiated `capabilities`;
- negotiated `max_message_size`;
- `peer_id_authenticated`;
- immutable remote hello `metadata`.

No session is returned before successful negotiation or after deactivation.

## Control validation and failure behavior

Control payloads are UTF-8 JSON and are limited to 64 KiB. The decoder rejects
duplicate object keys, non-finite numbers, integers over 4096 bits, excessive
nesting, malformed UTF-8/JSON, wrong magic/type, invalid field shapes, and
unexpected controls. Hello metadata must be a JSON object. The handshake has a
separate configurable timeout.

After READY, only negotiated ACK and heartbeat controls are accepted. An early
application frame, a repeated hello, an unsupported control, or a malformed
frame is a protocol error. Handshake failure or cancellation closes the
connection. Reader-side protocol failure closes that session through normal
connection cleanup.

`HandshakeOffer`, `ProtocolSession`, `TechnicalAcknowledgement`,
`HeartbeatPing`, and `HeartbeatPong` are public models, but applications
normally interact through `PaqtoNode` rather than encoding frames directly.

Wire compatibility is currently exact Paqto v1 plus exact serializer identity.
A formal cross-language protocol specification and test vectors remain future
work; do not infer features that are not in the current implementation.

