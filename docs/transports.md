# LAN transport

`LanTransport` implements Paqto's framed `Transport` contract using asyncio TCP
streams. It can run as plain TCP or add TLS at connection establishment.

## Endpoints and listener addresses

LAN endpoints use:

```text
tcp://HOST:PORT
```

The parser rejects other schemes, user information, paths, query strings,
fragments, missing hosts, and invalid or missing ports. IPv6 literals produced
by the address builder are bracketed, but built-in LAN discovery itself is
IPv4-only.

`LanTransport(host, port)` defines the default listener bind. Port `0` asks the
OS to select a free port. A caller using the transport directly may pass a LAN
`Endpoint` to `create_listener(bind=...)` to override those values; nodes call
`create_listener()` without an override.

The bind address and advertised address are different concepts. When bound to
`0.0.0.0`, `::`, or an empty host, `TcpListener` tries to advertise a detected
primary IPv4 address, then a hostname-resolved IPv4 address, and finally the
bind value. Endpoint metadata records:

- `bind_host`: the configured local bind;
- `advertised_host_source`: `bind_host`, `primary_ipv4`, or `hostname_ipv4`.

This detection is best effort and may select the wrong interface on a
multi-homed host. Configure an explicit host when the advertised route matters.

## TCP framing

`TcpConnection` presents complete byte frames above TCP's byte stream:

```text
[4-byte unsigned big-endian payload length][exactly that many payload bytes]
```

`receive_frame()` returns only the payload. Independent send and receive locks
allow full-duplex use while preventing concurrent callers in one direction from
interleaving a frame. Empty frames are valid at the transport layer, although
an empty Paqto protocol frame is invalid.

The default `max_frame_size` is 16 MiB plus one byte. The extra byte fits
Paqto's frame-kind marker above the default 16 MiB serialized-message limit.
The maximum configurable value is `2**32 - 1`, imposed by the four-byte header.
Oversized outgoing frames are rejected. An oversized incoming declaration
closes the connection.

After a legal length is declared, `frame_payload_timeout` bounds how long the
peer may take to complete the payload. The default is 30 seconds; expiry closes
the connection. `None` removes this protection and is inappropriate for
hostile peers.

The core protocol's negotiated `max_message_size` and the transport's
`max_frame_size` are separate limits. Configure the transport to hold the
serialized application payload plus Paqto's one-byte marker. Control frames
are independently capped at 64 KiB.

## Listener behavior

`TcpListener.start()` creates the TCP server. `accept()` waits for the next
`TcpConnection`; calling it before start is a `TransportError`, and after close
is a `ConnectionClosedError`.

Established connections waiting to be returned are capped by
`max_pending_accepts`, default 128. If no `accept()` waiter is ready and the
deque is full, a new connection is closed. This limit is below the node and is
distinct from `PaqtoConfig.max_connections`, which runs after acceptance and
includes connections negotiating the Paqto handshake.

Closing a listener is idempotent. It wakes all pending `accept()` calls, closes
queued and already accepted listener-owned connections, closes the server, and
waits for final shutdown.

## Connection lifecycle

`LanTransport.start()` validates and creates TLS contexts when configured.
`connect()` validates the endpoint, opens the stream within the caller's
timeout, captures endpoints and `SecurityInfo`, and tracks the connection.
If a connect finishes after the transport has stopped or restarted, it is
closed and rejected rather than returned into a newer lifecycle generation.

`stop()` marks the transport stopped, closes its listeners and outgoing
connections, and clears TLS contexts. The same transport object can be started
again.

## What TCP and Paqto each add

TCP provides an ordered, reliable byte stream while one connection remains
alive. It does not preserve application message boundaries, authenticate a
peer, make data durable, or prove that remote application code succeeded.

The LAN adapter adds explicit frame boundaries and per-frame size/time limits.
Optional TLS adds channel encryption and authentication properties. The Paqto
protocol then adds READY negotiation, logical identity consistency,
capabilities, application-size negotiation, control/application separation,
and higher-level volatile correlation.

None of these layers supplies exactly-once processing, durable queues,
automatic message resend, transactions, or application authorization.

See [Security](security.md) for TLS and [Reliability](reliability.md) for the
logical session lifecycle.
