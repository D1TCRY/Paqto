# Paqto

Paqto is a pre-alpha, async-first Python framework for generic peer-to-peer
communication between devices. It separates application messages from
discovery, transport, serialization, protocol negotiation, and transport
security, so those layers can evolve independently.

The current built-in network implementation provides IPv4 UDP discovery and
length-prefixed TCP connections, with optional TLS or mutual TLS. Above the
transport, Paqto establishes a versioned READY session before it accepts
application messages.

Paqto provides volatile messaging primitives, not a message broker or an
application transaction system. In particular, it does not inherently provide
exactly-once or at-least-once delivery, durable messaging, automatic
application-message retry, transactional processing, application
authorization, or application success guarantees.

## Requirements and installation

Paqto requires Python 3.10 or later.

```console
python -m pip install -e .
```

For development and tests:

```console
python -m pip install -e ".[dev]"
python -m pytest
```

The offline platform-conformance suite is separate from the normal pytest
suite and is not included in the runtime wheel:

```console
python -m platform_conformance --profile full --json paqto-conformance.json
```

A platform is marked tested only after this suite runs on a real interpreter
on that platform. See [platform testing](docs/platform-testing.md) for report
semantics, desktop CI, the real-device procedure, and the two-device
interoperability tool.

## Minimal LAN example

Applications provide a `Serializer` for the complete `Message` envelope. This
excerpt assumes `JsonSerializer` is implemented as shown in the
[getting-started guide](docs/getting-started.md#implement-a-serializer).

```python
from paqto import Message, PaqtoNode
from paqto.lan import LanDiscovery, LanTransport

node = PaqtoNode(
    name="device-a",
    transport=LanTransport(),
    discovery=LanDiscovery(),
    serializer=JsonSerializer(),
)

@node.on_message("greeting")
async def handle_greeting(message: Message) -> None:
    print(message.sender, message.payload)

await node.start()
try:
    peers = await node.discover()
    await node.send(peers[0], {"text": "hello"}, type="greeting")
finally:
    await node.stop()
```

`start()` / `stop()` are the restartable network lifecycle. The host owns the
process or application lifecycle and decides when to call them. If the host
detects that network interfaces or addresses changed while the node is running,
`await node.network_changed()` rebuilds listeners, discovery, TLS connections,
and Paqto sessions from fresh endpoint observations.

See [`examples/lan_two_nodes.py`](examples/lan_two_nodes.py) for a complete
runnable two-node example.

## Security status

Plain `LanTransport()` and UDP discovery do not authenticate peers. TLS is
opt-in. TLS certificate verification alone also does not define a logical
`Peer.id`; strict identity binding requires a certificate identity resolver,
mutual TLS where incoming identity proof is required, and
`PaqtoConfig(require_authenticated_peer_id_match=True)`.

TLS can use Paqto's file-based `TlsConfig`, including custom CA data held in
memory, or caller-prepared client and server `ssl.SSLContext` objects through
`TlsContextConfig`. The two configuration modes are mutually exclusive.

Paqto is suitable for controlled-LAN evaluation and integration testing. Do
not describe it as secure by default or production-ready on hostile networks.
Review the [security model](docs/security.md) and
[production considerations](docs/production.md) before deployment.

## Documentation

- [Documentation home](docs/index.md)
- [Getting started](docs/getting-started.md)
- [Architecture and lifecycle](docs/architecture.md)
- [Configuration reference](docs/configuration.md)
- [Messaging, request/reply, and ACKs](docs/messaging.md)
- [Protocol and READY sessions](docs/protocol.md)
- [LAN discovery](docs/discovery.md)
- [LAN transport](docs/transports.md)
- [Security model](docs/security.md)
- [Reliability and concurrency](docs/reliability.md)
- [Events, logging, and errors](docs/events-and-errors.md)
- [Public API overview](docs/api-overview.md)
- [Production considerations](docs/production.md)
- [Platform support and portability](docs/platform-support.md)
- [Platform conformance and interoperability testing](docs/platform-testing.md)

Historical engineering reports remain under `docs/development_logs/`; they
record how the implementation evolved and are not the current API reference.
