# Getting started

Paqto applications assemble a node from four pieces:

1. a `Transport` for framed byte connections;
2. a `DiscoveryService` for reachability hints;
3. a `Serializer` for complete `Message` envelopes;
4. a `PaqtoNode` that owns the protocol and application-facing lifecycle.

The built-in LAN adapters are `LanTransport` and `LanDiscovery`.

## Choose a serializer

Serializer selection is explicit because it defines the application wire
format. Paqto includes two dependency-free implementations:

```python
from paqto.serializers import BytesSerializer, JsonSerializer
```

`JsonSerializer` handles portable JSON values. `BytesSerializer` handles exact
`bytes` payloads using canonical Base64 inside the envelope. Applications with
different schema, performance, or interoperability requirements can still
implement the public `Serializer` contract.

Both peers must use the same format and advertise the same stable
`protocol_id`. See [Built-in serializers](serializers.md) for supported payloads,
resource limits, safety guarantees, and custom implementations.

## Create two LAN nodes

```python
import asyncio

from paqto import Message, PaqtoConfig, PaqtoNode
from paqto.lan import LanDiscovery, LanTransport
from paqto.serializers import JsonSerializer


async def main() -> None:
    config = PaqtoConfig(
        connect_timeout=5,
        send_timeout=5,
        discover_timeout=2,
    )
    node_a = PaqtoNode(
        name="device-a",
        peer_id="device-a",
        transport=LanTransport(host="127.0.0.1"),
        discovery=LanDiscovery(),
        serializer=JsonSerializer(),
        config=config,
    )
    node_b = PaqtoNode(
        name="device-b",
        peer_id="device-b",
        transport=LanTransport(host="127.0.0.1"),
        discovery=LanDiscovery(),
        serializer=JsonSerializer(),
        config=config,
    )
    received = asyncio.Event()

    @node_b.on_message("greeting")
    async def greeting(message: Message) -> None:
        print(message.payload)
        received.set()

    try:
        await node_a.start()
        await node_b.start()
        peers = await node_a.discover(timeout=2)
        target = next(item for item in peers if item.peer.id == node_b.peer.id)
        await node_a.send(target, {"text": "hello"}, type="greeting")
        await asyncio.wait_for(received.wait(), timeout=5)
    finally:
        await asyncio.gather(node_a.stop(), node_b.stop())


asyncio.run(main())
```

`asyncio.run()` above belongs only to this standalone program entry point.
Paqto itself exposes awaitable APIs and runs inside the event loop controlled by
the host. The same node can complete `start()` / `stop()` more than once with
restartable adapters. When host-specific monitoring reports a route, interface,
or address change while running, use `await node.network_changed()` to rebuild
network resources and rediscover endpoints.

On a real LAN, UDP broadcast behavior depends on interfaces, firewall rules,
and OS socket behavior. The repository's integration tests inject discovery
announcements and use loopback TCP so they remain deterministic.

## Register handlers

`on_message(type)` registers sync or async handlers. A `None` type registers a
catch-all handler. For one message, type-specific handlers run first and
catch-all handlers run afterward, each in registration order.

```python
@node.on_message("status")
async def status(message: Message) -> None:
    ...


@node.on_message()
def observe_all(message: Message) -> None:
    ...
```

Handlers for different messages may run concurrently up to
`handler_concurrency`. Synchronous handlers execute on the event-loop thread;
they must return quickly. Offload blocking or CPU-heavy work explicitly.

## Request and reply

```python
@node_b.on_message("query")
async def query(message: Message) -> None:
    await node_b.reply(message, {"value": 42}, type="query-result")


response = await node_a.request(
    target,
    {"key": "example"},
    type="query",
    timeout=5,
)
print(response.payload)
```

The timeout applies to waiting for the correlated reply. Connect and send use
their configured timeouts. Request/reply state is in memory and is tied to the
exact READY connection; it does not survive reconnect or restart.

## Next steps

- Read [Messaging](messaging.md) before relying on ACKs or correlation.
- Read [Security](security.md) before enabling TLS.
- Tune limits using [Configuration](configuration.md).
- Review [Production considerations](production.md) before non-test deployment.
