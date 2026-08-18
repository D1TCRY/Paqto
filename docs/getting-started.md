# Getting started

Paqto applications assemble a node from four pieces:

1. a `Transport` for framed byte connections;
2. a `DiscoveryService` for reachability hints;
3. a `Serializer` for complete `Message` envelopes;
4. a `PaqtoNode` that owns the protocol and application-facing lifecycle.

The built-in LAN adapters are `LanTransport` and `LanDiscovery`.

## Implement a serializer

Paqto intentionally has no default application serializer. A serializer must
preserve the complete envelope, especially `id` and `reply_to` for
request/reply and ACK correlation.

```python
import json
from datetime import datetime
from typing import Any

from paqto import Message, Serializer


class JsonSerializer(Serializer):
    @property
    def protocol_id(self) -> str:
        return "example/message-json-v1"

    def serialize(self, message: Message) -> bytes:
        return json.dumps(
            {
                "payload": message.payload,
                "type": message.type,
                "sender": message.sender,
                "recipient": message.recipient,
                "headers": message.headers,
                "id": message.id,
                "created_at": message.created_at.isoformat(),
                "reply_to": message.reply_to,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    def deserialize(self, data: bytes) -> Message:
        raw: dict[str, Any] = json.loads(data.decode("utf-8"))
        return Message(
            payload=raw["payload"],
            type=raw["type"],
            sender=raw["sender"],
            recipient=raw["recipient"],
            headers=raw["headers"],
            id=raw["id"],
            created_at=datetime.fromisoformat(raw["created_at"]),
            reply_to=raw["reply_to"],
        )
```

The explicit `protocol_id` gives different processes and implementations a
stable wire identifier. Without the override, `Serializer.protocol_id` is
derived from the Python module and qualified class name.

Only deserialize formats that are safe for untrusted input. Paqto does not
sandbox serializer code or constrain the size of the resulting Python object.

## Create two LAN nodes

```python
import asyncio

from paqto import Message, PaqtoConfig, PaqtoNode
from paqto.lan import LanDiscovery, LanTransport


async def main() -> None:
    config = PaqtoConfig(
        connect_timeout=5,
        send_timeout=5,
        discover_timeout=2,
        serializer_id="example/message-json-v1",
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
