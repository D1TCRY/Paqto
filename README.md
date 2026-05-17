# Paqto

Async-first primitives for transport-agnostic peer communication.

## LAN example

```python
from paqto import Message, PaqtoConfig, PaqtoNode, Serializer
from paqto.lan import LanDiscovery, LanTransport


class JsonSerializer(Serializer):
    ...


node_a = PaqtoNode(
    name="node-a",
    transport=LanTransport(),
    discovery=LanDiscovery(),
    serializer=JsonSerializer(),
    config=PaqtoConfig(discover_timeout=2),
)
node_b = PaqtoNode(
    name="node-b",
    transport=LanTransport(),
    discovery=LanDiscovery(),
    serializer=JsonSerializer(),
)


@node_b.on_message("greeting")
def on_message(message: Message) -> None:
    print(message.payload)


await node_a.start()
await node_b.start()
peers = await node_a.discover(timeout=2)
target = next(peer for peer in peers if peer.peer.id == node_b.peer.id)
await node_a.send(target, {"text": "hello over LAN"}, type="greeting")
await node_a.stop()
await node_b.stop()
```

See `examples/lan_two_nodes.py` for a complete runnable version.
