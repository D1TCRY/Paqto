from __future__ import annotations

import asyncio
import json
from typing import Any

from paqto import Message, PaqtoConfig, PaqtoNode, Serializer
from paqto.lan import LanDiscovery, LanTransport


class JsonSerializer(Serializer):
    def serialize(self, message: Message) -> bytes:
        return json.dumps(
            {
                "payload": message.payload,
                "type": message.type,
                "sender": message.sender,
                "recipient": message.recipient,
                "headers": message.headers,
                "id": message.id,
                "reply_to": message.reply_to,
            }
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
            reply_to=raw["reply_to"],
        )


async def main() -> None:
    config = PaqtoConfig(connect_timeout=5, send_timeout=5, discover_timeout=2)
    node_a = PaqtoNode(
        name="node-a",
        transport=LanTransport(),
        discovery=LanDiscovery(),
        serializer=JsonSerializer(),
        config=config,
    )
    node_b = PaqtoNode(
        name="node-b",
        transport=LanTransport(),
        discovery=LanDiscovery(),
        serializer=JsonSerializer(),
        config=config,
    )
    received = asyncio.Event()

    @node_b.on_message("greeting")
    def on_greeting(message: Message) -> None:
        print(f"node-b received from {message.sender}: {message.payload}")
        received.set()

    try:
        await node_a.start()
        await node_b.start()

        discovered = await node_a.discover(timeout=2)
        target = next(peer for peer in discovered if peer.peer.id == node_b.peer.id)
        await node_a.send(target, {"text": "hello from node-a"}, type="greeting")
        await asyncio.wait_for(received.wait(), timeout=5)
    finally:
        await asyncio.gather(node_a.stop(), node_b.stop(), return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
