import asyncio
import json
from typing import Any

import pytest

from paqto.core import Message, PaqtoConfig, PaqtoNode, Serializer
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


def _node(name: str, peer_id: str) -> PaqtoNode:
    return PaqtoNode(
        name=name,
        peer_id=peer_id,
        transport=LanTransport(host="127.0.0.1", port=0),
        discovery=LanDiscovery(
            discovery_port=0,
            broadcast_host="127.0.0.1",
            announce_interval=3600,
            default_discover_timeout=0,
        ),
        serializer=JsonSerializer(),
        config=PaqtoConfig(connect_timeout=2, send_timeout=2, discover_timeout=0),
    )


def _inject_announce(source: PaqtoNode, target: PaqtoNode) -> None:
    payload = source.discovery._announce_payload()
    target.discovery._datagram_received(
        json.dumps(payload).encode("utf-8"),
        ("127.0.0.1", 0),
    )


@pytest.mark.asyncio
async def test_two_lan_nodes_exchange_message_and_stop_cleanly() -> None:
    sender = _node("sender", "sender-peer")
    receiver = _node("receiver", "receiver-peer")
    received: asyncio.Queue[Message] = asyncio.Queue()

    @receiver.on_message("greeting")
    async def handle_greeting(message: Message) -> None:
        await received.put(message)

    try:
        await sender.start()
        await receiver.start()
        _inject_announce(receiver, sender)
        discovered_receiver = sender.discovery._discovered[receiver.peer.id]

        await sender.send(
            discovered_receiver,
            {"text": "hello over lan"},
            type="greeting",
        )
        message = await asyncio.wait_for(received.get(), timeout=2)

        assert message.payload == {"text": "hello over lan"}
        assert message.sender == sender.peer.id
        assert message.recipient == receiver.peer.id
    finally:
        await asyncio.gather(sender.stop(), receiver.stop(), return_exceptions=True)

    await asyncio.sleep(0)
    assert sender._accept_task is None
    assert receiver._accept_task is None
    assert sender._reader_tasks == {}
    assert receiver._reader_tasks == {}
