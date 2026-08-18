import asyncio
import json
from typing import Any

import pytest

from paqto.core import (
    ConnectionClosedError,
    Message,
    PaqtoConfig,
    PaqtoNode,
    ProtocolVersionError,
    Serializer,
)
from paqto.core.protocol import encode_application_frame
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


def _node(
    name: str,
    peer_id: str,
    *,
    protocol_version: int = 1,
) -> PaqtoNode:
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
        config=PaqtoConfig(
            connect_timeout=2,
            send_timeout=2,
            discover_timeout=0,
            protocol_version=protocol_version,
        ),
    )


def _lan_discovery(node: PaqtoNode) -> LanDiscovery:
    discovery = node.discovery
    assert isinstance(discovery, LanDiscovery)
    return discovery


def _inject_announce(source: PaqtoNode, target: PaqtoNode) -> None:
    payload = _lan_discovery(source)._announce_payload()
    _lan_discovery(target)._datagram_received(
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
        discovered_receiver = _lan_discovery(sender)._discovered[receiver.peer.id]

        await sender.send(
            discovered_receiver,
            {"text": "hello over lan"},
            type="greeting",
        )
        message = await asyncio.wait_for(received.get(), timeout=2)

        assert message.payload == {"text": "hello over lan"}
        assert message.sender == sender.peer.id
        assert message.recipient == receiver.peer.id

        connection = sender._connections.get(receiver.peer)
        assert connection is not None
        sender_session = sender.session_for(connection)
        assert sender_session is not None
        assert sender_session.peer_id == receiver.peer.id
        assert sender_session.peer_id_authenticated is False
        receiver_sessions = list(receiver._sessions.values())
        assert len(receiver_sessions) == 1
        assert receiver_sessions[0].peer_id == sender.peer.id
        assert receiver_sessions[0].peer_id_authenticated is False
    finally:
        await asyncio.gather(sender.stop(), receiver.stop(), return_exceptions=True)

    await asyncio.sleep(0)
    assert sender._accept_task is None
    assert receiver._accept_task is None
    assert sender._reader_tasks == {}
    assert receiver._reader_tasks == {}


@pytest.mark.asyncio
async def test_application_message_before_handshake_is_never_dispatched() -> None:
    receiver = _node("receiver", "receiver-peer")
    raw_transport = LanTransport(host="127.0.0.1", port=0)
    dispatched = asyncio.Event()

    @receiver.on_message("early")
    def handle_early(message: Message) -> None:
        dispatched.set()

    connection = None
    try:
        await receiver.start()
        await raw_transport.start()
        assert receiver._listener is not None
        connection = await raw_transport.connect(receiver._listener.local_endpoint)

        hello = await asyncio.wait_for(connection.receive_frame(), timeout=1)
        assert hello.startswith(b"\x00")
        serialized = receiver.serializer.serialize(
            Message(
                payload="must not dispatch",
                type="early",
                sender="raw-peer",
                recipient=receiver.peer.id,
            )
        )
        await connection.send_frame(
            encode_application_frame(serialized, max_message_size=len(serialized))
        )

        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(connection.receive_frame(), timeout=1)
        await asyncio.sleep(0)
        assert dispatched.is_set() is False
        assert receiver._sessions == {}
    finally:
        await asyncio.gather(
            receiver.stop(),
            raw_transport.stop(),
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_nodes_reject_incompatible_protocol_versions_before_ready() -> None:
    version_one = _node("version-one", "peer-v1", protocol_version=1)
    version_two = _node("version-two", "peer-v2", protocol_version=2)

    try:
        await version_one.start()
        await version_two.start()
        _inject_announce(version_two, version_one)
        discovered = _lan_discovery(version_one)._discovered[version_two.peer.id]

        with pytest.raises(ProtocolVersionError, match="local 1, remote 2"):
            await version_one.connect(discovered)

        assert version_one._connections.get(version_two.peer) is None
        assert version_one._sessions == {}
        await asyncio.sleep(0)
        assert version_two._sessions == {}
    finally:
        await asyncio.gather(
            version_one.stop(),
            version_two.stop(),
            return_exceptions=True,
        )
