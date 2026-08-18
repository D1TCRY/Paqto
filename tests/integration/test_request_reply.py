import asyncio
import json
from typing import Any

import pytest

from paqto.core import (
    AcknowledgementTimeoutError,
    AcknowledgementUnavailableError,
    DiscoveredPeer,
    Message,
    PaqtoConfig,
    PaqtoNode,
    RequestError,
    RequestTimeoutError,
    Serializer,
)
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
        ).encode()

    def deserialize(self, data: bytes) -> Message:
        raw: dict[str, Any] = json.loads(data.decode())
        return Message(
            payload=raw["payload"],
            type=raw["type"],
            sender=raw["sender"],
            recipient=raw["recipient"],
            headers=raw["headers"],
            id=raw["id"],
            reply_to=raw["reply_to"],
        )


def _node(peer_id: str, *, acknowledgements: bool = True) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
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
            request_timeout=1,
            acknowledgement_timeout=1,
            discover_timeout=0,
            enable_acknowledgements=acknowledgements,
        ),
    )


def _lan_discovery(node: PaqtoNode) -> LanDiscovery:
    discovery = node.discovery
    assert isinstance(discovery, LanDiscovery)
    return discovery


def _make_known(source: PaqtoNode, target: PaqtoNode) -> DiscoveredPeer:
    payload = _lan_discovery(target)._announce_payload()
    source_discovery = _lan_discovery(source)
    source_discovery._datagram_received(
        json.dumps(payload).encode(),
        ("127.0.0.1", 0),
    )
    return source_discovery._discovered[target.peer.id]


async def _stop_and_assert_clean(*nodes: PaqtoNode) -> None:
    await asyncio.gather(*(node.stop() for node in nodes), return_exceptions=True)
    await asyncio.sleep(0)
    for node in nodes:
        assert node.pending_request_count == 0
        assert node.pending_acknowledgement_count == 0
        assert node._handler_tasks == set()


@pytest.mark.asyncio
async def test_request_reply_uses_message_id_correlation() -> None:
    requester = _node("requester")
    responder = _node("responder")

    @responder.on_message("lookup")
    async def handle(message: Message) -> None:
        reply = await responder.reply(message, {"value": 42}, type="result")
        assert reply.reply_to == message.id

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)

        response = await requester.request(
            target,
            {"key": "generic"},
            type="lookup",
        )

        assert response.payload == {"value": 42}
        assert response.type == "result"
        assert response.sender == responder.peer.id
        assert response.recipient == requester.peer.id
        assert response.reply_to is not None
        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_many_concurrent_requests_are_correlated_independently() -> None:
    requester = _node("requester")
    responder = _node("responder")

    @responder.on_message("compute")
    async def handle(message: Message) -> None:
        await asyncio.sleep((int(message.payload) % 5) / 1000)
        await responder.reply(message, int(message.payload) * 2)

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)
        responses = await asyncio.gather(
            *(
                requester.request(target, value, type="compute", timeout=2)
                for value in range(40)
            )
        )

        assert [response.payload for response in responses] == [
            value * 2 for value in range(40)
        ]
        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_request_timeout_and_late_reply_are_cleanly_ignored() -> None:
    requester = _node("requester")
    responder = _node("responder")
    late_reply_sent = asyncio.Event()
    incorrectly_dispatched = asyncio.Event()

    @responder.on_message("slow")
    async def handle(message: Message) -> None:
        await asyncio.sleep(0.08)
        await responder.reply(message, "late")
        late_reply_sent.set()

    @requester.on_message(None)
    def catch_reply(message: Message) -> None:
        incorrectly_dispatched.set()

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)

        with pytest.raises(RequestTimeoutError, match="Timed out"):
            await requester.request(target, None, type="slow", timeout=0.02)

        assert requester.pending_request_count == 0
        await asyncio.wait_for(late_reply_sent.wait(), timeout=1)
        await asyncio.sleep(0.02)
        assert incorrectly_dispatched.is_set() is False
        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_request_cancellation_removes_pending_correlation() -> None:
    requester = _node("requester")
    responder = _node("responder")
    received = asyncio.Event()

    @responder.on_message("hold")
    def hold(message: Message) -> None:
        received.set()

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)
        request = asyncio.create_task(
            requester.request(target, None, type="hold", timeout=2)
        )
        await asyncio.wait_for(received.wait(), timeout=1)
        assert requester.pending_request_count == 1

        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request

        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_unknown_reply_to_does_not_complete_or_dispatch_request() -> None:
    requester = _node("requester")
    responder = _node("responder")
    incorrectly_dispatched = asyncio.Event()

    @responder.on_message("bad-correlation")
    async def handle(message: Message) -> None:
        unrelated = Message(payload=None, sender=message.sender, id="unknown-request")
        await responder.reply(unrelated, "wrong")

    @requester.on_message(None)
    def catch_reply(message: Message) -> None:
        incorrectly_dispatched.set()

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)

        with pytest.raises(RequestTimeoutError):
            await requester.request(
                target,
                None,
                type="bad-correlation",
                timeout=0.05,
            )

        assert incorrectly_dispatched.is_set() is False
        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_disconnect_fails_request_pending_on_that_connection() -> None:
    requester = _node("requester")
    responder = _node("responder")

    @responder.on_message("disconnect")
    async def handle(message: Message) -> None:
        context = responder._message_context.get()
        assert context is not None
        await context[1].close()

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)

        with pytest.raises(RequestError, match="Connection closed"):
            await requester.request(target, None, type="disconnect", timeout=1)

        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_node_stop_fails_all_pending_requests() -> None:
    requester = _node("requester")
    responder = _node("responder")
    received = asyncio.Event()

    @responder.on_message("wait")
    def handle(message: Message) -> None:
        received.set()

    try:
        await requester.start()
        await responder.start()
        target = _make_known(requester, responder)
        request = asyncio.create_task(
            requester.request(target, None, type="wait", timeout=2)
        )
        await asyncio.wait_for(received.wait(), timeout=1)
        assert requester.pending_request_count == 1

        await requester.stop()
        with pytest.raises(RequestError, match="stopped"):
            await request
        assert requester.pending_request_count == 0
    finally:
        await _stop_and_assert_clean(requester, responder)


@pytest.mark.asyncio
async def test_technical_ack_is_consumed_before_application_handlers() -> None:
    sender = _node("sender")
    receiver = _node("receiver")
    received = asyncio.Event()
    sender_dispatch_count = 0

    @receiver.on_message("acknowledged")
    def handle(message: Message) -> None:
        received.set()

    @sender.on_message(None)
    def should_not_receive_ack(message: Message) -> None:
        nonlocal sender_dispatch_count
        sender_dispatch_count += 1

    try:
        await sender.start()
        await receiver.start()
        target = _make_known(sender, receiver)

        await sender.send(
            target,
            "data",
            type="acknowledged",
            require_ack=True,
        )

        await asyncio.wait_for(received.wait(), timeout=1)
        assert sender_dispatch_count == 0
        assert sender.pending_acknowledgement_count == 0
    finally:
        await _stop_and_assert_clean(sender, receiver)


@pytest.mark.asyncio
async def test_acknowledgement_timeout_cleans_pending_future() -> None:
    sender = _node("sender")
    receiver = _node("receiver")

    async def suppress_ack(connection: Any, message_id: str) -> None:
        return None

    receiver._send_acknowledgement = suppress_ack  # type: ignore[method-assign]

    try:
        await sender.start()
        await receiver.start()
        target = _make_known(sender, receiver)

        with pytest.raises(AcknowledgementTimeoutError, match="Timed out"):
            await sender.send(
                target,
                "data",
                require_ack=True,
                acknowledgement_timeout=0.02,
            )

        assert sender.pending_acknowledgement_count == 0
    finally:
        await _stop_and_assert_clean(sender, receiver)


@pytest.mark.asyncio
async def test_acknowledgement_must_be_negotiated() -> None:
    sender = _node("sender", acknowledgements=True)
    receiver = _node("receiver", acknowledgements=False)

    try:
        await sender.start()
        await receiver.start()
        target = _make_known(sender, receiver)

        with pytest.raises(AcknowledgementUnavailableError, match="did not negotiate"):
            await sender.send(target, "data", require_ack=True)

        assert sender.pending_acknowledgement_count == 0
    finally:
        await _stop_and_assert_clean(sender, receiver)
