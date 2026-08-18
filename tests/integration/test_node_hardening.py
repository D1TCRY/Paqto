import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import pytest

from paqto.core import (
    BackpressurePolicy,
    DiscoveredPeer,
    HandlerErrorPolicy,
    Message,
    NodeEvent,
    NodeEventType,
    PaqtoConfig,
    PaqtoError,
    PaqtoNode,
    Peer,
    ProtocolFrameError,
    RequestError,
    ResourceLimitError,
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
        return Message(**raw)


def _node(peer_id: str, **config_values: Any) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(host="127.0.0.1", port=0, max_frame_size=4096),
        discovery=LanDiscovery(
            discovery_port=0,
            broadcast_host="127.0.0.1",
            announce_interval=3600,
            default_discover_timeout=0,
        ),
        serializer=JsonSerializer(),
        config=PaqtoConfig(
            connect_timeout=1,
            send_timeout=1,
            request_timeout=1,
            acknowledgement_timeout=1,
            discover_timeout=0,
            max_message_size=2048,
            **config_values,
        ),
    )


def _lan_discovery(node: PaqtoNode) -> LanDiscovery:
    discovery = node.discovery
    assert isinstance(discovery, LanDiscovery)
    return discovery


def _known(source: PaqtoNode, target: PaqtoNode) -> DiscoveredPeer:
    payload = _lan_discovery(target)._announce_payload()
    source_discovery = _lan_discovery(source)
    source_discovery._datagram_received(
        json.dumps(payload).encode(),
        ("127.0.0.1", 0),
    )
    return source_discovery._discovered[target.peer.id]


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 1,
) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout=timeout)


async def _stop_cleanly(*nodes: PaqtoNode) -> None:
    await asyncio.gather(*(node.stop() for node in nodes), return_exceptions=True)
    await asyncio.sleep(0)
    for node in nodes:
        assert node._reader_tasks == {}
        assert node._handler_tasks == set()
        assert node._heartbeat_tasks == {}
        assert node._reconnect_tasks == {}
        assert node._outbound_channels == {}
        assert node._event_task is None
        assert node.inbound_queue_size == 0
        assert node.outbound_queue_size == 0
        assert node.active_connection_count == 0
        assert node.pending_request_count == 0
        assert node.pending_acknowledgement_count == 0


@pytest.mark.asyncio
async def test_handler_failure_is_observable_and_does_not_close_connection(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sender = _node("sender")
    receiver = _node("receiver", handler_concurrency=1)
    handler_error = asyncio.Event()
    second_received = asyncio.Event()
    observed: list[NodeEvent] = []

    @receiver.on_event(NodeEventType.HANDLER_ERROR)
    def observe(event: NodeEvent) -> None:
        observed.append(event)
        handler_error.set()

    @receiver.on_message("broken")
    def broken(message: Message) -> None:
        raise RuntimeError("handler-secret-value")

    @receiver.on_message("healthy")
    def healthy(message: Message) -> None:
        second_received.set()

    caplog.set_level(logging.DEBUG, logger="paqto.core.node")
    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        connection = await sender.connect(target)

        await sender.send(target, "payload-secret-value", type="broken")
        await asyncio.wait_for(handler_error.wait(), timeout=1)
        await sender.send(target, "ordinary", type="healthy")
        await asyncio.wait_for(second_received.wait(), timeout=1)

        assert not connection.is_closed
        assert observed[0].peer_id == "sender"
        assert observed[0].metadata["message_type"] == "broken"
        rendered = " ".join(record.getMessage() for record in caplog.records)
        assert "payload-secret-value" not in rendered
        assert "handler-secret-value" not in rendered
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_slow_handler_is_bounded_and_does_not_spawn_per_message_tasks() -> None:
    sender = _node("sender")
    receiver = _node(
        "receiver",
        handler_concurrency=1,
        max_inbound_queue=1,
        inbound_backpressure=BackpressurePolicy.WAIT,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    handled: list[int] = []

    @receiver.on_message("slow")
    async def slow(message: Message) -> None:
        entered.set()
        await release.wait()
        handled.append(int(message.payload))

    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        await sender.connect(target)

        await sender.send(target, 1, type="slow")
        await asyncio.wait_for(entered.wait(), timeout=1)
        await sender.send(target, 2, type="slow")
        await sender.send(target, 3, type="slow")
        await _wait_until(lambda: receiver.inbound_queue_size == 1)

        assert len(receiver._handler_tasks) == 1
        assert all(not task.done() for task in receiver._handler_tasks)
        release.set()
        await _wait_until(lambda: handled == [1, 2, 3])
    finally:
        release.set()
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_reject_policy_closes_a_peer_that_overflows_inbound_queue() -> None:
    sender = _node("sender")
    receiver = _node(
        "receiver",
        handler_concurrency=1,
        max_inbound_queue=1,
        inbound_backpressure=BackpressurePolicy.REJECT,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    limited = asyncio.Event()

    @receiver.on_message("slow")
    async def slow(message: Message) -> None:
        entered.set()
        await release.wait()

    @receiver.on_event(NodeEventType.RESOURCE_LIMIT)
    def on_limit(event: NodeEvent) -> None:
        limited.set()

    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        await sender.connect(target)
        await sender.send(target, 1, type="slow")
        await asyncio.wait_for(entered.wait(), timeout=1)
        await sender.send(target, 2, type="slow")
        await sender.send(target, 3, type="slow")

        await asyncio.wait_for(limited.wait(), timeout=1)
        await _wait_until(lambda: receiver.connection_for_peer("sender") is None)
    finally:
        release.set()
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_outbound_queue_rejects_without_unbounded_send_waiters() -> None:
    sender = _node(
        "sender",
        max_outbound_queue=1,
        outbound_backpressure=BackpressurePolicy.REJECT,
    )
    receiver = _node("receiver")
    writer_entered = asyncio.Event()
    release_writer = asyncio.Event()

    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        connection = await sender.connect(target)
        original_send = connection.send_frame

        async def blocked_send(data: bytes) -> None:
            writer_entered.set()
            await release_writer.wait()
            await original_send(data)

        connection.send_frame = blocked_send  # type: ignore[method-assign]
        first = asyncio.create_task(sender.send(target, 1))
        await asyncio.wait_for(writer_entered.wait(), timeout=1)
        second = asyncio.create_task(sender.send(target, 2))
        await _wait_until(lambda: sender.outbound_queue_size == 1)

        with pytest.raises(ResourceLimitError, match="Outbound frame queue"):
            await sender.send(target, 3)

        release_writer.set()
        await asyncio.gather(first, second)
        assert sender.outbound_queue_size == 0
    finally:
        release_writer.set()
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_pending_request_limit_and_cleanup() -> None:
    requester = _node("requester", max_pending_requests=1)
    responder = _node("responder")
    received = asyncio.Event()

    @responder.on_message("hold")
    def hold(message: Message) -> None:
        received.set()

    try:
        await requester.start()
        await responder.start()
        target = _known(requester, responder)
        pending = asyncio.create_task(
            requester.request(target, 1, type="hold", timeout=2)
        )
        await asyncio.wait_for(received.wait(), timeout=1)

        with pytest.raises(ResourceLimitError, match="pending requests"):
            await requester.request(target, 2, type="hold", timeout=2)

        await requester.disconnect(Peer(id="responder"))
        with pytest.raises(RequestError, match="Connection closed"):
            await pending
        assert requester.pending_request_count == 0
    finally:
        await _stop_cleanly(requester, responder)


@pytest.mark.asyncio
async def test_invalid_application_envelope_is_rejected_before_send() -> None:
    sender = _node("sender")
    receiver = _node("receiver")
    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        connection = await sender.connect(target)

        with pytest.raises(ProtocolFrameError, match="type"):
            await sender.send(target, None, type="")
        with pytest.raises(ProtocolFrameError, match="headers"):
            await sender.send(
                target,
                None,
                headers={"invalid": 1},  # type: ignore[dict-item]
            )
        assert not connection.is_closed
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_pending_acknowledgement_limit_is_bounded() -> None:
    sender = _node("sender", max_pending_acknowledgements=1)
    receiver = _node("receiver")
    ack_suppressed = asyncio.Event()

    async def suppress_ack(connection: Any, message_id: str) -> None:
        ack_suppressed.set()

    receiver._send_acknowledgement = suppress_ack  # type: ignore[method-assign]
    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        pending = asyncio.create_task(
            sender.send(
                target,
                1,
                require_ack=True,
                acknowledgement_timeout=2,
            )
        )
        await asyncio.wait_for(ack_suppressed.wait(), timeout=1)

        with pytest.raises(ResourceLimitError, match="pending acknowledgements"):
            await sender.send(target, 2, require_ack=True)

        await sender.disconnect(Peer(id="receiver"))
        with pytest.raises(PaqtoError):
            await pending
        assert sender.pending_acknowledgement_count == 0
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_event_listener_failure_is_logged_and_network_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sender = _node("sender")
    receiver = _node("receiver")
    received = asyncio.Event()

    @sender.on_event(NodeEventType.CONNECTED)
    def broken_listener(event: NodeEvent) -> None:
        raise RuntimeError("event-listener-failed")

    @receiver.on_message("data")
    def handle(message: Message) -> None:
        received.set()

    caplog.set_level(logging.ERROR, logger="paqto.core.node")
    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        await sender.connect(target)
        await _wait_until(
            lambda: any(
                record.getMessage() == "Paqto event listener failed"
                for record in caplog.records
            )
        )
        await sender.send(target, "still-alive", type="data")
        await asyncio.wait_for(received.wait(), timeout=1)
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_discovery_and_connection_events_are_emitted() -> None:
    sender = _node("sender")
    receiver = _node("receiver")
    events: list[NodeEventType] = []

    @sender.on_event()
    def observe(event: NodeEvent) -> None:
        events.append(event.type)

    try:
        await sender.start()
        await receiver.start()
        _known(sender, receiver)
        discovered = await sender.discover(timeout=0.1)
        await sender.connect(discovered[0])
        await _wait_until(lambda: NodeEventType.CONNECTED in events)

        assert NodeEventType.PEER_DISCOVERED in events
        assert NodeEventType.CONNECTING in events
        assert NodeEventType.CONNECTED in events
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_shutdown_under_load_and_repeated_start_stop_are_clean() -> None:
    sender = _node("sender")
    receiver = _node("receiver", handler_concurrency=2, max_inbound_queue=8)
    entered = asyncio.Event()
    release = asyncio.Event()

    @receiver.on_message("load")
    async def load(message: Message) -> None:
        entered.set()
        await release.wait()

    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        await sender.connect(target)
        await asyncio.gather(
            *(sender.send(target, value, type="load") for value in range(10))
        )
        await asyncio.wait_for(entered.wait(), timeout=1)

        await receiver.stop()
        assert receiver._handler_tasks == set()
        assert receiver._reader_tasks == {}
        assert receiver._outbound_channels == {}
        assert receiver.inbound_queue_size == 0

        await receiver.start()
        await receiver.stop()
        assert receiver._event_task is None
        assert receiver._handler_tasks == set()
    finally:
        release.set()
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_messages_across_multiple_connections_within_limit() -> None:
    hub = _node("hub", max_connections=4, handler_concurrency=2)
    peers = [_node(f"peer-{index}") for index in range(3)]
    received: set[str] = set()

    @hub.on_message("fan-in")
    def receive(message: Message) -> None:
        assert message.sender is not None
        received.add(message.sender)

    try:
        await hub.start()
        await asyncio.gather(*(peer.start() for peer in peers))
        targets = [_known(peer, hub) for peer in peers]
        await asyncio.gather(
            *(
                peer.send(target, index, type="fan-in")
                for index, (peer, target) in enumerate(zip(peers, targets))
            )
        )
        await _wait_until(lambda: len(received) == 3)
        assert hub.active_connection_count == 3
    finally:
        await _stop_cleanly(hub, *peers)


@pytest.mark.asyncio
async def test_connection_limit_rejects_excess_physical_connection() -> None:
    hub = _node("hub", max_connections=1)
    first = _node("first")
    second = _node("second")
    limited = asyncio.Event()

    @hub.on_event(NodeEventType.RESOURCE_LIMIT)
    def observe_limit(event: NodeEvent) -> None:
        limited.set()

    try:
        await hub.start()
        await first.start()
        await second.start()
        await first.connect(_known(first, hub))
        assert hub.active_connection_count == 1

        with pytest.raises(PaqtoError):
            await second.connect(_known(second, hub))
        await asyncio.wait_for(limited.wait(), timeout=1)
        assert hub.active_connection_count == 1
    finally:
        await _stop_cleanly(hub, first, second)


@pytest.mark.asyncio
async def test_handler_close_policy_affects_only_failing_connection() -> None:
    sender = _node("sender")
    receiver = _node(
        "receiver",
        handler_error_policy=HandlerErrorPolicy.CLOSE_CONNECTION,
    )
    failed = asyncio.Event()

    @receiver.on_message("broken")
    def broken(message: Message) -> None:
        raise RuntimeError("failure")

    @receiver.on_event(NodeEventType.HANDLER_ERROR)
    def observe(event: NodeEvent) -> None:
        failed.set()

    try:
        await sender.start()
        await receiver.start()
        target = _known(sender, receiver)
        await sender.connect(target)
        await sender.send(target, None, type="broken")
        await asyncio.wait_for(failed.wait(), timeout=1)
        await _wait_until(lambda: receiver.connection_for_peer("sender") is None)
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_idle_timeout_closes_silent_connection() -> None:
    sender = _node("sender")
    receiver = _node("receiver", idle_timeout=0.02)
    disconnected = asyncio.Event()

    @receiver.on_event(NodeEventType.DISCONNECTED)
    def observe(event: NodeEvent) -> None:
        disconnected.set()

    try:
        await sender.start()
        await receiver.start()
        await sender.connect(_known(sender, receiver))

        await asyncio.wait_for(disconnected.wait(), timeout=1)
        assert receiver.connection_for_peer("sender") is None
    finally:
        await _stop_cleanly(sender, receiver)


@pytest.mark.asyncio
async def test_peer_expiry_event_is_emitted_when_freshness_is_rechecked() -> None:
    node = _node("node", peer_ttl=0.01)
    expired = asyncio.Event()
    stale = DiscoveredPeer(peer=Peer(id="stale"))
    stale.last_seen = stale.last_seen.replace(year=2000)
    node._known_peers[stale.peer.id] = stale

    @node.on_event(NodeEventType.PEER_EXPIRED)
    def observe(event: NodeEvent) -> None:
        assert event.peer_id == "stale"
        expired.set()

    try:
        await node.start()
        await node.discover(timeout=0.1)
        await asyncio.wait_for(expired.wait(), timeout=1)
        assert "stale" not in node._known_peers
    finally:
        await _stop_cleanly(node)
