import asyncio
import json
import socket
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from paqto.core import (
    ConnectionState,
    DiscoveredPeer,
    Message,
    PaqtoConfig,
    PaqtoNode,
    Peer,
    PeerExpiredError,
    ReconnectPolicy,
    RequestError,
    Serializer,
)
from paqto.lan import LanDiscovery, LanTransport, TlsConfig

CERTIFICATES = Path(__file__).parent.parent / "certificates"
CA = CERTIFICATES / "ca.pem"
NODE_A_CERT = CERTIFICATES / "node-a.pem"
NODE_A_KEY = CERTIFICATES / "node-a-key.pem"
NODE_B_CERT = CERTIFICATES / "node-b.pem"
NODE_B_KEY = CERTIFICATES / "node-b-key.pem"
TEST_IDENTITY_PREFIX = "urn:test:peer:"


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


def _identity_from_test_uri(certificate: Mapping[str, Any]) -> str | None:
    for kind, value in certificate.get("subjectAltName", ()):
        if kind == "URI" and value.startswith(TEST_IDENTITY_PREFIX):
            return value.removeprefix(TEST_IDENTITY_PREFIX)
    return None


def _unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _node(
    peer_id: str,
    *,
    port: int = 0,
    reconnect: ReconnectPolicy | None = None,
    heartbeat_interval: float | None = None,
    heartbeat_timeout: float = 0.05,
    tls: TlsConfig | None = None,
    require_authenticated_peer_id_match: bool = False,
) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(host="127.0.0.1", port=port, tls=tls),
        discovery=LanDiscovery(
            discovery_port=0,
            broadcast_host="127.0.0.1",
            announce_interval=3600,
            default_discover_timeout=0,
        ),
        serializer=JsonSerializer(),
        config=PaqtoConfig(
            connect_timeout=0.2,
            send_timeout=0.2,
            discover_timeout=0,
            request_timeout=0.5,
            handshake_timeout=0.2,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            reconnect=reconnect or ReconnectPolicy(),
            require_authenticated_peer_id_match=require_authenticated_peer_id_match,
        ),
    )


def _known(source: PaqtoNode, target: PaqtoNode) -> DiscoveredPeer:
    assert target._listener is not None
    discovered = DiscoveredPeer(
        peer=target.peer,
        endpoints=[target._listener.local_endpoint],
    )
    source._remember(discovered)
    return discovered


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 1,
) -> None:
    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), timeout=timeout)


async def _stop_cleanly(*nodes: PaqtoNode) -> None:
    results = await asyncio.gather(
        *(node.stop() for node in nodes),
        return_exceptions=True,
    )
    assert not [result for result in results if isinstance(result, BaseException)]
    for node in nodes:
        assert node._reader_tasks == {}
        assert node._handler_tasks == set()
        assert node._heartbeat_tasks == {}
        assert node._reconnect_tasks == {}
        assert node._pending_heartbeats == {}
        assert node.pending_request_count == 0
        assert node.pending_acknowledgement_count == 0


@pytest.mark.asyncio
async def test_simultaneous_opens_select_one_deterministic_session() -> None:
    node_a = _node("node-a")
    node_b = _node("node-b")
    received = asyncio.Event()

    @node_b.on_message("probe")
    def receive(message: Message) -> None:
        received.set()

    try:
        await node_a.start()
        await node_b.start()
        target_b = _known(node_a, node_b)
        target_a = _known(node_b, node_a)

        connection_a, connection_b = await asyncio.gather(
            node_a.connect(target_b),
            node_b.connect(target_a),
        )
        await _wait_until(
            lambda: len(node_a._sessions) == 1 and len(node_b._sessions) == 1
        )

        assert node_a.connection_for_peer("node-b") is connection_a
        assert node_b.connection_for_peer("node-a") is connection_b
        assert node_a._session_directions[id(connection_a)].value == "outbound"
        assert node_b._session_directions[id(connection_b)].value == "inbound"
        assert node_a.connection_state("node-b") is ConnectionState.CONNECTED
        assert node_b.connection_state("node-a") is ConnectionState.CONNECTED

        await node_a.send(target_b, "data", type="probe")
        await asyncio.wait_for(received.wait(), timeout=0.5)
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_explicit_disconnect_suppresses_reconnect_until_manual_connect() -> None:
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.01,
        maximum_delay=0.02,
        max_attempts=3,
    )
    node_a = _node("node-a", reconnect=policy)
    node_b = _node("node-b")

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        first = await node_a.connect(target)

        await node_a.disconnect(target.peer)

        assert first.is_closed
        assert node_a.connection_state("node-b") is ConnectionState.CLOSED
        assert node_a.reconnect_task_count == 0

        second = await node_a.connect(target)
        assert second is not first
        assert node_a.connection_state("node-b") is ConnectionState.CONNECTED
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_heartbeat_ping_pong_keeps_session_alive() -> None:
    node_a = _node("node-a", heartbeat_interval=0.01, heartbeat_timeout=0.05)
    node_b = _node("node-b")
    pong_sent = asyncio.Event()
    original = node_b._send_heartbeat_response

    async def tracked_response(connection: Any, ping_id: str) -> None:
        await original(connection, ping_id)
        pong_sent.set()

    node_b._send_heartbeat_response = tracked_response  # type: ignore[method-assign]

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        connection = await node_a.connect(target)

        await asyncio.wait_for(pong_sent.wait(), timeout=0.5)

        assert connection.is_closed is False
        assert node_a.connection_state("node-b") is ConnectionState.CONNECTED
        assert node_a.heartbeat_task_count == 1
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_heartbeat_timeout_closes_unresponsive_peer() -> None:
    node_a = _node("node-a", heartbeat_interval=0.01, heartbeat_timeout=0.015)
    node_b = _node("node-b")
    ping_received = asyncio.Event()

    async def suppress_response(connection: Any, ping_id: str) -> None:
        ping_received.set()

    node_b._send_heartbeat_response = suppress_response  # type: ignore[method-assign]

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        connection = await node_a.connect(target)

        await asyncio.wait_for(ping_received.wait(), timeout=0.5)
        await _wait_until(
            lambda: node_a.connection_state("node-b")
            is ConnectionState.DISCONNECTED
            and node_a.heartbeat_task_count == 0
        )

        assert connection.is_closed
        assert node_a.connection_for_peer("node-b") is None
        assert node_a.heartbeat_task_count == 0
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_reconnect_creates_new_session_and_old_request_stays_failed() -> None:
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.01,
        multiplier=1.5,
        maximum_delay=0.03,
        max_attempts=20,
    )
    port = _unused_tcp_port()
    node_a = _node("node-a", reconnect=policy)
    node_b = _node("node-b", port=port)
    old_request_received = asyncio.Event()

    @node_b.on_message("hold")
    def hold(message: Message) -> None:
        old_request_received.set()

    @node_b.on_message("echo")
    async def echo(message: Message) -> None:
        await node_b.reply(message, message.payload)

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        old_connection = await node_a.connect(target)
        old_session = node_a.session_for(old_connection)
        pending = asyncio.create_task(
            node_a.request(target, "old", type="hold", timeout=0.5)
        )
        await asyncio.wait_for(old_request_received.wait(), timeout=0.5)

        await node_b.stop()

        with pytest.raises(RequestError, match="Connection closed"):
            await pending
        assert node_a.pending_request_count == 0
        await _wait_until(lambda: node_a.reconnect_task_count == 1)

        await node_b.start()
        await _wait_until(
            lambda: node_a.connection_state("node-b") is ConnectionState.CONNECTED
        )
        new_connection = node_a.connection_for_peer("node-b")
        assert new_connection is not None
        assert new_connection is not old_connection
        assert node_a.session_for(new_connection) is not old_session

        response = await node_a.request(target, "new", type="echo", timeout=0.5)
        assert response.payload == "new"
        assert node_a.pending_request_count == 0
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_failed_reconnect_uses_bounded_exponential_backoff() -> None:
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.01,
        multiplier=2,
        maximum_delay=0.04,
        max_attempts=3,
    )
    port = _unused_tcp_port()
    node_a = _node("node-a", reconnect=policy)
    node_b = _node("node-b", port=port)
    delays: list[float] = []

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    node_a._sleep = controlled_sleep  # type: ignore[method-assign]

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        await node_a.connect(target)

        await node_b.stop()
        await _wait_until(
            lambda: len(delays) == 3 and node_a.reconnect_task_count == 0
        )

        assert delays == pytest.approx([0.01, 0.02, 0.04])
        assert node_a.connection_state("node-b") is ConnectionState.DISCONNECTED
        assert node_a.last_reconnect_error("node-b") is not None
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_stop_cancels_reconnect_during_backoff() -> None:
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.1,
        maximum_delay=0.1,
        max_attempts=None,
    )
    port = _unused_tcp_port()
    node_a = _node("node-a", reconnect=policy)
    node_b = _node("node-b", port=port)
    sleep_entered = asyncio.Event()
    sleep_cancelled = asyncio.Event()
    blocker = asyncio.Event()

    async def controlled_sleep(delay: float) -> None:
        sleep_entered.set()
        try:
            await blocker.wait()
        finally:
            sleep_cancelled.set()

    node_a._sleep = controlled_sleep  # type: ignore[method-assign]

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        await node_a.connect(target)
        await node_b.stop()
        await asyncio.wait_for(sleep_entered.wait(), timeout=0.5)

        await node_a.stop()

        assert sleep_cancelled.is_set()
        assert node_a.reconnect_task_count == 0
        assert node_a.heartbeat_task_count == 0
    finally:
        await _stop_cleanly(node_a, node_b)


@pytest.mark.asyncio
async def test_stale_discovery_cannot_open_a_new_connection() -> None:
    node = _node("node-a")
    node.config.peer_ttl = 0.01
    stale = DiscoveredPeer(peer=Peer(id="remote"))
    stale.last_seen = stale.last_seen.replace(year=2000)

    try:
        await node.start()
        with pytest.raises(PeerExpiredError, match="expired"):
            await node.connect(stale)
    finally:
        await _stop_cleanly(node)


@pytest.mark.asyncio
async def test_tls_authentication_and_handshake_repeat_after_reconnect() -> None:
    resolver_calls = 0

    def counting_resolver(certificate: Mapping[str, Any]) -> str | None:
        nonlocal resolver_calls
        resolver_calls += 1
        return _identity_from_test_uri(certificate)

    source_tls = TlsConfig(
        certfile=NODE_A_CERT,
        keyfile=NODE_A_KEY,
        cafile=CA,
        peer_identity_resolver=counting_resolver,
    )
    target_tls = TlsConfig(
        certfile=NODE_B_CERT,
        keyfile=NODE_B_KEY,
        cafile=CA,
        require_client_certificate=True,
        peer_identity_resolver=_identity_from_test_uri,
    )
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.01,
        maximum_delay=0.03,
        max_attempts=20,
    )
    port = _unused_tcp_port()
    node_a = _node(
        "node-a",
        reconnect=policy,
        tls=source_tls,
        require_authenticated_peer_id_match=True,
    )
    node_b = _node(
        "node-b",
        port=port,
        tls=target_tls,
        require_authenticated_peer_id_match=True,
    )

    try:
        await node_a.start()
        await node_b.start()
        target = _known(node_a, node_b)
        first = await node_a.connect(target)
        assert first.security_info.authenticated_peer_id == "node-b"
        assert node_a.session_for(first).peer_id_authenticated is True  # type: ignore[union-attr]

        await node_b.stop()
        await _wait_until(lambda: node_a.reconnect_task_count == 1)
        await node_b.start()
        await _wait_until(
            lambda: node_a.connection_state("node-b") is ConnectionState.CONNECTED
        )

        second = node_a.connection_for_peer("node-b")
        assert second is not None and second is not first
        assert second.security_info.authenticated is True
        assert second.security_info.authenticated_peer_id == "node-b"
        second_session = node_a.session_for(second)
        assert second_session is not None
        assert second_session.peer_id_authenticated is True
        assert resolver_calls >= 2
    finally:
        await _stop_cleanly(node_a, node_b)
