import asyncio
import json
import socket
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

from paqto.core import (
    ConnectionState,
    DiscoveredPeer,
    DiscoveryError,
    Endpoint,
    Message,
    PaqtoConfig,
    PaqtoNode,
    Peer,
    ReconnectPolicy,
    Serializer,
)
from paqto.core.discovery import DiscoveryService
from paqto.lan import LanTransport, TlsConfig

CERTIFICATES = Path(__file__).parent.parent / "certificates"
CA = CERTIFICATES / "ca.pem"
NODE_A_CERT = CERTIFICATES / "node-a.pem"
NODE_A_KEY = CERTIFICATES / "node-a-key.pem"
NODE_B_CERT = CERTIFICATES / "node-b.pem"
NODE_B_KEY = CERTIFICATES / "node-b-key.pem"


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
        return Message(**raw)


class MutableDiscovery(DiscoveryService):
    def __init__(self) -> None:
        self.target: PaqtoNode | None = None
        self.started = False
        self.published_endpoints: list[list[Endpoint]] = []

    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        self.started = True
        self.published_endpoints.append(list(endpoints))

    async def stop(self) -> None:
        self.started = False

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        if not self.started:
            raise DiscoveryError("Discovery is not running.")
        target = self.target
        if target is None or target._listener is None:
            return []
        return [
            DiscoveredPeer(
                peer=Peer(id=target.peer.id, name=target.peer.name),
                endpoints=[target._listener.local_endpoint],
            )
        ]


class PassiveDiscovery(DiscoveryService):
    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        return []


def _unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _node(
    peer_id: str,
    discovery: DiscoveryService,
    *,
    port: int = 0,
    reconnect: ReconnectPolicy | None = None,
    tls: TlsConfig | None = None,
) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(host="127.0.0.1", port=port, tls=tls),
        discovery=discovery,
        serializer=JsonSerializer(),
        config=PaqtoConfig(
            connect_timeout=2,
            send_timeout=2,
            discover_timeout=0,
            handshake_timeout=2,
            reconnect=reconnect or ReconnectPolicy(),
        ),
    )


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2) -> None:
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
        assert node.is_running is False
        assert node.active_connection_count == 0
        assert node._reader_tasks == {}
        assert node._heartbeat_tasks == {}
        assert node._reconnect_tasks == {}
        assert node._outbound_channels == {}


@pytest.mark.asyncio
async def test_network_refresh_reconnects_with_a_new_remote_endpoint() -> None:
    reconnect = ReconnectPolicy(
        enabled=True,
        initial_delay=0.01,
        maximum_delay=0.02,
        max_attempts=50,
    )
    discovery = MutableDiscovery()
    source_tls = TlsConfig(certfile=NODE_A_CERT, keyfile=NODE_A_KEY, cafile=CA)
    target_tls = TlsConfig(certfile=NODE_B_CERT, keyfile=NODE_B_KEY, cafile=CA)
    source = _node(
        "source",
        discovery,
        reconnect=reconnect,
        tls=source_tls,
    )
    first_port = _unused_tcp_port()
    first_target = _node(
        "target",
        PassiveDiscovery(),
        port=first_port,
        tls=target_tls,
    )
    second_port = _unused_tcp_port()
    while second_port == first_port:
        second_port = _unused_tcp_port()
    second_target = _node(
        "target",
        PassiveDiscovery(),
        port=second_port,
        tls=target_tls,
    )
    received = asyncio.Event()

    @second_target.on_message("after-refresh")
    def receive(message: Message) -> None:
        received.set()

    try:
        await source.start()
        await first_target.start()
        discovery.target = first_target
        first_observation = (await source.discover())[0]
        first_connection = await source.connect(first_observation)
        assert first_connection.security_info.encrypted is True
        old_address = first_observation.endpoints[0].address

        await first_target.stop()
        await _wait_until(
            lambda: source.connection_state("target")
            is not ConnectionState.CONNECTED
        )
        await second_target.start()
        discovery.target = second_target

        refreshed = await source.network_changed()
        new_address = refreshed[0].endpoints[0].address
        assert new_address != old_address

        await _wait_until(
            lambda: source.connection_state("target") is ConnectionState.CONNECTED
        )
        replacement = source.connection_for_peer("target")
        assert replacement is not None
        assert replacement is not first_connection
        assert replacement.remote_endpoint.address == new_address
        assert replacement.security_info.encrypted is True
        assert replacement.security_info.authenticated is True

        await source.send(Peer(id="target"), "online", type="after-refresh")
        await asyncio.wait_for(received.wait(), timeout=1)
    finally:
        await _stop_cleanly(source, first_target, second_target)


@pytest.mark.asyncio
@pytest.mark.parametrize("tls_handshake", [False, True])
async def test_stop_cancels_connection_setup_and_closes_socket(
    tls_handshake: bool,
) -> None:
    accepted = asyncio.Event()
    release_server = asyncio.Event()
    server_writers: set[asyncio.StreamWriter] = set()

    async def hold_connection(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        server_writers.add(writer)
        accepted.set()
        await release_server.wait()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(hold_connection, "127.0.0.1", 0)
    assert server.sockets
    port = int(server.sockets[0].getsockname()[1])
    tls = (
        TlsConfig(certfile=NODE_A_CERT, keyfile=NODE_A_KEY, cafile=CA)
        if tls_handshake
        else None
    )
    node = _node("source", PassiveDiscovery(), tls=tls)
    target = DiscoveredPeer(
        peer=Peer(id="unresponsive"),
        endpoints=[Endpoint("lan", f"tcp://127.0.0.1:{port}")],
    )

    try:
        await node.start()
        connecting = asyncio.create_task(node.connect(target))
        await asyncio.wait_for(accepted.wait(), timeout=1)

        await asyncio.wait_for(node.stop(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await connecting

        assert node.active_connection_count == 0
        assert node._connections._in_flight == {}
        assert node._sessions == {}
    finally:
        release_server.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(
            *(writer.wait_closed() for writer in server_writers),
            return_exceptions=True,
        )
        await node.stop()


@pytest.mark.asyncio
async def test_repeated_lifecycle_runs_inside_the_existing_event_loop() -> None:
    node = _node("source", PassiveDiscovery())
    loop = asyncio.get_running_loop()

    for _ in range(2):
        await node.start()
        assert asyncio.get_running_loop() is loop
        await node.stop()

    assert node._listener is None
    assert node._known_peers == {}
