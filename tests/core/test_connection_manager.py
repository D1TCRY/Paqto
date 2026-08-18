import asyncio

import pytest

from paqto.core.connection import Connection, ConnectionState
from paqto.core.endpoint import Endpoint
from paqto.core.listener import Listener
from paqto.core.manager import ConnectionManager
from paqto.core.peer import Peer
from paqto.core.transport import Transport


class FakeConnection(Connection):
    def __init__(self, endpoint: Endpoint) -> None:
        self._endpoint = endpoint
        self._closed = False

    @property
    def local_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def remote_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def send_frame(self, data: bytes) -> None:
        return None

    async def receive_frame(self) -> bytes:
        return b""

    async def close(self) -> None:
        self._closed = True


class FailingCloseConnection(FakeConnection):
    async def close(self) -> None:
        self._closed = True
        raise RuntimeError("close failed")


class BlockingTransport(Transport):
    def __init__(self) -> None:
        self.connect_calls = 0
        self.connect_entered = asyncio.Event()
        self.release_connect = asyncio.Event()

    @property
    def name(self) -> str:
        return "test"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        self.connect_calls += 1
        self.connect_entered.set()
        await self.release_connect.wait()
        return FakeConnection(endpoint)

    async def create_listener(self, bind: Endpoint | None = None) -> Listener:
        raise NotImplementedError


@pytest.mark.asyncio
async def test_concurrent_connects_reuse_one_connection() -> None:
    transport = BlockingTransport()
    manager = ConnectionManager(transport)
    peer = Peer(id="peer-1")
    endpoint = Endpoint(transport="test", address="test://peer-1")

    first = asyncio.create_task(manager.connect(peer, endpoint))
    await transport.connect_entered.wait()
    assert manager.state(peer) is ConnectionState.CONNECTING
    second = asyncio.create_task(manager.connect(peer, endpoint))
    await asyncio.sleep(0)
    transport.release_connect.set()

    first_connection, second_connection = await asyncio.gather(first, second)

    assert transport.connect_calls == 1
    assert first_connection is second_connection
    assert manager.state(peer) is ConnectionState.CONNECTED

    await manager.close_all()
    assert first_connection.is_closed
    assert manager.state(peer) is ConnectionState.CLOSED


@pytest.mark.asyncio
async def test_failed_preparation_is_closed_and_never_cached() -> None:
    transport = BlockingTransport()
    manager = ConnectionManager(transport)
    peer = Peer(id="peer-1")
    endpoint = Endpoint(transport="test", address="test://peer-1")
    transport.release_connect.set()
    prepared: list[Connection] = []

    async def fail_preparation(connection: Connection) -> None:
        prepared.append(connection)
        raise RuntimeError("handshake failed")

    with pytest.raises(RuntimeError, match="handshake failed"):
        await manager.connect(peer, endpoint, prepare=fail_preparation)

    assert manager.get(peer) is None
    assert len(prepared) == 1
    assert prepared[0].is_closed


@pytest.mark.asyncio
async def test_connects_for_different_peers_are_not_globally_serialized() -> None:
    transport = BlockingTransport()
    manager = ConnectionManager(transport)
    first_peer = Peer(id="peer-1")
    second_peer = Peer(id="peer-2")

    first = asyncio.create_task(
        manager.connect(
            first_peer,
            Endpoint(transport="test", address="test://peer-1"),
        )
    )
    second = asyncio.create_task(
        manager.connect(
            second_peer,
            Endpoint(transport="test", address="test://peer-2"),
        )
    )
    while transport.connect_calls < 2:
        await asyncio.sleep(0)

    assert manager.state(first_peer) is ConnectionState.CONNECTING
    assert manager.state(second_peer) is ConnectionState.CONNECTING
    transport.release_connect.set()
    await asyncio.gather(first, second)
    await manager.close_all()


@pytest.mark.asyncio
async def test_cancelling_one_waiter_does_not_cancel_shared_connect() -> None:
    transport = BlockingTransport()
    manager = ConnectionManager(transport)
    peer = Peer(id="peer-1")
    endpoint = Endpoint(transport="test", address="test://peer-1")

    cancelled_waiter = asyncio.create_task(manager.connect(peer, endpoint))
    await transport.connect_entered.wait()
    surviving_waiter = asyncio.create_task(manager.connect(peer, endpoint))
    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter
    transport.release_connect.set()

    connection = await surviving_waiter

    assert transport.connect_calls == 1
    assert manager.get(peer) is connection
    await manager.close_all()


@pytest.mark.asyncio
async def test_failed_preparation_with_failing_close_does_not_poison_single_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = BlockingTransport()
    manager = ConnectionManager(transport)
    peer = Peer(id="peer-1")
    endpoint = Endpoint(transport="test", address="test://peer-1")
    transport.release_connect.set()

    async def connect_with_failing_close(
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        return FailingCloseConnection(endpoint)

    monkeypatch.setattr(transport, "connect", connect_with_failing_close)

    async def fail_preparation(connection: Connection) -> None:
        raise RuntimeError("handshake failed")

    with pytest.raises(RuntimeError, match="handshake failed"):
        await manager.connect(peer, endpoint, prepare=fail_preparation)

    assert peer.id not in manager._in_flight
    assert manager.state(peer) is ConnectionState.DISCONNECTED


@pytest.mark.asyncio
async def test_close_peer_restores_terminal_state_when_connection_close_fails() -> None:
    manager = ConnectionManager(BlockingTransport())
    peer = Peer(id="peer-1")
    connection = FailingCloseConnection(
        Endpoint(transport="test", address="test://peer-1")
    )
    await manager.adopt(peer, connection)

    with pytest.raises(RuntimeError, match="close failed"):
        await manager.close_peer(peer)

    assert peer.id not in manager._closing_peers
    assert manager.state(peer) is ConnectionState.CLOSED
