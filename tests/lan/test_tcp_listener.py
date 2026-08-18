import asyncio

import pytest

from paqto.core.errors import ConnectionClosedError
from paqto.lan.address import parse_tcp_address
from paqto.lan.connection import TcpConnection
from paqto.lan.listener import TcpListener


@pytest.mark.asyncio
async def test_start_on_port_zero_exposes_real_local_endpoint() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)

    try:
        await listener.start()
        parsed = parse_tcp_address(listener.local_endpoint.address)

        assert listener.local_endpoint.transport == "lan"
        assert parsed.host == "127.0.0.1"
        assert parsed.port > 0
    finally:
        await asyncio.wait_for(listener.close(), timeout=1)


@pytest.mark.asyncio
async def test_accept_returns_tcp_connection() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)

    try:
        await listener.start()
        parsed = parse_tcp_address(listener.local_endpoint.address)
        _, writer = await asyncio.open_connection(parsed.host, parsed.port)
        connection = await asyncio.wait_for(listener.accept(), timeout=1)

        assert isinstance(connection, TcpConnection)
    finally:
        writer.close()
        await writer.wait_closed()
        await connection.close()
        await listener.close()


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)

    await listener.start()
    await listener.close()
    await listener.close()


@pytest.mark.asyncio
async def test_close_wakes_all_pending_accepts() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)
    await listener.start()
    first = asyncio.create_task(listener.accept())
    second = asyncio.create_task(listener.accept())
    await asyncio.sleep(0)

    await listener.close()

    for task in (first, second):
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_close_closes_connections_waiting_in_accept_queue() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    reader, writer = await asyncio.open_connection(parsed.host, parsed.port)

    try:
        for _ in range(10):
            if listener._accepted:
                break
            await asyncio.sleep(0)
        assert listener._accepted

        await asyncio.wait_for(listener.close(), timeout=1)

        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        await listener.close()


@pytest.mark.asyncio
async def test_close_closes_already_accepted_connections() -> None:
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    reader, writer = await asyncio.open_connection(parsed.host, parsed.port)
    connection = await asyncio.wait_for(listener.accept(), timeout=1)

    try:
        await asyncio.wait_for(listener.close(), timeout=1)

        assert connection.is_closed
        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        await connection.close()
        await listener.close()


@pytest.mark.asyncio
async def test_pending_accept_queue_rejects_excess_connections() -> None:
    listener = TcpListener(
        host="127.0.0.1",
        port=0,
        max_frame_size=1024,
        max_pending_accepts=1,
    )
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    _first_reader, first_writer = await asyncio.open_connection(
        parsed.host,
        parsed.port,
    )

    try:
        for _ in range(20):
            if len(listener._accepted) == 1:
                break
            await asyncio.sleep(0)
        assert len(listener._accepted) == 1

        second_reader, second_writer = await asyncio.open_connection(
            parsed.host,
            parsed.port,
        )
        try:
            assert await asyncio.wait_for(second_reader.read(), timeout=1) == b""
            assert len(listener._accepted) == 1
            accepted = await asyncio.wait_for(listener.accept(), timeout=1)
            assert not accepted.is_closed
            await accepted.close()
        finally:
            second_writer.close()
            await second_writer.wait_closed()
    finally:
        first_writer.close()
        await first_writer.wait_closed()
        await listener.close()


@pytest.mark.parametrize("value", [0, -1])
def test_pending_accept_limit_must_be_positive(value: int) -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        TcpListener(
            host="127.0.0.1",
            port=0,
            max_frame_size=1024,
            max_pending_accepts=value,
        )


@pytest.mark.asyncio
async def test_close_waits_for_concurrent_start_and_closes_created_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class FakeSocket:
        def getsockname(self) -> tuple[str, int]:
            return ("127.0.0.1", 54321)

    class FakeServer:
        def __init__(self) -> None:
            self.sockets = [FakeSocket()]
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

    server = FakeServer()

    async def delayed_start_server(*args: object, **kwargs: object) -> FakeServer:
        entered.set()
        await release.wait()
        return server

    monkeypatch.setattr(asyncio, "start_server", delayed_start_server)
    listener = TcpListener(host="127.0.0.1", port=0, max_frame_size=1024)
    starting = asyncio.create_task(listener.start())
    await entered.wait()
    closing = asyncio.create_task(listener.close())
    await asyncio.sleep(0)
    release.set()

    await asyncio.gather(starting, closing)

    assert server.closed is True
    assert listener._server is None
    assert listener._closed is True
