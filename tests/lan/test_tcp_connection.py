import asyncio

import pytest

from paqto.core.errors import ConnectionClosedError, TransportError
from paqto.lan.address import endpoint_from_sockname, parse_tcp_address
from paqto.lan.connection import TcpConnection
from paqto.lan.listener import TcpListener


async def _open_connection_pair(
    *,
    max_frame_size: int = 1024 * 1024,
) -> tuple[TcpListener, TcpConnection, TcpConnection]:
    listener = TcpListener(
        host="127.0.0.1",
        port=0,
        max_frame_size=max_frame_size,
    )
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    reader, writer = await asyncio.open_connection(parsed.host, parsed.port)
    client = TcpConnection(
        reader,
        writer,
        local_endpoint=endpoint_from_sockname(writer.get_extra_info("sockname")),
        remote_endpoint=listener.local_endpoint,
        max_frame_size=max_frame_size,
    )
    server = await asyncio.wait_for(listener.accept(), timeout=1)
    return listener, client, server


async def _open_raw_client(
    *,
    max_frame_size: int,
) -> tuple[TcpListener, asyncio.StreamWriter, TcpConnection]:
    listener = TcpListener(
        host="127.0.0.1",
        port=0,
        max_frame_size=max_frame_size,
    )
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    _, writer = await asyncio.open_connection(parsed.host, parsed.port)
    server = await asyncio.wait_for(listener.accept(), timeout=1)
    return listener, writer, server


async def _close_all(*connections: TcpConnection) -> None:
    await asyncio.gather(
        *(connection.close() for connection in connections),
        return_exceptions=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b"small payload",
        b"",
        b"x" * 65_536,
    ],
    ids=["small", "empty", "medium"],
)
async def test_send_receive_frame_payloads(payload: bytes) -> None:
    listener, client, server = await _open_connection_pair()

    try:
        await client.send_frame(payload)

        assert await server.receive_frame() == payload
        assert client.security_info.encrypted is False
        assert client.security_info.authenticated is False
    finally:
        await _close_all(client, server)
        await listener.close()


@pytest.mark.asyncio
async def test_concurrent_receives_preserve_frame_boundaries() -> None:
    listener, client, server = await _open_connection_pair()

    try:
        first_receive = asyncio.create_task(server.receive_frame())
        second_receive = asyncio.create_task(server.receive_frame())
        await asyncio.sleep(0)

        await client.send_frame(b"first")
        await client.send_frame(b"second")

        assert await first_receive == b"first"
        assert await second_receive == b"second"
    finally:
        await _close_all(client, server)
        await listener.close()


@pytest.mark.asyncio
async def test_two_consecutive_frames_are_read_separately() -> None:
    listener, client, server = await _open_connection_pair()

    try:
        await client.send_frame(b"first")
        await client.send_frame(b"second")

        assert await server.receive_frame() == b"first"
        assert await server.receive_frame() == b"second"
    finally:
        await _close_all(client, server)
        await listener.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_frame",
    [
        b"\x00\x00",
        (5).to_bytes(4, "big") + b"ab",
    ],
    ids=["incomplete-header", "truncated-payload"],
)
async def test_incomplete_header_or_truncated_payload_closes_connection(
    raw_frame: bytes,
) -> None:
    listener, writer, server = await _open_raw_client(max_frame_size=1024)

    try:
        writer.write(raw_frame)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        with pytest.raises(ConnectionClosedError):
            await server.receive_frame()
    finally:
        writer.close()
        await server.close()
        await listener.close()


@pytest.mark.asyncio
async def test_send_frame_rejects_payload_over_max_frame_size() -> None:
    listener, client, server = await _open_connection_pair(max_frame_size=3)

    try:
        with pytest.raises(TransportError):
            await client.send_frame(b"over")
    finally:
        await _close_all(client, server)
        await listener.close()


@pytest.mark.asyncio
async def test_receive_frame_rejects_frame_over_max_frame_size() -> None:
    listener, writer, server = await _open_raw_client(max_frame_size=3)

    try:
        writer.write((4).to_bytes(4, "big") + b"over")
        await writer.drain()

        with pytest.raises(TransportError):
            await server.receive_frame()
    finally:
        writer.close()
        await writer.wait_closed()
        await server.close()
        await listener.close()


@pytest.mark.asyncio
async def test_incomplete_frame_payload_times_out_and_closes_connection() -> None:
    listener = TcpListener(
        host="127.0.0.1",
        port=0,
        max_frame_size=1024,
        frame_payload_timeout=0.01,
    )
    await listener.start()
    parsed = parse_tcp_address(listener.local_endpoint.address)
    reader, writer = await asyncio.open_connection(parsed.host, parsed.port)
    server = await asyncio.wait_for(listener.accept(), timeout=1)

    try:
        writer.write((10).to_bytes(4, "big") + b"x")
        await writer.drain()

        with pytest.raises(TransportError, match="complete TCP frame payload"):
            await asyncio.wait_for(server.receive_frame(), timeout=1)

        assert server.is_closed
        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
    finally:
        writer.close()
        await writer.wait_closed()
        await server.close()
        await listener.close()
