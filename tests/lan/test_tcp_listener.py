import asyncio

import pytest

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
        await listener.close()


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
