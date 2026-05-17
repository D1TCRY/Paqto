import asyncio

import pytest

import paqto.lan as lan
from paqto.core.endpoint import Endpoint
from paqto.core.errors import TransportError
from paqto.lan.connection import TcpConnection
from paqto.lan.transport import LanTransport


def test_lan_package_exports_public_api() -> None:
    assert lan.LanTransport is LanTransport
    assert lan.TcpConnection is TcpConnection


def test_transport_name_is_lan() -> None:
    assert LanTransport().name == "lan"


@pytest.mark.asyncio
async def test_connect_rejects_endpoint_with_different_transport() -> None:
    transport = LanTransport(host="127.0.0.1")

    try:
        await transport.start()

        with pytest.raises(TransportError):
            await transport.connect(
                Endpoint(transport="memory", address="tcp://127.0.0.1:5050")
            )
    finally:
        await transport.stop()


@pytest.mark.asyncio
async def test_connect_opens_working_tcp_connection() -> None:
    server_transport = LanTransport(host="127.0.0.1", port=0)
    client_transport = LanTransport(host="127.0.0.1", port=0)
    accepted: TcpConnection | None = None

    try:
        await server_transport.start()
        await client_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()

        accept_task = asyncio.create_task(listener.accept())
        client = await client_transport.connect(listener.local_endpoint, timeout=1)
        accepted = await asyncio.wait_for(accept_task, timeout=1)

        await client.send_frame(b"ping")

        assert isinstance(client, TcpConnection)
        assert isinstance(accepted, TcpConnection)
        assert await accepted.receive_frame() == b"ping"
    finally:
        if accepted is not None:
            await accepted.close()
        await client_transport.stop()
        await server_transport.stop()


@pytest.mark.asyncio
async def test_connection_errors_are_converted_to_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_open_connection(host: str, port: int) -> None:
        raise OSError("connection failed")

    monkeypatch.setattr(asyncio, "open_connection", fail_open_connection)
    transport = LanTransport(host="127.0.0.1")
    endpoint = Endpoint(transport="lan", address="tcp://127.0.0.1:5050")

    try:
        await transport.start()

        with pytest.raises(TransportError):
            await transport.connect(endpoint, timeout=1)
    finally:
        await transport.stop()
