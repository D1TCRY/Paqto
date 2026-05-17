from __future__ import annotations

import asyncio
from typing import Any

from paqto.core.endpoint import Endpoint
from paqto.core.errors import ConnectionClosedError, TransportError
from paqto.core.listener import Listener
from paqto.lan.address import (
    choose_advertised_host,
    endpoint_from_host_port,
    endpoint_from_sockname,
    parse_sockname,
    validate_max_frame_size,
)
from paqto.lan.connection import TcpConnection


class TcpListener(Listener):
    """Asyncio TCP listener for LAN connections.

    If the listener is bound to ``0.0.0.0`` or ``::``, ``local_endpoint`` tries
    to publish a primary local IPv4 address. The real bind host is always kept
    in ``local_endpoint.metadata["bind_host"]``.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        max_frame_size: int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        validate_max_frame_size(max_frame_size)
        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._metadata = dict(metadata or {})
        self._server: asyncio.AbstractServer | None = None
        self._accepted: asyncio.Queue[TcpConnection | None] = asyncio.Queue()
        self._local_endpoint: Endpoint | None = None
        self._closed = False

    @property
    def local_endpoint(self) -> Endpoint:
        """Endpoint advertised for incoming LAN connections."""
        if self._local_endpoint is None:
            return self._build_local_endpoint(self._host, self._port)
        return self._local_endpoint

    async def start(self) -> None:
        """Start the TCP server and begin accepting incoming connections."""
        if self._server is not None:
            return
        if self._closed:
            raise TransportError("Cannot restart a closed TCP listener.")

        try:
            self._server = await asyncio.start_server(
                self._handle_client,
                host=self._host,
                port=self._port,
            )
        except OSError as exc:
            raise TransportError(
                f"Could not start TCP listener on {self._host}:{self._port}."
            ) from exc

        sockname = self._first_sockname()
        _, assigned_port = parse_sockname(sockname)
        self._local_endpoint = self._build_local_endpoint(self._host, assigned_port)

    async def accept(self) -> TcpConnection:
        """Wait for and return the next accepted TCP connection."""
        if self._closed:
            raise ConnectionClosedError("TCP listener is closed.")
        if self._server is None:
            raise TransportError("TCP listener must be started before accept().")

        connection = await self._accepted.get()
        if connection is None:
            raise ConnectionClosedError("TCP listener is closed.")
        return connection

    async def close(self) -> None:
        """Close the TCP server. Calling this more than once is harmless."""
        if self._closed:
            return

        self._closed = True
        server = self._server
        self._server = None

        if server is not None:
            server.close()
            await server.wait_closed()

        self._accepted.put_nowait(None)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        if self._closed:
            writer.close()
            await writer.wait_closed()
            return

        try:
            remote_endpoint = endpoint_from_sockname(writer.get_extra_info("peername"))
        except TransportError:
            writer.close()
            await writer.wait_closed()
            return

        connection = TcpConnection(
            reader,
            writer,
            local_endpoint=self.local_endpoint,
            remote_endpoint=remote_endpoint,
            max_frame_size=self._max_frame_size,
        )
        self._accepted.put_nowait(connection)

    def _first_sockname(self) -> Any:
        if self._server is None or not self._server.sockets:
            raise TransportError("TCP listener did not expose a server socket.")
        return self._server.sockets[0].getsockname()

    def _build_local_endpoint(self, bind_host: str, port: int) -> Endpoint:
        advertised_host, source = choose_advertised_host(bind_host)
        metadata = {
            **self._metadata,
            "bind_host": bind_host,
            "advertised_host_source": source,
        }
        return endpoint_from_host_port(advertised_host, port, metadata=metadata)
