from __future__ import annotations

import asyncio
from typing import Awaitable, TypeVar

from paqto.core.endpoint import Endpoint
from paqto.core.errors import TransportError
from paqto.core.transport import Transport
from paqto.lan.address import (
    TRANSPORT_NAME,
    endpoint_from_sockname,
    parse_tcp_address,
    validate_max_frame_size,
)
from paqto.lan.connection import TcpConnection
from paqto.lan.listener import TcpListener

T = TypeVar("T")


class LanTransport(Transport):
    """TCP transport for communicating with peers on a LAN."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        max_frame_size: int = 16 * 1024 * 1024,
    ) -> None:
        validate_max_frame_size(max_frame_size)
        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._started = False
        self._listeners: set[TcpListener] = set()
        self._connections: set[TcpConnection] = set()

    @property
    def name(self) -> str:
        """Stable transport name used by LAN endpoints."""
        return TRANSPORT_NAME

    async def start(self) -> None:
        """Initialize transport state."""
        self._started = True

    async def stop(self) -> None:
        """Close listeners and outgoing connections created by this transport."""
        listeners = list(self._listeners)
        connections = list(self._connections)
        self._listeners.clear()
        self._connections.clear()
        self._started = False

        await asyncio.gather(
            *(listener.close() for listener in listeners),
            *(connection.close() for connection in connections),
            return_exceptions=True,
        )

    async def create_listener(self, bind: Endpoint | None = None) -> TcpListener:
        """Create a TCP listener for incoming LAN connections."""
        self._ensure_started()

        host = self._host
        port = self._port
        metadata = None
        if bind is not None:
            self._validate_endpoint_transport(bind)
            parsed = parse_tcp_address(bind.address)
            host = parsed.host
            port = parsed.port
            metadata = bind.metadata

        listener = TcpListener(
            host=host,
            port=port,
            max_frame_size=self._max_frame_size,
            metadata=metadata,
        )
        self._listeners.add(listener)
        return listener

    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> TcpConnection:
        """Open an outgoing TCP connection to a LAN endpoint."""
        self._ensure_started()
        self._validate_endpoint_transport(endpoint)
        parsed = parse_tcp_address(endpoint.address)

        try:
            reader, writer = await self._maybe_wait_for(
                asyncio.open_connection(parsed.host, parsed.port),
                timeout,
            )
        except TimeoutError:
            raise
        except OSError as exc:
            raise TransportError(
                f"Could not connect to LAN endpoint {endpoint.address!r}."
            ) from exc

        local_endpoint = endpoint_from_sockname(writer.get_extra_info("sockname"))
        connection = TcpConnection(
            reader,
            writer,
            local_endpoint=local_endpoint,
            remote_endpoint=Endpoint(
                transport=endpoint.transport,
                address=endpoint.address,
                metadata=dict(endpoint.metadata),
            ),
            max_frame_size=self._max_frame_size,
        )
        self._connections.add(connection)
        return connection

    def _ensure_started(self) -> None:
        if not self._started:
            raise TransportError("LAN transport must be started before use.")

    def _validate_endpoint_transport(self, endpoint: Endpoint) -> None:
        if endpoint.transport != self.name:
            raise TransportError(
                f"Endpoint uses transport {endpoint.transport!r}, "
                f"but LAN transport uses {self.name!r}."
            )

    @staticmethod
    async def _maybe_wait_for(awaitable: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout)
