"""Asyncio TCP listener with bounded pending accepts and optional TLS."""

from __future__ import annotations

import asyncio
import ssl
from collections import deque
from typing import Any

from paqto.core.endpoint import Endpoint
from paqto.core.errors import ConnectionClosedError, TransportError
from paqto.core.listener import Listener
from paqto.lan.address import (
    choose_advertised_host,
    endpoint_from_host_port,
    endpoint_from_sockname,
    parse_sockname,
    validate_frame_payload_timeout,
    validate_max_frame_size,
)
from paqto.lan.connection import TcpConnection
from paqto.lan.security import (
    TlsPeerIdentityResolver,
    security_info_from_writer,
)


class TcpListener(Listener):
    """Asyncio TCP listener for LAN connections.

    If the listener is bound to ``0.0.0.0`` or ``::``, ``local_endpoint`` tries
    to publish a primary local IPv4 address. The real bind host is always kept
    in ``local_endpoint.metadata["bind_host"]``.

    Established connections waiting for ``accept()`` are limited by
    ``max_pending_accepts``. Excess connections are closed. A listener cannot
    be restarted after ``close()``.

    Args:
        host: Local TCP bind host.
        port: Local TCP bind port; zero requests an OS-assigned port.
        max_frame_size: Payload byte limit applied to accepted connections.
        max_pending_accepts: Maximum established connections waiting for
            ``accept()``.
        frame_payload_timeout: Seconds allowed to finish an incoming declared
            payload, or ``None`` for no deadline.
        metadata: Extra metadata copied into the advertised endpoint.
        ssl_context: Server TLS context, or ``None`` for plain TCP.
        peer_identity_resolver: Mapping for an already verified client
            certificate; it does not authenticate an optional certificate.
        ssl_handshake_timeout: TLS handshake deadline in seconds.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        max_frame_size: int,
        max_pending_accepts: int = 128,
        frame_payload_timeout: float | None = 30.0,
        metadata: dict[str, Any] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        peer_identity_resolver: TlsPeerIdentityResolver | None = None,
        ssl_handshake_timeout: float | None = None,
    ) -> None:
        validate_max_frame_size(max_frame_size)
        validate_frame_payload_timeout(frame_payload_timeout)
        if not isinstance(max_pending_accepts, int) or isinstance(
            max_pending_accepts, bool
        ):
            raise TypeError("max_pending_accepts must be an integer.")
        if max_pending_accepts <= 0:
            raise ValueError("max_pending_accepts must be greater than zero.")
        self._host = host
        self._port = port
        self._max_frame_size = max_frame_size
        self._max_pending_accepts = max_pending_accepts
        self._frame_payload_timeout = frame_payload_timeout
        self._metadata = dict(metadata or {})
        self._ssl_context = ssl_context
        self._peer_identity_resolver = peer_identity_resolver
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._server: asyncio.Server | None = None
        self._accepted: deque[TcpConnection] = deque()
        self._accept_waiters: deque[asyncio.Future[TcpConnection]] = deque()
        self._connections: set[TcpConnection] = set()
        self._local_endpoint: Endpoint | None = None
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def local_endpoint(self) -> Endpoint:
        """Endpoint advertised for incoming LAN connections."""
        if self._local_endpoint is None:
            return self._build_local_endpoint(self._host, self._port)
        return self._local_endpoint

    async def start(self) -> None:
        """Start the TCP server and begin accepting incoming connections."""
        async with self._lifecycle_lock:
            await self._start()

    async def _start(self) -> None:
        """Start the server while the lifecycle lock is held."""
        if self._server is not None:
            return
        if self._closed:
            raise TransportError("Cannot restart a closed TCP listener.")

        try:
            self._server = await asyncio.start_server(
                self._handle_client,
                host=self._host,
                port=self._port,
                ssl=self._ssl_context,
                ssl_handshake_timeout=self._ssl_handshake_timeout,
            )
        except OSError as exc:
            raise TransportError(
                f"Could not start TCP listener on {self._host}:{self._port}."
            ) from exc

        sockname = self._first_sockname()
        _, assigned_port = parse_sockname(sockname)
        self._local_endpoint = self._build_local_endpoint(self._host, assigned_port)

    async def accept(self) -> TcpConnection:
        """Wait for the next connection or fail if not started or already closed."""
        if self._closed:
            raise ConnectionClosedError("TCP listener is closed.")
        if self._server is None:
            raise TransportError("TCP listener must be started before accept().")

        if self._accepted:
            return self._accepted.popleft()

        waiter = asyncio.get_running_loop().create_future()
        self._accept_waiters.append(waiter)
        try:
            return await waiter
        finally:
            if not waiter.done():
                waiter.cancel()
            try:
                self._accept_waiters.remove(waiter)
            except ValueError:
                pass

    async def close(self) -> None:
        """Close the TCP server. Calling this more than once is harmless."""
        async with self._lifecycle_lock:
            await self._close()

    async def _close(self) -> None:
        """Wake accept waiters and close all listener-owned connections."""
        if self._closed:
            return

        self._closed = True
        server = self._server
        self._server = None

        while self._accept_waiters:
            waiter = self._accept_waiters.popleft()
            if not waiter.done():
                waiter.set_exception(ConnectionClosedError("TCP listener is closed."))

        if server is not None:
            server.close()

        connections = list(self._connections)
        self._connections.clear()
        self._accepted.clear()
        if connections:
            await asyncio.gather(
                *(connection.close() for connection in connections),
                return_exceptions=True,
            )

        if server is not None:
            await server.wait_closed()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Wrap one accepted stream, derive security metadata, and admit it."""
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

        try:
            security_info = None
            if self._ssl_context is not None:
                security_info = security_info_from_writer(
                    writer,
                    peer_authenticated=(
                        self._ssl_context.verify_mode == ssl.CERT_REQUIRED
                    ),
                    identity_resolver=self._peer_identity_resolver,
                )
            connection = TcpConnection(
                reader,
                writer,
                local_endpoint=self.local_endpoint,
                remote_endpoint=remote_endpoint,
                max_frame_size=self._max_frame_size,
                frame_payload_timeout=self._frame_payload_timeout,
                security_info=security_info,
            )
        except TransportError:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, RuntimeError):
                pass
            return
        while self._accept_waiters:
            waiter = self._accept_waiters.popleft()
            if not waiter.done():
                self._connections = {
                    existing
                    for existing in self._connections
                    if not existing.is_closed
                }
                self._connections.add(connection)
                waiter.set_result(connection)
                return
        if len(self._accepted) >= self._max_pending_accepts:
            await connection.close()
            return
        self._connections = {
            existing for existing in self._connections if not existing.is_closed
        }
        self._connections.add(connection)
        self._accepted.append(connection)

    def _first_sockname(self) -> Any:
        if self._server is None or not self._server.sockets:
            raise TransportError("TCP listener did not expose a server socket.")
        return self._server.sockets[0].getsockname()

    def _build_local_endpoint(self, bind_host: str, port: int) -> Endpoint:
        """Build the advertised endpoint while retaining the real bind host."""
        advertised_host, source = choose_advertised_host(bind_host)
        metadata = {
            **self._metadata,
            "bind_host": bind_host,
            "advertised_host_source": source,
        }
        return endpoint_from_host_port(advertised_host, port, metadata=metadata)
