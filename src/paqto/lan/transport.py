"""Built-in framed TCP transport with optional TLS."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable
from typing import TypeVar

from paqto.core.endpoint import Endpoint
from paqto.core.errors import TransportError
from paqto.core.transport import Transport
from paqto.lan.address import (
    TRANSPORT_NAME,
    endpoint_from_sockname,
    parse_tcp_address,
    validate_frame_payload_timeout,
    validate_max_frame_size,
)
from paqto.lan.connection import TcpConnection
from paqto.lan.listener import TcpListener
from paqto.lan.security import TlsConfig, security_info_from_writer

T = TypeVar("T")
DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024 + 1
DEFAULT_MAX_PENDING_ACCEPTS = 128
DEFAULT_FRAME_PAYLOAD_TIMEOUT = 30.0


class LanTransport(Transport):
    """Framed TCP transport for LAN endpoints, optionally protected by TLS.

    Args:
        host: Listener bind host. Wildcard binds use best-effort advertised-host
            selection; configure an explicit host when routing matters.
        port: Listener TCP port; zero asks the OS to choose one.
        max_frame_size: Maximum complete TCP-frame payload in bytes.
        tls: TLS settings, or ``None`` for unauthenticated plain TCP.
        max_pending_accepts: Established connections allowed to wait for
            listener acceptance.
        frame_payload_timeout: Seconds allowed to finish a declared payload, or
            ``None`` for no payload deadline.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        tls: TlsConfig | None = None,
        max_pending_accepts: int = DEFAULT_MAX_PENDING_ACCEPTS,
        frame_payload_timeout: float | None = DEFAULT_FRAME_PAYLOAD_TIMEOUT,
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
        if tls is not None and not isinstance(tls, TlsConfig):
            raise TypeError("tls must be a TlsConfig or None.")
        self._tls = tls
        self._client_ssl_context: ssl.SSLContext | None = None
        self._server_ssl_context: ssl.SSLContext | None = None
        self._started = False
        self._listeners: set[TcpListener] = set()
        self._connections: set[TcpConnection] = set()
        self._lifecycle_generation = 0

    @property
    def name(self) -> str:
        """Stable transport name used by LAN endpoints."""
        return TRANSPORT_NAME

    async def start(self) -> None:
        """Initialize optional TLS contexts; repeated calls are harmless."""
        if self._started:
            return
        if self._tls is not None:
            try:
                self._client_ssl_context = self._tls.create_client_context()
                self._server_ssl_context = self._tls.create_server_context()
            except (OSError, ssl.SSLError, ValueError) as exc:
                self._client_ssl_context = None
                self._server_ssl_context = None
                raise TransportError(
                    "Could not initialize LAN TLS configuration."
                ) from exc
        self._lifecycle_generation += 1
        self._started = True

    async def stop(self) -> None:
        """Close owned listeners and connections; repeated calls are safe."""
        self._started = False
        self._lifecycle_generation += 1
        listeners = list(self._listeners)
        connections = list(self._connections)
        self._listeners.clear()
        self._connections.clear()
        self._client_ssl_context = None
        self._server_ssl_context = None

        await asyncio.gather(
            *(listener.close() for listener in listeners),
            *(connection.close() for connection in connections),
            return_exceptions=True,
        )

    async def create_listener(self, bind: Endpoint | None = None) -> TcpListener:
        """Create an unstarted listener using defaults or a LAN bind endpoint."""
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
            max_pending_accepts=self._max_pending_accepts,
            frame_payload_timeout=self._frame_payload_timeout,
            metadata=metadata,
            ssl_context=self._server_ssl_context,
            peer_identity_resolver=(
                self._tls.peer_identity_resolver if self._tls is not None else None
            ),
            ssl_handshake_timeout=(
                self._tls.handshake_timeout if self._tls is not None else None
            ),
        )
        self._listeners.add(listener)
        return listener

    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> TcpConnection:
        """Open a TCP/TLS connection to a LAN endpoint within ``timeout`` seconds.

        TLS verification failures are normalized to :class:`TransportError`.
        A connection that finishes after the transport stops is closed and not
        returned into a newer lifecycle.
        """
        self._ensure_started()
        self._validate_endpoint_transport(endpoint)
        parsed = parse_tcp_address(endpoint.address)
        generation = self._lifecycle_generation

        if self._tls is None:
            connection_attempt = asyncio.open_connection(parsed.host, parsed.port)
        else:
            connection_attempt = asyncio.open_connection(
                parsed.host,
                parsed.port,
                ssl=self._client_ssl_context,
                server_hostname=parsed.host if self._tls.check_hostname else None,
                ssl_handshake_timeout=self._tls.handshake_timeout,
            )

        try:
            reader, writer = await self._maybe_wait_for(
                connection_attempt,
                timeout,
            )
        except TimeoutError:
            raise
        except (OSError, ssl.SSLError) as exc:
            raise TransportError(
                f"Could not connect to LAN endpoint {endpoint.address!r}."
            ) from exc

        if not self._started or self._lifecycle_generation != generation:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, RuntimeError):
                pass
            raise TransportError("LAN transport stopped while opening a connection.")

        try:
            security_info = None
            if self._tls is not None:
                security_info = security_info_from_writer(
                    writer,
                    peer_authenticated=self._tls.verify_peer,
                    identity_resolver=self._tls.peer_identity_resolver,
                    verified_server_name=(
                        parsed.host if self._tls.check_hostname else None
                    ),
                )
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
                frame_payload_timeout=self._frame_payload_timeout,
                security_info=security_info,
            )
        except Exception:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, RuntimeError):
                pass
            raise
        self._connections = {
            existing for existing in self._connections if not existing.is_closed
        }
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
