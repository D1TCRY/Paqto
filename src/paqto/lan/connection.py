"""Length-prefixed asyncio TCP connection implementation."""

from __future__ import annotations

import asyncio

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.errors import ConnectionClosedError, TransportError
from paqto.core.security import SecurityInfo
from paqto.lan.address import (
    validate_frame_payload_timeout,
    validate_max_frame_size,
)

HEADER_SIZE = 4


class TcpConnection(Connection):
    """A TCP-backed byte-frame connection.

    Frames are encoded as an unsigned 4-byte big-endian payload length followed
    by exactly that many payload bytes. The payload returned by
    :meth:`receive_frame` never includes the length prefix.

    ``max_frame_size`` limits each payload in bytes. Once a legal length header
    arrives, ``frame_payload_timeout`` bounds completion in seconds; ``None``
    permits an unbounded payload wait. Independent send and receive locks allow
    full-duplex use without interleaving frames in either direction.

    Args:
        reader: Asyncio stream reader owned by this connection.
        writer: Matching asyncio stream writer.
        local_endpoint: Local address snapshot for diagnostics.
        remote_endpoint: Remote address snapshot for diagnostics.
        max_frame_size: Maximum payload bytes in either direction.
        frame_payload_timeout: Seconds to complete an incoming declared payload,
            or ``None`` for no deadline.
        security_info: Established transport-security snapshot. ``None`` makes
            no encryption or authentication claim.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        local_endpoint: Endpoint,
        remote_endpoint: Endpoint,
        max_frame_size: int,
        frame_payload_timeout: float | None = 30.0,
        security_info: SecurityInfo | None = None,
    ) -> None:
        validate_max_frame_size(max_frame_size)
        validate_frame_payload_timeout(frame_payload_timeout)
        self._reader = reader
        self._writer = writer
        self._local_endpoint = local_endpoint
        self._remote_endpoint = remote_endpoint
        self._max_frame_size = max_frame_size
        self._frame_payload_timeout = frame_payload_timeout
        self._security_info = security_info or SecurityInfo()
        self._closed = False
        self._send_lock = asyncio.Lock()
        self._receive_lock = asyncio.Lock()

    @property
    def local_endpoint(self) -> Endpoint:
        """Endpoint used locally by this TCP connection."""
        return self._local_endpoint

    @property
    def remote_endpoint(self) -> Endpoint:
        """Endpoint reached by this TCP connection."""
        return self._remote_endpoint

    @property
    def is_closed(self) -> bool:
        """Whether the TCP stream can no longer be used."""
        return self._closed or self._writer.is_closing() or self._reader.at_eof()

    @property
    def security_info(self) -> SecurityInfo:
        """Security guarantees established for this TCP stream."""
        return self._security_info

    async def send_frame(self, data: bytes) -> None:
        """Write one length-prefixed payload and wait for stream backpressure."""
        async with self._send_lock:
            if self.is_closed:
                raise ConnectionClosedError("Cannot send on a closed TCP connection.")
            if len(data) > self._max_frame_size:
                raise TransportError(
                    f"Frame size {len(data)} exceeds limit {self._max_frame_size}."
                )

            header = len(data).to_bytes(HEADER_SIZE, "big", signed=False)
            try:
                self._writer.write(header + data)
                await self._writer.drain()
            except (ConnectionError, OSError) as exc:
                self._closed = True
                raise ConnectionClosedError(
                    "TCP connection closed while sending a frame."
                ) from exc

    async def receive_frame(self) -> bytes:
        """Return one payload, closing on oversize or incomplete-frame timeout."""
        async with self._receive_lock:
            if self.is_closed:
                raise ConnectionClosedError(
                    "Cannot receive from a closed TCP connection."
                )

            header = await self._read_exactly(HEADER_SIZE)
            frame_size = int.from_bytes(header, "big", signed=False)
            if frame_size > self._max_frame_size:
                await self.close()
                raise TransportError(
                    f"Incoming frame size {frame_size} exceeds limit "
                    f"{self._max_frame_size}."
                )

            try:
                if self._frame_payload_timeout is None:
                    return await self._read_exactly(frame_size)
                return await asyncio.wait_for(
                    self._read_exactly(frame_size),
                    timeout=self._frame_payload_timeout,
                )
            except TimeoutError as exc:
                await self.close()
                raise TransportError(
                    "Timed out while receiving a complete TCP frame payload."
                ) from exc

    async def close(self) -> None:
        """Close the TCP stream safely. Calling this more than once is harmless."""
        already_closed = self._closed and self._writer.is_closing()
        self._closed = True

        if not self._writer.is_closing():
            self._writer.close()

        if already_closed:
            return

        try:
            await self._writer.wait_closed()
        except (ConnectionError, OSError, RuntimeError):
            pass

    async def _read_exactly(self, size: int) -> bytes:
        """Normalize incomplete stream reads to :class:`ConnectionClosedError`."""
        try:
            return await self._reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            self._closed = True
            raise ConnectionClosedError(
                "TCP connection closed before a full frame was received."
            ) from exc
        except (ConnectionError, OSError) as exc:
            self._closed = True
            raise ConnectionClosedError(
                "TCP connection closed while receiving a frame."
            ) from exc
