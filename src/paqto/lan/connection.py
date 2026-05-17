from __future__ import annotations

import asyncio

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.errors import ConnectionClosedError, TransportError
from paqto.lan.address import validate_max_frame_size

HEADER_SIZE = 4


class TcpConnection(Connection):
    """A TCP-backed byte-frame connection.

    Frames are encoded as an unsigned 4-byte big-endian payload length followed
    by exactly that many payload bytes. The payload returned by
    :meth:`receive_frame` never includes the length prefix.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        local_endpoint: Endpoint,
        remote_endpoint: Endpoint,
        max_frame_size: int,
    ) -> None:
        validate_max_frame_size(max_frame_size)
        self._reader = reader
        self._writer = writer
        self._local_endpoint = local_endpoint
        self._remote_endpoint = remote_endpoint
        self._max_frame_size = max_frame_size
        self._closed = False

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

    async def send_frame(self, data: bytes) -> None:
        """Send one complete payload frame over TCP."""
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
        """Receive and return exactly one complete payload frame."""
        if self.is_closed:
            raise ConnectionClosedError("Cannot receive from a closed TCP connection.")

        header = await self._read_exactly(HEADER_SIZE)
        frame_size = int.from_bytes(header, "big", signed=False)
        if frame_size > self._max_frame_size:
            await self.close()
            raise TransportError(
                f"Incoming frame size {frame_size} exceeds limit "
                f"{self._max_frame_size}."
            )

        return await self._read_exactly(frame_size)

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
