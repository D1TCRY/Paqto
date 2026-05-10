from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.endpoint import Endpoint


class Connection(ABC):
    """An active byte-frame channel to a remote endpoint."""

    @property
    @abstractmethod
    def local_endpoint(self) -> Endpoint:
        """Endpoint used locally for this connection."""

    @property
    @abstractmethod
    def remote_endpoint(self) -> Endpoint:
        """Endpoint reached by this connection."""

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        """Whether the connection can no longer be used."""

    @abstractmethod
    async def send_frame(self, data: bytes) -> None:
        """Send one complete frame of bytes."""

    @abstractmethod
    async def receive_frame(self) -> bytes:
        """Receive one complete frame of bytes."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

