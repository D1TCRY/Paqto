from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint


class Listener(ABC):
    """Accepts incoming byte-frame connections."""

    @property
    @abstractmethod
    def local_endpoint(self) -> Endpoint:
        """Endpoint advertised for incoming connections."""

    @abstractmethod
    async def start(self) -> None:
        """Begin accepting incoming connections."""

    @abstractmethod
    async def accept(self) -> Connection:
        """Wait for and return the next incoming connection."""

    @abstractmethod
    async def close(self) -> None:
        """Close the listener."""

    async def __aenter__(self) -> Listener:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

