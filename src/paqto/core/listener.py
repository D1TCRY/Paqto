"""Abstract incoming-connection listener contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint


class Listener(ABC):
    """Accept incoming complete-frame connections for a transport.

    Implementations allocate resources in ``start()``, block in ``accept()``
    until a connection or terminal error is available, and wake pending accepts
    during ``close()``. Closed listeners need not be restartable.
    """

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
        """Stop accepting and release listener-owned resources."""

    async def __aenter__(self) -> Listener:  # noqa: PYI034 - Python 3.10 API
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
