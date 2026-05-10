from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.listener import Listener


class Transport(ABC):
    """Creates outgoing connections and incoming listeners for one medium."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable transport name, for example 'lan' or 'bluetooth'."""

    @abstractmethod
    async def start(self) -> None:
        """Initialize transport-level resources."""

    @abstractmethod
    async def stop(self) -> None:
        """Release transport-level resources."""

    @abstractmethod
    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        """Open an outgoing connection to a compatible endpoint."""

    @abstractmethod
    async def create_listener(self, bind: Endpoint | None = None) -> Listener:
        """Create a listener for incoming connections."""

