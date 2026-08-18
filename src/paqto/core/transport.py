"""Transport adapter contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.listener import Listener


class Transport(ABC):
    """Create outgoing connections and incoming listeners for one medium.

    A transport owns its resources between ``start()`` and ``stop()`` and
    returns complete-frame :class:`Connection` objects. It does not perform the
    Paqto protocol handshake or define application delivery guarantees.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable transport name, for example 'lan' or 'bluetooth'."""

    @abstractmethod
    async def start(self) -> None:
        """Initialize resources required by ``connect`` and listeners."""

    @abstractmethod
    async def stop(self) -> None:
        """Close transport-owned resources and cooperate with cancellation."""

    @abstractmethod
    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        """Open a connection, applying the optional timeout in seconds."""

    @abstractmethod
    async def create_listener(self, bind: Endpoint | None = None) -> Listener:
        """Create an unstarted listener, optionally for a specific bind route."""
