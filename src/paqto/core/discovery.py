"""Abstract discovery service contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from paqto.core.discovered import DiscoveredPeer
from paqto.core.endpoint import Endpoint
from paqto.core.errors import NotStartedError
from paqto.core.peer import Peer


class DiscoveryService(ABC):
    """Announce local reachability and return remote reachability claims.

    Implementations own their discovery resources between ``start()`` and
    ``stop()``. A discovered peer is not authenticated unless a concrete
    service explicitly supplies and documents such a mechanism.
    """

    @abstractmethod
    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        """Start discovery with the local identity and reachable endpoints."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop discovery and release its resources."""

    @abstractmethod
    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        """Return current peer observations within the optional seconds budget."""


class NoDiscovery(DiscoveryService):
    """Restartable no-op discovery for explicitly provisioned endpoints.

    This adapter creates no sockets and performs no name or interface lookup.
    Pass it explicitly, or omit ``PaqtoNode.discovery``, when peers are supplied
    as :class:`DiscoveredPeer` values by the host application.
    """

    def __init__(self) -> None:
        self._started = False

    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        del local_peer, endpoints
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        del timeout
        if not self._started:
            raise NotStartedError("NoDiscovery must be started before discovery.")
        return []
