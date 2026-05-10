from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from paqto.core.discovered import DiscoveredPeer
from paqto.core.endpoint import Endpoint
from paqto.core.peer import Peer


class DiscoveryService(ABC):
    """Announces the local peer and discovers remote peers."""

    @abstractmethod
    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        """Start announcing the local peer."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop announcing and discovering peers."""

    @abstractmethod
    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        """Return peers found by this discovery service."""

