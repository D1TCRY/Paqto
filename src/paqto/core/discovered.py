from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from paqto.core.endpoint import Endpoint
from paqto.core.peer import Peer


@dataclass(slots=True)
class DiscoveredPeer:
    """A peer found through discovery, together with reachable endpoints."""

    peer: Peer
    endpoints: list[Endpoint] = field(default_factory=list)
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def endpoint_for(self, transport: str) -> Endpoint | None:
        for endpoint in self.endpoints:
            if endpoint.transport == transport:
                return endpoint
        return None

    def touch(self) -> None:
        self.last_seen = datetime.now(timezone.utc)

