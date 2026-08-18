"""Discovery observations and freshness evaluation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from paqto.core.endpoint import Endpoint
from paqto.core.peer import Peer


class PeerFreshness(str, Enum):
    """Whether a discovery observation is still usable for reachability."""

    FRESH = "fresh"
    EXPIRED = "expired"


@dataclass(slots=True)
class DiscoveredPeer:
    """Untrusted discovery claim with observed reachable endpoints.

    Attributes:
        peer: Identity declared by the discovery announcement.
        endpoints: Transport-specific routes advertised for the peer.
        last_seen: Timezone-aware time of the most recent observation.
        metadata: Discovery-service metadata. It is not authenticated by the
            generic discovery contract.
    """

    peer: Peer
    endpoints: list[Endpoint] = field(default_factory=list)
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def endpoint_for(self, transport: str) -> Endpoint | None:
        """Return the first endpoint for ``transport``, if one was advertised."""
        for endpoint in self.endpoints:
            if endpoint.transport == transport:
                return endpoint
        return None

    def touch(self) -> None:
        """Set ``last_seen`` to the current UTC time."""
        self.last_seen = datetime.now(timezone.utc)

    def freshness(
        self,
        ttl: float | None,
        *,
        now: datetime | None = None,
    ) -> PeerFreshness:
        """Return freshness for ``ttl`` without treating discovery as trust.

        ``None`` disables expiration.  A future timestamp is treated as fresh,
        which avoids expiring a peer solely because wall clocks moved backward.
        """
        _validate_ttl(ttl)
        if ttl is None:
            return PeerFreshness.FRESH
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None or self.last_seen.tzinfo is None:
            raise ValueError("Discovery timestamps must be timezone-aware.")
        age = max(0.0, (reference - self.last_seen).total_seconds())
        if age <= ttl:
            return PeerFreshness.FRESH
        return PeerFreshness.EXPIRED

    def is_fresh(
        self,
        ttl: float | None,
        *,
        now: datetime | None = None,
    ) -> bool:
        """Return whether this discovery observation is still fresh."""
        return self.freshness(ttl, now=now) is PeerFreshness.FRESH


def _validate_ttl(value: float | None) -> None:
    if value is None:
        return
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError("ttl must be a number or None.")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("ttl must be finite and greater than zero.")
