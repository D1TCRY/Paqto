"""Discovery observations and freshness evaluation."""

from __future__ import annotations

import math
import time
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
    _observed_at_monotonic: float = field(init=False, repr=False, compare=False)
    _last_seen_snapshot: datetime = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._anchor_monotonic_age()

    def endpoint_for(self, transport: str) -> Endpoint | None:
        """Return the first endpoint for ``transport``, if one was advertised."""
        for endpoint in self.endpoints:
            if endpoint.transport == transport:
                return endpoint
        return None

    def touch(self) -> None:
        """Refresh diagnostic wall time and operational monotonic age."""
        self.last_seen = datetime.now(timezone.utc)
        self._last_seen_snapshot = self.last_seen
        self._observed_at_monotonic = time.monotonic()

    def freshness(
        self,
        ttl: float | None,
        *,
        now: datetime | None = None,
        monotonic_now: float | None = None,
    ) -> PeerFreshness:
        """Return freshness for ``ttl`` without treating discovery as trust.

        ``None`` disables expiration. Normal operational checks use a monotonic
        clock, so wall-clock corrections cannot extend or shorten the TTL.
        ``now`` retains deterministic wall-time evaluation for callers that
        explicitly need it; ``monotonic_now`` is available for deterministic
        monotonic tests. A future timestamp is treated as fresh.
        """
        _validate_ttl(ttl)
        if ttl is None:
            return PeerFreshness.FRESH
        if now is not None and monotonic_now is not None:
            raise ValueError("now and monotonic_now are mutually exclusive.")
        if now is not None:
            if now.tzinfo is None or self.last_seen.tzinfo is None:
                raise ValueError("Discovery timestamps must be timezone-aware.")
            age = max(0.0, (now - self.last_seen).total_seconds())
        else:
            self._synchronize_manually_changed_timestamp()
            if self.last_seen.tzinfo is None:
                raise ValueError("Discovery timestamps must be timezone-aware.")
            reference = time.monotonic() if monotonic_now is None else monotonic_now
            if not isinstance(reference, (int, float)) or isinstance(reference, bool):
                raise TypeError("monotonic_now must be a number or None.")
            if not math.isfinite(reference):
                raise ValueError("monotonic_now must be finite.")
            age = max(0.0, reference - self._observed_at_monotonic)
        if age <= ttl:
            return PeerFreshness.FRESH
        return PeerFreshness.EXPIRED

    def is_fresh(
        self,
        ttl: float | None,
        *,
        now: datetime | None = None,
        monotonic_now: float | None = None,
    ) -> bool:
        """Return whether this discovery observation is still fresh."""
        return (
            self.freshness(ttl, now=now, monotonic_now=monotonic_now)
            is PeerFreshness.FRESH
        )

    def _anchor_monotonic_age(self) -> None:
        """Translate the initial diagnostic timestamp into a local TTL anchor."""
        if self.last_seen.tzinfo is None:
            self._observed_at_monotonic = time.monotonic()
            self._last_seen_snapshot = self.last_seen
            return
        wall_age = max(
            0.0,
            (datetime.now(timezone.utc) - self.last_seen).total_seconds(),
        )
        self._observed_at_monotonic = time.monotonic() - wall_age
        self._last_seen_snapshot = self.last_seen

    def _synchronize_manually_changed_timestamp(self) -> None:
        """Honor the public mutable timestamp without tracking later wall jumps."""
        if self.last_seen != self._last_seen_snapshot:
            self._anchor_monotonic_age()


def _validate_ttl(value: float | None) -> None:
    if value is None:
        return
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError("ttl must be a number or None.")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("ttl must be finite and greater than zero.")
