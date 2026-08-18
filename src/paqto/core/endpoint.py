"""Transport-specific endpoint model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Endpoint:
    """Transport-specific address that can be used to reach a peer.

    An endpoint describes a route, not an authenticated peer identity.

    Attributes:
        transport: Stable transport name understood by an adapter.
        address: Adapter-specific address string.
        metadata: Optional adapter-specific address details.
    """

    transport: str
    address: str
    metadata: dict[str, Any] = field(default_factory=dict)
