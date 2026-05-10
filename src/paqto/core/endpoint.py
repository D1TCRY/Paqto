from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Endpoint:
    """A transport-specific address that can be used to reach a peer."""

    transport: str
    address: str
    metadata: dict[str, Any] = field(default_factory=dict)

