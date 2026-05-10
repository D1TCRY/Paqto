from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class Peer:
    """Logical identity of a paqto node."""

    id: str
    name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        name: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Peer:
        return cls(id=uuid4().hex, name=name, metadata=dict(metadata or {}))

