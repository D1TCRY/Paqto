"""Logical peer identity model."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class Peer:
    """Logical identity of a Paqto node.

    Identity declared by a ``Peer`` or discovery announcement is not proof of
    authentication. Transport security may establish a separately verified id.

    Attributes:
        id: Stable non-empty identifier used by protocol and routing logic.
        name: Optional human-readable label.
        metadata: Optional application-defined identity metadata.
    """

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
        """Create a peer with a random hexadecimal id and copied metadata."""
        return cls(id=uuid4().hex, name=name, metadata=dict(metadata or {}))
