"""Result models shared by Paqto compatibility checks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum


class Status(str, Enum):
    """Outcome of one conformance check."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    UNAVAILABLE = "UNAVAILABLE"


class CapabilityUnavailable(RuntimeError):
    """An optional environmental capability could not be exercised."""


CheckFunction = Callable[[], Awaitable[str | None]]


@dataclass(frozen=True, slots=True)
class Check:
    """One independently reported conformance check."""

    id: str
    category: str
    description: str
    function: CheckFunction
    run_profiles: frozenset[str] = frozenset({"ci", "full"})
    required_profiles: frozenset[str] = frozenset({"ci", "full"})


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Serializable result of one conformance check."""

    id: str
    category: str
    description: str
    status: Status
    required: bool
    duration_ms: float
    detail: str

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe representation."""
        return {
            "id": self.id,
            "category": self.category,
            "description": self.description,
            "status": self.status.value,
            "required": self.required,
            "duration_ms": round(self.duration_ms, 3),
            "detail": self.detail,
        }
