from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PaqtoConfig:
    """Runtime options used by the core facade."""

    connect_timeout: float | None = 10.0
    send_timeout: float | None = 10.0
    discover_timeout: float | None = 3.0

