from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class Message:
    """Application-level message envelope."""

    payload: Any
    type: str = "message"
    sender: str | None = None
    recipient: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid4().hex)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reply_to: str | None = None

