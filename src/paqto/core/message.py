"""Generic application message envelope."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class Message:
    """Application-level message envelope serialized by an application adapter.

    Attributes:
        payload: Application-defined value.
        type: Non-empty routing key used by :class:`MessageRouter`.
        sender: Declared sender peer id, or ``None`` before assignment.
        recipient: Intended peer id, or ``None`` for an unspecified recipient.
        headers: Application-defined string metadata.
        id: Non-empty message identifier used for ACK and reply correlation.
        created_at: Timezone-aware creation time.
        reply_to: Request message id correlated by request/reply, if any.
    """

    payload: Any
    type: str = "message"
    sender: str | None = None
    recipient: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid4().hex)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reply_to: str | None = None
