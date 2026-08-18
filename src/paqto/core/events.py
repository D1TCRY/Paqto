"""Best-effort node lifecycle events and event routing."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any


class NodeEventType(str, Enum):
    """Observable, transport-neutral node lifecycle events."""

    PEER_DISCOVERED = "peer_discovered"
    PEER_EXPIRED = "peer_expired"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    TRANSPORT_ERROR = "transport_error"
    PROTOCOL_ERROR = "protocol_error"
    HANDLER_ERROR = "handler_error"
    RESOURCE_LIMIT = "resource_limit"


@dataclass(frozen=True, slots=True)
class NodeEvent:
    """One immutable best-effort node observation.

    Attributes:
        type: Category of lifecycle or failure observation.
        local_peer_id: Identity of the node that emitted the event.
        peer_id: Related declared or session peer id, when known.
        connection_id: Process-local diagnostic connection id, when known.
        error: Associated exception object, when applicable.
        metadata: Small diagnostic details; application-message payloads are
            intentionally excluded.
        occurred_at: Timezone-aware UTC creation time.
    """

    type: NodeEventType
    local_peer_id: str
    peer_id: str | None = None
    connection_id: str | None = None
    error: BaseException | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not isinstance(self.type, NodeEventType):
            raise TypeError("type must be a NodeEventType.")
        if not isinstance(self.local_peer_id, str) or not self.local_peer_id:
            raise ValueError("local_peer_id must be a non-empty string.")
        if self.peer_id is not None and (
            not isinstance(self.peer_id, str) or not self.peer_id
        ):
            raise ValueError("peer_id must be a non-empty string or None.")
        if self.connection_id is not None and (
            not isinstance(self.connection_id, str) or not self.connection_id
        ):
            raise ValueError("connection_id must be a non-empty string or None.")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


EventHandler = Callable[[NodeEvent], Awaitable[None] | None]


class EventRouter:
    """Dispatch node events while isolating individual listener failures."""

    def __init__(self) -> None:
        self._handlers: dict[NodeEventType | None, list[EventHandler]] = {}

    def on(
        self,
        type: NodeEventType | None = None,
    ) -> Callable[[EventHandler], EventHandler]:
        """Return a decorator that registers a listener for one or all types."""
        def decorator(handler: EventHandler) -> EventHandler:
            self.add_handler(type, handler)
            return handler

        return decorator

    def add_handler(
        self,
        type: NodeEventType | None,
        handler: EventHandler,
    ) -> None:
        """Register a synchronous or asynchronous event listener."""
        if type is not None and not isinstance(type, NodeEventType):
            raise TypeError("type must be a NodeEventType or None.")
        if not callable(handler):
            raise TypeError("handler must be callable.")
        self._handlers.setdefault(type, []).append(handler)

    async def dispatch(self, event: NodeEvent) -> tuple[Exception, ...]:
        """Run all matching listeners and return, rather than raise, failures."""
        handlers = [
            *self._handlers.get(event.type, []),
            *self._handlers.get(None, []),
        ]
        errors: list[Exception] = []
        for handler in handlers:
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001 - listener isolation boundary
                errors.append(exc)
        return tuple(errors)
