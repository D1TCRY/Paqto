"""Application message handler registration and dispatch."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from paqto.core.errors import MessageRoutingError
from paqto.core.message import Message

MessageHandler = Callable[[Message], Awaitable[None] | None]


class MessageRouter:
    """Dispatch messages to synchronous or asynchronous registered handlers.

    Handlers for the exact message type run first, followed by handlers
    registered for ``None``. Dispatch is sequential and stops on the first
    failure, which is normalized to :class:`MessageRoutingError`.
    """

    def __init__(self) -> None:
        self._handlers: dict[str | None, list[MessageHandler]] = {}

    def on(self, type: str | None = None) -> Callable[[MessageHandler], MessageHandler]:
        """Return a decorator registering a handler for one or all types."""
        def decorator(handler: MessageHandler) -> MessageHandler:
            self.add_handler(type, handler)
            return handler

        return decorator

    def add_handler(self, type: str | None, handler: MessageHandler) -> None:
        """Register a handler for ``type`` or for every message when ``None``."""
        self._handlers.setdefault(type, []).append(handler)

    async def dispatch(self, message: Message) -> int:
        """Run matching handlers and return the number successfully invoked."""
        handlers = [
            *self._handlers.get(message.type, []),
            *self._handlers.get(None, []),
        ]

        for handler in handlers:
            try:
                result: Any = handler(message)
                if inspect.isawaitable(result):
                    await result
            except MessageRoutingError:
                raise
            except Exception as exc:
                raise MessageRoutingError(
                    f"Handler failed for message type {message.type!r}."
                ) from exc

        return len(handlers)
