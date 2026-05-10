from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from paqto.core.message import Message

MessageHandler = Callable[[Message], Awaitable[None] | None]


class MessageRouter:
    """Dispatches messages to handlers registered by message type."""

    def __init__(self) -> None:
        self._handlers: dict[str | None, list[MessageHandler]] = {}

    def on(self, type: str | None = None) -> Callable[[MessageHandler], MessageHandler]:
        def decorator(handler: MessageHandler) -> MessageHandler:
            self.add_handler(type, handler)
            return handler

        return decorator

    def add_handler(self, type: str | None, handler: MessageHandler) -> None:
        self._handlers.setdefault(type, []).append(handler)

    async def dispatch(self, message: Message) -> int:
        handlers = [
            *self._handlers.get(message.type, []),
            *self._handlers.get(None, []),
        ]

        for handler in handlers:
            result: Any = handler(message)
            if inspect.isawaitable(result):
                await result

        return len(handlers)

