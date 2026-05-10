from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.errors import NoEndpointError, PaqtoTimeoutError, TransportError
from paqto.core.peer import Peer
from paqto.core.transport import Transport

T = TypeVar("T")


class ConnectionManager:
    """Keeps reusable outgoing connections for a single transport."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._connections: dict[str, Connection] = {}

    def get(self, peer: Peer) -> Connection | None:
        connection = self._connections.get(peer.id)
        if connection is not None and connection.is_closed:
            self._connections.pop(peer.id, None)
            return None
        return connection

    async def connect(
        self,
        peer: Peer,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        if endpoint.transport != self._transport.name:
            raise TransportError(
                f"Endpoint uses transport {endpoint.transport!r}, "
                f"but manager uses {self._transport.name!r}."
            )

        existing = self.get(peer)
        if existing is not None:
            return existing

        if not endpoint.address:
            raise NoEndpointError(f"Peer {peer.id!r} has an empty endpoint address.")

        try:
            connection = await self._wait_for(
                self._transport.connect(endpoint, timeout=timeout),
                timeout,
            )
        except TimeoutError as exc:
            raise PaqtoTimeoutError("Timed out while opening connection.") from exc

        self._connections[peer.id] = connection
        return connection

    async def close_peer(self, peer: Peer) -> None:
        connection = self._connections.pop(peer.id, None)
        if connection is not None:
            await connection.close()

    async def close_all(self) -> None:
        connections = list(self._connections.values())
        self._connections.clear()
        await asyncio.gather(
            *(connection.close() for connection in connections),
            return_exceptions=True,
        )

    @staticmethod
    async def _wait_for(awaitable: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout)
