"""Canonical connection ownership and single-flight connection setup."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from paqto.core.connection import Connection, ConnectionState
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    ConnectionClosedError,
    NoEndpointError,
    PaqtoTimeoutError,
    TransportError,
)
from paqto.core.peer import Peer
from paqto.core.transport import Transport

T = TypeVar("T")
ConnectionPreparation = Callable[[Connection], Awaitable[Connection | None]]


class ConnectionManager:
    """Own canonical peer connections and coalesce concurrent opens.

    Connection creation is single-flight per peer, while unrelated peers may
    connect concurrently. ``prepare`` may return an already established
    canonical connection; this is used when a simultaneous inbound session
    wins deterministic duplicate resolution.
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._connections: dict[str, Connection] = {}
        self._in_flight: dict[str, asyncio.Task[Connection]] = {}
        self._states: dict[str, ConnectionState] = {}
        self._closing_peers: set[str] = set()
        self._closing = False
        self._lock = asyncio.Lock()

    def get(self, peer: Peer) -> Connection | None:
        """Return the current usable canonical connection for ``peer``."""
        return self.get_by_id(peer.id)

    def get_by_id(self, peer_id: str) -> Connection | None:
        """Return a usable canonical connection and forget it if already closed."""
        connection = self._connections.get(peer_id)
        if connection is not None and connection.is_closed:
            self._connections.pop(peer_id, None)
            if self._states.get(peer_id) is ConnectionState.CONNECTED:
                self._states[peer_id] = ConnectionState.DISCONNECTED
            return None
        return connection

    def state(self, peer: Peer | str) -> ConnectionState:
        """Return the logical orchestration state for a peer id."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        connection = self.get_by_id(peer_id)
        if connection is not None:
            return ConnectionState.CONNECTED
        return self._states.get(peer_id, ConnectionState.DISCONNECTED)

    async def mark_reconnecting(self, peer: Peer | str) -> None:
        """Mark a peer as reconnecting if no close or connection supersedes it."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        async with self._lock:
            if (
                not self._closing
                and peer_id not in self._closing_peers
                and self.get_by_id(peer_id) is None
            ):
                self._states[peer_id] = ConnectionState.RECONNECTING

    async def mark_disconnected(self, peer: Peer | str) -> None:
        """Mark a peer disconnected when it has no canonical connection."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        async with self._lock:
            if self.get_by_id(peer_id) is None and peer_id not in self._closing_peers:
                self._states[peer_id] = ConnectionState.DISCONNECTED

    async def connect(
        self,
        peer: Peer,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
        prepare: ConnectionPreparation | None = None,
        reconnecting: bool = False,
    ) -> Connection:
        """Open or reuse the canonical connection for a peer.

        Concurrent calls for the same peer await one shielded setup task.
        ``prepare`` may perform protocol setup and select a different already
        READY connection. Cancelling one waiter does not cancel shared setup.
        """
        self._validate_target(peer, endpoint)
        async with self._lock:
            if self._closing or peer.id in self._closing_peers:
                raise ConnectionClosedError(
                    f"Connection lifecycle for peer {peer.id!r} is closing."
                )
            existing = self.get(peer)
            if existing is not None:
                return existing
            task = self._in_flight.get(peer.id)
            if task is None:
                self._states[peer.id] = (
                    ConnectionState.RECONNECTING
                    if reconnecting
                    else ConnectionState.CONNECTING
                )
                task = asyncio.create_task(
                    self._open_connection(
                        peer,
                        endpoint,
                        timeout=timeout,
                        prepare=prepare,
                    )
                )
                self._in_flight[peer.id] = task

        # One cancelled waiter must not cancel the shared connection attempt.
        return await asyncio.shield(task)

    async def adopt(self, peer: Peer, connection: Connection) -> Connection | None:
        """Install a READY inbound connection and return the one it replaced."""
        async with self._lock:
            if self._closing or peer.id in self._closing_peers:
                raise ConnectionClosedError(
                    f"Connection lifecycle for peer {peer.id!r} is closing."
                )
            previous = self._connections.get(peer.id)
            self._connections[peer.id] = connection
            self._states[peer.id] = ConnectionState.CONNECTED
            if previous is connection:
                return None
            return previous

    async def connection_lost(self, peer: Peer | str, connection: Connection) -> bool:
        """Forget ``connection`` only if it is still canonical."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        async with self._lock:
            if self._connections.get(peer_id) is not connection:
                return False
            self._connections.pop(peer_id, None)
            if peer_id not in self._closing_peers and not self._closing:
                self._states[peer_id] = ConnectionState.DISCONNECTED
            return True

    async def close_peer(self, peer: Peer | str) -> None:
        """Cancel setup, close the canonical connection, and finish at CLOSED."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        async with self._lock:
            self._closing_peers.add(peer_id)
            self._states[peer_id] = ConnectionState.CLOSING
            task = self._in_flight.pop(peer_id, None)
            connection = self._connections.pop(peer_id, None)
        try:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if connection is not None:
                await connection.close()
        finally:
            async with self._lock:
                self._closing_peers.discard(peer_id)
                if peer_id not in self._connections and peer_id not in self._in_flight:
                    self._states[peer_id] = ConnectionState.CLOSED

    async def close_all(self) -> None:
        """Cancel all setup and close every canonical physical connection."""
        async with self._lock:
            self._closing = True
            peer_ids = set(self._states) | set(self._connections) | set(self._in_flight)
            for peer_id in peer_ids:
                self._states[peer_id] = ConnectionState.CLOSING
            tasks = list(self._in_flight.values())
            connections = list(
                {
                    id(connection): connection
                    for connection in self._connections.values()
                }.values()
            )
            self._in_flight.clear()
            self._connections.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(
            *(connection.close() for connection in connections),
            return_exceptions=True,
        )
        async with self._lock:
            for peer_id in peer_ids:
                self._states[peer_id] = ConnectionState.CLOSED
            self._closing_peers.clear()
            self._closing = False

    async def _open_connection(
        self,
        peer: Peer,
        endpoint: Endpoint,
        *,
        timeout: float | None,
        prepare: ConnectionPreparation | None,
    ) -> Connection:
        """Run one peer's transport and preparation attempt with full cleanup."""
        opened: Connection | None = None
        selected: Connection | None = None
        succeeded = False
        primary_error: BaseException | None = None
        try:
            try:
                opened = await self._wait_for(
                    self._transport.connect(endpoint, timeout=timeout),
                    timeout,
                )
            except TimeoutError as exc:
                raise PaqtoTimeoutError("Timed out while opening connection.") from exc

            selected = opened
            if prepare is not None:
                replacement = await prepare(opened)
                if replacement is not None:
                    selected = replacement
            if selected is not opened and not opened.is_closed:
                await opened.close()

            async with self._lock:
                if self._closing or peer.id in self._closing_peers:
                    raise ConnectionClosedError(
                        f"Connection lifecycle for peer {peer.id!r} closed "
                        "during connection setup."
                    )
                previous = self._connections.get(peer.id)
                self._connections[peer.id] = selected
                self._states[peer.id] = ConnectionState.CONNECTED
            if previous is not None and previous is not selected:
                await previous.close()
            succeeded = True
            return selected
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            cleanup_error: BaseException | None = None
            if not succeeded and opened is not None and not opened.is_closed:
                try:
                    await opened.close()
                except BaseException as exc:  # noqa: BLE001 - preserve primary failure
                    cleanup_error = exc
            async with self._lock:
                current = self._in_flight.get(peer.id)
                if current is asyncio.current_task():
                    self._in_flight.pop(peer.id, None)
                if not succeeded and self.get_by_id(peer.id) is None:
                    if self._closing or peer.id in self._closing_peers:
                        self._states[peer.id] = ConnectionState.CLOSED
                    else:
                        self._states[peer.id] = ConnectionState.DISCONNECTED
            if primary_error is None and cleanup_error is not None:
                raise cleanup_error

    def _validate_target(self, peer: Peer, endpoint: Endpoint) -> None:
        if endpoint.transport != self._transport.name:
            raise TransportError(
                f"Endpoint uses transport {endpoint.transport!r}, "
                f"but manager uses {self._transport.name!r}."
            )
        if not endpoint.address:
            raise NoEndpointError(f"Peer {peer.id!r} has an empty endpoint address.")

    @staticmethod
    async def _wait_for(awaitable: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout)
