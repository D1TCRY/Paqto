from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar
from uuid import uuid4

from paqto.core.config import PaqtoConfig
from paqto.core.connection import Connection
from paqto.core.discovered import DiscoveredPeer
from paqto.core.discovery import DiscoveryService
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    AlreadyStartedError,
    NoEndpointError,
    NotStartedError,
    PaqtoTimeoutError,
    PeerNotFoundError,
)
from paqto.core.listener import Listener
from paqto.core.manager import ConnectionManager
from paqto.core.message import Message
from paqto.core.peer import Peer
from paqto.core.router import MessageHandler, MessageRouter
from paqto.core.serializer import Serializer
from paqto.core.transport import Transport

T = TypeVar("T")


class PaqtoNode:
    """Public async facade for one local paqto node."""

    def __init__(
        self,
        *,
        name: str,
        transport: Transport,
        discovery: DiscoveryService,
        serializer: Serializer,
        config: PaqtoConfig | None = None,
        peer_id: str | None = None,
    ) -> None:
        self.peer = Peer(id=peer_id or uuid4().hex, name=name)
        self.transport = transport
        self.discovery = discovery
        self.serializer = serializer
        self.config = config or PaqtoConfig()

        self._connections = ConnectionManager(transport)
        self._router = MessageRouter()
        self._listener: Listener | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._reader_tasks: dict[int, asyncio.Task[None]] = {}
        self._incoming_connections: set[Connection] = set()
        self._known_peers: dict[str, DiscoveredPeer] = {}
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            raise AlreadyStartedError("PaqtoNode is already running.")

        await self.transport.start()
        self._listener = await self.transport.create_listener()
        await self._listener.start()
        self._running = True

        await self.discovery.start(self.peer, [self._listener.local_endpoint])
        self._accept_task = asyncio.create_task(self._accept_loop())

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False

        if self._accept_task is not None:
            self._accept_task.cancel()

        if self._listener is not None:
            await self._listener.close()
            self._listener = None

        await asyncio.gather(
            *(connection.close() for connection in list(self._incoming_connections)),
            return_exceptions=True,
        )
        self._incoming_connections.clear()
        await self._connections.close_all()

        for task in list(self._reader_tasks.values()):
            task.cancel()

        tasks = list(self._reader_tasks.values())
        if self._accept_task is not None:
            tasks.append(self._accept_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._reader_tasks.clear()
        self._accept_task = None
        await self.discovery.stop()
        await self.transport.stop()

    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        self._ensure_running()
        effective_timeout = self.config.discover_timeout if timeout is None else timeout
        try:
            peers = await self._wait_for(
                self.discovery.discover(timeout=effective_timeout),
                effective_timeout,
            )
        except TimeoutError as exc:
            raise PaqtoTimeoutError("Timed out while discovering peers.") from exc

        for discovered in peers:
            self._remember(discovered)
        return peers

    async def connect(
        self,
        target: Peer | DiscoveredPeer,
        *,
        timeout: float | None = None,
    ) -> Connection:
        self._ensure_running()
        discovered = self._resolve_target(target)
        endpoint = self._select_endpoint(discovered)
        effective_timeout = self.config.connect_timeout if timeout is None else timeout
        connection = await self._connections.connect(
            discovered.peer,
            endpoint,
            timeout=effective_timeout,
        )
        self._remember(discovered)
        self._ensure_reader(connection)
        return connection

    async def send(
        self,
        target: Peer | DiscoveredPeer,
        payload: Any,
        *,
        type: str = "message",
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Message:
        discovered = self._resolve_target(target)
        connection = await self.connect(discovered, timeout=timeout)
        message = Message(
            payload=payload,
            type=type,
            sender=self.peer.id,
            recipient=discovered.peer.id,
            headers=headers or {},
        )
        data = self.serializer.serialize(message)
        effective_timeout = self.config.send_timeout if timeout is None else timeout

        try:
            await self._wait_for(connection.send_frame(data), effective_timeout)
        except TimeoutError as exc:
            raise PaqtoTimeoutError("Timed out while sending message.") from exc

        return message

    def on_message(
        self,
        type: str | None = None,
    ) -> Callable[[MessageHandler], MessageHandler]:
        return self._router.on(type)

    async def _accept_loop(self) -> None:
        assert self._listener is not None
        while self._running:
            connection = await self._listener.accept()
            self._incoming_connections.add(connection)
            self._ensure_reader(connection)

    async def _read_connection(self, connection: Connection) -> None:
        try:
            while self._running and not connection.is_closed:
                frame = await connection.receive_frame()
                message = self.serializer.deserialize(frame)
                await self._router.dispatch(message)
        finally:
            self._incoming_connections.discard(connection)
            await connection.close()

    def _ensure_reader(self, connection: Connection) -> None:
        key = id(connection)
        task = self._reader_tasks.get(key)
        if task is not None and not task.done():
            return

        task = asyncio.create_task(self._read_connection(connection))
        self._reader_tasks[key] = task
        task.add_done_callback(lambda finished: self._reader_finished(key, finished))

    def _reader_finished(self, key: int, task: asyncio.Task[None]) -> None:
        self._reader_tasks.pop(key, None)
        if not task.cancelled():
            task.exception()

    def _remember(self, discovered: DiscoveredPeer) -> None:
        if discovered.peer.id == self.peer.id:
            return
        self._known_peers[discovered.peer.id] = discovered

    def _resolve_target(self, target: Peer | DiscoveredPeer) -> DiscoveredPeer:
        if isinstance(target, DiscoveredPeer):
            return target

        known = self._known_peers.get(target.id)
        if known is not None:
            return known

        raise PeerNotFoundError(
            f"Peer {target.id!r} is unknown. Discover it first or pass a DiscoveredPeer."
        )

    def _select_endpoint(self, discovered: DiscoveredPeer) -> Endpoint:
        endpoint = discovered.endpoint_for(self.transport.name)
        if endpoint is None:
            raise NoEndpointError(
                f"Peer {discovered.peer.id!r} has no endpoint for transport "
                f"{self.transport.name!r}."
            )
        return endpoint

    def _ensure_running(self) -> None:
        if not self._running:
            raise NotStartedError("PaqtoNode must be started before this operation.")

    @staticmethod
    async def _wait_for(awaitable: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout)
