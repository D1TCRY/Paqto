"""High-level Paqto node lifecycle, sessions, messaging, and reliability."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar
from uuid import uuid4

from paqto.core.config import (
    BackpressurePolicy,
    HandlerErrorPolicy,
    PaqtoConfig,
)
from paqto.core.connection import Connection, ConnectionState
from paqto.core.discovered import DiscoveredPeer
from paqto.core.discovery import DiscoveryService, NoDiscovery
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    AcknowledgementError,
    AcknowledgementTimeoutError,
    AcknowledgementUnavailableError,
    AlreadyStartedError,
    ConnectionClosedError,
    ConnectionIdleTimeoutError,
    MessageRoutingError,
    NoEndpointError,
    NotStartedError,
    PaqtoTimeoutError,
    PeerExpiredError,
    PeerIdentityMismatchError,
    PeerNotFoundError,
    ProtocolError,
    ProtocolFrameError,
    RequestError,
    RequestTimeoutError,
    ResourceLimitError,
    SerializationError,
    TransportError,
)
from paqto.core.events import (
    EventHandler,
    EventRouter,
    NodeEvent,
    NodeEventType,
)
from paqto.core.listener import Listener
from paqto.core.manager import ConnectionManager
from paqto.core.message import Message
from paqto.core.peer import Peer
from paqto.core.protocol import (
    HEARTBEAT_CAPABILITY,
    TECHNICAL_ACK_CAPABILITY,
    HandshakeOffer,
    HeartbeatPing,
    HeartbeatPong,
    ProtocolSession,
    TechnicalAcknowledgement,
    decode_session_frame,
    encode_acknowledgement_frame,
    encode_application_frame,
    encode_ping_frame,
    encode_pong_frame,
    negotiate_protocol,
)
from paqto.core.router import MessageHandler, MessageRouter
from paqto.core.serializer import Serializer
from paqto.core.transport import Transport

T = TypeVar("T")
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _PendingRequest:
    """Exact-connection state for one in-memory request correlation."""

    future: asyncio.Future[Message]
    connection: Connection
    peer_id: str


@dataclass(slots=True)
class _PendingAcknowledgement:
    """Exact-connection state for one awaited technical acknowledgement."""

    future: asyncio.Future[None]
    connection: Connection
    peer_id: str


@dataclass(slots=True)
class _PendingHeartbeat:
    """Exact-connection state for one outstanding heartbeat challenge."""

    future: asyncio.Future[None]
    connection: Connection


@dataclass(slots=True)
class _InboundDispatch:
    """Application message and session context queued for a handler worker."""

    message: Message
    connection: Connection
    peer_id: str


@dataclass(slots=True)
class _OutboundFrame:
    """Frame data and completion signal stored in a writer queue."""

    data: bytes
    future: asyncio.Future[None]


@dataclass(slots=True)
class _OutboundChannel:
    """Bounded frame queue and sole writer task for one READY connection."""

    queue: asyncio.Queue[_OutboundFrame]
    task: asyncio.Task[None]
    current: _OutboundFrame | None = None


class _ConnectionDirection(str, Enum):
    """Direction used for deterministic duplicate-session selection."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"


class PaqtoNode:
    """Async facade that owns one local peer and its volatile sessions.

    The node coordinates transport, discovery, protocol negotiation, routing,
    request/reply, technical ACKs, heartbeat, reconnect, and cleanup. It does
    not provide persistence, retransmission, application authorization, or
    application-level success guarantees.

    Args:
        name: Human-readable name placed on the local :class:`Peer`.
        transport: Adapter used for outgoing and incoming frame connections.
        discovery: Service used to announce and find reachable peers. Omit or
            pass ``None`` to create no discovery sockets and use explicitly
            provisioned :class:`DiscoveredPeer` endpoints.
        serializer: Application envelope serializer shared with remote peers.
        config: Runtime options; omitted to use :class:`PaqtoConfig` defaults.
        peer_id: Stable local logical id, or ``None`` to generate one.
    """

    def __init__(
        self,
        *,
        name: str,
        transport: Transport,
        discovery: DiscoveryService | None = None,
        serializer: Serializer,
        config: PaqtoConfig | None = None,
        peer_id: str | None = None,
    ) -> None:
        self.peer = Peer(id=peer_id or uuid4().hex, name=name)
        self.transport = transport
        self.discovery = discovery if discovery is not None else NoDiscovery()
        self.serializer = serializer
        self.config = config or PaqtoConfig()

        self._connections = ConnectionManager(transport)
        self._router = MessageRouter()
        self._events = EventRouter()
        self._listener: Listener | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._reader_tasks: dict[int, asyncio.Task[None]] = {}
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._event_task: asyncio.Task[None] | None = None
        self._inbound_queue: asyncio.Queue[_InboundDispatch] | None = None
        self._event_queue: asyncio.Queue[NodeEvent] | None = None
        self._outbound_channels: dict[int, _OutboundChannel] = {}
        self._incoming_connections: set[Connection] = set()
        self._outgoing_connections: set[Connection] = set()
        self._sessions: dict[int, ProtocolSession] = {}
        self._session_connections: dict[int, Connection] = {}
        self._session_directions: dict[int, _ConnectionDirection] = {}
        self._peer_connections: dict[str, Connection] = {}
        self._pending_requests: dict[str, _PendingRequest] = {}
        self._pending_acknowledgements: dict[str, _PendingAcknowledgement] = {}
        self._pending_heartbeats: dict[str, _PendingHeartbeat] = {}
        self._heartbeat_tasks: dict[int, asyncio.Task[None]] = {}
        self._last_received_activity: dict[int, float] = {}
        self._reconnect_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_reconnect_errors: dict[str, BaseException] = {}
        self._reconnect_suppressed: set[str] = set()
        self._message_context: ContextVar[tuple[Message, Connection] | None] = (
            ContextVar(f"paqto_message_context_{id(self)}", default=None)
        )
        self._known_peers: dict[str, DiscoveredPeer] = {}
        self._running = False
        self._lifecycle_generation = 0
        self._lifecycle_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        """Whether startup completed and the node currently accepts operations."""
        return self._running

    def session_for(self, connection: Connection) -> ProtocolSession | None:
        """Return negotiated session data when ``connection`` is READY."""
        return self._sessions.get(id(connection))

    @property
    def pending_request_count(self) -> int:
        """Number of request correlations currently waiting for a reply."""
        return len(self._pending_requests)

    @property
    def pending_acknowledgement_count(self) -> int:
        """Number of messages currently waiting for a technical ACK."""
        return len(self._pending_acknowledgements)

    @property
    def reconnect_task_count(self) -> int:
        """Number of supervised reconnect loops currently active."""
        return len(self._reconnect_tasks)

    @property
    def heartbeat_task_count(self) -> int:
        """Number of supervised heartbeat loops currently active."""
        return len(self._heartbeat_tasks)

    @property
    def inbound_queue_size(self) -> int:
        """Number of application messages waiting for a dispatch worker."""
        return self._inbound_queue.qsize() if self._inbound_queue is not None else 0

    @property
    def outbound_queue_size(self) -> int:
        """Total number of READY-session frames waiting for writers."""
        return sum(
            channel.queue.qsize() for channel in self._outbound_channels.values()
        )

    @property
    def active_connection_count(self) -> int:
        """Physical connections admitted by the node, including handshakes."""
        return len(self._incoming_connections | self._outgoing_connections)

    def connection_state(self, peer: Peer | str) -> ConnectionState:
        """Return the logical connection lifecycle state for a peer."""
        return self._connections.state(peer)

    def connection_for_peer(self, peer: Peer | str) -> Connection | None:
        """Return the canonical READY connection for a peer, when available."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        connection = self._peer_connections.get(peer_id)
        if connection is None or connection.is_closed:
            return None
        return connection

    def last_reconnect_error(self, peer: Peer | str) -> BaseException | None:
        """Return the most recent failed reconnect attempt for diagnostics."""
        peer_id = peer.id if isinstance(peer, Peer) else peer
        return self._last_reconnect_errors.get(peer_id)

    async def start(self) -> None:
        """Start transport, listener, discovery, and node-owned workers.

        Partial startup is rolled back. Calling this while already running
        raises :class:`AlreadyStartedError`. A completed ``stop()`` invalidates
        cached reachability, and the same node may then start again if its
        adapters support restart.
        """
        async with self._lifecycle_lock:
            await self._start_locked()

    async def stop(self) -> None:
        """Stop the node and discard all volatile session and correlation state.

        The method is idempotent when already stopped. It attempts every cleanup
        stage and then raises the first non-cancellation cleanup failure, if any.
        Once entered, cleanup continues even if the calling task is cancelled;
        cancellation is re-raised after resources have been released. Custom
        adapters must cooperate with cancellation and return from close methods.
        """
        caller = asyncio.current_task()
        task = asyncio.create_task(
            self._stop_serialized(skip_task=caller),
            name=f"paqto-stop-{self.peer.id}",
        )
        await self._await_lifecycle_task(task)

    async def network_changed(self) -> list[DiscoveredPeer]:
        """Rebuild network resources after a host-observed environment change.

        The host decides when a network change occurred. Paqto atomically stops
        sessions, invalidates cached remote endpoints, creates a fresh listener,
        republishes its new endpoint through discovery, and performs one new
        discovery pass. Previously connected peers found again are scheduled
        through the configured reconnect policy. With reconnect disabled, the
        returned observations can be connected explicitly by the host.

        The operation requires a running node. Cancellation is reported only
        after the refresh has reached a consistent running or stopped state.
        """
        self._ensure_running()
        caller = asyncio.current_task()
        task = asyncio.create_task(
            self._network_changed_serialized(skip_task=caller),
            name=f"paqto-network-changed-{self.peer.id}",
        )
        return await self._await_lifecycle_task(task)

    async def _start_locked(self) -> None:
        """Start all owned resources while the lifecycle lock is held."""
        if self._running:
            raise AlreadyStartedError("PaqtoNode is already running.")

        listener: Listener | None = None
        transport_attempted = False
        discovery_attempted = False
        try:
            transport_attempted = True
            await self.transport.start()
            listener = await self.transport.create_listener()
            await listener.start()
            discovery_attempted = True
            await self.discovery.start(self.peer, [listener.local_endpoint])
        except BaseException:
            cleanup: list[Awaitable[Any]] = []
            if discovery_attempted:
                cleanup.append(self.discovery.stop())
            if listener is not None:
                cleanup.append(listener.close())
            if transport_attempted:
                cleanup.append(self.transport.stop())
            if cleanup:
                await asyncio.gather(*cleanup, return_exceptions=True)
            self._listener = None
            raise

        self._listener = listener
        self._running = True
        self._lifecycle_generation += 1
        self._inbound_queue = asyncio.Queue(maxsize=self.config.max_inbound_queue)
        self._event_queue = asyncio.Queue(maxsize=self.config.max_event_queue)
        for index in range(self.config.handler_concurrency):
            self._start_dispatch_worker(str(index))
        self._event_task = asyncio.create_task(
            self._event_worker(),
            name=f"paqto-events-{self.peer.id}",
        )
        self._accept_task = asyncio.create_task(
            self._accept_loop(),
            name=f"paqto-accept-{self.peer.id}",
        )

    async def _stop_serialized(
        self,
        *,
        skip_task: asyncio.Task[Any] | None,
    ) -> None:
        async with self._lifecycle_lock:
            await self._stop_locked(skip_task=skip_task)

    async def _stop_locked(
        self,
        *,
        skip_task: asyncio.Task[Any] | None,
    ) -> None:
        """Stop all owned resources while the lifecycle lock is held."""
        if not self._running:
            return

        self._running = False
        self._fail_all_pending("PaqtoNode stopped before the operation completed.")
        self._fail_all_heartbeats()
        current_task = asyncio.current_task()
        excluded_tasks = {
            task for task in (current_task, skip_task) if task is not None
        }
        accept_task = self._accept_task
        if accept_task is not None and accept_task not in excluded_tasks:
            accept_task.cancel()

        reader_tasks = [
            task for task in self._reader_tasks.values() if task not in excluded_tasks
        ]
        for task in reader_tasks:
            task.cancel()
        handler_tasks = [
            task for task in self._handler_tasks if task not in excluded_tasks
        ]
        for task in handler_tasks:
            task.cancel()
        heartbeat_tasks = [
            task
            for task in self._heartbeat_tasks.values()
            if task not in excluded_tasks
        ]
        reconnect_tasks = [
            task
            for task in self._reconnect_tasks.values()
            if task not in excluded_tasks
        ]
        outbound_tasks = [
            channel.task
            for channel in self._outbound_channels.values()
            if channel.task not in excluded_tasks
        ]
        event_task = self._event_task
        for task in [
            *heartbeat_tasks,
            *reconnect_tasks,
            *outbound_tasks,
        ]:
            task.cancel()
        if event_task is not None and event_task not in excluded_tasks:
            event_task.cancel()

        self._fail_all_outbound(
            ConnectionClosedError(
                "PaqtoNode stopped before the frame could be sent."
            )
        )

        listener = self._listener
        self._listener = None
        physical_connections = list(
            self._incoming_connections | self._outgoing_connections
        )
        self._incoming_connections.clear()
        self._outgoing_connections.clear()
        self._sessions.clear()
        self._session_connections.clear()
        self._session_directions.clear()
        self._peer_connections.clear()
        self._last_received_activity.clear()

        cleanup: list[Awaitable[Any]] = [
            *(connection.close() for connection in physical_connections),
            self._connections.close_all(),
        ]
        if listener is not None:
            cleanup.append(listener.close())
        cleanup_results = await asyncio.gather(
            *cleanup,
            return_exceptions=True,
        )

        tasks = [
            *reader_tasks,
            *handler_tasks,
            *heartbeat_tasks,
            *reconnect_tasks,
            *outbound_tasks,
        ]
        if event_task is not None and event_task not in excluded_tasks:
            tasks.append(event_task)
        if accept_task is not None and accept_task not in excluded_tasks:
            tasks.append(accept_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._reader_tasks.clear()
        self._handler_tasks.clear()
        self._heartbeat_tasks.clear()
        self._reconnect_tasks.clear()
        self._reconnect_suppressed.clear()
        self._last_reconnect_errors.clear()
        self._known_peers.clear()
        self._accept_task = None
        self._event_task = None
        self._outbound_channels.clear()
        self._discard_queue(self._inbound_queue)
        self._discard_queue(self._event_queue)
        self._inbound_queue = None
        self._event_queue = None
        service_results = await asyncio.gather(
            self.discovery.stop(),
            self.transport.stop(),
            return_exceptions=True,
        )

        errors = [
            result
            for result in [*cleanup_results, *service_results]
            if isinstance(result, BaseException)
            and not isinstance(result, asyncio.CancelledError)
        ]
        if errors:
            raise errors[0]

    async def _network_changed_serialized(
        self,
        *,
        skip_task: asyncio.Task[Any] | None,
    ) -> list[DiscoveredPeer]:
        async with self._lifecycle_lock:
            if not self._running:
                raise NotStartedError(
                    "PaqtoNode must be running before network_changed()."
                )
            reconnect_peer_ids = set(self._peer_connections) | set(
                self._reconnect_tasks
            )
            await self._stop_locked(skip_task=skip_task)
            await self._start_locked()
            discovered = await self.discover()
            for peer in discovered:
                if peer.peer.id in reconnect_peer_ids:
                    self._schedule_reconnect(peer.peer.id)
            return discovered

    @staticmethod
    async def _await_lifecycle_task(task: asyncio.Task[T]) -> T:
        """Finish an atomic lifecycle transition before propagating cancellation."""
        cancellation: asyncio.CancelledError | None = None
        failure: BaseException | None = None
        result: T | None = None
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError as exc:
                cancellation = exc
                if task.done():
                    break
            except BaseException as exc:  # noqa: BLE001 - preserve cleanup result
                failure = exc
                break

        if task.done() and not task.cancelled() and failure is None:
            try:
                result = task.result()
            except BaseException as exc:  # noqa: BLE001 - propagate below
                failure = exc
        if cancellation is not None:
            raise cancellation from failure
        if failure is not None:
            raise failure
        return result  # type: ignore[return-value]

    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        """Discover peers and return observations still fresh under node policy.

        The node must be running. ``timeout`` is a seconds budget; ``None`` uses
        ``config.discover_timeout``. Results are reachability claims and do not
        authenticate the declared peer ids.
        """
        self._ensure_running()
        generation = self._lifecycle_generation
        for peer_id, known in list(self._known_peers.items()):
            if not known.is_fresh(self.config.peer_ttl):
                self._known_peers.pop(peer_id, None)
                self._emit_event(NodeEventType.PEER_EXPIRED, peer_id=peer_id)
        effective_timeout = self.config.discover_timeout if timeout is None else timeout
        try:
            discovery = self.discovery.discover(timeout=effective_timeout)
            if effective_timeout == 0:
                peers = await discovery
            else:
                peers = await self._wait_for(discovery, effective_timeout)
        except TimeoutError as exc:
            raise PaqtoTimeoutError("Timed out while discovering peers.") from exc

        if not self._running or generation != self._lifecycle_generation:
            raise NotStartedError("PaqtoNode stopped while discovery was running.")

        peers = [
            discovered
            for discovered in peers
            if discovered.is_fresh(self.config.peer_ttl)
        ]
        for discovered in peers:
            is_new = discovered.peer.id not in self._known_peers
            self._remember(discovered)
            if is_new:
                self._emit_event(
                    NodeEventType.PEER_DISCOVERED,
                    peer_id=discovered.peer.id,
                )
        return peers

    async def connect(
        self,
        target: Peer | DiscoveredPeer,
        *,
        timeout: float | None = None,
    ) -> Connection:
        """Open or reuse a READY connection to ``target``.

        ``Peer`` targets must already be known through discovery unless a READY
        connection exists. The method completes only after transport security,
        hello negotiation, identity checks, and duplicate selection. ``timeout``
        overrides the default transport-connect deadline in seconds.
        """
        self._ensure_running()
        discovered = self._resolve_target(target)
        self._reconnect_suppressed.discard(discovered.peer.id)
        return await self._connect_discovered(
            discovered,
            timeout=timeout,
            reconnecting=False,
        )

    async def disconnect(self, target: Peer | DiscoveredPeer) -> None:
        """Close a peer session without scheduling automatic reconnect."""
        peer_id = target.peer.id if isinstance(target, DiscoveredPeer) else target.id
        self._reconnect_suppressed.add(peer_id)
        reconnect_task = self._reconnect_tasks.get(peer_id)
        if reconnect_task is not None and reconnect_task is not asyncio.current_task():
            reconnect_task.cancel()
            await asyncio.gather(reconnect_task, return_exceptions=True)
        connection = self._peer_connections.get(peer_id)
        if connection is not None:
            self._deactivate_session(connection)
            self._fail_pending_for_connection(connection)
            await self._cancel_heartbeat(connection)
            await self._close_outbound_channel(connection)
            self._incoming_connections.discard(connection)
            self._outgoing_connections.discard(connection)
            self._emit_event(
                NodeEventType.DISCONNECTED,
                peer_id=peer_id,
                connection=connection,
                metadata={"reason": "explicit_disconnect"},
            )
        await self._connections.close_peer(peer_id)

    async def _connect_discovered(
        self,
        discovered: DiscoveredPeer,
        *,
        timeout: float | None,
        reconnecting: bool,
    ) -> Connection:
        """Establish one fresh discovered peer as the canonical READY session."""
        existing = self.connection_for_peer(discovered.peer.id)
        if existing is not None:
            return existing
        if not discovered.is_fresh(self.config.peer_ttl):
            self._emit_event(
                NodeEventType.PEER_EXPIRED,
                peer_id=discovered.peer.id,
            )
            raise PeerExpiredError(
                f"Discovery information for peer {discovered.peer.id!r} expired."
            )
        endpoint = self._select_endpoint(discovered)
        effective_timeout = self.config.connect_timeout if timeout is None else timeout
        self._emit_event(
            NodeEventType.RECONNECTING if reconnecting else NodeEventType.CONNECTING,
            peer_id=discovered.peer.id,
        )
        logger.info(
            "Paqto connection attempt",
            extra=self._log_context(
                peer_id=discovered.peer.id,
                reconnecting=reconnecting,
            ),
        )
        connection = await self._connections.connect(
            discovered.peer,
            endpoint,
            timeout=effective_timeout,
            prepare=lambda candidate: self._prepare_outgoing_connection(
                candidate,
                discovered.peer,
            ),
            reconnecting=reconnecting,
        )
        self._remember(discovered)
        session = self.session_for(connection)
        if session is None:
            await self._connections.close_peer(discovered.peer)
            raise ProtocolFrameError(
                "Connection manager returned a connection without a ready "
                "Paqto protocol session."
            )
        self._ensure_reader(connection, session)
        self._last_reconnect_errors.pop(discovered.peer.id, None)
        reconnect_task = self._reconnect_tasks.get(discovered.peer.id)
        if reconnect_task is not None and reconnect_task is not asyncio.current_task():
            reconnect_task.cancel()
        return connection

    async def send(
        self,
        target: Peer | DiscoveredPeer,
        payload: Any,
        *,
        type: str = "message",
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        require_ack: bool = False,
        acknowledgement_timeout: float | None = None,
    ) -> Message:
        """Construct and send an application message to a peer.

        The node must be running. The return value is the locally constructed
        envelope after its frame writer completes and, when requested, a Paqto
        technical ACK arrives. Neither condition means remote application code
        succeeded. Queue backpressure may block until capacity is available.
        """
        self._ensure_running()
        discovered = self._resolve_target(target)
        connection = await self.connect(discovered, timeout=timeout)
        message = Message(
            payload=payload,
            type=type,
            sender=self.peer.id,
            recipient=discovered.peer.id,
            headers=headers or {},
        )
        effective_timeout = self.config.send_timeout if timeout is None else timeout
        await self._send_message(
            connection,
            message,
            timeout=effective_timeout,
            require_ack=require_ack,
            acknowledgement_timeout=acknowledgement_timeout,
        )

        return message

    async def request(
        self,
        target: Peer | DiscoveredPeer,
        payload: Any,
        *,
        type: str = "request",
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        require_ack: bool = False,
        acknowledgement_timeout: float | None = None,
    ) -> Message:
        """Send a request and wait for an in-memory, exact-session reply.

        ``timeout`` controls only the reply wait; connection and frame writing
        use their configured defaults. Timeout, cancellation, disconnect, and
        shutdown remove the correlation. A later reply is not returned to the
        expired caller.
        """
        self._ensure_running()
        discovered = self._resolve_target(target)
        connection = await self.connect(discovered)
        message = Message(
            payload=payload,
            type=type,
            sender=self.peer.id,
            recipient=discovered.peer.id,
            headers=headers or {},
        )
        future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        if message.id in self._pending_requests:
            raise RequestError(f"A request with id {message.id!r} is already pending.")
        if len(self._pending_requests) >= self.config.max_pending_requests:
            self._report_resource_limit(
                "Pending request limit reached.",
                peer_id=discovered.peer.id,
                connection=connection,
                message=message,
            )
            raise ResourceLimitError(
                "The node reached its maximum number of pending requests."
            )
        self._ensure_running()
        self._pending_requests[message.id] = _PendingRequest(
            future=future,
            connection=connection,
            peer_id=discovered.peer.id,
        )
        logger.debug(
            "Paqto request correlation registered",
            extra=self._log_context(
                peer_id=discovered.peer.id,
                connection=connection,
                message=message,
            ),
        )
        effective_timeout = self.config.request_timeout if timeout is None else timeout
        try:
            await self._send_message(
                connection,
                message,
                timeout=self.config.send_timeout,
                require_ack=require_ack,
                acknowledgement_timeout=acknowledgement_timeout,
            )
            try:
                return await self._wait_for(future, effective_timeout)
            except TimeoutError as exc:
                raise RequestTimeoutError(
                    f"Timed out waiting for reply to message {message.id!r}."
                ) from exc
        finally:
            current = self._pending_requests.get(message.id)
            if current is not None and current.future is future:
                self._pending_requests.pop(message.id, None)
            if not future.done():
                future.cancel()

    async def reply(
        self,
        message: Message,
        payload: Any,
        *,
        type: str = "reply",
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        require_ack: bool = False,
        acknowledgement_timeout: float | None = None,
    ) -> Message:
        """Reply to an inbound message on its READY session.

        The response uses ``message.id`` as ``reply_to``. Calling from the
        message handler preserves exact connection context; outside a handler,
        the peer must have one unambiguous READY connection.
        """
        self._ensure_running()
        if not isinstance(message, Message):
            raise TypeError("message must be a Message.")
        if not isinstance(message.id, str) or not message.id:
            raise RequestError("Cannot reply to a message without a valid id.")
        if not isinstance(message.sender, str) or not message.sender:
            raise RequestError("Cannot reply to a message without a sender.")
        connection = self._connection_for_reply(message)
        response = Message(
            payload=payload,
            type=type,
            sender=self.peer.id,
            recipient=message.sender,
            headers=headers or {},
            reply_to=message.id,
        )
        effective_timeout = self.config.send_timeout if timeout is None else timeout
        await self._send_message(
            connection,
            response,
            timeout=effective_timeout,
            require_ack=require_ack,
            acknowledgement_timeout=acknowledgement_timeout,
        )
        return response

    def on_message(
        self,
        type: str | None = None,
    ) -> Callable[[MessageHandler], MessageHandler]:
        """Register a sync or async handler for one message type or all types."""
        return self._router.on(type)

    def on_event(
        self,
        type: NodeEventType | None = None,
    ) -> Callable[[EventHandler], EventHandler]:
        """Register a sync or async observer for node lifecycle events."""
        return self._events.on(type)

    async def _accept_loop(self) -> None:
        """Accept physical connections and enforce post-accept node admission."""
        assert self._listener is not None
        generation = self._lifecycle_generation
        while self._running and generation == self._lifecycle_generation:
            try:
                connection = await self._listener.accept()
            except asyncio.CancelledError:
                raise
            except ConnectionClosedError as exc:
                if self._running:
                    self._report_transport_error(exc)
                return
            except Exception as exc:  # noqa: BLE001 - supervises transport adapters
                self._report_transport_error(exc)
                await asyncio.sleep(0.1)
                continue
            if self.active_connection_count >= self.config.max_connections:
                self._report_resource_limit(
                    "Incoming connection rejected because max_connections was reached.",
                    connection=connection,
                )
                await connection.close()
                continue
            self._incoming_connections.add(connection)
            self._ensure_incoming_task(connection)

    async def _prepare_outgoing_connection(
        self,
        connection: Connection,
        peer: Peer,
    ) -> Connection:
        """Negotiate and canonicalize an outgoing connection before manager use."""
        if (
            connection not in self._outgoing_connections
            and self.active_connection_count >= self.config.max_connections
        ):
            self._report_resource_limit(
                "Outgoing connection rejected because max_connections was reached.",
                peer_id=peer.id,
                connection=connection,
            )
            raise ResourceLimitError(
                "The node reached its maximum simultaneous connection limit."
            )
        self._outgoing_connections.add(connection)
        try:
            session = await self._negotiate(
                connection,
                expected_peer_id=peer.id,
            )
            selected, loser = self._select_ready_connection(
                connection,
                session,
                _ConnectionDirection.OUTBOUND,
            )
            if loser is not None and loser is not connection:
                await self._retire_replaced_connection(loser)
            if selected is not connection:
                self._outgoing_connections.discard(connection)
            return selected
        except BaseException as exc:
            self._outgoing_connections.discard(connection)
            if isinstance(exc, ProtocolError):
                self._report_protocol_error(connection, exc, peer_id=peer.id)
            raise

    async def _run_incoming_connection(self, connection: Connection) -> None:
        """Negotiate, adopt, and read one admitted incoming connection."""
        reading = False
        try:
            try:
                session = await self._negotiate(connection)
            except ConnectionClosedError:
                raise
            except ProtocolError as exc:
                self._report_protocol_error(connection, exc)
                raise
            except Exception as exc:
                self._report_protocol_error(connection, exc)
                raise
            selected, loser = self._select_ready_connection(
                connection,
                session,
                _ConnectionDirection.INBOUND,
            )
            if selected is not connection:
                return
            if loser is not None:
                await self._retire_replaced_connection(loser)
            previous = await self._connections.adopt(
                Peer(id=session.peer_id),
                connection,
            )
            if previous is not None and previous is not loser:
                await self._retire_replaced_connection(previous)
            if self._peer_connections.get(session.peer_id) is not connection:
                await self._connections.connection_lost(session.peer_id, connection)
                return
            self._ensure_heartbeat(connection, session)
            reading = True
            await self._read_connection(connection, session)
        finally:
            if not reading:
                self._deactivate_session(connection)
                self._incoming_connections.discard(connection)
                self._fail_pending_for_connection(connection)
                await connection.close()

    async def _read_connection(
        self,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        """Process controls and application envelopes for one READY session."""
        try:
            while self._running and not connection.is_closed:
                try:
                    frame = await self._wait_for(
                        connection.receive_frame(),
                        self.config.idle_timeout,
                    )
                except TimeoutError as exc:
                    raise ConnectionIdleTimeoutError(
                        "READY connection exceeded the configured idle timeout."
                    ) from exc
                self._last_received_activity[id(connection)] = (
                    asyncio.get_running_loop().time()
                )
                decoded = decode_session_frame(
                    frame,
                    max_message_size=session.max_message_size,
                )
                if isinstance(decoded, TechnicalAcknowledgement):
                    if TECHNICAL_ACK_CAPABILITY not in session.capabilities:
                        raise ProtocolFrameError(
                            "Received an acknowledgement that was not negotiated."
                        )
                    self._resolve_acknowledgement(connection, session, decoded)
                    continue
                if isinstance(decoded, HeartbeatPing):
                    if HEARTBEAT_CAPABILITY not in session.capabilities:
                        raise ProtocolFrameError(
                            "Received a heartbeat that was not negotiated."
                        )
                    await self._send_heartbeat_response(
                        connection,
                        decoded.ping_id,
                    )
                    continue
                if isinstance(decoded, HeartbeatPong):
                    if HEARTBEAT_CAPABILITY not in session.capabilities:
                        raise ProtocolFrameError(
                            "Received a heartbeat response that was not negotiated."
                        )
                    self._resolve_heartbeat(connection, decoded)
                    continue
                application_data = decoded
                try:
                    message = self.serializer.deserialize(application_data)
                except SerializationError:
                    raise
                except Exception as exc:
                    raise SerializationError("Could not deserialize message.") from exc
                if not isinstance(message, Message):
                    raise SerializationError(
                        "Serializer.deserialize() must return a Message."
                    )
                self._validate_incoming_message(message)
                if message.sender != session.peer_id:
                    raise PeerIdentityMismatchError(
                        f"Application message sender {message.sender!r} does not "
                        f"match session peer {session.peer_id!r}."
                    )
                if message.recipient not in (None, self.peer.id):
                    raise ProtocolFrameError(
                        f"Application message recipient {message.recipient!r} does "
                        f"not match local peer {self.peer.id!r}."
                    )
                if TECHNICAL_ACK_CAPABILITY in session.capabilities:
                    await self._send_acknowledgement(connection, message.id)
                if message.reply_to is not None:
                    self._resolve_reply(connection, session, message)
                    continue
                await self._enqueue_dispatch(message, connection, session)
        except ConnectionClosedError:
            pass
        except ResourceLimitError:
            pass
        except (ProtocolError, SerializationError, TransportError) as exc:
            self._report_protocol_error(connection, exc, peer_id=session.peer_id)
        except ConnectionIdleTimeoutError as exc:
            logger.warning(
                "Paqto connection idle timeout",
                extra=self._log_context(
                    peer_id=session.peer_id,
                    connection=connection,
                    error=exc,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - supervises protocol adapters
            self._report_protocol_error(connection, exc, peer_id=session.peer_id)
        finally:
            await self._connection_ended(connection, session)

    def _select_ready_connection(
        self,
        connection: Connection,
        session: ProtocolSession,
        direction: _ConnectionDirection,
    ) -> tuple[Connection, Connection | None]:
        """Choose a canonical READY connection using stable peer-id ordering.

        Same-direction duplicates keep the first READY connection. For opposite
        directions, the lexicographically smaller peer keeps its outbound side,
        so both peers independently select the same physical connection.
        """
        if session.peer_id == self.peer.id:
            raise PeerIdentityMismatchError(
                "A remote Paqto session cannot use the local peer identity."
            )
        existing = self._peer_connections.get(session.peer_id)
        if existing is not None and existing.is_closed:
            self._deactivate_session(existing)
            existing = None
        if existing is None:
            self._register_session(connection, session, direction)
            return connection, None
        if existing is connection:
            return connection, None

        existing_direction = self._session_directions[id(existing)]
        preferred_direction = (
            _ConnectionDirection.OUTBOUND
            if self.peer.id < session.peer_id
            else _ConnectionDirection.INBOUND
        )
        if (
            direction is preferred_direction
            and existing_direction is not preferred_direction
        ):
            self._deactivate_session(existing)
            self._register_session(connection, session, direction)
            return connection, existing

        # Same-direction duplicates keep the first READY session. Opposite
        # directions keep the side dictated by the stable peer-id ordering.
        return existing, connection

    async def _retire_replaced_connection(self, connection: Connection) -> None:
        """Remove and close a connection that lost duplicate-session selection."""
        session = self._sessions.get(id(connection))
        self._deactivate_session(connection)
        self._fail_pending_for_connection(connection)
        await self._cancel_heartbeat(connection)
        await self._close_outbound_channel(connection)
        self._incoming_connections.discard(connection)
        self._outgoing_connections.discard(connection)
        if session is not None:
            await self._connections.connection_lost(session.peer_id, connection)
        await connection.close()

    async def _connection_ended(
        self,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        """Clean up one ended session and schedule eligible session reconnect."""
        was_canonical = self._peer_connections.get(session.peer_id) is connection
        self._deactivate_session(connection)
        self._incoming_connections.discard(connection)
        self._outgoing_connections.discard(connection)
        self._fail_pending_for_connection(connection)
        await self._cancel_heartbeat(connection)
        await self._close_outbound_channel(connection)
        removed = await self._connections.connection_lost(
            session.peer_id,
            connection,
        )
        await connection.close()
        if was_canonical or removed:
            self._emit_event(
                NodeEventType.DISCONNECTED,
                peer_id=session.peer_id,
                connection=connection,
            )
            logger.info(
                "Paqto session disconnected",
                extra=self._log_context(
                    peer_id=session.peer_id,
                    connection=connection,
                ),
            )
            self._schedule_reconnect(session.peer_id)

    def _ensure_heartbeat(
        self,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        """Start one heartbeat supervisor when the canonical session supports it."""
        interval = self.config.heartbeat_interval
        if (
            interval is None
            or HEARTBEAT_CAPABILITY not in session.capabilities
            or self._peer_connections.get(session.peer_id) is not connection
        ):
            return
        key = id(connection)
        task = self._heartbeat_tasks.get(key)
        if task is not None and not task.done():
            return
        self._last_received_activity[key] = asyncio.get_running_loop().time()
        task = asyncio.create_task(self._heartbeat_loop(connection, session))
        self._heartbeat_tasks[key] = task
        task.add_done_callback(lambda finished: self._heartbeat_finished(key, finished))

    async def _heartbeat_loop(
        self,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        """Probe an otherwise idle READY session and close it on PONG timeout."""
        interval = self.config.heartbeat_interval
        timeout = self.config.heartbeat_timeout
        assert interval is not None
        assert timeout is not None
        key = id(connection)
        try:
            while (
                self._running
                and not connection.is_closed
                and self._peer_connections.get(session.peer_id) is connection
            ):
                await self._sleep(interval)
                if (
                    not self._running
                    or connection.is_closed
                    or self._peer_connections.get(session.peer_id) is not connection
                ):
                    return
                now = asyncio.get_running_loop().time()
                last_activity = self._last_received_activity.get(key, now)
                if now - last_activity < interval:
                    continue

                ping_id = uuid4().hex
                future: asyncio.Future[None] = (
                    asyncio.get_running_loop().create_future()
                )
                pending = _PendingHeartbeat(future=future, connection=connection)
                self._pending_heartbeats[ping_id] = pending
                try:
                    await self._queue_frame(connection, encode_ping_frame(ping_id))
                    try:
                        await self._wait_for(future, timeout)
                    except TimeoutError:
                        await connection.close()
                        return
                finally:
                    current = self._pending_heartbeats.get(ping_id)
                    if current is pending:
                        self._pending_heartbeats.pop(ping_id, None)
                    if not future.done():
                        future.cancel()
        finally:
            self._fail_heartbeats_for_connection(connection)
            current_task = asyncio.current_task()
            if self._heartbeat_tasks.get(key) is current_task:
                self._heartbeat_tasks.pop(key, None)

    def _resolve_heartbeat(
        self,
        connection: Connection,
        pong: HeartbeatPong,
    ) -> None:
        """Resolve a heartbeat only on the connection that created its ping id."""
        pending = self._pending_heartbeats.get(pong.ping_id)
        if pending is None or pending.connection is not connection:
            return
        self._pending_heartbeats.pop(pong.ping_id, None)
        if not pending.future.done():
            pending.future.set_result(None)

    async def _cancel_heartbeat(self, connection: Connection) -> None:
        key = id(connection)
        task = self._heartbeat_tasks.pop(key, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._last_received_activity.pop(key, None)
        self._fail_heartbeats_for_connection(connection)

    def _heartbeat_finished(
        self,
        key: int,
        task: asyncio.Task[None],
    ) -> None:
        if self._heartbeat_tasks.get(key) is task:
            self._heartbeat_tasks.pop(key, None)
        if not task.cancelled():
            task.exception()

    def _fail_heartbeats_for_connection(self, connection: Connection) -> None:
        for ping_id, pending in list(self._pending_heartbeats.items()):
            if pending.connection is connection:
                self._pending_heartbeats.pop(ping_id, None)
                if not pending.future.done():
                    pending.future.cancel()

    def _fail_all_heartbeats(self) -> None:
        pending = list(self._pending_heartbeats.values())
        self._pending_heartbeats.clear()
        for heartbeat in pending:
            if not heartbeat.future.done():
                heartbeat.future.cancel()

    def _schedule_reconnect(self, peer_id: str) -> None:
        """Start at most one reconnect loop for a fresh, unsuppressed peer."""
        policy = self.config.reconnect
        if (
            not self._running
            or not policy.enabled
            or peer_id in self._reconnect_suppressed
        ):
            return
        discovered = self._known_peers.get(peer_id)
        if discovered is None or not discovered.is_fresh(self.config.peer_ttl):
            return
        task = self._reconnect_tasks.get(peer_id)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(self._reconnect_loop(peer_id))
        self._reconnect_tasks[peer_id] = task
        task.add_done_callback(
            lambda finished: self._reconnect_finished(peer_id, finished)
        )

    async def _reconnect_loop(self, peer_id: str) -> None:
        """Recreate a peer session with bounded exponential-backoff attempts."""
        policy = self.config.reconnect
        attempt = 0
        while self._running and peer_id not in self._reconnect_suppressed:
            if policy.max_attempts is not None and attempt >= policy.max_attempts:
                await self._connections.mark_disconnected(peer_id)
                return
            discovered = self._known_peers.get(peer_id)
            if discovered is None or not discovered.is_fresh(self.config.peer_ttl):
                await self._connections.mark_disconnected(peer_id)
                return
            await self._connections.mark_reconnecting(peer_id)
            await self._sleep(policy.delay_for_attempt(attempt))
            if not self._running or peer_id in self._reconnect_suppressed:
                return
            try:
                await self._connect_discovered(
                    discovered,
                    timeout=self.config.connect_timeout,
                    reconnecting=True,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervises transport adapters
                self._last_reconnect_errors[peer_id] = exc
                attempt += 1
        await self._connections.mark_disconnected(peer_id)

    def _reconnect_finished(
        self,
        peer_id: str,
        task: asyncio.Task[None],
    ) -> None:
        if self._reconnect_tasks.get(peer_id) is task:
            self._reconnect_tasks.pop(peer_id, None)
        if not task.cancelled():
            task.exception()

    @staticmethod
    async def _sleep(delay: float) -> None:
        await asyncio.sleep(delay)

    def _ensure_reader(
        self,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        key = id(connection)
        task = self._reader_tasks.get(key)
        if task is not None and not task.done():
            return

        task = asyncio.create_task(self._read_connection(connection, session))
        self._register_connection_task(key, task)
        self._ensure_heartbeat(connection, session)

    def _ensure_incoming_task(self, connection: Connection) -> None:
        key = id(connection)
        task = self._reader_tasks.get(key)
        if task is not None and not task.done():
            return

        task = asyncio.create_task(self._run_incoming_connection(connection))
        self._register_connection_task(key, task)

    def _register_connection_task(
        self,
        key: int,
        task: asyncio.Task[None],
    ) -> None:
        self._reader_tasks[key] = task
        task.add_done_callback(lambda finished: self._reader_finished(key, finished))

    def _reader_finished(self, key: int, task: asyncio.Task[None]) -> None:
        self._reader_tasks.pop(key, None)
        if not task.cancelled():
            task.exception()

    async def _enqueue_dispatch(
        self,
        message: Message,
        connection: Connection,
        session: ProtocolSession,
    ) -> None:
        """Submit an ordinary message under configured inbound backpressure."""
        queue = self._inbound_queue
        if queue is None:
            raise ConnectionClosedError("The inbound dispatch queue is not running.")
        item = _InboundDispatch(
            message=message,
            connection=connection,
            peer_id=session.peer_id,
        )
        if self.config.inbound_backpressure is BackpressurePolicy.WAIT:
            await queue.put(item)
            return
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull as exc:
            self._report_resource_limit(
                "Inbound application queue is full.",
                peer_id=session.peer_id,
                connection=connection,
                message=message,
            )
            raise ResourceLimitError(
                "Inbound application queue reached its configured limit."
            ) from exc

    async def _dispatch_worker(self) -> None:
        """Consume the bounded inbound queue and apply handler failure policy."""
        generation = self._lifecycle_generation
        while self._running and generation == self._lifecycle_generation:
            queue = self._inbound_queue
            if queue is None:
                return
            item = await queue.get()
            try:
                await self._dispatch_message(item.message, item.connection)
            except MessageRoutingError as exc:
                self._report_handler_error(item, exc)
                if (
                    self.config.handler_error_policy
                    is HandlerErrorPolicy.CLOSE_CONNECTION
                ):
                    await item.connection.close()
            finally:
                queue.task_done()

    async def _dispatch_message(
        self,
        message: Message,
        connection: Connection,
    ) -> None:
        """Dispatch while exposing exact reply connection context to handlers."""
        token = self._message_context.set((message, connection))
        try:
            await self._router.dispatch(message)
        finally:
            self._message_context.reset(token)

    def _handler_finished(
        self,
        task: asyncio.Task[None],
        generation: int,
    ) -> None:
        self._handler_tasks.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    "Paqto dispatch worker terminated unexpectedly",
                    extra=self._log_context(error=error),
                )
        if self._running and generation == self._lifecycle_generation:
            self._start_dispatch_worker("replacement")

    def _start_dispatch_worker(self, label: str) -> None:
        generation = self._lifecycle_generation
        task = asyncio.create_task(
            self._dispatch_worker(),
            name=f"paqto-dispatch-{self.peer.id}-{label}",
        )
        self._handler_tasks.add(task)
        task.add_done_callback(
            lambda finished: self._handler_finished(finished, generation)
        )

    async def _event_worker(self) -> None:
        """Deliver best-effort events outside connection-processing tasks."""
        generation = self._lifecycle_generation
        while self._running and generation == self._lifecycle_generation:
            queue = self._event_queue
            if queue is None:
                return
            event = await queue.get()
            try:
                errors = await self._events.dispatch(event)
                for error in errors:
                    logger.error(
                        "Paqto event listener failed",
                        extra=self._log_context(
                            peer_id=event.peer_id,
                            connection_id=event.connection_id,
                            error=error,
                            event_type=event.type.value,
                        ),
                    )
            finally:
                queue.task_done()

    def _emit_event(
        self,
        type: NodeEventType,
        *,
        peer_id: str | None = None,
        connection: Connection | None = None,
        error: BaseException | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Queue a best-effort event, dropping it when the event queue is full."""
        queue = self._event_queue
        if queue is None:
            return
        event = NodeEvent(
            type=type,
            local_peer_id=self.peer.id,
            peer_id=peer_id,
            connection_id=(self._connection_id(connection) if connection else None),
            error=error,
            metadata=metadata or {},
        )
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(
                "Paqto event queue is full; observation was dropped",
                extra=self._log_context(
                    peer_id=peer_id,
                    connection=connection,
                    event_type=type.value,
                ),
            )

    def _report_handler_error(
        self,
        item: _InboundDispatch,
        error: MessageRoutingError,
    ) -> None:
        self._emit_event(
            NodeEventType.HANDLER_ERROR,
            peer_id=item.peer_id,
            connection=item.connection,
            error=error,
            metadata={
                "message_id": item.message.id,
                "message_type": item.message.type,
            },
        )
        logger.error(
            "Paqto application handler failed",
            extra=self._log_context(
                peer_id=item.peer_id,
                connection=item.connection,
                message=item.message,
                error=error,
            ),
        )

    def _report_protocol_error(
        self,
        connection: Connection,
        error: BaseException,
        *,
        peer_id: str | None = None,
    ) -> None:
        self._emit_event(
            NodeEventType.PROTOCOL_ERROR,
            peer_id=peer_id,
            connection=connection,
            error=error,
        )
        logger.warning(
            "Paqto protocol/session processing failed",
            extra=self._log_context(
                peer_id=peer_id,
                connection=connection,
                error=error,
            ),
        )

    def _report_transport_error(self, error: BaseException) -> None:
        self._emit_event(NodeEventType.TRANSPORT_ERROR, error=error)
        logger.warning(
            "Paqto listener/transport processing failed",
            extra=self._log_context(error=error),
        )

    def _report_resource_limit(
        self,
        description: str,
        *,
        peer_id: str | None = None,
        connection: Connection | None = None,
        message: Message | None = None,
    ) -> None:
        error = ResourceLimitError(description)
        self._emit_event(
            NodeEventType.RESOURCE_LIMIT,
            peer_id=peer_id,
            connection=connection,
            error=error,
        )
        logger.warning(
            "Paqto resource limit reached",
            extra=self._log_context(
                peer_id=peer_id,
                connection=connection,
                message=message,
                error=error,
            ),
        )

    def _ensure_outbound_channel(self, connection: Connection) -> None:
        """Create the sole bounded writer queue for a READY connection."""
        key = id(connection)
        channel = self._outbound_channels.get(key)
        if channel is not None and not channel.task.done():
            return
        queue: asyncio.Queue[_OutboundFrame] = asyncio.Queue(
            maxsize=self.config.max_outbound_queue
        )
        task = asyncio.create_task(
            self._outbound_worker(connection, queue),
            name=f"paqto-writer-{self.peer.id}-{self._connection_id(connection)}",
        )
        channel = _OutboundChannel(queue=queue, task=task)
        self._outbound_channels[key] = channel
        task.add_done_callback(lambda finished: self._outbound_finished(key, finished))

    async def _queue_frame(self, connection: Connection, data: bytes) -> None:
        """Queue a frame and wait until its connection writer finishes it."""
        channel = self._outbound_channels.get(id(connection))
        if channel is None or channel.task.done() or connection.is_closed:
            raise ConnectionClosedError(
                "No active READY-session writer is available for the connection."
            )
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        item = _OutboundFrame(data=data, future=future)
        if self.config.outbound_backpressure is BackpressurePolicy.WAIT:
            await channel.queue.put(item)
        else:
            try:
                channel.queue.put_nowait(item)
            except asyncio.QueueFull as exc:
                self._report_resource_limit(
                    "Outbound frame queue is full.",
                    connection=connection,
                )
                raise ResourceLimitError(
                    "Outbound frame queue reached its configured limit."
                ) from exc
        try:
            await future
        finally:
            if not future.done():
                future.cancel()

    async def _outbound_worker(
        self,
        connection: Connection,
        queue: asyncio.Queue[_OutboundFrame],
    ) -> None:
        """Serialize frame writes for one READY connection and resolve waiters."""
        channel = self._outbound_channels[id(connection)]
        try:
            while self._running and not connection.is_closed:
                item = await queue.get()
                channel.current = item
                try:
                    await connection.send_frame(item.data)
                except asyncio.CancelledError:
                    self._set_future_error(
                        item.future,
                        ConnectionClosedError(
                            "Connection writer was cancelled before the frame was sent."
                        ),
                    )
                    raise
                except Exception as exc:  # noqa: BLE001 - supervises adapters
                    self._set_future_error(item.future, exc)
                    await connection.close()
                    return
                else:
                    if not item.future.done():
                        item.future.set_result(None)
                finally:
                    channel.current = None
                    queue.task_done()
        finally:
            error = ConnectionClosedError(
                "Connection writer stopped before the frame could be sent."
            )
            if channel.current is not None:
                self._set_future_error(channel.current.future, error)
                channel.current = None
            self._fail_outbound_queue(queue, error)

    async def _close_outbound_channel(self, connection: Connection) -> None:
        channel = self._outbound_channels.pop(id(connection), None)
        if channel is None:
            return
        task = channel.task
        if task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._fail_outbound_queue(
            channel.queue,
            ConnectionClosedError("Connection closed before the frame was sent."),
        )

    def _outbound_finished(
        self,
        key: int,
        task: asyncio.Task[None],
    ) -> None:
        channel = self._outbound_channels.get(key)
        if channel is not None and channel.task is task:
            self._outbound_channels.pop(key, None)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    "Paqto connection writer terminated unexpectedly",
                    extra=self._log_context(error=error),
                )

    def _fail_all_outbound(self, error: Exception) -> None:
        for channel in self._outbound_channels.values():
            if channel.current is not None:
                self._set_future_error(channel.current.future, error)
            self._fail_outbound_queue(channel.queue, error)

    def _fail_outbound_queue(
        self,
        queue: asyncio.Queue[_OutboundFrame],
        error: Exception,
    ) -> None:
        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._set_future_error(item.future, error)
            queue.task_done()

    async def _send_acknowledgement(
        self,
        connection: Connection,
        message_id: str,
    ) -> None:
        """Queue protocol receipt after deserialization and envelope validation."""
        await self._queue_frame(
            connection,
            encode_acknowledgement_frame(message_id),
        )

    async def _send_heartbeat_response(
        self,
        connection: Connection,
        ping_id: str,
    ) -> None:
        await self._queue_frame(connection, encode_pong_frame(ping_id))

    async def _send_message(
        self,
        connection: Connection,
        message: Message,
        *,
        timeout: float | None,
        require_ack: bool,
        acknowledgement_timeout: float | None,
    ) -> None:
        """Validate, serialize, queue, and optionally await a technical ACK.

        Writer completion is not proof of remote receipt. A technical ACK proves
        remote deserialization and envelope validation, but not dispatch,
        handler execution, durable storage, or application success.
        """
        if not isinstance(require_ack, bool):
            raise TypeError("require_ack must be a boolean.")
        self._validate_outgoing_message(message)
        session = self.session_for(connection)
        if session is None:
            raise ProtocolFrameError("Cannot send before the Paqto session is READY.")
        if require_ack and TECHNICAL_ACK_CAPABILITY not in session.capabilities:
            raise AcknowledgementUnavailableError(
                "The READY session did not negotiate technical acknowledgements."
            )
        try:
            data = self.serializer.serialize(message)
        except SerializationError:
            raise
        except Exception as exc:
            raise SerializationError("Could not serialize message.") from exc
        if not isinstance(data, bytes):
            raise SerializationError("Serializer.serialize() must return bytes.")
        frame = encode_application_frame(
            data,
            max_message_size=session.max_message_size,
        )

        pending: _PendingAcknowledgement | None = None
        if require_ack:
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            if message.id in self._pending_acknowledgements:
                raise AcknowledgementError(
                    f"An acknowledgement for message {message.id!r} is already pending."
                )
            if (
                len(self._pending_acknowledgements)
                >= self.config.max_pending_acknowledgements
            ):
                self._report_resource_limit(
                    "Pending acknowledgement limit reached.",
                    peer_id=session.peer_id,
                    connection=connection,
                    message=message,
                )
                raise ResourceLimitError(
                    "The node reached its maximum number of pending acknowledgements."
                )
            pending = _PendingAcknowledgement(
                future=future,
                connection=connection,
                peer_id=session.peer_id,
            )
            self._pending_acknowledgements[message.id] = pending

        try:
            try:
                await self._wait_for(self._queue_frame(connection, frame), timeout)
            except TimeoutError as exc:
                raise PaqtoTimeoutError("Timed out while sending message.") from exc
            if pending is not None:
                effective_ack_timeout = (
                    self.config.acknowledgement_timeout
                    if acknowledgement_timeout is None
                    else acknowledgement_timeout
                )
                try:
                    await self._wait_for(pending.future, effective_ack_timeout)
                except TimeoutError as exc:
                    raise AcknowledgementTimeoutError(
                        f"Timed out waiting for acknowledgement of message "
                        f"{message.id!r}."
                    ) from exc
        finally:
            if pending is not None:
                current = self._pending_acknowledgements.get(message.id)
                if current is pending:
                    self._pending_acknowledgements.pop(message.id, None)
                if not pending.future.done():
                    pending.future.cancel()

    def _resolve_reply(
        self,
        connection: Connection,
        session: ProtocolSession,
        message: Message,
    ) -> None:
        """Resolve a reply only for its original peer and physical connection."""
        assert message.reply_to is not None
        pending = self._pending_requests.get(message.reply_to)
        if pending is None:
            return
        if pending.connection is not connection or pending.peer_id != session.peer_id:
            return
        self._pending_requests.pop(message.reply_to, None)
        if not pending.future.done():
            pending.future.set_result(message)
        logger.debug(
            "Paqto request correlation resolved",
            extra=self._log_context(
                peer_id=session.peer_id,
                connection=connection,
                message=message,
                correlation_id=message.reply_to,
            ),
        )

    def _resolve_acknowledgement(
        self,
        connection: Connection,
        session: ProtocolSession,
        acknowledgement: TechnicalAcknowledgement,
    ) -> None:
        """Resolve an ACK only for its original peer and physical connection."""
        pending = self._pending_acknowledgements.get(acknowledgement.message_id)
        if pending is None:
            return
        if pending.connection is not connection or pending.peer_id != session.peer_id:
            return
        self._pending_acknowledgements.pop(acknowledgement.message_id, None)
        if not pending.future.done():
            pending.future.set_result(None)

    def _fail_pending_for_connection(self, connection: Connection) -> None:
        """Fail request and ACK correlations owned by a lost connection."""
        for request_id, pending_request in list(self._pending_requests.items()):
            if pending_request.connection is connection:
                self._pending_requests.pop(request_id, None)
                self._set_future_error(
                    pending_request.future,
                    RequestError("Connection closed before a reply was received."),
                )
        for message_id, pending_ack in list(self._pending_acknowledgements.items()):
            if pending_ack.connection is connection:
                self._pending_acknowledgements.pop(message_id, None)
                self._set_future_error(
                    pending_ack.future,
                    AcknowledgementError(
                        "Connection closed before an acknowledgement was received."
                    ),
                )

    def _fail_all_pending(self, reason: str) -> None:
        requests = list(self._pending_requests.values())
        acknowledgements = list(self._pending_acknowledgements.values())
        self._pending_requests.clear()
        self._pending_acknowledgements.clear()
        for pending_request in requests:
            self._set_future_error(pending_request.future, RequestError(reason))
        for pending_ack in acknowledgements:
            self._set_future_error(
                pending_ack.future,
                AcknowledgementError(reason),
            )

    @staticmethod
    def _set_future_error(
        future: asyncio.Future[Any],
        error: Exception,
    ) -> None:
        if future.done():
            return
        future.set_exception(error)
        future.add_done_callback(
            lambda completed: None if completed.cancelled() else completed.exception()
        )

    def _connection_for_reply(self, message: Message) -> Connection:
        """Recover exact handler context or require one unambiguous peer session."""
        context = self._message_context.get()
        if context is not None and context[0] is message:
            return context[1]
        candidates = [
            connection
            for key, connection in self._session_connections.items()
            if not connection.is_closed
            and self._sessions[key].peer_id == message.sender
        ]
        if not candidates:
            raise RequestError(
                f"No READY connection is available for peer {message.sender!r}."
            )
        if len(candidates) > 1:
            raise RequestError(
                f"Multiple READY connections exist for peer {message.sender!r}; "
                "reply from the message handler to preserve its connection context."
            )
        return candidates[0]

    def _register_session(
        self,
        connection: Connection,
        session: ProtocolSession,
        direction: _ConnectionDirection,
    ) -> None:
        """Publish one canonical READY session and start its writer channel."""
        key = id(connection)
        self._sessions[key] = session
        self._session_connections[key] = connection
        self._session_directions[key] = direction
        self._peer_connections[session.peer_id] = connection
        self._last_received_activity[key] = asyncio.get_running_loop().time()
        self._ensure_outbound_channel(connection)
        if connection.security_info.authenticated:
            self._emit_event(
                NodeEventType.AUTHENTICATED,
                peer_id=session.peer_id,
                connection=connection,
                metadata={
                    "authenticated_peer_id": (
                        connection.security_info.authenticated_peer_id
                    ),
                    "mechanism": connection.security_info.mechanism,
                    "peer_id_authenticated": session.peer_id_authenticated,
                },
            )
        self._emit_event(
            NodeEventType.CONNECTED,
            peer_id=session.peer_id,
            connection=connection,
            metadata={"peer_id_authenticated": session.peer_id_authenticated},
        )
        logger.info(
            "Paqto session ready",
            extra=self._log_context(
                peer_id=session.peer_id,
                connection=connection,
                authenticated=session.peer_id_authenticated,
            ),
        )

    def _deactivate_session(self, connection: Connection) -> None:
        """Remove volatile READY state without closing the physical connection."""
        key = id(connection)
        session = self._sessions.pop(key, None)
        self._session_connections.pop(key, None)
        self._session_directions.pop(key, None)
        self._last_received_activity.pop(key, None)
        if (
            session is not None
            and self._peer_connections.get(session.peer_id) is connection
        ):
            self._peer_connections.pop(session.peer_id, None)

    @staticmethod
    def _validate_outgoing_message(message: Message) -> None:
        PaqtoNode._validate_message_shape(message, direction="Outgoing")
        if not isinstance(message.id, str) or not message.id:
            raise ProtocolFrameError("Outgoing message id must be a non-empty string.")
        if message.reply_to is not None and (
            not isinstance(message.reply_to, str) or not message.reply_to
        ):
            raise ProtocolFrameError(
                "Outgoing message reply_to must be a non-empty string or None."
            )

    @staticmethod
    def _validate_incoming_message(message: Message) -> None:
        PaqtoNode._validate_message_shape(message, direction="Incoming")
        if not isinstance(message.id, str) or not message.id:
            raise ProtocolFrameError("Incoming message id must be a non-empty string.")
        if message.reply_to is not None and (
            not isinstance(message.reply_to, str) or not message.reply_to
        ):
            raise ProtocolFrameError(
                "Incoming message reply_to must be a non-empty string or None."
            )

    @staticmethod
    def _validate_message_shape(message: Message, *, direction: str) -> None:
        """Validate envelope fields needed before identity and routing checks."""
        if not isinstance(message.type, str) or not message.type:
            raise ProtocolFrameError(
                f"{direction} message type must be a non-empty string."
            )
        if not isinstance(message.headers, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in message.headers.items()
        ):
            raise ProtocolFrameError(
                f"{direction} message headers must map strings to strings."
            )
        for name in ("sender", "recipient"):
            value = getattr(message, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ProtocolFrameError(
                    f"{direction} message {name} must be a non-empty string or None."
                )

    def _remember(self, discovered: DiscoveredPeer) -> None:
        """Cache the newest non-local discovery observation by declared peer id."""
        if discovered.peer.id == self.peer.id:
            return
        existing = self._known_peers.get(discovered.peer.id)
        if existing is not None and existing.last_seen > discovered.last_seen:
            return
        self._known_peers[discovered.peer.id] = discovered

    def _resolve_target(self, target: Peer | DiscoveredPeer) -> DiscoveredPeer:
        """Resolve a target to discovery reachability without authenticating it."""
        if isinstance(target, DiscoveredPeer):
            return target

        known = self._known_peers.get(target.id)
        if known is not None:
            return known

        if self.connection_for_peer(target.id) is not None:
            return DiscoveredPeer(peer=target)

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
    def _connection_id(connection: Connection) -> str:
        return f"{id(connection):x}"

    def _log_context(
        self,
        *,
        peer_id: str | None = None,
        connection: Connection | None = None,
        connection_id: str | None = None,
        message: Message | None = None,
        error: BaseException | None = None,
        **values: Any,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {"paqto_local_peer_id": self.peer.id}
        context.update({f"paqto_{name}": value for name, value in values.items()})
        if peer_id is not None:
            context["paqto_peer_id"] = peer_id
        if connection is not None:
            context["paqto_connection_id"] = self._connection_id(connection)
        elif connection_id is not None:
            context["paqto_connection_id"] = connection_id
        if message is not None:
            context["paqto_message_id"] = message.id
            context["paqto_message_type"] = message.type
        if error is not None:
            context["paqto_error_type"] = type(error).__name__
        return context

    @staticmethod
    def _discard_queue(queue: asyncio.Queue[Any] | None) -> None:
        if queue is None:
            return
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            queue.task_done()

    async def _negotiate(
        self,
        connection: Connection,
        *,
        expected_peer_id: str | None = None,
    ) -> ProtocolSession:
        """Build the local offer and establish identity-consistent READY state."""
        offer = HandshakeOffer(
            peer_id=self.peer.id,
            serializer_id=self.config.serializer_id or self.serializer.protocol_id,
            version=self.config.protocol_version,
            capabilities=self._offered_capabilities(),
            max_message_size=self.config.max_message_size,
            metadata=self.config.protocol_metadata or {},
        )
        return await negotiate_protocol(
            connection,
            offer,
            timeout=self.config.handshake_timeout,
            expected_peer_id=expected_peer_id,
            require_authenticated_peer_id=(
                self.config.require_authenticated_peer_id_match
            ),
        )

    def _offered_capabilities(self) -> tuple[str, ...]:
        capabilities = list(self.config.capabilities)
        if (
            self.config.enable_acknowledgements
            and TECHNICAL_ACK_CAPABILITY not in capabilities
        ):
            capabilities.append(TECHNICAL_ACK_CAPABILITY)
        if HEARTBEAT_CAPABILITY not in capabilities:
            capabilities.append(HEARTBEAT_CAPABILITY)
        return tuple(capabilities)

    @staticmethod
    async def _wait_for(awaitable: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout)
