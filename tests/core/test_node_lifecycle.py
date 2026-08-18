import asyncio

import pytest

from paqto import NoDiscovery
from paqto.core.connection import Connection
from paqto.core.discovered import DiscoveredPeer
from paqto.core.discovery import DiscoveryService
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    AlreadyStartedError,
    DiscoveryError,
    NotStartedError,
    TransportError,
)
from paqto.core.events import NodeEvent, NodeEventType
from paqto.core.listener import Listener
from paqto.core.message import Message
from paqto.core.node import PaqtoNode
from paqto.core.peer import Peer
from paqto.core.serializer import Serializer
from paqto.core.transport import Transport


class FakeListener(Listener):
    def __init__(self, address: str = "test://local") -> None:
        self.started = False
        self.closed = False
        self._address = address
        self._never_accept = asyncio.Event()

    @property
    def local_endpoint(self) -> Endpoint:
        return Endpoint(transport="test", address=self._address)

    async def start(self) -> None:
        self.started = True

    async def accept(self) -> Connection:
        await self._never_accept.wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


class TransientAcceptFailureListener(FakeListener):
    def __init__(self) -> None:
        super().__init__()
        self.accept_calls = 0
        self.retried = asyncio.Event()

    async def accept(self) -> Connection:
        self.accept_calls += 1
        if self.accept_calls == 1:
            raise TransportError("transient accept failure")
        self.retried.set()
        return await super().accept()


class FakeTransport(Transport):
    def __init__(self) -> None:
        self.listener = FakeListener()
        self.start_calls = 0
        self.stop_calls = 0
        self.create_listener_calls = 0

    @property
    def name(self) -> str:
        return "test"

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1

    async def connect(
        self,
        endpoint: Endpoint,
        *,
        timeout: float | None = None,
    ) -> Connection:
        raise NotImplementedError

    async def create_listener(self, bind: Endpoint | None = None) -> Listener:
        self.create_listener_calls += 1
        return self.listener


class FakeSerializer(Serializer):
    def serialize(self, message: Message) -> bytes:
        return b""

    def deserialize(self, data: bytes) -> Message:
        return Message(payload=None)


class FailingDiscovery(DiscoveryService):
    def __init__(self) -> None:
        self.stop_calls = 0

    async def start(self, local_peer: Peer, endpoints: list[Endpoint]) -> None:
        raise DiscoveryError("startup failed")

    async def stop(self) -> None:
        self.stop_calls += 1

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        return []


class BlockingDiscovery(DiscoveryService):
    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0
        self.start_entered = asyncio.Event()
        self.release_start = asyncio.Event()

    async def start(self, local_peer: Peer, endpoints: list[Endpoint]) -> None:
        self.start_calls += 1
        self.start_entered.set()
        await self.release_start.wait()

    async def stop(self) -> None:
        self.stop_calls += 1

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        return []


class PassiveDiscovery(DiscoveryService):
    async def start(self, local_peer: Peer, endpoints: list[Endpoint]) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        return []


class RefreshingTransport(FakeTransport):
    async def create_listener(self, bind: Endpoint | None = None) -> Listener:
        self.create_listener_calls += 1
        self.listener = FakeListener(f"test://local-{self.create_listener_calls}")
        return self.listener


class RefreshingDiscovery(DiscoveryService):
    def __init__(self) -> None:
        self.started_endpoints: list[list[Endpoint]] = []
        self.stop_calls = 0
        self.remote_address = "test://remote-1"

    async def start(self, local_peer: Peer, endpoints: list[Endpoint]) -> None:
        self.started_endpoints.append(list(endpoints))

    async def stop(self) -> None:
        self.stop_calls += 1

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        return [
            DiscoveredPeer(
                peer=Peer(id="remote"),
                endpoints=[Endpoint("test", self.remote_address)],
            )
        ]


class BlockingStopDiscovery(PassiveDiscovery):
    def __init__(self) -> None:
        self.stop_entered = asyncio.Event()
        self.release_stop = asyncio.Event()
        self.stop_completed = False

    async def stop(self) -> None:
        self.stop_entered.set()
        await self.release_stop.wait()
        self.stop_completed = True


class BlockingDiscoverDiscovery(PassiveDiscovery):
    def __init__(self) -> None:
        self.discover_entered = asyncio.Event()
        self.release_discover = asyncio.Event()
        self.discover_finished = asyncio.Event()

    async def discover(
        self,
        *,
        timeout: float | None = None,
    ) -> list[DiscoveredPeer]:
        self.discover_entered.set()
        try:
            await self.release_discover.wait()
            return []
        finally:
            self.discover_finished.set()


def _node(transport: Transport, discovery: DiscoveryService) -> PaqtoNode:
    return PaqtoNode(
        name="test-node",
        transport=transport,
        discovery=discovery,
        serializer=FakeSerializer(),
    )


@pytest.mark.asyncio
async def test_node_defaults_to_restartable_no_discovery() -> None:
    transport = FakeTransport()
    node = PaqtoNode(
        name="explicit-endpoints-only",
        transport=transport,
        serializer=FakeSerializer(),
    )

    assert isinstance(node.discovery, NoDiscovery)
    with pytest.raises(NotStartedError):
        await node.discovery.discover()

    await node.start()
    assert await node.discover(timeout=0) == []
    await node.stop()

    await node.start()
    assert await node.discover(timeout=0) == []
    await node.stop()


@pytest.mark.asyncio
async def test_start_failure_rolls_back_started_resources() -> None:
    transport = FakeTransport()
    discovery = FailingDiscovery()
    node = _node(transport, discovery)

    with pytest.raises(DiscoveryError, match="startup failed"):
        await node.start()

    assert node.is_running is False
    assert node._listener is None
    assert transport.listener.closed is True
    assert transport.stop_calls == 1
    assert discovery.stop_calls == 1


@pytest.mark.asyncio
async def test_concurrent_starts_are_serialized() -> None:
    transport = FakeTransport()
    discovery = BlockingDiscovery()
    node = _node(transport, discovery)

    first = asyncio.create_task(node.start())
    await discovery.start_entered.wait()
    second = asyncio.create_task(node.start())
    await asyncio.sleep(0)
    discovery.release_start.set()

    await first
    with pytest.raises(AlreadyStartedError):
        await second

    assert transport.start_calls == 1
    assert transport.create_listener_calls == 1
    assert discovery.start_calls == 1
    await node.stop()


@pytest.mark.asyncio
async def test_accept_loop_reports_transient_failure_and_keeps_accepting() -> None:
    transport = FakeTransport()
    listener = TransientAcceptFailureListener()
    transport.listener = listener
    node = _node(transport, PassiveDiscovery())
    errors: list[NodeEvent] = []

    @node.on_event(NodeEventType.TRANSPORT_ERROR)
    def observe(event: NodeEvent) -> None:
        errors.append(event)

    try:
        await node.start()
        await asyncio.wait_for(listener.retried.wait(), timeout=1)

        assert listener.accept_calls >= 2
        assert len(errors) == 1
        assert isinstance(errors[0].error, TransportError)
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_cancelled_node_start_rolls_back_all_started_resources() -> None:
    transport = FakeTransport()
    discovery = BlockingDiscovery()
    node = _node(transport, discovery)

    starting = asyncio.create_task(node.start())
    await discovery.start_entered.wait()
    starting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await starting

    assert node.is_running is False
    assert node._listener is None
    assert transport.listener.closed is True
    assert transport.stop_calls == 1
    assert discovery.stop_calls == 1


@pytest.mark.asyncio
async def test_cancelled_stop_finishes_cleanup_before_propagating_cancellation() -> None:
    transport = FakeTransport()
    discovery = BlockingStopDiscovery()
    node = _node(transport, discovery)
    await node.start()

    stopping = asyncio.create_task(node.stop())
    await discovery.stop_entered.wait()
    stopping.cancel()
    await asyncio.sleep(0)

    assert stopping.done() is False
    discovery.release_stop.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping

    assert node.is_running is False
    assert discovery.stop_completed is True
    assert transport.listener.closed is True
    assert transport.stop_calls == 1
    assert node._accept_task is None
    assert node._handler_tasks == set()


@pytest.mark.asyncio
async def test_network_changed_restarts_and_replaces_endpoint_snapshots() -> None:
    transport = RefreshingTransport()
    discovery = RefreshingDiscovery()
    node = _node(transport, discovery)

    try:
        await node.start()
        old_remote = DiscoveredPeer(
            peer=Peer(id="old-remote"),
            endpoints=[Endpoint("test", "test://stale")],
        )
        node._remember(old_remote)
        discovery.remote_address = "test://remote-2"

        refreshed = await node.network_changed()

        assert node.is_running is True
        assert transport.start_calls == 2
        assert transport.stop_calls == 1
        assert discovery.stop_calls == 1
        assert [
            endpoints[0].address for endpoints in discovery.started_endpoints
        ] == ["test://local-1", "test://local-2"]
        assert [peer.endpoints[0].address for peer in refreshed] == [
            "test://remote-2"
        ]
        assert "old-remote" not in node._known_peers
        assert node._known_peers["remote"].endpoints[0].address == (
            "test://remote-2"
        )
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_network_changed_requires_a_running_node() -> None:
    node = _node(FakeTransport(), PassiveDiscovery())

    with pytest.raises(NotStartedError, match="started"):
        await node.network_changed()


@pytest.mark.asyncio
async def test_zero_discovery_timeout_allows_an_immediate_result() -> None:
    transport = FakeTransport()
    discovery = RefreshingDiscovery()
    node = _node(transport, discovery)
    node.config.discover_timeout = 0

    try:
        await node.start()
        peers = await node.discover()

        assert [peer.peer.id for peer in peers] == ["remote"]
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_discovery_cancellation_and_concurrent_stop_leave_no_resources() -> None:
    transport = FakeTransport()
    discovery = BlockingDiscoverDiscovery()
    node = _node(transport, discovery)
    node.config.discover_timeout = None
    await node.start()

    discovering = asyncio.create_task(node.discover())
    await discovery.discover_entered.wait()
    await node.stop()
    discovering.cancel()

    with pytest.raises(asyncio.CancelledError):
        await discovering

    assert discovery.discover_finished.is_set()
    assert node.is_running is False
    assert node._listener is None
    assert node._known_peers == {}
    assert transport.listener.closed is True


@pytest.mark.asyncio
async def test_cancelled_network_refresh_finishes_in_a_consistent_state() -> None:
    transport = RefreshingTransport()
    discovery = BlockingDiscoverDiscovery()
    node = _node(transport, discovery)
    node.config.discover_timeout = None
    await node.start()

    refreshing = asyncio.create_task(node.network_changed())
    await discovery.discover_entered.wait()
    refreshing.cancel()
    await asyncio.sleep(0)

    assert refreshing.done() is False
    discovery.release_discover.set()
    with pytest.raises(asyncio.CancelledError):
        await refreshing

    assert node.is_running is True
    assert transport.start_calls == 2
    assert transport.stop_calls == 1
    assert node._listener is not None
    await node.stop()
