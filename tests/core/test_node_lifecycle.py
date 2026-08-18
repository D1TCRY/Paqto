import asyncio

import pytest

from paqto.core.connection import Connection
from paqto.core.discovered import DiscoveredPeer
from paqto.core.discovery import DiscoveryService
from paqto.core.endpoint import Endpoint
from paqto.core.errors import AlreadyStartedError, DiscoveryError, TransportError
from paqto.core.events import NodeEvent, NodeEventType
from paqto.core.listener import Listener
from paqto.core.message import Message
from paqto.core.node import PaqtoNode
from paqto.core.peer import Peer
from paqto.core.serializer import Serializer
from paqto.core.transport import Transport


class FakeListener(Listener):
    def __init__(self) -> None:
        self.started = False
        self.closed = False
        self._never_accept = asyncio.Event()

    @property
    def local_endpoint(self) -> Endpoint:
        return Endpoint(transport="test", address="test://local")

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


def _node(transport: Transport, discovery: DiscoveryService) -> PaqtoNode:
    return PaqtoNode(
        name="test-node",
        transport=transport,
        discovery=discovery,
        serializer=FakeSerializer(),
    )


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
