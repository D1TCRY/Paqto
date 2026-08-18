import asyncio
from typing import Any

import pytest

from paqto.core import (
    BackpressurePolicy,
    EventRouter,
    HandlerErrorPolicy,
    NodeEvent,
    NodeEventType,
    PaqtoConfig,
)
from paqto.lan import LanTransport


@pytest.mark.parametrize(
    "field",
    [
        "max_pending_requests",
        "max_pending_acknowledgements",
        "max_inbound_queue",
        "max_outbound_queue",
        "max_event_queue",
        "max_connections",
        "handler_concurrency",
    ],
)
def test_bounded_resource_limits_must_be_positive_integers(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        invalid_zero: dict[str, Any] = {field: 0}
        PaqtoConfig(**invalid_zero)
    with pytest.raises(TypeError, match=field):
        invalid_boolean: dict[str, Any] = {field: True}
        PaqtoConfig(**invalid_boolean)


def test_hardening_policies_require_explicit_enum_values() -> None:
    with pytest.raises(TypeError, match="inbound_backpressure"):
        PaqtoConfig(inbound_backpressure="wait")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="outbound_backpressure"):
        PaqtoConfig(outbound_backpressure="reject")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="handler_error_policy"):
        PaqtoConfig(handler_error_policy="continue")  # type: ignore[arg-type]

    config = PaqtoConfig(
        inbound_backpressure=BackpressurePolicy.REJECT,
        outbound_backpressure=BackpressurePolicy.WAIT,
        handler_error_policy=HandlerErrorPolicy.CLOSE_CONNECTION,
        idle_timeout=3,
    )
    assert config.idle_timeout == 3


def test_lan_frame_limit_is_validated_at_transport_configuration() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        LanTransport(max_frame_size=0)
    with pytest.raises(TypeError, match="integer"):
        LanTransport(max_frame_size=True)
    with pytest.raises(ValueError, match="4,294,967,295"):
        LanTransport(max_frame_size=2**32)

    assert LanTransport()._max_frame_size == PaqtoConfig().max_message_size + 1


def test_message_envelope_identity_fields_have_strict_shapes() -> None:
    from paqto.core import Message, PaqtoNode, ProtocolFrameError

    message = Message(payload=None, sender={"spoofed": True})  # type: ignore[arg-type]
    with pytest.raises(ProtocolFrameError, match="sender"):
        PaqtoNode._validate_incoming_message(message)


def test_discovery_peer_capacity_must_be_a_positive_integer() -> None:
    from paqto.lan import LanDiscovery

    with pytest.raises(ValueError, match="max_discovered_peers"):
        LanDiscovery(max_discovered_peers=0)
    with pytest.raises(TypeError, match="max_discovered_peers"):
        LanDiscovery(max_discovered_peers=True)


@pytest.mark.asyncio
async def test_event_router_isolates_a_failing_listener() -> None:
    router = EventRouter()
    received: list[NodeEventType] = []

    @router.on(NodeEventType.CONNECTED)
    def broken(event: NodeEvent) -> None:
        raise RuntimeError("listener failed")

    @router.on(NodeEventType.CONNECTED)
    async def healthy(event: NodeEvent) -> None:
        await asyncio.sleep(0)
        received.append(event.type)

    errors = await router.dispatch(
        NodeEvent(type=NodeEventType.CONNECTED, local_peer_id="local")
    )

    assert received == [NodeEventType.CONNECTED]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)


def test_node_event_metadata_is_an_immutable_snapshot() -> None:
    metadata = {"mechanism": "generic"}
    event = NodeEvent(
        type=NodeEventType.AUTHENTICATED,
        local_peer_id="local",
        peer_id="remote",
        metadata=metadata,
    )
    metadata["mechanism"] = "changed"

    assert event.metadata == {"mechanism": "generic"}
    with pytest.raises(TypeError):
        event.metadata["new"] = "value"  # type: ignore[index]
