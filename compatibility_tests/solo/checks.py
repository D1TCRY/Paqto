"""Offline solo checks for Paqto public APIs and local capabilities."""

from __future__ import annotations

import asyncio
import socket
import ssl
import sys
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import paqto
from compatibility_tests.common.models import CapabilityUnavailable, Check
from compatibility_tests.common.serializer import (
    PROTOCOL_ID,
    CompatibilityJsonSerializer,
)
from paqto import (
    BackpressurePolicy,
    ConnectionClosedError,
    DiscoveredPeer,
    Endpoint,
    Message,
    MessageRouter,
    NodeEvent,
    NodeEventType,
    PaqtoConfig,
    PaqtoNode,
    Peer,
    ProtocolFrameError,
    RequestTimeoutError,
    ResourceLimitError,
    TransportError,
)
from paqto.lan import LanDiscovery, LanTransport, TlsConfig, TlsContextConfig

CERTIFICATES = Path(__file__).parents[1] / "fixtures" / "tls"
CA = CERTIFICATES / "ca.pem"
NODE_A_CERT = CERTIFICATES / "node-a.pem"
NODE_A_KEY = CERTIFICATES / "node-a-key.pem"
NODE_B_CERT = CERTIFICATES / "node-b.pem"
NODE_B_KEY = CERTIFICATES / "node-b-key.pem"
TEST_IDENTITY_PREFIX = "urn:test:peer:"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _reserve_port(family: socket.AddressFamily, kind: socket.SocketKind) -> int:
    host = "::1" if family == socket.AF_INET6 else "127.0.0.1"
    with socket.socket(family, kind) as probe:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])


def _known_peer(peer_id: str, port: int) -> DiscoveredPeer:
    return DiscoveredPeer(
        peer=Peer(id=peer_id, name=peer_id),
        endpoints=[Endpoint(transport="lan", address=f"tcp://127.0.0.1:{port}")],
    )


def _config(**overrides: Any) -> PaqtoConfig:
    values: dict[str, Any] = {
        "connect_timeout": 2.0,
        "send_timeout": 2.0,
        "discover_timeout": 0.0,
        "handshake_timeout": 2.0,
        "request_timeout": 1.0,
        "acknowledgement_timeout": 1.0,
        "serializer_id": PROTOCOL_ID,
    }
    values.update(overrides)
    return PaqtoConfig(**values)


def _node(
    peer_id: str,
    port: int,
    *,
    config: PaqtoConfig | None = None,
    tls: TlsConfig | None = None,
    tls_contexts: TlsContextConfig | None = None,
) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(
            host="127.0.0.1",
            advertised_host="127.0.0.1",
            port=port,
            tls=tls,
            tls_contexts=tls_contexts,
        ),
        serializer=CompatibilityJsonSerializer(),
        config=config or _config(),
    )


@asynccontextmanager
async def _running_pair(
    *,
    source_config: PaqtoConfig | None = None,
    target_config: PaqtoConfig | None = None,
    source_tls: TlsConfig | None = None,
    target_tls: TlsConfig | None = None,
    source_contexts: TlsContextConfig | None = None,
    target_contexts: TlsContextConfig | None = None,
) -> AsyncIterator[tuple[PaqtoNode, PaqtoNode, DiscoveredPeer]]:
    source_port = _reserve_port(socket.AF_INET, socket.SOCK_STREAM)
    target_port = _reserve_port(socket.AF_INET, socket.SOCK_STREAM)
    source = _node(
        "node-a",
        source_port,
        config=source_config,
        tls=source_tls,
        tls_contexts=source_contexts,
    )
    target = _node(
        "node-b",
        target_port,
        config=target_config,
        tls=target_tls,
        tls_contexts=target_contexts,
    )
    try:
        await source.start()
        await target.start()
        yield source, target, _known_peer("node-b", target_port)
    finally:
        await asyncio.gather(source.stop(), target.stop(), return_exceptions=True)
        await asyncio.sleep(0)


def _identity_from_test_uri(certificate: Mapping[str, Any]) -> str | None:
    for kind, value in certificate.get("subjectAltName", ()):
        if kind == "URI" and value.startswith(TEST_IDENTITY_PREFIX):
            return value.removeprefix(TEST_IDENTITY_PREFIX)
    return None


def _tls(
    certificate: Path,
    key: Path,
    *,
    require_client_certificate: bool = False,
) -> TlsConfig:
    return TlsConfig(
        certfile=certificate,
        keyfile=key,
        cadata=CA.read_text(encoding="ascii"),
        require_client_certificate=require_client_certificate,
        peer_identity_resolver=_identity_from_test_uri,
    )


def _tls_contexts(
    certificate: Path,
    key: Path,
    *,
    require_client_certificate: bool = True,
) -> TlsContextConfig:
    ca_data = CA.read_text(encoding="ascii")
    client_context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cadata=ca_data,
    )
    client_context.minimum_version = ssl.TLSVersion.TLSv1_2
    client_context.load_cert_chain(certificate, key)

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.minimum_version = ssl.TLSVersion.TLSv1_2
    server_context.load_cert_chain(certificate, key)
    if require_client_certificate:
        server_context.verify_mode = ssl.CERT_REQUIRED
        server_context.load_verify_locations(cadata=ca_data)

    return TlsContextConfig(
        client_context=client_context,
        server_context=server_context,
        peer_identity_resolver=_identity_from_test_uri,
    )


async def check_import_and_python() -> str:
    _require(hasattr(paqto, "PaqtoNode"), "paqto.PaqtoNode is not importable")
    version = tuple(sys.version_info[:2])
    _require(version >= (3, 10), "Paqto requires Python 3.10 or newer")
    return f"paqto imported on supported Python {version[0]}.{version[1]}"


async def check_serializer_and_router() -> str:
    serializer = CompatibilityJsonSerializer()
    original = Message(
        payload={"value": [1, 2, 3]},
        type="route",
        sender="source",
        recipient="target",
        headers={"x-test": "true"},
        reply_to="request-id",
    )
    restored = serializer.deserialize(serializer.serialize(original))
    _require(restored.payload == original.payload, "serializer changed payload")
    _require(restored.id == original.id, "serializer changed message id")
    _require(restored.reply_to == original.reply_to, "serializer changed reply_to")

    calls: list[str] = []
    router = MessageRouter()

    @router.on("route")
    def exact(message: Message) -> None:
        calls.append(f"exact:{message.id}")

    @router.on(None)
    async def wildcard(message: Message) -> None:
        await asyncio.sleep(0)
        calls.append(f"wildcard:{message.id}")

    count = await router.dispatch(restored)
    _require(count == 2, "router did not invoke exact and wildcard handlers")
    _require(calls == [f"exact:{original.id}", f"wildcard:{original.id}"], "router order mismatch")
    return "serializer round-trip and sync/async routing passed"


async def check_ipv4_capability() -> str:
    try:
        tcp_port = _reserve_port(socket.AF_INET, socket.SOCK_STREAM)
    except OSError as exc:
        raise CapabilityUnavailable(f"IPv4 TCP loopback bind unavailable: {exc}") from exc
    return f"IPv4 TCP loopback bind passed (ephemeral port {bool(tcp_port)})"


async def check_udp_ipv4_capability() -> str:
    try:
        udp_port = _reserve_port(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError as exc:
        raise CapabilityUnavailable(f"IPv4 UDP loopback bind unavailable: {exc}") from exc
    return f"IPv4 UDP loopback bind passed (ephemeral port {bool(udp_port)})"


async def check_ipv6_capability() -> str:
    if not socket.has_ipv6:
        raise CapabilityUnavailable("Python reports IPv6 support unavailable")
    try:
        tcp_port = _reserve_port(socket.AF_INET6, socket.SOCK_STREAM)
        udp_port = _reserve_port(socket.AF_INET6, socket.SOCK_DGRAM)
    except OSError as exc:
        raise CapabilityUnavailable(f"IPv6 loopback bind unavailable: {exc}") from exc

    server_transport = LanTransport(host="::1", advertised_host="::1", port=0)
    client_transport = LanTransport(host="::1", port=0)
    await server_transport.start()
    await client_transport.start()
    try:
        listener = await server_transport.create_listener()
        await listener.start()
        endpoint = listener.local_endpoint
        _require(
            endpoint.address.startswith("tcp://[::1]:"),
            "IPv6 listener endpoint was not bracketed correctly",
        )
        client = await client_transport.connect(endpoint, timeout=2)
        server = await asyncio.wait_for(listener.accept(), timeout=2)
        await client.send_frame(b"ipv6")
        _require(
            await server.receive_frame() == b"ipv6",
            "IPv6 LAN transport frame failed",
        )
        await asyncio.gather(client.close(), server.close())
    finally:
        await asyncio.gather(
            client_transport.stop(),
            server_transport.stop(),
            return_exceptions=True,
        )
    return (
        "IPv6 Paqto TCP listener/connect/framing and UDP loopback bind passed "
        f"(preflight ports {bool(tcp_port and udp_port)})"
    )


async def check_tcp_framing_reconnect() -> str:
    server_transport = LanTransport(
        host="127.0.0.1",
        advertised_host="127.0.0.1",
        port=0,
    )
    client_transport = LanTransport(host="127.0.0.1", port=0)
    await server_transport.start()
    await client_transport.start()
    try:
        listener = await server_transport.create_listener()
        await listener.start()
        endpoint = listener.local_endpoint
        _require(endpoint.address.startswith("tcp://127.0.0.1:"), "explicit advertised host was not retained")
        _require(endpoint.metadata.get("bind_host") == "127.0.0.1", "explicit bind host missing from metadata")

        client = await client_transport.connect(endpoint, timeout=2)
        server = await asyncio.wait_for(listener.accept(), timeout=2)
        frames = [b"one", b"two", b"three"]
        for frame in frames:
            await client.send_frame(frame)
        received = [await server.receive_frame() for _ in frames]
        _require(received == frames, "multiple TCP frames were corrupted or reordered")
        await server.send_frame(b"reverse")
        _require(await client.receive_frame() == b"reverse", "TCP reverse frame failed")

        await client.close()
        try:
            await server.receive_frame()
        except ConnectionClosedError:
            pass
        else:
            raise AssertionError("disconnect did not terminate the peer stream")
        await server.close()

        client2 = await client_transport.connect(endpoint, timeout=2)
        server2 = await asyncio.wait_for(listener.accept(), timeout=2)
        await client2.send_frame(b"reconnected")
        _require(await server2.receive_frame() == b"reconnected", "TCP reconnect frame failed")
        await asyncio.gather(client2.close(), server2.close())
    finally:
        await asyncio.gather(client_transport.stop(), server_transport.stop(), return_exceptions=True)
    return "loopback listener, framing, disconnect, and fresh reconnect passed"


async def check_broadcast_discovery() -> str:
    try:
        port = _reserve_port(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError as exc:
        raise CapabilityUnavailable(f"UDP port allocation unavailable: {exc}") from exc
    first = LanDiscovery(
        discovery_port=port,
        bind_host="0.0.0.0",
        broadcast_host="255.255.255.255",
        announce_interval=0.2,
        default_discover_timeout=0.25,
    )
    second = LanDiscovery(
        discovery_port=port,
        bind_host="0.0.0.0",
        broadcast_host="255.255.255.255",
        announce_interval=0.2,
        default_discover_timeout=0.25,
    )
    endpoint_a = Endpoint(transport="lan", address="tcp://127.0.0.1:41001")
    endpoint_b = Endpoint(transport="lan", address="tcp://127.0.0.1:41002")
    try:
        try:
            await first.start(Peer(id="discovery-a", name="A"), [endpoint_a])
            await second.start(Peer(id="discovery-b", name="B"), [endpoint_b])
        except Exception as exc:
            if isinstance(exc.__cause__, OSError):
                raise CapabilityUnavailable(f"UDP broadcast startup unavailable: {exc}") from exc
            raise

        for _ in range(3):
            try:
                first_peers, second_peers = await asyncio.gather(
                    first.discover(timeout=0.25),
                    second.discover(timeout=0.25),
                )
            except Exception as exc:
                if isinstance(exc.__cause__, OSError):
                    raise CapabilityUnavailable(f"UDP broadcast send unavailable: {exc}") from exc
                raise
            if (
                any(peer.peer.id == "discovery-b" for peer in first_peers)
                and any(peer.peer.id == "discovery-a" for peer in second_peers)
            ):
                return "two local LanDiscovery instances exchanged broadcast announcements"
        raise CapabilityUnavailable(
            "UDP sockets opened, but this environment did not deliver local broadcast discovery"
        )
    finally:
        await asyncio.gather(first.stop(), second.stop(), return_exceptions=True)


async def check_tls_high_level() -> str:
    server_transport = LanTransport(
        host="127.0.0.1",
        advertised_host="127.0.0.1",
        tls=_tls(NODE_B_CERT, NODE_B_KEY),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(NODE_A_CERT, NODE_A_KEY),
    )
    await server_transport.start()
    await client_transport.start()
    try:
        listener = await server_transport.create_listener()
        await listener.start()
        client = await client_transport.connect(listener.local_endpoint, timeout=2)
        server = await asyncio.wait_for(listener.accept(), timeout=2)
        _require(client.security_info.encrypted, "TLS client is not encrypted")
        _require(client.security_info.authenticated, "custom CA did not authenticate server")
        _require(client.security_info.authenticated_peer_id == "node-b", "TLS identity resolver mismatch")
        _require(client.security_info.metadata.get("verified_server_name") == "127.0.0.1", "hostname verification was not reported")
        await client.send_frame(b"tls-local")
        _require(await server.receive_frame() == b"tls-local", "TLS framed data failed")
        await asyncio.gather(client.close(), server.close())
    finally:
        await asyncio.gather(client_transport.stop(), server_transport.stop(), return_exceptions=True)
    return "local TLS, in-memory custom CA, hostname verification, and identity resolution passed"


async def check_tls_context_mtls_identity() -> str:
    strict = _config(require_authenticated_peer_id_match=True)
    contexts_a = _tls_contexts(NODE_A_CERT, NODE_A_KEY)
    contexts_b = _tls_contexts(NODE_B_CERT, NODE_B_KEY)
    async with _running_pair(
        source_config=strict,
        target_config=_config(require_authenticated_peer_id_match=True),
        source_contexts=contexts_a,
        target_contexts=contexts_b,
    ) as (source, target, known):

        @target.on_message("secure-request")
        async def reply(message: Message) -> None:
            await target.reply(message, {"secure": True})

        connection = await source.connect(known)
        session = source.session_for(connection)
        _require(connection.security_info.encrypted, "injected SSLContext did not enable TLS")
        _require(connection.security_info.authenticated_peer_id == "node-b", "outgoing mTLS identity mismatch")
        _require(session is not None and session.peer_id_authenticated, "Paqto identity binding did not authenticate READY session")
        response = await source.request(known, {"ping": True}, type="secure-request")
        _require(response.payload == {"secure": True}, "mTLS request/reply failed")
    return "caller SSLContext injection, mTLS, Paqto handshake, and strict identity binding passed"


async def check_messaging() -> str:
    async with _running_pair() as (source, target, known):
        sent = asyncio.Event()

        @target.on_message("send")
        def receive_send(message: Message) -> None:
            _require(message.payload == {"value": 1}, "send payload mismatch")
            sent.set()

        @target.on_message("request")
        async def reply_request(message: Message) -> None:
            await target.reply(message, int(message.payload) * 2, type="reply")

        @target.on_message("never-reply")
        def never_reply(message: Message) -> None:
            del message

        cancellation_seen = asyncio.Event()

        @target.on_message("cancel")
        def see_cancel(message: Message) -> None:
            del message
            cancellation_seen.set()

        await source.send(known, {"value": 1}, type="send", require_ack=True)
        await asyncio.wait_for(sent.wait(), timeout=1)

        response = await source.request(known, 21, type="request", require_ack=True)
        _require(response.payload == 42, "request/reply correlation failed")
        responses = await asyncio.gather(
            *(source.request(known, value, type="request") for value in range(12))
        )
        _require([response.payload for response in responses] == [value * 2 for value in range(12)], "concurrent requests were mis-correlated")

        try:
            await source.request(known, None, type="never-reply", timeout=0.03)
        except RequestTimeoutError:
            pass
        else:
            raise AssertionError("request timeout did not raise RequestTimeoutError")
        _require(source.pending_request_count == 0, "timed-out request leaked correlation")

        pending = asyncio.create_task(source.request(known, None, type="cancel", timeout=2))
        await asyncio.wait_for(cancellation_seen.wait(), timeout=1)
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("request cancellation was not propagated")
        _require(source.pending_request_count == 0, "cancelled request leaked correlation")
        _require(source.pending_acknowledgement_count == 0, "ACK correlation leaked")
    return "send, ACK, request/reply, concurrency, timeout, and cancellation passed"


async def check_lifecycle_network_change_cleanup() -> str:
    port = _reserve_port(socket.AF_INET, socket.SOCK_STREAM)
    node = _node("lifecycle-node", port)
    await node.start()
    _require(node.is_running, "node did not start")
    observations = await node.network_changed()
    _require(observations == [], "NoDiscovery unexpectedly returned peers")
    _require(node.is_running, "network_changed did not leave node running")
    await node.stop()
    _require(not node.is_running, "node did not stop")
    await node.start()
    _require(node.is_running, "node did not restart")
    await node.stop()
    await node.stop()
    _require(node.active_connection_count == 0, "active connections leaked after stop")
    _require(node.pending_request_count == 0, "pending requests leaked after stop")
    _require(node.pending_acknowledgement_count == 0, "pending ACKs leaked after stop")
    _require(node.reconnect_task_count == 0, "reconnect task leaked after stop")
    _require(node.heartbeat_task_count == 0, "heartbeat task leaked after stop")
    _require(node.inbound_queue_size == 0 and node.outbound_queue_size == 0, "node queues were not cleared")
    return "start/stop/start/stop, network refresh, idempotent stop, and public cleanup counters passed"


async def check_frame_and_message_limits() -> str:
    server_transport = LanTransport(host="127.0.0.1", max_frame_size=8)
    client_transport = LanTransport(host="127.0.0.1", max_frame_size=8)
    await server_transport.start()
    await client_transport.start()
    try:
        listener = await server_transport.create_listener()
        await listener.start()
        client = await client_transport.connect(listener.local_endpoint, timeout=2)
        server = await asyncio.wait_for(listener.accept(), timeout=2)
        try:
            await client.send_frame(b"123456789")
        except TransportError:
            pass
        else:
            raise AssertionError("max_frame_size did not reject an oversized frame")
        await asyncio.gather(client.close(), server.close())
    finally:
        await asyncio.gather(client_transport.stop(), server_transport.stop(), return_exceptions=True)

    async with _running_pair(source_config=_config(max_message_size=256)) as (source, target, known):
        del target
        try:
            await source.send(known, "x" * 4096)
        except ProtocolFrameError:
            pass
        else:
            raise AssertionError("max_message_size did not reject an oversized application envelope")
    return "transport max frame and negotiated application message limits passed"


async def check_pending_request_limit() -> str:
    source_config = _config(max_pending_requests=1)
    async with _running_pair(source_config=source_config) as (source, target, known):
        first_seen = asyncio.Event()

        @target.on_message("hold")
        def hold(message: Message) -> None:
            del message
            first_seen.set()

        first = asyncio.create_task(source.request(known, 1, type="hold", timeout=2))
        await asyncio.wait_for(first_seen.wait(), timeout=1)
        _require(source.pending_request_count == 1, "first pending request was not registered")
        try:
            await source.request(known, 2, type="hold", timeout=0.1)
        except ResourceLimitError:
            pass
        else:
            raise AssertionError("max_pending_requests did not reject overflow")
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        _require(source.pending_request_count == 0, "pending request limit check leaked state")
    return "max_pending_requests admission and cancellation cleanup passed"


async def check_bounded_queue_limit() -> str:
    target_config = _config(
        max_inbound_queue=1,
        max_outbound_queue=1,
        handler_concurrency=1,
        inbound_backpressure=BackpressurePolicy.REJECT,
    )
    async with _running_pair(target_config=target_config) as (source, target, known):
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()
        limit_seen = asyncio.Event()

        @target.on_message("queue")
        async def block_first(message: Message) -> None:
            del message
            handler_started.set()
            await release_handler.wait()

        @target.on_event(NodeEventType.RESOURCE_LIMIT)
        def observe_limit(event: NodeEvent) -> None:
            del event
            limit_seen.set()

        await source.send(known, 1, type="queue")
        await asyncio.wait_for(handler_started.wait(), timeout=1)
        await source.send(known, 2, type="queue")
        for _ in range(100):
            if target.inbound_queue_size == 1:
                break
            await asyncio.sleep(0.005)
        _require(target.inbound_queue_size == 1, "bounded inbound queue did not fill")
        await source.send(known, 3, type="queue")
        await asyncio.wait_for(limit_seen.wait(), timeout=1)
        release_handler.set()
    return "bounded queue capacity and public resource-limit event passed"


CHECKS: tuple[Check, ...] = (
    Check("core.import_python", "core", "package import and supported Python", check_import_and_python),
    Check("core.serializer_router", "core", "serializer and router public APIs", check_serializer_and_router),
    Check("capability.ipv4", "capability", "IPv4 TCP loopback capability", check_ipv4_capability),
    Check("capability.udp_ipv4", "capability", "IPv4 UDP loopback capability", check_udp_ipv4_capability),
    Check(
        "capability.ipv6",
        "capability",
        "IPv6 Paqto TCP path and UDP loopback capability",
        check_ipv6_capability,
        required_profiles=frozenset(),
    ),
    Check("tcp.framing_reconnect", "tcp", "listener, connection, frames, disconnect, reconnect", check_tcp_framing_reconnect),
    Check(
        "discovery.broadcast",
        "discovery",
        "real local IPv4 UDP broadcast discovery",
        check_broadcast_discovery,
        run_profiles=frozenset({"full"}),
        required_profiles=frozenset({"full"}),
    ),
    Check("tls.high_level", "tls", "TLS with high-level custom trust", check_tls_high_level),
    Check("tls.context_mtls_identity", "tls", "injected contexts, mTLS, identity binding", check_tls_context_mtls_identity),
    Check("messaging.operations", "messaging", "send/request/reply/ACK/concurrency/timeouts/cancellation", check_messaging),
    Check("lifecycle.restart_refresh_cleanup", "lifecycle", "restart, network change, cleanup", check_lifecycle_network_change_cleanup),
    Check("limits.frames", "limits", "transport and protocol frame limits", check_frame_and_message_limits),
    Check("limits.pending_requests", "limits", "pending request admission", check_pending_request_limit),
    Check("limits.queues", "limits", "bounded queue admission", check_bounded_queue_limit),
)
