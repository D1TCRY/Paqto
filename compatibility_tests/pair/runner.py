"""Real two-process/two-device Paqto compatibility scenarios."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from compatibility_tests.common.models import (
    CapabilityUnavailable,
    CheckResult,
    Status,
)
from compatibility_tests.common.package_info import (
    collect_package_info,
    installation_warning,
)
from compatibility_tests.common.platform_info import (
    collect_platform_info,
    collect_python_info,
)
from compatibility_tests.common.reporting import (
    SCHEMA_VERSION,
    generated_at,
    overall_status,
    tests_payload,
)
from compatibility_tests.common.serializer import (
    PROTOCOL_ID,
    CompatibilityJsonSerializer,
)
from compatibility_tests.pair.protocol import (
    RemotePairFailure,
    failure_message,
    metadata_message,
    raise_remote_failure,
    session_payload,
    validate_metadata,
    validate_session_payload,
)
from paqto import (
    DiscoveredPeer,
    DiscoveryError,
    Message,
    NoDiscovery,
    PaqtoConfig,
    PaqtoNode,
    PaqtoTimeoutError,
    Peer,
)
from paqto.lan import LanDiscovery, LanTransport, TlsConfig, endpoint_from_host_port

FIXTURES = Path(__file__).parents[1] / "fixtures" / "tls"
CA = FIXTURES / "ca.pem"
TEST_IDENTITY_PREFIX = "urn:test:peer:"
PAYLOAD_SIZE = 32 * 1024
CONCURRENT_REQUESTS = 8
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PairConfig:
    """Validated CLI inputs for one pair role."""

    role: str
    scenario: str
    target: str | None
    bind: str
    advertise: str
    port: int
    timeout: float
    discovery_port: int
    broadcast: str
    keep_alive: bool = False
    require_installed: bool = False

    @property
    def local_peer_id(self) -> str:
        return "node-b" if self.role == "server" else "node-a"

    @property
    def remote_peer_id(self) -> str:
        return "node-a" if self.role == "server" else "node-b"

    @property
    def operation_timeout(self) -> float:
        return min(15.0, self.timeout)


@dataclass(slots=True)
class PairEvidence:
    """Mutable evidence assembled during one role execution."""

    session_id: str | None = None
    remote: dict[str, object] | None = None


@dataclass(slots=True)
class Recorder:
    """Collect each pair assertion once with stable identifiers."""

    results: list[CheckResult] = field(default_factory=list)
    _ids: set[str] = field(default_factory=set)

    def add(
        self,
        id: str,
        description: str,
        status: Status,
        detail: str,
        *,
        required: bool = True,
        duration_ms: float = 0.0,
    ) -> None:
        if id in self._ids:
            return
        self._ids.add(id)
        self.results.append(
            CheckResult(
                id=id,
                category=id.split(".", 1)[0],
                description=description,
                status=status,
                required=required,
                duration_ms=duration_ms,
                detail=detail,
            )
        )

    def passed(self, id: str, description: str, detail: str = "passed") -> None:
        self.add(id, description, Status.PASS, detail)


def _identity_from_test_uri(certificate: Mapping[str, Any]) -> str | None:
    for kind, value in certificate.get("subjectAltName", ()):
        if kind == "URI" and value.startswith(TEST_IDENTITY_PREFIX):
            return value.removeprefix(TEST_IDENTITY_PREFIX)
    return None


def _tls_for_role(config: PairConfig) -> TlsConfig:
    stem = "node-b" if config.role == "server" else "node-a"
    return TlsConfig(
        certfile=FIXTURES / f"{stem}.pem",
        keyfile=FIXTURES / f"{stem}-key.pem",
        cadata=CA.read_text(encoding="ascii"),
        check_hostname=False,
        require_client_certificate=True,
        peer_identity_resolver=_identity_from_test_uri,
        handshake_timeout=config.operation_timeout,
    )


def _node(config: PairConfig) -> PaqtoNode:
    discovery = (
        LanDiscovery(
            discovery_port=config.discovery_port,
            bind_host=config.bind,
            broadcast_host=config.broadcast,
            announce_interval=0.4,
            default_discover_timeout=min(1.5, config.operation_timeout),
        )
        if config.scenario == "discovery"
        else NoDiscovery()
    )
    local_port = config.port if config.role == "server" else 0
    return PaqtoNode(
        name=config.local_peer_id,
        peer_id=config.local_peer_id,
        transport=LanTransport(
            host=config.bind,
            advertised_host=config.advertise,
            port=local_port,
            tls=_tls_for_role(config),
        ),
        discovery=discovery,
        serializer=CompatibilityJsonSerializer(),
        config=PaqtoConfig(
            connect_timeout=config.operation_timeout,
            send_timeout=config.operation_timeout,
            discover_timeout=min(1.5, config.operation_timeout),
            handshake_timeout=config.operation_timeout,
            request_timeout=config.operation_timeout,
            acknowledgement_timeout=config.operation_timeout,
            serializer_id=PROTOCOL_ID,
            require_authenticated_peer_id_match=True,
        ),
    )


def _local_environment() -> dict[str, object]:
    return {
        "platform": collect_platform_info(),
        "python": collect_python_info(),
        "paqto": collect_package_info(),
        "capabilities": {
            "tcp": True,
            "tls": True,
            "mtls": True,
            "strict_peer_identity": True,
        },
    }


def _metadata(local: dict[str, object], session_id: str | None) -> dict[str, object]:
    platform_info = local["platform"]
    python_info = local["python"]
    package = local["paqto"]
    capabilities = local["capabilities"]
    assert isinstance(platform_info, dict)
    assert isinstance(python_info, dict)
    assert isinstance(package, dict)
    assert isinstance(capabilities, dict)
    return metadata_message(
        session_id=session_id,
        platform=platform_info,
        python=python_info,
        paqto_version=str(package.get("version", "unknown")),
        capabilities=capabilities,
    )


def _known_direct(config: PairConfig) -> DiscoveredPeer:
    if config.target is None:
        raise ValueError("direct client requires a target")
    return DiscoveredPeer(
        peer=Peer(id=config.remote_peer_id, name=config.remote_peer_id),
        endpoints=[endpoint_from_host_port(config.target, config.port)],
    )


async def _discover_remote(
    node: PaqtoNode,
    config: PairConfig,
) -> DiscoveredPeer:
    deadline = asyncio.get_running_loop().time() + config.timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            peers = await node.discover(timeout=min(1.5, remaining))
        except (DiscoveryError, PaqtoTimeoutError):
            peers = []
        for discovered in peers:
            if discovered.peer.id == config.remote_peer_id:
                if discovered.endpoint_for(node.transport.name) is None:
                    raise CapabilityUnavailable(
                        "remote discovery announcement has no LAN endpoint"
                    )
                return discovered
        await asyncio.sleep(min(0.2, max(remaining, 0)))
    raise CapabilityUnavailable(
        f"cross-device broadcast did not discover {config.remote_peer_id!r} "
        f"within {config.timeout:g}s"
    )


async def _start_node(node: PaqtoNode, config: PairConfig) -> None:
    try:
        await node.start()
    except DiscoveryError as exc:
        if config.scenario == "discovery":
            raise CapabilityUnavailable(
                f"LAN broadcast discovery could not start: {exc}"
            ) from exc
        raise


def _inspect_ready(
    node: PaqtoNode,
    connection: Any,
    expected_peer_id: str,
    recorder: Recorder,
    *,
    reconnect: bool = False,
) -> int:
    session = node.session_for(connection)
    if session is None or session.peer_id != expected_peer_id:
        raise AssertionError("connection has no READY session for the expected peer")
    security = connection.security_info
    if not security.encrypted:
        raise AssertionError("pair connection is not encrypted")
    if not security.authenticated:
        raise AssertionError("pair connection is not mutually authenticated")
    if security.authenticated_peer_id != expected_peer_id:
        raise AssertionError("certificate identity does not match the remote peer id")
    if not session.peer_id_authenticated:
        raise AssertionError("READY session is not strictly bound to TLS identity")
    if reconnect:
        recorder.passed(
            "lifecycle.new_tls_session",
            "new TLS/mTLS connection after disconnect",
            "fresh connection completed TLS and mutual certificate authentication",
        )
        recorder.passed(
            "lifecycle.new_handshake",
            "new Paqto handshake after disconnect",
            "fresh connection negotiated a distinct READY session",
        )
    else:
        recorder.passed("network.tcp", "TCP connection")
        recorder.passed("security.tls", "TLS encryption and CA validation")
        recorder.passed("security.mtls", "mutual TLS authentication")
        recorder.passed("security.identity", "authenticated peer identity binding")
        recorder.passed("protocol.handshake", "Paqto protocol handshake")
        recorder.passed("protocol.ready", "Paqto READY session")
    return id(session)


def _assert_clean(node: PaqtoNode) -> None:
    counters = {
        "active connections": node.active_connection_count,
        "pending requests": node.pending_request_count,
        "pending acknowledgements": node.pending_acknowledgement_count,
        "reconnect tasks": node.reconnect_task_count,
        "heartbeat tasks": node.heartbeat_task_count,
        "inbound queue": node.inbound_queue_size,
        "outbound queue": node.outbound_queue_size,
    }
    leaked = {name: value for name, value in counters.items() if value}
    if leaked:
        raise AssertionError(f"Paqto resources remain after stop: {leaked}")


async def _run_server(
    config: PairConfig,
    local: dict[str, object],
    evidence: PairEvidence,
    recorder: Recorder,
) -> None:
    node = _node(config)
    evidence.session_id = str(uuid4())
    session_id = evidence.session_id
    metadata_confirmed = asyncio.Event()
    client_send_seen = asyncio.Event()
    complete = asyncio.Event()
    connection_ids: set[int] = set()
    session_objects: set[int] = set()
    echo_count = 0
    payload_verified = False

    def connection_for(message: Message) -> Any:
        connection = node.connection_for_peer(message.sender or "")
        if connection is None:
            raise AssertionError("server cannot inspect the inbound READY connection")
        connection_ids.add(id(connection))
        return connection

    @node.on_message("compat.metadata.c2s")
    async def receive_metadata(message: Message) -> None:
        remote = validate_metadata(message.payload, allow_missing_session=True)
        evidence.remote = remote
        connection = connection_for(message)
        session_objects.add(
            _inspect_ready(node, connection, config.remote_peer_id, recorder)
        )
        await node.reply(
            message,
            _metadata(local, session_id),
            type="compat.metadata.reply",
            require_ack=True,
        )
        recorder.passed(
            "messaging.client_server_request",
            "client to server request/reply",
            "metadata request/reply completed after READY",
        )

    @node.on_message("compat.metadata.confirm")
    def confirm_metadata(message: Message) -> None:
        validate_session_payload(message.payload, session_id)
        connection_for(message)
        metadata_confirmed.set()

    @node.on_message("compat.send.c2s")
    def receive_client_send(message: Message) -> None:
        payload = validate_session_payload(message.payload, session_id)
        if payload.get("value") != "client-to-server":
            raise AssertionError("client send payload mismatch")
        connection_for(message)
        client_send_seen.set()
        recorder.passed(
            "messaging.client_server_send", "client to server application send"
        )

    @node.on_message("compat.echo")
    async def echo(message: Message) -> None:
        nonlocal echo_count
        payload = validate_session_payload(message.payload, session_id)
        value = payload.get("value")
        if not isinstance(value, int):
            raise TypeError("echo value is not an integer")
        connection_for(message)
        echo_count += 1
        await node.reply(
            message,
            session_payload(session_id, value=value),
            type="compat.echo.reply",
        )

    @node.on_message("compat.payload")
    async def verify_payload(message: Message) -> None:
        nonlocal payload_verified
        payload = validate_session_payload(message.payload, session_id)
        value = payload.get("value")
        if not isinstance(value, str):
            raise TypeError("large payload is not a string")
        connection_for(message)
        payload_verified = True
        await node.reply(
            message,
            session_payload(
                session_id,
                length=len(value),
                sha256=hashlib.sha256(value.encode("utf-8")).hexdigest(),
            ),
            type="compat.payload.reply",
        )

    @node.on_message("compat.reconnect")
    async def verify_reconnect(message: Message) -> None:
        validate_session_payload(message.payload, session_id)
        connection = connection_for(message)
        session_objects.add(
            _inspect_ready(
                node,
                connection,
                config.remote_peer_id,
                recorder,
                reconnect=True,
            )
        )
        await node.reply(
            message,
            session_payload(session_id, ready=True),
            type="compat.reconnect.reply",
            require_ack=True,
        )
        recorder.passed(
            "lifecycle.messaging_after_reconnect",
            "request/reply after fresh reconnect",
        )

    @node.on_message("compat.complete")
    async def finish(message: Message) -> None:
        validate_session_payload(message.payload, session_id)
        connection_for(message)
        if echo_count != CONCURRENT_REQUESTS:
            raise AssertionError("server did not receive every concurrent request")
        if not payload_verified:
            raise AssertionError("server did not verify the reasonable-size payload")
        if len(connection_ids) < 2 or len(session_objects) < 2:
            raise AssertionError("server did not observe a fresh connection and session")
        await node.reply(
            message,
            session_payload(session_id, server_status="PASS"),
            type="compat.complete.reply",
        )
        complete.set()

    await _start_node(node, config)
    remote_target: DiscoveredPeer | Peer | None = None
    try:
        if config.scenario == "discovery":
            remote_target = await _discover_remote(node, config)
            recorder.passed(
                "discovery.cross_device",
                "cross-device LAN broadcast discovery",
                "remote peer id and announced endpoint were observed",
            )
        else:
            recorder.add(
                "discovery.cross_device",
                "cross-device LAN broadcast discovery",
                Status.SKIP,
                "direct scenario uses NoDiscovery and an explicit endpoint",
                required=False,
            )
        await asyncio.wait_for(metadata_confirmed.wait(), timeout=config.timeout)
        target: DiscoveredPeer | Peer = remote_target or Peer(id=config.remote_peer_id)
        await node.send(
            target,
            session_payload(session_id, value="server-to-client"),
            type="compat.send.s2c",
            require_ack=True,
        )
        recorder.passed(
            "messaging.server_client_send", "server to client application send"
        )
        response = await node.request(
            target,
            session_payload(session_id, value="server-request"),
            type="compat.request.s2c",
            require_ack=True,
        )
        validated = validate_session_payload(response.payload, session_id)
        if validated.get("value") != "server-reply":
            raise AssertionError("server to client request/reply payload mismatch")
        recorder.passed(
            "messaging.server_client_request", "server to client request/reply"
        )
        recorder.passed("messaging.ack", "technical acknowledgements in both directions")
        await asyncio.wait_for(client_send_seen.wait(), timeout=config.operation_timeout)
        await asyncio.wait_for(complete.wait(), timeout=config.timeout)
        recorder.passed(
            "messaging.multiple", "multiple ordered application messages"
        )
        recorder.passed(
            "messaging.concurrent", "concurrent request correlation across devices"
        )
        recorder.passed(
            "messaging.payload", "reasonable-size application payload"
        )
        recorder.passed("lifecycle.disconnect", "controlled software disconnect")
        recorder.passed("lifecycle.reconnect", "fresh TCP reconnect")
        if config.keep_alive:
            print("Pair complete; --keep-alive is active. Press Ctrl+C to stop.", flush=True)
            await asyncio.Event().wait()
    except Exception as exc:
        try:
            if node.connection_for_peer(config.remote_peer_id) is not None:
                await node.send(
                    Peer(id=config.remote_peer_id),
                    failure_message(session_id, exc),
                    type="compat.failure",
                )
        except Exception as propagation_error:
            logger.debug(
                "Could not propagate pair failure during teardown",
                exc_info=propagation_error,
            )
        raise
    finally:
        await node.stop()
    _assert_clean(node)
    recorder.passed("lifecycle.cleanup", "final task/socket/correlation cleanup")


async def _wait_client_event(
    expected: asyncio.Event,
    failure: asyncio.Event,
    failure_error: list[BaseException],
    timeout: float,
) -> None:
    expected_task = asyncio.create_task(expected.wait())
    failure_task = asyncio.create_task(failure.wait())
    done, pending = await asyncio.wait(
        {expected_task, failure_task},
        timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if not done:
        raise TimeoutError("timed out waiting for the remote pair role")
    if failure_task in done:
        raise failure_error[0]


async def _run_client(
    config: PairConfig,
    local: dict[str, object],
    evidence: PairEvidence,
    recorder: Recorder,
) -> None:
    node = _node(config)
    server_send_seen = asyncio.Event()
    server_request_seen = asyncio.Event()
    remote_failure_seen = asyncio.Event()
    remote_failure: list[BaseException] = []

    @node.on_message("compat.send.s2c")
    def receive_server_send(message: Message) -> None:
        if evidence.session_id is None:
            raise AssertionError("server sent application data before metadata confirmation")
        payload = validate_session_payload(message.payload, evidence.session_id)
        if payload.get("value") != "server-to-client":
            raise AssertionError("server send payload mismatch")
        server_send_seen.set()
        recorder.passed(
            "messaging.server_client_send", "server to client application send"
        )

    @node.on_message("compat.request.s2c")
    async def receive_server_request(message: Message) -> None:
        if evidence.session_id is None:
            raise AssertionError("server requested before metadata confirmation")
        payload = validate_session_payload(message.payload, evidence.session_id)
        if payload.get("value") != "server-request":
            raise AssertionError("server request payload mismatch")
        await node.reply(
            message,
            session_payload(evidence.session_id, value="server-reply"),
            type="compat.request.reply",
            require_ack=True,
        )
        server_request_seen.set()
        recorder.passed(
            "messaging.server_client_request", "server to client request/reply"
        )

    @node.on_message("compat.failure")
    def receive_remote_failure(message: Message) -> None:
        try:
            raise_remote_failure(message.payload, evidence.session_id)
        except RemotePairFailure as exc:
            remote_failure.append(exc)
            remote_failure_seen.set()

    await _start_node(node, config)
    try:
        if config.scenario == "discovery":
            target = await _discover_remote(node, config)
            recorder.passed(
                "discovery.cross_device",
                "cross-device LAN broadcast discovery",
                "remote peer id and announced endpoint were observed",
            )
        else:
            target = _known_direct(config)
            recorder.add(
                "discovery.cross_device",
                "cross-device LAN broadcast discovery",
                Status.SKIP,
                "direct scenario uses NoDiscovery and an explicit endpoint",
                required=False,
            )

        first_connection = await node.connect(target, timeout=config.operation_timeout)
        first_session = _inspect_ready(
            node, first_connection, config.remote_peer_id, recorder
        )
        response = await node.request(
            target,
            _metadata(local, None),
            type="compat.metadata.c2s",
            require_ack=True,
        )
        remote = validate_metadata(response.payload)
        session_id = str(remote["session_id"])
        evidence.session_id = session_id
        evidence.remote = remote
        recorder.passed(
            "messaging.client_server_request", "client to server request/reply"
        )
        await node.send(
            target,
            session_payload(session_id, confirmed=True),
            type="compat.metadata.confirm",
            require_ack=True,
        )

        await _wait_client_event(
            server_send_seen,
            remote_failure_seen,
            remote_failure,
            config.operation_timeout,
        )
        await _wait_client_event(
            server_request_seen,
            remote_failure_seen,
            remote_failure,
            config.operation_timeout,
        )
        await node.send(
            target,
            session_payload(session_id, value="client-to-server"),
            type="compat.send.c2s",
            require_ack=True,
        )
        recorder.passed(
            "messaging.client_server_send", "client to server application send"
        )
        recorder.passed("messaging.ack", "technical acknowledgements in both directions")

        responses = await asyncio.gather(
            *(
                node.request(
                    target,
                    session_payload(session_id, value=value),
                    type="compat.echo",
                    require_ack=True,
                )
                for value in range(CONCURRENT_REQUESTS)
            )
        )
        values = [
            validate_session_payload(item.payload, session_id).get("value")
            for item in responses
        ]
        if values != list(range(CONCURRENT_REQUESTS)):
            raise AssertionError("concurrent pair requests were mis-correlated")
        recorder.passed("messaging.multiple", "multiple ordered application messages")
        recorder.passed(
            "messaging.concurrent", "concurrent request correlation across devices"
        )

        large_value = "paqto-compatibility-" * (PAYLOAD_SIZE // 20)
        payload_response = await node.request(
            target,
            session_payload(session_id, value=large_value),
            type="compat.payload",
            require_ack=True,
        )
        payload_result = validate_session_payload(payload_response.payload, session_id)
        expected_digest = hashlib.sha256(large_value.encode("utf-8")).hexdigest()
        if payload_result.get("length") != len(large_value):
            raise AssertionError("reasonable-size payload length changed")
        if payload_result.get("sha256") != expected_digest:
            raise AssertionError("reasonable-size payload content changed")
        recorder.passed("messaging.payload", "reasonable-size application payload")

        await node.disconnect(target)
        recorder.passed("lifecycle.disconnect", "controlled software disconnect")
        await asyncio.sleep(0.2)
        second_connection = await node.connect(target, timeout=config.operation_timeout)
        if id(second_connection) == id(first_connection):
            raise AssertionError("disconnect reused the old connection object")
        second_session = _inspect_ready(
            node,
            second_connection,
            config.remote_peer_id,
            recorder,
            reconnect=True,
        )
        if second_session == first_session:
            raise AssertionError("reconnect reused the old Paqto session object")
        recorder.passed("lifecycle.reconnect", "fresh TCP reconnect")
        reconnect_response = await node.request(
            target,
            session_payload(session_id, ready=True),
            type="compat.reconnect",
            require_ack=True,
        )
        reconnect_payload = validate_session_payload(
            reconnect_response.payload, session_id
        )
        if reconnect_payload.get("ready") is not True:
            raise AssertionError("post-reconnect READY response mismatch")
        recorder.passed(
            "lifecycle.messaging_after_reconnect",
            "request/reply after fresh reconnect",
        )
        completion = await node.request(
            target,
            session_payload(session_id, complete=True),
            type="compat.complete",
        )
        completion_payload = validate_session_payload(completion.payload, session_id)
        if completion_payload.get("server_status") != "PASS":
            raise RemotePairFailure("server did not propagate a PASS result")
    finally:
        await node.stop()
    _assert_clean(node)
    recorder.passed("lifecycle.cleanup", "final task/socket/correlation cleanup")


def build_pair_report(
    config: PairConfig,
    local: dict[str, object],
    evidence: PairEvidence,
    results: list[CheckResult],
    duration_ms: float,
) -> dict[str, object]:
    """Build one role's pair report with shared session and remote metadata."""
    warning = installation_warning(local["paqto"])  # type: ignore[arg-type]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at(),
        "mode": "pair",
        "scenario": config.scenario,
        "role": config.role,
        "session_id": evidence.session_id,
        "status": overall_status(results),
        "local": local,
        "remote": evidence.remote,
        "tests": tests_payload(results),
        "capabilities": {
            result.id: {"status": result.status.value, "detail": result.detail}
            for result in results
        },
        "durations": {"total_ms": round(duration_ms, 3)},
        "warnings": [warning] if warning else [],
    }


async def execute_pair(config: PairConfig) -> tuple[dict[str, object], list[CheckResult]]:
    """Run a finite pair role and always return machine-readable evidence."""
    started = time.perf_counter()
    recorder = Recorder()
    evidence = PairEvidence()
    local = _local_environment()
    package = local["paqto"]
    assert isinstance(package, dict)
    if config.require_installed:
        distribution = package.get("distribution")
        metadata_available = isinstance(distribution, dict) and bool(
            distribution.get("available")
        )
        if package.get("repository_source") or not metadata_available:
            recorder.add(
                "package.require_installed",
                "Paqto installed-package provenance",
                Status.FAIL,
                "Paqto is a repository-source import or lacks distribution metadata",
            )
            duration_ms = (time.perf_counter() - started) * 1000
            return (
                build_pair_report(config, local, evidence, recorder.results, duration_ms),
                recorder.results,
            )
        recorder.passed(
            "package.require_installed", "Paqto installed-package provenance"
        )
    try:
        if config.role == "server":
            await _run_server(config, local, evidence, recorder)
        else:
            await _run_client(config, local, evidence, recorder)
    except CapabilityUnavailable as exc:
        recorder.add(
            "pair.execution",
            "requested pair scenario",
            Status.UNAVAILABLE,
            str(exc),
        )
    except Exception as exc:  # noqa: BLE001 - role/report isolation boundary
        recorder.add(
            "pair.execution",
            "requested pair scenario",
            Status.FAIL,
            f"{type(exc).__name__}: {exc}",
        )
    duration_ms = (time.perf_counter() - started) * 1000
    return (
        build_pair_report(config, local, evidence, recorder.results, duration_ms),
        recorder.results,
    )


def print_pair_header(config: PairConfig) -> None:
    """Print actionable coordination data before waiting for the peer."""
    if config.role == "server":
        print("Paqto pair compatibility server", flush=True)
        print(f"Scenario: {config.scenario}", flush=True)
        print(f"Bind address: {config.bind}", flush=True)
        print(f"Listening port: {config.port}", flush=True)
        print(f"Advertised address: {config.advertise}", flush=True)
        print(f"Test peer identity: {config.local_peer_id}", flush=True)
        print(f"Waiting for peer {config.remote_peer_id!r}...", flush=True)
