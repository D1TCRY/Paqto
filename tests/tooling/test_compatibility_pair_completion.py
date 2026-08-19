from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import pytest

from compatibility_tests.common.models import Status
from compatibility_tests.pair import runner
from compatibility_tests.pair.protocol import (
    COMPLETION_CONFIRMATION_REPLY,
    COMPLETION_REPLY,
    completion_payload,
)
from compatibility_tests.pair.runner import (
    PairConfig,
    Recorder,
    _run_client_completion,
    _wait_server_stage,
)
from paqto import (
    DiscoveredPeer,
    Message,
    PaqtoNode,
    Peer,
    RequestError,
    RequestTimeoutError,
)


class ScriptedCompletionNode:
    def __init__(
        self,
        steps: list[Message | BaseException | Callable[[], Awaitable[Message]]],
    ) -> None:
        self.steps = steps
        self.request_types: list[str] = []
        self.disconnected = asyncio.Event()

    async def request(self, target: object, payload: object, **kwargs: object) -> Message:
        del target, payload
        self.request_types.append(str(kwargs["type"]))
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if callable(step):
            return await step()
        return step

    async def disconnect(self, target: object) -> None:
        del target
        self.disconnected.set()


def _reply(session_id: str, phase: str) -> Message:
    return Message(
        payload=completion_payload(session_id, phase, server_status="PASS"),
        type=f"compat.{phase}",
        sender="node-b",
        recipient="node-a",
    )


def _pair_configs(port: int) -> tuple[PairConfig, PairConfig]:
    server = PairConfig(
        "server",
        "direct",
        None,
        "127.0.0.1",
        "127.0.0.1",
        port,
        5,
        45454,
        "255.255.255.255",
    )
    client = PairConfig(
        "client",
        "direct",
        "127.0.0.1",
        "127.0.0.1",
        "127.0.0.1",
        port,
        5,
        45454,
        "255.255.255.255",
    )
    return server, client


async def _wait_for_pair_gate(
    event: asyncio.Event,
    server_task: asyncio.Task[tuple[dict[str, object], list[Any]]],
    client_task: asyncio.Task[tuple[dict[str, object], list[Any]]],
) -> None:
    gate = asyncio.create_task(event.wait())
    done, pending = await asyncio.wait(
        {gate, server_task, client_task},
        timeout=5,
        return_when=asyncio.FIRST_COMPLETED,
    )
    if gate in done:
        return
    gate.cancel()
    await asyncio.gather(gate, return_exceptions=True)
    completed_reports = [
        task.result()[0]
        for task in (server_task, client_task)
        if task in done
    ]
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if completed_reports:
        raise AssertionError(f"pair role ended before test barrier: {completed_reports}")
    raise TimeoutError("pair test did not reach its deterministic barrier")


@pytest.mark.asyncio
async def test_client_completion_requires_both_replies_before_disconnect() -> None:
    session_id = str(uuid4())
    confirmation_entered = asyncio.Event()
    release_confirmation = asyncio.Event()

    async def delayed_confirmation() -> Message:
        confirmation_entered.set()
        await release_confirmation.wait()
        return _reply(session_id, COMPLETION_CONFIRMATION_REPLY)

    node = ScriptedCompletionNode(
        [
            _reply(session_id, COMPLETION_REPLY),
            delayed_confirmation,
        ]
    )
    recorder = Recorder()
    task = asyncio.create_task(
        _run_client_completion(
            node,
            Peer(id="node-b"),
            session_id,
            recorder,
            1,
        )
    )

    await asyncio.wait_for(confirmation_entered.wait(), timeout=1)
    assert not node.disconnected.is_set()
    assert not task.done()

    release_confirmation.set()
    await asyncio.wait_for(task, timeout=1)

    assert node.disconnected.is_set()
    assert node.request_types == ["compat.complete", "compat.complete.confirmed"]
    assert [result.status for result in recorder.results] == [
        Status.PASS,
        Status.PASS,
        Status.PASS,
    ]


@pytest.mark.asyncio
async def test_client_completion_reports_wrong_session_and_timeout_precisely() -> None:
    session_id = str(uuid4())
    wrong_session = str(uuid4())
    wrong_node = ScriptedCompletionNode([_reply(wrong_session, COMPLETION_REPLY)])
    wrong_recorder = Recorder()

    with pytest.raises(runner.CompletionWorkflowError, match="different session"):
        await _run_client_completion(
            wrong_node,
            Peer(id="node-b"),
            session_id,
            wrong_recorder,
            1,
        )
    assert wrong_recorder.results[-1].id == "protocol.completion_request"
    assert wrong_recorder.results[-1].status is Status.FAIL

    timeout_node = ScriptedCompletionNode(
        [RequestTimeoutError("scripted completion timeout")]
    )
    timeout_recorder = Recorder()
    with pytest.raises(runner.CompletionWorkflowError, match="timed out"):
        await _run_client_completion(
            timeout_node,
            Peer(id="node-b"),
            session_id,
            timeout_recorder,
            1,
        )
    assert timeout_recorder.results[-1].id == "protocol.completion_request"
    assert "timed out" in timeout_recorder.results[-1].detail

    loss_node = ScriptedCompletionNode(
        [RequestError("connection closed before scripted completion reply")]
    )
    loss_recorder = Recorder()
    with pytest.raises(runner.CompletionWorkflowError, match="connection was lost"):
        await _run_client_completion(
            loss_node,
            Peer(id="node-b"),
            session_id,
            loss_recorder,
            1,
        )
    assert loss_recorder.results[-1].id == "protocol.completion_request"
    assert "connection was lost" in loss_recorder.results[-1].detail


@pytest.mark.asyncio
async def test_server_completion_distinguishes_missing_confirmation_and_disconnect() -> None:
    timeout_recorder = Recorder()
    with pytest.raises(runner.CompletionWorkflowError, match="timed out"):
        await _wait_server_stage(
            expected=asyncio.Event(),
            failure=asyncio.Event(),
            failure_errors=[],
            remote_disconnected=asyncio.Event(),
            timeout=0.01,
            recorder=timeout_recorder,
            check_id="protocol.completion_confirmation",
            description="pair completion confirmation request/reply",
            stage="completion confirmation",
        )
    assert timeout_recorder.results[-1].id == "protocol.completion_confirmation"
    assert "timed out" in timeout_recorder.results[-1].detail

    disconnect = asyncio.Event()
    disconnect.set()
    disconnect_recorder = Recorder()
    with pytest.raises(runner.CompletionWorkflowError, match="disconnected"):
        await _wait_server_stage(
            expected=asyncio.Event(),
            failure=asyncio.Event(),
            failure_errors=[],
            remote_disconnected=disconnect,
            timeout=1,
            recorder=disconnect_recorder,
            check_id="protocol.completion_confirmation",
            description="pair completion confirmation request/reply",
            stage="completion confirmation",
        )
    assert "peer disconnected" in disconnect_recorder.results[-1].detail


@pytest.mark.asyncio
async def test_real_pair_server_waits_for_confirmation_and_remote_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server_config, client_config = _pair_configs(0)
    server_node = runner._node(server_config)
    client_node = runner._node(client_config)
    server_started = asyncio.Event()
    first_reply_received = asyncio.Event()
    confirmation_attempted = asyncio.Event()
    release_confirmation = asyncio.Event()
    original_start = runner._start_node
    original_client_request = client_node.request

    async def synchronized_start(node: PaqtoNode, config: PairConfig) -> None:
        if config.role == "client":
            await server_started.wait()
        await original_start(node, config)
        if config.role == "server":
            server_started.set()

    def selected_node(config: PairConfig) -> PaqtoNode:
        return server_node if config.role == "server" else client_node

    def dynamic_direct_target(config: PairConfig) -> DiscoveredPeer:
        del config
        listener = server_node._listener
        assert listener is not None
        return DiscoveredPeer(
            peer=Peer(id="node-b", name="node-b"),
            endpoints=[listener.local_endpoint],
        )

    async def gated_client_request(*args: Any, **kwargs: Any) -> Message:
        message_type = kwargs.get("type")
        if message_type == "compat.complete.confirmed":
            confirmation_attempted.set()
            await release_confirmation.wait()
        response = await original_client_request(*args, **kwargs)
        if message_type == "compat.complete":
            first_reply_received.set()
        return response

    monkeypatch.setattr(runner, "_start_node", synchronized_start)
    monkeypatch.setattr(runner, "_node", selected_node)
    monkeypatch.setattr(runner, "_known_direct", dynamic_direct_target)
    monkeypatch.setattr(client_node, "request", gated_client_request)

    server_task = asyncio.create_task(runner.execute_pair(server_config))
    client_task = asyncio.create_task(runner.execute_pair(client_config))
    await _wait_for_pair_gate(first_reply_received, server_task, client_task)
    await _wait_for_pair_gate(confirmation_attempted, server_task, client_task)

    assert not server_task.done()
    assert server_node.connection_for_peer("node-a") is not None

    release_confirmation.set()
    client_report, _ = await asyncio.wait_for(client_task, timeout=5)
    server_report, _ = await asyncio.wait_for(server_task, timeout=5)

    assert client_report["status"] == "PASS"
    assert server_report["status"] == "PASS"
    assert client_report["session_id"] == server_report["session_id"]
    server_checks = {item["id"]: item for item in server_report["tests"]["results"]}  # type: ignore[index]
    client_checks = {item["id"]: item for item in client_report["tests"]["results"]}  # type: ignore[index]
    assert server_checks["protocol.completion_confirmation"]["status"] == "PASS"
    assert server_checks["lifecycle.remote_graceful_disconnect"]["status"] == "PASS"
    assert client_checks["lifecycle.graceful_disconnect"]["status"] == "PASS"
    assert server_checks["lifecycle.cleanup"]["status"] == "PASS"
    assert client_checks["lifecycle.cleanup"]["status"] == "PASS"
