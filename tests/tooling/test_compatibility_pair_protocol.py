from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from compatibility_tests.pair.protocol import (
    COMPLETION_CONFIRMATION,
    COMPLETION_CONFIRMATION_REPLY,
    COMPLETION_REPLY,
    COMPLETION_REQUEST,
    PairProtocolError,
    RemotePairFailure,
    ServerCompletionState,
    completion_payload,
    failure_message,
    metadata_message,
    raise_remote_failure,
    session_payload,
    validate_completion_payload,
    validate_metadata,
    validate_session_payload,
)
from compatibility_tests.pair.runner import PairConfig, _wait_client_event


def _metadata(session_id: str | None) -> dict[str, object]:
    return metadata_message(
        session_id=session_id,
        platform={
            "os_family": "Windows",
            "kernel_system": "Windows",
            "release": "11",
            "architecture": "AMD64",
            "sys_platform": "win32",
        },
        python={"implementation": "CPython", "version": "3.14.6"},
        paqto_version="0.0.1",
        paqto_import_path="C:/site-packages/paqto/__init__.py",
        compatibility_suite={
            "version": "2",
            "schema_version": 3,
            "pair_protocol_version": 2,
            "build_id": "sha256:test",
            "source_file_count": 12,
        },
        capabilities={"tcp": True, "mtls": True},
    )


def test_server_client_roles_have_matching_test_identities() -> None:
    server = PairConfig(
        "server", "direct", None, "0.0.0.0", "127.0.0.1", 7450, 10, 45454, "255.255.255.255"
    )
    client = PairConfig(
        "client", "direct", "127.0.0.1", "0.0.0.0", "127.0.0.1", 7450, 10, 45454, "255.255.255.255"
    )

    assert server.local_peer_id == client.remote_peer_id == "node-b"
    assert client.local_peer_id == server.remote_peer_id == "node-a"


def test_remote_metadata_and_session_id_are_retained() -> None:
    session_id = str(uuid4())

    validated = validate_metadata(_metadata(session_id), expected_session_id=session_id)

    assert validated["session_id"] == session_id
    assert validated["platform"]["os_family"] == "Windows"  # type: ignore[index]
    assert validated["python"]["version"] == "3.14.6"  # type: ignore[index]
    assert validated["paqto"]["import_path"].endswith("paqto/__init__.py")  # type: ignore[index,union-attr]
    assert validated["compatibility_suite"]["pair_protocol_version"] == 2  # type: ignore[index]


def test_session_id_mismatch_is_rejected() -> None:
    first = str(uuid4())
    second = str(uuid4())

    with pytest.raises(PairProtocolError, match="different session"):
        validate_session_payload(session_payload(first), second)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"kind": "metadata", "protocol_version": 999},
        {**_metadata(str(uuid4())), "serializer_id": "wrong"},
        {**_metadata(str(uuid4())), "platform": "not-an-object"},
    ],
)
def test_malformed_pair_metadata_is_rejected(payload: object) -> None:
    with pytest.raises(PairProtocolError):
        validate_metadata(payload)


def test_remote_failure_is_propagated() -> None:
    session_id = str(uuid4())
    payload = failure_message(session_id, RuntimeError("remote test failed"))

    with pytest.raises(RemotePairFailure, match="remote test failed"):
        raise_remote_failure(payload, session_id)


@pytest.mark.asyncio
async def test_pair_wait_has_a_finite_timeout() -> None:
    with pytest.raises(TimeoutError, match="timed out"):
        await _wait_client_event(asyncio.Event(), asyncio.Event(), [], 0.01)


def test_completion_payload_validates_session_and_phase() -> None:
    session_id = str(uuid4())
    payload = completion_payload(session_id, COMPLETION_REQUEST)

    assert validate_completion_payload(
        payload,
        session_id,
        expected_phase=COMPLETION_REQUEST,
    )["completion_phase"] == COMPLETION_REQUEST

    with pytest.raises(PairProtocolError, match="different session"):
        validate_completion_payload(
            payload,
            str(uuid4()),
            expected_phase=COMPLETION_REQUEST,
        )
    with pytest.raises(PairProtocolError, match="completion phase"):
        validate_completion_payload(
            payload,
            session_id,
            expected_phase=COMPLETION_REPLY,
        )


def test_server_completion_state_requires_order_and_rejects_duplicates() -> None:
    session_id = str(uuid4())
    state = ServerCompletionState(session_id)

    with pytest.raises(PairProtocolError, match="before the completion request"):
        state.accept_confirmation(
            completion_payload(session_id, COMPLETION_CONFIRMATION)
        )

    state.accept_request(completion_payload(session_id, COMPLETION_REQUEST))
    with pytest.raises(PairProtocolError, match="more than once"):
        state.accept_request(completion_payload(session_id, COMPLETION_REQUEST))

    with pytest.raises(PairProtocolError, match="different session"):
        state.accept_confirmation(
            completion_payload(str(uuid4()), COMPLETION_CONFIRMATION)
        )

    state.accept_confirmation(
        completion_payload(session_id, COMPLETION_CONFIRMATION)
    )
    with pytest.raises(PairProtocolError, match="more than once"):
        state.accept_confirmation(
            completion_payload(session_id, COMPLETION_CONFIRMATION)
        )


@pytest.mark.parametrize(
    "phase",
    [COMPLETION_REPLY, COMPLETION_CONFIRMATION_REPLY],
)
def test_completion_reply_phases_are_session_bound(phase: str) -> None:
    session_id = str(uuid4())
    payload = completion_payload(session_id, phase, server_status="PASS")

    assert validate_completion_payload(
        payload,
        session_id,
        expected_phase=phase,
    )["server_status"] == "PASS"

