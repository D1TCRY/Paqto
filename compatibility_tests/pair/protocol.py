"""Application-level coordination protocol for two-device compatibility runs."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from compatibility_tests.common.serializer import PROTOCOL_ID

PAIR_PROTOCOL_VERSION = 2

COMPLETION_REQUEST = "complete"
COMPLETION_REPLY = "complete.reply"
COMPLETION_CONFIRMATION = "complete.confirmed"
COMPLETION_CONFIRMATION_REPLY = "complete.confirmed.reply"


class PairProtocolError(ValueError):
    """A compatibility coordination message is malformed or inconsistent."""


class RemotePairFailure(RuntimeError):
    """The other role explicitly reported a suite failure."""


@dataclass(slots=True)
class ServerCompletionState:
    """Validate the ordered, session-bound server side completion workflow."""

    session_id: str
    request_received: bool = False
    confirmation_received: bool = False

    def accept_request(self, payload: object) -> dict[str, object]:
        """Accept exactly one completion request for this pair session."""
        validated = validate_completion_payload(
            payload,
            self.session_id,
            expected_phase=COMPLETION_REQUEST,
        )
        if self.request_received:
            raise PairProtocolError("completion request was received more than once")
        if self.confirmation_received:
            raise PairProtocolError("completion request arrived after confirmation")
        self.request_received = True
        return validated

    def accept_confirmation(self, payload: object) -> dict[str, object]:
        """Accept one confirmation only after the initial request completed."""
        validated = validate_completion_payload(
            payload,
            self.session_id,
            expected_phase=COMPLETION_CONFIRMATION,
        )
        if not self.request_received:
            raise PairProtocolError(
                "completion confirmation arrived before the completion request"
            )
        if self.confirmation_received:
            raise PairProtocolError("completion confirmation was received more than once")
        self.confirmation_received = True
        return validated


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PairProtocolError(f"{field} must be an object")
    return value


def _non_empty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PairProtocolError(f"{field} must be a non-empty string")
    return value


def validate_session_id(value: object, *, allow_none: bool = False) -> str | None:
    """Validate canonical UUID session identifiers."""
    if value is None and allow_none:
        return None
    text = _non_empty_string(value, "session_id")
    try:
        parsed = UUID(text)
    except ValueError as exc:
        raise PairProtocolError("session_id must be a UUID") from exc
    if str(parsed) != text.casefold():
        raise PairProtocolError("session_id must use canonical UUID form")
    return str(parsed)


def metadata_message(
    *,
    session_id: str | None,
    platform: Mapping[str, object],
    python: Mapping[str, object],
    paqto_version: str,
    paqto_import_path: str,
    compatibility_suite: Mapping[str, object],
    capabilities: Mapping[str, object],
) -> dict[str, object]:
    """Build the minimal non-sensitive metadata exchanged after READY."""
    return {
        "kind": "metadata",
        "protocol_version": PAIR_PROTOCOL_VERSION,
        "serializer_id": PROTOCOL_ID,
        "session_id": session_id,
        "platform": {
            "os_family": platform.get("os_family"),
            "kernel_system": platform.get("kernel_system"),
            "release": platform.get("release"),
            "architecture": platform.get("architecture"),
            "sys_platform": platform.get("sys_platform"),
        },
        "python": {
            "implementation": python.get("implementation"),
            "version": python.get("version"),
        },
        "paqto": {
            "version": paqto_version,
            "import_path": paqto_import_path,
        },
        "compatibility_suite": dict(compatibility_suite),
        "capabilities": dict(capabilities),
    }


def validate_metadata(
    payload: object,
    *,
    expected_session_id: str | None = None,
    allow_missing_session: bool = False,
) -> dict[str, object]:
    """Validate remote metadata before it enters a report."""
    raw = _mapping(payload, "metadata")
    if raw.get("kind") != "metadata":
        raise PairProtocolError("metadata kind is invalid")
    if raw.get("protocol_version") != PAIR_PROTOCOL_VERSION:
        raise PairProtocolError("compatibility protocol versions are incompatible")
    if raw.get("serializer_id") != PROTOCOL_ID:
        raise PairProtocolError("serializer identifiers are incompatible")
    session_id = validate_session_id(
        raw.get("session_id"), allow_none=allow_missing_session
    )
    if expected_session_id is not None and session_id != expected_session_id:
        raise PairProtocolError("remote session_id does not match this run")

    platform = _mapping(raw.get("platform"), "platform")
    python = _mapping(raw.get("python"), "python")
    paqto = _mapping(raw.get("paqto"), "paqto")
    compatibility_suite = _mapping(
        raw.get("compatibility_suite"), "compatibility_suite"
    )
    capabilities = _mapping(raw.get("capabilities"), "capabilities")
    for field in (
        "os_family",
        "kernel_system",
        "release",
        "architecture",
        "sys_platform",
    ):
        _non_empty_string(platform.get(field), f"platform.{field}")
    for field in ("implementation", "version"):
        _non_empty_string(python.get(field), f"python.{field}")
    _non_empty_string(paqto.get("version"), "paqto.version")
    _non_empty_string(paqto.get("import_path"), "paqto.import_path")
    _non_empty_string(compatibility_suite.get("version"), "compatibility_suite.version")
    _non_empty_string(
        compatibility_suite.get("build_id"), "compatibility_suite.build_id"
    )
    for field in ("schema_version", "pair_protocol_version", "source_file_count"):
        value = compatibility_suite.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PairProtocolError(
                f"compatibility_suite.{field} must be a positive integer"
            )
    if not all(isinstance(key, str) for key in capabilities):
        raise PairProtocolError("capability keys must be strings")
    return deepcopy(dict(raw))


def session_payload(session_id: str, **values: object) -> dict[str, object]:
    """Create a small session-bound coordination payload."""
    validated = validate_session_id(session_id)
    return {"session_id": validated, **values}


def completion_payload(
    session_id: str,
    phase: str,
    **values: object,
) -> dict[str, object]:
    """Create one explicitly phased message in the completion workflow."""
    if phase not in {
        COMPLETION_REQUEST,
        COMPLETION_REPLY,
        COMPLETION_CONFIRMATION,
        COMPLETION_CONFIRMATION_REPLY,
    }:
        raise PairProtocolError("completion phase is invalid")
    return session_payload(session_id, completion_phase=phase, **values)


def validate_completion_payload(
    payload: object,
    expected_session_id: str,
    *,
    expected_phase: str,
) -> dict[str, object]:
    """Validate the pair session and exact phase of a completion message."""
    raw = validate_session_payload(payload, expected_session_id)
    phase = raw.get("completion_phase")
    if phase != expected_phase:
        raise PairProtocolError(
            f"completion phase must be {expected_phase!r}, got {phase!r}"
        )
    return raw


def validate_session_payload(
    payload: object,
    expected_session_id: str,
) -> dict[str, object]:
    """Reject malformed or cross-run messages."""
    raw = dict(_mapping(payload, "pair payload"))
    actual = validate_session_id(raw.get("session_id"))
    if actual != expected_session_id:
        raise PairProtocolError("pair message belongs to a different session")
    return raw


def failure_message(session_id: str | None, error: BaseException) -> dict[str, object]:
    """Build a redacted remote-failure notification (never key/certificate data)."""
    return {
        "kind": "failure",
        "session_id": session_id,
        "error_type": type(error).__name__,
        "detail": str(error),
    }


def raise_remote_failure(payload: object, expected_session_id: str | None) -> None:
    """Validate and raise a failure explicitly propagated by the other role."""
    raw = _mapping(payload, "failure")
    if raw.get("kind") != "failure":
        raise PairProtocolError("failure kind is invalid")
    session_id = validate_session_id(
        raw.get("session_id"), allow_none=expected_session_id is None
    )
    if expected_session_id is not None and session_id != expected_session_id:
        raise PairProtocolError("remote failure belongs to a different session")
    error_type = _non_empty_string(raw.get("error_type"), "error_type")
    detail = _non_empty_string(raw.get("detail"), "detail")
    raise RemotePairFailure(f"{error_type}: {detail}")

