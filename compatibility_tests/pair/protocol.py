"""Application-level coordination protocol for two-device compatibility runs."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any
from uuid import UUID

from compatibility_tests.common.serializer import PROTOCOL_ID

PAIR_PROTOCOL_VERSION = 1


class PairProtocolError(ValueError):
    """A compatibility coordination message is malformed or inconsistent."""


class RemotePairFailure(RuntimeError):
    """The other role explicitly reported a suite failure."""


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
        "paqto": {"version": paqto_version},
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
    if not all(isinstance(key, str) for key in capabilities):
        raise PairProtocolError("capability keys must be strings")
    return deepcopy(dict(raw))


def session_payload(session_id: str, **values: object) -> dict[str, object]:
    """Create a small session-bound coordination payload."""
    validated = validate_session_id(session_id)
    return {"session_id": validated, **values}


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

