"""Paqto hello negotiation and READY-session frame encoding."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from paqto.core.connection import Connection
from paqto.core.errors import (
    ConnectionClosedError,
    PeerAuthenticationError,
    PeerIdentityMismatchError,
    ProtocolFrameError,
    ProtocolHandshakeError,
    ProtocolHandshakeTimeoutError,
    ProtocolVersionError,
    TransportError,
)

PROTOCOL_MAGIC = "PAQTO"
PROTOCOL_VERSION = 1
TECHNICAL_ACK_CAPABILITY = "paqto.ack.v1"
HEARTBEAT_CAPABILITY = "paqto.heartbeat.v1"

_CONTROL_FRAME = 0
_APPLICATION_FRAME = 1
_HELLO_TYPE = "hello"
_ACK_TYPE = "ack"
_PING_TYPE = "ping"
_PONG_TYPE = "pong"
_MAX_CONTROL_PAYLOAD_SIZE = 64 * 1024
_MAX_JSON_NESTING = 32
_MAX_JSON_INTEGER_BITS = 4096


@dataclass(frozen=True, slots=True)
class HandshakeOffer:
    """Protocol properties sent in a Paqto hello control frame.

    Attributes:
        peer_id: Logical identity declared by the sender. This declaration is
            not authenticated by itself.
        serializer_id: Stable identifier for the application wire format.
        version: Exact protocol version required from the remote peer.
        capabilities: Ordered optional feature names offered by the sender.
        max_message_size: Maximum serialized application bytes accepted.
        metadata: Read-only shallow copy of generic JSON-safe hello metadata.
    """

    peer_id: str
    serializer_id: str
    version: int = PROTOCOL_VERSION
    capabilities: tuple[str, ...] = ()
    max_message_size: int = 16 * 1024 * 1024
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.peer_id, str) or not self.peer_id:
            raise ValueError("Handshake peer_id must be a non-empty string.")
        if not isinstance(self.serializer_id, str) or not self.serializer_id:
            raise ValueError("Handshake serializer_id must be a non-empty string.")
        if not isinstance(self.version, int) or isinstance(self.version, bool):
            raise TypeError("Handshake version must be an integer.")
        if self.version < 1:
            raise ValueError("Handshake version must be at least 1.")
        if not isinstance(self.capabilities, tuple) or any(
            not isinstance(capability, str) or not capability
            for capability in self.capabilities
        ):
            raise TypeError(
                "Handshake capabilities must be a tuple of non-empty strings."
            )
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("Handshake capabilities must not contain duplicates.")
        if not isinstance(self.max_message_size, int) or isinstance(
            self.max_message_size, bool
        ):
            raise TypeError("Handshake max_message_size must be an integer.")
        if self.max_message_size <= 0:
            raise ValueError("Handshake max_message_size must be greater than zero.")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("Handshake metadata must be a mapping.")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class ProtocolSession:
    """Immutable negotiated properties of a READY connection.

    Attributes:
        peer_id: Remote identity declared in the validated hello.
        version: Exact protocol version shared by both peers.
        serializer_id: Exact serializer identity shared by both peers.
        capabilities: Local-order intersection of offered capabilities.
        max_message_size: Lower of the two offered serialized-byte limits.
        peer_id_authenticated: Whether transport-authenticated identity exists
            and matches ``peer_id``.
        metadata: Read-only shallow copy of the remote hello metadata.
    """

    peer_id: str
    version: int
    serializer_id: str
    capabilities: tuple[str, ...]
    max_message_size: int
    peer_id_authenticated: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class TechnicalAcknowledgement:
    """Protocol receipt for one application message.

    This confirms only remote receipt, deserialization, and envelope
    validation. It may be sent before dispatch queue admission and makes no
    claim about handler completion, application success, or durable processing.
    """

    message_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, str) or not self.message_id:
            raise ValueError("Acknowledged message_id must be a non-empty string.")


@dataclass(frozen=True, slots=True)
class HeartbeatPing:
    """Liveness challenge sent as a protocol control frame."""

    ping_id: str

    def __post_init__(self) -> None:
        _validate_ping_id(self.ping_id)


@dataclass(frozen=True, slots=True)
class HeartbeatPong:
    """Response to one :class:`HeartbeatPing`."""

    ping_id: str

    def __post_init__(self) -> None:
        _validate_ping_id(self.ping_id)


async def negotiate_protocol(
    connection: Connection,
    offer: HandshakeOffer,
    *,
    timeout: float | None,
    expected_peer_id: str | None = None,
    require_authenticated_peer_id: bool = False,
) -> ProtocolSession:
    """Exchange Paqto hello frames and return a ready protocol session.

    The connection is closed on every unsuccessful handshake, including
    cancellation. Discovery may supply ``expected_peer_id`` as the intended
    destination, but only ``Connection.security_info`` can make the resulting
    identity cryptographically authenticated.

    Versions and serializer ids must match exactly. Capabilities are
    intersected and the smaller message-size offer wins. If an authenticated
    transport id exists, it must match the hello even when strict identity is
    disabled.
    """

    async def exchange() -> bytes:
        await connection.send_frame(_encode_hello(offer))
        return await connection.receive_frame()

    try:
        try:
            if timeout is None:
                frame = await exchange()
            else:
                frame = await asyncio.wait_for(exchange(), timeout)
        except TimeoutError as exc:
            raise ProtocolHandshakeTimeoutError(
                "Timed out while waiting for the Paqto protocol handshake."
            ) from exc
        except ConnectionClosedError as exc:
            raise ProtocolHandshakeError(
                "Remote peer closed the connection during the Paqto handshake."
            ) from exc
        except TransportError as exc:
            raise ProtocolHandshakeError(
                "Transport failed during the Paqto protocol handshake."
            ) from exc

        remote = _decode_hello(frame)
        if remote.version != offer.version:
            raise ProtocolVersionError(
                f"Incompatible Paqto protocol version: local {offer.version}, "
                f"remote {remote.version}."
            )
        if remote.serializer_id != offer.serializer_id:
            raise ProtocolHandshakeError(
                f"Incompatible serializer encoding: local {offer.serializer_id!r}, "
                f"remote {remote.serializer_id!r}."
            )
        if expected_peer_id is not None and remote.peer_id != expected_peer_id:
            raise PeerIdentityMismatchError(
                f"Expected peer identity {expected_peer_id!r} does not match the "
                f"Paqto handshake identity {remote.peer_id!r}."
            )

        security = connection.security_info
        authenticated_peer_id = security.authenticated_peer_id
        if (
            authenticated_peer_id is not None
            and authenticated_peer_id != remote.peer_id
        ):
            raise PeerIdentityMismatchError(
                f"Paqto handshake identity {remote.peer_id!r} does not match "
                f"authenticated connection identity {authenticated_peer_id!r}."
            )
        if require_authenticated_peer_id:
            if not security.authenticated:
                raise PeerAuthenticationError(
                    f"Peer {remote.peer_id!r} did not establish an authenticated "
                    "connection identity."
                )
            if authenticated_peer_id is None:
                raise PeerAuthenticationError(
                    f"Peer {remote.peer_id!r} did not provide an authenticated "
                    "peer identity."
                )

        negotiated_capabilities = tuple(
            capability
            for capability in offer.capabilities
            if capability in remote.capabilities
        )
        return ProtocolSession(
            peer_id=remote.peer_id,
            version=remote.version,
            serializer_id=remote.serializer_id,
            capabilities=negotiated_capabilities,
            max_message_size=min(
                offer.max_message_size,
                remote.max_message_size,
            ),
            peer_id_authenticated=(
                security.authenticated
                and authenticated_peer_id is not None
                and authenticated_peer_id == remote.peer_id
            ),
            metadata=remote.metadata,
        )
    except BaseException:
        await _close_after_failed_handshake(connection)
        raise


def encode_application_frame(data: bytes, *, max_message_size: int) -> bytes:
    """Wrap serialized application bytes in a Paqto application frame."""
    if not isinstance(data, bytes):
        raise TypeError("Application frame data must be bytes.")
    if len(data) > max_message_size:
        raise ProtocolFrameError(
            f"Application message size {len(data)} exceeds negotiated limit "
            f"{max_message_size}."
        )
    return bytes((_APPLICATION_FRAME,)) + data


def decode_application_frame(frame: bytes, *, max_message_size: int) -> bytes:
    """Return application bytes, rejecting control or malformed frames."""
    kind, payload = _split_frame(frame)
    if kind == _CONTROL_FRAME:
        raise ProtocolFrameError(
            "Unexpected Paqto control frame after the handshake completed."
        )
    if kind != _APPLICATION_FRAME:
        raise ProtocolFrameError(f"Unknown Paqto frame type {kind}.")
    if len(payload) > max_message_size:
        raise ProtocolFrameError(
            f"Incoming application message size {len(payload)} exceeds negotiated "
            f"limit {max_message_size}."
        )
    return payload


def encode_acknowledgement_frame(message_id: str) -> bytes:
    """Encode a technical acknowledgement as a Paqto control frame."""
    acknowledgement = TechnicalAcknowledgement(message_id)
    return _encode_control(
        {
            "magic": PROTOCOL_MAGIC,
            "type": _ACK_TYPE,
            "message_id": acknowledgement.message_id,
        }
    )


def encode_ping_frame(ping_id: str) -> bytes:
    """Encode a heartbeat PING control frame."""
    ping = HeartbeatPing(ping_id)
    return _encode_control(
        {"magic": PROTOCOL_MAGIC, "type": _PING_TYPE, "ping_id": ping.ping_id}
    )


def encode_pong_frame(ping_id: str) -> bytes:
    """Encode a heartbeat PONG control frame."""
    pong = HeartbeatPong(ping_id)
    return _encode_control(
        {"magic": PROTOCOL_MAGIC, "type": _PONG_TYPE, "ping_id": pong.ping_id}
    )


def decode_session_frame(
    frame: bytes,
    *,
    max_message_size: int,
) -> bytes | TechnicalAcknowledgement | HeartbeatPing | HeartbeatPong:
    """Decode a READY-session frame into application bytes or a control value."""
    kind, payload = _split_frame(frame)
    if kind == _APPLICATION_FRAME:
        if len(payload) > max_message_size:
            raise ProtocolFrameError(
                f"Incoming application message size {len(payload)} exceeds "
                f"negotiated limit {max_message_size}."
            )
        return payload
    if kind != _CONTROL_FRAME:
        raise ProtocolFrameError(f"Unknown Paqto frame type {kind}.")
    raw = _decode_control(payload)
    if raw.get("magic") != PROTOCOL_MAGIC:
        raise ProtocolFrameError("Invalid Paqto protocol identifier.")
    control_type = raw.get("type")
    if control_type == _ACK_TYPE:
        message_id = raw.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ProtocolFrameError(
                "Acknowledgement message_id must be a non-empty string."
            )
        return TechnicalAcknowledgement(message_id)
    if control_type in (_PING_TYPE, _PONG_TYPE):
        ping_id = raw.get("ping_id")
        if not isinstance(ping_id, str) or not ping_id:
            raise ProtocolFrameError("Heartbeat ping_id must be a non-empty string.")
        if control_type == _PING_TYPE:
            return HeartbeatPing(ping_id)
        return HeartbeatPong(ping_id)
    raise ProtocolFrameError("Unexpected Paqto control frame after READY.")


def _encode_hello(offer: HandshakeOffer) -> bytes:
    """Encode a validated local offer as a bounded hello control frame."""
    payload = {
        "magic": PROTOCOL_MAGIC,
        "type": _HELLO_TYPE,
        "version": offer.version,
        "peer_id": offer.peer_id,
        "capabilities": list(offer.capabilities),
        "serializer": offer.serializer_id,
        "max_message_size": offer.max_message_size,
        "metadata": dict(offer.metadata),
    }
    try:
        return _encode_control(payload)
    except ProtocolFrameError as exc:
        raise ProtocolHandshakeError(
            "Local Paqto handshake metadata is not valid control data."
        ) from exc


def _decode_hello(frame: bytes) -> HandshakeOffer:
    """Decode and validate one remote hello without authenticating its peer id."""
    try:
        kind, payload = _split_frame(frame)
    except ProtocolFrameError as exc:
        raise ProtocolHandshakeError("Malformed Paqto hello frame.") from exc
    if kind != _CONTROL_FRAME:
        raise ProtocolHandshakeError(
            "Expected a Paqto hello control frame before application data."
        )
    if len(payload) > _MAX_CONTROL_PAYLOAD_SIZE:
        raise ProtocolHandshakeError(
            f"Remote Paqto handshake exceeds {_MAX_CONTROL_PAYLOAD_SIZE} bytes."
        )
    try:
        raw = _decode_control(payload)
    except ProtocolFrameError as exc:
        raise ProtocolHandshakeError("Malformed Paqto handshake JSON.") from exc
    if raw.get("magic") != PROTOCOL_MAGIC:
        raise ProtocolHandshakeError("Invalid Paqto protocol identifier.")
    if raw.get("type") != _HELLO_TYPE:
        raise ProtocolHandshakeError("Expected a Paqto hello handshake.")

    version = raw.get("version")
    peer_id = raw.get("peer_id")
    serializer_id = raw.get("serializer")
    capabilities = raw.get("capabilities")
    max_message_size = raw.get("max_message_size")
    metadata = raw.get("metadata")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ProtocolHandshakeError("Handshake version must be a positive integer.")
    if not isinstance(peer_id, str) or not peer_id:
        raise ProtocolHandshakeError("Handshake peer_id must be a non-empty string.")
    if not isinstance(serializer_id, str) or not serializer_id:
        raise ProtocolHandshakeError("Handshake serializer must be a non-empty string.")
    if not isinstance(capabilities, list) or any(
        not isinstance(capability, str) or not capability for capability in capabilities
    ):
        raise ProtocolHandshakeError(
            "Handshake capabilities must be a list of non-empty strings."
        )
    if len(set(capabilities)) != len(capabilities):
        raise ProtocolHandshakeError(
            "Handshake capabilities must not contain duplicates."
        )
    if (
        not isinstance(max_message_size, int)
        or isinstance(max_message_size, bool)
        or max_message_size <= 0
    ):
        raise ProtocolHandshakeError(
            "Handshake max_message_size must be a positive integer."
        )
    if not isinstance(metadata, dict):
        raise ProtocolHandshakeError("Handshake metadata must be a JSON object.")

    return HandshakeOffer(
        peer_id=peer_id,
        serializer_id=serializer_id,
        version=version,
        capabilities=tuple(capabilities),
        max_message_size=max_message_size,
        metadata=metadata,
    )


def _split_frame(frame: bytes) -> tuple[int, bytes]:
    """Split the one-byte Paqto frame kind from its payload."""
    if not isinstance(frame, bytes):
        raise ProtocolFrameError("Paqto connection frames must be bytes.")
    if not frame:
        raise ProtocolFrameError("Received an empty Paqto protocol frame.")
    return frame[0], frame[1:]


def _encode_control(payload: Mapping[str, Any]) -> bytes:
    """Encode bounded, finite, uniquely interpretable JSON control data."""
    if not _is_safe_json_value(payload):
        raise ProtocolFrameError(
            "Paqto control data contains unsafe numbers or exceeds the "
            f"maximum nesting depth of {_MAX_JSON_NESTING}."
        )
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (RecursionError, TypeError, ValueError) as exc:
        raise ProtocolFrameError(
            "Paqto control data is not JSON serializable."
        ) from exc
    if len(encoded) > _MAX_CONTROL_PAYLOAD_SIZE:
        raise ProtocolFrameError(
            f"Paqto control frame exceeds {_MAX_CONTROL_PAYLOAD_SIZE} bytes."
        )
    return bytes((_CONTROL_FRAME,)) + encoded


def _decode_control(payload: bytes) -> dict[str, Any]:
    """Decode bounded control JSON while rejecting duplicate keys and unsafe values."""
    if len(payload) > _MAX_CONTROL_PAYLOAD_SIZE:
        raise ProtocolFrameError(
            f"Paqto control frame exceeds {_MAX_CONTROL_PAYLOAD_SIZE} bytes."
        )
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (RecursionError, UnicodeDecodeError, ValueError) as exc:
        raise ProtocolFrameError("Malformed Paqto control JSON.") from exc
    if not isinstance(raw, dict):
        raise ProtocolFrameError("Paqto control data must be a JSON object.")
    if not _is_safe_json_value(raw):
        raise ProtocolFrameError(
            "Paqto control data contains unsafe numbers or exceeds the "
            f"maximum nesting depth of {_MAX_JSON_NESTING}."
        )
    return raw


async def _close_after_failed_handshake(connection: Connection) -> None:
    """Best-effort close a connection without masking its handshake failure."""
    try:
        await asyncio.shield(connection.close())
    except (ConnectionError, OSError, RuntimeError, TransportError):
        pass


def _validate_ping_id(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError("Heartbeat ping_id must be a non-empty string.")


def _is_safe_json_value(value: Any) -> bool:
    """Return whether nested JSON data satisfies number and depth limits."""
    pending: list[tuple[Any, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, float) and not math.isfinite(item):
            return False
        if (
            isinstance(item, int)
            and not isinstance(item, bool)
            and item.bit_length() > _MAX_JSON_INTEGER_BITS
        ):
            return False
        if isinstance(item, dict):
            if depth >= _MAX_JSON_NESTING:
                return False
            pending.extend((nested, depth + 1) for nested in item.values())
        elif isinstance(item, list):
            if depth >= _MAX_JSON_NESTING:
                return False
            pending.extend((nested, depth + 1) for nested in item)
    return True


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while rejecting ambiguous duplicate member names."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key {key!r}.")
        result[key] = value
    return result
