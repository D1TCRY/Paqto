"""Shared validation for built-in application-message serializers."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from paqto.core.errors import SerializationError
from paqto.core.message import Message

_ENVELOPE_FIELDS = frozenset(
    {
        "payload",
        "type",
        "sender",
        "recipient",
        "headers",
        "id",
        "created_at",
        "reply_to",
    }
)


def encode_envelope(message: Message, payload: object) -> dict[str, object]:
    """Return the canonical mapping for one complete message envelope."""
    if not isinstance(message, Message):
        raise SerializationError("Built-in serializers require a Message instance.")
    _validate_message_fields(message)
    return {
        "payload": payload,
        "type": message.type,
        "sender": message.sender,
        "recipient": message.recipient,
        "headers": message.headers,
        "id": message.id,
        "created_at": message.created_at.isoformat(),
        "reply_to": message.reply_to,
    }


def decode_envelope(
    raw: object,
    *,
    decode_payload: Callable[[object], Any],
) -> Message:
    """Validate a canonical mapping and construct its complete message."""
    if not isinstance(raw, dict):
        raise SerializationError("Serialized message envelope must be a JSON object.")
    fields = set(raw)
    if fields != _ENVELOPE_FIELDS:
        missing = sorted(_ENVELOPE_FIELDS - fields)
        unexpected = sorted(fields - _ENVELOPE_FIELDS)
        details: list[str] = []
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected fields: {', '.join(unexpected)}")
        raise SerializationError(
            "Serialized message envelope has an invalid schema ("
            + "; ".join(details)
            + ")."
        )

    created_at_raw = raw["created_at"]
    if not isinstance(created_at_raw, str):
        raise SerializationError("Message created_at must be an ISO 8601 string.")
    try:
        created_at = datetime.fromisoformat(created_at_raw)
    except ValueError as exc:
        raise SerializationError(
            "Message created_at must be a valid ISO 8601 datetime."
        ) from exc

    message = Message(
        payload=decode_payload(raw["payload"]),
        type=raw["type"],  # type: ignore[arg-type]
        sender=raw["sender"],  # type: ignore[arg-type]
        recipient=raw["recipient"],  # type: ignore[arg-type]
        headers=raw["headers"],  # type: ignore[arg-type]
        id=raw["id"],  # type: ignore[arg-type]
        created_at=created_at,
        reply_to=raw["reply_to"],  # type: ignore[arg-type]
    )
    _validate_message_fields(message)
    return message


def _validate_message_fields(message: Message) -> None:
    if not isinstance(message.type, str) or not message.type:
        raise SerializationError("Message type must be a non-empty string.")
    if not isinstance(message.id, str) or not message.id:
        raise SerializationError("Message id must be a non-empty string.")
    for name in ("sender", "recipient", "reply_to"):
        value = getattr(message, name)
        if value is not None and (not isinstance(value, str) or not value):
            raise SerializationError(
                f"Message {name} must be a non-empty string or None."
            )
    if not isinstance(message.headers, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in message.headers.items()
    ):
        raise SerializationError("Message headers must map strings to strings.")
    if not isinstance(message.created_at, datetime):
        raise SerializationError("Message created_at must be a datetime.")
    if message.created_at.tzinfo is None or message.created_at.utcoffset() is None:
        raise SerializationError("Message created_at must be timezone-aware.")
