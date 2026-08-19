import json
from datetime import datetime, timezone

import pytest

from paqto import Message, SerializationError, Serializer
from paqto.serializers import (
    BYTES_PROTOCOL_ID,
    JSON_PROTOCOL_ID,
    BytesSerializer,
    JsonSerializer,
)


def _message(payload: object) -> Message:
    return Message(
        payload=payload,
        type="example.event",
        sender="sender",
        recipient="recipient",
        headers={"schema": "v1"},
        id="message-id",
        created_at=datetime(2026, 8, 19, 12, 30, tzinfo=timezone.utc),
        reply_to="request-id",
    )


def test_built_in_serializers_are_public_serializer_implementations() -> None:
    json_serializer = JsonSerializer()
    bytes_serializer = BytesSerializer()

    assert isinstance(json_serializer, Serializer)
    assert isinstance(bytes_serializer, Serializer)
    assert json_serializer.protocol_id == JSON_PROTOCOL_ID
    assert bytes_serializer.protocol_id == BYTES_PROTOCOL_ID
    assert JSON_PROTOCOL_ID != BYTES_PROTOCOL_ID


def test_json_serializer_round_trips_the_complete_envelope() -> None:
    serializer = JsonSerializer()
    message = _message(
        {
            "active": True,
            "count": 3,
            "ratio": 1.5,
            "items": [None, "value"],
        }
    )

    restored = serializer.deserialize(serializer.serialize(message))

    assert restored == message


def test_json_serializer_encoding_is_deterministic() -> None:
    serializer = JsonSerializer()
    first = _message({"z": 1, "a": 2})
    second = _message({"a": 2, "z": 1})

    assert serializer.serialize(first) == serializer.serialize(second)


@pytest.mark.parametrize(
    "payload",
    [
        b"binary",
        ("tuple",),
        {1: "non-string key"},
        float("nan"),
        object(),
    ],
)
def test_json_serializer_rejects_non_portable_payloads(payload: object) -> None:
    with pytest.raises(SerializationError):
        JsonSerializer().serialize(_message(payload))


def test_json_serializer_enforces_configured_resource_limits() -> None:
    serializer = JsonSerializer(
        max_nesting=1,
        max_collection_items=2,
        max_string_length=3,
    )

    with pytest.raises(SerializationError, match="nesting"):
        serializer.serialize(_message([[1]]))
    with pytest.raises(SerializationError, match="collection items"):
        serializer.serialize(_message([1, 2, 3]))
    with pytest.raises(SerializationError, match="string"):
        serializer.serialize(_message("long"))


@pytest.mark.parametrize(
    ("name", "value", "error"),
    [
        ("max_nesting", 0, ValueError),
        ("max_collection_items", True, TypeError),
        ("max_string_length", 1.5, TypeError),
    ],
)
def test_json_serializer_rejects_invalid_limits(
    name: str,
    value: object,
    error: type[Exception],
) -> None:
    arguments = {name: value}
    with pytest.raises(error, match=name):
        JsonSerializer(**arguments)  # type: ignore[arg-type]


def test_bytes_serializer_round_trips_every_byte_value() -> None:
    serializer = BytesSerializer()
    message = _message(bytes(range(256)))

    restored = serializer.deserialize(serializer.serialize(message))

    assert restored == message


def test_bytes_serializer_rejects_non_bytes_payload() -> None:
    with pytest.raises(SerializationError, match="must be bytes"):
        BytesSerializer().serialize(_message(bytearray(b"binary")))


def test_built_in_serializers_reject_invalid_envelopes() -> None:
    serializer = JsonSerializer()
    encoded = serializer.serialize(_message({"valid": True}))
    raw = json.loads(encoded)
    del raw["reply_to"]

    with pytest.raises(SerializationError, match="missing fields: reply_to"):
        serializer.deserialize(json.dumps(raw).encode())

    duplicate = encoded.replace(b'"type":"example.event"', b'"type":"a","type":"b"')
    with pytest.raises(SerializationError, match="valid UTF-8 JSON"):
        serializer.deserialize(duplicate)


def test_built_in_serializers_require_timezone_aware_created_at() -> None:
    message = _message({"valid": True})
    message.created_at = message.created_at.replace(tzinfo=None)

    with pytest.raises(SerializationError, match="timezone-aware"):
        JsonSerializer().serialize(message)


def test_bytes_serializer_rejects_noncanonical_base64() -> None:
    serializer = BytesSerializer()
    raw = json.loads(serializer.serialize(_message(b"payload")))
    raw["payload"] = "cGF5bG9hZA==="

    with pytest.raises(SerializationError, match="Base64"):
        serializer.deserialize(json.dumps(raw).encode())
