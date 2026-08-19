"""Built-in serializer for JSON-compatible application payloads."""

from __future__ import annotations

from typing import Any

from paqto.core.message import Message
from paqto.core.serializer import Serializer
from paqto.serializers._envelope import decode_envelope, encode_envelope
from paqto.serializers._json import (
    decode_json,
    encode_json,
    validate_json_value,
    validate_limit,
)

JSON_PROTOCOL_ID = "paqto.message-json.v1"


class JsonSerializer(Serializer):
    """Serialize complete messages whose payloads contain portable JSON values.

    JSON payloads may contain ``None``, booleans, finite numbers, strings,
    lists, and dictionaries with string keys. Tuples, bytes, and arbitrary
    Python objects are rejected instead of being converted implicitly.
    """

    def __init__(
        self,
        *,
        max_nesting: int = 64,
        max_collection_items: int = 100_000,
        max_string_length: int = 1_000_000,
    ) -> None:
        validate_limit("max_nesting", max_nesting)
        validate_limit("max_collection_items", max_collection_items)
        validate_limit("max_string_length", max_string_length)
        self.max_nesting = max_nesting
        self.max_collection_items = max_collection_items
        self.max_string_length = max_string_length

    @property
    def protocol_id(self) -> str:
        return JSON_PROTOCOL_ID

    def serialize(self, message: Message) -> bytes:
        self._validate_payload(message.payload)
        return encode_json(encode_envelope(message, message.payload))

    def deserialize(self, data: bytes) -> Message:
        raw = decode_json(data)
        return decode_envelope(raw, decode_payload=self._decode_payload)

    def _decode_payload(self, payload: object) -> Any:
        self._validate_payload(payload)
        return payload

    def _validate_payload(self, payload: object) -> None:
        validate_json_value(
            payload,
            max_nesting=self.max_nesting,
            max_collection_items=self.max_collection_items,
            max_string_length=self.max_string_length,
        )
