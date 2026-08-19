"""Built-in serializer for binary application payloads."""

from __future__ import annotations

import base64
import binascii

from paqto.core.errors import SerializationError
from paqto.core.message import Message
from paqto.core.serializer import Serializer
from paqto.serializers._envelope import decode_envelope, encode_envelope
from paqto.serializers._json import decode_json, encode_json

BYTES_PROTOCOL_ID = "paqto.message-bytes.v1"


class BytesSerializer(Serializer):
    """Serialize complete messages with an exact ``bytes`` payload.

    The envelope uses deterministic UTF-8 JSON and carries the payload as
    canonical Base64, making the format dependency-free and safe to decode.
    """

    @property
    def protocol_id(self) -> str:
        return BYTES_PROTOCOL_ID

    def serialize(self, message: Message) -> bytes:
        if not isinstance(message.payload, bytes):
            raise SerializationError("BytesSerializer payload must be bytes.")
        payload = base64.b64encode(message.payload).decode("ascii")
        return encode_json(encode_envelope(message, payload))

    def deserialize(self, data: bytes) -> Message:
        return decode_envelope(decode_json(data), decode_payload=_decode_payload)


def _decode_payload(payload: object) -> bytes:
    if not isinstance(payload, str):
        raise SerializationError("BytesSerializer payload must be a Base64 string.")
    try:
        encoded = payload.encode("ascii")
        decoded = base64.b64decode(encoded, validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise SerializationError(
            "BytesSerializer payload must be valid Base64 data."
        ) from exc
    if base64.b64encode(decoded) != encoded:
        raise SerializationError("BytesSerializer payload must use canonical Base64.")
    return decoded
