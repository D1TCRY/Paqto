"""Stable JSON envelope used only by compatibility scenarios."""

from __future__ import annotations

import json
from typing import Any

from paqto import Message, Serializer

PROTOCOL_ID = "paqto.compatibility.json.v1"


class CompatibilityJsonSerializer(Serializer):
    """Serialize the complete public Message envelope deterministically."""

    @property
    def protocol_id(self) -> str:
        return PROTOCOL_ID

    def serialize(self, message: Message) -> bytes:
        return json.dumps(
            {
                "payload": message.payload,
                "type": message.type,
                "sender": message.sender,
                "recipient": message.recipient,
                "headers": message.headers,
                "id": message.id,
                "reply_to": message.reply_to,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def deserialize(self, data: bytes) -> Message:
        raw: dict[str, Any] = json.loads(data.decode("utf-8"))
        return Message(
            payload=raw["payload"],
            type=raw["type"],
            sender=raw["sender"],
            recipient=raw["recipient"],
            headers=raw["headers"],
            id=raw["id"],
            reply_to=raw["reply_to"],
        )

