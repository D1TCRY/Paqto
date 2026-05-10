from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.message import Message


class Serializer(ABC):
    """Converts complete Message envelopes to and from bytes."""

    @abstractmethod
    def serialize(self, message: Message) -> bytes:
        """Serialize a complete message envelope."""

    @abstractmethod
    def deserialize(self, data: bytes) -> Message:
        """Deserialize a complete message envelope."""

