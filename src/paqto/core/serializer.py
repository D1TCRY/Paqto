"""Application message serialization contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from paqto.core.message import Message


class Serializer(ABC):
    """Convert complete :class:`Message` envelopes to and from bytes.

    Implementations own payload and envelope safety. They should raise
    :class:`SerializationError` for expected conversion failures and must
    preserve fields needed for routing and request/reply correlation.
    """

    @property
    def protocol_id(self) -> str:
        """Encoding identifier advertised during protocol negotiation.

        Subclasses shared across processes get a useful default. Cross-language
        or dynamically named serializers should override this property or set
        ``PaqtoConfig.serializer_id`` explicitly.
        """
        serializer_type = type(self)
        return f"python:{serializer_type.__module__}.{serializer_type.__qualname__}"

    @abstractmethod
    def serialize(self, message: Message) -> bytes:
        """Serialize a complete message envelope to bytes."""

    @abstractmethod
    def deserialize(self, data: bytes) -> Message:
        """Deserialize bytes into a complete message envelope."""
