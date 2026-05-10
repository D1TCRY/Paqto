from __future__ import annotations


class PaqtoError(Exception):
    """Base exception for paqto."""


class AlreadyStartedError(PaqtoError):
    """Raised when a component is started more than once."""


class NotStartedError(PaqtoError):
    """Raised when an operation requires a running component."""


class TransportError(PaqtoError):
    """Raised by transport implementations or transport orchestration."""


class ConnectionClosedError(TransportError):
    """Raised when a connection is closed while being used."""


class DiscoveryError(PaqtoError):
    """Raised by discovery implementations."""


class SerializationError(PaqtoError):
    """Raised when a message cannot be serialized or deserialized."""


class MessageRoutingError(PaqtoError):
    """Raised when dispatching a message fails."""


class PaqtoTimeoutError(PaqtoError):
    """Raised when an operation exceeds its timeout."""


class PeerNotFoundError(PaqtoError):
    """Raised when a peer is not known to the local node."""


class NoEndpointError(PaqtoError):
    """Raised when no compatible endpoint exists for a peer."""

