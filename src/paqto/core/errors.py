"""Public Paqto exception hierarchy."""

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


class ConnectionIdleTimeoutError(PaqtoTimeoutError):
    """Raised internally when a READY connection exceeds its idle limit."""


class ResourceLimitError(PaqtoError):
    """Raised when a configured bounded resource limit is reached."""


class PeerNotFoundError(PaqtoError):
    """Raised when a peer is not known to the local node."""


class PeerExpiredError(PeerNotFoundError):
    """Raised when discovery information is too old for a new connection."""


class PeerAuthenticationError(PaqtoError):
    """Raised when a connection does not prove a required peer identity."""


class PeerIdentityMismatchError(PeerAuthenticationError):
    """Raised when authenticated and declared peer identities differ."""


class ProtocolError(PaqtoError):
    """Base error for the Paqto protocol layer."""


class ProtocolHandshakeError(ProtocolError):
    """Raised when a connection cannot establish a Paqto protocol session."""


class ProtocolVersionError(ProtocolHandshakeError):
    """Raised when peers use incompatible Paqto protocol versions."""


class ProtocolFrameError(ProtocolError):
    """Raised for malformed or unexpected Paqto protocol frames."""


class ProtocolHandshakeTimeoutError(ProtocolHandshakeError, PaqtoTimeoutError):
    """Raised when the Paqto protocol handshake exceeds its deadline."""


class RequestError(PaqtoError):
    """Base error for request/reply correlation failures."""


class RequestTimeoutError(RequestError, PaqtoTimeoutError):
    """Raised when a correlated reply is not received before its deadline."""


class AcknowledgementError(ProtocolError):
    """Base error for technical acknowledgement failures."""


class AcknowledgementTimeoutError(AcknowledgementError, PaqtoTimeoutError):
    """Raised when a requested technical acknowledgement times out."""


class AcknowledgementUnavailableError(AcknowledgementError):
    """Raised when the READY session did not negotiate acknowledgements."""


class NoEndpointError(PaqtoError):
    """Raised when no compatible endpoint exists for a peer."""
