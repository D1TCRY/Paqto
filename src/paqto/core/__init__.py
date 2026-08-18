"""Core abstractions for paqto."""

from paqto.core.config import (
    BackpressurePolicy,
    HandlerErrorPolicy,
    PaqtoConfig,
    ReconnectPolicy,
)
from paqto.core.connection import Connection, ConnectionState
from paqto.core.discovered import DiscoveredPeer, PeerFreshness
from paqto.core.discovery import DiscoveryService, NoDiscovery
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    AcknowledgementError,
    AcknowledgementTimeoutError,
    AcknowledgementUnavailableError,
    AlreadyStartedError,
    ConnectionClosedError,
    ConnectionIdleTimeoutError,
    DiscoveryError,
    MessageRoutingError,
    NoEndpointError,
    NotStartedError,
    PaqtoError,
    PaqtoTimeoutError,
    PeerAuthenticationError,
    PeerExpiredError,
    PeerIdentityMismatchError,
    PeerNotFoundError,
    ProtocolError,
    ProtocolFrameError,
    ProtocolHandshakeError,
    ProtocolHandshakeTimeoutError,
    ProtocolVersionError,
    RequestError,
    RequestTimeoutError,
    ResourceLimitError,
    SerializationError,
    TransportError,
)
from paqto.core.events import EventHandler, EventRouter, NodeEvent, NodeEventType
from paqto.core.listener import Listener
from paqto.core.manager import ConnectionManager
from paqto.core.message import Message
from paqto.core.node import PaqtoNode
from paqto.core.peer import Peer
from paqto.core.protocol import (
    HEARTBEAT_CAPABILITY,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    TECHNICAL_ACK_CAPABILITY,
    HandshakeOffer,
    HeartbeatPing,
    HeartbeatPong,
    ProtocolSession,
    TechnicalAcknowledgement,
)
from paqto.core.router import MessageHandler, MessageRouter
from paqto.core.security import SecurityInfo
from paqto.core.serializer import Serializer
from paqto.core.transport import Transport

__all__ = [
    "HEARTBEAT_CAPABILITY",
    "PROTOCOL_MAGIC",
    "PROTOCOL_VERSION",
    "TECHNICAL_ACK_CAPABILITY",
    "AcknowledgementError",
    "AcknowledgementTimeoutError",
    "AcknowledgementUnavailableError",
    "AlreadyStartedError",
    "BackpressurePolicy",
    "Connection",
    "ConnectionClosedError",
    "ConnectionIdleTimeoutError",
    "ConnectionManager",
    "ConnectionState",
    "DiscoveredPeer",
    "DiscoveryError",
    "DiscoveryService",
    "Endpoint",
    "EventHandler",
    "EventRouter",
    "HandlerErrorPolicy",
    "HandshakeOffer",
    "HeartbeatPing",
    "HeartbeatPong",
    "Listener",
    "Message",
    "MessageHandler",
    "MessageRouter",
    "MessageRoutingError",
    "NoDiscovery",
    "NoEndpointError",
    "NodeEvent",
    "NodeEventType",
    "NotStartedError",
    "PaqtoConfig",
    "PaqtoError",
    "PaqtoNode",
    "PaqtoTimeoutError",
    "Peer",
    "PeerAuthenticationError",
    "PeerExpiredError",
    "PeerFreshness",
    "PeerIdentityMismatchError",
    "PeerNotFoundError",
    "ProtocolError",
    "ProtocolFrameError",
    "ProtocolHandshakeError",
    "ProtocolHandshakeTimeoutError",
    "ProtocolSession",
    "ProtocolVersionError",
    "ReconnectPolicy",
    "RequestError",
    "RequestTimeoutError",
    "ResourceLimitError",
    "SecurityInfo",
    "SerializationError",
    "Serializer",
    "TechnicalAcknowledgement",
    "Transport",
    "TransportError",
]
