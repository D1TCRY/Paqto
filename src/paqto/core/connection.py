"""Transport-neutral connection contract and lifecycle state."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

from paqto.core.endpoint import Endpoint
from paqto.core.security import SecurityInfo

_UNSECURED = SecurityInfo()


class ConnectionState(str, Enum):
    """Lifecycle state of the logical connection to a peer.

    A physical :class:`Connection` is one socket-like channel.  This state is
    intentionally owned by connection orchestration because reconnect can
    replace that physical channel while preserving the same logical peer.
    """

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"
    CLOSED = "closed"


class Connection(ABC):
    """An asynchronous complete-frame channel to a remote endpoint.

    Implementations preserve frame boundaries, serialize concurrent operations
    as needed, and make ``close()`` safe during cleanup. The contract does not
    imply TCP ordering, encryption, authentication, persistence, or delivery to
    remote application code.
    """

    @property
    @abstractmethod
    def local_endpoint(self) -> Endpoint:
        """Endpoint used locally for this connection."""

    @property
    @abstractmethod
    def remote_endpoint(self) -> Endpoint:
        """Endpoint reached by this connection."""

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        """Whether the connection can no longer be used."""

    @property
    def security_info(self) -> SecurityInfo:
        """Security properties guaranteed by this connection.

        The base implementation reports no security guarantees so transports
        are not required to implement a security mechanism.
        """
        return _UNSECURED

    @abstractmethod
    async def send_frame(self, data: bytes) -> None:
        """Write one complete frame or raise a transport-layer exception."""

    @abstractmethod
    async def receive_frame(self) -> bytes:
        """Wait for and return one complete frame without transport framing."""

    @abstractmethod
    async def close(self) -> None:
        """Release connection resources; implementations should be idempotent."""

    async def __aenter__(self) -> Connection:  # noqa: PYI034 - Python 3.10 API
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
