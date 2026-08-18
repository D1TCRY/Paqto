"""Transport-neutral established-connection security metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True, slots=True)
class SecurityInfo:
    """Transport-neutral security properties of a connection.

    The default value makes no security claims. Transports may expose a more
    specific snapshot without requiring encryption or any particular security
    mechanism.

    Attributes:
        encrypted: Whether the transport guarantees channel encryption.
        authenticated: Whether the remote endpoint was authenticated.
        authenticated_peer_id: Logical id derived from an authenticated
            mechanism, or ``None`` when no mapping was established.
        mechanism: Security mechanism name, such as ``"tls"``.
        metadata: Read-only shallow snapshot of mechanism details.
    """

    encrypted: bool = False
    authenticated: bool = False
    authenticated_peer_id: str | None = None
    mechanism: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )
