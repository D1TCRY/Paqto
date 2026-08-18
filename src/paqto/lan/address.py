"""LAN TCP endpoint parsing, construction, and validation helpers."""

from __future__ import annotations

import math
import socket
from dataclasses import dataclass
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlparse

from paqto.core.endpoint import Endpoint
from paqto.core.errors import TransportError

TRANSPORT_NAME = "lan"
TCP_SCHEME = "tcp"
MAX_UINT32 = 0xFFFFFFFF


@dataclass(frozen=True, slots=True)
class TcpAddress:
    """Parsed host and port from a LAN TCP endpoint address."""

    host: str
    port: int


def parse_tcp_address(address: str) -> TcpAddress:
    """Parse a ``tcp://HOST:PORT`` address into host and port parts."""
    parsed = urlparse(address)
    if parsed.scheme != TCP_SCHEME:
        raise TransportError(
            f"LAN endpoint addresses must use the {TCP_SCHEME!r} scheme."
        )
    if parsed.username or parsed.password:
        raise TransportError("LAN endpoint addresses must not include user info.")
    if parsed.path or parsed.params or parsed.query or parsed.fragment:
        raise TransportError(
            "LAN endpoint addresses must have the form tcp://HOST:PORT."
        )

    host = parsed.hostname
    if not host:
        raise TransportError("LAN endpoint address is missing a host.")

    try:
        port = parsed.port
    except ValueError as exc:
        raise TransportError("LAN endpoint address contains an invalid port.") from exc

    if port is None:
        raise TransportError("LAN endpoint address is missing a port.")

    return TcpAddress(host=host, port=port)


def build_tcp_address(host: str, port: int) -> str:
    """Build a ``tcp://HOST:PORT`` address string."""
    if port < 0 or port > 65535:
        raise TransportError(f"TCP port out of range: {port!r}.")
    return f"{TCP_SCHEME}://{_format_host(host)}:{port}"


def endpoint_from_host_port(
    host: str,
    port: int,
    *,
    metadata: dict[str, Any] | None = None,
) -> Endpoint:
    """Create a LAN endpoint from host and port values."""
    return Endpoint(
        transport=TRANSPORT_NAME,
        address=build_tcp_address(host, port),
        metadata=dict(metadata or {}),
    )


def endpoint_from_sockname(
    sockname: Any,
    *,
    metadata: dict[str, Any] | None = None,
) -> Endpoint:
    """Create a LAN endpoint from an asyncio socket address tuple."""
    host, port = parse_sockname(sockname)
    return endpoint_from_host_port(host, port, metadata=metadata)


def parse_sockname(sockname: Any) -> tuple[str, int]:
    """Return ``(host, port)`` from an IPv4 or IPv6 socket address tuple."""
    if not isinstance(sockname, tuple) or len(sockname) < 2:
        raise TransportError("Could not determine TCP socket address.")

    host = str(sockname[0])
    port = sockname[1]
    if not isinstance(port, int):
        raise TransportError("Could not determine TCP socket port.")

    return host, port


def choose_advertised_host(
    bind_host: str,
    *,
    advertised_host: str | None = None,
) -> tuple[str, str]:
    """Choose the host to publish for a listener bound on ``bind_host``.

    ``advertised_host`` is an explicit host-environment override and takes
    precedence over automatic selection. When binding to all interfaces,
    Paqto otherwise tries local hostname resolution for the bind address
    family before falling back to the configured wildcard. Automatic
    selection never opens a socket toward an Internet address.

    The second return value describes the selection source for endpoint
    metadata. Address selection is best effort and does not authenticate a host.
    """
    if advertised_host is not None:
        if not isinstance(advertised_host, str):
            raise TypeError("advertised_host must be a string or None.")
        if not advertised_host:
            raise ValueError("advertised_host must be a non-empty string or None.")
        return advertised_host, "configured"

    if bind_host not in {"0.0.0.0", "::", ""}:
        return bind_host, "bind_host"

    family = socket.AF_INET6 if bind_host == "::" else socket.AF_INET
    resolved = _resolve_hostname_address(family)
    if resolved is not None:
        source = "hostname_ipv6" if family == socket.AF_INET6 else "hostname_ipv4"
        return resolved, source

    return bind_host or "0.0.0.0", "bind_host"


def validate_max_frame_size(max_frame_size: int) -> None:
    """Validate the frame size limit supported by the 4-byte TCP header."""
    if not isinstance(max_frame_size, int) or isinstance(max_frame_size, bool):
        raise TypeError("max_frame_size must be an integer.")
    if max_frame_size <= 0:
        raise ValueError("max_frame_size must be greater than zero.")
    if max_frame_size > MAX_UINT32:
        raise ValueError("max_frame_size cannot exceed 4,294,967,295 bytes.")


def validate_frame_payload_timeout(value: float | None) -> None:
    """Validate the deadline for completing a declared TCP frame payload."""
    if value is None:
        return
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError("frame_payload_timeout must be a number or None.")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(
            "frame_payload_timeout must be finite and greater than zero."
        )


def _format_host(host: str) -> str:
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        return f"[{host}]"
    return host


def _resolve_hostname_address(family: socket.AddressFamily) -> str | None:
    """Return a non-loopback local address resolved for this hostname."""
    try:
        infos = socket.getaddrinfo(
            socket.gethostname(),
            None,
            family=family,
            type=socket.SOCK_STREAM,
        )
    except OSError:
        return None

    for info in infos:
        host = info[4][0]
        if not isinstance(host, str):
            continue
        try:
            parsed = ip_address(host.split("%", 1)[0])
        except ValueError:
            continue
        if not parsed.is_loopback and not parsed.is_unspecified:
            return host
    return None
