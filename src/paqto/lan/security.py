"""TLS configuration and established identity metadata for LAN streams."""

from __future__ import annotations

import math
import os
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from paqto.core.errors import TransportError
from paqto.core.security import SecurityInfo

TlsPeerIdentityResolver = Callable[[Mapping[str, Any]], str | None]


@dataclass(frozen=True, slots=True)
class TlsConfig:
    """TLS configuration for both directions of a LAN transport.

    Supplying this configuration explicitly enables TLS. Outgoing connections
    verify the peer certificate and endpoint hostname by default. Incoming
    connections require a peer certificate only when
    ``require_client_certificate`` is enabled.

    ``peer_identity_resolver`` may map Python's verified certificate mapping to
    a logical identity. Paqto deliberately does not impose an X.509 naming
    convention.

    Attributes:
        certfile: PEM certificate chain presented by this node.
        keyfile: Private key for ``certfile``.
        cafile: Trust roots, or ``None`` to use system roots.
        verify_peer: Whether outgoing connections validate the server chain.
        check_hostname: Whether outgoing connections also verify the endpoint
            host against the server certificate.
        require_client_certificate: Whether incoming connections require and
            validate a client certificate.
        peer_identity_resolver: Callback mapping an already verified decoded
            certificate to a logical peer id, or ``None`` for no id mapping.
        handshake_timeout: Maximum TLS handshake duration in seconds.
    """

    certfile: str | os.PathLike[str]
    keyfile: str | os.PathLike[str]
    cafile: str | os.PathLike[str] | None = None
    verify_peer: bool = True
    check_hostname: bool = True
    require_client_certificate: bool = False
    peer_identity_resolver: TlsPeerIdentityResolver | None = None
    handshake_timeout: float = 10.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "certfile", _normalize_path(self.certfile, "certfile"))
        object.__setattr__(self, "keyfile", _normalize_path(self.keyfile, "keyfile"))
        if self.cafile is not None:
            object.__setattr__(self, "cafile", _normalize_path(self.cafile, "cafile"))

        for name in ("verify_peer", "check_hostname", "require_client_certificate"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a boolean.")
        if self.check_hostname and not self.verify_peer:
            raise ValueError(
                "check_hostname cannot be enabled when verify_peer is disabled."
            )
        if self.peer_identity_resolver is not None and not callable(
            self.peer_identity_resolver
        ):
            raise TypeError("peer_identity_resolver must be callable.")
        if not isinstance(self.handshake_timeout, (int, float)) or isinstance(
            self.handshake_timeout, bool
        ):
            raise TypeError("handshake_timeout must be a number.")
        if not math.isfinite(self.handshake_timeout) or self.handshake_timeout <= 0:
            raise ValueError(
                "handshake_timeout must be finite and greater than zero."
            )

    def create_client_context(self) -> ssl.SSLContext:
        """Build a client context using system or configured trust roots."""
        if self.verify_peer:
            context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=self.cafile,
            )
            context.check_hostname = self.check_hostname
        else:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        return context

    def create_server_context(self) -> ssl.SSLContext:
        """Build a server context, optionally requiring mutual TLS."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)

        if self.require_client_certificate:
            context.verify_mode = ssl.CERT_REQUIRED
            if self.cafile is None:
                context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
            else:
                context.load_verify_locations(cafile=self.cafile)
        else:
            context.verify_mode = ssl.CERT_NONE
        return context


def security_info_from_writer(
    writer: object,
    *,
    peer_authenticated: bool,
    identity_resolver: TlsPeerIdentityResolver | None,
    verified_server_name: str | None = None,
) -> SecurityInfo:
    """Build security metadata from an established asyncio TLS stream.

    The resolver is called only when ``peer_authenticated`` is true. Its result
    is an authenticated logical id only because the certificate was already
    verified; a certificate merely being present is not authentication.
    """
    get_extra_info = getattr(writer, "get_extra_info", None)
    if not callable(get_extra_info):
        raise TransportError("TLS stream does not expose connection metadata.")

    ssl_object = get_extra_info("ssl_object")
    if ssl_object is None:
        raise TransportError("TLS was configured but the stream is not encrypted.")

    certificate = ssl_object.getpeercert()
    binary_certificate = ssl_object.getpeercert(binary_form=True)
    if peer_authenticated and (not certificate or not binary_certificate):
        raise TransportError(
            "TLS peer verification succeeded without an accessible peer certificate."
        )

    authenticated_peer_id: str | None = None
    if peer_authenticated and identity_resolver is not None:
        try:
            authenticated_peer_id = identity_resolver(dict(certificate))
        except Exception as exc:
            raise TransportError(
                "Could not determine the authenticated TLS peer identity."
            ) from exc
        if authenticated_peer_id is not None and (
            not isinstance(authenticated_peer_id, str) or not authenticated_peer_id
        ):
            raise TransportError(
                "TLS peer identity resolver must return a non-empty string or None."
            )

    metadata: dict[str, Any] = {
        "peer_certificate_present": bool(binary_certificate),
    }
    version = ssl_object.version()
    if version is not None:
        metadata["tls_version"] = version
    cipher = ssl_object.cipher()
    if cipher is not None:
        metadata["cipher"] = cipher[0]
    if verified_server_name is not None and peer_authenticated:
        metadata["verified_server_name"] = verified_server_name

    return SecurityInfo(
        encrypted=True,
        authenticated=peer_authenticated,
        authenticated_peer_id=authenticated_peer_id,
        mechanism="tls",
        metadata=metadata,
    )


def _normalize_path(value: str | os.PathLike[str], name: str) -> str:
    try:
        path = os.fspath(value)
    except TypeError as exc:
        raise TypeError(f"{name} must be a filesystem path.") from exc
    if not isinstance(path, str) or not path:
        raise ValueError(f"{name} must be a non-empty filesystem path.")
    return path
