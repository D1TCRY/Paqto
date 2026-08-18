"""TLS configuration and established identity metadata for LAN streams."""

from __future__ import annotations

import math
import os
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from paqto.core.errors import TransportError
from paqto.core.security import SecurityInfo

TlsPeerIdentityResolver = Callable[[Mapping[str, Any]], str | None]
TlsCaData = str | bytes


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
        cadata: Optional PEM or DER trust data loaded directly from memory.
            When combined with ``cafile``, both sources are loaded.
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
    cadata: TlsCaData | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "certfile", _normalize_path(self.certfile, "certfile"))
        object.__setattr__(self, "keyfile", _normalize_path(self.keyfile, "keyfile"))
        if self.cafile is not None:
            object.__setattr__(self, "cafile", _normalize_path(self.cafile, "cafile"))
        if self.cadata is not None:
            _validate_cadata(self.cadata)

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
        _validate_handshake_timeout(self.handshake_timeout)

    def create_client_context(self) -> ssl.SSLContext:
        """Build a client context using system or configured trust roots."""
        if self.verify_peer:
            context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=self.cafile,
                cadata=self.cadata,
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
            if self.cafile is None and self.cadata is None:
                context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
            else:
                context.load_verify_locations(
                    cafile=self.cafile,
                    cadata=self.cadata,
                )
        else:
            context.verify_mode = ssl.CERT_NONE
        return context


@dataclass(frozen=True, slots=True)
class TlsContextConfig:
    """Caller-prepared TLS contexts and Paqto connection policy.

    This is the advanced alternative to :class:`TlsConfig`. Paqto uses both
    contexts unchanged and does not need to know how certificates, private
    keys, or trust anchors were provisioned. The client context determines
    outgoing verification and hostname checking. The server context determines
    whether incoming client certificates are optional or required.

    Args:
        client_context: Context used for outgoing TLS connections.
        server_context: Context used for incoming TLS connections.
        peer_identity_resolver: Optional mapping from an already verified peer
            certificate to a logical identity.
        handshake_timeout: Maximum TLS handshake duration in seconds.
    """

    client_context: ssl.SSLContext
    server_context: ssl.SSLContext
    peer_identity_resolver: TlsPeerIdentityResolver | None = None
    handshake_timeout: float = 10.0

    def __post_init__(self) -> None:
        if not isinstance(self.client_context, ssl.SSLContext):
            raise TypeError("client_context must be an ssl.SSLContext.")
        if not isinstance(self.server_context, ssl.SSLContext):
            raise TypeError("server_context must be an ssl.SSLContext.")
        if self.client_context.protocol == ssl.PROTOCOL_TLS_SERVER:
            raise ValueError("client_context cannot use the TLS server protocol.")
        if self.server_context.protocol == ssl.PROTOCOL_TLS_CLIENT:
            raise ValueError("server_context cannot use the TLS client protocol.")
        if self.peer_identity_resolver is not None and not callable(
            self.peer_identity_resolver
        ):
            raise TypeError("peer_identity_resolver must be callable.")
        _validate_handshake_timeout(self.handshake_timeout)

    @property
    def verify_peer(self) -> bool:
        """Whether the client context verifies outgoing peer certificates."""
        return self.client_context.verify_mode != ssl.CERT_NONE

    @property
    def check_hostname(self) -> bool:
        """Whether the client context verifies the outgoing endpoint name."""
        return self.client_context.check_hostname


def security_info_from_writer(
    writer: object,
    *,
    peer_authenticated: bool,
    identity_resolver: TlsPeerIdentityResolver | None,
    verified_server_name: str | None = None,
    peer_certificate_required: bool = True,
) -> SecurityInfo:
    """Build security metadata from an established asyncio TLS stream.

    The resolver is called only when ``peer_authenticated`` is true. Its result
    is an authenticated logical id only because the certificate was already
    verified; a certificate merely being present is not authentication.
    """
    get_extra_info = getattr(writer, "get_extra_info", None)
    if not callable(get_extra_info):
        raise TransportError("TLS stream does not expose connection metadata.")

    raw_ssl_object = get_extra_info("ssl_object")
    if raw_ssl_object is None:
        raise TransportError("TLS was configured but the stream is not encrypted.")
    ssl_object = cast(ssl.SSLObject | ssl.SSLSocket, raw_ssl_object)

    certificate = ssl_object.getpeercert()
    binary_certificate = ssl_object.getpeercert(binary_form=True)
    if peer_authenticated and (not certificate or not binary_certificate):
        if peer_certificate_required:
            raise TransportError(
                "TLS peer verification succeeded without an accessible peer "
                "certificate."
            )
        peer_authenticated = False

    authenticated_peer_id: str | None = None
    if peer_authenticated and identity_resolver is not None:
        if not isinstance(certificate, Mapping):
            raise TransportError("TLS peer certificate metadata is not a mapping.")
        try:
            decoded_certificate = cast(Mapping[str, Any], certificate)
            authenticated_peer_id = identity_resolver(dict(decoded_certificate))
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


def _validate_cadata(value: TlsCaData) -> None:
    if not isinstance(value, (str, bytes)):
        raise TypeError("cadata must be a string, bytes, or None.")
    if not value:
        raise ValueError("cadata must be non-empty when provided.")


def _validate_handshake_timeout(value: float) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError("handshake_timeout must be a number.")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("handshake_timeout must be finite and greater than zero.")
