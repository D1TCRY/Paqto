# Security model

Paqto separates transport security, protocol identity, and application
authorization. TLS can protect and authenticate a connection, the Paqto hello
binds a logical peer claim to that connection, and the application decides what
an authenticated peer is allowed to do.

Enabling TLS does not make unauthenticated discovery trustworthy and does not
make Paqto generally safe on a hostile network.

## Transport-neutral security metadata

Every `Connection` exposes an immutable `SecurityInfo` snapshot:

- `encrypted`: whether the transport guarantees channel encryption;
- `authenticated`: whether the remote endpoint was authenticated;
- `authenticated_peer_id`: optional logical identity derived from the
  authenticated mechanism;
- `mechanism`: mechanism name such as `"tls"`;
- `metadata`: immutable generic details.

The abstract default is `SecurityInfo()`, which makes no encryption or
authentication claim. Plain `TcpConnection` uses that default.

For TLS connections, metadata can include TLS version, cipher name, whether a
peer certificate is present, and the verified server name. It does not expose
certificate bodies or private key data.

## Enabling TLS

TLS is enabled only by passing `TlsConfig` to `LanTransport`:

```python
from paqto.lan import LanTransport, TlsConfig

tls = TlsConfig(
    certfile="/path/to/device-certificate.pem",
    keyfile="/path/to/device-private-key.pem",
    cafile="/path/to/trusted-ca.pem",
)
transport = LanTransport(tls=tls)
```

When TLS is configured:

- client-side certificate-chain verification defaults to enabled;
- client-side hostname or IP verification defaults to enabled;
- `cafile=None` uses Python's system trust roots;
- TLS 1.2 is the minimum accepted version;
- each direction presents the configured certificate because the same node
  certificate/key pair is loaded into client and server contexts;
- TLS handshake time is bounded independently from the Paqto hello.

Untrusted certificate chains and hostname mismatches fail connection
establishment. Missing or unreadable key material fails transport startup as
`TransportError`.

Disabling certificate verification requires both
`verify_peer=False` and `check_hostname=False`. This produces an encrypted but
unauthenticated outgoing stream; an identity resolver is not trusted or called
for that peer. `check_hostname=False` with `verify_peer=True` still validates
the certificate chain but does not verify that it names the endpoint host.

## Mutual TLS

Incoming TLS is encrypted whenever TLS is configured. It authenticates the
client only when `require_client_certificate=True`:

```python
tls = TlsConfig(
    certfile="/path/to/device-certificate.pem",
    keyfile="/path/to/device-private-key.pem",
    cafile="/path/to/trusted-ca.pem",
    require_client_certificate=True,
)
```

The server then uses `CERT_REQUIRED` and validates the client certificate
against `cafile` or system client-authentication roots. Without this option,
the server-side connection reports `encrypted=True` but
`authenticated=False`, even though the outgoing side may have authenticated
the server.

For nodes that can accept connections from one another and require identity
proof on every READY session, configure mTLS on every listener.

## Mapping a certificate to `Peer.id`

Certificate verification answers whether a certificate chains to a trusted
root and, for outgoing connections by default, names the endpoint host. It does
not define Paqto's logical `Peer.id`.

`peer_identity_resolver` receives Python's decoded mapping for an already
verified peer certificate and returns a non-empty string or `None`. Paqto does
not impose a subject, SAN, URI, or OID convention.

```python
from collections.abc import Mapping
from typing import Any


def peer_id_from_certificate(certificate: Mapping[str, Any]) -> str | None:
    prefix = "urn:example:paqto-peer:"
    for kind, value in certificate.get("subjectAltName", ()):
        if kind == "URI" and value.startswith(prefix):
            return value.removeprefix(prefix)
    return None
```

Resolver exceptions, wrong return types, or empty-string results fail the TLS
connection as `TransportError`. Paqto never copies an id from discovery into
`SecurityInfo.authenticated_peer_id`.

## Protocol identity binding

During the hello exchange, Paqto compares:

1. the intended discovered id, for an outgoing connection;
2. the hello's declared peer id;
3. `SecurityInfo.authenticated_peer_id`, when available.

Any disagreement raises `PeerIdentityMismatchError`, closes the connection,
and prevents READY. This consistency rule is enforced whenever an authenticated
id exists, even if strict mode is off.

`PaqtoConfig(require_authenticated_peer_id_match=True)` additionally requires
every READY session to have `security_info.authenticated=True` and a resolved
authenticated peer id. Missing proof raises `PeerAuthenticationError`.

`ProtocolSession.peer_id_authenticated` is true only when the authenticated id
exists and matches the hello. An authenticated certificate with no resolver can
therefore produce `SecurityInfo.authenticated=True` while the session's logical
peer id remains unauthenticated.

## Strict authenticated identity profile

A strict two-node setup uses verified TLS, mTLS in both directions, the same
deployment identity convention, and strict protocol binding:

```python
from paqto import PaqtoConfig, PaqtoNode
from paqto.lan import LanDiscovery, LanTransport, TlsConfig


def secure_node(peer_id: str, certfile: str, keyfile: str) -> PaqtoNode:
    tls = TlsConfig(
        certfile=certfile,
        keyfile=keyfile,
        cafile="/path/to/trusted-ca.pem",
        require_client_certificate=True,
        peer_identity_resolver=peer_id_from_certificate,
        handshake_timeout=10,
    )
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(tls=tls),
        discovery=LanDiscovery(),
        serializer=JsonSerializer(),
        config=PaqtoConfig(
            serializer_id="example/message-json-v1",
            require_authenticated_peer_id_match=True,
        ),
    )
```

Certificates must contain identities matching the configured `peer_id`, and
server certificates must also match the discovered endpoint host when hostname
verification is enabled. The repository's certificate files are public
loopback-only test fixtures and must never be reused in deployments.

This profile authenticates session identity. It still does not implement
application authorization. A handler must decide whether an authenticated peer
may perform a requested application action.

## Residual risks

- Plain TCP provides no confidentiality, integrity protection against an active
  intermediary, or peer authentication.
- UDP discovery is always unauthenticated and spoofable in the current
  implementation. Strict TLS stops a spoofed hint from becoming authenticated
  identity but does not stop redirection, connection churn, or cache pressure.
- TLS handshakes occur before `PaqtoNode.max_connections` admission. The finite
  TLS handshake timeout limits duration, but there is no built-in aggregate
  pre-TLS concurrency or rate limiter.
- Certificate issuance, storage, password handling, rotation, revocation,
  pinning, expiry monitoring, and trust-store changes are deployment concerns.
- Paqto does not add a control-frame nonce, transcript signature, application
  replay scheme, or proprietary cryptography.
- A safe authenticated channel cannot make an unsafe deserializer safe.
- TLS protects a channel, not availability, application correctness,
  authorization, durability, or exactly-once processing.

See [Production considerations](production.md) for deployment guidance.

