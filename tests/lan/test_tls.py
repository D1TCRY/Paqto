import asyncio
import json
import ssl
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from paqto.core import (
    DiscoveredPeer,
    Endpoint,
    Message,
    PaqtoConfig,
    PaqtoNode,
    Peer,
    PeerIdentityMismatchError,
    Serializer,
    TransportError,
)
from paqto.lan import LanDiscovery, LanTransport, TlsConfig, TlsContextConfig
from paqto.lan.address import parse_tcp_address

CERTIFICATES = Path(__file__).parent.parent / "certificates"
CA = CERTIFICATES / "ca.pem"
NODE_A_CERT = CERTIFICATES / "node-a.pem"
NODE_A_KEY = CERTIFICATES / "node-a-key.pem"
NODE_B_CERT = CERTIFICATES / "node-b.pem"
NODE_B_KEY = CERTIFICATES / "node-b-key.pem"
UNTRUSTED_CERT = CERTIFICATES / "untrusted.pem"
UNTRUSTED_KEY = CERTIFICATES / "untrusted-key.pem"
TEST_IDENTITY_PREFIX = "urn:test:peer:"


def _identity_from_test_uri(certificate: Mapping[str, Any]) -> str | None:
    for kind, value in certificate.get("subjectAltName", ()):
        if kind == "URI" and value.startswith(TEST_IDENTITY_PREFIX):
            return value.removeprefix(TEST_IDENTITY_PREFIX)
    return None


def _tls(
    certificate: Path,
    key: Path,
    *,
    require_client_certificate: bool = False,
) -> TlsConfig:
    return TlsConfig(
        certfile=certificate,
        keyfile=key,
        cafile=CA,
        require_client_certificate=require_client_certificate,
        peer_identity_resolver=_identity_from_test_uri,
    )


def _prepared_tls_contexts(
    certificate: Path,
    key: Path,
    *,
    require_client_certificate: bool = False,
) -> TlsContextConfig:
    ca_data = CA.read_text(encoding="ascii")
    client_context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cadata=ca_data,
    )
    client_context.minimum_version = ssl.TLSVersion.TLSv1_2
    client_context.load_cert_chain(certificate, key)

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.minimum_version = ssl.TLSVersion.TLSv1_2
    server_context.load_cert_chain(certificate, key)
    if require_client_certificate:
        server_context.verify_mode = ssl.CERT_REQUIRED
        server_context.load_verify_locations(cadata=ca_data)

    return TlsContextConfig(
        client_context=client_context,
        server_context=server_context,
        peer_identity_resolver=_identity_from_test_uri,
    )


async def _open_tls_pair(
    *,
    mutual_tls: bool = False,
) -> tuple[LanTransport, LanTransport, Any, Any, Any]:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(
            NODE_B_CERT,
            NODE_B_KEY,
            require_client_certificate=mutual_tls,
        ),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(NODE_A_CERT, NODE_A_KEY),
    )
    await server_transport.start()
    await client_transport.start()
    listener = await server_transport.create_listener()
    await listener.start()
    client = await client_transport.connect(listener.local_endpoint, timeout=2)
    server = await asyncio.wait_for(listener.accept(), timeout=2)
    return server_transport, client_transport, listener, client, server


async def _stop_transports(*transports: LanTransport) -> None:
    await asyncio.wait_for(
        asyncio.gather(
            *(transport.stop() for transport in transports),
            return_exceptions=True,
        ),
        timeout=2,
    )


@pytest.mark.asyncio
async def test_tls_connection_is_verified_and_transports_data() -> None:
    server_transport, client_transport, listener, client, server = (
        await _open_tls_pair()
    )

    try:
        await client.send_frame(b"encrypted frame")
        assert await server.receive_frame() == b"encrypted frame"

        client_security = client.security_info
        assert client_security.encrypted is True
        assert client_security.authenticated is True
        assert client_security.authenticated_peer_id == "node-b"
        assert client_security.mechanism == "tls"
        assert client_security.metadata["verified_server_name"] == "127.0.0.1"
        assert str(client_security.metadata["tls_version"]).startswith("TLSv1.")

        server_security = server.security_info
        assert server_security.encrypted is True
        assert server_security.authenticated is False
        assert server_security.authenticated_peer_id is None
    finally:
        await _stop_transports(client_transport, server_transport)

    assert client.is_closed
    assert server.is_closed
    assert listener._server is None


@pytest.mark.asyncio
async def test_caller_preconfigured_contexts_support_mtls_identity_and_close() -> None:
    server_contexts = _prepared_tls_contexts(
        NODE_B_CERT,
        NODE_B_KEY,
        require_client_certificate=True,
    )
    client_contexts = _prepared_tls_contexts(NODE_A_CERT, NODE_A_KEY)
    server_transport = LanTransport(
        host="127.0.0.1",
        tls_contexts=server_contexts,
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls_contexts=client_contexts,
    )

    await server_transport.start()
    await client_transport.start()
    assert server_transport._server_ssl_context is server_contexts.server_context
    assert client_transport._client_ssl_context is client_contexts.client_context
    listener = await server_transport.create_listener()
    await listener.start()
    client = await client_transport.connect(listener.local_endpoint, timeout=2)
    server = await asyncio.wait_for(listener.accept(), timeout=2)

    try:
        assert client.security_info.authenticated_peer_id == "node-b"
        assert server.security_info.authenticated_peer_id == "node-a"
        assert client.security_info.metadata["verified_server_name"] == "127.0.0.1"
        await client.send_frame(b"caller contexts")
        assert await server.receive_frame() == b"caller contexts"
    finally:
        await _stop_transports(client_transport, server_transport)

    assert client.is_closed
    assert server.is_closed
    assert listener._server is None
    assert client_transport._client_ssl_context is None
    assert server_transport._server_ssl_context is None


@pytest.mark.asyncio
async def test_high_level_tls_accepts_ca_data_without_a_ca_file() -> None:
    ca_data = CA.read_text(encoding="ascii")
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=TlsConfig(
            certfile=NODE_B_CERT,
            keyfile=NODE_B_KEY,
            cadata=ca_data,
            require_client_certificate=True,
            peer_identity_resolver=_identity_from_test_uri,
        ),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls=TlsConfig(
            certfile=NODE_A_CERT,
            keyfile=NODE_A_KEY,
            cadata=ca_data,
            peer_identity_resolver=_identity_from_test_uri,
        ),
    )

    try:
        await server_transport.start()
        await client_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()
        client = await client_transport.connect(listener.local_endpoint, timeout=2)
        server = await asyncio.wait_for(listener.accept(), timeout=2)

        assert client.security_info.authenticated_peer_id == "node-b"
        assert server.security_info.authenticated_peer_id == "node-a"
    finally:
        await _stop_transports(client_transport, server_transport)


def test_file_and_preconfigured_tls_modes_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        LanTransport(
            tls=_tls(NODE_A_CERT, NODE_A_KEY),
            tls_contexts=_prepared_tls_contexts(NODE_A_CERT, NODE_A_KEY),
        )


def test_tls_context_configuration_rejects_wrong_context_roles() -> None:
    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    with pytest.raises(ValueError, match="client_context"):
        TlsContextConfig(
            client_context=server_context,
            server_context=server_context,
        )
    with pytest.raises(ValueError, match="server_context"):
        TlsContextConfig(
            client_context=client_context,
            server_context=client_context,
        )


@pytest.mark.parametrize("cadata", ["", b"", object()])
def test_tls_configuration_rejects_invalid_ca_data(cadata: object) -> None:
    expected = ValueError if isinstance(cadata, (str, bytes)) else TypeError
    with pytest.raises(expected):
        TlsConfig(
            certfile=NODE_A_CERT,
            keyfile=NODE_A_KEY,
            cadata=cadata,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_untrusted_server_certificate_is_rejected() -> None:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=TlsConfig(
            certfile=UNTRUSTED_CERT,
            keyfile=UNTRUSTED_KEY,
            cafile=CA,
        ),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(NODE_A_CERT, NODE_A_KEY),
    )

    try:
        await server_transport.start()
        await client_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()

        with pytest.raises(TransportError) as captured:
            await client_transport.connect(listener.local_endpoint, timeout=2)

        assert isinstance(captured.value.__cause__, ssl.SSLCertVerificationError)
    finally:
        await _stop_transports(client_transport, server_transport)


@pytest.mark.asyncio
async def test_preconfigured_client_context_rejects_untrusted_server() -> None:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls_contexts=_prepared_tls_contexts(UNTRUSTED_CERT, UNTRUSTED_KEY),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls_contexts=_prepared_tls_contexts(NODE_A_CERT, NODE_A_KEY),
    )

    try:
        await server_transport.start()
        await client_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()

        with pytest.raises(TransportError) as captured:
            await client_transport.connect(listener.local_endpoint, timeout=2)

        assert isinstance(captured.value.__cause__, ssl.SSLCertVerificationError)
    finally:
        await _stop_transports(client_transport, server_transport)


@pytest.mark.asyncio
async def test_certificate_hostname_mismatch_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(NODE_B_CERT, NODE_B_KEY),
    )
    client_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(NODE_A_CERT, NODE_A_KEY),
    )

    try:
        await server_transport.start()
        await client_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()
        port = parse_tcp_address(listener.local_endpoint.address).port
        loop = asyncio.get_running_loop()
        original_getaddrinfo = loop.getaddrinfo

        async def loopback_getaddrinfo(
            host: str,
            port: int,
            *args: Any,
            **kwargs: Any,
        ) -> list[Any]:
            resolved_host = "127.0.0.1" if host == "not-certified.invalid" else host
            return await original_getaddrinfo(resolved_host, port, *args, **kwargs)

        monkeypatch.setattr(loop, "getaddrinfo", loopback_getaddrinfo)
        mismatched = Endpoint(
            transport="lan",
            address=f"tcp://not-certified.invalid:{port}",
        )

        with pytest.raises(TransportError) as captured:
            await client_transport.connect(mismatched, timeout=2)

        assert isinstance(captured.value.__cause__, ssl.SSLCertVerificationError)
    finally:
        await _stop_transports(client_transport, server_transport)


@pytest.mark.asyncio
async def test_mutual_tls_authenticates_both_peers() -> None:
    server_transport, client_transport, _, client, server = await _open_tls_pair(
        mutual_tls=True
    )

    try:
        assert client.security_info.authenticated is True
        assert client.security_info.authenticated_peer_id == "node-b"
        assert server.security_info.authenticated is True
        assert server.security_info.authenticated_peer_id == "node-a"

        await server.send_frame(b"mutual tls")
        assert await client.receive_frame() == b"mutual tls"
    finally:
        await _stop_transports(client_transport, server_transport)


@pytest.mark.asyncio
async def test_server_requiring_client_certificate_rejects_missing_certificate() -> None:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=_tls(
            NODE_B_CERT,
            NODE_B_KEY,
            require_client_certificate=True,
        ),
    )
    writer: asyncio.StreamWriter | None = None

    try:
        await server_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()
        address = parse_tcp_address(listener.local_endpoint.address)
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA)

        try:
            reader, writer = await asyncio.open_connection(
                address.host,
                address.port,
                ssl=context,
                server_hostname=address.host,
            )
        except (ConnectionError, OSError, ssl.SSLError):
            pass
        else:
            writer.write(b"not a framed message")
            try:
                await writer.drain()
                assert await asyncio.wait_for(reader.read(1), timeout=1) == b""
            except (ConnectionError, OSError, ssl.SSLError):
                pass

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(listener.accept(), timeout=0.2)
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, ssl.SSLError):
                pass
        await _stop_transports(server_transport)


@pytest.mark.asyncio
async def test_invalid_tls_material_fails_start_without_starting_transport(
) -> None:
    transport = LanTransport(
        host="127.0.0.1",
        tls=TlsConfig(
            certfile=CERTIFICATES / "missing-cert.pem",
            keyfile=CERTIFICATES / "missing-key.pem",
            cafile=CA,
        ),
    )

    with pytest.raises(TransportError) as captured:
        await transport.start()

    assert isinstance(captured.value.__cause__, OSError)
    with pytest.raises(TransportError, match="must be started"):
        await transport.create_listener()
    await transport.stop()


def test_tls_configuration_requires_explicit_verification_opt_out() -> None:
    with pytest.raises(ValueError, match="check_hostname"):
        TlsConfig(
            certfile=NODE_A_CERT,
            keyfile=NODE_A_KEY,
            verify_peer=False,
        )

    config = TlsConfig(
        certfile=NODE_A_CERT,
        keyfile=NODE_A_KEY,
        verify_peer=False,
        check_hostname=False,
    )
    context = config.create_client_context()
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False

    with pytest.raises(ValueError, match="handshake_timeout"):
        TlsConfig(
            certfile=NODE_A_CERT,
            keyfile=NODE_A_KEY,
            handshake_timeout=0,
        )


def test_tls_configuration_preserves_existing_positional_argument_order() -> None:
    config = TlsConfig(
        NODE_A_CERT,
        NODE_A_KEY,
        CA,
        False,
        False,
    )

    assert config.verify_peer is False
    assert config.check_hostname is False
    assert config.cadata is None


@pytest.mark.asyncio
async def test_incoming_tls_handshake_timeout_closes_slow_client() -> None:
    server_transport = LanTransport(
        host="127.0.0.1",
        tls=TlsConfig(
            certfile=NODE_B_CERT,
            keyfile=NODE_B_KEY,
            cafile=CA,
            handshake_timeout=0.05,
        ),
    )
    writer: asyncio.StreamWriter | None = None
    try:
        await server_transport.start()
        listener = await server_transport.create_listener()
        await listener.start()
        address = parse_tcp_address(listener.local_endpoint.address)
        reader, writer = await asyncio.open_connection(address.host, address.port)

        assert await asyncio.wait_for(reader.read(), timeout=1) == b""
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(listener.accept(), timeout=0.1)
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        await _stop_transports(server_transport)


class _UnusedSerializer(Serializer):
    def serialize(self, message: Message) -> bytes:
        return json.dumps(message.payload).encode()

    def deserialize(self, data: bytes) -> Message:
        return Message(payload=json.loads(data.decode()))


def _secure_node(
    *,
    peer_id: str,
    certificate: Path,
    key: Path,
    require_identity_match: bool,
    require_client_certificate: bool = False,
) -> PaqtoNode:
    return PaqtoNode(
        name=peer_id,
        peer_id=peer_id,
        transport=LanTransport(
            host="127.0.0.1",
            tls=_tls(
                certificate,
                key,
                require_client_certificate=require_client_certificate,
            ),
        ),
        discovery=LanDiscovery(
            discovery_port=0,
            broadcast_host="127.0.0.1",
            announce_interval=3600,
            default_discover_timeout=0,
        ),
        serializer=_UnusedSerializer(),
        config=PaqtoConfig(
            connect_timeout=2,
            discover_timeout=0,
            require_authenticated_peer_id_match=require_identity_match,
        ),
    )


@pytest.mark.asyncio
async def test_discovery_and_authenticated_identity_mismatch_is_rejected() -> None:
    source = _secure_node(
        peer_id="node-a",
        certificate=NODE_A_CERT,
        key=NODE_A_KEY,
        require_identity_match=True,
    )
    target = _secure_node(
        peer_id="node-b",
        certificate=NODE_B_CERT,
        key=NODE_B_KEY,
        require_identity_match=False,
    )

    try:
        await source.start()
        await target.start()
        assert target._listener is not None
        endpoint = target._listener.local_endpoint
        declared_peer = Peer(
            id="discovery-claimed-node",
            name="untrusted discovery claim",
        )
        discovered = DiscoveredPeer(
            peer=declared_peer,
            endpoints=[
                Endpoint(
                    transport=endpoint.transport,
                    address=endpoint.address,
                    metadata=dict(endpoint.metadata),
                )
            ],
        )

        with pytest.raises(PeerIdentityMismatchError, match="does not match"):
            await source.connect(discovered)

        assert source._connections.get(declared_peer) is None
    finally:
        await asyncio.gather(source.stop(), target.stop(), return_exceptions=True)


@pytest.mark.asyncio
async def test_mutual_tls_and_protocol_handshake_authenticate_ready_session() -> None:
    source = _secure_node(
        peer_id="node-a",
        certificate=NODE_A_CERT,
        key=NODE_A_KEY,
        require_identity_match=True,
    )
    target = _secure_node(
        peer_id="node-b",
        certificate=NODE_B_CERT,
        key=NODE_B_KEY,
        require_identity_match=True,
        require_client_certificate=True,
    )

    try:
        await source.start()
        await target.start()
        assert target._listener is not None
        discovered = DiscoveredPeer(
            peer=target.peer,
            endpoints=[target._listener.local_endpoint],
        )

        connection = await source.connect(discovered)
        source_session = source.session_for(connection)
        assert source_session is not None
        assert source_session.peer_id == "node-b"
        assert source_session.peer_id_authenticated is True

        async def incoming_session_ready() -> bool:
            while not target._sessions:
                await asyncio.sleep(0)
            return True

        await asyncio.wait_for(incoming_session_ready(), timeout=1)
        target_session = next(iter(target._sessions.values()))
        assert target_session.peer_id == "node-a"
        assert target_session.peer_id_authenticated is True
    finally:
        await asyncio.gather(source.stop(), target.stop(), return_exceptions=True)


@pytest.mark.asyncio
async def test_tls_identity_cannot_contradict_protocol_handshake_identity() -> None:
    source = _secure_node(
        peer_id="node-a",
        certificate=NODE_A_CERT,
        key=NODE_A_KEY,
        require_identity_match=False,
    )
    target = _secure_node(
        peer_id="handshake-alias",
        certificate=NODE_B_CERT,
        key=NODE_B_KEY,
        require_identity_match=False,
    )

    try:
        await source.start()
        await target.start()
        assert target._listener is not None
        discovered = DiscoveredPeer(
            peer=target.peer,
            endpoints=[target._listener.local_endpoint],
        )

        with pytest.raises(PeerIdentityMismatchError, match="authenticated"):
            await source.connect(discovered)

        assert source._connections.get(target.peer) is None
        assert source._sessions == {}
    finally:
        await asyncio.gather(source.stop(), target.stop(), return_exceptions=True)
