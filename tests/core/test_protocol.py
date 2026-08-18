import asyncio
import json

import pytest

from paqto.core.config import PaqtoConfig
from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.errors import (
    ConnectionClosedError,
    PeerIdentityMismatchError,
    ProtocolFrameError,
    ProtocolHandshakeError,
    ProtocolHandshakeTimeoutError,
    ProtocolVersionError,
)
from paqto.core.protocol import (
    PROTOCOL_MAGIC,
    HandshakeOffer,
    HeartbeatPing,
    HeartbeatPong,
    TechnicalAcknowledgement,
    decode_application_frame,
    decode_session_frame,
    encode_acknowledgement_frame,
    encode_application_frame,
    encode_ping_frame,
    encode_pong_frame,
    negotiate_protocol,
)
from paqto.core.security import SecurityInfo


class ScriptedConnection(Connection):
    def __init__(
        self,
        incoming: bytes | BaseException | None,
        *,
        security_info: SecurityInfo | None = None,
    ) -> None:
        self.incoming = incoming
        self.sent: list[bytes] = []
        self.closed = False
        self.receive_entered = asyncio.Event()
        self.release_receive = asyncio.Event()
        self._security_info = security_info or SecurityInfo()
        self._endpoint = Endpoint(transport="test", address="test://peer")

    @property
    def local_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def remote_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def is_closed(self) -> bool:
        return self.closed

    @property
    def security_info(self) -> SecurityInfo:
        return self._security_info

    async def send_frame(self, data: bytes) -> None:
        self.sent.append(data)

    async def receive_frame(self) -> bytes:
        self.receive_entered.set()
        if self.incoming is None:
            await self.release_receive.wait()
            raise AssertionError("unreachable")
        if isinstance(self.incoming, BaseException):
            raise self.incoming
        return self.incoming

    async def close(self) -> None:
        self.closed = True
        self.release_receive.set()


def _hello(
    peer_id: str,
    *,
    version: int = 1,
    serializer: str = "test-json",
) -> bytes:
    return b"\x00" + json.dumps(
        {
            "magic": PROTOCOL_MAGIC,
            "type": "hello",
            "version": version,
            "peer_id": peer_id,
            "capabilities": ["common", "remote-only"],
            "serializer": serializer,
            "max_message_size": 1024,
            "metadata": {"implementation": "test"},
        }
    ).encode()


def _offer() -> HandshakeOffer:
    return HandshakeOffer(
        peer_id="local-peer",
        serializer_id="test-json",
        capabilities=("local-only", "common"),
        max_message_size=2048,
    )


@pytest.mark.asyncio
async def test_valid_handshake_negotiates_ready_session_without_authentication(
) -> None:
    connection = ScriptedConnection(_hello("remote-peer"))

    session = await negotiate_protocol(
        connection,
        _offer(),
        timeout=1,
        expected_peer_id="remote-peer",
    )

    assert session.peer_id == "remote-peer"
    assert session.version == 1
    assert session.capabilities == ("common",)
    assert session.max_message_size == 1024
    assert session.peer_id_authenticated is False
    assert session.metadata == {"implementation": "test"}
    assert connection.sent[0].startswith(b"\x00")
    assert connection.closed is False


@pytest.mark.asyncio
async def test_incompatible_protocol_version_is_specific_and_closes() -> None:
    connection = ScriptedConnection(_hello("remote-peer", version=2))

    with pytest.raises(ProtocolVersionError, match="local 1, remote 2"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_authenticated_identity_must_match_handshake_peer_id() -> None:
    connection = ScriptedConnection(
        _hello("declared-peer"),
        security_info=SecurityInfo(
            encrypted=True,
            authenticated=True,
            authenticated_peer_id="authenticated-peer",
            mechanism="test-security",
        ),
    )

    with pytest.raises(PeerIdentityMismatchError, match="authenticated"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_malformed_handshake_is_rejected_and_closes() -> None:
    connection = ScriptedConnection(b"\x00not-json")

    with pytest.raises(ProtocolHandshakeError, match="Malformed"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_recursively_nested_handshake_is_normalized_and_closes() -> None:
    nested = "[" * 1100 + "0" + "]" * 1100
    frame = (
        b'\x00{"magic":"PAQTO","type":"hello","version":1,'
        b'"peer_id":"remote","capabilities":[],"serializer":"test-json",'
        b'"max_message_size":1024,"metadata":{"nested":'
        + nested.encode()
        + b"}}"
    )
    connection = ScriptedConnection(frame)

    with pytest.raises(ProtocolHandshakeError, match="Malformed"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_duplicate_handshake_fields_are_rejected_and_close() -> None:
    frame = _hello("remote-peer").replace(
        b'"peer_id": "remote-peer"',
        b'"peer_id": "remote-peer", "peer_id": "remote-peer"',
    )
    connection = ScriptedConnection(frame)

    with pytest.raises(ProtocolHandshakeError, match="Malformed"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_pathologically_large_json_integer_is_normalized_and_closes() -> None:
    huge_integer = b"9" * 5000
    frame = _hello("remote-peer").replace(
        b'"metadata": {"implementation": "test"}',
        b'"metadata": {"huge": ' + huge_integer + b"}",
    )
    connection = ScriptedConnection(frame)

    with pytest.raises(ProtocolHandshakeError, match="Malformed"):
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_non_standard_json_numbers_are_rejected_in_local_handshake() -> None:
    connection = ScriptedConnection(_hello("remote-peer"))
    offer = HandshakeOffer(
        peer_id="local-peer",
        serializer_id="test-json",
        metadata={"not_json": float("nan")},
    )

    with pytest.raises(ProtocolHandshakeError, match="not valid control data"):
        await negotiate_protocol(connection, offer, timeout=1)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_handshake_timeout_is_specific_and_closes() -> None:
    connection = ScriptedConnection(None)

    with pytest.raises(ProtocolHandshakeTimeoutError, match="Timed out"):
        await negotiate_protocol(connection, _offer(), timeout=0.01)

    assert connection.closed is True


@pytest.mark.asyncio
async def test_remote_close_during_handshake_is_normalized() -> None:
    connection = ScriptedConnection(
        ConnectionClosedError("remote closed"),
    )

    with pytest.raises(ProtocolHandshakeError, match="closed") as captured:
        await negotiate_protocol(connection, _offer(), timeout=1)

    assert isinstance(captured.value.__cause__, ConnectionClosedError)
    assert connection.closed is True


def test_application_and_control_frames_are_distinct() -> None:
    frame = encode_application_frame(b"payload", max_message_size=7)
    assert decode_application_frame(frame, max_message_size=7) == b"payload"

    with pytest.raises(ProtocolFrameError, match="control"):
        decode_application_frame(_hello("remote-peer"), max_message_size=1024)

    with pytest.raises(ProtocolFrameError, match="exceeds"):
        encode_application_frame(b"too large", max_message_size=2)


def test_technical_ack_control_frame_is_decoded_without_application_data() -> None:
    frame = encode_acknowledgement_frame("message-123")

    decoded = decode_session_frame(frame, max_message_size=1024)

    assert decoded == TechnicalAcknowledgement("message-123")
    with pytest.raises(ProtocolFrameError, match="control"):
        decode_application_frame(frame, max_message_size=1024)


def test_heartbeat_control_frames_are_not_application_data() -> None:
    ping = encode_ping_frame("ping-1")
    pong = encode_pong_frame("ping-1")

    assert decode_session_frame(ping, max_message_size=1024) == HeartbeatPing(
        "ping-1"
    )
    assert decode_session_frame(pong, max_message_size=1024) == HeartbeatPong(
        "ping-1"
    )
    with pytest.raises(ProtocolFrameError, match="control"):
        decode_application_frame(ping, max_message_size=1024)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_handshake_timeout_configuration_must_be_positive_and_finite(
    timeout: float,
) -> None:
    with pytest.raises(ValueError, match="handshake_timeout"):
        PaqtoConfig(handshake_timeout=timeout)
