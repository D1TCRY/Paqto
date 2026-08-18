from dataclasses import FrozenInstanceError

import pytest

import paqto
from paqto.connection import Connection as CompatibilityConnection
from paqto.core.connection import Connection
from paqto.core.endpoint import Endpoint
from paqto.core.security import SecurityInfo


class MinimalConnection(Connection):
    def __init__(self) -> None:
        self._endpoint = Endpoint(transport="test", address="test://local")
        self._closed = False

    @property
    def local_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def remote_endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def send_frame(self, data: bytes) -> None:
        return None

    async def receive_frame(self) -> bytes:
        return b""

    async def close(self) -> None:
        self._closed = True


def test_connection_defaults_to_no_security_guarantees() -> None:
    info = MinimalConnection().security_info

    assert info == SecurityInfo()
    assert info.encrypted is False
    assert info.authenticated is False
    assert info.authenticated_peer_id is None
    assert info.mechanism is None
    assert info.metadata == {}


def test_security_info_is_an_immutable_metadata_snapshot() -> None:
    metadata = {"cipher": "example"}
    info = SecurityInfo(
        encrypted=True,
        authenticated=True,
        authenticated_peer_id="peer-1",
        mechanism="custom",
        metadata=metadata,
    )
    metadata["cipher"] = "changed"

    assert info.metadata == {"cipher": "example"}
    with pytest.raises(TypeError):
        info.metadata["cipher"] = "changed"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        info.encrypted = False  # type: ignore[misc]


def test_connection_import_paths_resolve_to_the_same_async_abstraction() -> None:
    assert CompatibilityConnection is Connection
    assert paqto.Connection is Connection
    assert paqto.SecurityInfo is SecurityInfo
    assert paqto.ConnectionClosedError is paqto.core.ConnectionClosedError
