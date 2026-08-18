import asyncio
import json
import socket
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from paqto.core.discovered import DiscoveredPeer
from paqto.core.endpoint import Endpoint
from paqto.core.errors import DiscoveryError
from paqto.core.peer import Peer
from paqto.lan.discovery import PROTOCOL_VERSION, LanDiscovery


def _discovery_with_local_peer(peer_id: str = "local") -> LanDiscovery:
    discovery = LanDiscovery()
    discovery._local_peer = Peer(id=peer_id, name="Local")
    return discovery


def _announce_payload(
    *,
    peer_id: str = "remote",
    peer_name: str = "Remote",
    address: str = "tcp://127.0.0.1:5050",
    metadata: dict[str, Any] | None = None,
    endpoint_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "paqto": PROTOCOL_VERSION,
        "kind": "announce",
        "peer": {
            "id": peer_id,
            "name": peer_name,
            "metadata": {"role": "test"},
        },
        "endpoints": [
            {
                "transport": "lan",
                "address": address,
                "metadata": endpoint_metadata or {"zone": "lab"},
            }
        ],
        "metadata": metadata or {"source": "unit"},
    }


def _packet(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_valid_announce_builds_discovered_peer() -> None:
    discovery = _discovery_with_local_peer()

    discovery._datagram_received(_packet(_announce_payload()), ("127.0.0.1", 37020))

    discovered = discovery._discovered["remote"]
    assert isinstance(discovered, DiscoveredPeer)
    assert discovered.peer == Peer(
        id="remote",
        name="Remote",
        metadata={"role": "test"},
    )
    assert discovered.endpoints == [
        Endpoint(
            transport="lan",
            address="tcp://127.0.0.1:5050",
            metadata={"zone": "lab"},
        )
    ]
    assert discovered.metadata == {"source": "unit"}


def test_invalid_json_is_ignored() -> None:
    discovery = _discovery_with_local_peer()

    discovery._datagram_received(b"{not-json", ("127.0.0.1", 37020))

    assert discovery._discovered == {}


def test_duplicate_json_fields_are_ignored() -> None:
    discovery = _discovery_with_local_peer()
    packet = _packet(_announce_payload()).replace(
        b'{"paqto": 1',
        b'{"paqto": 1, "paqto": 1',
        1,
    )

    discovery._datagram_received(packet, ("127.0.0.1", 37020))

    assert discovery._discovered == {}


def test_recursively_nested_json_is_ignored() -> None:
    discovery = _discovery_with_local_peer()
    nested = "[" * 1100 + "0" + "]" * 1100
    packet = (
        '{"paqto":1,"kind":"announce","peer":{"id":"remote",'
        '"metadata":{"nested":'
        + nested
        + '}},"endpoints":[],"metadata":{}}'
    ).encode()

    discovery._datagram_received(packet, ("127.0.0.1", 37020))

    assert discovery._discovered == {}


def test_own_peer_id_is_ignored() -> None:
    discovery = _discovery_with_local_peer(peer_id="local")

    discovery._datagram_received(
        _packet(_announce_payload(peer_id="local")),
        ("127.0.0.1", 37020),
    )

    assert discovery._discovered == {}


def test_duplicate_peer_id_updates_existing_peer_and_last_seen() -> None:
    discovery = _discovery_with_local_peer()

    discovery._datagram_received(
        _packet(_announce_payload(address="tcp://127.0.0.1:5050")),
        ("127.0.0.1", 37020),
    )
    first = discovery._discovered["remote"]
    ancient = datetime(2000, 1, 1, tzinfo=timezone.utc)
    first.last_seen = ancient

    discovery._datagram_received(
        _packet(
            _announce_payload(
                peer_name="Remote updated",
                address="tcp://127.0.0.1:6060",
                metadata={"source": "refresh"},
                endpoint_metadata={"zone": "updated"},
            )
        ),
        ("127.0.0.1", 37020),
    )

    assert len(discovery._discovered) == 1
    assert discovery._discovered["remote"] is first
    assert first.peer.name == "Remote updated"
    assert first.endpoints[0].address == "tcp://127.0.0.1:6060"
    assert first.metadata == {"source": "refresh"}
    assert first.last_seen > ancient


def test_expired_peers_are_pruned_from_discovery_cache() -> None:
    discovery = _discovery_with_local_peer()
    discovery._datagram_received(
        _packet(_announce_payload()),
        ("127.0.0.1", 37020),
    )
    discovery._discovered["remote"].last_seen = (
        datetime.now(timezone.utc) - timedelta(seconds=61)
    )

    discovery._prune_expired()

    assert discovery._discovered == {}


def test_discovery_cache_has_bounded_peer_admission() -> None:
    discovery = LanDiscovery(max_discovered_peers=2)
    discovery._local_peer = Peer(id="local", name="Local")

    for peer_id in ("peer-1", "peer-2", "peer-3"):
        discovery._datagram_received(
            _packet(_announce_payload(peer_id=peer_id)),
            ("127.0.0.1", 37020),
        )

    assert set(discovery._discovered) == {"peer-1", "peer-2"}

    discovery._datagram_received(
        _packet(_announce_payload(peer_id="peer-1", peer_name="Updated")),
        ("127.0.0.1", 37020),
    )
    assert discovery._discovered["peer-1"].peer.name == "Updated"


@pytest.mark.asyncio
async def test_cancelled_start_closes_socket_and_rolls_back_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = LanDiscovery()
    entered = asyncio.Event()

    class FakeSocket:
        closed = False

        def close(self) -> None:
            self.closed = True

    sock = FakeSocket()

    async def delayed_endpoint(*args: Any, **kwargs: Any) -> None:
        entered.set()
        await asyncio.Future()

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(discovery, "_create_socket", lambda: sock)
    monkeypatch.setattr(loop, "create_datagram_endpoint", delayed_endpoint)

    starting = asyncio.create_task(
        discovery.start(Peer(id="local"), []),
    )
    await entered.wait()
    starting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await starting
    assert sock.closed is True
    assert discovery._local_peer is None
    assert discovery._endpoints == []
    assert discovery._transport is None


@pytest.mark.asyncio
async def test_socket_startup_errors_are_normalized_and_state_is_rolled_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = LanDiscovery()

    def fail_create_socket() -> socket.socket:
        raise OSError("bind failed")

    monkeypatch.setattr(discovery, "_create_socket", fail_create_socket)

    with pytest.raises(DiscoveryError) as captured:
        await discovery.start(
            Peer(id="local", name="Local"),
            [Endpoint(transport="lan", address="tcp://127.0.0.1:5050")],
        )

    assert isinstance(captured.value.__cause__, OSError)
    assert discovery._local_peer is None
    assert discovery._endpoints == []
