import json
from datetime import datetime, timezone
from typing import Any

from paqto.core.discovered import DiscoveredPeer
from paqto.core.endpoint import Endpoint
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
