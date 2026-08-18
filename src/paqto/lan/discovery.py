"""Unauthenticated IPv4 UDP broadcast discovery for LAN peers."""

from __future__ import annotations

import asyncio
import json
import math
import socket
from collections.abc import Mapping, Sequence
from typing import Any, cast

from paqto.core.discovered import DiscoveredPeer
from paqto.core.discovery import DiscoveryService
from paqto.core.endpoint import Endpoint
from paqto.core.errors import DiscoveryError, TransportError
from paqto.core.peer import Peer
from paqto.lan.address import TRANSPORT_NAME, parse_tcp_address

PROTOCOL_VERSION = 1
DEFAULT_DISCOVERY_PORT = 37020
DEFAULT_BROADCAST_HOST = "255.255.255.255"
DEFAULT_BIND_HOST = "0.0.0.0"
DEFAULT_ANNOUNCE_INTERVAL = 5.0
DEFAULT_DISCOVER_TIMEOUT = 1.0
DEFAULT_PEER_TTL = 60.0
DEFAULT_MAX_DISCOVERED_PEERS = 1024
MAX_UDP_DATAGRAM_SIZE = 65_507
MAX_DISCOVER_TIMEOUT_GUARD = 0.1
MAX_JSON_NESTING = 32
MAX_JSON_INTEGER_BITS = 4096

SocketAddress = tuple[str, int]


class LanDiscovery(DiscoveryService):
    """UDP broadcast discovery for Paqto peers on an IPv4 LAN.

    ``start()`` binds one UDP socket, enables broadcast, and begins sending
    periodic ``announce`` packets containing the local peer and its LAN
    endpoints. ``discover()`` requires the service to be started: it broadcasts
    a ``discover`` packet, waits for a bounded collection window, and returns
    the current cache of discovered peers. When ``timeout`` is ``None`` a short
    default collection window is used.

    Malformed incoming packets are ignored. Valid announces update the cache by
    ``peer.id`` and refresh ``last_seen`` with :meth:`DiscoveredPeer.touch`.

    Announcements are unauthenticated reachability hints. A matching peer id in
    discovery and the Paqto hello is still only a declared identity; transport
    security must establish any authenticated identity.

    Args:
        discovery_port: UDP port used to bind and send discovery packets.
        bind_host: Local IPv4 address used for the UDP socket.
        broadcast_host: IPv4 broadcast destination.
        announce_interval: Seconds between periodic announcements.
        default_discover_timeout: Default collection budget in seconds.
        metadata: Generic JSON-safe metadata included in announcements.
        max_datagram_size: Maximum accepted and emitted UDP payload bytes.
        peer_ttl: Cache lifetime in seconds, or ``None`` to disable expiry.
        max_discovered_peers: Maximum distinct declared peer ids cached at once.
    """

    def __init__(
        self,
        *,
        discovery_port: int = DEFAULT_DISCOVERY_PORT,
        bind_host: str = DEFAULT_BIND_HOST,
        broadcast_host: str = DEFAULT_BROADCAST_HOST,
        announce_interval: float = DEFAULT_ANNOUNCE_INTERVAL,
        default_discover_timeout: float = DEFAULT_DISCOVER_TIMEOUT,
        metadata: Mapping[str, Any] | None = None,
        max_datagram_size: int = MAX_UDP_DATAGRAM_SIZE,
        peer_ttl: float | None = DEFAULT_PEER_TTL,
        max_discovered_peers: int = DEFAULT_MAX_DISCOVERED_PEERS,
    ) -> None:
        _validate_port(discovery_port, "discovery_port")
        _validate_positive_float(announce_interval, "announce_interval")
        _validate_non_negative_float(
            default_discover_timeout,
            "default_discover_timeout",
        )
        _validate_max_datagram_size(max_datagram_size)
        if peer_ttl is not None:
            _validate_positive_float(peer_ttl, "peer_ttl")
        _validate_positive_integer(max_discovered_peers, "max_discovered_peers")

        self._discovery_port = discovery_port
        self._bind_host = bind_host
        self._broadcast_host = broadcast_host
        self._announce_interval = announce_interval
        self._default_discover_timeout = default_discover_timeout
        self._metadata = dict(metadata or {})
        self._max_datagram_size = max_datagram_size
        self._peer_ttl = peer_ttl
        self._max_discovered_peers = max_discovered_peers

        self._local_peer: Peer | None = None
        self._endpoints: list[Endpoint] = []
        self._discovered: dict[str, DiscoveredPeer] = {}
        self._transport: asyncio.DatagramTransport | None = None
        self._announce_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()

    async def start(self, local_peer: Peer, endpoints: Sequence[Endpoint]) -> None:
        """Start listening for discovery packets and announcing ``local_peer``.

        Only valid LAN endpoints using ``tcp://HOST:PORT`` addresses are
        published. Invalid local endpoints are skipped so that discovery remains
        separate from transport setup.
        """
        async with self._lifecycle_lock:
            await self._start(local_peer, endpoints)

    async def _start(
        self,
        local_peer: Peer,
        endpoints: Sequence[Endpoint],
    ) -> None:
        """Validate announcement state and create the UDP endpoint atomically."""
        if self._transport is not None:
            raise DiscoveryError("LAN discovery is already started.")

        peer = _copy_peer(local_peer)
        if not peer.id:
            raise DiscoveryError("LAN discovery requires a non-empty local peer id.")

        self._local_peer = peer
        self._endpoints = [
            copied
            for endpoint in endpoints
            if (copied := _copy_valid_endpoint(endpoint)) is not None
        ]

        try:
            self._ensure_announce_is_serializable()
        except DiscoveryError:
            self._clear_local_state()
            raise

        try:
            sock = self._create_socket()
        except OSError as exc:
            self._clear_local_state()
            raise DiscoveryError(
                f"Could not start LAN discovery on UDP port {self._discovery_port}."
            ) from exc

        try:
            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _LanDiscoveryProtocol(self),
                sock=sock,
            )
        except OSError as exc:
            sock.close()
            self._clear_local_state()
            raise DiscoveryError(
                f"Could not start LAN discovery on UDP port {self._discovery_port}."
            ) from exc
        except BaseException:
            sock.close()
            self._clear_local_state()
            raise

        self._transport = cast(asyncio.DatagramTransport, transport)
        self._announce_task = asyncio.create_task(self._announce_periodically())

    async def stop(self) -> None:
        """Stop UDP discovery, close the socket, and clear internal state.

        The method is idempotent and may be called safely even if discovery was
        never started or has already been stopped.
        """
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self) -> None:
        """Cancel announcements, close the socket, and clear volatile caches."""
        task = self._announce_task
        self._announce_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        transport = self._transport
        self._transport = None
        if transport is not None:
            transport.close()

        self._clear_local_state()
        self._discovered.clear()

    async def discover(self, *, timeout: float | None = None) -> list[DiscoveredPeer]:
        """Broadcast a discovery request and return the updated peer cache.

        The service must already be started, otherwise :class:`DiscoveryError`
        is raised. ``timeout`` is treated as the maximum collection budget; when
        it is ``None`` a short default window is used. Peers discovered before
        this call may be returned too, with any fresh announces applied before
        the result list is produced.
        """
        self._ensure_started()
        self._send_discover()

        window = self._collection_window(timeout)
        if window > 0:
            await asyncio.sleep(window)

        self._prune_expired()
        return list(self._discovered.values())

    def _create_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind((self._bind_host, self._discovery_port))
            sock.setblocking(False)
        except OSError:
            sock.close()
            raise
        return sock

    async def _announce_periodically(self) -> None:
        while True:
            try:
                self._send_announce()
            except DiscoveryError:
                pass
            await asyncio.sleep(self._announce_interval)

    def _datagram_received(self, data: bytes, addr: Any) -> None:
        """Validate and route one untrusted discovery datagram."""
        if len(data) > self._max_datagram_size:
            return

        payload = self._decode_packet(data)
        if payload is None:
            return

        kind = payload.get("kind")
        if kind == "discover":
            self._handle_discover(payload, addr)
        elif kind == "announce":
            self._handle_announce(payload)

    def _handle_discover(self, payload: dict[str, Any], addr: Any) -> None:
        """Answer a valid non-local discovery request with an announcement."""
        local_peer = self._local_peer
        if local_peer is None:
            return

        peer_id = payload.get("peer_id")
        if not isinstance(peer_id, str) or peer_id == local_peer.id:
            return
        if not _is_socket_address(addr):
            return

        try:
            self._send_announce(addr)
        except DiscoveryError:
            pass

    def _handle_announce(self, payload: dict[str, Any]) -> None:
        """Validate and cache one untrusted announcement within admission limits."""
        local_peer = self._local_peer
        if local_peer is None:
            return

        peer = self._parse_peer(payload.get("peer"))
        if peer is None or peer.id == local_peer.id:
            return

        endpoints = self._parse_endpoints(payload.get("endpoints"))
        if endpoints is None:
            return

        metadata = _optional_dict(payload.get("metadata"))
        if metadata is None:
            return

        discovered = self._discovered.get(peer.id)
        if discovered is None:
            self._prune_expired()
            if len(self._discovered) >= self._max_discovered_peers:
                return
            self._discovered[peer.id] = DiscoveredPeer(
                peer=peer,
                endpoints=endpoints,
                metadata=metadata,
            )
            return

        discovered.peer = peer
        discovered.endpoints = endpoints
        discovered.metadata = metadata
        discovered.touch()

    def _prune_expired(self) -> None:
        """Remove observations older than the configured discovery cache TTL."""
        if self._peer_ttl is None:
            return
        for peer_id, discovered in list(self._discovered.items()):
            if not discovered.is_fresh(self._peer_ttl):
                self._discovered.pop(peer_id, None)

    def _parse_peer(self, value: Any) -> Peer | None:
        """Parse a declared peer from untrusted JSON without authenticating it."""
        if not isinstance(value, dict):
            return None

        peer_id = value.get("id")
        if not isinstance(peer_id, str) or not peer_id:
            return None

        name = value.get("name")
        if name is not None and not isinstance(name, str):
            return None

        metadata = _optional_dict(value.get("metadata"))
        if metadata is None:
            return None

        return Peer(id=peer_id, name=name, metadata=metadata)

    def _parse_endpoints(self, value: Any) -> list[Endpoint] | None:
        """Parse the valid LAN endpoints from an announcement list."""
        if not isinstance(value, list):
            return None

        endpoints: list[Endpoint] = []
        for item in value:
            endpoint = self._parse_endpoint(item)
            if endpoint is not None:
                endpoints.append(endpoint)
        return endpoints

    def _parse_endpoint(self, value: Any) -> Endpoint | None:
        if not isinstance(value, dict):
            return None

        if value.get("transport") != TRANSPORT_NAME:
            return None

        address = value.get("address")
        if not isinstance(address, str):
            return None

        try:
            parse_tcp_address(address)
        except TransportError:
            return None

        metadata = _optional_dict(value.get("metadata"))
        if metadata is None:
            return None

        return Endpoint(
            transport=TRANSPORT_NAME,
            address=address,
            metadata=metadata,
        )

    def _decode_packet(self, data: bytes) -> dict[str, Any] | None:
        """Decode bounded JSON and reject ambiguous or incompatible packets."""
        try:
            payload = json.loads(
                data.decode("utf-8"),
                object_pairs_hook=_unique_json_object,
            )
        except (RecursionError, UnicodeDecodeError, ValueError):
            return None

        if not isinstance(payload, dict):
            return None
        if not _is_safe_json_value(payload):
            return None
        if payload.get("paqto") != PROTOCOL_VERSION:
            return None

        return payload

    def _send_discover(self) -> None:
        local_peer = self._local_peer
        if local_peer is None:
            raise DiscoveryError("LAN discovery must be started before discover().")

        self._send_packet(
            {
                "paqto": PROTOCOL_VERSION,
                "kind": "discover",
                "peer_id": local_peer.id,
            },
            self._broadcast_address,
        )

    def _send_announce(self, addr: SocketAddress | None = None) -> None:
        self._send_packet(self._announce_payload(), addr or self._broadcast_address)

    def _send_packet(self, payload: dict[str, Any], addr: SocketAddress) -> None:
        transport = self._transport
        if transport is None:
            raise DiscoveryError("LAN discovery must be started before sending.")

        data = self._encode_packet(payload)
        try:
            transport.sendto(data, addr)
        except (OSError, RuntimeError) as exc:
            raise DiscoveryError(
                f"Could not send LAN discovery packet to {addr}."
            ) from exc

    def _announce_payload(self) -> dict[str, Any]:
        local_peer = self._local_peer
        if local_peer is None:
            raise DiscoveryError("LAN discovery must be started before announcing.")

        return {
            "paqto": PROTOCOL_VERSION,
            "kind": "announce",
            "peer": {
                "id": local_peer.id,
                "name": local_peer.name,
                "metadata": dict(local_peer.metadata),
            },
            "endpoints": [
                {
                    "transport": endpoint.transport,
                    "address": endpoint.address,
                    "metadata": dict(endpoint.metadata),
                }
                for endpoint in self._endpoints
            ],
            "metadata": dict(self._metadata),
        }

    def _encode_packet(self, payload: dict[str, Any]) -> bytes:
        """Encode JSON after enforcing number, nesting, and datagram limits."""
        if not _is_safe_json_value(payload):
            raise DiscoveryError(
                "LAN discovery data contains unsafe numbers or exceeds the "
                f"maximum nesting depth of {MAX_JSON_NESTING}."
            )
        try:
            data = json.dumps(
                payload,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (RecursionError, TypeError, ValueError) as exc:
            raise DiscoveryError(
                "LAN discovery packet is not JSON serializable."
            ) from exc

        if len(data) > self._max_datagram_size:
            raise DiscoveryError(
                f"LAN discovery packet exceeds {self._max_datagram_size} bytes."
            )
        return data

    def _ensure_announce_is_serializable(self) -> None:
        self._encode_packet(self._announce_payload())

    def _ensure_started(self) -> None:
        if self._transport is None:
            raise DiscoveryError("LAN discovery must be started before discover().")

    def _collection_window(self, timeout: float | None) -> float:
        value = self._default_discover_timeout if timeout is None else timeout
        _validate_non_negative_float(value, "timeout")
        guard = min(MAX_DISCOVER_TIMEOUT_GUARD, value * 0.2)
        return max(0.0, value - guard)

    @property
    def _broadcast_address(self) -> SocketAddress:
        return (self._broadcast_host, self._discovery_port)

    def _clear_local_state(self) -> None:
        self._local_peer = None
        self._endpoints = []


class _LanDiscoveryProtocol(asyncio.DatagramProtocol):
    """Minimal asyncio callback bridge into :class:`LanDiscovery`."""

    def __init__(self, discovery: LanDiscovery) -> None:
        self._discovery = discovery

    def datagram_received(self, data: bytes, addr: Any) -> None:
        """Forward one received datagram to the discovery validator."""
        self._discovery._datagram_received(data, addr)

    def error_received(self, exc: Exception) -> None:
        return None

    def connection_lost(self, exc: Exception | None) -> None:
        return None


def _copy_peer(peer: Peer) -> Peer:
    if not isinstance(peer.id, str):
        raise DiscoveryError("LAN discovery requires a string local peer id.")
    if peer.name is not None and not isinstance(peer.name, str):
        raise DiscoveryError("LAN discovery requires a string local peer name.")
    return Peer(id=peer.id, name=peer.name, metadata=dict(peer.metadata))


def _copy_valid_endpoint(endpoint: Endpoint) -> Endpoint | None:
    """Copy a syntactically valid LAN endpoint or ignore another transport."""
    if endpoint.transport != TRANSPORT_NAME:
        return None
    if not isinstance(endpoint.address, str):
        return None
    try:
        parse_tcp_address(endpoint.address)
    except TransportError:
        return None
    return Endpoint(
        transport=TRANSPORT_NAME,
        address=endpoint.address,
        metadata=dict(endpoint.metadata),
    )


def _optional_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return {}
    if not isinstance(value, dict):
        return None
    return dict(value)


def _is_socket_address(value: Any) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], str)
        and isinstance(value[1], int)
    )


def _validate_port(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer.")
    if value < 0 or value > 65_535:
        raise ValueError(f"{name} must be between 0 and 65535.")


def _validate_positive_float(value: float, name: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number.")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite.")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")


def _validate_non_negative_float(value: float, name: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number.")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite.")
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to zero.")


def _validate_max_datagram_size(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("max_datagram_size must be an integer.")
    if value <= 0 or value > MAX_UDP_DATAGRAM_SIZE:
        raise ValueError(
            f"max_datagram_size must be between 1 and {MAX_UDP_DATAGRAM_SIZE}."
        )


def _validate_positive_integer(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer.")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")


def _is_safe_json_value(value: Any) -> bool:
    """Return whether nested JSON data satisfies number and depth limits."""
    pending: list[tuple[Any, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, float) and not math.isfinite(item):
            return False
        if (
            isinstance(item, int)
            and not isinstance(item, bool)
            and item.bit_length() > MAX_JSON_INTEGER_BITS
        ):
            return False
        if isinstance(item, dict):
            if depth >= MAX_JSON_NESTING:
                return False
            pending.extend((nested, depth + 1) for nested in item.values())
        elif isinstance(item, list):
            if depth >= MAX_JSON_NESTING:
                return False
            pending.extend((nested, depth + 1) for nested in item)
    return True


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while rejecting duplicate member names."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key {key!r}.")
        result[key] = value
    return result
