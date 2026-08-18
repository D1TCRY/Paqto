"""Two-process/two-device Paqto LAN interoperability exercise.

Run one copy with ``--role server`` and another with ``--role client``. All
addresses, ports, discovery settings, and TLS material are caller supplied;
the script contains no platform integration and uses only public Paqto APIs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
from collections.abc import Mapping
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from paqto import (
    DiscoveredPeer,
    DiscoveryService,
    Message,
    NoDiscovery,
    PaqtoConfig,
    PaqtoNode,
    Peer,
    Serializer,
)
from paqto.lan import LanDiscovery, LanTransport, TlsConfig, endpoint_from_host_port


class JsonEnvelopeSerializer(Serializer):
    """Stable JSON envelope serializer shared by both tool roles."""

    @property
    def protocol_id(self) -> str:
        return "paqto.interop.json.v1"

    def serialize(self, message: Message) -> bytes:
        return json.dumps(
            {
                "payload": message.payload,
                "type": message.type,
                "sender": message.sender,
                "recipient": message.recipient,
                "headers": message.headers,
                "id": message.id,
                "reply_to": message.reply_to,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def deserialize(self, data: bytes) -> Message:
        raw: dict[str, Any] = json.loads(data.decode("utf-8"))
        return Message(
            payload=raw["payload"],
            type=raw["type"],
            sender=raw["sender"],
            recipient=raw["recipient"],
            headers=raw["headers"],
            id=raw["id"],
            reply_to=raw["reply_to"],
        )


def _version() -> str:
    try:
        return metadata.version("paqto")
    except metadata.PackageNotFoundError:
        return "unknown"


def _identity_resolver(prefix: str | None) -> Any:
    if prefix is None:
        return None

    def resolve(certificate: Mapping[str, Any]) -> str | None:
        for kind, value in certificate.get("subjectAltName", ()):
            if kind == "URI" and value.startswith(prefix):
                return value.removeprefix(prefix)
        return None

    return resolve


def _tls(args: argparse.Namespace) -> TlsConfig | None:
    if args.security == "plain":
        return None
    missing = [name for name in ("cert", "key", "ca") if getattr(args, name) is None]
    if missing:
        raise ValueError(
            f"--security {args.security} requires "
            + ", ".join(f"--{name}" for name in missing)
        )
    return TlsConfig(
        certfile=args.cert,
        keyfile=args.key,
        cafile=args.ca,
        check_hostname=not args.no_check_hostname,
        require_client_certificate=args.security == "mtls",
        peer_identity_resolver=_identity_resolver(args.identity_san_uri_prefix),
    )


def _discovery(args: argparse.Namespace) -> DiscoveryService:
    if not args.discovery:
        return NoDiscovery()
    return LanDiscovery(
        discovery_port=args.discovery_port,
        bind_host=args.discovery_bind_host,
        broadcast_host=args.discovery_broadcast_host,
        announce_interval=args.announce_interval,
        default_discover_timeout=args.discovery_timeout,
    )


def _node(args: argparse.Namespace) -> PaqtoNode:
    return PaqtoNode(
        name=args.peer_id,
        peer_id=args.peer_id,
        transport=LanTransport(
            host=args.bind_host,
            advertised_host=args.advertised_host,
            port=args.local_port,
            tls=_tls(args),
        ),
        discovery=_discovery(args),
        serializer=JsonEnvelopeSerializer(),
        config=PaqtoConfig(
            connect_timeout=args.timeout,
            send_timeout=args.timeout,
            discover_timeout=args.discovery_timeout,
            handshake_timeout=args.timeout,
            request_timeout=args.timeout,
            acknowledgement_timeout=args.timeout,
            serializer_id="paqto.interop.json.v1",
            require_authenticated_peer_id_match=args.require_identity_match,
        ),
    )


def _base_report(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "role": args.role,
        "platform": platform.system(),
        "python_version": platform.python_version(),
        "paqto_version": _version(),
        "security": args.security,
        "discovery_requested": args.discovery,
        "peer_id": args.peer_id,
        "remote_peer_id": args.remote_peer_id,
        "status": "RUNNING",
        "checks": {},
    }


async def _server(args: argparse.Namespace, report: dict[str, Any]) -> None:
    node = _node(args)
    completed = asyncio.Event()
    request_count = 0
    connection_ids: set[int] = set()
    encrypted = False
    authenticated = False

    @node.on_message("interop-request")
    async def handle(message: Message) -> None:
        nonlocal authenticated, encrypted, request_count
        connection = node.connection_for_peer(message.sender or "")
        if connection is None:
            raise AssertionError("server cannot inspect the READY peer connection")
        session = node.session_for(connection)
        if session is None:
            raise AssertionError("server received a request without a READY session")
        connection_ids.add(id(connection))
        encrypted = connection.security_info.encrypted
        authenticated = connection.security_info.authenticated
        request_count += 1
        await node.reply(
            message,
            {"round": request_count, "server": node.peer.id},
            type="interop-reply",
            require_ack=True,
        )
        if args.expected_requests > 0 and request_count >= args.expected_requests:
            completed.set()

    await node.start()
    report["checks"]["node_started"] = "PASS"
    print(
        f"Server {args.peer_id!r} listening via {args.bind_host}:{args.local_port}; "
        f"advertised host {args.advertised_host!r}",
        flush=True,
    )
    try:
        if args.expected_requests > 0:
            await asyncio.wait_for(completed.wait(), timeout=args.server_timeout)
        else:
            await asyncio.Event().wait()
        report["checks"]["request_reply"] = f"PASS ({request_count} requests)"
        if len(connection_ids) < 2:
            raise AssertionError("server did not observe a fresh connection after disconnect")
        report["checks"]["tcp"] = "PASS"
        report["checks"]["paqto_handshake"] = "PASS"
        report["checks"]["reconnect"] = "PASS"
        report["checks"]["tls"] = "PASS" if encrypted else "SKIP (plain mode)"
        report["checks"]["mtls"] = (
            "PASS" if args.security == "mtls" and authenticated else "SKIP"
        )
        report["checks"]["discovery"] = (
            "ACTIVE (client report proves discovery)"
            if args.discovery
            else "SKIP (explicit endpoint)"
        )
    finally:
        await node.stop()
    report["checks"]["cleanup"] = "PASS"


async def _resolve_target(
    args: argparse.Namespace,
    node: PaqtoNode,
    report: dict[str, Any],
) -> DiscoveredPeer:
    if args.discovery:
        for _ in range(args.discovery_attempts):
            peers = await node.discover(timeout=args.discovery_timeout)
            for discovered in peers:
                if discovered.peer.id == args.remote_peer_id:
                    report["checks"]["discovery"] = "PASS"
                    return discovered
        report["checks"]["discovery"] = "FAIL"
        raise RuntimeError(
            f"Discovery did not find remote peer {args.remote_peer_id!r}."
        )
    if args.peer_host is None or args.peer_port is None:
        raise ValueError("client without --discovery requires --peer-host and --peer-port")
    report["checks"]["discovery"] = "SKIP (explicit endpoint)"
    return DiscoveredPeer(
        peer=Peer(id=args.remote_peer_id, name=args.remote_peer_id),
        endpoints=[endpoint_from_host_port(args.peer_host, args.peer_port)],
    )


async def _client(args: argparse.Namespace, report: dict[str, Any]) -> None:
    node = _node(args)
    await node.start()
    report["checks"]["node_started"] = "PASS"
    try:
        target = await _resolve_target(args, node, report)
        first = await node.request(
            target,
            {"round": 1},
            type="interop-request",
            require_ack=True,
        )
        if first.payload.get("round") != 1:
            raise AssertionError("first request/reply payload mismatch")
        connection = node.connection_for_peer(args.remote_peer_id)
        if connection is None:
            raise AssertionError("first READY connection is unavailable")
        report["checks"]["tcp"] = "PASS"
        report["checks"]["paqto_handshake"] = "PASS"
        report["checks"]["request_reply"] = "PASS"
        report["checks"]["tls"] = (
            "PASS" if connection.security_info.encrypted else "SKIP (plain mode)"
        )
        report["checks"]["mtls"] = (
            "PASS"
            if args.security == "mtls" and connection.security_info.authenticated
            else "SKIP"
        )

        await node.disconnect(target)
        await asyncio.sleep(args.reconnect_delay)
        second = await node.request(
            target,
            {"round": 2},
            type="interop-request",
            require_ack=True,
        )
        if second.payload.get("round") != 2:
            raise AssertionError("post-disconnect request/reply payload mismatch")
        report["checks"]["reconnect"] = "PASS"
    finally:
        await node.stop()
    report["checks"]["cleanup"] = "PASS"


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    report = _base_report(args)
    try:
        if args.role == "server":
            await _server(args, report)
        else:
            await _client(args, report)
    except Exception as exc:  # noqa: BLE001 - report boundary for a CLI exercise
        report["status"] = "FAIL"
        report["error"] = f"{type(exc).__name__}: {exc}"
        return report
    report["status"] = "PASS"
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=("server", "client"))
    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--remote-peer-id", required=True)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--advertised-host")
    parser.add_argument("--local-port", type=int, default=7450)
    parser.add_argument("--peer-host")
    parser.add_argument("--peer-port", type=int)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--reconnect-delay", type=float, default=0.2)
    parser.add_argument("--expected-requests", type=int, default=2)
    parser.add_argument("--server-timeout", type=float, default=120.0)
    parser.add_argument("--discovery", action="store_true")
    parser.add_argument("--discovery-port", type=int, default=45454)
    parser.add_argument("--discovery-bind-host", default="0.0.0.0")
    parser.add_argument("--discovery-broadcast-host", default="255.255.255.255")
    parser.add_argument("--discovery-timeout", type=float, default=2.0)
    parser.add_argument("--discovery-attempts", type=int, default=3)
    parser.add_argument("--announce-interval", type=float, default=0.5)
    parser.add_argument("--security", choices=("plain", "tls", "mtls"), default="plain")
    parser.add_argument("--cert", type=Path)
    parser.add_argument("--key", type=Path)
    parser.add_argument("--ca", type=Path)
    parser.add_argument("--no-check-hostname", action="store_true")
    parser.add_argument("--identity-san-uri-prefix")
    parser.add_argument("--require-identity-match", action="store_true")
    parser.add_argument("--json", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.advertised_host is None:
        args.advertised_host = args.bind_host
    try:
        report = asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130
    payload = json.dumps(report, indent=2, sort_keys=True)
    print(payload)
    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(payload + "\n", encoding="utf-8")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
