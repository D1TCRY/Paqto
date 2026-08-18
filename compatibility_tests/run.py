"""Main command-line entry point for Paqto compatibility evidence."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if __package__ in {None, ""}:
    repository_root = Path(__file__).resolve().parent.parent
    if str(repository_root) not in sys.path:
        sys.path.insert(0, str(repository_root))

from compatibility_tests.common.reporting import (
    default_report_path,
    exit_code,
    write_json,
)
from compatibility_tests.solo.runner import (
    PROFILES,
    configure_logging,
    execute_solo_sync,
    print_human,
)

if TYPE_CHECKING:
    from compatibility_tests.pair.runner import PairConfig


def build_parser() -> argparse.ArgumentParser:
    """Create the discoverable, intentionally small compatibility CLI."""
    parser = argparse.ArgumentParser(
        description=(
            "Run Paqto offline compatibility tests on one runtime (solo) or "
            "between two real processes/devices on the same LAN (pair)."
        ),
        epilog=(
            "Start with 'solo', or run 'pair --help' to see role, scenario, "
            "target, bind, port, timeout, JSON, discovery, and verbose options."
        ),
    )
    commands = parser.add_subparsers(dest="mode", required=True)

    solo = commands.add_parser(
        "solo",
        help="test the current OS/Python/Paqto installation without Internet",
    )
    solo.add_argument(
        "--profile",
        choices=PROFILES,
        default="full",
        help="full requires local broadcast; ci skips that environmental check",
    )
    solo.add_argument(
        "--require-installed",
        action="store_true",
        help="fail when Paqto is imported from this repository's src tree",
    )
    solo.add_argument("--json", metavar="PATH", help="override automatic JSON path")
    solo.add_argument("--verbose", action="store_true", help="show check details/logging")

    pair = commands.add_parser(
        "pair",
        help="test two real Paqto processes/devices over a LAN",
        description=(
            "Run the same command on both devices. Start the server first, then "
            "the client. Both roles write a JSON report with one shared session_id."
        ),
    )
    pair.add_argument("--role", required=True, choices=("server", "client"))
    pair.add_argument(
        "--scenario",
        required=True,
        choices=("direct", "discovery"),
        help="direct uses an explicit endpoint; discovery requires real LAN broadcast",
    )
    pair.add_argument(
        "--target",
        help="server IP/hostname; required only for a direct client",
    )
    pair.add_argument("--bind", default="0.0.0.0", help="local TCP/UDP bind address")
    pair.add_argument(
        "--advertise",
        help="reachable local address announced by discovery (required with wildcard bind)",
    )
    pair.add_argument(
        "--port",
        type=int,
        default=7450,
        help="server TCP port (default: 7450)",
    )
    pair.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="finite seconds to wait for pair coordination (default: 120)",
    )
    pair.add_argument(
        "--discovery-port",
        type=int,
        default=45454,
        help="shared UDP discovery port (default: 45454)",
    )
    pair.add_argument(
        "--broadcast",
        default="255.255.255.255",
        help="LAN broadcast destination used only by discovery",
    )
    pair.add_argument(
        "--keep-alive",
        action="store_true",
        help="keep a successful server running until interrupted",
    )
    pair.add_argument(
        "--require-installed",
        action="store_true",
        help="fail when Paqto is imported from this repository's src tree",
    )
    pair.add_argument("--json", metavar="PATH", help="override automatic JSON path")
    pair.add_argument("--verbose", action="store_true", help="enable Paqto INFO logs")
    return parser


def _pair_config(args: argparse.Namespace, parser: argparse.ArgumentParser) -> PairConfig:
    from compatibility_tests.pair.runner import PairConfig

    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if not 1 <= args.discovery_port <= 65535:
        parser.error("--discovery-port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.scenario == "direct" and args.role == "client" and not args.target:
        parser.error("pair direct --role client requires --target")
    wildcard = args.bind in {"0.0.0.0", "::"}
    if args.scenario == "discovery" and wildcard and not args.advertise:
        parser.error("pair discovery with a wildcard --bind requires --advertise")
    advertise = args.advertise or ("127.0.0.1" if wildcard else args.bind)
    return PairConfig(
        role=args.role,
        scenario=args.scenario,
        target=args.target,
        bind=args.bind,
        advertise=advertise,
        port=args.port,
        timeout=args.timeout,
        discovery_port=args.discovery_port,
        broadcast=args.broadcast,
        keep_alive=args.keep_alive,
        require_installed=args.require_installed,
    )


def _pair_human(report: dict[str, object], results: Sequence[object]) -> None:
    print("Paqto pair compatibility result")
    print(f"Role/scenario: {report['role']} / {report['scenario']}")
    print(f"Session ID: {report['session_id']}")
    remote = report.get("remote")
    if isinstance(remote, dict):
        platform_info = remote.get("platform")
        python_info = remote.get("python")
        paqto_info = remote.get("paqto")
        if isinstance(platform_info, dict):
            print(
                "Remote: "
                f"{platform_info.get('os_family')} / "
                f"{platform_info.get('architecture')}"
            )
        if isinstance(python_info, dict) and isinstance(paqto_info, dict):
            print(
                f"Remote Python/Paqto: {python_info.get('version')} / "
                f"{paqto_info.get('version')}"
            )
    for item in results:
        status = getattr(getattr(item, "status", None), "value", "UNKNOWN")
        description = getattr(item, "description", "unknown check")
        print(f"{status:<11} {description}")
    print(f"Result: {report['status']}")


def main(argv: list[str] | None = None) -> int:
    """Run the requested mode, automatically persist JSON, and return status."""
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    if args.mode == "solo":
        report, results = execute_solo_sync(
            args.profile,
            require_installed=args.require_installed,
        )
        print_human(report, results, sys.stdout, verbose=args.verbose)
        platform_info = report["platform"]
        python_info = report["python"]
        assert isinstance(platform_info, dict)
        assert isinstance(python_info, dict)
        destination = args.json or default_report_path(
            mode="solo",
            platform_info=platform_info,
            python_info=python_info,
        )
    else:
        from compatibility_tests.pair.runner import execute_pair, print_pair_header

        config = _pair_config(args, parser)
        print_pair_header(config)
        try:
            report, results = asyncio.run(execute_pair(config))
        except KeyboardInterrupt:
            return 130
        _pair_human(report, results)
        local = report["local"]
        assert isinstance(local, dict)
        platform_info = local["platform"]
        python_info = local["python"]
        assert isinstance(platform_info, dict)
        assert isinstance(python_info, dict)
        destination = args.json or default_report_path(
            mode="pair",
            scenario=config.scenario,
            role=config.role,
            platform_info=platform_info,
            python_info=python_info,
        )
    written = write_json(destination, report)
    if written is not None:
        print(f"JSON report: {written.resolve()}")
    return exit_code(report["status"])


if __name__ == "__main__":
    raise SystemExit(main())
