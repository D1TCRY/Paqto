"""Runner and machine-readable reporting for Paqto platform conformance."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import platform
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import TextIO

from platform_conformance.models import (
    CapabilityUnavailable,
    Check,
    CheckResult,
    Status,
)

SCHEMA_VERSION = 1
PROFILES = ("ci", "full")


def _paqto_version() -> str:
    try:
        return metadata.version("paqto")
    except metadata.PackageNotFoundError:
        return "unknown"


async def _run_check(check: Check, profile: str) -> CheckResult:
    required = profile in check.required_profiles
    if profile not in check.run_profiles:
        return CheckResult(
            id=check.id,
            category=check.category,
            description=check.description,
            status=Status.SKIP,
            required=required,
            duration_ms=0.0,
            detail=f"not selected by {profile!r} profile",
        )

    started = time.perf_counter()
    try:
        detail = await check.function()
    except CapabilityUnavailable as exc:
        status = Status.UNAVAILABLE
        message = str(exc)
    except Exception as exc:  # noqa: BLE001 - check isolation boundary
        status = Status.FAIL
        message = f"{type(exc).__name__}: {exc}"
    else:
        status = Status.PASS
        message = detail or "passed"
    return CheckResult(
        id=check.id,
        category=check.category,
        description=check.description,
        status=status,
        required=required,
        duration_ms=(time.perf_counter() - started) * 1000,
        detail=message,
    )


async def run_suite(profile: str) -> list[CheckResult]:
    """Execute all selected checks sequentially for deterministic ownership."""
    from platform_conformance.checks import CHECKS

    return [await _run_check(check, profile) for check in CHECKS]


def _overall_status(results: list[CheckResult]) -> str:
    if any(result.status is Status.FAIL for result in results):
        return "FAIL"
    if any(
        result.required and result.status in {Status.SKIP, Status.UNAVAILABLE}
        for result in results
    ):
        return "INCOMPLETE"
    return "PASS"


def _aggregate(results: list[CheckResult], ids: set[str]) -> dict[str, object]:
    selected = [result for result in results if result.id in ids]
    statuses = {result.status for result in selected}
    if Status.FAIL in statuses:
        status = Status.FAIL
    elif Status.UNAVAILABLE in statuses:
        status = Status.UNAVAILABLE
    elif Status.PASS in statuses:
        status = Status.PASS
    else:
        status = Status.SKIP
    return {
        "status": status.value,
        "checks": [result.id for result in selected],
        "detail": [result.detail for result in selected],
    }


def build_report(profile: str, results: list[CheckResult]) -> dict[str, object]:
    """Build the stable JSON report without collecting host identity data."""
    counts = Counter(result.status.value for result in results)
    capabilities = {
        "ipv4": _aggregate(results, {"capability.ipv4", "capability.udp_ipv4"}),
        "ipv6": _aggregate(results, {"capability.ipv6"}),
        "tcp": _aggregate(results, {"capability.ipv4", "tcp.framing_reconnect"}),
        "udp": _aggregate(results, {"capability.udp_ipv4"}),
        "broadcast_discovery": _aggregate(results, {"discovery.broadcast"}),
        "tls": _aggregate(results, {"tls.high_level", "tls.context_mtls_identity"}),
        "mtls": _aggregate(results, {"tls.context_mtls_identity"}),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "status": _overall_status(results),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "sys_platform": sys.platform,
        },
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
        },
        "paqto_version": _paqto_version(),
        "capabilities": capabilities,
        "tests": {
            "passed": counts[Status.PASS.value],
            "failed": counts[Status.FAIL.value],
            "skipped": counts[Status.SKIP.value],
            "unavailable": counts[Status.UNAVAILABLE.value],
            "total": len(results),
            "results": [result.as_dict() for result in results],
        },
    }


def _print_human(report: dict[str, object], results: list[CheckResult], stream: TextIO) -> None:
    platform_data = report["platform"]
    python_data = report["python"]
    assert isinstance(platform_data, dict)
    assert isinstance(python_data, dict)
    print("Paqto platform conformance", file=stream)
    print(
        f"Platform: {platform_data['system']} {platform_data['release']} "
        f"({platform_data['machine']})",
        file=stream,
    )
    print(
        f"Python: {python_data['version']} ({python_data['implementation']})",
        file=stream,
    )
    print(f"Paqto: {report['paqto_version']}  Profile: {report['profile']}", file=stream)
    print(file=stream)
    for result in results:
        requirement = "required" if result.required else "optional"
        print(
            f"{result.status.value:<11} {result.id:<36} "
            f"[{requirement}] {result.detail}",
            file=stream,
        )
    tests = report["tests"]
    assert isinstance(tests, dict)
    print(file=stream)
    print(
        f"Result: {report['status']} | PASS {tests['passed']} | "
        f"FAIL {tests['failed']} | SKIP {tests['skipped']} | "
        f"UNAVAILABLE {tests['unavailable']}",
        file=stream,
    )


def _write_json(destination: str, report: dict[str, object]) -> None:
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if destination == "-":
        sys.stdout.write(payload)
        return
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _exit_code(report: dict[str, object]) -> int:
    status = report["status"]
    if status == "PASS":
        return 0
    if status == "INCOMPLETE":
        return 2
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Paqto's offline platform-conformance suite. Exit 0 means all "
            "checks required by the selected profile passed; exit 1 means a "
            "failure; exit 2 means a required capability was unavailable."
        )
    )
    parser.add_argument(
        "--profile",
        choices=PROFILES,
        default="full",
        help="full includes required UDP broadcast; ci runs the deterministic subset",
    )
    parser.add_argument(
        "--json",
        metavar="PATH",
        help="write a machine-readable JSON report; use '-' for standard output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return its documented process exit code."""
    args = _parser().parse_args(argv)
    logging.getLogger("paqto").addHandler(logging.NullHandler())
    logging.getLogger("paqto").setLevel(logging.CRITICAL)
    try:
        results = asyncio.run(run_suite(args.profile))
    except Exception as exc:  # noqa: BLE001 - bootstrap/import failure report
        results = [
            CheckResult(
                id="suite.bootstrap",
                category="core",
                description="load conformance checks and Paqto",
                status=Status.FAIL,
                required=True,
                duration_ms=0.0,
                detail=f"{type(exc).__name__}: {exc}",
            )
        ]
    report = build_report(args.profile, results)
    human_stream = sys.stderr if args.json == "-" else sys.stdout
    _print_human(report, results, human_stream)
    if args.json:
        _write_json(args.json, report)
    return _exit_code(report)
