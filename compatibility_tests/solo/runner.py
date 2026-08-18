"""Execution and reporting for single-runtime compatibility checks."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TextIO

from compatibility_tests.common.models import (
    CapabilityUnavailable,
    Check,
    CheckResult,
    Status,
)
from compatibility_tests.common.package_info import (
    collect_package_info,
    installation_warning,
)
from compatibility_tests.common.platform_info import (
    collect_platform_info,
    collect_python_info,
)
from compatibility_tests.common.reporting import (
    SCHEMA_VERSION,
    generated_at,
    overall_status,
    tests_payload,
)

PROFILES = ("ci", "full")


async def run_check(check: Check, profile: str) -> CheckResult:
    """Run one isolated check and normalize its result."""
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
    """Execute selected checks sequentially for deterministic ownership."""
    from compatibility_tests.solo.checks import CHECKS

    return [await run_check(check, profile) for check in CHECKS]


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


def build_report(
    profile: str,
    results: list[CheckResult],
    *,
    package: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build one schema-v2 solo report without personal machine metadata."""
    platform_info = collect_platform_info()
    python_info = collect_python_info()
    package_info = package or collect_package_info()
    warning = installation_warning(package_info)
    capabilities = {
        "ipv4": _aggregate(results, {"capability.ipv4", "capability.udp_ipv4"}),
        "ipv6": _aggregate(results, {"capability.ipv6"}),
        "tcp_ipv4": _aggregate(
            results, {"capability.ipv4", "tcp.framing_reconnect"}
        ),
        "udp_ipv4": _aggregate(results, {"capability.udp_ipv4"}),
        "tcp_ipv6": _aggregate(results, {"capability.ipv6"}),
        "udp_ipv6": _aggregate(results, {"capability.ipv6"}),
        "local_broadcast_discovery": _aggregate(
            results, {"discovery.broadcast"}
        ),
        "no_discovery": _aggregate(
            results, {"messaging.operations", "lifecycle.restart_refresh_cleanup"}
        ),
        "tls": _aggregate(
            results, {"tls.high_level", "tls.context_mtls_identity"}
        ),
        "custom_ca": _aggregate(results, {"tls.high_level"}),
        "in_memory_ca": _aggregate(results, {"tls.high_level"}),
        "ssl_context_injection": _aggregate(
            results, {"tls.context_mtls_identity"}
        ),
        "mtls": _aggregate(results, {"tls.context_mtls_identity"}),
        "strict_peer_identity": _aggregate(
            results, {"tls.context_mtls_identity"}
        ),
    }
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at(),
        "mode": "solo",
        "profile": profile,
        "status": overall_status(results),
        "platform": platform_info,
        "python": python_info,
        "paqto": package_info,
        "paqto_version": package_info.get("version", "unknown"),
        "capabilities": capabilities,
        "tests": tests_payload(results),
        "durations": {
            "total_ms": round(sum(result.duration_ms for result in results), 3)
        },
        "warnings": [warning] if warning else [],
    }
    return report


async def execute_solo(
    profile: str,
    *,
    require_installed: bool = False,
) -> tuple[dict[str, object], list[CheckResult]]:
    """Run solo checks and enforce optional wheel/installed-package provenance."""
    package = collect_package_info()
    results = await run_suite(profile)
    if require_installed:
        distribution = package.get("distribution")
        metadata_available = isinstance(distribution, dict) and bool(
            distribution.get("available")
        )
        installed = not bool(package.get("repository_source")) and metadata_available
        results.append(
            CheckResult(
                id="package.require_installed",
                category="core",
                description="Paqto is imported from an installed distribution",
                status=Status.PASS if installed else Status.FAIL,
                required=True,
                duration_ms=0.0,
                detail=(
                    "Paqto import is outside the repository source tree and "
                    "distribution metadata is available"
                    if installed
                    else "Paqto is a repository-source import or has no distribution metadata"
                ),
            )
        )
    return build_report(profile, results, package=package), results


def print_human(
    report: dict[str, object],
    results: list[CheckResult],
    stream: TextIO,
    *,
    verbose: bool = False,
) -> None:
    """Render the compact human-readable solo summary."""
    platform_info = report["platform"]
    python_info = report["python"]
    package = report["paqto"]
    assert isinstance(platform_info, dict)
    assert isinstance(python_info, dict)
    assert isinstance(package, dict)
    print("Paqto compatibility test", file=stream)
    print(file=stream)
    print(
        f"Platform: {platform_info['os_family']} / {platform_info['architecture']} "
        f"({platform_info['kernel_system']} {platform_info['release']})",
        file=stream,
    )
    print(
        f"Python: {python_info['implementation']} {python_info['version']}",
        file=stream,
    )
    print(f"Paqto: {package['version']}", file=stream)
    print(f"Imported from: {package['import_path']}", file=stream)
    warnings = report.get("warnings", [])
    if isinstance(warnings, list):
        for warning in warnings:
            print(f"WARNING: {warning}", file=stream)
    print(file=stream)
    for result in results:
        suffix = f" - {result.detail}" if verbose or result.status is not Status.PASS else ""
        print(f"{result.status.value:<11} {result.description}{suffix}", file=stream)
    tests = report["tests"]
    assert isinstance(tests, dict)
    print(file=stream)
    print(f"Result: {report['status']}", file=stream)
    print(
        f"{tests['passed']} passed, {tests['failed']} failed, "
        f"{tests['skipped']} skipped, {tests['unavailable']} unavailable",
        file=stream,
    )


def configure_logging(verbose: bool) -> None:
    """Keep ordinary runs readable while allowing opt-in Paqto diagnostics."""
    logger = logging.getLogger("paqto")
    if verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logger.addHandler(logging.NullHandler())
        logger.setLevel(logging.CRITICAL)


def execute_solo_sync(
    profile: str,
    *,
    require_installed: bool = False,
) -> tuple[dict[str, object], list[CheckResult]]:
    """Synchronous boundary used by both CLIs."""
    try:
        return asyncio.run(
            execute_solo(profile, require_installed=require_installed)
        )
    except Exception as exc:  # noqa: BLE001 - bootstrap/import reporting boundary
        result = CheckResult(
            id="suite.bootstrap",
            category="core",
            description="load compatibility checks and Paqto",
            status=Status.FAIL,
            required=True,
            duration_ms=0.0,
            detail=f"{type(exc).__name__}: {exc}",
        )
        unavailable_package: dict[str, object] = {
            "version": "unknown",
            "import_path": None,
            "import_origin": "unavailable",
            "repository_source": False,
            "distribution": {"available": False},
        }
        return (
            build_report(profile, [result], package=unavailable_package),
            [result],
        )
