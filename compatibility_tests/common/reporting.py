"""Versioned JSON reporting shared by solo and pair modes."""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from compatibility_tests.common.models import CheckResult, Status

SCHEMA_VERSION = 2
REPORTS_DIRECTORY = Path(__file__).parents[1] / "reports"


def generated_at() -> str:
    """Return one timezone-aware UTC report timestamp."""
    return datetime.now(timezone.utc).isoformat()


def overall_status(results: Iterable[CheckResult]) -> str:
    """Apply the stable failure/incomplete/pass policy."""
    materialized = list(results)
    if any(result.status is Status.FAIL for result in materialized):
        return "FAIL"
    if any(
        result.required and result.status in {Status.SKIP, Status.UNAVAILABLE}
        for result in materialized
    ):
        return "INCOMPLETE"
    return "PASS"


def tests_payload(results: list[CheckResult]) -> dict[str, object]:
    """Serialize results and status totals."""
    counts = Counter(result.status.value for result in results)
    return {
        "passed": counts[Status.PASS.value],
        "failed": counts[Status.FAIL.value],
        "skipped": counts[Status.SKIP.value],
        "unavailable": counts[Status.UNAVAILABLE.value],
        "total": len(results),
        "results": [result.as_dict() for result in results],
    }


def exit_code(status: object) -> int:
    """Map report status to the documented process exit code."""
    if status == "PASS":
        return 0
    if status == "INCOMPLETE":
        return 2
    return 1


def _slug(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value).casefold()).strip("-") or "unknown"


def default_report_path(
    *,
    mode: str,
    platform_info: dict[str, object],
    python_info: dict[str, object],
    scenario: str | None = None,
    role: str | None = None,
    now: datetime | None = None,
) -> Path:
    """Build a readable collision-resistant default report path."""
    instant = now or datetime.now(timezone.utc)
    timestamp = instant.strftime("%Y-%m-%dT%H%M%S-%fZ")
    version = python_info.get("version_info", [])
    version_parts = version if isinstance(version, (list, tuple)) else []
    python_tag = "python" + "".join(str(part) for part in version_parts[:2])
    parts = [
        timestamp,
        _slug(platform_info.get("os_family")),
        _slug(platform_info.get("architecture")),
        python_tag,
        mode,
    ]
    if scenario:
        parts.append(scenario)
    if role:
        parts.append(role)
    return REPORTS_DIRECTORY / ("_".join(parts) + ".json")


def write_json(destination: str | Path, report: dict[str, object]) -> Path | None:
    """Write one UTF-8 JSON report; '-' writes to standard output."""
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if str(destination) == "-":
        sys.stdout.write(payload)
        return None
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return path
