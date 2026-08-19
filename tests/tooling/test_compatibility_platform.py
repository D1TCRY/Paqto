from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from compatibility_tests.common.platform_info import (
    PlatformProbe,
    collect_platform_info,
    environment_android_indicators,
)
from compatibility_tests.common.reporting import default_report_path, write_json
from compatibility_tests.common.suite_info import collect_suite_info


@pytest.mark.parametrize(
    ("system", "sys_platform", "family"),
    [
        ("Windows", "win32", "Windows"),
        ("Linux", "linux", "Linux"),
        ("Darwin", "darwin", "macOS"),
        ("Plan9", "plan9", "Other"),
    ],
)
def test_platform_detection_simulated(
    system: str,
    sys_platform: str,
    family: str,
) -> None:
    result = collect_platform_info(
        PlatformProbe(
            system=system,
            release="test-release",
            machine="test-arch",
            sys_platform=sys_platform,
        )
    )

    assert result["os_family"] == family
    assert result["architecture"] == "test-arch"


@pytest.mark.parametrize(
    "probe",
    [
        PlatformProbe("Linux", "android-kernel", "aarch64", "linux", 35),
        PlatformProbe("Linux", "android-kernel", "aarch64", "android"),
        PlatformProbe(
            "Linux",
            "android-kernel",
            "aarch64",
            "linux",
            android_environment=True,
        ),
    ],
)
def test_android_detection_is_not_misclassified_as_linux(probe: PlatformProbe) -> None:
    assert collect_platform_info(probe)["os_family"] == "Android"


def test_android_environment_fallback_requires_both_standard_indicators() -> None:
    assert environment_android_indicators(
        {"ANDROID_ROOT": "/system", "ANDROID_DATA": "/data"}
    )
    assert not environment_android_indicators({"ANDROID_ROOT": "/system"})


def test_json_report_is_utf8_and_round_trips() -> None:
    destination = Path("compatibility_tests/reports") / f"test-{uuid4()}.json"
    report = {"schema_version": 3, "status": "PASS", "value": "compatibilità"}

    try:
        written = write_json(destination, report)

        assert written == destination
        assert json.loads(destination.read_text(encoding="utf-8")) == report
    finally:
        destination.unlink(missing_ok=True)


def test_default_json_name_is_readable_and_deterministic() -> None:
    path = default_report_path(
        mode="pair",
        scenario="direct",
        role="client",
        platform_info={"os_family": "Android", "architecture": "aarch64"},
        python_info={"version_info": [3, 12, 11]},
        now=datetime(2026, 8, 18, 19, 16, 14, tzinfo=timezone.utc),
    )

    assert path.name == (
        "2026-08-18T191614-000000Z_android_aarch64_python312_"
        "pair_direct_client.json"
    )


def test_compatibility_suite_provenance_is_stable_and_versioned() -> None:
    first = collect_suite_info(schema_version=3, pair_protocol_version=2)
    second = collect_suite_info(schema_version=3, pair_protocol_version=2)

    assert first == second
    assert first["version"] == "2"
    assert first["schema_version"] == 3
    assert first["pair_protocol_version"] == 2
    assert str(first["build_id"]).startswith("sha256:")
    assert len(str(first["build_id"])) == len("sha256:") + 64
    source_file_count = first["source_file_count"]
    assert isinstance(source_file_count, int)
    assert source_file_count > 0
