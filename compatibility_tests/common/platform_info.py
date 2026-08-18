"""Privacy-conscious platform and Python runtime detection for reports."""

from __future__ import annotations

import os
import platform
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast


@dataclass(frozen=True, slots=True)
class PlatformProbe:
    """Raw values used to classify an environment, injectable in unit tests."""

    system: str
    release: str
    machine: str
    sys_platform: str
    android_api_level: int | None = None
    android_environment: bool = False


def current_probe() -> PlatformProbe:
    """Collect standard-library indicators without host/user identity data."""
    android_api_level: int | None = None
    get_android_api_level = getattr(sys, "getandroidapilevel", None)
    if callable(get_android_api_level):
        try:
            probe_android_api_level = cast(Callable[[], int], get_android_api_level)
            android_api_level = int(probe_android_api_level())
        except (TypeError, ValueError, OSError):
            android_api_level = None
    android_environment = bool(
        os.environ.get("ANDROID_ROOT") and os.environ.get("ANDROID_DATA")
    )
    return PlatformProbe(
        system=platform.system() or "unknown",
        release=platform.release() or "unknown",
        machine=platform.machine() or "unknown",
        sys_platform=sys.platform,
        android_api_level=android_api_level,
        android_environment=android_environment,
    )


def classify_os(probe: PlatformProbe) -> tuple[str, list[str]]:
    """Return a user-facing OS family and the non-sensitive evidence used."""
    system = probe.system.casefold()
    sys_platform = probe.sys_platform.casefold()
    evidence: list[str] = []
    if probe.android_api_level is not None:
        evidence.append("sys.getandroidapilevel")
    if sys_platform.startswith("android"):
        evidence.append("sys.platform")
    if system == "android":
        evidence.append("platform.system")
    if system == "linux" and probe.android_environment:
        evidence.append("Android runtime environment")
    if evidence:
        return "Android", evidence
    if system == "windows" or sys_platform.startswith("win"):
        return "Windows", ["platform/system runtime"]
    if system == "darwin" or sys_platform == "darwin":
        return "macOS", ["Darwin runtime"]
    if system == "linux" or sys_platform.startswith("linux"):
        return "Linux", ["Linux runtime"]
    return "Other", ["unrecognized standard-library values"]


def collect_platform_info(probe: PlatformProbe | None = None) -> dict[str, object]:
    """Build report metadata, distinguishing Android from generic Linux."""
    actual = probe or current_probe()
    family, evidence = classify_os(actual)
    result: dict[str, object] = {
        "os_family": family,
        "kernel_system": actual.system,
        "release": actual.release,
        "architecture": actual.machine,
        "sys_platform": actual.sys_platform,
        "detection_evidence": evidence,
    }
    if actual.android_api_level is not None:
        result["android_api_level"] = actual.android_api_level
    return result


def collect_python_info() -> dict[str, object]:
    """Return the interpreter details relevant to cross-version evidence."""
    return {
        "implementation": platform.python_implementation(),
        "version": platform.python_version(),
        "version_info": list(sys.version_info[:3]),
    }


def environment_android_indicators(environ: Mapping[str, str]) -> bool:
    """Expose the conservative environment fallback for focused tests."""
    return bool(environ.get("ANDROID_ROOT") and environ.get("ANDROID_DATA"))

