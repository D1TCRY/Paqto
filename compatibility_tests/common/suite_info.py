"""Stable provenance for the repository compatibility suite."""

from __future__ import annotations

import hashlib
from pathlib import Path

COMPATIBILITY_SUITE_VERSION = "2"


def collect_suite_info(
    *,
    schema_version: int,
    pair_protocol_version: int,
) -> dict[str, object]:
    """Return suite versions and a cross-platform normalized source identifier."""
    suite_root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    source_paths = sorted(suite_root.rglob("*.py"))
    for path in source_paths:
        relative = path.relative_to(suite_root).as_posix().encode("utf-8")
        normalized = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(normalized).to_bytes(8, "big"))
        digest.update(normalized)
    return {
        "version": COMPATIBILITY_SUITE_VERSION,
        "schema_version": schema_version,
        "pair_protocol_version": pair_protocol_version,
        "build_id": f"sha256:{digest.hexdigest()}",
        "source_file_count": len(source_paths),
    }
