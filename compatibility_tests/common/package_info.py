"""Identify exactly which Paqto package a compatibility run imports."""

from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path
from typing import Any


def _within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def collect_package_info(repository_root: Path | None = None) -> dict[str, object]:
    """Return import and distribution metadata, including source-tree detection."""
    import paqto

    root = (repository_root or Path(__file__).parents[2]).resolve()
    raw_file = getattr(paqto, "__file__", None)
    import_path = Path(raw_file).resolve() if raw_file else None
    source_root = (root / "src" / "paqto").resolve()
    repository_source = import_path is not None and _within(import_path, source_root)

    distribution: dict[str, object] = {"available": False}
    version = "unknown"
    try:
        installed = metadata.distribution("paqto")
    except metadata.PackageNotFoundError:
        installed = None
    if installed is not None:
        version = installed.version
        distribution = {
            "available": True,
            "name": installed.metadata["Name"] or "paqto",
            "version": installed.version,
            "location": str(Path(str(installed.locate_file(""))).resolve()),
        }
        direct_url = installed.read_text("direct_url.json")
        if direct_url:
            try:
                parsed: Any = json.loads(direct_url)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                directory_info = parsed.get("dir_info")
                if isinstance(directory_info, dict):
                    distribution["editable"] = bool(directory_info.get("editable"))

    return {
        "version": version,
        "import_path": str(import_path) if import_path is not None else None,
        "import_origin": "repository_source" if repository_source else "installed",
        "repository_source": repository_source,
        "distribution": distribution,
    }


def installation_warning(package: dict[str, object]) -> str | None:
    """Explain when the source checkout, rather than a built wheel, is tested."""
    if package.get("repository_source"):
        return (
            "Paqto is imported from this repository's src/paqto tree. "
            "Use --require-installed to reject editable/source imports when "
            "validating a wheel installation."
        )
    distribution = package.get("distribution")
    if not isinstance(distribution, dict) or not distribution.get("available"):
        return "Paqto distribution metadata is unavailable for this import."
    return None
