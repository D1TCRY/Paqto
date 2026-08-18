"""Deprecated path alias for Paqto's permanent pair compatibility command."""

from __future__ import annotations

import sys
from pathlib import Path

repository_root = Path(__file__).resolve().parent.parent
if str(repository_root) not in sys.path:
    sys.path.insert(0, str(repository_root))

from compatibility_tests.run import main


def legacy_main(argv: list[str] | None = None) -> int:
    """Delegate to the only maintained pair implementation."""
    arguments = sys.argv[1:] if argv is None else argv
    print(
        "DEPRECATED: use 'python compatibility_tests/run.py pair ...' instead.",
        file=sys.stderr,
    )
    return main(["pair", *arguments])


if __name__ == "__main__":
    raise SystemExit(legacy_main())
