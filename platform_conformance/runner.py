"""Backward-compatible CLI for the relocated compatibility solo suite."""

from __future__ import annotations

import argparse
import sys

from compatibility_tests.common.reporting import exit_code, write_json
from compatibility_tests.solo.runner import (
    PROFILES,
    build_report,
    configure_logging,
    execute_solo_sync,
    print_human,
    run_suite,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility alias for 'python compatibility_tests/run.py solo'. "
            "Exit 0 is PASS, 1 is FAIL, and 2 is INCOMPLETE."
        )
    )
    parser.add_argument("--profile", choices=PROFILES, default="full")
    parser.add_argument("--json", metavar="PATH")
    parser.add_argument("--require-installed", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the old command while delegating to the single new implementation."""
    args = _parser().parse_args(argv)
    configure_logging(args.verbose)
    report, results = execute_solo_sync(
        args.profile, require_installed=args.require_installed
    )
    print_human(
        report,
        results,
        sys.stderr if args.json == "-" else sys.stdout,
        verbose=args.verbose,
    )
    if args.json:
        write_json(args.json, report)
    return exit_code(report["status"])


__all__ = ["build_report", "main", "run_suite"]
