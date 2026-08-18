"""Offline platform-conformance tooling for Paqto source checkouts.

This package is intentionally outside ``src`` and is not installed with the
Paqto runtime wheel. Run it from a source checkout with
``python -m platform_conformance`` after installing Paqto in the interpreter
under test.
"""

from platform_conformance.runner import main

__all__ = ["main"]
