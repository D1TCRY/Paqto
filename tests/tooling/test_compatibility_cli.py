from __future__ import annotations

import pytest

from compatibility_tests import run as cli
from compatibility_tests.common.reporting import exit_code


def test_cli_parses_solo_and_pair_options() -> None:
    parser = cli.build_parser()
    solo = parser.parse_args(["solo", "--require-installed", "--verbose"])
    pair = parser.parse_args(
        [
            "pair",
            "--role",
            "client",
            "--scenario",
            "direct",
            "--target",
            "192.0.2.10",
            "--port",
            "7451",
        ]
    )

    assert solo.mode == "solo"
    assert solo.require_installed is True
    assert pair.role == "client"
    assert pair.target == "192.0.2.10"
    assert pair.port == 7451


def test_direct_client_requires_target() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(
        ["pair", "--role", "client", "--scenario", "direct"]
    )

    with pytest.raises(SystemExit) as raised:
        cli._pair_config(args, parser)

    assert raised.value.code == 2


@pytest.mark.parametrize(
    ("status", "expected"),
    [("PASS", 0), ("FAIL", 1), ("INCOMPLETE", 2), ("unknown", 1)],
)
def test_exit_codes_are_stable(status: str, expected: int) -> None:
    assert exit_code(status) == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [("PASS", 0), ("FAIL", 1), ("INCOMPLETE", 2)],
)
def test_cli_propagates_report_status_to_process_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    expected: int,
) -> None:
    captured: list[dict[str, object]] = []
    report: dict[str, object] = {
        "status": status,
        "platform": {"os_family": "Test", "architecture": "test"},
        "python": {"version_info": [3, 12, 0]},
    }
    monkeypatch.setattr(cli, "execute_solo_sync", lambda *args, **kwargs: (report, []))
    monkeypatch.setattr(cli, "print_human", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "write_json",
        lambda destination, payload: captured.append(payload),
    )

    actual = cli.main(["solo", "--json", "captured.json"])

    assert actual == expected
    assert captured[0]["status"] == status
