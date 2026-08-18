from platform_conformance.models import CheckResult, Status
from platform_conformance.runner import build_report


def _result(
    id: str,
    status: Status,
    *,
    required: bool = True,
) -> CheckResult:
    return CheckResult(
        id=id,
        category="test",
        description=id,
        status=status,
        required=required,
        duration_ms=1.0,
        detail="diagnostic",
    )


def test_conformance_report_contains_required_machine_fields() -> None:
    report = build_report(
        "ci",
        [
            _result("capability.ipv4", Status.PASS),
            _result("capability.udp_ipv4", Status.PASS),
            _result("capability.ipv6", Status.UNAVAILABLE, required=False),
            _result("tcp.framing_reconnect", Status.PASS),
            _result("discovery.broadcast", Status.SKIP, required=False),
            _result("tls.high_level", Status.PASS),
            _result("tls.context_mtls_identity", Status.PASS),
        ],
    )

    assert report["schema_version"] == 1
    assert report["profile"] == "ci"
    assert report["status"] == "PASS"
    assert isinstance(report["platform"], dict)
    assert isinstance(report["python"], dict)
    assert report["paqto_version"] == "0.0.1"
    capabilities = report["capabilities"]
    assert isinstance(capabilities, dict)
    assert set(capabilities) == {
        "ipv4",
        "ipv6",
        "tcp",
        "udp",
        "broadcast_discovery",
        "tls",
        "mtls",
    }
    tests = report["tests"]
    assert isinstance(tests, dict)
    assert tests["passed"] == 5
    assert tests["failed"] == 0
    assert tests["skipped"] == 1
    assert tests["unavailable"] == 1


def test_required_unavailable_capability_makes_report_incomplete() -> None:
    report = build_report(
        "full",
        [_result("discovery.broadcast", Status.UNAVAILABLE)],
    )

    assert report["status"] == "INCOMPLETE"


def test_any_executed_failure_makes_report_fail() -> None:
    report = build_report(
        "ci",
        [_result("tcp.framing_reconnect", Status.FAIL)],
    )

    assert report["status"] == "FAIL"
