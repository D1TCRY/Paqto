from datetime import datetime, timedelta, timezone

import pytest

from paqto.core.config import PaqtoConfig, ReconnectPolicy
from paqto.core.discovered import DiscoveredPeer, PeerFreshness
from paqto.core.peer import Peer


def test_reconnect_backoff_is_exponential_bounded_and_deterministic() -> None:
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=1,
        multiplier=2,
        maximum_delay=5,
        jitter=0,
    )

    assert [policy.delay_for_attempt(attempt) for attempt in range(5)] == [
        1,
        2,
        4,
        5,
        5,
    ]

    jittered = ReconnectPolicy(jitter=0.25)
    assert jittered.delay_for_attempt(0, random_value=0) == pytest.approx(0.375)
    assert jittered.delay_for_attempt(0, random_value=1) == pytest.approx(0.625)


def test_reconnect_backoff_remains_bounded_after_extreme_attempt_count() -> None:
    policy = ReconnectPolicy(
        initial_delay=0.5,
        multiplier=2,
        maximum_delay=30,
    )

    assert policy.delay_for_attempt(10_000) == 30


def test_discovered_peer_freshness_uses_explicit_ttl() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    discovered = DiscoveredPeer(
        peer=Peer(id="peer"),
        last_seen=now - timedelta(seconds=5),
    )

    assert discovered.freshness(5, now=now) is PeerFreshness.FRESH
    assert discovered.freshness(4.9, now=now) is PeerFreshness.EXPIRED
    assert discovered.is_fresh(None, now=now) is True


def test_discovered_peer_operational_ttl_uses_monotonic_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic = 100.0
    monkeypatch.setattr("paqto.core.discovered.time.monotonic", lambda: monotonic)
    discovered = DiscoveredPeer(peer=Peer(id="peer"))

    assert discovered.is_fresh(5, monotonic_now=104.9) is True
    assert discovered.is_fresh(5, monotonic_now=105.1) is False

    wall_jump = discovered.last_seen + timedelta(days=365)
    assert discovered.is_fresh(5, now=wall_jump) is False
    assert discovered.is_fresh(5, monotonic_now=101.0) is True


def test_discovered_peer_rejects_ambiguous_clock_inputs() -> None:
    discovered = DiscoveredPeer(peer=Peer(id="peer"))

    with pytest.raises(ValueError, match="mutually exclusive"):
        discovered.is_fresh(
            1,
            now=datetime.now(timezone.utc),
            monotonic_now=1,
        )


@pytest.mark.parametrize(
    "policy",
    [
        {"initial_delay": 0},
        {"multiplier": 0.5},
        {"maximum_delay": 0.1},
        {"jitter": 1.1},
        {"max_attempts": 0},
    ],
)
def test_invalid_reconnect_policy_is_rejected(policy: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        ReconnectPolicy(**policy)  # type: ignore[arg-type]


def test_heartbeat_requires_a_timeout() -> None:
    with pytest.raises(ValueError, match="heartbeat_timeout"):
        PaqtoConfig(heartbeat_interval=1, heartbeat_timeout=None)
