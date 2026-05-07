"""Tests for state persistence — ensures new fields round-trip through save/load."""

from datetime import UTC, datetime

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore


def _cfg() -> Config:
    return Config(symbols=["BTCUSDT"])


def _snap(price: float = 100.0, atr: float | None = 1.0) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price, atr_1h=atr)


def test_last_liveness_ping_date_round_trips(tmp_path):
    """Daily liveness state field must persist across restarts."""
    store = StateStore(path=tmp_path)
    assert store.last_liveness_ping_date is None
    store.last_liveness_ping_date = "2025-11-04"
    store.save()
    fresh = StateStore(path=tmp_path)
    assert fresh.last_liveness_ping_date == "2025-11-04"


def test_position_strong_flag_round_trips(tmp_path):
    """STRONG (confluence) flag must persist across restarts."""
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[
            FiredRule(name="funding_extreme", description="x"),
            FiredRule(name="lsr_extreme", description="y"),
        ],
        snapshot=_snap(),
    )
    pos = store.open_from_signal(candidate, cfg)
    assert pos.strong is True
    store.save()
    fresh = StateStore(path=tmp_path)
    reloaded = list(fresh.positions.values())[0]
    assert reloaded.strong is True


def test_single_rule_position_not_strong(tmp_path):
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="funding_extreme", description="x")],
        snapshot=_snap(),
    )
    pos = store.open_from_signal(candidate, cfg)
    assert pos.strong is False
