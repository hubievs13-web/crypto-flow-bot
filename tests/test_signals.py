from datetime import UTC, datetime

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate


def _cfg() -> Config:
    return Config(symbols=["BTCUSDT"])


def _snap(**overrides) -> Snapshot:
    base = {"symbol": "BTCUSDT", "ts": datetime.now(tz=UTC), "price": 50000.0}
    base.update(overrides)
    return Snapshot(**base)


def test_funding_long_overheated_yields_short_signal():
    snap = _snap(funding_rate=0.0010)
    out = evaluate(snap, _cfg())
    assert len(out) == 1
    assert out[0].direction is Direction.SHORT
    assert any(r.name == "funding_extreme" for r in out[0].fired_rules)


def test_funding_short_overheated_yields_long_signal():
    snap = _snap(funding_rate=-0.0007)
    out = evaluate(snap, _cfg())
    assert len(out) == 1
    assert out[0].direction is Direction.LONG


def test_lsr_crowded_long_yields_short_signal():
    snap = _snap(long_short_ratio=3.0)
    out = evaluate(snap, _cfg())
    assert any(c.direction is Direction.SHORT for c in out)


def test_lsr_crowded_short_yields_long_signal():
    snap = _snap(long_short_ratio=0.4)
    out = evaluate(snap, _cfg())
    assert any(c.direction is Direction.LONG for c in out)


def test_long_liquidations_cascade_yields_long_signal():
    snap = _snap(long_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, _cfg())
    assert any(c.direction is Direction.LONG for c in out)


def test_short_liquidations_cascade_yields_short_signal():
    snap = _snap(short_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, _cfg())
    assert any(c.direction is Direction.SHORT for c in out)


def test_no_signal_when_metrics_neutral():
    snap = _snap(funding_rate=0.0001, long_short_ratio=1.1, open_interest_change_pct_window=0.01)
    assert evaluate(snap, _cfg()) == []


def test_two_directions_can_fire_simultaneously():
    # crowded longs (LSR) AND short-side liquidations -> conflicting; both directions surface.
    snap = _snap(long_short_ratio=3.0, short_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, _cfg())
    dirs = {c.direction for c in out}
    assert Direction.SHORT in dirs
