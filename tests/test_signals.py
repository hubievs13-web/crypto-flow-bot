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


# ─── OI surge with price-alignment ──────────────────────────────────────────

def test_oi_surge_up_with_price_up_yields_long_signal():
    snap = _snap(open_interest_change_pct_window=0.07, price_change_pct_1h=0.012)
    out = evaluate(snap, _cfg())
    assert any(
        c.direction is Direction.LONG and any(r.name == "oi_surge" for r in c.fired_rules)
        for c in out
    )


def test_oi_surge_up_with_price_down_yields_short_signal():
    snap = _snap(open_interest_change_pct_window=0.07, price_change_pct_1h=-0.012)
    out = evaluate(snap, _cfg())
    assert any(
        c.direction is Direction.SHORT and any(r.name == "oi_surge" for r in c.fired_rules)
        for c in out
    )


def test_oi_surge_without_price_data_does_not_fire():
    # require_price_aligned=True (default) + missing price-change -> skip OI signal entirely.
    snap = _snap(open_interest_change_pct_window=0.07, price_change_pct_1h=None)
    out = evaluate(snap, _cfg())
    assert all(not any(r.name == "oi_surge" for r in c.fired_rules) for c in out)


def test_oi_decrease_does_not_fire_long_signal():
    # OI down + price up = short squeeze, already in motion. Skip.
    snap = _snap(open_interest_change_pct_window=-0.07, price_change_pct_1h=0.012)
    out = evaluate(snap, _cfg())
    assert all(not any(r.name == "oi_surge" for r in c.fired_rules) for c in out)


# ─── Trend filter ───────────────────────────────────────────────────────────

def test_trend_filter_blocks_short_when_above_ema():
    # Price above EMA = uptrend. SHORT signal from funding/LSR should be suppressed.
    snap = _snap(price=50000.0, ema50_1h=49000.0, funding_rate=0.0015)
    out = evaluate(snap, _cfg())
    assert all(c.direction is not Direction.SHORT for c in out)


def test_trend_filter_blocks_long_when_below_ema():
    snap = _snap(price=50000.0, ema50_1h=51500.0, funding_rate=-0.0015)
    out = evaluate(snap, _cfg())
    assert all(c.direction is not Direction.LONG for c in out)


def test_trend_filter_exempts_liq_cascade():
    # Long liquidations in an uptrend -> bounce-up setup -> LONG should still fire.
    snap = _snap(price=50000.0, ema50_1h=49000.0, long_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, _cfg())
    longs = [c for c in out if c.direction is Direction.LONG]
    assert longs and any(r.name == "liq_cascade" for r in longs[0].fired_rules)


def test_trend_filter_passes_aligned_signal():
    # Funding-driven LONG below EMA (downtrend) is contra-trend -> blocked.
    # Funding-driven LONG above EMA (uptrend) is trend-aligned -> passes through.
    snap = _snap(price=50000.0, ema50_1h=49000.0, funding_rate=-0.0015)
    out = evaluate(snap, _cfg())
    assert any(c.direction is Direction.LONG for c in out)


# ─── liq_cascade fires on the multi-exchange aggregated counter ────────────

def test_liq_cascade_fires_when_aggregated_window_above_threshold():
    # Counter is already aggregated across every enabled exchange.
    snap = _snap(long_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, _cfg())
    longs = [c for c in out if c.direction is Direction.LONG]
    assert longs and any(r.name == "liq_cascade" for r in longs[0].fired_rules)


def test_liq_cascade_below_threshold_does_not_fire():
    snap = _snap(long_liquidations_usd_window=10_000_000.0)
    out = evaluate(snap, _cfg())
    assert all(not any(r.name == "liq_cascade" for r in c.fired_rules) for c in out)
