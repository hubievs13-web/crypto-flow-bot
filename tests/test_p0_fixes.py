"""Tests for the P0 safety fixes: funding cycle dedup, lookahead removal,
predicted-funding formula, separate predicted cache, EMA slope kline budget,
hard-block trend/slope gates, and hard-block stale-data gate.

Each test below maps directly to a bullet in the P0 list (P0-1..P0-7) so a
future regression is easy to bisect from the assertion message.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import (
    Config,
    FreshnessCfg,
    LiqCascadeCfg,
    LsrExtremeCfg,
    NotifierCfg,
    OiSurgeCfg,
    PredictedFundingCfg,
    SignalsCfg,
    TrendFilterCfg,
)
from crypto_flow_bot.data.binance import _compute_predicted_funding, _kline_derivatives
from crypto_flow_bot.engine.funding_history import FundingHistoryCache
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate


def _snap(**kw) -> Snapshot:
    base = {
        "symbol": "BTCUSDT",
        "ts": datetime.now(tz=UTC),
        "price": 100.0,
        "long_liquidations_usd_window": 0.0,
        "short_liquidations_usd_window": 0.0,
    }
    base.update(kw)
    return Snapshot(**base)


def _base_cfg(**signals_kw) -> Config:
    signals = SignalsCfg(funding_extreme_requires_confirmation=False, **signals_kw)
    return Config(symbols=["BTCUSDT"], notifier=NotifierCfg(), signals=signals)


# ─── P0-1 + P0-2 ──────────────────────────────────────────────────────────


def test_funding_history_dedupes_by_cycle_ts() -> None:
    """The cache must NOT log the same cycle ts twice in a row."""
    cache = FundingHistoryCache(max_points=10)
    base = datetime(2026, 5, 1, tzinfo=UTC)
    # Same cycle ts -> only the first observation is stored.
    cache.update("BTCUSDT", base, 0.0001)
    cache.update("BTCUSDT", base, 0.00015)  # duplicate cycle ts
    cache.update("BTCUSDT", base, 0.00020)  # duplicate cycle ts
    pts = cache.points_within("BTCUSDT", base + timedelta(days=1), 30)
    assert pts == [0.0001], "duplicate cycle observations leaked into history"


def test_funding_history_accepts_new_cycle_ts() -> None:
    cache = FundingHistoryCache(max_points=10)
    base = datetime(2026, 5, 1, tzinfo=UTC)
    cache.update("BTCUSDT", base, 0.0001)
    cache.update("BTCUSDT", base + timedelta(hours=8), 0.0002)
    cache.update("BTCUSDT", base + timedelta(hours=16), 0.0003)
    pts = cache.points_within("BTCUSDT", base + timedelta(days=1), 30)
    assert pts == [0.0001, 0.0002, 0.0003]


# ─── P0-3 ─────────────────────────────────────────────────────────────────


def test_predicted_funding_formula_uses_interest_minus_premium_clamp() -> None:
    """Premium=0.001, interest=0.002 -> adj=clamp(0.001, ±0.0005)=0.0005 -> 0.0015.

    The old buggy formula returned premium + interest = 0.003.
    """
    # mark/idx tuned so premium = 0.001 exactly.
    val = _compute_predicted_funding(
        mark_price=100.1, index_price=100.0, interest_rate=0.002, funding_cap=0.0075
    )
    assert val is not None
    assert abs(val - 0.0015) < 1e-12, f"expected 0.0015, got {val}"


def test_predicted_funding_clamps_to_funding_cap() -> None:
    """Even an extreme premium must outer-clamp to ±funding_cap."""
    # premium = 1.0 (100% above index), funding_cap = 0.0075.
    val = _compute_predicted_funding(
        mark_price=200.0, index_price=100.0, interest_rate=0.0, funding_cap=0.0075
    )
    assert val == 0.0075


def test_predicted_funding_zero_index_returns_none() -> None:
    assert _compute_predicted_funding(100.0, 0.0, 0.0001, 0.0075) is None


# ─── P0-5 ─────────────────────────────────────────────────────────────────


def test_ema_slope_actually_computed_with_sufficient_klines() -> None:
    """With 58+ bars (51 closed) the EMA50 slope must be non-None."""
    bars: list[list] = []
    # 58 bars -> 57 closed -> EMA50 series of length 8 -> slope_window=6 fits.
    for i in range(58):
        close = 100.0 + i * 0.01
        bars.append([0, "0", "0", "0", str(close), "0", 0, "0", 0, "0", "0", "0"])
    _, _, _, slope = _kline_derivatives(bars, slope_window_bars=6)
    assert slope is not None, "slope must compute when limit >= ema_period+slope_window+1"
    assert slope > 0, f"expected positive slope on an up trend, got {slope}"


# ─── P0-6 ─────────────────────────────────────────────────────────────────


def test_hard_block_on_4h_drops_misaligned_short() -> None:
    cfg = _base_cfg(trend_filter=TrendFilterCfg(hard_block_on_4h=True))
    # LSR triggers SHORT but 4h EMA is below price -> uptrend -> SHORT misaligned.
    snap = _snap(long_short_ratio=2.7, ema50_4h=90.0)
    out = evaluate(snap, cfg)
    assert all(c.direction is not Direction.SHORT for c in out), \
        "hard_block_on_4h must drop the candidate entirely, not just downgrade"


def test_hard_block_on_slope_drops_misaligned_long() -> None:
    cfg = _base_cfg(trend_filter=TrendFilterCfg(hard_block_on_slope=True))
    # LSR triggers LONG but 1h slope is -0.5% / 6h -> misaligned -> drop.
    snap = _snap(long_short_ratio=0.5, ema50_slope_1h=-0.005)
    out = evaluate(snap, cfg)
    assert all(c.direction is not Direction.LONG for c in out), \
        "hard_block_on_slope must drop the candidate entirely"


# ─── P0-7 ─────────────────────────────────────────────────────────────────


def test_hard_block_freshness_drops_everything_when_critical_stale() -> None:
    """Stale funding ts must abort ALL candidates, not just funding_extreme."""
    fresh = FreshnessCfg(
        enabled=True,
        hard_block_on_stale=True,
        missing_ts_is_stale=False,
        funding_max_age_seconds=60,
    )
    cfg = _base_cfg(freshness=fresh)
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    # liq_cascade is plenty to fire a candidate, but funding ts is 5min old.
    snap = _snap(
        ts=now,
        long_liquidations_usd_window=80_000_000.0,
        funding_rate=0.0001,
        funding_rate_ts=now - timedelta(minutes=5),
        open_interest_ts=now,
        long_short_ratio_ts=now,
        klines_1h_ts=now,
    )
    out = evaluate(snap, cfg)
    assert out == [], "hard-block freshness must return [] when any critical ts is stale"


def test_hard_block_freshness_missing_ts_is_stale() -> None:
    """When missing_ts_is_stale=True, an unset ts behaves like an old one."""
    fresh = FreshnessCfg(
        enabled=True,
        hard_block_on_stale=True,
        missing_ts_is_stale=True,
        funding_max_age_seconds=60,
    )
    cfg = _base_cfg(freshness=fresh)
    snap = _snap(long_liquidations_usd_window=80_000_000.0)  # no ts fields set
    out = evaluate(snap, cfg)
    assert out == [], "missing critical ts must hard-block when missing_ts_is_stale=True"


def test_hard_block_freshness_disabled_keeps_legacy_per_rule_behavior() -> None:
    """Defaults (hard_block_on_stale=False) preserve legacy per-rule semantics."""
    cfg = _base_cfg()  # defaults
    snap = _snap(long_liquidations_usd_window=80_000_000.0)
    out = evaluate(snap, cfg)
    assert any(c.direction is Direction.LONG for c in out), \
        "legacy mode (no hard-block) must still let liq_cascade through"


# ─── P0-3 + P0-4 (predicted funding signal still wired correctly) ────────


def test_predicted_funding_uses_own_thresholds_in_fixed_mode() -> None:
    """Predicted-funding fixed-mode path must use PredictedFundingCfg thresholds,
    not realized-funding ones (PR fix P0-4)."""
    pf = PredictedFundingCfg(
        enabled=True,
        mode="fixed",
        long_overheated_above=0.001,
        short_overheated_below=-0.001,
    )
    cfg = _base_cfg(
        predicted_funding=pf,
        oi_surge=OiSurgeCfg(enabled=False),
        lsr_extreme=LsrExtremeCfg(enabled=False),
        liq_cascade=LiqCascadeCfg(enabled=False),
    )
    # predicted_funding_rate > 0.001 must fire SHORT.
    out = evaluate(_snap(predicted_funding_rate=0.0015), cfg)
    assert any(c.direction is Direction.SHORT for c in out)
    # predicted_funding_rate < -0.001 must fire LONG.
    out2 = evaluate(_snap(predicted_funding_rate=-0.0015), cfg)
    assert any(c.direction is Direction.LONG for c in out2)
