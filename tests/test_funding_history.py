"""Unit tests for FundingHistoryCache + the auto-mode funding_extreme rule."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import (
    Config,
    FundingExtremeCfg,
    LiqCascadeCfg,
    LsrExtremeCfg,
    NotifierCfg,
    OiSurgeCfg,
    SignalsCfg,
)
from crypto_flow_bot.engine.funding_history import FundingHistoryCache
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate

# ─── FundingHistoryCache mechanics ─────────────────────────────────────────


def _ts(base: datetime, hours: float) -> datetime:
    return base + timedelta(hours=hours)


def test_backfill_sorts_and_caps_to_max_points():
    cache = FundingHistoryCache(max_points=3)
    base = datetime(2026, 5, 1, tzinfo=UTC)
    # Out of order, more than capacity: only the 3 newest survive after sort.
    points = [
        (_ts(base, 8), 0.0001),
        (_ts(base, 0), 0.0000),
        (_ts(base, 24), 0.0003),
        (_ts(base, 16), 0.0002),
        (_ts(base, 32), 0.0004),
    ]
    n = cache.backfill("BTCUSDT", points)
    assert n == 3
    assert cache.size("BTCUSDT") == 3
    # Window covering all stored points -> the 3 newest.
    window = cache.points_within("BTCUSDT", _ts(base, 40), days=10)
    assert window == [0.0002, 0.0003, 0.0004]


def test_update_dedupes_older_or_equal_timestamps():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    cache.update("BTCUSDT", _ts(base, 0), 0.0001)
    cache.update("BTCUSDT", _ts(base, 0), 0.0002)  # same ts -> dropped
    cache.update("BTCUSDT", _ts(base, -1), 0.0003)  # older -> dropped
    cache.update("BTCUSDT", _ts(base, 8), 0.0004)  # newer -> kept
    assert cache.size("BTCUSDT") == 2


def test_zscore_returns_none_with_insufficient_points():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    for i in range(5):
        cache.update("BTCUSDT", _ts(base, i * 8), 0.0001)
    z = cache.zscore("BTCUSDT", 0.0010, _ts(base, 40), lookback_days=14, min_points=20)
    assert z is None


def test_zscore_returns_none_with_zero_variance():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    # 30 flat observations -> variance is 0.
    for i in range(30):
        cache.update("BTCUSDT", _ts(base, i * 8), 0.0001)
    z = cache.zscore(
        "BTCUSDT",
        0.0010,
        _ts(base, 30 * 8),
        lookback_days=30,
        min_points=20,
    )
    assert z is None


def test_zscore_matches_textbook_formula():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    # Known distribution: rates 0.0, 0.0002, 0.0004, ..., 0.0058 (30 pts).
    values = [i * 0.0002 for i in range(30)]
    for i, v in enumerate(values):
        cache.update("BTCUSDT", _ts(base, i * 8), v)
    mean = sum(values) / len(values)
    var = sum((x - mean) ** 2 for x in values) / len(values)
    expected = (0.01 - mean) / math.sqrt(var)
    z = cache.zscore(
        "BTCUSDT",
        0.01,
        _ts(base, 30 * 8 + 1),
        lookback_days=30,
        min_points=20,
    )
    assert z is not None
    assert math.isclose(z, expected, rel_tol=1e-6)


def test_percentile_rank_handles_extremes():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    for i in range(30):
        cache.update("BTCUSDT", _ts(base, i * 8), i * 0.0001)  # 0 .. 0.0029
    # value above all observations -> rank 1.0 ("longs overheated")
    p_top = cache.percentile_rank(
        "BTCUSDT", 1.0, _ts(base, 30 * 8 + 1), lookback_days=30, min_points=20
    )
    assert p_top == 1.0
    # value below all observations -> rank 0.0
    p_bot = cache.percentile_rank(
        "BTCUSDT", -1.0, _ts(base, 30 * 8 + 1), lookback_days=30, min_points=20
    )
    assert p_bot == 0.0


def test_percentile_rank_lookback_filters_out_old_points():
    cache = FundingHistoryCache()
    base = datetime(2026, 5, 1, tzinfo=UTC)
    # 30 old points and 5 fresh.
    for i in range(30):
        cache.update("BTCUSDT", _ts(base, i * 8), 0.001)
    fresh_base = _ts(base, 60 * 24)  # +60 days later
    for i in range(5):
        cache.update("BTCUSDT", _ts(fresh_base, i * 8), 0.0001)
    # 7-day window -> 5 fresh pts, all < query -> rank 1.0
    p = cache.percentile_rank(
        "BTCUSDT",
        0.01,
        _ts(fresh_base, 5 * 8),
        lookback_days=7,
        min_points=5,
    )
    assert p == 1.0


# ─── Auto-mode signal integration ──────────────────────────────────────────


def _cfg(funding_cfg: FundingExtremeCfg) -> Config:
    """Build a minimal Config wrapping just the funding_extreme subconfig.

    Other rules are disabled so they cannot contaminate the assertion;
    confluence is disabled so `funding_extreme` can fire on its own.
    """
    return Config(
        symbols=["BTCUSDT"],
        poll_interval_seconds=60,
        notifier=NotifierCfg(),
        signals=SignalsCfg(
            funding_extreme=funding_cfg,
            oi_surge=OiSurgeCfg(enabled=False),
            lsr_extreme=LsrExtremeCfg(enabled=False),
            liq_cascade=LiqCascadeCfg(enabled=False),
            funding_extreme_requires_confirmation=False,
        ),
    )


def _snap(**fields) -> Snapshot:
    base = {
        "symbol": "BTCUSDT",
        "ts": datetime.now(tz=UTC),
        "price": 100.0,
        "funding_rate": None,
        "open_interest_usd": None,
        "open_interest_change_pct_window": None,
        "long_short_ratio": None,
        "long_liquidations_usd_window": 0.0,
        "short_liquidations_usd_window": 0.0,
    }
    base.update(fields)
    return Snapshot(**base)


def test_auto_mode_zscore_extreme_fires_short():
    """High positive z -> SHORT (longs overheated)."""
    cfg = _cfg(FundingExtremeCfg(mode="auto", zscore_high_abs=2.0))
    snap = _snap(funding_rate=0.0010, funding_rate_zscore=2.5)
    candidates = evaluate(snap, cfg)
    shorts = [c for c in candidates if c.direction is Direction.SHORT]
    assert len(shorts) == 1
    rule_names = {r.name for r in shorts[0].fired_rules}
    assert "funding_extreme" in rule_names


def test_auto_mode_zscore_extreme_fires_long_on_negative():
    """Deep negative z -> LONG (shorts overheated)."""
    cfg = _cfg(FundingExtremeCfg(mode="auto", zscore_high_abs=2.0))
    snap = _snap(funding_rate=-0.0010, funding_rate_zscore=-2.5)
    candidates = evaluate(snap, cfg)
    longs = [c for c in candidates if c.direction is Direction.LONG]
    assert len(longs) == 1


def test_auto_mode_percentile_extreme_fires_short():
    """High percentile (>=0.95) -> SHORT, even with weak z-score."""
    cfg = _cfg(FundingExtremeCfg(mode="auto", pct_high=0.95))
    snap = _snap(
        funding_rate=0.0010,
        funding_rate_zscore=0.5,  # not extreme on z
        funding_rate_percentile=0.97,  # extreme on percentile
    )
    candidates = evaluate(snap, cfg)
    shorts = [c for c in candidates if c.direction is Direction.SHORT]
    assert len(shorts) == 1


def test_auto_mode_with_stats_below_threshold_does_not_fire():
    """When stats are computed but neither crosses, the rule must not fire,
    EVEN if the fixed thresholds would have."""
    cfg = _cfg(
        FundingExtremeCfg(
            mode="auto",
            zscore_high_abs=2.0,
            pct_high=0.95,
            pct_low=0.05,
            long_overheated_above=0.0001,  # would normally fire on 0.0010
        )
    )
    snap = _snap(
        funding_rate=0.0010,
        funding_rate_zscore=1.0,  # not extreme
        funding_rate_percentile=0.70,  # not extreme
    )
    candidates = evaluate(snap, cfg)
    assert candidates == []


def test_auto_mode_cold_start_falls_back_to_fixed_thresholds():
    """When zscore AND percentile are both None (cache too thin), auto mode
    must fall back to the fixed thresholds so day-1 alerts keep flowing."""
    cfg = _cfg(
        FundingExtremeCfg(
            mode="auto",
            long_overheated_above=0.0005,
        )
    )
    snap = _snap(
        funding_rate=0.0010,
        funding_rate_zscore=None,
        funding_rate_percentile=None,
    )
    candidates = evaluate(snap, cfg)
    shorts = [c for c in candidates if c.direction is Direction.SHORT]
    assert len(shorts) == 1


def test_fixed_mode_ignores_stats_even_if_present():
    """`mode=fixed` ignores zscore/percentile and only uses thresholds."""
    cfg = _cfg(
        FundingExtremeCfg(
            mode="fixed",
            long_overheated_above=0.0020,  # 0.0010 below this -> no fire
        )
    )
    snap = _snap(
        funding_rate=0.0010,
        funding_rate_zscore=3.0,  # would fire in auto mode
        funding_rate_percentile=0.99,
    )
    candidates = evaluate(snap, cfg)
    assert candidates == []
