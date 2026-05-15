from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config, SignalsCfg
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import ConfluenceCache, evaluate


def _cfg(funding_requires_confirmation: bool = True) -> Config:
    return Config(
        symbols=["BTCUSDT"],
        signals=SignalsCfg(
            funding_extreme_requires_confirmation=funding_requires_confirmation,
        ),
    )


def _snap(**overrides) -> Snapshot:
    base = {"symbol": "BTCUSDT", "ts": datetime.now(tz=UTC), "price": 50000.0}
    base.update(overrides)
    return Snapshot(**base)


def test_funding_alone_blocked_by_confirmation_gate():
    """funding_extreme firing alone (no other rule in the confluence window)
    must produce NO candidate when the confirmation gate is on."""
    snap = _snap(funding_rate=0.0010)
    out = evaluate(snap, _cfg())
    assert out == []


def test_funding_alone_with_gate_off_yields_signal():
    """With the confirmation gate explicitly disabled, funding_extreme alone
    is allowed to open a candidate (legacy behavior)."""
    snap = _snap(funding_rate=0.0010)
    out = evaluate(snap, _cfg(funding_requires_confirmation=False))
    assert len(out) == 1
    assert out[0].direction is Direction.SHORT
    assert any(r.name == "funding_extreme" for r in out[0].fired_rules)


def test_funding_with_lsr_confirming_yields_signal():
    """funding_extreme + lsr_extreme in the same direction on the same
    snapshot passes the confirmation gate."""
    snap = _snap(funding_rate=0.0010, long_short_ratio=2.7)
    out = evaluate(snap, _cfg())
    assert len(out) == 1
    assert out[0].direction is Direction.SHORT
    rule_names = {r.name for r in out[0].fired_rules}
    assert {"funding_extreme", "lsr_extreme"}.issubset(rule_names)


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


# ─── Cross-snapshot confluence window ──────────────────────────────────────

def test_confluence_window_lets_funding_team_up_with_earlier_liq():
    """liq_cascade fired 5 min ago → funding fires now alone → entry allowed
    because the cache remembers the earlier non-funding rule."""
    cache = ConfluenceCache(window_minutes=30)
    now = datetime.now(tz=UTC)
    # 5 min ago: liq cascade fires SHORT on its own.
    earlier = _snap(
        ts=now - timedelta(minutes=5),
        short_liquidations_usd_window=80_000_000.0,
    )
    evaluate(earlier, _cfg(), cache=cache, now=earlier.ts)
    # Now: only funding fires SHORT. Without the cache this would be blocked;
    # with it, the candidate is allowed.
    later = _snap(ts=now, funding_rate=0.0010)
    out = evaluate(later, _cfg(), cache=cache, now=later.ts)
    assert len(out) == 1
    assert out[0].direction is Direction.SHORT
    assert {"funding_extreme", "liq_cascade"}.issubset(out[0].confluence_window_rules)


def test_confluence_window_drops_stale_partners():
    """A liq_cascade older than the window must not rescue a funding-only
    candidate."""
    cache = ConfluenceCache(window_minutes=30)
    now = datetime.now(tz=UTC)
    stale = _snap(
        ts=now - timedelta(minutes=45),
        short_liquidations_usd_window=80_000_000.0,
    )
    evaluate(stale, _cfg(), cache=cache, now=stale.ts)
    later = _snap(ts=now, funding_rate=0.0010)
    out = evaluate(later, _cfg(), cache=cache, now=later.ts)
    assert out == []


def test_confluence_window_keyed_by_direction():
    """A liq_cascade in LONG direction must NOT rescue a SHORT funding
    candidate — confluence is per (symbol, direction)."""
    cache = ConfluenceCache(window_minutes=30)
    now = datetime.now(tz=UTC)
    long_only = _snap(
        ts=now - timedelta(minutes=5),
        long_liquidations_usd_window=80_000_000.0,
    )
    evaluate(long_only, _cfg(), cache=cache, now=long_only.ts)
    short_funding = _snap(ts=now, funding_rate=0.0010)
    out = evaluate(short_funding, _cfg(), cache=cache, now=short_funding.ts)
    assert out == []


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
    # Gate is off in this scenario because funding alone is the rule under test.
    snap = _snap(price=50000.0, ema50_1h=49000.0, funding_rate=-0.0015)
    out = evaluate(snap, _cfg(funding_requires_confirmation=False))
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


# ─── STRONG: 2+ non-funding rules within the confluence window ─────────────

def test_single_non_funding_rule_is_not_strong():
    """One non-funding rule fired = regular signal, not strong."""
    snap = _snap(long_short_ratio=2.7)  # SHORT via LSR alone
    out = evaluate(snap, _cfg())
    assert len(out) == 1
    assert out[0].is_strong is False


def test_funding_plus_one_non_funding_rule_is_not_strong():
    """funding_extreme + lsr_extreme is the regular entry condition, NOT a
    strong confluence. STRONG is reserved for two *fast* triggers."""
    snap = _snap(funding_rate=0.0012, long_short_ratio=2.7)
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT]
    assert short
    rule_names = {r.name for r in short[0].fired_rules}
    assert {"funding_extreme", "lsr_extreme"}.issubset(rule_names)
    assert short[0].is_strong is False


def test_two_non_funding_rules_is_strong():
    """LSR (longs crowded) + short-side liq cascade both yield SHORT →
    two non-funding rules → strong confluence."""
    snap = _snap(
        long_short_ratio=2.7,
        short_liquidations_usd_window=80_000_000.0,
    )
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT]
    assert short
    rule_names = {r.name for r in short[0].fired_rules}
    assert {"lsr_extreme", "liq_cascade"}.issubset(rule_names)
    assert short[0].is_strong is True


def test_rules_split_across_directions_not_strong_for_either():
    """funding is + (SHORT signal) and LSR shows shorts heavy (LONG signal) →
    opposite directions, neither is a strong confluence. The funding-only
    SHORT candidate gets dropped by the confirmation gate; the LSR-only
    LONG candidate is allowed but is not strong (one non-funding rule)."""
    snap = _snap(funding_rate=0.0012, long_short_ratio=0.5)
    out = evaluate(snap, _cfg())
    for c in out:
        assert c.is_strong is False


# ─── Per-symbol threshold overrides ────────────────────────────────────────

from crypto_flow_bot.config import (  # noqa: E402
    FundingExtremeCfg,
    LiqCascadeCfg,
    LsrExtremeCfg,
    SymbolOverridesCfg,
)


def _cfg_with_per_symbol(funding_requires_confirmation: bool = False) -> Config:
    """Two symbols: BTC has very tight thresholds, SOL has loose ones.

    The funding-confirmation gate defaults to OFF here so per-symbol
    threshold tests can observe the underlying rule-firing behavior in
    isolation; the gate's own coverage lives in the earlier section.
    """
    return Config(
        symbols=["BTCUSDT", "SOLUSDT"],
        signals=SignalsCfg(
            funding_extreme=FundingExtremeCfg(long_overheated_above=0.0010,
                                              short_overheated_below=-0.0008),
            funding_extreme_requires_confirmation=funding_requires_confirmation,
            per_symbol={
                "BTCUSDT": SymbolOverridesCfg(
                    funding_extreme=FundingExtremeCfg(long_overheated_above=0.0005,
                                                     short_overheated_below=-0.0004),
                    liq_cascade=LiqCascadeCfg(usd_threshold=50_000_000),
                ),
                "SOLUSDT": SymbolOverridesCfg(
                    lsr_extreme=LsrExtremeCfg(long_heavy_above=2.5, short_heavy_below=0.6),
                    liq_cascade=LiqCascadeCfg(usd_threshold=15_000_000),
                ),
            },
        ),
    )


def test_for_symbol_returns_global_when_no_overrides():
    cfg = _cfg()  # uses global SignalsCfg defaults, no per_symbol
    eff = cfg.signals.for_symbol("BTCUSDT")
    assert eff is cfg.signals  # short-circuit, same instance


def test_for_symbol_applies_only_specified_fields():
    cfg = _cfg_with_per_symbol()
    btc = cfg.signals.for_symbol("BTCUSDT")
    # funding overridden -> tighter (BTC at 0.0005 vs global 0.0010)
    assert btc.funding_extreme.long_overheated_above == 0.0005
    # LSR not overridden -> falls back to whatever global is (default LsrExtremeCfg)
    assert btc.lsr_extreme.long_heavy_above == cfg.signals.lsr_extreme.long_heavy_above


def test_per_symbol_funding_threshold_fires_on_btc_only():
    """Funding +0.06% triggers BTC (override 0.05%) but NOT global (0.10%).

    Confirmation gate is off here so we test rule firing only.
    """
    cfg = _cfg_with_per_symbol()  # confirmation gate off by default in this helper
    snap_btc = _snap(symbol="BTCUSDT", funding_rate=0.0006)
    snap_eth = _snap(symbol="ETHUSDT", funding_rate=0.0006)  # no override -> global
    assert any(any(r.name == "funding_extreme" for r in c.fired_rules)
               for c in evaluate(snap_btc, cfg))
    assert not any(any(r.name == "funding_extreme" for r in c.fired_rules)
                   for c in evaluate(snap_eth, cfg))


def test_per_symbol_liq_threshold_fires_on_sol_only():
    """$25M long-liq triggers SOL (override $15M) but NOT BTC (override $50M)."""
    cfg = _cfg_with_per_symbol()
    snap_sol = _snap(symbol="SOLUSDT", long_liquidations_usd_window=25_000_000)
    snap_btc = _snap(symbol="BTCUSDT", long_liquidations_usd_window=25_000_000)
    assert any(any(r.name == "liq_cascade" for r in c.fired_rules)
               for c in evaluate(snap_sol, cfg))
    assert not any(any(r.name == "liq_cascade" for r in c.fired_rules)
                   for c in evaluate(snap_btc, cfg))


def test_unknown_symbol_uses_global_defaults():
    cfg = _cfg_with_per_symbol()
    eff = cfg.signals.for_symbol("DOGEUSDT")
    assert eff.funding_extreme.long_overheated_above == cfg.signals.funding_extreme.long_overheated_above


# ─── Freshness gate ────────────────────────────────────────────────────────

def test_freshness_gate_drops_stale_funding_rule():
    """A funding metric older than freshness.funding_max_age_seconds is
    ignored even if its value would otherwise cross the threshold."""
    now = datetime.now(tz=UTC)
    snap = _snap(
        ts=now,
        funding_rate=0.0010,
        funding_rate_ts=now - timedelta(seconds=300),  # 5 min old > default 120s
        long_short_ratio=2.7,
        long_short_ratio_ts=now,
    )
    out = evaluate(snap, _cfg())
    rule_names = {r.name for c in out for r in c.fired_rules}
    assert "funding_extreme" not in rule_names
    # LSR remains fresh and is allowed to fire.
    assert "lsr_extreme" in rule_names


def test_freshness_gate_drops_stale_lsr_rule():
    now = datetime.now(tz=UTC)
    snap = _snap(
        ts=now,
        long_short_ratio=2.7,
        long_short_ratio_ts=now - timedelta(seconds=900),  # 15 min > default 600s
    )
    out = evaluate(snap, _cfg())
    rule_names = {r.name for c in out for r in c.fired_rules}
    assert "lsr_extreme" not in rule_names


def test_freshness_gate_disabled_lets_stale_metric_fire():
    """With freshness.enabled=False the timestamp is ignored."""
    from crypto_flow_bot.config import FreshnessCfg
    now = datetime.now(tz=UTC)
    cfg = Config(
        symbols=["BTCUSDT"],
        signals=SignalsCfg(
            funding_extreme_requires_confirmation=False,
            freshness=FreshnessCfg(enabled=False),
        ),
    )
    snap = _snap(
        ts=now,
        funding_rate=0.0010,
        funding_rate_ts=now - timedelta(hours=2),  # very stale
    )
    out = evaluate(snap, cfg)
    rule_names = {r.name for c in out for r in c.fired_rules}
    assert "funding_extreme" in rule_names


def test_freshness_gate_treats_missing_ts_as_fresh():
    """An older Snapshot loaded from state has no per-metric ts; rules
    must still fire — the gate only kicks in when we have a ts and it's old."""
    snap = _snap(funding_rate=0.0010, long_short_ratio=2.7)
    # funding_rate_ts / long_short_ratio_ts default to None.
    out = evaluate(snap, _cfg())
    rule_names = {r.name for c in out for r in c.fired_rules}
    assert {"funding_extreme", "lsr_extreme"}.issubset(rule_names)


def test_freshness_per_metric_zero_disables_individual_gate():
    """`*_max_age_seconds: 0` disables the gate for that metric only."""
    from crypto_flow_bot.config import FreshnessCfg
    now = datetime.now(tz=UTC)
    cfg = Config(
        symbols=["BTCUSDT"],
        signals=SignalsCfg(
            funding_extreme_requires_confirmation=False,
            freshness=FreshnessCfg(funding_max_age_seconds=0),  # never stale
        ),
    )
    snap = _snap(
        ts=now,
        funding_rate=0.0010,
        funding_rate_ts=now - timedelta(hours=2),
    )
    out = evaluate(snap, cfg)
    rule_names = {r.name for c in out for r in c.fired_rules}
    assert "funding_extreme" in rule_names
