from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.exits import evaluate_exit
from crypto_flow_bot.engine.models import Direction, Position, Snapshot, TpLevelState


def _cfg() -> Config:
    return Config(symbols=["BTCUSDT"])


def _long_position(
    entry: float = 100.0,
    age_minutes: float = 0.0,
    reason: str = "funding_extreme",
    metrics_at_entry: dict | None = None,
) -> Position:
    cfg = _cfg()
    sl = entry * (1 - cfg.exits.stop_loss_pct)
    return Position(
        id="t1",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=entry,
        entry_ts=datetime.now(tz=UTC) - timedelta(minutes=age_minutes),
        reason=reason,
        reason_metric_at_entry=metrics_at_entry or {},
        stop_loss_price=sl,
        initial_stop_loss_price=sl,
        tp_levels=[TpLevelState(pct=lvl.pct, fraction=lvl.fraction) for lvl in cfg.exits.take_profit_levels],
    )


def _short_position(
    entry: float = 100.0,
    age_minutes: float = 0.0,
    reason: str = "funding_extreme",
    metrics_at_entry: dict | None = None,
) -> Position:
    cfg = _cfg()
    sl = entry * (1 + cfg.exits.stop_loss_pct)
    return Position(
        id="t1",
        symbol="BTCUSDT",
        direction=Direction.SHORT,
        entry_price=entry,
        entry_ts=datetime.now(tz=UTC) - timedelta(minutes=age_minutes),
        reason=reason,
        reason_metric_at_entry=metrics_at_entry or {},
        stop_loss_price=sl,
        initial_stop_loss_price=sl,
        tp_levels=[TpLevelState(pct=lvl.pct, fraction=lvl.fraction) for lvl in cfg.exits.take_profit_levels],
    )


def _snap(price: float) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price)


def test_long_sl_hit_triggers_full_close():
    pos = _long_position(entry=100.0)
    # Drop below SL (1.5% = 98.5)
    events = evaluate_exit(pos, _snap(98.0), _cfg())
    kinds = [e.kind for e in events]
    assert "SL_HIT" in kinds
    assert events[-1].fraction_closed == pos.open_fraction


def test_short_sl_hit_triggers_full_close():
    pos = _short_position(entry=100.0)
    events = evaluate_exit(pos, _snap(102.0), _cfg())
    kinds = [e.kind for e in events]
    assert "SL_HIT" in kinds


def test_first_tp_level_fires_then_second():
    pos = _long_position(entry=100.0)
    # +1.5% -> first TP
    events = evaluate_exit(pos, _snap(101.5), _cfg())
    tp_events = [e for e in events if e.kind == "TP_HIT"]
    assert len(tp_events) == 1
    assert tp_events[0].fraction_closed == 0.5
    # First level marked hit
    assert pos.tp_levels[0].hit
    # Now jump to +3% -> second TP fires (first won't re-fire)
    events2 = evaluate_exit(pos, _snap(103.0), _cfg())
    tp_events2 = [e for e in events2 if e.kind == "TP_HIT"]
    assert len(tp_events2) == 1
    assert tp_events2[0].fraction_closed == 0.5


def test_trailing_locks_in_minimum_profit_after_activation():
    pos = _long_position(entry=100.0)
    cfg = _cfg()
    assert pos.stop_loss_price < 100.0  # initial SL below entry
    # Hit +1.5% -> trailing activates with default lock_in_pct=0.005,
    # so SL moves to entry * (1 + 0.005) = 100.5 (locks in +0.5%).
    events = evaluate_exit(pos, _snap(101.5), cfg)
    trail = [e for e in events if e.kind == "TRAILING_MOVE"]
    assert len(trail) == 1
    expected_sl = 100.0 * (1 + cfg.exits.trailing.lock_in_pct)
    assert trail[0].new_stop_loss_price == expected_sl
    assert trail[0].new_stop_loss_price > 100.0  # locked into profit, not just BE


def test_time_stop_after_configured_minutes():
    cfg = _cfg()
    pos = _long_position(entry=100.0, age_minutes=cfg.exits.time_stop_minutes + 1)
    events = evaluate_exit(pos, _snap(100.5), cfg)
    assert any(e.kind == "TIME_STOP" for e in events)


def test_reason_invalidation_on_funding_retracement():
    """Entry funding +0.01%, current +0.004% -> 60% retraced (>50% default)."""
    cfg = _cfg()
    pos = _short_position(
        entry=100.0,
        reason="funding_extreme",
        metrics_at_entry={"funding_rate": 0.0001},  # +0.010%
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, funding_rate=0.00004
    )
    events = evaluate_exit(pos, snap, cfg)
    assert any(e.kind == "REASON_INVALIDATED" for e in events)


def test_funding_reason_does_not_invalidate_at_entry_value():
    """Regression: funding gates used to fire immediately because the
    normalization threshold was wider than per-symbol entry triggers. Under
    retrace_pct semantics, an entry at +0.010% must NOT invalidate while
    funding is still at +0.010%."""
    cfg = _cfg()
    pos = _short_position(
        entry=100.0,
        reason="funding_extreme",
        metrics_at_entry={"funding_rate": 0.0001},
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, funding_rate=0.0001
    )
    events = evaluate_exit(pos, snap, cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_funding_reason_invalidates_on_sign_flip():
    """A flip from +0.010% to -0.001% counts as over-retraced -> close."""
    cfg = _cfg()
    pos = _short_position(
        entry=100.0,
        reason="funding_extreme",
        metrics_at_entry={"funding_rate": 0.0001},
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, funding_rate=-0.00001
    )
    events = evaluate_exit(pos, snap, cfg)
    assert any(e.kind == "REASON_INVALIDATED" for e in events)


def test_funding_reason_no_invalidation_without_entry_metric():
    """Backward compat for old positions reloaded from state without the
    reason_metric_at_entry payload — the gate must simply skip rather than
    raise or false-fire."""
    cfg = _cfg()
    pos = _short_position(entry=100.0, reason="funding_extreme")  # no metrics
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, funding_rate=0.00001
    )
    events = evaluate_exit(pos, snap, cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_lsr_reason_invalidation_on_retracement():
    """Entry LSR 2.5 (SHORT entry), current 1.5 -> ~67% retraced toward 1.0."""
    cfg = _cfg()
    pos = _short_position(
        entry=100.0,
        reason="lsr_extreme",
        metrics_at_entry={"long_short_ratio": 2.5},
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, long_short_ratio=1.5
    )
    events = evaluate_exit(pos, snap, cfg)
    assert any(e.kind == "REASON_INVALIDATED" for e in events)


def test_lsr_reason_does_not_invalidate_near_entry():
    """Entry LSR 2.5, current 2.4 -> only 7% retraced -> must NOT invalidate."""
    cfg = _cfg()
    pos = _short_position(
        entry=100.0,
        reason="lsr_extreme",
        metrics_at_entry={"long_short_ratio": 2.5},
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=99.9, long_short_ratio=2.4
    )
    events = evaluate_exit(pos, snap, cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_lsr_long_reason_invalidation_symmetry():
    """LONG entry at LSR 0.65, current 0.85 -> ~57% retraced toward 1.0."""
    cfg = _cfg()
    pos = _long_position(
        entry=100.0,
        reason="lsr_extreme",
        metrics_at_entry={"long_short_ratio": 0.65},
    )
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.1, long_short_ratio=0.85
    )
    events = evaluate_exit(pos, snap, cfg)
    assert any(e.kind == "REASON_INVALIDATED" for e in events)


def test_momentum_reversal_invalidates_oi_surge_long():
    """LONG opened on oi_surge — price drops 0.6% in first 30m → bail out."""
    cfg = _cfg()
    pos = _long_position(entry=100.0, age_minutes=30, reason="oi_surge")
    # Drop 0.6% — beyond default 0.5% threshold, but still above SL.
    events = evaluate_exit(pos, _snap(99.4), cfg)
    invalidated = [e for e in events if e.kind == "REASON_INVALIDATED"]
    assert len(invalidated) == 1
    assert "reversed" in invalidated[0].description
    assert invalidated[0].fraction_closed == pos.open_fraction


def test_momentum_reversal_invalidates_liq_cascade_short():
    """SHORT opened on liq_cascade — price pumps 0.6% in first 45m → bail out."""
    cfg = _cfg()
    pos = _short_position(entry=100.0, age_minutes=45, reason="liq_cascade")
    events = evaluate_exit(pos, _snap(100.6), cfg)
    invalidated = [e for e in events if e.kind == "REASON_INVALIDATED"]
    assert len(invalidated) == 1
    assert "reversed" in invalidated[0].description


def test_momentum_reversal_below_threshold_does_not_invalidate():
    """0.3% reverse (below 0.5% threshold) is just noise."""
    cfg = _cfg()
    pos = _long_position(entry=100.0, age_minutes=30, reason="oi_surge")
    events = evaluate_exit(pos, _snap(99.7), cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_momentum_reversal_outside_window_does_not_invalidate():
    """After window expires (default 60m), no momentum-reversal exit."""
    cfg = _cfg()
    pos = _long_position(entry=100.0, age_minutes=90, reason="oi_surge")
    events = evaluate_exit(pos, _snap(99.4), cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_momentum_reversal_does_not_apply_to_funding_only():
    """Funding-only reason still uses metric-normalization gate, not momentum."""
    cfg = _cfg()
    pos = _long_position(entry=100.0, age_minutes=30, reason="funding_extreme")
    # 0.6% drop, no funding metric on snapshot — must NOT invalidate.
    events = evaluate_exit(pos, _snap(99.4), cfg)
    assert not any(e.kind == "REASON_INVALIDATED" for e in events)


def test_no_events_when_position_calm():
    pos = _long_position(entry=100.0)
    events = evaluate_exit(pos, _snap(100.5), _cfg())
    # Slight move up — no SL, no TP, no trailing, no time stop, no reason data.
    assert events == []
