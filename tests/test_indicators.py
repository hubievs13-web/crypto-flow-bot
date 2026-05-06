"""Tests for the technical-indicator helpers added alongside trend filter / ATR sizing."""

from datetime import UTC, datetime

from crypto_flow_bot.config import Config
from crypto_flow_bot.data.binance import compute_atr, compute_ema
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore

# ─── compute_ema ────────────────────────────────────────────────────────────

def test_ema_returns_none_when_too_short():
    assert compute_ema([1.0, 2.0, 3.0], period=10) is None


def test_ema_on_constant_series_equals_constant():
    # Any EMA over a flat series collapses to that value.
    assert compute_ema([100.0] * 50, period=50) == 100.0


def test_ema_reacts_to_recent_values_more_than_old():
    # An upward-trending series longer than the period should produce an EMA
    # higher than the simple average over the same window — EMA weights the
    # latest values more.
    series = [float(i) for i in range(1, 81)]  # 1..80, period=50 -> 30 EMA updates
    ema = compute_ema(series, period=50)
    sma_full = sum(series) / len(series)
    assert ema is not None and ema > sma_full


# ─── compute_atr ────────────────────────────────────────────────────────────

def test_atr_returns_none_when_too_short():
    highs = [10.0, 11.0]
    lows = [9.0, 10.0]
    closes = [9.5, 10.5]
    assert compute_atr(highs, lows, closes, period=14) is None


def test_atr_on_flat_bars_is_bar_range():
    # Each bar has a $1 range and matching closes -> True Range = 1.0 every bar -> ATR = 1.0.
    n = 20
    highs = [101.0] * n
    lows = [100.0] * n
    closes = [100.5] * n
    atr = compute_atr(highs, lows, closes, period=14)
    assert atr is not None and abs(atr - 1.0) < 1e-9


def test_atr_grows_with_volatility():
    # Wider bars -> higher ATR.
    n = 20
    closes = [100.0] * n
    quiet = compute_atr([101.0] * n, [99.0] * n, closes, period=14)
    wild = compute_atr([110.0] * n, [90.0] * n, closes, period=14)
    assert quiet is not None and wild is not None and wild > quiet


# ─── ATR-based SL/TP sizing in StateStore ───────────────────────────────────

def _candidate(price: float, direction: Direction, atr: float | None) -> SignalCandidate:
    snap = Snapshot(
        symbol="BTCUSDT",
        ts=datetime.now(tz=UTC),
        price=price,
        atr_1h=atr,
    )
    return SignalCandidate(
        symbol="BTCUSDT",
        direction=direction,
        fired_rules=[FiredRule(name="funding_extreme", description="test")],
        snapshot=snap,
    )


def test_atr_sizing_long_position(tmp_path):
    cfg = Config(symbols=["BTCUSDT"])
    cfg.exits.atr_sizing.enabled = True
    cfg.exits.atr_sizing.sl_atr_mult = 2.0
    cfg.exits.atr_sizing.tp_atr_mults = [1.5, 3.0]
    store = StateStore(path=tmp_path)
    cand = _candidate(price=50_000.0, direction=Direction.LONG, atr=500.0)
    pos = store.open_from_signal(cand, cfg)
    # SL at price - 2 × ATR = 50_000 - 1000 = 49_000
    assert abs(pos.stop_loss_price - 49_000.0) < 1e-6
    # TP1 distance = 1.5 × 500 / 50_000 = 0.015 = 1.5%
    assert abs(pos.tp_levels[0].pct - 0.015) < 1e-6
    assert abs(pos.tp_levels[1].pct - 0.030) < 1e-6


def test_atr_sizing_short_position_uses_correct_sign(tmp_path):
    cfg = Config(symbols=["BTCUSDT"])
    store = StateStore(path=tmp_path)
    cand = _candidate(price=50_000.0, direction=Direction.SHORT, atr=500.0)
    pos = store.open_from_signal(cand, cfg)
    # SHORT -> SL is *above* entry. Default sl_atr_mult = 1.5 -> 50_000 + 750 = 50_750.
    assert abs(pos.stop_loss_price - 50_750.0) < 1e-6


def test_atr_sizing_falls_back_when_atr_missing(tmp_path):
    cfg = Config(symbols=["BTCUSDT"])
    cfg.exits.stop_loss_pct = 0.02  # explicit fallback %
    store = StateStore(path=tmp_path)
    cand = _candidate(price=50_000.0, direction=Direction.LONG, atr=None)
    pos = store.open_from_signal(cand, cfg)
    # Falls back to fixed 2% -> 50_000 × 0.98 = 49_000
    assert abs(pos.stop_loss_price - 49_000.0) < 1e-6
    # TPs come from cfg.exits.take_profit_levels (default [0.015, 0.030]).
    assert abs(pos.tp_levels[0].pct - 0.015) < 1e-6
