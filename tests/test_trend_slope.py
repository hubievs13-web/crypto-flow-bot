from datetime import UTC, datetime

from crypto_flow_bot.config import Config, SignalsCfg
from crypto_flow_bot.data.binance import _kline_derivatives
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate


def _cfg() -> Config:
    c = Config(symbols=["BTCUSDT"], signals=SignalsCfg(funding_extreme_requires_confirmation=False))
    return c


def _snap(**kw):
    base = dict(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0)
    base.update(kw)
    return Snapshot(**base)


def test_slope_from_synthetic_klines_positive_negative_flat():
    up = [[0, "0", "0", "0", str(100+i), "0", 0, "0", 0, "0", "0", "0"] for i in range(60)]
    dn = [[0, "0", "0", "0", str(200-i), "0", 0, "0", 0, "0", "0", "0"] for i in range(60)]
    flat = [[0, "0", "0", "0", "100", "0", 0, "0", 0, "0", "0", "0"] for _ in range(60)]
    assert _kline_derivatives(up)[3] > 0
    assert _kline_derivatives(dn)[3] < 0
    sf = _kline_derivatives(flat)[3]
    assert sf is not None and abs(sf) < 1e-9


def test_4h_misalignment_downgrades_and_hard_block_drops():
    snap = _snap(long_short_ratio=2.7, open_interest_change_pct_window=0.07, price_change_pct_1h=0.01, ema50_4h=90.0)
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert short.is_strong is False
    cfg = _cfg()
    cfg.signals.trend_filter.hard_block_on_4h = True
    out2 = evaluate(snap, cfg)
    assert all(c.direction is not Direction.SHORT for c in out2)


def test_1h_slope_misalignment_downgrades():
    snap = _snap(long_short_ratio=2.7, open_interest_change_pct_window=0.07, price_change_pct_1h=0.01, ema50_slope_1h=0.01)
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert short.is_strong is False


def test_aligned_candidate_untouched_and_missing_passes_through():
    s1 = _snap(long_short_ratio=2.7, open_interest_change_pct_window=0.07, price_change_pct_1h=0.01, ema50_4h=120.0, ema50_slope_1h=-0.01)
    out1 = evaluate(s1, _cfg())
    c1 = [c for c in out1 if c.direction is Direction.SHORT][0]
    assert all(r.name not in {"trend_4h", "slope_1h"} for r in c1.fired_rules)
    s2 = _snap(long_short_ratio=2.7, open_interest_change_pct_window=0.07, price_change_pct_1h=0.01)
    out2 = evaluate(s2, _cfg())
    c2 = [c for c in out2 if c.direction is Direction.SHORT][0]
    assert all(r.name not in {"trend_4h", "slope_1h"} for r in c2.fired_rules)


def test_liq_cascade_exempt_from_trend_slope_gates():
    snap = _snap(short_liquidations_usd_window=80_000_000.0, ema50_4h=50.0, ema50_slope_1h=0.02)
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert all(r.name != "trend_4h" for r in short.fired_rules)
    assert all(r.name != "slope_1h" for r in short.fired_rules)
