from datetime import UTC, datetime

from crypto_flow_bot.config import Config, SignalsCfg
from crypto_flow_bot.data.binance import _classify_oi_quality, _cvd_window_usd, _taker_buy_dominance
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate


def _cfg(*, require_healthy: bool = True) -> Config:
    cfg = Config(symbols=["BTCUSDT"], signals=SignalsCfg(funding_extreme_requires_confirmation=False))
    cfg.signals.oi_surge.require_healthy = require_healthy
    cfg.signals.taker_confirmation.enabled = True
    return cfg


def _snap(**overrides) -> Snapshot:
    base = {"symbol": "BTCUSDT", "ts": datetime.now(tz=UTC), "price": 50_000.0}
    base.update(overrides)
    return Snapshot(**base)


def test_taker_buy_dominance_computed_from_buy_and_sell():
    assert _taker_buy_dominance(55.0, 45.0) == 0.55


def test_cvd_window_sums_last_n_closed_bars_only():
    # [quoteVolume, takerBuyQuoteVolume] -> (2*buy - quote)
    bars = [
        [0, 0, 0, 0, 0, 0, 0, 100.0, 0, 0, 60.0, 0],  # +20
        [0, 0, 0, 0, 0, 0, 0, 100.0, 0, 0, 40.0, 0],  # -20
        [0, 0, 0, 0, 0, 0, 0, 200.0, 0, 0, 120.0, 0],  # +40
        [0, 0, 0, 0, 0, 0, 0, 100.0, 0, 0, 90.0, 0],  # in-progress (ignored)
    ]
    assert _cvd_window_usd(bars, window_bars=2) == 20.0


def test_oi_quality_classification_quadrants_and_epsilon():
    eps = 0.0005
    assert _classify_oi_quality(+0.01, +0.03, eps) == "healthy_short"
    assert _classify_oi_quality(-0.01, +0.03, eps) == "healthy_long"
    assert _classify_oi_quality(+0.01, -0.03, eps) == "dangerous_long"
    assert _classify_oi_quality(-0.01, -0.03, eps) == "dangerous_short"
    assert _classify_oi_quality(0.0001, 0.03, eps) is None


def test_require_healthy_drops_dangerous_long_surge():
    snap = _snap(
        open_interest_change_pct_window=0.07,
        price_change_pct_1h=0.01,
        oi_quality="dangerous_long",
    )
    out = evaluate(snap, _cfg(require_healthy=True))
    assert all(not any(r.name == "oi_surge" for r in c.fired_rules) for c in out)


def test_require_healthy_off_keeps_legacy_behavior():
    snap = _snap(
        open_interest_change_pct_window=0.07,
        price_change_pct_1h=0.01,
        oi_quality="dangerous_long",
    )
    out = evaluate(snap, _cfg(require_healthy=False))
    assert any(c.direction is Direction.LONG and any(r.name == "oi_surge" for r in c.fired_rules) for c in out)


def test_taker_confirmation_downgrades_strong_when_dominance_fails():
    snap = _snap(
        long_short_ratio=2.7,
        open_interest_change_pct_window=0.07,
        price_change_pct_1h=0.01,
        oi_quality="healthy_short",
        taker_buy_dominance_1h=0.70,
    )
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert short.is_strong is False
    assert any(r.name == "taker_confirmation" for r in short.fired_rules)


def test_missing_dominance_does_not_downgrade():
    snap = _snap(
        long_short_ratio=2.7,
        open_interest_change_pct_window=0.07,
        price_change_pct_1h=0.01,
        oi_quality="healthy_short",
        taker_buy_dominance_1h=None,
    )
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert short.is_strong is True


def test_liq_cascade_never_downgraded():
    snap = _snap(short_liquidations_usd_window=80_000_000.0, taker_buy_dominance_1h=0.99)
    out = evaluate(snap, _cfg())
    short = [c for c in out if c.direction is Direction.SHORT][0]
    assert short.is_strong is False
    assert all(r.name != "taker_confirmation" for r in short.fired_rules)
