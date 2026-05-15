from datetime import UTC, datetime

from crypto_flow_bot.config import (
    Config,
    LiqCascadeCfg,
    LsrExtremeCfg,
    NotifierCfg,
    OiSurgeCfg,
    PredictedFundingCfg,
    SignalsCfg,
)
from crypto_flow_bot.data.binance import _compute_predicted_funding
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import evaluate


def _snap(**kw):
    base={'symbol':'BTCUSDT','ts':datetime.now(tz=UTC),'price':100.0,'long_liquidations_usd_window':0.0,'short_liquidations_usd_window':0.0}
    base.update(kw)
    return Snapshot(**base)


def test_predicted_clamp():
    assert _compute_predicted_funding(120, 100, 0.002, 0.0075) == 0.0075
    assert _compute_predicted_funding(80, 100, -0.002, 0.0075) == -0.0075


def test_predicted_signal_short_long_and_default_disabled():
    cfg = Config(symbols=['BTCUSDT'], notifier=NotifierCfg(), signals=SignalsCfg(
        oi_surge=OiSurgeCfg(enabled=False), lsr_extreme=LsrExtremeCfg(enabled=False), liq_cascade=LiqCascadeCfg(enabled=False),
        funding_extreme_requires_confirmation=False,
    ))
    assert evaluate(_snap(predicted_funding_rate=0.003, predicted_funding_zscore=3.0), cfg) == []
    cfg.signals.predicted_funding = PredictedFundingCfg(enabled=True, mode='auto', zscore_high_abs=2.0)
    short = evaluate(_snap(predicted_funding_rate=0.003, predicted_funding_zscore=3.0), cfg)
    assert any(c.direction is Direction.SHORT for c in short)
    long = evaluate(_snap(predicted_funding_rate=-0.003, predicted_funding_zscore=-3.0), cfg)
    assert any(c.direction is Direction.LONG for c in long)
