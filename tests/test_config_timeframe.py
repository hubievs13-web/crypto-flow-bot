import pytest
from pydantic import ValidationError

from crypto_flow_bot.config import Config, RegimeCfg, SignalsCfg, TakerConfirmationCfg, TrendFilterCfg


def test_signals_timeframe_short_default_is_15m() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.timeframe_short == "15m"


def test_signals_timeframe_short_accepts_15m_and_default_is_15m() -> None:
    default_cfg = Config(symbols=["BTCUSDT"])
    override_cfg = Config(symbols=["BTCUSDT"], signals=SignalsCfg(timeframe_short="15m"))
    assert default_cfg.signals.timeframe_short == "15m"
    assert override_cfg.signals.timeframe_short == "15m"


def test_trend_filter_default_windows_match_15m_migration() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.trend_filter.slope_window_bars == 24
    assert cfg.signals.trend_filter.slope_window_bars_4h == 6


def test_taker_confirmation_defaults_match_15m_migration() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.taker_confirmation.cvd_window_bars == 24
    assert cfg.signals.taker_confirmation.bullish_threshold == 0.55
    assert cfg.signals.taker_confirmation.bearish_threshold == 0.45


def test_taker_confirmation_cvd_window_bars_rejects_non_positive() -> None:
    with pytest.raises(ValidationError):
        SignalsCfg(taker_confirmation=TakerConfirmationCfg(cvd_window_bars=0))


def test_trend_filter_default_atr_period_is_56() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.trend_filter.atr_period == 56


def test_trend_filter_atr_period_rejects_non_positive() -> None:
    with pytest.raises(ValidationError):
        SignalsCfg(trend_filter=TrendFilterCfg(atr_period=0))


def test_regime_timeframe_default_is_15m() -> None:
    """Regime axis defaults to 15m, matching the 15m entry timeframe."""
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.regime.timeframe == "15m"


def test_regime_timeframe_accepts_1h_override() -> None:
    default_cfg = Config(symbols=["BTCUSDT"])
    override_cfg = Config(symbols=["BTCUSDT"], signals=SignalsCfg(regime=RegimeCfg(timeframe="1h")))
    assert default_cfg.signals.regime.timeframe == "15m"
    assert override_cfg.signals.regime.timeframe == "1h"


def test_15m_time_windows_defaults_are_conservative() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.confluence_window_minutes == 15
    assert cfg.exits.reason_invalidation.momentum_window_minutes == 15
