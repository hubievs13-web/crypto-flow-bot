import pytest
from pydantic import ValidationError

from crypto_flow_bot.config import Config, SignalsCfg, TakerConfirmationCfg, TrendFilterCfg


def test_signals_timeframe_short_default_is_1h() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.timeframe_short == "1h"


def test_signals_timeframe_short_accepts_15m_without_changing_default() -> None:
    default_cfg = Config(symbols=["BTCUSDT"])
    override_cfg = Config(symbols=["BTCUSDT"], signals=SignalsCfg(timeframe_short="15m"))
    assert default_cfg.signals.timeframe_short == "1h"
    assert override_cfg.signals.timeframe_short == "15m"


def test_trend_filter_default_slope_windows_are_both_6() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.trend_filter.slope_window_bars == 6
    assert cfg.signals.trend_filter.slope_window_bars_4h == 6


def test_taker_confirmation_default_cvd_window_bars_is_6() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.taker_confirmation.cvd_window_bars == 6


def test_taker_confirmation_cvd_window_bars_rejects_non_positive() -> None:
    with pytest.raises(ValidationError):
        SignalsCfg(taker_confirmation=TakerConfirmationCfg(cvd_window_bars=0))


def test_trend_filter_default_atr_period_is_14() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.trend_filter.atr_period == 14


def test_trend_filter_atr_period_rejects_non_positive() -> None:
    with pytest.raises(ValidationError):
        SignalsCfg(trend_filter=TrendFilterCfg(atr_period=0))
