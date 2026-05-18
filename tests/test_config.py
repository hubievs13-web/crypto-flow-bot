from pathlib import Path

from crypto_flow_bot.config import load_config


def test_default_config_yaml_migrates_to_15m_defaults() -> None:
    cfg = load_config("config.yaml")
    assert cfg.signals.timeframe_short == "15m"
    assert cfg.signals.trend_filter.ema_period == 200
    assert cfg.signals.trend_filter.slope_window_bars == 24
    assert cfg.signals.taker_confirmation.cvd_window_bars == 24
    assert cfg.signals.trend_filter.atr_period == 56
    assert cfg.signals.regime.timeframe == "15m"


def test_15m_example_config_parses_and_matches_future_values() -> None:
    cfg = load_config(Path("configs/config.15m.example.yaml"))
    assert cfg.signals.timeframe_short == "15m"
    assert cfg.signals.trend_filter.ema_period == 200
    assert cfg.signals.trend_filter.slope_window_bars == 24
    assert cfg.signals.taker_confirmation.cvd_window_bars == 24
    assert cfg.signals.trend_filter.atr_period == 56
    assert cfg.signals.regime.timeframe == "15m"
