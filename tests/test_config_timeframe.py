from crypto_flow_bot.config import Config, SignalsCfg


def test_signals_timeframe_short_default_is_1h() -> None:
    cfg = Config(symbols=["BTCUSDT"])
    assert cfg.signals.timeframe_short == "1h"


def test_signals_timeframe_short_accepts_15m_without_changing_default() -> None:
    default_cfg = Config(symbols=["BTCUSDT"])
    override_cfg = Config(symbols=["BTCUSDT"], signals=SignalsCfg(timeframe_short="15m"))
    assert default_cfg.signals.timeframe_short == "1h"
    assert override_cfg.signals.timeframe_short == "15m"
