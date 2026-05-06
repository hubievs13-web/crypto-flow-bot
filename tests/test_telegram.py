from crypto_flow_bot.config import Config, NotifierCfg
from crypto_flow_bot.notify.telegram import format_greeting


def _cfg() -> Config:
    return Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        notifier=NotifierCfg(pretty_names={"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL"}),
    )


def test_greeting_lists_watched_symbols():
    text = format_greeting(_cfg())
    assert "BTC" in text and "ETH" in text and "SOL" in text


def test_greeting_mentions_signal_kinds():
    text = format_greeting(_cfg())
    for kind in ("LONG", "SHORT", "TP", "SL", "Trailing", "Time stop"):
        assert kind in text


def test_greeting_includes_disclaimer():
    text = format_greeting(_cfg())
    # honesty about the bot not being a holy grail
    assert "does NOT execute trades" in text
    assert "statistical heuristics" in text
