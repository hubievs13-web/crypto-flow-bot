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


# ─── Confluence / STRONG marker (item #6) ──────────────────────────────────

from datetime import UTC, datetime  # noqa: E402

from crypto_flow_bot.engine.models import Direction, Snapshot  # noqa: E402
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate  # noqa: E402
from crypto_flow_bot.engine.state import StateStore  # noqa: E402
from crypto_flow_bot.notify.telegram import format_entry_alert  # noqa: E402


def _snap(price: float = 100.0, atr: float | None = 1.0) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price, atr_1h=atr)


def test_format_entry_alert_includes_strong_marker_when_confluence(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTO_FLOW_BOT_STATE_DIR", str(tmp_path))
    cfg = _cfg()
    snap = _snap(price=100.0, atr=1.0)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[
            FiredRule(name="funding_extreme", description="funding -0.10%"),
            FiredRule(name="lsr_extreme", description="L/S 0.55"),
        ],
        snapshot=snap,
    )
    assert candidate.is_strong is True
    store = StateStore(path=tmp_path)
    pos = store.open_from_signal(candidate, cfg)
    alert = format_entry_alert(candidate, pos, cfg)
    assert "STRONG" in alert.text
    # Last TP multiplier should be the wider strong one (default 4.0).
    expected_pct = 4.0 * 1.0 / 100.0
    assert abs(pos.tp_levels[-1].pct - expected_pct) < 1e-9


def test_format_entry_alert_omits_strong_for_single_rule(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTO_FLOW_BOT_STATE_DIR", str(tmp_path))
    cfg = _cfg()
    snap = _snap(price=100.0, atr=1.0)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="funding_extreme", description="funding -0.10%")],
        snapshot=snap,
    )
    assert candidate.is_strong is False
    store = StateStore(path=tmp_path)
    pos = store.open_from_signal(candidate, cfg)
    alert = format_entry_alert(candidate, pos, cfg)
    assert "STRONG" not in alert.text
    # Single-rule signal uses default tp_atr_mults[-1] = 3.0
    expected_pct = 3.0 * 1.0 / 100.0
    assert abs(pos.tp_levels[-1].pct - expected_pct) < 1e-9
