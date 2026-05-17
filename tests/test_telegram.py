from crypto_flow_bot.config import (
    AtrSizingCfg,
    Config,
    ExitsCfg,
    FundingExtremeCfg,
    LsrExtremeCfg,
    NotifierCfg,
    SignalsCfg,
    SymbolOverridesCfg,
)
from crypto_flow_bot.notify.telegram import format_greeting, format_startup


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

from crypto_flow_bot.engine.models import Direction, Position, Snapshot  # noqa: E402
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate  # noqa: E402
from crypto_flow_bot.engine.state import StateStore  # noqa: E402
from crypto_flow_bot.notify.telegram import format_entry_alert  # noqa: E402


def _snap(price: float = 100.0, atr: float | None = 1.0) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price, atr_1h=atr)


def test_format_entry_alert_includes_strong_marker_when_two_non_funding_rules(tmp_path, monkeypatch):
    """STRONG marker requires 2+ non-funding rules in the confluence window.

    funding_extreme + lsr_extreme used to qualify; under the new STRONG
    definition (P0-8) only LSR + OI / LSR + liq_cascade / OI + liq_cascade
    do, because STRONG is meant for two genuinely *fast* triggers agreeing.
    """
    monkeypatch.setenv("CRYPTO_FLOW_BOT_STATE_DIR", str(tmp_path))
    cfg = _cfg()
    snap = _snap(price=100.0, atr=1.0)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[
            FiredRule(name="lsr_extreme", description="L/S 0.55"),
            FiredRule(name="liq_cascade", description="short liqs $80M"),
        ],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme", "liq_cascade"},
    )
    assert candidate.is_strong is True
    store = StateStore(path=tmp_path)
    pos = store.open_from_signal(candidate, cfg)
    alert = format_entry_alert(candidate, pos, cfg)
    assert "STRONG" in alert.text
    # Last TP multiplier should be the wider strong one (default 4.0).
    expected_pct = 4.0 * 1.0 / 100.0
    assert abs(pos.tp_levels[-1].pct - expected_pct) < 1e-9


def test_format_entry_alert_omits_strong_for_funding_plus_one_rule(tmp_path, monkeypatch):
    """funding_extreme + 1 non-funding rule is a regular entry (no STRONG)."""
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
        confluence_window_rules={"funding_extreme", "lsr_extreme"},
    )
    assert candidate.is_strong is False
    store = StateStore(path=tmp_path)
    pos = store.open_from_signal(candidate, cfg)
    alert = format_entry_alert(candidate, pos, cfg)
    assert "STRONG" not in alert.text
    # Regular signal uses default tp_atr_mults[-1] = 3.0
    expected_pct = 3.0 * 1.0 / 100.0
    assert abs(pos.tp_levels[-1].pct - expected_pct) < 1e-9


def test_format_entry_alert_omits_strong_for_single_rule(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTO_FLOW_BOT_STATE_DIR", str(tmp_path))
    cfg = _cfg()
    snap = _snap(price=100.0, atr=1.0)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="L/S 0.55")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
    )
    assert candidate.is_strong is False
    store = StateStore(path=tmp_path)
    pos = store.open_from_signal(candidate, cfg)
    alert = format_entry_alert(candidate, pos, cfg)
    assert "STRONG" not in alert.text


def test_entry_alert_trailing_activation_uses_atr_based_pct_when_available():
    cfg = Config(symbols=["BTCUSDT"], notifier=NotifierCfg(), signals=SignalsCfg())
    cfg.exits.trailing.activate_at_atr_mult = 1.5
    cfg.exits.trailing.activate_at_pct = 0.02
    snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0)
    cand = SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="L/S 0.55")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
    )
    pos = Position(
        id="xatr",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=100.0,
        entry_ts=datetime.now(tz=UTC),
        reason="lsr_extreme",
        stop_loss_price=99.0,
        initial_stop_loss_price=99.0,
        entry_atr_1h=2.0,
    )
    alert = format_entry_alert(cand, pos, cfg)
    assert "Trailing: after 3.00%" in alert.text
    assert "SL:" in alert.text and "Time stop:" in alert.text


def test_entry_alert_trailing_activation_falls_back_without_entry_atr():
    cfg = Config(symbols=["BTCUSDT"], notifier=NotifierCfg(), signals=SignalsCfg())
    cfg.exits.trailing.activate_at_atr_mult = 1.5
    cfg.exits.trailing.activate_at_pct = 0.02
    snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0)
    cand = SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="L/S 0.55")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
    )
    pos = Position(
        id="xfallback",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=100.0,
        entry_ts=datetime.now(tz=UTC),
        reason="lsr_extreme",
        stop_loss_price=99.0,
        initial_stop_loss_price=99.0,
        entry_atr_1h=None,
    )
    alert = format_entry_alert(cand, pos, cfg)
    assert "Trailing: after 2.00%" in alert.text


def test_entry_alert_trailing_activation_falls_back_when_entry_price_invalid():
    cfg = Config(symbols=["BTCUSDT"], notifier=NotifierCfg(), signals=SignalsCfg())
    cfg.exits.trailing.activate_at_atr_mult = 1.5
    cfg.exits.trailing.activate_at_pct = 0.02
    snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0)
    cand = SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="L/S 0.55")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
    )
    pos = Position(
        id="xprice0",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=0.0,
        entry_ts=datetime.now(tz=UTC),
        reason="lsr_extreme",
        stop_loss_price=0.0,
        initial_stop_loss_price=0.0,
        entry_atr_1h=2.0,
    )
    alert = format_entry_alert(cand, pos, cfg)
    assert "Trailing: after 2.00%" in alert.text


def test_entry_alert_renders_downgrades_section():
    cfg = Config(symbols=["BTCUSDT"], notifier=NotifierCfg(), signals=SignalsCfg())
    snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0)
    cand = SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.SHORT,
        fired_rules=[FiredRule(name="lsr_extreme", description="L/S 2.70")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
        entry_downgrades=[
            FiredRule(name="taker_confirmation", description="taker n/c (40.0%)"),
            FiredRule(name="trend_4h", description="trend_4h n/a"),
        ],
    )
    pos = Position(
        id="x1", symbol="BTCUSDT", direction=Direction.SHORT,
        entry_price=100.0, entry_ts=datetime.now(tz=UTC),
        reason="lsr_extreme", stop_loss_price=101.0,
        initial_stop_loss_price=101.0,
        entry_strength="weak",
        entry_downgrades=["taker_confirmation", "trend_4h"],
    )
    alert = format_entry_alert(cand, pos, cfg)
    assert "<b>Downgrades:</b>" in alert.text
    assert "taker n/c" in alert.text
    assert "trend_4h n/a" in alert.text
    assert "STRONG" not in alert.text


# ─── Startup message reflects per-symbol thresholds + ATR-based exits ──────


def _per_symbol_cfg() -> Config:
    """Mirror config.yaml: per-symbol funding/LSR overrides + ATR-based exits."""
    return Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        signals=SignalsCfg(
            funding_extreme=FundingExtremeCfg(
                long_overheated_above=0.0008, short_overheated_below=-0.0008,
            ),
            lsr_extreme=LsrExtremeCfg(long_heavy_above=2.5, short_heavy_below=0.6),
            per_symbol={
                "BTCUSDT": SymbolOverridesCfg(
                    funding_extreme=FundingExtremeCfg(
                        long_overheated_above=0.00003, short_overheated_below=-0.00006,
                    ),
                    lsr_extreme=LsrExtremeCfg(long_heavy_above=2.0, short_heavy_below=0.70),
                ),
                "SOLUSDT": SymbolOverridesCfg(
                    funding_extreme=FundingExtremeCfg(
                        long_overheated_above=0.00010, short_overheated_below=-0.00005,
                    ),
                    lsr_extreme=LsrExtremeCfg(long_heavy_above=2.2, short_heavy_below=0.65),
                ),
            },
        ),
        exits=ExitsCfg(atr_sizing=AtrSizingCfg(enabled=True, sl_atr_mult=1.5, tp_atr_mults=[1.5, 3.0])),
        notifier=NotifierCfg(pretty_names={"BTCUSDT": "BTC", "ETHUSDT": "ETH", "SOLUSDT": "SOL"}),
    )


def test_startup_lists_per_symbol_thresholds():
    alert = format_startup(_per_symbol_cfg(), version="x")
    text = alert.text
    # Each symbol is named in its own block.
    assert "BTC" in text and "ETH" in text and "SOL" in text
    # BTC override: funding short threshold -0.006% (i.e. -0.00006).
    assert "-0.006%" in text
    # SOL override: funding long threshold +0.010%.
    assert "+0.010%" in text
    # Per-symbol LSR (BTC=2.00/0.70, SOL=2.20/0.65) — different cuts must appear.
    assert "2.00" in text and "0.70" in text
    assert "2.20" in text and "0.65" in text


def test_startup_advertises_atr_based_sl_when_atr_enabled():
    alert = format_startup(_per_symbol_cfg(), version="x")
    text = alert.text
    # ATR-based SL/TP shown, not a fixed percent as the headline number.
    assert "1.5×ATR" in text
    assert "ATR(1h)" in text
    # Headline SL is the ATR multiplier, not the fixed percent.
    assert "<b>Exits:</b> SL 1.5×ATR" in text
    assert "<b>Exits:</b> SL 1.50%" not in text


def test_startup_advertises_short_timeframe_label_from_config():
    cfg = _per_symbol_cfg()
    cfg.signals.timeframe_short = "15m"
    alert = format_startup(cfg, version="x")
    text = alert.text
    assert "ATR(15m)" in text
    assert "ATR(1h)" not in text


def test_startup_falls_back_to_pct_when_atr_disabled():
    cfg = _per_symbol_cfg()
    cfg.exits.atr_sizing.enabled = False
    alert = format_startup(cfg, version="x")
    text = alert.text
    # When ATR sizing is off, the fixed percent SL is advertised.
    assert "SL 1.50%" in text
    assert "ATR(1h)" not in text

import asyncio  # noqa: E402
import logging  # noqa: E402

from crypto_flow_bot.notify.telegram import TelegramNotifier, _mask_telegram_token  # noqa: E402


class _DummyResponse:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code
        self.text = "ok"

    def json(self) -> dict[str, object]:
        return {"result": []}


class _DummyHTTP:
    def __init__(self) -> None:
        self.last_post_url: str | None = None
        self.last_get_url: str | None = None

    async def post(self, url: str, json: dict[str, object]) -> _DummyResponse:
        _ = json
        self.last_post_url = url
        return _DummyResponse()

    async def get(self, url: str, params: dict[str, object], timeout: float) -> _DummyResponse:
        _ = (params, timeout)
        self.last_get_url = url
        return _DummyResponse()


def test_send_debug_logs_mask_token_and_http_uses_real_token(caplog):
    token = "123456:ABCDEF"
    http = _DummyHTTP()
    notifier = TelegramNotifier(bot_token=token, chat_ids=["42"], http=http)  # type: ignore[arg-type]
    with caplog.at_level(logging.DEBUG):
        asyncio.run(notifier.send("hello"))

    logs = "\n".join(caplog.messages)
    assert token not in logs
    assert "bot***" in logs
    assert http.last_post_url == f"https://api.telegram.org/bot{token}/sendMessage"


def test_poll_commands_debug_logs_mask_token_and_http_uses_real_token(caplog):
    token = "123456:ABCDEF"
    http = _DummyHTTP()
    notifier = TelegramNotifier(bot_token=token, chat_ids=["42"], http=http)  # type: ignore[arg-type]
    with caplog.at_level(logging.DEBUG):
        asyncio.run(notifier.poll_commands(_cfg()))

    logs = "\n".join(caplog.messages)
    assert token not in logs
    assert "bot***" in logs
    assert http.last_get_url == f"https://api.telegram.org/bot{token}/getUpdates"


def test_mask_telegram_token_masks_send_and_updates_urls():
    assert _mask_telegram_token("https://api.telegram.org/bot123456:ABCDEF/sendMessage") == (
        "https://api.telegram.org/bot***/sendMessage"
    )
    assert _mask_telegram_token("https://api.telegram.org/bot999:XYZ/getUpdates") == (
        "https://api.telegram.org/bot***/getUpdates"
    )
