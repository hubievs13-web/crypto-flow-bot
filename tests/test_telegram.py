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

from crypto_flow_bot.engine.models import Direction, Snapshot  # noqa: E402
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


def test_startup_falls_back_to_pct_when_atr_disabled():
    cfg = _per_symbol_cfg()
    cfg.exits.atr_sizing.enabled = False
    alert = format_startup(cfg, version="x")
    text = alert.text
    # When ATR sizing is off, the fixed percent SL is advertised.
    assert "SL 1.50%" in text
    assert "ATR(1h)" not in text
