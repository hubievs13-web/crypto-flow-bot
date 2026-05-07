"""Tests for the weekly stats digest module."""

from datetime import UTC, datetime, timedelta

from crypto_flow_bot.notify.stats import (
    SignalStats,
    compute_stats,
    format_stats_digest,
    is_past_weekly_send_time,
    read_latest_positions,
)


def _pos(
    pid: str,
    *,
    reason: str,
    entry_ts: datetime,
    direction: str = "LONG",
    entry_price: float = 50_000.0,
    closed: bool = True,
    close_price: float | None = None,
    close_reason: str | None = None,
    tp_levels: list[dict] | None = None,
) -> dict:
    return {
        "id": pid,
        "symbol": "BTCUSDT",
        "direction": direction,
        "entry_price": entry_price,
        "entry_ts": entry_ts.isoformat(),
        "reason": reason,
        "stop_loss_price": entry_price * 0.985,
        "tp_levels": tp_levels or [
            {"pct": 0.015, "fraction": 0.5, "hit": False},
            {"pct": 0.030, "fraction": 0.5, "hit": False},
        ],
        "open_fraction": 1.0 if not closed else 0.0,
        "closed": closed,
        "close_ts": entry_ts.isoformat() if closed else None,
        "close_reason": close_reason,
        "close_price": close_price,
    }


def test_compute_stats_empty():
    out = compute_stats([], now=datetime.now(tz=UTC), window_days=7)
    assert out == {}


def test_compute_stats_groups_by_reason():
    now = datetime.now(tz=UTC)
    # one funding signal that hit TP1 only, one LSR signal that hit SL with no TP.
    positions = [
        _pos("a", reason="funding_extreme", entry_ts=now - timedelta(days=1),
             entry_price=50_000.0, close_price=50_750.0, close_reason="TP_HIT",
             tp_levels=[
                 {"pct": 0.015, "fraction": 0.5, "hit": True},
                 {"pct": 0.030, "fraction": 0.5, "hit": False},
             ]),
        _pos("b", reason="lsr_extreme", entry_ts=now - timedelta(days=2),
             entry_price=50_000.0, close_price=49_250.0, close_reason="SL_HIT"),
    ]
    out = compute_stats(positions, now=now, window_days=7)
    assert set(out.keys()) == {"funding_extreme", "lsr_extreme"}
    assert out["funding_extreme"].count == 1
    assert out["funding_extreme"].tp1_hit == 1
    assert out["funding_extreme"].sl_no_tp == 0
    assert out["lsr_extreme"].count == 1
    assert out["lsr_extreme"].tp1_hit == 0
    assert out["lsr_extreme"].sl_no_tp == 1


def test_compute_stats_drops_outside_window():
    now = datetime.now(tz=UTC)
    positions = [
        _pos("a", reason="funding_extreme", entry_ts=now - timedelta(days=10),
             closed=True, close_price=51_000.0, close_reason="TP_HIT"),
    ]
    out = compute_stats(positions, now=now, window_days=7)
    assert out == {}


def test_compute_stats_combo_reason_counts_each():
    now = datetime.now(tz=UTC)
    # Position fires on two rules at once -> both count it.
    positions = [
        _pos("a", reason="funding_extreme+lsr_extreme",
             entry_ts=now - timedelta(hours=1),
             closed=False),  # still open
    ]
    out = compute_stats(positions, now=now, window_days=7)
    assert set(out.keys()) == {"funding_extreme", "lsr_extreme"}
    assert out["funding_extreme"].open_unresolved == 1
    assert out["lsr_extreme"].open_unresolved == 1


def test_compute_stats_pnl_long_vs_short_sign():
    now = datetime.now(tz=UTC)
    # LONG: close_price > entry -> positive PnL
    long_pos = _pos("a", reason="funding_extreme", direction="LONG",
                    entry_ts=now - timedelta(hours=1),
                    entry_price=100.0, close_price=102.0,
                    close_reason="TP_HIT")
    # SHORT: close_price > entry -> negative PnL
    short_pos = _pos("b", reason="funding_extreme", direction="SHORT",
                     entry_ts=now - timedelta(hours=1),
                     entry_price=100.0, close_price=102.0,
                     close_reason="SL_HIT")
    out = compute_stats([long_pos, short_pos], now=now, window_days=7)
    assert out["funding_extreme"].count == 2
    assert out["funding_extreme"].closed == 2
    # LONG: +2%, SHORT: -2% -> avg 0%
    assert abs(out["funding_extreme"].avg_pnl_pct) < 1e-6


def test_format_digest_empty_message():
    text = format_stats_digest({}, window_days=7)
    assert "No signals fired" in text
    assert "last 7 days" in text


def test_format_digest_includes_signal_lines():
    s = SignalStats(name="funding_extreme", count=5, tp1_hit=3, tp2_hit=1, sl_no_tp=1)
    s.total_pnl_pct = 0.04  # 4% summed across 5 closed -> 0.8% avg
    text = format_stats_digest({"funding_extreme": s}, window_days=7, total_positions=5)
    assert "funding_extreme" in text
    assert "5 fires" in text
    assert "TP1 3" in text


def test_read_latest_positions_keeps_last_per_id(tmp_path):
    pf = tmp_path / "positions.jsonl"
    pf.write_text(
        '{"id":"a","entry_ts":"2025-01-01T00:00:00+00:00","reason":"f","closed":false}\n'
        '{"id":"a","entry_ts":"2025-01-01T00:00:00+00:00","reason":"f","closed":true,"close_reason":"TP_HIT"}\n'
        '{"id":"b","entry_ts":"2025-01-02T00:00:00+00:00","reason":"l","closed":true,"close_reason":"SL_HIT"}\n'
    )
    rows = read_latest_positions(pf)
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["closed"] is True
    assert by_id["a"]["close_reason"] == "TP_HIT"
    assert by_id["b"]["close_reason"] == "SL_HIT"


def test_read_latest_positions_missing_file_returns_empty(tmp_path):
    assert read_latest_positions(tmp_path / "does_not_exist.jsonl") == []


# ─── Weekly digest gate (Mon 12:00 UTC by default) ──────────────────────────


def test_digest_gate_fires_at_exact_send_time():
    # 2025-11-03 (Mon) 12:00 UTC, target Mon 12:00.
    assert is_past_weekly_send_time(
        datetime(2025, 11, 3, 12, 0, tzinfo=UTC), weekday=0, hour_utc=12
    )


def test_digest_gate_fires_one_minute_after_send_time():
    # The whole point of the fix: 12:01 still counts as "past".
    assert is_past_weekly_send_time(
        datetime(2025, 11, 3, 12, 1, tzinfo=UTC), weekday=0, hour_utc=12
    )


def test_digest_gate_fires_later_in_the_week():
    # Tue 09:00 — already past Mon 12:00.
    assert is_past_weekly_send_time(
        datetime(2025, 11, 4, 9, 0, tzinfo=UTC), weekday=0, hour_utc=12
    )


def test_digest_gate_does_not_fire_before_send_time():
    # Mon 11:59 — too early.
    assert not is_past_weekly_send_time(
        datetime(2025, 11, 3, 11, 59, tzinfo=UTC), weekday=0, hour_utc=12
    )


def test_digest_gate_handles_sunday_target():
    # weekday=6 (Sun). Sat is too early; Sun 12:00 is on time.
    assert not is_past_weekly_send_time(
        datetime(2025, 11, 1, 23, 0, tzinfo=UTC), weekday=6, hour_utc=12
    )
    assert is_past_weekly_send_time(
        datetime(2025, 11, 2, 12, 0, tzinfo=UTC), weekday=6, hour_utc=12
    )


# ─── Weighted PnL across partial closes (item #8) ──────────────────────────

from crypto_flow_bot.notify.stats import SymbolStats, compute_symbol_stats, position_pnl_pct  # noqa: E402


def test_position_pnl_weighted_50_tp1_50_be():
    """50% closed at TP1 (+1.5%) + 50% at BE → 0.5*0.015 + 0.5*0.0 = 0.75%."""
    pos = _pos(
        "x", reason="funding_extreme", direction="LONG",
        entry_ts=datetime.now(tz=UTC), entry_price=100.0, close_price=100.0,
        close_reason="REASON_INVALIDATED",
        tp_levels=[
            {"pct": 0.015, "fraction": 0.5, "hit": True},
            {"pct": 0.030, "fraction": 0.5, "hit": False},
        ],
    )
    pnl = position_pnl_pct(pos)
    assert pnl is not None and abs(pnl - 0.0075) < 1e-9


def test_position_pnl_weighted_both_tps_hit():
    """Both TP levels hit → 0.5*0.015 + 0.5*0.030 = 2.25%."""
    pos = _pos(
        "x", reason="funding_extreme", direction="LONG",
        entry_ts=datetime.now(tz=UTC), entry_price=100.0, close_price=103.0,
        close_reason="TP_HIT",
        tp_levels=[
            {"pct": 0.015, "fraction": 0.5, "hit": True},
            {"pct": 0.030, "fraction": 0.5, "hit": True},
        ],
    )
    pnl = position_pnl_pct(pos)
    assert pnl is not None and abs(pnl - 0.0225) < 1e-9


def test_position_pnl_weighted_short_full_loss():
    """No TPs hit, SHORT closed at SL → full loss = -SL%."""
    pos = _pos(
        "x", reason="lsr_extreme", direction="SHORT",
        entry_ts=datetime.now(tz=UTC), entry_price=100.0, close_price=101.5,
        close_reason="SL_HIT",
        tp_levels=[
            {"pct": 0.015, "fraction": 0.5, "hit": False},
            {"pct": 0.030, "fraction": 0.5, "hit": False},
        ],
    )
    pnl = position_pnl_pct(pos)
    # SHORT: (101.5 - 100)/100 * -1 = -0.015
    assert pnl is not None and abs(pnl - (-0.015)) < 1e-9


def test_position_pnl_handles_missing_fields():
    assert position_pnl_pct({}) is None
    assert position_pnl_pct({"entry_price": 100.0}) is None


def test_compute_stats_uses_weighted_pnl():
    """compute_stats should sum the weighted PnL, not the entry→close move."""
    now = datetime.now(tz=UTC)
    # Single position: 50% TP1 (+1.5%), then SL/BE close → expected 0.75%.
    positions = [
        _pos("a", reason="funding_extreme", direction="LONG",
             entry_ts=now - timedelta(hours=1),
             entry_price=100.0, close_price=100.0,
             close_reason="REASON_INVALIDATED",
             tp_levels=[
                 {"pct": 0.015, "fraction": 0.5, "hit": True},
                 {"pct": 0.030, "fraction": 0.5, "hit": False},
             ]),
    ]
    out = compute_stats(positions, now=now, window_days=7)
    assert abs(out["funding_extreme"].avg_pnl_pct - 0.75) < 1e-6


# ─── Per-symbol stats (item #14) ────────────────────────────────────────────

def test_compute_symbol_stats_groups_by_symbol():
    now = datetime.now(tz=UTC)
    btc_win = _pos(
        "a", reason="funding_extreme", direction="LONG",
        entry_ts=now - timedelta(hours=1), entry_price=100.0, close_price=101.5,
        close_reason="TP_HIT",
        tp_levels=[
            {"pct": 0.015, "fraction": 0.5, "hit": True},
            {"pct": 0.030, "fraction": 0.5, "hit": False},
        ],
    )
    btc_win["symbol"] = "BTCUSDT"
    sol_loss = _pos(
        "b", reason="lsr_extreme", direction="SHORT",
        entry_ts=now - timedelta(hours=2), entry_price=100.0, close_price=101.5,
        close_reason="SL_HIT",
    )
    sol_loss["symbol"] = "SOLUSDT"
    out = compute_symbol_stats([btc_win, sol_loss], now=now, window_days=7)
    assert set(out.keys()) == {"BTCUSDT", "SOLUSDT"}
    assert out["BTCUSDT"].count == 1
    assert out["BTCUSDT"].tp1_hit == 1
    assert out["BTCUSDT"].closed == 1
    assert out["BTCUSDT"].win_rate_pct == 100.0
    assert out["SOLUSDT"].tp1_hit == 0
    assert out["SOLUSDT"].closed == 1
    assert out["SOLUSDT"].win_rate_pct == 0.0


def test_format_digest_includes_per_symbol_section():
    sig = SignalStats(name="funding_extreme", count=2, tp1_hit=1)
    sig.total_pnl_pct = 0.01
    sym_btc = SymbolStats(symbol="BTCUSDT", count=1, closed=1, tp1_hit=1, total_pnl_pct=0.0075)
    text = format_stats_digest(
        {"funding_extreme": sig}, window_days=7, total_positions=1, per_symbol={"BTCUSDT": sym_btc}
    )
    assert "By symbol" in text
    assert "BTCUSDT" in text
    assert "By signal type" in text


def test_format_digest_omits_symbol_section_when_empty():
    sig = SignalStats(name="funding_extreme", count=1)
    text = format_stats_digest({"funding_extreme": sig}, window_days=7, total_positions=1)
    assert "By symbol" not in text


def test_format_digest_disclaimer_says_weighted():
    sig = SignalStats(name="funding_extreme", count=1, tp1_hit=1)
    sig.total_pnl_pct = 0.01
    text = format_stats_digest({"funding_extreme": sig}, window_days=7, total_positions=1)
    assert "weighted" in text.lower()
