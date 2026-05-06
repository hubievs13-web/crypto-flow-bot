"""Tests for the weekly stats digest module."""

from datetime import UTC, datetime, timedelta

from crypto_flow_bot.notify.stats import (
    SignalStats,
    compute_stats,
    format_stats_digest,
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
