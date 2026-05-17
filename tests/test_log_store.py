from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from crypto_flow_bot.engine.models import Alert, Direction, Position, Snapshot, TpLevelState
from crypto_flow_bot.log.store import JsonlLogger


def test_write_alert_uses_daily_partition(tmp_path):
    logger = JsonlLogger(path=tmp_path)
    ts = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    asyncio.run(logger.write_alert(
        Alert(kind="ENTRY", symbol="BTCUSDT", ts=ts, text="entry", direction=Direction.LONG),
    ))
    assert (tmp_path / "alerts-2026-05-18.jsonl").is_file()
    assert not (tmp_path / "alerts.jsonl").exists()


def test_write_position_uses_daily_partition(tmp_path):
    logger = JsonlLogger(path=tmp_path)
    close_ts = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    pos = Position(
        id="p1",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=100.0,
        entry_ts=datetime(2026, 5, 17, 10, 0, tzinfo=UTC),
        reason="funding_extreme",
        stop_loss_price=99.0,
        initial_stop_loss_price=99.0,
        tp_levels=[TpLevelState(pct=0.015, fraction=1.0)],
        closed=True,
        close_ts=close_ts,
        close_reason="TP_HIT",
        close_price=101.5,
    )
    asyncio.run(logger.write_position(pos))
    assert (tmp_path / "positions-2026-05-18.jsonl").is_file()
    assert not (tmp_path / "positions.jsonl").exists()


def test_write_snapshot_uses_daily_partition(tmp_path):
    logger = JsonlLogger(path=tmp_path)
    ts = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    asyncio.run(logger.write_snapshot(Snapshot(symbol="BTCUSDT", ts=ts, price=100.0)))
    assert (tmp_path / "snapshots-2026-05-18.jsonl").is_file()
    assert not (tmp_path / "snapshots.jsonl").exists()


def test_write_blocked_uses_daily_partition(tmp_path):
    logger = JsonlLogger(path=tmp_path)
    ts = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
    asyncio.run(logger.write_blocked(
        signal_id="s1",
        symbol="BTCUSDT",
        direction=Direction.SHORT,
        blocked_reason="cooldown",
        fired_rules=["lsr_extreme"],
        confluence_window_rules=["lsr_extreme"],
        snapshot_ts=ts,
    ))
    assert (tmp_path / "blocked-2026-05-18.jsonl").is_file()
    assert not (tmp_path / "blocked.jsonl").exists()
