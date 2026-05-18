from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.exits import ExitEvent
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot, DecisionSummary


def _bot(tmp_path) -> Bot:
    bot = cast(Bot, Bot.__new__(Bot))
    bot.cfg = Config(symbols=["BTCUSDT"], alert_cooldown_seconds=3600)
    bot.state = StateStore(path=tmp_path)
    bot.notifier = MagicMock()
    cast(Any, bot.notifier).send = AsyncMock()
    bot.logger = MagicMock()
    cast(Any, bot.logger).write_alert = AsyncMock()
    cast(Any, bot.logger).write_position = AsyncMock()
    cast(Any, bot.logger).write_snapshot = AsyncMock()
    cast(Any, bot.logger).write_blocked = AsyncMock()
    cast(Any, bot.logger).write_decision_summary = AsyncMock()
    bot._entry_lock = asyncio.Lock()
    bot.confluence_cache = cast(Any, None)
    bot._decision_summary = DecisionSummary()
    return bot


def _candidate() -> SignalCandidate:
    snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0)
    return SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="x")],
        snapshot=snap,
        confluence_window_rules={"lsr_extreme"},
    )


def test_entry_telegram_failure_keeps_position_and_cooldown_and_no_duplicate(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        cand = _candidate()
        cast(Any, bot.notifier).send.side_effect = RuntimeError("telegram down")
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand]):
            await bot._handle_entry_signals_locked(cand.snapshot)
            await bot._handle_entry_signals_locked(cand.snapshot)
        assert len(bot.state.open_positions()) == 1
        assert bot.state.cooldown_remaining_seconds("BTCUSDT", Direction.LONG, 3600) > 0
        assert cast(Any, bot.notifier).send.await_count == 1
        assert cast(Any, bot.logger).write_alert.await_count == 1
    asyncio.run(_run())


def test_entry_write_position_failure_rolls_back_and_skips_telegram(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        cand = _candidate()
        cast(Any, bot.logger).write_position.side_effect = OSError("disk full")
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand]):
            await bot._handle_entry_signals_locked(cand.snapshot)
        assert bot.state.open_positions() == []
        cast(Any, bot.notifier).send.assert_not_awaited()
    asyncio.run(_run())


def test_poll_loop_survives_snapshot_log_write_error(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        bot._stop = asyncio.Event()
        bot.client = MagicMock()
        bot.liq_stream = MagicMock()
        bot._last_full_snapshot = {}
        cast(Any, bot)._augment_with_funding_stats = MagicMock()
        cast(Any, bot)._handle_entry_signals = AsyncMock()
        cast(Any, bot)._sleep = AsyncMock(side_effect=lambda _s: bot._stop.set())
        cast(Any, bot.logger).write_snapshot.side_effect = OSError("io err")
        snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0)
        with patch("crypto_flow_bot.main.build_snapshot", new=AsyncMock(return_value=snap)):
            await bot._poll_loop()
        cast(Any, bot)._handle_entry_signals.assert_awaited_once()
    asyncio.run(_run())


def test_exit_write_position_failure_rolls_back_close_and_skips_telegram(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        cand = _candidate()
        pos = bot.state.open_from_signal(cand, bot.cfg)
        pre_open_fraction = pos.open_fraction
        pre_closed = pos.closed
        pre_close_ts = pos.close_ts
        pre_close_reason = pos.close_reason
        pre_close_price = pos.close_price
        pre_last_close_ts = bot.state.last_close_ts.get((pos.symbol, pos.direction))
        ev = ExitEvent(kind="SL_HIT", fraction_closed=1.0, description="x")
        cast(Any, bot.logger).write_position.side_effect = OSError("disk full")
        await bot._handle_exit_event(pos, ev, price=90.0)
        cast(Any, bot.notifier).send.assert_not_awaited()
        assert pos.open_fraction == pre_open_fraction
        assert pos.closed is pre_closed
        assert pos.close_ts == pre_close_ts
        assert pos.close_reason == pre_close_reason
        assert pos.close_price == pre_close_price
        assert bot.state.open_positions() == [pos]
        assert bot.state.last_close_ts.get((pos.symbol, pos.direction)) == pre_last_close_ts
    asyncio.run(_run())


def test_exit_write_position_failure_rolls_back_trailing_move(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        cand = _candidate()
        pos = bot.state.open_from_signal(cand, bot.cfg)
        old_sl = pos.stop_loss_price
        ev = ExitEvent(kind="TRAILING_MOVE", fraction_closed=0.0, new_stop_loss_price=old_sl + 10.0)
        cast(Any, bot.logger).write_position.side_effect = OSError("disk full")
        await bot._handle_exit_event(pos, ev, price=110.0)
        cast(Any, bot.notifier).send.assert_not_awaited()
        assert pos.stop_loss_price == old_sl
    asyncio.run(_run())


def test_decision_summary_counts_blocks_accepts_and_exits(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        bot._decision_summary = DecisionSummary()
        cand = _candidate()
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand]):
            await bot._handle_entry_signals_locked(cand.snapshot)
        assert bot._decision_summary.total_candidates == 1
        assert bot._decision_summary.long_candidates == 1
        assert bot._decision_summary.accepted_signals == 1

        cand2 = _candidate()
        bot.state.mark_alerted("BTCUSDT", Direction.LONG)
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand2]):
            await bot._handle_entry_signals_locked(cand2.snapshot)
        assert bot._decision_summary.rejected_or_blocked >= 1
        assert bot._decision_summary.blocked_counts_by_reason.get("cooldown", 0) >= 1

        pos = bot.state.open_positions()[0]
        await bot._handle_exit_event(pos, ExitEvent(kind="EXIT_REGIME_INVALIDATED", fraction_closed=1.0, description="x"), 99.0)
        assert bot._decision_summary.exit_counts_by_reason.get("exit_regime_invalidated", 0) == 1

    asyncio.run(_run())


def test_collect_data_health_counts_missing_and_stale(tmp_path):
    bot = _bot(tmp_path)
    bot._decision_summary = DecisionSummary()
    now = datetime.now(tz=UTC)
    snap = Snapshot(symbol="BTCUSDT", ts=now, price=100.0, funding_rate_ts=now.replace(year=now.year-1))
    bot._collect_data_health(snap)
    assert bot._decision_summary.missing_data_counts_by_reason.get("missing_regime_ema", 0) == 1
    assert bot._decision_summary.stale_data_counts_by_reason.get("stale_funding", 0) == 1


def test_persist_decision_summary_writes_required_schema_before_reset(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        bot._decision_summary = DecisionSummary(
            total_candidates=3,
            long_candidates=2,
            short_candidates=1,
            accepted_signals=1,
            rejected_or_blocked=2,
            downgrade_counts_by_reason={"trend_regime": 2},
            blocked_counts_by_reason={"cooldown": 2},
            exit_counts_by_reason={"exit_regime_invalidated": 1},
            missing_data_counts_by_reason={"missing_regime_ema": 1},
            stale_data_counts_by_reason={"stale_funding": 1},
        )
        now = datetime(2026, 5, 18, 10, 0, tzinfo=UTC)
        await bot._persist_decision_summary(now)
        cast(Any, bot.logger).write_decision_summary.assert_awaited_once()
        payload = cast(Any, bot.logger).write_decision_summary.await_args.args[0]
        assert payload["event_type"] == "decision_summary"
        assert payload["timeframe_short"] == "15m"
        # README intent: regime stays on 1h even when entry tf is 15m.
        assert payload["regime_timeframe"] == "1h"
        for key in (
            "total_candidates",
            "long_candidates",
            "short_candidates",
            "accepted_signals",
            "rejected_or_blocked",
            "downgrade_counts_by_reason",
            "blocked_counts_by_reason",
            "exit_counts_by_reason",
            "missing_data_counts_by_reason",
            "stale_data_counts_by_reason",
        ):
            assert key in payload
        assert payload["blocked_counts_by_reason"] == {"cooldown": 2}
        assert payload["downgrade_counts_by_reason"] == {"trend_regime": 2}
        assert bot._decision_summary.total_candidates == 3

    asyncio.run(_run())
