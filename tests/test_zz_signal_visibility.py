from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot, DecisionSummary

from crypto_flow_bot.signal_visibility import install_trade_signal_visibility_patch


def _bot(tmp_path) -> Bot:
    install_trade_signal_visibility_patch()
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


def test_blocked_entry_signal_still_sends_telegram_and_logs_alert(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        first = _candidate()
        bot.state.open_from_signal(first, bot.cfg)
        blocked = _candidate()
        with patch("crypto_flow_bot.main.evaluate", return_value=[blocked]):
            await bot._handle_entry_signals_locked(blocked.snapshot)
        assert len(bot.state.open_positions()) == 1
        cast(Any, bot.notifier).send.assert_awaited_once()
        cast(Any, bot.logger).write_blocked.assert_awaited_once()
        cast(Any, bot.logger).write_alert.assert_awaited_once()
        alert = cast(Any, bot.logger).write_alert.await_args.args[0]
        assert alert.kind == "SIGNAL_BLOCKED"
        assert alert.payload["blocked_reason"] == "position_open"
        assert alert.payload["signal_id"] == blocked.signal_id

    asyncio.run(_run())


def test_liq_fast_loop_fetches_snapshot_even_when_crossed_direction_on_cooldown(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        bot._stop = asyncio.Event()
        bot.client = MagicMock()
        bot.liq_stream = MagicMock()
        threshold = bot.cfg.signals.liq_cascade.usd_threshold
        bot.liq_stream.totals.return_value = (threshold, 0.0)
        bot._last_full_snapshot = {}
        cast(Any, bot)._augment_with_funding_stats = MagicMock()
        cast(Any, bot)._handle_entry_signals = AsyncMock()
        cast(Any, bot)._sleep = AsyncMock(side_effect=lambda _s: bot._stop.set())
        bot.state.mark_alerted("BTCUSDT", Direction.LONG)
        snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0)
        with patch("crypto_flow_bot.main.build_snapshot", new=AsyncMock(return_value=snap)) as build:
            await bot._liq_fast_loop()
        build.assert_awaited_once()
        cast(Any, bot)._handle_entry_signals.assert_awaited_once()

    asyncio.run(_run())
