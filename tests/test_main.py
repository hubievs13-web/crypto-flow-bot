from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch


from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot


def _bot(tmp_path) -> Bot:
    bot = Bot.__new__(Bot)
    bot.cfg = Config(symbols=["BTCUSDT"], alert_cooldown_seconds=3600)
    bot.state = StateStore(path=tmp_path)
    bot.notifier = MagicMock()
    bot.notifier.send = AsyncMock()
    bot.logger = MagicMock()
    bot.logger.write_alert = AsyncMock()
    bot.logger.write_position = AsyncMock()
    bot.logger.write_snapshot = AsyncMock()
    bot.logger.write_blocked = AsyncMock()
    bot._entry_lock = asyncio.Lock()
    bot.confluence_cache = None
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
        bot.notifier.send.side_effect = RuntimeError("telegram down")
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand]):
            await bot._handle_entry_signals_locked(cand.snapshot)
            await bot._handle_entry_signals_locked(cand.snapshot)
        assert len(bot.state.open_positions()) == 1
        assert bot.state.cooldown_remaining_seconds("BTCUSDT", Direction.LONG, 3600) > 0
        assert bot.notifier.send.await_count == 1
        assert bot.logger.write_alert.await_count == 1
    asyncio.run(_run())


def test_entry_write_position_failure_rolls_back_and_skips_telegram(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        cand = _candidate()
        bot.logger.write_position.side_effect = OSError("disk full")
        with patch("crypto_flow_bot.main.evaluate", return_value=[cand]):
            await bot._handle_entry_signals_locked(cand.snapshot)
        assert bot.state.open_positions() == []
        bot.notifier.send.assert_not_awaited()
    asyncio.run(_run())


def test_poll_loop_survives_snapshot_log_write_error(tmp_path):
    async def _run() -> None:
        bot = _bot(tmp_path)
        bot._stop = asyncio.Event()
        bot.client = MagicMock()
        bot.liq_stream = MagicMock()
        bot._last_full_snapshot = {}
        bot._augment_with_funding_stats = MagicMock()
        bot._handle_entry_signals = AsyncMock()
        bot._sleep = AsyncMock(side_effect=lambda _s: bot._stop.set())
        bot.logger.write_snapshot.side_effect = IOError("io err")
        snap = Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0)
        with patch("crypto_flow_bot.main.build_snapshot", new=AsyncMock(return_value=snap)):
            await bot._poll_loop()
        bot._handle_entry_signals.assert_awaited_once()
    asyncio.run(_run())
