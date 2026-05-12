"""Tests for the real-time liquidation-cascade fast path (`Bot._liq_fast_loop`).

The fast loop reads the liquidation aggregator's in-memory rolling window
every `liq_fast_check_interval_seconds` and fires the normal entry pipeline
as soon as a symbol's window crosses the per-symbol `liq_cascade.usd_threshold`,
bypassing the 60s `_poll_loop` cadence for this one signal type.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_flow_bot.config import Config, LiqCascadeCfg, RiskCfg, SignalsCfg
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot


def _bot(tmp_path, cfg: Config, liq_totals: dict[str, tuple[float, float]]) -> Bot:
    bot = Bot.__new__(Bot)
    bot.cfg = cfg
    bot.state = StateStore(path=tmp_path)
    bot.notifier = MagicMock()
    bot.notifier.send = AsyncMock()
    bot.logger = MagicMock()
    bot.logger.write_alert = AsyncMock()
    bot.logger.write_position = AsyncMock()
    bot.logger.write_snapshot = AsyncMock()
    bot._last_full_snapshot = {}
    bot._entry_lock = asyncio.Lock()
    bot._stop = asyncio.Event()
    bot.client = MagicMock()
    bot.liq_stream = MagicMock()
    bot.liq_stream.totals = MagicMock(side_effect=lambda s: liq_totals.get(s, (0.0, 0.0)))
    return bot


def _liq_snap(symbol: str, long_usd: float, short_usd: float) -> Snapshot:
    """Snapshot that engine.signals.evaluate() will treat as a liq_cascade fire.

    `atr_1h` is set so `state.open_from_signal` has a valid stop-loss distance.
    `ema50_1h` is omitted so the trend filter does not block either direction
    (liq_cascade is exempt from trend filter anyway, but we keep it simple).
    """
    return Snapshot(
        symbol=symbol,
        ts=datetime.now(tz=UTC),
        price=100.0,
        atr_1h=1.0,
        long_liquidations_usd_window=long_usd,
        short_liquidations_usd_window=short_usd,
    )


def _liq_cfg(threshold: float = 1_000_000.0) -> Config:
    """Config where liq_cascade fires above `threshold` and other signals are off."""
    from crypto_flow_bot.config import (
        FundingExtremeCfg,
        LsrExtremeCfg,
        OiSurgeCfg,
        TrendFilterCfg,
    )
    return Config(
        symbols=["BTCUSDT", "ETHUSDT"],
        alert_cooldown_seconds=1800,
        liq_fast_check_interval_seconds=5,
        risk=RiskCfg(max_concurrent_positions=10, max_daily_losses=None),
        signals=SignalsCfg(
            funding_extreme=FundingExtremeCfg(enabled=False),
            oi_surge=OiSurgeCfg(enabled=False),
            lsr_extreme=LsrExtremeCfg(enabled=False),
            liq_cascade=LiqCascadeCfg(enabled=True, usd_threshold=threshold),
            trend_filter=TrendFilterCfg(enabled=False),
        ),
    )


# ─── Fast-loop trigger behavior ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fast_loop_fires_alert_when_long_liq_window_crosses_threshold(tmp_path):
    """Long-side liq flush above threshold → LONG entry opened, alert sent."""
    cfg = _liq_cfg(threshold=1_000_000.0)
    # BTC long-liq window is well above threshold; short-side is zero.
    bot = _bot(tmp_path, cfg, liq_totals={"BTCUSDT": (5_000_000.0, 0.0)})
    fake_snap = _liq_snap("BTCUSDT", long_usd=5_000_000.0, short_usd=0.0)

    with patch("crypto_flow_bot.main.build_snapshot", new=AsyncMock(return_value=fake_snap)):
        # Stop after one iteration so the loop doesn't run forever.
        async def _stop_after_one_iter() -> None:
            await asyncio.sleep(0)  # let the loop start
            bot._stop.set()

        await asyncio.gather(bot._liq_fast_loop(), _stop_after_one_iter())

    # Exactly one Telegram alert and exactly one open LONG position.
    bot.notifier.send.assert_awaited()
    open_positions = bot.state.open_positions()
    assert len(open_positions) == 1
    assert open_positions[0].symbol == "BTCUSDT"
    assert open_positions[0].direction == Direction.LONG


@pytest.mark.asyncio
async def test_fast_loop_does_not_fire_below_threshold(tmp_path):
    """Long-liq window below threshold → no snapshot fetch, no alert, no position."""
    cfg = _liq_cfg(threshold=10_000_000.0)
    bot = _bot(tmp_path, cfg, liq_totals={"BTCUSDT": (1_000.0, 1_000.0)})
    build = AsyncMock()

    with patch("crypto_flow_bot.main.build_snapshot", new=build):
        async def _stop_after_one_iter() -> None:
            await asyncio.sleep(0)
            bot._stop.set()

        await asyncio.gather(bot._liq_fast_loop(), _stop_after_one_iter())

    build.assert_not_awaited()
    bot.notifier.send.assert_not_awaited()
    assert bot.state.open_positions() == []


@pytest.mark.asyncio
async def test_fast_loop_skips_build_snapshot_when_cooldown_active(tmp_path):
    """Crossed threshold but cooldown ticking → no REST roundtrip until cooldown expires."""
    cfg = _liq_cfg(threshold=1_000_000.0)
    bot = _bot(tmp_path, cfg, liq_totals={"BTCUSDT": (5_000_000.0, 0.0)})
    # Pretend we already alerted LONG on BTCUSDT a moment ago — cooldown is active.
    bot.state.mark_alerted("BTCUSDT", Direction.LONG)
    build = AsyncMock()

    with patch("crypto_flow_bot.main.build_snapshot", new=build):
        async def _stop_after_one_iter() -> None:
            await asyncio.sleep(0)
            bot._stop.set()

        await asyncio.gather(bot._liq_fast_loop(), _stop_after_one_iter())

    # Crucial: build_snapshot is NOT called while cooldown is active. Without
    # this short-circuit the loop would do a ~250ms REST roundtrip every 5s
    # for up to 2h after every alert.
    build.assert_not_awaited()
    bot.notifier.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_fast_loop_disabled_when_liq_cascade_disabled(tmp_path):
    """Globally disabling liq_cascade in config disables the fast path."""
    cfg = _liq_cfg(threshold=1_000_000.0)
    cfg.signals.liq_cascade.enabled = False
    bot = _bot(tmp_path, cfg, liq_totals={"BTCUSDT": (5_000_000.0, 0.0)})
    build = AsyncMock()

    with patch("crypto_flow_bot.main.build_snapshot", new=build):
        async def _stop_after_one_iter() -> None:
            await asyncio.sleep(0)
            bot._stop.set()

        await asyncio.gather(bot._liq_fast_loop(), _stop_after_one_iter())

    build.assert_not_awaited()


# ─── Concurrency / lock behavior ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_entry_lock_prevents_double_fire_on_concurrent_handle_calls(tmp_path):
    """Two coroutines (poll-loop + fast-loop) calling `_handle_entry_signals`
    concurrently on the same (symbol, direction) must result in exactly one
    open position and one Telegram alert.

    Without the lock, both could pass the `cooldown_remaining_seconds == 0`
    + `open_for(...) is None` checks before either reaches `mark_alerted`,
    so the user would see a duplicate alert + a phantom second position.
    """
    cfg = _liq_cfg(threshold=1_000_000.0)
    bot = _bot(tmp_path, cfg, liq_totals={})
    snap = _liq_snap("BTCUSDT", long_usd=5_000_000.0, short_usd=0.0)

    await asyncio.gather(
        bot._handle_entry_signals(snap),
        bot._handle_entry_signals(snap),
    )

    open_positions = bot.state.open_positions()
    assert len(open_positions) == 1, (
        f"expected exactly 1 open position, got {len(open_positions)}"
    )
    # One Telegram send for the single fired alert.
    assert bot.notifier.send.await_count == 1
