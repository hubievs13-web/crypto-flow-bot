"""Integration tests for the entry-side risk gate (max_concurrent + max_daily_losses)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_flow_bot.config import Config, RiskCfg
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot


def _snap(symbol: str, price: float = 100.0) -> Snapshot:
    return Snapshot(
        symbol=symbol, ts=datetime.now(tz=UTC), price=price, atr_1h=1.0,
        funding_rate=0.0012,  # triggers funding_extreme SHORT
    )


def _bot(tmp_path, cfg: Config) -> Bot:
    """Construct a Bot with the side effects (notifier, logger, http) mocked."""
    bot = Bot.__new__(Bot)
    bot.cfg = cfg
    bot.state = StateStore(path=tmp_path)
    bot.notifier = MagicMock()
    bot.notifier.send = AsyncMock()
    bot.logger = MagicMock()
    bot.logger.write_alert = AsyncMock()
    bot.logger.write_position = AsyncMock()
    return bot


@pytest.mark.asyncio
async def test_max_concurrent_positions_blocks_third_entry(tmp_path):
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(max_concurrent_positions=2, max_daily_losses=None),
    )
    bot = _bot(tmp_path, cfg)
    # First two snapshots → both opened (different symbols, different cooldown buckets).
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    await bot._handle_entry_signals(_snap("ETHUSDT"))
    assert len(bot.state.open_positions()) == 2
    # Third snapshot → blocked by max_concurrent_positions.
    await bot._handle_entry_signals(_snap("SOLUSDT"))
    assert len(bot.state.open_positions()) == 2


@pytest.mark.asyncio
async def test_max_per_direction_blocks_same_side_extra(tmp_path):
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(max_concurrent_positions=3, max_per_direction=1, max_daily_losses=None),
    )
    bot = _bot(tmp_path, cfg)
    # All three snapshots produce SHORT signals (positive funding).
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    await bot._handle_entry_signals(_snap("ETHUSDT"))
    await bot._handle_entry_signals(_snap("SOLUSDT"))
    open_positions = bot.state.open_positions()
    assert len(open_positions) == 1
    assert open_positions[0].direction == Direction.SHORT


@pytest.mark.asyncio
async def test_daily_loss_cap_blocks_new_entries(tmp_path):
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(max_concurrent_positions=10, max_daily_losses=2),
    )
    bot = _bot(tmp_path, cfg)
    # Simulate 2 losses already taken today.
    bot.state.record_loss_if_today("SL_HIT")
    bot.state.record_loss_if_today("SL_HIT")
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    assert len(bot.state.open_positions()) == 0


@pytest.mark.asyncio
async def test_handle_exit_event_increments_loss_counter(tmp_path):
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="funding_extreme", description="x")],
        snapshot=_snap("BTCUSDT"),
    )
    pos = bot.state.open_from_signal(candidate, cfg)
    # Synthetic SL_HIT event.
    ev = MagicMock()
    ev.kind = "SL_HIT"
    ev.fraction_closed = 1.0
    ev.new_stop_loss_price = None
    await bot._handle_exit_event(pos, ev, price=98.0)
    assert bot.state.losses_today_count == 1


@pytest.mark.asyncio
async def test_handle_exit_event_does_not_count_tp_or_time_stop(tmp_path):
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="funding_extreme", description="x")],
        snapshot=_snap("BTCUSDT"),
    )
    pos = bot.state.open_from_signal(candidate, cfg)
    for kind, frac in [("TP_HIT", 0.5), ("TIME_STOP", 0.5)]:
        ev = MagicMock()
        ev.kind = kind
        ev.fraction_closed = frac
        ev.new_stop_loss_price = None
        await bot._handle_exit_event(pos, ev, price=100.0)
    assert bot.state.losses_today_count == 0
