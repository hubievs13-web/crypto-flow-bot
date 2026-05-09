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
    # Bot.__new__ skips __init__, so wire up the snapshot cache used by
    # `_build_exit_snapshot`.
    bot._last_full_snapshot = {}
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
async def test_conflict_policy_skips_when_long_and_short_both_fire(tmp_path):
    """A snapshot that fires *both* a LONG and a SHORT rule must be skipped
    entirely instead of arbitrarily opening LONG first (and then blocking the
    SHORT as opposite-side, which was the previous behavior)."""
    from crypto_flow_bot.config import (
        FundingExtremeCfg,
        LsrExtremeCfg,
        SignalsCfg,
    )

    cfg = Config(
        symbols=["BTCUSDT"],
        risk=RiskCfg(max_concurrent_positions=10, max_daily_losses=None),
        signals=SignalsCfg(
            # funding +0.001 → LONG side normally NOT triggered, SHORT trigger above 0.0008
            funding_extreme=FundingExtremeCfg(
                long_overheated_above=0.0008, short_overheated_below=-0.0008,
            ),
            # LSR 0.5 → SHORT side normally NOT triggered, LONG trigger below 0.6
            lsr_extreme=LsrExtremeCfg(long_heavy_above=2.5, short_heavy_below=0.6),
        ),
    )
    bot = _bot(tmp_path, cfg)
    # Construct a snapshot where funding fires SHORT (+0.0012) AND LSR fires
    # LONG (0.55) at the same time → conflict.
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0,
        funding_rate=0.0012, long_short_ratio=0.55,
    )
    await bot._handle_entry_signals(snap)
    # Conflict policy: skip both directions, no position opened.
    assert len(bot.state.open_positions()) == 0


@pytest.mark.asyncio
async def test_conflict_policy_does_not_block_single_direction(tmp_path):
    """Sanity: when only one direction fires (even with multiple rules), the
    conflict policy must NOT skip — strong confluence in one direction is the
    happy path."""
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0,
        funding_rate=0.0012,           # SHORT
        long_short_ratio=2.7,          # SHORT (same direction)
    )
    await bot._handle_entry_signals(snap)
    open_positions = bot.state.open_positions()
    assert len(open_positions) == 1
    assert open_positions[0].direction == Direction.SHORT


def test_build_exit_snapshot_uses_last_full_snapshot_metrics(tmp_path):
    """Exit-loop must combine fresh price with the latest cached funding/LSR
    so that reason_invalidation for funding_extreme/lsr_extreme can fire."""
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    bot._last_full_snapshot["BTCUSDT"] = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0,
        funding_rate=0.0001, long_short_ratio=1.05, atr_1h=1.2,
        open_interest_change_pct_window=0.01,
        long_liquidations_usd_window=1_000_000.0,
        short_liquidations_usd_window=2_000_000.0,
        ema50_1h=98.7,
    )
    snap = bot._build_exit_snapshot("BTCUSDT", price=101.5)
    assert snap.price == 101.5
    assert snap.funding_rate == 0.0001
    assert snap.long_short_ratio == 1.05
    assert snap.atr_1h == 1.2
    assert snap.open_interest_change_pct_window == 0.01
    assert snap.long_liquidations_usd_window == 1_000_000.0
    assert snap.short_liquidations_usd_window == 2_000_000.0
    assert snap.ema50_1h == 98.7


def test_build_exit_snapshot_falls_back_to_price_only_without_cache(tmp_path):
    """Right after process start the cache is empty — must not crash, just
    return a price-only snapshot."""
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    snap = bot._build_exit_snapshot("BTCUSDT", price=42.0)
    assert snap.price == 42.0
    assert snap.funding_rate is None
    assert snap.long_short_ratio is None


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
