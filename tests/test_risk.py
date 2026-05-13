"""Integration tests for the entry-side risk gate.

The gates (in order): cooldown, position_open, opposite_open,
post_exit_cooldown, max_concurrent, max_per_direction_group.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_flow_bot.config import Config, RiskCfg, SignalsCfg
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import ConfluenceCache, FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.main import Bot


def _snap(symbol: str, price: float = 100.0) -> Snapshot:
    # Funding + LSR both fire SHORT — funding alone would be blocked by the
    # confirmation gate, so we add LSR as the confirming partner.
    return Snapshot(
        symbol=symbol, ts=datetime.now(tz=UTC), price=price, atr_1h=1.0,
        funding_rate=0.0012, long_short_ratio=2.7,
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
    bot.logger.write_blocked = AsyncMock()
    # Bot.__new__ skips __init__, so wire up the snapshot cache used by
    # `_build_exit_snapshot` and the lock used by `_handle_entry_signals`.
    bot._last_full_snapshot = {}
    bot._entry_lock = asyncio.Lock()
    bot.confluence_cache = ConfluenceCache(
        window_minutes=cfg.signals.confluence_window_minutes,
    )
    return bot


@pytest.mark.asyncio
async def test_max_concurrent_positions_blocks_third_entry(tmp_path):
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(max_concurrent_positions=2),
    )
    bot = _bot(tmp_path, cfg)
    # First two snapshots → both opened (different symbols, different cooldown buckets).
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    await bot._handle_entry_signals(_snap("ETHUSDT"))
    assert len(bot.state.open_positions()) == 2
    # Third snapshot → blocked by max_concurrent_positions.
    await bot._handle_entry_signals(_snap("SOLUSDT"))
    assert len(bot.state.open_positions()) == 2
    # The block must have been recorded with a stable reason token.
    blocked_calls = bot.logger.write_blocked.await_args_list
    reasons = [c.kwargs["blocked_reason"] for c in blocked_calls]
    assert "max_concurrent" in reasons


@pytest.mark.asyncio
async def test_max_per_direction_applies_only_within_correlated_group(tmp_path):
    """max_per_direction=1 inside [BTCUSDT, ETHUSDT] must block a second
    same-side entry between BTC and ETH, but must NOT block SOLUSDT (which
    is outside every group)."""
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(
            max_concurrent_positions=3,
            max_per_direction=1,
            correlated_groups=[["BTCUSDT", "ETHUSDT"]],
        ),
    )
    bot = _bot(tmp_path, cfg)
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    await bot._handle_entry_signals(_snap("ETHUSDT"))  # same group, same side -> blocked
    await bot._handle_entry_signals(_snap("SOLUSDT"))  # no group -> allowed
    open_positions = bot.state.open_positions()
    open_symbols = {p.symbol for p in open_positions}
    assert open_symbols == {"BTCUSDT", "SOLUSDT"}
    assert all(p.direction == Direction.SHORT for p in open_positions)


@pytest.mark.asyncio
async def test_max_per_direction_without_groups_has_no_effect(tmp_path):
    """max_per_direction with an empty correlated_groups list must NOT
    behave like the old global cap — it applies per group, and no group
    means no cap."""
    cfg = Config(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        risk=RiskCfg(
            max_concurrent_positions=3,
            max_per_direction=1,
            correlated_groups=[],
        ),
    )
    bot = _bot(tmp_path, cfg)
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    await bot._handle_entry_signals(_snap("ETHUSDT"))
    await bot._handle_entry_signals(_snap("SOLUSDT"))
    assert len(bot.state.open_positions()) == 3


@pytest.mark.asyncio
async def test_post_exit_cooldown_blocks_reentry(tmp_path):
    """After a position closes on a (symbol, direction), the same side must
    not reopen for `post_exit_cooldown_seconds`."""
    cfg = Config(
        symbols=["BTCUSDT"],
        risk=RiskCfg(post_exit_cooldown_seconds=7200),
    )
    bot = _bot(tmp_path, cfg)
    # Open and immediately close a BTC SHORT.
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.SHORT,
        fired_rules=[
            FiredRule(name="funding_extreme", description="x"),
            FiredRule(name="lsr_extreme", description="y"),
        ],
        snapshot=_snap("BTCUSDT"),
        confluence_window_rules={"funding_extreme", "lsr_extreme"},
    )
    pos = bot.state.open_from_signal(candidate, cfg)
    bot.state.close_position(pos, price=100.0, reason="TIME_STOP")

    # A fresh entry attempt on BTC SHORT must be blocked by post_exit_cooldown.
    await bot._handle_entry_signals(_snap("BTCUSDT"))
    assert len(bot.state.open_positions()) == 0
    reasons = [c.kwargs["blocked_reason"] for c in bot.logger.write_blocked.await_args_list]
    assert "post_exit_cooldown" in reasons


@pytest.mark.asyncio
async def test_post_exit_cooldown_zero_disables_gate(tmp_path):
    """With cooldown=0, an immediate re-entry on a fresh signal is allowed."""
    cfg = Config(
        symbols=["BTCUSDT"],
        risk=RiskCfg(post_exit_cooldown_seconds=0),
    )
    bot = _bot(tmp_path, cfg)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.SHORT,
        fired_rules=[
            FiredRule(name="funding_extreme", description="x"),
            FiredRule(name="lsr_extreme", description="y"),
        ],
        snapshot=_snap("BTCUSDT"),
        confluence_window_rules={"funding_extreme", "lsr_extreme"},
    )
    pos = bot.state.open_from_signal(candidate, cfg)
    bot.state.close_position(pos, price=100.0, reason="TIME_STOP")
    # Bypass alert cooldown so this isn't masked.
    bot.state.last_alert_ts.clear()

    await bot._handle_entry_signals(_snap("BTCUSDT"))
    assert len(bot.state.open_positions()) == 1


@pytest.mark.asyncio
async def test_handle_exit_event_does_not_modify_legacy_loss_counter(tmp_path):
    """Sanity: the entry path no longer reads/writes any daily-loss counter
    (the gate was deleted). Exit events should just close the position and
    notify, with no side effects beyond that."""
    cfg = Config(symbols=["BTCUSDT"])
    bot = _bot(tmp_path, cfg)
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="x")],
        snapshot=_snap("BTCUSDT"),
        confluence_window_rules={"lsr_extreme"},
    )
    pos = bot.state.open_from_signal(candidate, cfg)
    ev = MagicMock()
    ev.kind = "SL_HIT"
    ev.fraction_closed = 1.0
    ev.new_stop_loss_price = None
    await bot._handle_exit_event(pos, ev, price=98.0)
    # No daily-loss counter exists anymore.
    assert not hasattr(bot.state, "losses_today_count")
    # Post-exit cooldown bookkeeping was activated.
    assert ("BTCUSDT", Direction.LONG) in bot.state.last_close_ts


@pytest.mark.asyncio
async def test_conflict_policy_skips_when_long_and_short_both_fire(tmp_path):
    """A snapshot that fires *both* a LONG and a SHORT rule must be skipped
    entirely instead of arbitrarily opening LONG first (and then blocking the
    SHORT as opposite-side, which was the previous behavior)."""
    from crypto_flow_bot.config import (
        FundingExtremeCfg,
        LsrExtremeCfg,
    )

    cfg = Config(
        symbols=["BTCUSDT"],
        risk=RiskCfg(max_concurrent_positions=10),
        signals=SignalsCfg(
            funding_extreme=FundingExtremeCfg(
                long_overheated_above=0.0008, short_overheated_below=-0.0008,
            ),
            lsr_extreme=LsrExtremeCfg(long_heavy_above=2.5, short_heavy_below=0.6),
            # Disable the funding-confirmation gate so this scenario produces
            # one candidate per direction (otherwise SHORT-funding alone is
            # dropped and there is no conflict to test).
            funding_extreme_requires_confirmation=False,
        ),
    )
    bot = _bot(tmp_path, cfg)
    # funding +0.0012 -> SHORT, LSR 0.55 -> LONG -> conflict.
    snap = Snapshot(
        symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=100.0, atr_1h=1.0,
        funding_rate=0.0012, long_short_ratio=0.55,
    )
    await bot._handle_entry_signals(snap)
    assert len(bot.state.open_positions()) == 0
    reasons = [c.kwargs["blocked_reason"] for c in bot.logger.write_blocked.await_args_list]
    assert reasons == ["conflicting_signals", "conflicting_signals"]


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
