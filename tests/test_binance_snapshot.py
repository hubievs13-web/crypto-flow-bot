"""Tests for the data-layer expansion (4h klines, taker quote volume,
per-metric freshness timestamps) added in the data-layer-foundation PR.

We use a fake `BinanceClient` and `LiquidationStream` so the test does not
hit the network — only the orchestration in `build_snapshot` is exercised.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from crypto_flow_bot.data.binance import (
    _kline_derivatives,
    _taker_quote_volumes,
    build_snapshot,
)

# ─── _kline_derivatives ─────────────────────────────────────────────────────


def test_kline_derivatives_drops_in_progress_last_bar():
    """The last kline from Binance is always in-progress; price_change_pct
    must be computed on the two prior fully-closed bars, NOT the partial one."""
    # 52 bars total: indices 0..51. Last (index 51) is in-progress.
    # Use distinct closes so we can verify which two were compared.
    bars: list[list] = []
    for i in range(52):
        close = 100.0 + i  # 100, 101, ..., 151
        bars.append([0, "0", "0", "0", str(close), "0", 0, "0", 0, "0", "0", "0"])
    # Override the last (in-progress) close to a wild value.
    bars[-1][4] = "9999.0"
    pc, ema, atr = _kline_derivatives(bars)
    # Closed bars are 0..50 (51 bars). Last two closed closes are 150 and 149.
    # Expected price_change_pct = (150 - 149) / 149.
    assert pc is not None
    assert abs(pc - (1.0 / 149.0)) < 1e-9


def test_kline_derivatives_handles_empty_input():
    pc, ema, atr = _kline_derivatives([])
    assert pc is None and ema is None and atr is None


def test_kline_derivatives_handles_malformed_bar():
    """A malformed bar should not crash; we return None for everything."""
    # Each bar must have at least indices 0..4, but values are unparseable.
    bars = [
        [0, "x", "x", "x", "x", "0", 0, "0", 0, "0", "0", "0"],
        [0, "x", "x", "x", "x", "0", 0, "0", 0, "0", "0", "0"],
    ]
    pc, ema, atr = _kline_derivatives(bars)
    assert pc is None and ema is None and atr is None


# ─── _taker_quote_volumes ───────────────────────────────────────────────────


def test_taker_quote_volumes_extracts_buy_and_residual_sell():
    """Bar quote volume = 1000 USDT, taker buy quote = 600 → taker sell = 400."""
    # Provide 2 bars; the helper uses the second-to-last (the last is in-progress).
    bars = [
        [0, "0", "0", "0", "0", "0", 0, "1000.0", 10, "0", "600.0", "0"],
        [0, "0", "0", "0", "0", "0", 0, "0", 0, "0", "0", "0"],  # in-progress (ignored)
    ]
    buy, sell = _taker_quote_volumes(bars)
    assert buy == 600.0
    assert sell == 400.0


def test_taker_quote_volumes_returns_none_on_insufficient_bars():
    assert _taker_quote_volumes([]) == (None, None)
    one_bar = [[0, "0", "0", "0", "0", "0", 0, "100.0", 1, "0", "50.0", "0"]]
    assert _taker_quote_volumes(one_bar) == (None, None)


def test_taker_quote_volumes_clamps_negative_residual_sell():
    """Floating-point edge: when taker buy == total, the residual sell can
    come out slightly negative. It should be clamped to zero, not propagated."""
    bars = [
        [0, "0", "0", "0", "0", "0", 0, "100.0", 1, "0", "100.0000001", "0"],
        [0, "0", "0", "0", "0", "0", 0, "0", 0, "0", "0", "0"],
    ]
    buy, sell = _taker_quote_volumes(bars)
    assert buy is not None and sell == 0.0


def test_taker_quote_volumes_returns_none_on_malformed_fields():
    bars = [
        [0, "0", "0", "0", "0", "0", 0, "x", 1, "0", "x", "0"],
        [0, "0", "0", "0", "0", "0", 0, "0", 0, "0", "0", "0"],
    ]
    assert _taker_quote_volumes(bars) == (None, None)


# ─── build_snapshot orchestration ───────────────────────────────────────────


def _kline_row(close: float, *, total_qv: str = "1000", taker_buy_qv: str = "500") -> list:
    """One Binance kline array with controllable close and taker volumes."""
    return [0, str(close), str(close + 5), str(close - 5), str(close),
            "10", 0, total_qv, 10, "5", taker_buy_qv, "0"]


def _trending_klines(n: int = 52) -> list[list]:
    """A clean upward-trending series suitable for EMA/ATR computation."""
    return [_kline_row(100.0 + i) for i in range(n)]


@pytest.mark.asyncio
async def test_build_snapshot_populates_freshness_timestamps():
    """build_snapshot must stamp every real-time metric with a freshness
    timestamp so the signals freshness gate has something to compare."""
    client = AsyncMock()
    client.funding_rate.return_value = 0.0001
    client.open_interest_usd.return_value = 1_000_000.0
    client.top_long_short_position_ratio.return_value = 1.2
    client.latest_price.return_value = 50_000.0
    client.open_interest_history.return_value = [
        {"sumOpenInterestValue": "1000000"},
        {"sumOpenInterestValue": "1000000"},
    ]
    client.klines = AsyncMock(return_value=_trending_klines())

    liq_stream = AsyncMock()
    liq_stream.totals = lambda _symbol: (0.0, 0.0)

    before = datetime.now(tz=UTC)
    snap = await build_snapshot(client, liq_stream, "BTCUSDT", oi_window_minutes=60)
    after = datetime.now(tz=UTC)

    for ts in (snap.funding_rate_ts, snap.open_interest_ts, snap.long_short_ratio_ts, snap.ts):
        assert ts is not None
        assert before <= ts <= after


@pytest.mark.asyncio
async def test_build_snapshot_populates_4h_kline_derivatives_when_enabled():
    """With `enable_4h_klines=True` (default), `klines` is called twice
    (once for 1h, once for 4h) and the snapshot carries 4h EMA/ATR/pct fields."""
    client = AsyncMock()
    client.funding_rate.return_value = 0.0
    client.open_interest_usd.return_value = 1_000_000.0
    client.top_long_short_position_ratio.return_value = 1.0
    client.latest_price.return_value = 100.0
    client.open_interest_history.return_value = []
    client.klines = AsyncMock(return_value=_trending_klines())

    liq_stream = AsyncMock()
    liq_stream.totals = lambda _symbol: (0.0, 0.0)

    snap = await build_snapshot(client, liq_stream, "BTCUSDT", oi_window_minutes=60)

    assert client.klines.await_count == 2
    intervals = {call.args[1] for call in client.klines.await_args_list}
    assert intervals == {"1h", "4h"}

    # 4h derivatives must be populated (same trending series so non-None).
    assert snap.price_change_pct_4h is not None
    assert snap.ema50_4h is not None
    assert snap.atr_4h is not None


@pytest.mark.asyncio
async def test_build_snapshot_skips_4h_when_disabled():
    """With `enable_4h_klines=False` only the 1h kline call happens and
    the 4h fields are left as None — saves one REST roundtrip per cycle."""
    client = AsyncMock()
    client.funding_rate.return_value = 0.0
    client.open_interest_usd.return_value = 1_000_000.0
    client.top_long_short_position_ratio.return_value = 1.0
    client.latest_price.return_value = 100.0
    client.open_interest_history.return_value = []
    client.klines = AsyncMock(return_value=_trending_klines())

    liq_stream = AsyncMock()
    liq_stream.totals = lambda _symbol: (0.0, 0.0)

    snap = await build_snapshot(
        client, liq_stream, "BTCUSDT", oi_window_minutes=60, enable_4h_klines=False,
    )

    assert client.klines.await_count == 1
    assert client.klines.await_args.args[1] == "1h"
    assert snap.price_change_pct_4h is None
    assert snap.ema50_4h is None
    assert snap.atr_4h is None


@pytest.mark.asyncio
async def test_build_snapshot_populates_taker_volumes_from_1h_kline():
    """taker_buy_quote_1h / taker_sell_quote_1h must be sourced from the
    last fully-closed 1h bar (not the in-progress one) and sum to total qv."""
    # Build 52 bars; second-to-last (index 50) is the last fully-closed.
    bars = [_kline_row(100.0 + i, total_qv="0", taker_buy_qv="0") for i in range(52)]
    # The bar we care about is bars[-2].
    bars[-2] = [0, "100", "105", "95", "100", "10", 0, "1000.0", 10, "5", "700.0", "0"]

    client = AsyncMock()
    client.funding_rate.return_value = 0.0
    client.open_interest_usd.return_value = 1_000_000.0
    client.top_long_short_position_ratio.return_value = 1.0
    client.latest_price.return_value = 100.0
    client.open_interest_history.return_value = []
    client.klines = AsyncMock(return_value=bars)

    liq_stream = AsyncMock()
    liq_stream.totals = lambda _symbol: (0.0, 0.0)

    snap = await build_snapshot(client, liq_stream, "BTCUSDT", oi_window_minutes=60)
    assert snap.taker_buy_quote_1h == 700.0
    assert snap.taker_sell_quote_1h == 300.0


@pytest.mark.asyncio
async def test_build_snapshot_to_log_dict_is_json_serializable():
    """Per-metric freshness timestamps must be flattened to ISO strings
    in `to_log_dict()` so the JSONL writer doesn't choke on datetime."""
    import json

    client = AsyncMock()
    client.funding_rate.return_value = 0.0
    client.open_interest_usd.return_value = 1_000_000.0
    client.top_long_short_position_ratio.return_value = 1.0
    client.latest_price.return_value = 100.0
    client.open_interest_history.return_value = []
    client.klines = AsyncMock(return_value=_trending_klines())

    liq_stream = AsyncMock()
    liq_stream.totals = lambda _symbol: (0.0, 0.0)

    snap = await build_snapshot(client, liq_stream, "BTCUSDT", oi_window_minutes=60)
    # Should not raise.
    json.dumps(snap.to_log_dict())
