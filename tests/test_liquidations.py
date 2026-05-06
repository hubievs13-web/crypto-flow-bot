"""Tests for the multi-exchange liquidation aggregator.

We avoid live WebSocket connections by exercising the parsers directly and the
in-memory aggregation surface (`totals`, `totals_per_exchange`).
"""

from __future__ import annotations

import asyncio

import pytest

from crypto_flow_bot.data.liquidations import (
    BinanceLiqStream,
    BybitLiqStream,
    LiquidationStream,
    _LiqEvent,
)

# ─── Per-exchange parsers ───────────────────────────────────────────────────


def _make_binance() -> BinanceLiqStream:
    return BinanceLiqStream(symbols=["BTCUSDT"], append=lambda _ev: None, stopped=asyncio.Event())


def _make_bybit() -> BybitLiqStream:
    return BybitLiqStream(symbols=["BTCUSDT"], append=lambda _ev: None, stopped=asyncio.Event())


def test_binance_parses_long_liquidation_from_sell_side():
    parser = _make_binance()
    msg = {"e": "forceOrder", "o": {"s": "BTCUSDT", "S": "SELL", "ap": "70000", "q": "10"}}
    events = parser.parse(msg)
    assert len(events) == 1
    assert events[0].symbol == "BTCUSDT"
    assert events[0].liquidated_side == "LONG"
    assert events[0].notional_usd == pytest.approx(700_000.0)
    assert events[0].exchange == "binance"


def test_binance_parses_short_liquidation_from_buy_side():
    parser = _make_binance()
    msg = {"o": {"s": "ETHUSDT", "S": "BUY", "p": "3500", "q": "5"}}
    events = parser.parse(msg)
    assert len(events) == 1
    assert events[0].liquidated_side == "SHORT"
    assert events[0].notional_usd == pytest.approx(17_500.0)


def test_binance_skips_zero_qty_or_price():
    parser = _make_binance()
    assert parser.parse({"o": {"s": "BTCUSDT", "S": "SELL", "ap": "0", "q": "1"}}) == []
    assert parser.parse({"o": {"s": "BTCUSDT", "S": "SELL", "ap": "1", "q": "0"}}) == []


def test_binance_skips_unknown_side():
    parser = _make_binance()
    assert parser.parse({"o": {"s": "BTCUSDT", "S": "FOO", "ap": "1", "q": "1"}}) == []


def test_bybit_parses_long_liquidation_from_buy_event():
    # Per Bybit V5 docs: a `Buy` event = a long position was liquidated.
    parser = _make_bybit()
    msg = {
        "topic": "allLiquidation.BTCUSDT",
        "data": [{"T": 0, "s": "BTCUSDT", "S": "Buy", "v": "0.5", "p": "70000"}],
    }
    events = parser.parse(msg)
    assert len(events) == 1
    assert events[0].liquidated_side == "LONG"
    assert events[0].notional_usd == pytest.approx(35_000.0)
    assert events[0].exchange == "bybit"


def test_bybit_parses_short_liquidation_from_sell_event():
    parser = _make_bybit()
    msg = {
        "topic": "allLiquidation.SOLUSDT",
        "data": [{"T": 0, "s": "SOLUSDT", "S": "Sell", "v": "100", "p": "180"}],
    }
    events = parser.parse(msg)
    assert events[0].liquidated_side == "SHORT"
    assert events[0].notional_usd == pytest.approx(18_000.0)


def test_bybit_handles_multi_item_data_list():
    parser = _make_bybit()
    msg = {
        "topic": "allLiquidation.BTCUSDT",
        "data": [
            {"T": 0, "s": "BTCUSDT", "S": "Buy", "v": "0.1", "p": "70000"},
            {"T": 0, "s": "BTCUSDT", "S": "Sell", "v": "0.2", "p": "70100"},
        ],
    }
    events = parser.parse(msg)
    assert len(events) == 2
    assert {e.liquidated_side for e in events} == {"LONG", "SHORT"}


def test_bybit_ignores_non_liquidation_topics():
    parser = _make_bybit()
    assert parser.parse({"topic": "tickers.BTCUSDT", "data": []}) == []
    assert parser.parse({"op": "subscribe", "success": True}) == []


# ─── Aggregator ─────────────────────────────────────────────────────────────


def test_unknown_exchange_is_skipped():
    stream = LiquidationStream(window_minutes=5, exchanges=["binance", "doesnotexist"])
    assert stream.configured_exchanges == ["binance"]


def test_totals_sums_across_exchanges():
    stream = LiquidationStream(window_minutes=5, exchanges=["binance", "bybit"])
    # Inject events directly to bypass websocket.
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=100.0, ts=_now(), exchange="binance")
    )
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=50.0, ts=_now(), exchange="bybit")
    )
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="SHORT",
                  notional_usd=10.0, ts=_now(), exchange="bybit")
    )
    long_liq, short_liq = stream.totals("BTCUSDT")
    assert long_liq == pytest.approx(150.0)
    assert short_liq == pytest.approx(10.0)


def test_totals_per_exchange_breakdown():
    stream = LiquidationStream(window_minutes=5, exchanges=["binance", "bybit"])
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=200.0, ts=_now(), exchange="binance")
    )
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="SHORT",
                  notional_usd=80.0, ts=_now(), exchange="bybit")
    )
    out = stream.totals_per_exchange("BTCUSDT")
    assert out["binance"] == pytest.approx((200.0, 0.0))
    assert out["bybit"] == pytest.approx((0.0, 80.0))


def test_totals_filters_by_symbol():
    stream = LiquidationStream(window_minutes=5, exchanges=["binance"])
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=100.0, ts=_now(), exchange="binance")
    )
    stream._append(
        _LiqEvent(symbol="ETHUSDT", liquidated_side="LONG",
                  notional_usd=999.0, ts=_now(), exchange="binance")
    )
    assert stream.totals("BTCUSDT") == pytest.approx((100.0, 0.0))
    assert stream.totals("SOLUSDT") == pytest.approx((0.0, 0.0))


def test_evicts_events_older_than_window():
    from datetime import UTC, datetime, timedelta

    stream = LiquidationStream(window_minutes=5, exchanges=["binance"])
    old_ts = datetime.now(tz=UTC) - timedelta(minutes=10)
    stream._events.append(  # bypass _append's eviction so we can keep the stale row
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=999.0, ts=old_ts, exchange="binance")
    )
    stream._append(
        _LiqEvent(symbol="BTCUSDT", liquidated_side="LONG",
                  notional_usd=10.0, ts=_now(), exchange="binance")
    )
    long_liq, _ = stream.totals("BTCUSDT")
    assert long_liq == pytest.approx(10.0)


def _now():
    from datetime import UTC, datetime
    return datetime.now(tz=UTC)
