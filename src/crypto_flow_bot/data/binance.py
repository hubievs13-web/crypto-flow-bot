"""Binance USD-M futures public data — funding, OI, LSR, price, and liquidations.

All endpoints used here are public (no API key required).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import websockets
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from crypto_flow_bot.data.coinglass import CoinglassClient, base_coin_from_symbol
from crypto_flow_bot.engine.models import Snapshot

log = logging.getLogger(__name__)

REST_BASE = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


@dataclass
class _LiquidationEvent:
    symbol: str
    side: str  # "BUY" = a short was liquidated; "SELL" = a long was liquidated.
    notional_usd: float
    ts: datetime


class BinanceClient:
    """REST client for Binance USD-M futures public data."""

    def __init__(self, http: httpx.AsyncClient | None = None) -> None:
        self._http = http or httpx.AsyncClient(base_url=REST_BASE, timeout=10.0)
        self._owns_http = http is None

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _get(self, path: str, params: dict | None = None) -> dict | list:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(4),
            wait=wait_exponential(multiplier=0.5, max=8),
            retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError)),
            reraise=True,
        ):
            with attempt:
                r = await self._http.get(path, params=params)
                r.raise_for_status()
                return r.json()
        raise RuntimeError("unreachable")

    async def funding_rate(self, symbol: str) -> float:
        """Current 8h funding rate. e.g. 0.0008 = +0.08% / 8h."""
        data = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        assert isinstance(data, dict)
        return float(data["lastFundingRate"])

    async def mark_price(self, symbol: str) -> float:
        data = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        assert isinstance(data, dict)
        return float(data["markPrice"])

    async def open_interest_usd(self, symbol: str) -> float:
        """Current OI in USD = OI (contracts) * mark price."""
        oi_resp, mark_resp = await asyncio.gather(
            self._get("/fapi/v1/openInterest", {"symbol": symbol}),
            self._get("/fapi/v1/premiumIndex", {"symbol": symbol}),
        )
        assert isinstance(oi_resp, dict)
        assert isinstance(mark_resp, dict)
        oi = float(oi_resp["openInterest"])
        mark = float(mark_resp["markPrice"])
        return oi * mark

    async def open_interest_history(self, symbol: str, period: str = "5m", limit: int = 30) -> list[dict]:
        """Past OI snapshots. period in {5m,15m,30m,1h,2h,4h,6h,12h,1d}."""
        data = await self._get(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        assert isinstance(data, list)
        return data

    async def top_long_short_position_ratio(self, symbol: str, period: str = "5m") -> float:
        """Latest top-traders long/short *position* ratio."""
        data = await self._get(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": symbol, "period": period, "limit": 1},
        )
        assert isinstance(data, list) and data
        return float(data[0]["longShortRatio"])

    async def latest_price(self, symbol: str) -> float:
        data = await self._get("/fapi/v1/ticker/price", {"symbol": symbol})
        assert isinstance(data, dict)
        return float(data["price"])

    async def klines_1h(self, symbol: str, limit: int = 51) -> list[list]:
        """Return the last `limit` 1h OHLCV bars for the symbol.

        Each entry is the raw Binance kline array:
            [openTime, open, high, low, close, volume, closeTime, ...]
        With limit=51 we get 50 fully closed bars + 1 in-progress bar.
        """
        data = await self._get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "1h", "limit": limit},
        )
        assert isinstance(data, list)
        return data


def compute_ema(values: list[float], period: int) -> float | None:
    """Standard EMA seeded with the SMA of the first `period` values."""
    if len(values) < period or period <= 0:
        return None
    alpha = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = alpha * v + (1.0 - alpha) * ema
    return ema


def compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    """Wilder-style ATR over the last `period` bars.

    Requires at least `period + 1` bars (we need the previous close for True Range).
    """
    if not (len(highs) == len(lows) == len(closes)) or len(highs) < period + 1 or period <= 0:
        return None
    trs: list[float] = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


class LiquidationStream:
    """Subscribes to Binance's all-symbol liquidation websocket and keeps a rolling window."""

    def __init__(self, window_minutes: int) -> None:
        self.window = timedelta(minutes=window_minutes)
        self._events: deque[_LiquidationEvent] = deque()
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="liq-stream")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def totals(self, symbol: str) -> tuple[float, float]:
        """Return (long_liquidations_usd, short_liquidations_usd) within the window."""
        self._evict_old()
        long_liq = 0.0
        short_liq = 0.0
        for ev in self._events:
            if ev.symbol != symbol:
                continue
            # Binance: side == SELL means a long was force-closed (sold to liquidate).
            # side == BUY means a short was force-closed.
            if ev.side == "SELL":
                long_liq += ev.notional_usd
            elif ev.side == "BUY":
                short_liq += ev.notional_usd
        return long_liq, short_liq

    def _evict_old(self) -> None:
        cutoff = datetime.now(tz=UTC) - self.window
        while self._events and self._events[0].ts < cutoff:
            self._events.popleft()

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopped.is_set():
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    log.info("liquidation stream connected")
                    backoff = 1.0
                    async for raw in ws:
                        if self._stopped.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        order = msg.get("o") or {}
                        symbol = order.get("s")
                        side = order.get("S")
                        price = float(order.get("ap") or order.get("p") or 0.0)
                        qty = float(order.get("q") or 0.0)
                        if not symbol or not side or qty <= 0 or price <= 0:
                            continue
                        notional = price * qty
                        self._events.append(
                            _LiquidationEvent(
                                symbol=symbol,
                                side=side,
                                notional_usd=notional,
                                ts=datetime.now(tz=UTC),
                            )
                        )
                        self._evict_old()
            except (TimeoutError, websockets.WebSocketException, OSError) as e:
                if self._stopped.is_set():
                    break
                log.warning("liquidation stream disconnected: %s; retrying in %.1fs", e, backoff)
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    break
                backoff = min(backoff * 2.0, 60.0)


async def build_snapshot(
    client: BinanceClient,
    liq_stream: LiquidationStream,
    symbol: str,
    oi_window_minutes: int,
    coinglass: CoinglassClient | None = None,
    coinglass_interval: str = "1h",
) -> Snapshot:
    """Pull a fresh snapshot for a symbol.

    Combines REST data and the WS-driven Binance liquidation totals. When a
    Coinglass client is supplied, also fetches cross-exchange aggregated
    liquidations for the most recent closed bar; on failure those fields stay
    None and the caller falls back to Binance-only.
    """
    coinglass_call = (
        coinglass.aggregated_liquidations(base_coin_from_symbol(symbol), coinglass_interval)
        if coinglass is not None
        else _none_async()
    )
    funding, oi_now, lsr, price, oi_hist, klines, agg_liq = await asyncio.gather(
        client.funding_rate(symbol),
        client.open_interest_usd(symbol),
        client.top_long_short_position_ratio(symbol),
        client.latest_price(symbol),
        client.open_interest_history(symbol, period="5m", limit=max(2, oi_window_minutes // 5 + 1)),
        client.klines_1h(symbol, limit=51),
        coinglass_call,
    )

    oi_change_pct: float | None = None
    if oi_hist:
        try:
            oldest = float(oi_hist[0]["sumOpenInterestValue"])
            if oldest > 0:
                oi_change_pct = (oi_now - oldest) / oldest
        except (KeyError, ValueError):
            oi_change_pct = None

    # 1h kline derivatives: price-change for OI alignment, EMA for trend, ATR for sizing.
    price_change_pct_1h: float | None = None
    ema50_1h: float | None = None
    atr_1h: float | None = None
    if klines and len(klines) >= 2:
        # Use only fully-closed bars; the last bar from Binance is in-progress.
        closed = klines[:-1] if len(klines) > 1 else klines
        try:
            highs = [float(b[2]) for b in closed]
            lows = [float(b[3]) for b in closed]
            closes = [float(b[4]) for b in closed]
            if len(closes) >= 2 and closes[-2] > 0:
                price_change_pct_1h = (closes[-1] - closes[-2]) / closes[-2]
            ema50_1h = compute_ema(closes, period=50)
            atr_1h = compute_atr(highs, lows, closes, period=14)
        except (IndexError, ValueError):
            pass

    long_liq, short_liq = liq_stream.totals(symbol)
    agg_long: float | None = None
    agg_short: float | None = None
    if agg_liq is not None:
        agg_long, agg_short = agg_liq
    return Snapshot(
        symbol=symbol,
        ts=datetime.now(tz=UTC),
        price=price,
        funding_rate=funding,
        open_interest_usd=oi_now,
        open_interest_change_pct_window=oi_change_pct,
        long_short_ratio=lsr,
        long_liquidations_usd_window=long_liq,
        short_liquidations_usd_window=short_liq,
        aggregated_long_liquidations_usd=agg_long,
        aggregated_short_liquidations_usd=agg_short,
        price_change_pct_1h=price_change_pct_1h,
        ema50_1h=ema50_1h,
        atr_1h=atr_1h,
    )


async def _none_async() -> None:
    """Helper for `asyncio.gather` placeholder when Coinglass is not configured."""
    return None
