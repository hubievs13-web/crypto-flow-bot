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
) -> Snapshot:
    """Pull a fresh snapshot for a symbol, combining REST data and the WS-driven liq totals."""
    funding, oi_now, lsr, price, oi_hist = await asyncio.gather(
        client.funding_rate(symbol),
        client.open_interest_usd(symbol),
        client.top_long_short_position_ratio(symbol),
        client.latest_price(symbol),
        client.open_interest_history(symbol, period="5m", limit=max(2, oi_window_minutes // 5 + 1)),
    )

    oi_change_pct: float | None = None
    if oi_hist:
        try:
            oldest = float(oi_hist[0]["sumOpenInterestValue"])
            if oldest > 0:
                oi_change_pct = (oi_now - oldest) / oldest
        except (KeyError, ValueError):
            oi_change_pct = None

    long_liq, short_liq = liq_stream.totals(symbol)
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
    )
