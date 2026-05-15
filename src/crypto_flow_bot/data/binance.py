"""Binance USD-M futures public data — funding, OI, LSR, price.

All endpoints used here are public (no API key required). The liquidation
stream lives in `crypto_flow_bot.data.liquidations` and aggregates across
multiple exchanges.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TypedDict

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from crypto_flow_bot.data.liquidations import LiquidationStream
from crypto_flow_bot.engine.models import Snapshot
from crypto_flow_bot.engine.regime import classify_regime, compute_adx

log = logging.getLogger(__name__)

REST_BASE = "https://fapi.binance.com"


class PremiumIndexData(TypedDict):
    lastFundingRate: float
    nextFundingTime: datetime
    interestRate: float
    markPrice: float
    indexPrice: float


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

    async def funding_rate_history(
        self,
        symbol: str,
        limit: int = 1000,
    ) -> list[tuple[datetime, float]]:
        """Historical funding rates (most recent last), up to 1000 points.

        Each point covers an 8h funding cycle, so limit=1000 ~ 333 days of
        history. Returns a chronologically-ordered list of (funding_time, rate)
        tuples; consumers (FundingHistoryCache) just push these straight into
        their rolling window.

        Endpoint: GET /fapi/v1/fundingRate
        Response: list of dicts with {symbol, fundingTime (ms), fundingRate (str)}.
        """
        # API caps `limit` at 1000; pass-through is fine.
        data = await self._get(
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": limit},
        )
        assert isinstance(data, list)
        out: list[tuple[datetime, float]] = []
        for row in data:
            try:
                ts = datetime.fromtimestamp(int(row["fundingTime"]) / 1000.0, tz=UTC)
                rate = float(row["fundingRate"])
            except (KeyError, ValueError, TypeError):
                continue
            out.append((ts, rate))
        # Binance returns oldest-first, but sort defensively in case that
        # ever changes silently.
        out.sort(key=lambda t: t[0])
        return out


    async def premium_index(self, symbol: str) -> PremiumIndexData | None:
        data = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        assert isinstance(data, dict)
        try:
            next_funding = datetime.fromtimestamp(int(data["nextFundingTime"]) / 1000.0, tz=UTC)
            return {
                "lastFundingRate": float(data["lastFundingRate"]),
                "nextFundingTime": next_funding,
                "interestRate": float(data["interestRate"]),
                "markPrice": float(data["markPrice"]),
                "indexPrice": float(data["indexPrice"]),
            }
        except (KeyError, TypeError, ValueError):
            return None

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

    async def klines(self, symbol: str, interval: str, limit: int = 51) -> list[list]:
        """Return the last `limit` OHLCV bars for the symbol at the given interval.

        Each entry is the raw Binance kline array:
            [openTime, open, high, low, close, volume, closeTime,
             quoteVolume, trades, takerBuyBaseVolume, takerBuyQuoteVolume, ignore]
        Interval examples: '1h', '4h'. With limit=51 we get 50 fully closed bars
        + 1 in-progress bar.
        """
        data = await self._get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        assert isinstance(data, list)
        return data

    async def klines_1h(self, symbol: str, limit: int = 51) -> list[list]:
        """Backward-compatible wrapper around `klines` for 1h bars.

        Older callers expect 1h klines specifically; new code should call
        `klines(symbol, '1h', limit)` directly to make the timeframe explicit.
        """
        return await self.klines(symbol, "1h", limit)


def compute_ema(values: list[float], period: int) -> float | None:
    """Standard EMA seeded with the SMA of the first `period` values."""
    if len(values) < period or period <= 0:
        return None
    alpha = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = alpha * v + (1.0 - alpha) * ema
    return ema


def _ema_series(values: list[float], period: int) -> list[float]:
    if len(values) < period or period <= 0:
        return []
    alpha = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    out = [ema]
    for v in values[period:]:
        ema = alpha * v + (1.0 - alpha) * ema
        out.append(ema)
    return out


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


def _kline_derivatives(
    klines: list[list],
    *,
    slope_window_bars: int = 6,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Compute (price_change_pct, ema50, atr14) from a list of OHLCV bars.

    Only fully-closed bars are used (the last bar from Binance is always
    in-progress and is dropped). Any field that can't be computed because
    of malformed data returns None independently of the others.
    """
    price_change_pct: float | None = None
    ema50: float | None = None
    atr14: float | None = None
    ema_slope: float | None = None
    if klines and len(klines) >= 2:
        closed = klines[:-1] if len(klines) > 1 else klines
        try:
            highs = [float(b[2]) for b in closed]
            lows = [float(b[3]) for b in closed]
            closes = [float(b[4]) for b in closed]
            if len(closes) >= 2 and closes[-2] > 0:
                price_change_pct = (closes[-1] - closes[-2]) / closes[-2]
            ema50 = compute_ema(closes, period=50)
            series = _ema_series(closes, period=50)
            lookback_idx = slope_window_bars
            if len(series) >= lookback_idx + 1 and series[-1 - lookback_idx] != 0:
                ema_slope = (series[-1] - series[-1 - lookback_idx]) / series[-1 - lookback_idx]
            atr14 = compute_atr(highs, lows, closes, period=14)
        except (IndexError, ValueError):
            pass
    return price_change_pct, ema50, atr14, ema_slope


def _taker_quote_volumes(klines: list[list]) -> tuple[float | None, float | None]:
    """Extract taker buy / taker sell quote volumes from the last fully-closed 1h kline.

    Binance kline array indices used:
        [7]  quoteAssetVolume      -- total quote volume on the bar
        [10] takerBuyQuoteAssetVolume -- quote volume of *taker buy* fills

    Taker-sell is the residual: total - taker-buy. Returns (None, None) when
    the kline payload is malformed or we have fewer than 2 bars (no closed bar).
    """
    if not klines or len(klines) < 2:
        return None, None
    try:
        bar = klines[-2]  # last fully-closed bar (index -1 is in-progress)
        total = float(bar[7])
        buy = float(bar[10])
    except (IndexError, ValueError, TypeError):
        return None, None
    sell = total - buy
    # Floating-point can drive `sell` slightly negative when buy == total.
    if sell < 0:
        sell = 0.0
    return buy, sell


def _taker_buy_dominance(buy: float | None, sell: float | None) -> float | None:
    if buy is None or sell is None:
        return None
    total = buy + sell
    if total <= 0:
        return None
    return buy / total


def _cvd_window_usd(klines: list[list], window_bars: int) -> float | None:
    if window_bars <= 0 or len(klines) < 2:
        return None
    closed = klines[:-1]
    if not closed:
        return None
    tail = closed[-window_bars:]
    total = 0.0
    for bar in tail:
        try:
            quote = float(bar[7])
            taker_buy_quote = float(bar[10])
        except (IndexError, ValueError, TypeError):
            return None
        total += 2.0 * taker_buy_quote - quote
    return total


def _compute_predicted_funding(mark_price: float, index_price: float, interest_rate: float, funding_cap: float) -> float | None:
    if index_price == 0:
        return None
    premium_index = (mark_price - index_price) / index_price
    predicted = premium_index + interest_rate
    return max(-funding_cap, min(funding_cap, predicted))


def _classify_oi_quality(
    price_change_pct_1h: float | None,
    open_interest_change_pct_window: float | None,
    epsilon: float,
) -> str | None:
    if price_change_pct_1h is None or open_interest_change_pct_window is None:
        return None
    if abs(price_change_pct_1h) < epsilon or abs(open_interest_change_pct_window) < epsilon:
        return None
    if price_change_pct_1h > 0 and open_interest_change_pct_window > 0:
        return "healthy_short"
    if price_change_pct_1h < 0 and open_interest_change_pct_window > 0:
        return "healthy_long"
    if price_change_pct_1h > 0 and open_interest_change_pct_window < 0:
        return "dangerous_long"
    if price_change_pct_1h < 0 and open_interest_change_pct_window < 0:
        return "dangerous_short"
    return None


async def build_snapshot(
    client: BinanceClient,
    liq_stream: LiquidationStream,
    symbol: str,
    oi_window_minutes: int,
    slope_window_bars: int = 6,
    cvd_window_bars: int = 6,
    oi_quality_epsilon_pct: float = 0.0005,
    *,
    enable_4h_klines: bool = True,
    predicted_funding_cap: float = 0.0075,
    regime_enabled: bool = True,
    regime_adx_period: int = 14,
    regime_cfg=None,
) -> Snapshot:
    """Pull a fresh snapshot for a symbol.

    Combines REST data and the multi-exchange WS-driven liquidation totals.
    `liq_stream.totals(symbol)` already aggregates across every enabled
    exchange (see `crypto_flow_bot.data.liquidations.LiquidationStream`).

    When `enable_4h_klines` is True (default), the snapshot also includes
    4h EMA50 / ATR(14) / price-change derivatives, used by the higher-TF
    trend filter in PR-4. Set False to skip the extra REST call when the
    filter is disabled in config.
    """
    fetch_ts = datetime.now(tz=UTC)
    funding = await client.funding_rate(symbol)
    oi_now = await client.open_interest_usd(symbol)
    lsr = await client.top_long_short_position_ratio(symbol)
    price = await client.latest_price(symbol)
    oi_hist = await client.open_interest_history(
        symbol, period="5m", limit=max(2, oi_window_minutes // 5 + 1)
    )
    klines_1h = await client.klines(symbol, "1h", limit=51)
    premium_idx = await client.premium_index(symbol)
    # 4h is fetched separately so the typed unpacking above stays stable
    # regardless of whether the higher-TF block is enabled.
    klines_4h: list[list] | None = None
    if enable_4h_klines:
        klines_4h = await client.klines(symbol, "4h", limit=51)

    oi_change_pct: float | None = None
    if oi_hist:
        try:
            oldest = float(oi_hist[0]["sumOpenInterestValue"])
            if oldest > 0:
                oi_change_pct = (oi_now - oldest) / oldest
        except (KeyError, ValueError):
            oi_change_pct = None

    price_change_pct_1h, ema50_1h, atr_1h, ema50_slope_1h = _kline_derivatives(
        klines_1h, slope_window_bars=slope_window_bars
    )
    taker_buy_1h, taker_sell_1h = _taker_quote_volumes(klines_1h)
    taker_buy_dominance_1h = _taker_buy_dominance(taker_buy_1h, taker_sell_1h)
    cvd_window_usd = _cvd_window_usd(klines_1h, cvd_window_bars)
    oi_quality = _classify_oi_quality(price_change_pct_1h, oi_change_pct, oi_quality_epsilon_pct)

    price_change_pct_4h: float | None = None
    ema50_4h: float | None = None
    atr_4h: float | None = None
    ema50_slope_4h: float | None = None
    if klines_4h is not None:
        price_change_pct_4h, ema50_4h, atr_4h, ema50_slope_4h = _kline_derivatives(
            klines_4h, slope_window_bars=slope_window_bars
        )

    long_liq, short_liq = liq_stream.totals(symbol)

    predicted_funding_rate = None
    if premium_idx is not None:
        predicted_funding_rate = _compute_predicted_funding(
            float(premium_idx["markPrice"]),
            float(premium_idx["indexPrice"]),
            float(premium_idx["interestRate"]),
            predicted_funding_cap,
        )
    else:
        log.warning("premium_index unavailable for %s", symbol)
    adx_1h = None
    atr_pct_1h = None
    regime = None
    if regime_enabled and klines_1h and len(klines_1h) >= 30 and atr_1h is not None and price > 0:
        atr_pct_1h = atr_1h / price
        adx_1h = compute_adx(klines_1h, period=regime_adx_period)
        if regime_cfg is not None:
            regime = classify_regime(adx_1h, atr_pct_1h, regime_cfg)

    return Snapshot(
        symbol=symbol,
        ts=fetch_ts,
        price=price,
        funding_rate=funding,
        open_interest_usd=oi_now,
        open_interest_change_pct_window=oi_change_pct,
        long_short_ratio=lsr,
        long_liquidations_usd_window=long_liq,
        short_liquidations_usd_window=short_liq,
        price_change_pct_1h=price_change_pct_1h,
        ema50_1h=ema50_1h,
        ema50_slope_1h=ema50_slope_1h,
        atr_1h=atr_1h,
        regime=regime,
        adx_1h=adx_1h,
        atr_pct_1h=atr_pct_1h,
        predicted_funding_rate=predicted_funding_rate,
        taker_buy_quote_1h=taker_buy_1h,
        taker_sell_quote_1h=taker_sell_1h,
        taker_buy_dominance_1h=taker_buy_dominance_1h,
        cvd_window_usd=cvd_window_usd,
        oi_quality=oi_quality,
        price_change_pct_4h=price_change_pct_4h,
        ema50_4h=ema50_4h,
        ema50_slope_4h=ema50_slope_4h,
        atr_4h=atr_4h,
        funding_rate_ts=fetch_ts,
        open_interest_ts=fetch_ts,
        long_short_ratio_ts=fetch_ts,
    )
