"""Coinglass open-api v4 client for cross-exchange aggregated liquidation data.

Used to upgrade `liq_cascade` from Binance-only liquidations to a real cross-
exchange picture: Binance + OKX + Bybit + Bitget + others. A $100M Binance
flush typically corresponds to $300-500M when summed across all exchanges.

Free ("Hobbyist") tier API key works — register at https://www.coinglass.com,
then `Account → API → Create API`. Set the key as a Fly secret:
    fly secrets set COINGLASS_API_KEY="<key>" -a crypto-flow-bot

If the key is missing or the API errors, the client returns None and the bot
falls back gracefully to Binance-only liquidations.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

log = logging.getLogger(__name__)

COINGLASS_BASE = "https://open-api-v4.coinglass.com"
DEFAULT_EXCHANGES = "Binance,OKX,Bybit,Bitget,HTX,Hyperliquid"


def base_coin_from_symbol(symbol: str) -> str:
    """Map Binance perp symbol to Coinglass coin name. 'BTCUSDT' -> 'BTC'."""
    for suffix in ("USDT", "USD", "USDC"):
        if symbol.endswith(suffix):
            return symbol[: -len(suffix)]
    return symbol


class CoinglassClient:
    """Cross-exchange liquidation aggregator. All methods return None on error."""

    def __init__(
        self,
        api_key: str,
        http: httpx.AsyncClient | None = None,
        exchanges: str = DEFAULT_EXCHANGES,
    ) -> None:
        self._key = api_key
        self._http = http or httpx.AsyncClient(base_url=COINGLASS_BASE, timeout=10.0)
        self._owns_http = http is None
        self._exchanges = exchanges

    @classmethod
    def from_env(cls, http: httpx.AsyncClient | None = None) -> CoinglassClient | None:
        """Return a client if `COINGLASS_API_KEY` is set, else None."""
        key = os.environ.get("COINGLASS_API_KEY", "").strip()
        if not key:
            return None
        exchanges = os.environ.get("COINGLASS_EXCHANGES", DEFAULT_EXCHANGES)
        return cls(api_key=key, http=http, exchanges=exchanges)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def aggregated_liquidations(
        self, coin: str, interval: str = "1h"
    ) -> tuple[float, float] | None:
        """Return (long_liq_usd, short_liq_usd) for the most recent closed bar.

        `coin` is the base coin (e.g. 'BTC'). Returns None on any failure so the
        caller can fall back to Binance-only data without breaking.
        """
        try:
            r = await self._http.get(
                "/api/futures/liquidation/aggregated-history",
                params={
                    "exchange_list": self._exchanges,
                    "symbol": coin,
                    "interval": interval,
                    "limit": 2,
                },
                headers={"CG-API-KEY": self._key, "accept": "application/json"},
            )
        except (httpx.HTTPError, TimeoutError) as e:
            log.debug("coinglass aggregated_liquidations %s errored: %s", coin, e)
            return None
        if r.status_code != 200:
            log.debug(
                "coinglass aggregated_liquidations %s -> HTTP %s: %s",
                coin, r.status_code, r.text[:200],
            )
            return None
        try:
            payload: dict[str, Any] = r.json()
        except ValueError:
            return None
        if str(payload.get("code")) != "0":
            log.debug("coinglass api err for %s: %s", coin, payload.get("msg"))
            return None
        data = payload.get("data") or []
        if not data:
            return None
        latest = data[-1]
        try:
            return (
                float(latest["long_liquidation_usd"]),
                float(latest["short_liquidation_usd"]),
            )
        except (KeyError, ValueError, TypeError):
            return None
