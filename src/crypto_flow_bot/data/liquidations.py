"""Multi-exchange liquidation aggregator.

Subscribes to each enabled exchange's public liquidation WebSocket and keeps a
unified rolling window. Public WebSocket streams require no API key — this is
the free replacement for paid services like Coinglass.

Currently supported exchanges:
    - Binance USD-M futures (`!forceOrder@arr`)
    - Bybit V5 linear perpetuals (`allLiquidation.{symbol}`)

Adding a new exchange is one new `_ExchangeStream` subclass.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import websockets

log = logging.getLogger(__name__)

LiquidatedSide = Literal["LONG", "SHORT"]


@dataclass
class _LiqEvent:
    """One liquidation event normalized across exchanges."""

    symbol: str            # canonical Binance-style, e.g. "BTCUSDT"
    liquidated_side: LiquidatedSide
    notional_usd: float
    ts: datetime
    exchange: str          # "binance", "bybit", ...


# ─── Per-exchange streams ───────────────────────────────────────────────────


class _ExchangeStream:
    """Base class: holds one WS connection in a long-running task."""

    name: str = "base"
    url: str = ""

    def __init__(
        self,
        symbols: list[str],
        append: Callable[[_LiqEvent], None],
        stopped: asyncio.Event,
    ) -> None:
        self._symbols = symbols
        self._append = append
        self._stopped = stopped

    async def run(self) -> None:
        """Connect with reconnect-with-backoff. Override `_handle_socket`."""
        backoff = 1.0
        while not self._stopped.is_set():
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20
                ) as ws:
                    log.info("%s liquidation stream connected", self.name)
                    backoff = 1.0
                    await self._on_connect(ws)
                    await self._handle_socket(ws)
            except (TimeoutError, websockets.WebSocketException, OSError) as e:
                if self._stopped.is_set():
                    break
                log.warning(
                    "%s liquidation stream disconnected: %s; retrying in %.1fs",
                    self.name, e, backoff,
                )
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    break
                backoff = min(backoff * 2.0, 60.0)

    async def _on_connect(self, ws: websockets.WebSocketClientProtocol) -> None:  # noqa: ARG002
        """Override to send subscribe messages right after connect."""
        return

    async def _handle_socket(self, ws: websockets.WebSocketClientProtocol) -> None:
        async for raw in ws:
            if self._stopped.is_set():
                break
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            for ev in self.parse(msg):
                self._append(ev)

    def parse(self, msg: dict) -> list[_LiqEvent]:
        raise NotImplementedError


class BinanceLiqStream(_ExchangeStream):
    """Binance USD-M futures all-symbol liquidation stream.

    Schema: `o.s` symbol, `o.S` SELL/BUY (SELL = long was force-closed),
    `o.ap` avg price (or `o.p`), `o.q` qty in base coin.
    """

    name = "binance"
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"

    def parse(self, msg: dict) -> list[_LiqEvent]:
        order = msg.get("o") or {}
        symbol = order.get("s")
        side = order.get("S")
        try:
            price = float(order.get("ap") or order.get("p") or 0.0)
            qty = float(order.get("q") or 0.0)
        except (TypeError, ValueError):
            return []
        if not symbol or not side or qty <= 0 or price <= 0:
            return []
        if side == "SELL":
            liq_side: LiquidatedSide = "LONG"
        elif side == "BUY":
            liq_side = "SHORT"
        else:
            return []
        return [
            _LiqEvent(
                symbol=symbol,
                liquidated_side=liq_side,
                notional_usd=price * qty,
                ts=datetime.now(tz=UTC),
                exchange=self.name,
            )
        ]


class BybitLiqStream(_ExchangeStream):
    """Bybit V5 linear perpetuals `allLiquidation.{symbol}` topic.

    Per Bybit docs: a `Buy` event means a long position was liquidated,
    `Sell` means a short was. `v` is executed size (in contracts/base coin),
    `p` is bankruptcy price. Push frequency: 500ms.
    """

    name = "bybit"
    url = "wss://stream.bybit.com/v5/public/linear"

    async def _on_connect(self, ws: websockets.WebSocketClientProtocol) -> None:
        topics = [f"allLiquidation.{s}" for s in self._symbols]
        await ws.send(json.dumps({"op": "subscribe", "args": topics}))

    def parse(self, msg: dict) -> list[_LiqEvent]:
        topic = msg.get("topic") or ""
        if not topic.startswith("allLiquidation."):
            return []
        data = msg.get("data") or []
        if not isinstance(data, list):
            return []
        out: list[_LiqEvent] = []
        for item in data:
            symbol = item.get("s")
            side = item.get("S")
            try:
                price = float(item.get("p") or 0.0)
                qty = float(item.get("v") or 0.0)
            except (TypeError, ValueError):
                continue
            if not symbol or not side or qty <= 0 or price <= 0:
                continue
            if side == "Buy":
                liq_side: LiquidatedSide = "LONG"
            elif side == "Sell":
                liq_side = "SHORT"
            else:
                continue
            out.append(
                _LiqEvent(
                    symbol=symbol,
                    liquidated_side=liq_side,
                    notional_usd=price * qty,
                    ts=datetime.now(tz=UTC),
                    exchange=self.name,
                )
            )
        return out


_AVAILABLE_STREAMS: dict[str, type[_ExchangeStream]] = {
    "binance": BinanceLiqStream,
    "bybit": BybitLiqStream,
}


# ─── Aggregator ─────────────────────────────────────────────────────────────


class LiquidationStream:
    """Aggregates liquidations across multiple exchanges into one rolling window.

    Public surface (`start`, `stop`, `totals`) is unchanged from the prior
    Binance-only version — callers don't need to be aware of multi-exchange.

    Args:
        window_minutes: rolling window length used by `totals()`.
        exchanges: lower-case names, e.g. `["binance", "bybit"]`.
        symbols: canonical (Binance-style) symbols to subscribe to. Used for
            exchanges that need explicit per-symbol subscribe messages (Bybit).
            Binance pushes the entire all-symbols stream and filters on read.
    """

    def __init__(
        self,
        window_minutes: int,
        exchanges: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> None:
        self.window = timedelta(minutes=window_minutes)
        self._events: deque[_LiqEvent] = deque()
        self._stopped = asyncio.Event()
        self._task: asyncio.Task | None = None
        ex_list = exchanges or ["binance"]
        self._streams: list[_ExchangeStream] = []
        for name in ex_list:
            cls = _AVAILABLE_STREAMS.get(name.lower())
            if cls is None:
                log.warning("unknown liquidation exchange %r; skipping", name)
                continue
            self._streams.append(
                cls(symbols=symbols or [], append=self._append, stopped=self._stopped)
            )

    # ---------- public API (unchanged surface) ----------

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_all(), name="liq-stream-aggregator")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def totals(self, symbol: str) -> tuple[float, float]:
        """Aggregated (long_liq_usd, short_liq_usd) for `symbol` in the window."""
        self._evict_old()
        long_liq = 0.0
        short_liq = 0.0
        for ev in self._events:
            if ev.symbol != symbol:
                continue
            if ev.liquidated_side == "LONG":
                long_liq += ev.notional_usd
            else:
                short_liq += ev.notional_usd
        return long_liq, short_liq

    def totals_per_exchange(self, symbol: str) -> dict[str, tuple[float, float]]:
        """Per-exchange breakdown — useful for diagnostics. Same shape as `totals`."""
        self._evict_old()
        out: dict[str, list[float]] = {}
        for ev in self._events:
            if ev.symbol != symbol:
                continue
            row = out.setdefault(ev.exchange, [0.0, 0.0])
            if ev.liquidated_side == "LONG":
                row[0] += ev.notional_usd
            else:
                row[1] += ev.notional_usd
        return {k: (v[0], v[1]) for k, v in out.items()}

    @property
    def configured_exchanges(self) -> list[str]:
        return [s.name for s in self._streams]

    # ---------- internals ----------

    def _append(self, ev: _LiqEvent) -> None:
        self._events.append(ev)
        # opportunistic eviction so the deque doesn't grow unbounded between
        # reads from the (slow) signal-eval loop
        self._evict_old()

    def _evict_old(self) -> None:
        cutoff = datetime.now(tz=UTC) - self.window
        while self._events and self._events[0].ts < cutoff:
            self._events.popleft()

    async def _run_all(self) -> None:
        if not self._streams:
            log.warning("no liquidation exchanges configured; aggregator is idle")
            await self._stopped.wait()
            return
        # Each stream runs forever with its own backoff. We just gather and
        # propagate cancellation cleanly.
        try:
            await asyncio.gather(*(s.run() for s in self._streams), return_exceptions=False)
        except asyncio.CancelledError:
            raise
