"""Core data shapes shared across modules."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def opposite(self) -> Direction:
        return Direction.SHORT if self is Direction.LONG else Direction.LONG

    @property
    def sign(self) -> int:
        """+1 for LONG, -1 for SHORT."""
        return 1 if self is Direction.LONG else -1


@dataclass
class Snapshot:
    """One poll result for one symbol — all metrics together."""

    symbol: str
    ts: datetime
    price: float

    funding_rate: float | None = None  # 8h rate, e.g. +0.0008 = 0.08%

    open_interest_usd: float | None = None
    open_interest_change_pct_window: float | None = None  # delta over the configured window

    long_short_ratio: float | None = None  # top traders position ratio

    # Sum of one-sided liquidations notional (USD) within the configured window,
    # aggregated across every exchange that the LiquidationStream is subscribed
    # to (Binance + Bybit + ...). See `crypto_flow_bot.data.liquidations`.
    long_liquidations_usd_window: float = 0.0
    short_liquidations_usd_window: float = 0.0

    # 1h kline derivatives — used for OI alignment, trend filtering, and ATR sizing.
    price_change_pct_1h: float | None = None  # last fully-closed 1h bar vs the one before
    ema50_1h: float | None = None             # EMA(50) on 1h closes
    atr_1h: float | None = None               # ATR(14) on 1h bars, in absolute price units

    def to_log_dict(self) -> dict:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        return d


@dataclass
class TpLevelState:
    pct: float
    fraction: float
    hit: bool = False


@dataclass
class Position:
    """Virtual position the bot tracks for SL/TP/trailing/time bookkeeping."""

    id: str
    symbol: str
    direction: Direction
    entry_price: float
    entry_ts: datetime
    reason: str  # which signal opened it
    reason_metric_at_entry: dict = field(default_factory=dict)

    # Risk levels — absolute prices, computed at entry.
    stop_loss_price: float = 0.0  # current effective SL (may move with trailing)
    initial_stop_loss_price: float = 0.0
    tp_levels: list[TpLevelState] = field(default_factory=list)

    # Lifecycle.
    open_fraction: float = 1.0  # how much of the position is still virtually open
    closed: bool = False
    close_ts: datetime | None = None
    close_reason: str | None = None
    close_price: float | None = None

    # Trailing tracking.
    best_favorable_pct: float = 0.0  # max favorable excursion since entry

    def to_log_dict(self) -> dict:
        d = {
            "id": self.id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "entry_price": self.entry_price,
            "entry_ts": self.entry_ts.isoformat(),
            "reason": self.reason,
            "reason_metric_at_entry": self.reason_metric_at_entry,
            "stop_loss_price": self.stop_loss_price,
            "initial_stop_loss_price": self.initial_stop_loss_price,
            "tp_levels": [{"pct": t.pct, "fraction": t.fraction, "hit": t.hit} for t in self.tp_levels],
            "open_fraction": self.open_fraction,
            "closed": self.closed,
            "close_ts": self.close_ts.isoformat() if self.close_ts else None,
            "close_reason": self.close_reason,
            "close_price": self.close_price,
            "best_favorable_pct": self.best_favorable_pct,
        }
        return d


@dataclass
class Alert:
    """A user-facing alert produced by the engine. Sent to Telegram + logged."""

    kind: str  # "ENTRY", "TP_HIT", "SL_HIT", "TRAILING_MOVE", "TIME_STOP", "REASON_INVALIDATED", "HEARTBEAT"
    symbol: str
    ts: datetime
    text: str  # ready-to-send message
    direction: Direction | None = None
    position_id: str | None = None
    payload: dict = field(default_factory=dict)

    def to_log_dict(self) -> dict:
        return {
            "kind": self.kind,
            "symbol": self.symbol,
            "ts": self.ts.isoformat(),
            "direction": self.direction.value if self.direction else None,
            "position_id": self.position_id,
            "payload": self.payload,
            "text": self.text,
        }
