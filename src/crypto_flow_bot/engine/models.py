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
    ema50_slope_1h: float | None = None       # (ema_now - ema_prev_window) / ema_prev_window
    atr_1h: float | None = None               # ATR(14) on 1h bars, in absolute price units

    # Taker buy/sell *quote* volume on the last fully-closed 1h bar, in USDT.
    # Sourced from the Binance kline fields takerBuyQuoteVolume and the bar's
    # total quote volume — the difference is taker sell. Used by PR-3 to
    # confirm aggressor side (LONG needs taker buy dominance, SHORT vice-versa).
    taker_buy_quote_1h: float | None = None
    taker_sell_quote_1h: float | None = None
    taker_buy_dominance_1h: float | None = None  # buy / (buy + sell) on last closed 1h bar, in [0, 1]
    cvd_window_usd: float | None = None          # rolling sum of (taker_buy - taker_sell) over last N closed 1h bars
    oi_quality: str | None = None                # healthy_long/healthy_short/dangerous_long/dangerous_short

    # 4h kline derivatives — same shape as the 1h block, used by PR-4 for
    # higher-timeframe trend confirmation and EMA-slope checks. None when the
    # 4h fetch failed or returned fewer than 51 bars.
    price_change_pct_4h: float | None = None  # last fully-closed 4h bar vs the one before
    ema50_4h: float | None = None             # EMA(50) on 4h closes
    ema50_slope_4h: float | None = None       # (ema_now - ema_prev_window) / ema_prev_window
    atr_4h: float | None = None               # ATR(14) on 4h bars, absolute price units

    # Per-metric freshness timestamps. None means "upstream value never
    # populated this snapshot". Used by `signals.evaluate` to drop rules
    # whose underlying metric is older than the configured threshold —
    # protects against stale REST responses leaking into entry decisions.
    funding_rate_ts: datetime | None = None
    open_interest_ts: datetime | None = None
    long_short_ratio_ts: datetime | None = None

    # Funding-rate statistics evaluated against the per-symbol rolling
    # history (FundingHistoryCache). Populated by the bot's poll loop before
    # passing the snapshot into `signals.evaluate`. None means either the
    # history was too thin (< min_history_points) or auto mode was disabled.
    # Surfaced primarily so snapshots.jsonl carries the same numbers the
    # signal rule used at decision time -- handy for post-hoc analysis.
    funding_rate_zscore: float | None = None
    funding_rate_percentile: float | None = None

    def to_log_dict(self) -> dict:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        # Per-metric freshness ts fields are datetimes; flatten them to
        # ISO strings (or None) so the row is directly JSON-serializable.
        for key in ("funding_rate_ts", "open_interest_ts", "long_short_ratio_ts"):
            val = d.get(key)
            if isinstance(val, datetime):
                d[key] = val.isoformat()
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

    # Confluence: was this opened with 2+ *non-funding* distinct rules in
    # the same direction within the confluence window? Used by the alert
    # formatter (STRONG marker) and stats (per-strength rates).
    strong: bool = False

    # Cross-snapshot signal identifier. Stable for the lifetime of one
    # candidate (alert / block / open / close all share the same id), so
    # rows in alerts.jsonl / positions.jsonl / blocked.jsonl can be joined.
    signal_id: str | None = None

    # ATR(1h) at entry time, used by ATR-based trailing-stop activation. None
    # when ATR was unavailable on the entry snapshot (rare; only when 1h
    # klines failed to load).
    entry_atr_1h: float | None = None

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
            "strong": self.strong,
            "signal_id": self.signal_id,
            "entry_atr_1h": self.entry_atr_1h,
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
