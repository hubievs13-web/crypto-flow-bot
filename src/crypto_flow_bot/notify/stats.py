"""Weekly stats digest.

Reads `positions.jsonl` (append-only log of position state changes), keeps the
*latest* row per `position_id`, and summarizes outcomes over the last N days
grouped by entry-signal type. The result is formatted as a Telegram-friendly
HTML message.

Why positions.jsonl and not alerts.jsonl: positions.jsonl already carries the
final state (TP hits, SL hit, close price, close reason) per position, which
is exactly what we need for win-rate / PnL math.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crypto_flow_bot.config import FeesCfg

log = logging.getLogger(__name__)


def _round_trip_cost(fees: FeesCfg | None) -> float:
    """Total fee+slippage cost for a fully-closed position, expressed as a
    unit fraction of notional.

    Each closed position has exactly two "full" fills' worth of cost: one on
    entry and one on exit (the exit is split across TP fills + the final
    close, but the *sum* of exit fractions is always 1.0). So the
    round-trip cost is simply 2 × per-fill.
    """
    if fees is None or not fees.enabled:
        return 0.0
    return 2.0 * (fees.commission_per_fill + fees.slippage_per_fill)


@dataclass
class SignalStats:
    """Per-signal-type aggregated outcomes for the digest window."""

    name: str
    count: int = 0
    tp1_hit: int = 0           # at least the first TP level was filled
    tp2_hit: int = 0           # all TP levels filled
    sl_no_tp: int = 0          # SL hit without any TP first (full loss case)
    time_stopped: int = 0      # closed by time-stop
    invalidated: int = 0       # closed by reason-invalidation
    open_unresolved: int = 0   # still open at digest time
    # Weighted PnL across all closed positions: each partial TP fill is
    # credited at the TP level's profit % weighted by its fraction; the
    # remainder is credited at (close_price - entry) / entry. So a 50/50
    # ladder closed at TP1 (+1.5%) and BE (0%) contributes +0.75%, not 0%.
    total_pnl_pct: float = 0.0

    @property
    def closed(self) -> int:
        return self.count - self.open_unresolved

    @property
    def win_rate_pct(self) -> float:
        c = self.closed
        return (self.tp1_hit / c * 100.0) if c else 0.0

    @property
    def avg_pnl_pct(self) -> float:
        c = self.closed
        return (self.total_pnl_pct / c * 100.0) if c else 0.0


def position_pnl_pct(pos: dict, *, fees: FeesCfg | None = None) -> float | None:
    """Weighted realized PnL on a closed position as a unit fraction (0.01 = +1%).

    For each TP level that was hit, we credit `fraction * pct` profit. The
    remaining open fraction at the final close is credited the close-vs-entry
    move. Returns None when fields are missing/malformed.

    Example: 50% at TP1 (+1.5%) + 50% at SL/BE (0%) → 0.5*0.015 + 0.5*0.0 = 0.0075.

    When `fees` is provided and enabled, subtract `2 × (commission + slippage)`
    from the gross PnL: every closed position has one entry fill and one
    exit's worth of fills (TP fractions + final close = 1.0), so the total
    notional turned over is always 2.0.
    """
    try:
        entry_raw = pos.get("entry_price")
        close = pos.get("close_price")
        direction = pos.get("direction")
        if entry_raw is None or close is None or direction not in ("LONG", "SHORT"):
            return None
        entry = float(entry_raw)
        if not entry:
            return None
        sign = 1.0 if direction == "LONG" else -1.0
        tp_levels = pos.get("tp_levels") or []
        pnl = 0.0
        hit_fraction = 0.0
        for lvl in tp_levels:
            if lvl.get("hit"):
                frac = float(lvl.get("fraction", 0.0) or 0.0)
                pct = float(lvl.get("pct", 0.0) or 0.0)
                pnl += frac * pct  # TP profit always counts as +pct (fav_pct)
                hit_fraction += frac
        remaining = max(0.0, 1.0 - hit_fraction)
        if remaining > 0:
            move = (float(close) - entry) / entry * sign
            pnl += remaining * move
        return pnl - _round_trip_cost(fees)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def read_latest_positions(positions_file: Path) -> list[dict]:
    """Return latest state dict per position id from a positions.jsonl file."""
    latest: dict[str, dict] = {}
    if not positions_file.is_file():
        return []
    with positions_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = row.get("id")
            if pid:
                latest[pid] = row
    return list(latest.values())


def compute_stats(
    positions: list[dict],
    now: datetime,
    window_days: int,
    *,
    fees: FeesCfg | None = None,
) -> dict[str, SignalStats]:
    """Group positions by entry signal type, count outcomes within the window.

    If `fees` is provided and enabled, the per-position PnL aggregated into
    `total_pnl_pct` is **net** of commission and slippage; otherwise it stays
    gross (the historical behavior).
    """
    cutoff = now - timedelta(days=window_days)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for pos in positions:
        try:
            entry_ts = datetime.fromisoformat(pos["entry_ts"])
        except (KeyError, ValueError, TypeError):
            continue
        if entry_ts < cutoff:
            continue
        # Each position can fire on multiple rules (joined by '+'). We count
        # the position once per fired rule so each signal type's row reflects
        # how that signal performs across all its triggers.
        reasons = [r.strip() for r in (pos.get("reason") or "").split("+") if r.strip()]
        if not reasons:
            reasons = ["unknown"]
        for r in reasons:
            grouped[r].append(pos)

    out: dict[str, SignalStats] = {}
    for name, positions_for_signal in grouped.items():
        s = SignalStats(name=name, count=len(positions_for_signal))
        for p in positions_for_signal:
            tp_levels = p.get("tp_levels") or []
            tp1_hit = bool(tp_levels and tp_levels[0].get("hit"))
            tp2_hit = len(tp_levels) >= 2 and all(t.get("hit") for t in tp_levels)
            close_reason = p.get("close_reason") or ""
            closed = bool(p.get("closed"))

            if not closed:
                s.open_unresolved += 1
                continue

            if tp1_hit:
                s.tp1_hit += 1
            if tp2_hit:
                s.tp2_hit += 1
            if close_reason == "SL_HIT" and not tp1_hit:
                s.sl_no_tp += 1
            elif close_reason == "TIME_STOP":
                s.time_stopped += 1
            elif close_reason == "REASON_INVALIDATED":
                s.invalidated += 1

            pnl = position_pnl_pct(p, fees=fees)
            if pnl is not None:
                s.total_pnl_pct += pnl
        out[name] = s
    return out


@dataclass
class SymbolStats:
    """Per-symbol aggregated outcomes for the digest window."""

    symbol: str
    count: int = 0
    closed: int = 0
    tp1_hit: int = 0
    total_pnl_pct: float = 0.0

    @property
    def win_rate_pct(self) -> float:
        return (self.tp1_hit / self.closed * 100.0) if self.closed else 0.0

    @property
    def avg_pnl_pct(self) -> float:
        return (self.total_pnl_pct / self.closed * 100.0) if self.closed else 0.0


def compute_symbol_stats(
    positions: list[dict],
    now: datetime,
    window_days: int,
    *,
    fees: FeesCfg | None = None,
) -> dict[str, SymbolStats]:
    """Group positions by symbol; same window/PnL logic as compute_stats.

    `fees` is forwarded to `position_pnl_pct` so per-symbol PnL is also net
    when enabled.
    """
    cutoff = now - timedelta(days=window_days)
    out: dict[str, SymbolStats] = {}
    for pos in positions:
        try:
            entry_ts = datetime.fromisoformat(pos["entry_ts"])
        except (KeyError, ValueError, TypeError):
            continue
        if entry_ts < cutoff:
            continue
        symbol = pos.get("symbol")
        if not symbol:
            continue
        s = out.setdefault(symbol, SymbolStats(symbol=symbol))
        s.count += 1
        if not bool(pos.get("closed")):
            continue
        s.closed += 1
        tp_levels = pos.get("tp_levels") or []
        if tp_levels and tp_levels[0].get("hit"):
            s.tp1_hit += 1
        pnl = position_pnl_pct(pos, fees=fees)
        if pnl is not None:
            s.total_pnl_pct += pnl
    return out


def format_stats_digest(
    stats: dict[str, SignalStats],
    window_days: int,
    *,
    total_positions: int | None = None,
    per_symbol: dict[str, SymbolStats] | None = None,
    fees: FeesCfg | None = None,
) -> str:
    """Render a Telegram-friendly HTML digest. Empty-stats path is handled."""
    header = f"📊 <b>Stats — last {window_days} days</b>"
    if not stats:
        body = "<i>No signals fired during this period.</i>"
        return f"{header}\n\n{body}"

    total_unique = total_positions if total_positions is not None else "?"
    lines: list[str] = [header, f"<b>Total positions:</b> {total_unique}", ""]
    lines.append("<b>By signal type</b>")
    for name, s in sorted(stats.items(), key=lambda kv: -kv[1].count):
        lines.append(f"<b>{name}</b> — {s.count} fires")
        if s.closed > 0:
            lines.append(
                f"  closed {s.closed} · TP1 {s.tp1_hit} ({s.win_rate_pct:.0f}%) · "
                f"TP2 {s.tp2_hit} · SL-no-TP {s.sl_no_tp}"
            )
            if s.time_stopped or s.invalidated:
                lines.append(
                    f"  time-stop {s.time_stopped} · invalidated {s.invalidated}"
                )
            lines.append(f"  avg PnL: {s.avg_pnl_pct:+.2f}%")
        if s.open_unresolved:
            lines.append(f"  still open: {s.open_unresolved}")
        lines.append("")

    if per_symbol:
        lines.append("<b>By symbol</b>")
        for sym, ss in sorted(per_symbol.items(), key=lambda kv: -kv[1].count):
            line = f"<b>{sym}</b> — {ss.count} positions"
            if ss.closed > 0:
                line += (
                    f" · closed {ss.closed} · TP1 {ss.tp1_hit} ({ss.win_rate_pct:.0f}%) · "
                    f"avg PnL {ss.avg_pnl_pct:+.2f}%"
                )
            lines.append(line)
        lines.append("")

    if fees is not None and fees.enabled:
        round_trip_bps = (fees.commission_per_fill + fees.slippage_per_fill) * 2 * 10_000
        fees_note = (
            f"PnL is <b>net</b> of fees: {fees.commission_per_fill * 10_000:.0f}bps "
            f"commission + {fees.slippage_per_fill * 10_000:.0f}bps slippage per fill "
            f"= {round_trip_bps:.0f}bps round-trip subtracted per position."
        )
    else:
        fees_note = "Fees not included."
    lines.append(
        "<i>PnL is weighted across partial TP fills and the final close (e.g. "
        "50% at TP1 + 50% at BE shows as +0.75%, not 0%). Win-rate = positions "
        f"that hit TP1 at least once. {fees_note}</i>"
    )
    return "\n".join(lines).rstrip()


def is_past_weekly_send_time(now: datetime, weekday: int, hour_utc: int) -> bool:
    """True iff `now` is at or after this ISO week's scheduled send moment.

    `weekday` follows Python's convention (0=Mon..6=Sun); `hour_utc` is the
    target hour. The check is "have we crossed the send-moment for the
    current ISO week", not "are we exactly at it" — so a short outage that
    spans the exact minute does not cause the digest to be skipped.
    """
    _iso_year, _iso_week, iso_weekday = now.isocalendar()  # iso_weekday: 1=Mon..7=Sun
    target_iso_weekday = weekday + 1
    if iso_weekday > target_iso_weekday:
        return True
    return iso_weekday == target_iso_weekday and now.hour >= hour_utc
