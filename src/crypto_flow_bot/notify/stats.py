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

log = logging.getLogger(__name__)


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
    total_pnl_pct: float = 0.0  # sum of close_price-vs-entry % per closed position

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
) -> dict[str, SignalStats]:
    """Group positions by entry signal type, count outcomes within the window."""
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

            entry = p.get("entry_price")
            close = p.get("close_price")
            direction = p.get("direction")
            if entry and close and direction:
                try:
                    sign = 1 if direction == "LONG" else -1
                    s.total_pnl_pct += (float(close) - float(entry)) / float(entry) * sign
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
        out[name] = s
    return out


def format_stats_digest(
    stats: dict[str, SignalStats],
    window_days: int,
    *,
    total_positions: int | None = None,
) -> str:
    """Render a Telegram-friendly HTML digest. Empty-stats path is handled."""
    header = f"📊 <b>Stats — last {window_days} days</b>"
    if not stats:
        body = "<i>No signals fired during this period.</i>"
        return f"{header}\n\n{body}"

    total_unique = total_positions if total_positions is not None else "?"
    lines: list[str] = [header, f"<b>Total positions:</b> {total_unique}", ""]
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

    lines.append(
        "<i>PnL is entry→close on the whole position; doesn't account for "
        "partial TP fills or fees. Win-rate = positions that hit TP1.</i>"
    )
    return "\n".join(lines).rstrip()
