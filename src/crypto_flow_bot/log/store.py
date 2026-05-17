"""Append-only JSONL logger for snapshots, alerts, position events, and
blocked-entry diagnostics.

Each kind goes to its own daily-partitioned file:
  - snapshots-YYYY-MM-DD.jsonl — full poll snapshots (metrics + price + ATR)
  - alerts-YYYY-MM-DD.jsonl    — every alert sent to Telegram (ENTRY / SL_HIT / ...)
  - positions-YYYY-MM-DD.jsonl — position lifecycle updates (one row per state change)
  - blocked-YYYY-MM-DD.jsonl   — entry candidates that were blocked before opening,
                        with `signal_id`, `blocked_reason`, and rule/window
                        context so they can be analyzed against snapshot data

These are readable line-by-line, easy to grep, and convertible to Parquet
(via `pandas.read_json(..., lines=True).to_parquet(...)`) for backtests later.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from crypto_flow_bot.engine.models import Alert, Direction, Position, Snapshot

log = logging.getLogger(__name__)


def _log_dir() -> Path:
    return Path(os.environ.get("CRYPTO_FLOW_BOT_LOG_DIR", "logs"))


class JsonlLogger:
    def __init__(self, path: Path | None = None) -> None:
        d = path or _log_dir()
        d.mkdir(parents=True, exist_ok=True)
        self._dir = d
        self._lock = asyncio.Lock()

    @property
    def positions_path(self) -> Path:
        return self._daily_path("positions")

    @property
    def alerts_path(self) -> Path:
        return self._daily_path("alerts")

    @property
    def blocked_path(self) -> Path:
        return self._daily_path("blocked")

    def _daily_path(self, prefix: str, ts: datetime | None = None) -> Path:
        event_ts = ts or datetime.now(tz=UTC)
        return self._dir / f"{prefix}-{event_ts.date().isoformat()}.jsonl"

    async def _append(self, file: Path, payload: dict) -> None:
        line = json.dumps(payload, ensure_ascii=False)
        async with self._lock:
            await asyncio.to_thread(self._sync_append, file, line)

    @staticmethod
    def _sync_append(file: Path, line: str) -> None:
        with file.open("a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")

    async def write_snapshot(self, snap: Snapshot) -> None:
        row = snap.to_log_dict()
        await self._append(self._daily_path("snapshots", snap.ts), row)

    async def write_alert(self, alert: Alert) -> None:
        row = alert.to_log_dict()
        await self._append(self._daily_path("alerts", alert.ts), row)

    async def write_position(self, pos: Position) -> None:
        row = pos.to_log_dict()
        event_ts = pos.close_ts if pos.closed else None
        await self._append(self._daily_path("positions", event_ts), row)

    async def write_blocked(
        self,
        *,
        signal_id: str,
        symbol: str,
        direction: Direction,
        blocked_reason: str,
        fired_rules: list[str],
        confluence_window_rules: list[str],
        snapshot_ts: datetime | None = None,
    ) -> None:
        """Persist one blocked-entry event for later analysis.

        `blocked_reason` is a stable machine-readable token (e.g.
        `"cooldown"`, `"max_concurrent"`, `"opposite_open"`,
        `"post_exit_cooldown"`, `"max_per_direction_group"`). Both rule sets
        are written as sorted lists so diffs across runs are stable.
        """
        payload = {
            "ts": (snapshot_ts or datetime.now(tz=UTC)).isoformat(),
            "signal_id": signal_id,
            "symbol": symbol,
            "direction": direction.value,
            "blocked_reason": blocked_reason,
            "fired_rules": sorted(fired_rules),
            "confluence_window_rules": sorted(confluence_window_rules),
        }
        await self._append(self._daily_path("blocked", snapshot_ts), payload)
