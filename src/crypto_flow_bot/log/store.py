"""Append-only JSONL logger for snapshots, alerts, and position events.

Each kind goes to its own file:
  - snapshots.jsonl
  - alerts.jsonl
  - positions.jsonl

These are readable line-by-line, easy to grep, and convertible to Parquet
(via `pandas.read_json(..., lines=True).to_parquet(...)`) for backtests later.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from crypto_flow_bot.engine.models import Alert, Position, Snapshot

log = logging.getLogger(__name__)


def _log_dir() -> Path:
    return Path(os.environ.get("CRYPTO_FLOW_BOT_LOG_DIR", "logs"))


class JsonlLogger:
    def __init__(self, path: Path | None = None) -> None:
        d = path or _log_dir()
        d.mkdir(parents=True, exist_ok=True)
        self._dir = d
        self._snap_file = d / "snapshots.jsonl"
        self._alert_file = d / "alerts.jsonl"
        self._pos_file = d / "positions.jsonl"
        self._lock = asyncio.Lock()

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
        await self._append(self._snap_file, snap.to_log_dict())

    async def write_alert(self, alert: Alert) -> None:
        await self._append(self._alert_file, alert.to_log_dict())

    async def write_position(self, pos: Position) -> None:
        await self._append(self._pos_file, pos.to_log_dict())
