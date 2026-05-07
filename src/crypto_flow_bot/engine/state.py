"""Persistent state: open virtual positions + last alert times.

State is stored as a single JSON file. This is enough for our scale (a few
symbols, a few open positions at a time) and is easy to inspect/edit by hand.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Position, Snapshot, TpLevelState
from crypto_flow_bot.engine.signals import SignalCandidate

log = logging.getLogger(__name__)


def _state_dir() -> Path:
    return Path(os.environ.get("CRYPTO_FLOW_BOT_STATE_DIR", "state"))


class StateStore:
    """Holds positions + cooldown timestamps. Persists to <state_dir>/state.json."""

    def __init__(self, path: Path | None = None) -> None:
        d = path or _state_dir()
        d.mkdir(parents=True, exist_ok=True)
        self.path = d / "state.json"
        self.positions: dict[str, Position] = {}
        self.last_alert_ts: dict[tuple[str, Direction], datetime] = {}
        # ISO-week key (e.g. "2025-W18") of the last weekly stats digest sent.
        self.last_stats_digest_week: str | None = None
        # ISO date (YYYY-MM-DD UTC) of the last daily-liveness ping sent.
        self.last_liveness_ping_date: str | None = None
        self._load()

    # ---------- persistence ----------

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            raw = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            log.warning("state file %s is corrupt; starting fresh", self.path)
            return
        for p in raw.get("positions", []):
            try:
                pos = Position(
                    id=p["id"],
                    symbol=p["symbol"],
                    direction=Direction(p["direction"]),
                    entry_price=float(p["entry_price"]),
                    entry_ts=datetime.fromisoformat(p["entry_ts"]),
                    reason=p["reason"],
                    reason_metric_at_entry=p.get("reason_metric_at_entry", {}),
                    stop_loss_price=float(p["stop_loss_price"]),
                    initial_stop_loss_price=float(p["initial_stop_loss_price"]),
                    tp_levels=[TpLevelState(**lvl) for lvl in p.get("tp_levels", [])],
                    open_fraction=float(p.get("open_fraction", 1.0)),
                    closed=bool(p.get("closed", False)),
                    close_ts=datetime.fromisoformat(p["close_ts"]) if p.get("close_ts") else None,
                    close_reason=p.get("close_reason"),
                    close_price=p.get("close_price"),
                    best_favorable_pct=float(p.get("best_favorable_pct", 0.0)),
                    strong=bool(p.get("strong", False)),
                )
                if not pos.closed:
                    self.positions[pos.id] = pos
            except (KeyError, ValueError) as e:
                log.warning("skipping malformed position in state: %s", e)
        for entry in raw.get("last_alert_ts", []):
            try:
                self.last_alert_ts[(entry["symbol"], Direction(entry["direction"]))] = (
                    datetime.fromisoformat(entry["ts"])
                )
            except (KeyError, ValueError):
                continue
        self.last_stats_digest_week = raw.get("last_stats_digest_week")
        self.last_liveness_ping_date = raw.get("last_liveness_ping_date")

    def save(self) -> None:
        body = {
            "positions": [p.to_log_dict() for p in self.positions.values()],
            "last_alert_ts": [
                {"symbol": s, "direction": d.value, "ts": ts.isoformat()}
                for (s, d), ts in self.last_alert_ts.items()
            ],
            "last_stats_digest_week": self.last_stats_digest_week,
            "last_liveness_ping_date": self.last_liveness_ping_date,
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(body, indent=2))
        tmp.replace(self.path)

    # ---------- cooldown ----------

    def cooldown_remaining_seconds(self, symbol: str, direction: Direction, cooldown_seconds: int) -> float:
        last = self.last_alert_ts.get((symbol, direction))
        if last is None:
            return 0.0
        elapsed = (datetime.now(tz=UTC) - last).total_seconds()
        return max(0.0, cooldown_seconds - elapsed)

    def mark_alerted(self, symbol: str, direction: Direction) -> None:
        self.last_alert_ts[(symbol, direction)] = datetime.now(tz=UTC)

    # ---------- positions ----------

    def open_for(self, symbol: str, direction: Direction) -> Position | None:
        for p in self.positions.values():
            if p.symbol == symbol and p.direction == direction and not p.closed:
                return p
        return None

    def open_positions(self, symbol: str | None = None) -> list[Position]:
        out = [p for p in self.positions.values() if not p.closed]
        if symbol:
            out = [p for p in out if p.symbol == symbol]
        return out

    def open_from_signal(self, candidate: SignalCandidate, cfg: Config) -> Position:
        snap: Snapshot = candidate.snapshot
        sign = candidate.direction.sign

        # ATR-based dynamic SL/TP when ATR is available; fall back to fixed % otherwise.
        atr_cfg = cfg.exits.atr_sizing
        use_atr = atr_cfg.enabled and snap.atr_1h is not None and snap.atr_1h > 0 and snap.price > 0
        if use_atr:
            atr = snap.atr_1h
            assert atr is not None
            sl_dist_pct = (atr_cfg.sl_atr_mult * atr) / snap.price
            sl_price = snap.price * (1 - sign * sl_dist_pct)
            existing_fractions = [lvl.fraction for lvl in cfg.exits.take_profit_levels]
            mults = list(atr_cfg.tp_atr_mults)
            # Pad/truncate the multiplier list to match the number of TP levels.
            if len(mults) < len(existing_fractions):
                mults = mults + [mults[-1]] * (len(existing_fractions) - len(mults))
            # Confluence bonus: widen the *last* TP for strong (2+ rules) signals.
            if candidate.is_strong and mults:
                mults[-1] = max(mults[-1], atr_cfg.strong_last_tp_mult)
            tp_levels = [
                TpLevelState(pct=(m * atr) / snap.price, fraction=frac)
                for m, frac in zip(mults, existing_fractions, strict=False)
            ]
        else:
            sl_price = snap.price * (1 - sign * cfg.exits.stop_loss_pct)
            tp_levels = [TpLevelState(pct=lvl.pct, fraction=lvl.fraction) for lvl in cfg.exits.take_profit_levels]
        metric_snap: dict = {}
        if snap.funding_rate is not None:
            metric_snap["funding_rate"] = snap.funding_rate
        if snap.long_short_ratio is not None:
            metric_snap["long_short_ratio"] = snap.long_short_ratio
        if snap.open_interest_change_pct_window is not None:
            metric_snap["oi_change_pct"] = snap.open_interest_change_pct_window
        position = Position(
            id=str(uuid.uuid4())[:8],
            symbol=candidate.symbol,
            direction=candidate.direction,
            entry_price=snap.price,
            entry_ts=datetime.now(tz=UTC),
            reason=candidate.reason_label,
            reason_metric_at_entry=metric_snap,
            stop_loss_price=sl_price,
            initial_stop_loss_price=sl_price,
            tp_levels=tp_levels,
            strong=candidate.is_strong,
        )
        self.positions[position.id] = position
        return position

    def close_position(
        self,
        position: Position,
        price: float,
        reason: str,
        fraction: float | None = None,
    ) -> None:
        f = fraction if fraction is not None else position.open_fraction
        position.open_fraction = max(0.0, position.open_fraction - f)
        if position.open_fraction <= 1e-9:
            position.open_fraction = 0.0
            position.closed = True
            position.close_ts = datetime.now(tz=UTC)
            position.close_reason = reason
            position.close_price = price
