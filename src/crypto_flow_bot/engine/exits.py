"""Exit rule evaluation for a single open virtual position.

`evaluate_exit` returns a list of `ExitEvent`s; each event is then turned into
an alert by the caller. SL/time-stop/reason-invalidation close the whole
position, TP levels close a fraction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Position, Snapshot


@dataclass
class ExitEvent:
    kind: str            # "TP_HIT", "SL_HIT", "TRAILING_MOVE", "TIME_STOP", "REASON_INVALIDATED"
    fraction_closed: float  # 0 for trailing-move (no fill, just SL update)
    new_stop_loss_price: float | None = None
    description: str = ""


def _favorable_pct(position: Position, price: float) -> float:
    """Return signed favorable excursion % from entry (positive = profit)."""
    raw = (price - position.entry_price) / position.entry_price
    return raw * position.direction.sign


def _metric_retraced_to_neutral(
    *,
    entry_value: float | None,
    current_value: float,
    neutral: float,
    retrace_pct: float,
) -> bool:
    """True if `current_value` has moved at least `retrace_pct` of the way
    from `entry_value` back toward `neutral`.

    Used by reason-invalidation for funding (neutral=0.0) and LSR
    (neutral=1.0). A position entered at funding=+0.010% with retrace_pct=0.5
    invalidates once funding has dropped to +0.005% or below — including
    sign flips, which count as over-retraced.

    Returns False when no entry value was recorded (older positions loaded
    from state), when retrace_pct is non-positive, or when the entry value
    is already at neutral (nothing to retrace from).
    """
    if entry_value is None or retrace_pct <= 0:
        return False
    distance = entry_value - neutral
    if abs(distance) < 1e-12:
        return True
    progress_remaining = (current_value - neutral) / distance
    return progress_remaining <= 1.0 - retrace_pct


def _trailing_stop_price(position: Position, lock_in_pct: float) -> float:
    """Compute the new SL price when trailing is engaged."""
    sign = position.direction.sign
    return position.entry_price * (1 + sign * lock_in_pct)


def evaluate_exit(position: Position, snap: Snapshot, cfg: Config) -> list[ExitEvent]:
    """Evaluate exit rules. Caller is responsible for mutating `position` based on returned events."""
    if position.closed or position.symbol != snap.symbol:
        return []

    events: list[ExitEvent] = []
    cfg_exits = cfg.exits
    price = snap.price

    # --- Stop loss ---
    sl = position.stop_loss_price
    if position.direction is Direction.LONG and price <= sl:
        events.append(
            ExitEvent(kind="SL_HIT", fraction_closed=position.open_fraction, description=f"price {price:g} <= SL {sl:g}")
        )
        return events
    if position.direction is Direction.SHORT and price >= sl:
        events.append(
            ExitEvent(kind="SL_HIT", fraction_closed=position.open_fraction, description=f"price {price:g} >= SL {sl:g}")
        )
        return events

    # --- Take profit ladder ---
    fav_pct = _favorable_pct(position, price)
    for level in position.tp_levels:
        if not level.hit and fav_pct >= level.pct:
            level.hit = True
            events.append(
                ExitEvent(
                    kind="TP_HIT",
                    fraction_closed=level.fraction,
                    description=f"reached +{level.pct * 100:.2f}% — fix {level.fraction * 100:.0f}% of position",
                )
            )

    # --- Trailing stop ---
    if cfg_exits.trailing.enabled:
        if fav_pct > position.best_favorable_pct:
            position.best_favorable_pct = fav_pct
        # ATR-based activation when the position recorded an entry-time ATR
        # and the config has `activate_at_atr_mult` set. Otherwise fall back
        # to the fixed `activate_at_pct`. The ATR path scales with each
        # symbol's actual volatility on entry, so trailing engages at a
        # comparable "distance traveled" regardless of regime.
        atr_mult = cfg_exits.trailing.activate_at_atr_mult
        activate_pct = cfg_exits.trailing.activate_at_pct
        if (
            atr_mult is not None
            and position.entry_atr_1h is not None
            and position.entry_atr_1h > 0
            and position.entry_price > 0
        ):
            activate_pct = (atr_mult * position.entry_atr_1h) / position.entry_price
        if (
            position.best_favorable_pct >= activate_pct
            and position.stop_loss_price == position.initial_stop_loss_price
        ):
            new_sl = _trailing_stop_price(position, cfg_exits.trailing.lock_in_pct)
            events.append(
                ExitEvent(
                    kind="TRAILING_MOVE",
                    fraction_closed=0.0,
                    new_stop_loss_price=new_sl,
                    description=(
                        f"reached +{position.best_favorable_pct * 100:.2f}% — "
                        f"SL moved to {new_sl:g} (lock {cfg_exits.trailing.lock_in_pct * 100:+.2f}%)"
                    ),
                )
            )

    # --- Time stop ---
    age = datetime.now(tz=UTC) - position.entry_ts
    if age >= timedelta(minutes=cfg_exits.time_stop_minutes):
        events.append(
            ExitEvent(
                kind="TIME_STOP",
                fraction_closed=position.open_fraction,
                description=f"age {age.total_seconds() / 60:.0f}m >= {cfg_exits.time_stop_minutes}m",
            )
        )
        return events

    # --- Reason invalidation ---
    ri = cfg_exits.reason_invalidation
    if ri.enabled:
        invalidated = False
        why = ""
        if "funding_extreme" in position.reason and snap.funding_rate is not None:
            entry_f = position.reason_metric_at_entry.get("funding_rate")
            if _metric_retraced_to_neutral(
                entry_value=entry_f,
                current_value=snap.funding_rate,
                neutral=0.0,
                retrace_pct=ri.funding_normalized_retrace_pct,
            ) and entry_f is not None:
                invalidated = True
                why = f"funding {snap.funding_rate * 100:+.3f}% retraced from {entry_f * 100:+.3f}%"
        if (
            not invalidated
            and "lsr_extreme" in position.reason
            and snap.long_short_ratio is not None
        ):
            entry_lsr = position.reason_metric_at_entry.get("long_short_ratio")
            if _metric_retraced_to_neutral(
                entry_value=entry_lsr,
                current_value=snap.long_short_ratio,
                neutral=1.0,
                retrace_pct=ri.lsr_normalized_retrace_pct,
            ):
                invalidated = True
                why = (
                    f"L/S {snap.long_short_ratio:.2f} retraced from {entry_lsr:.2f} toward 1.00"
                )
        # Momentum-reversal gate for point-in-time triggers (oi_surge, liq_cascade).
        # Idea: these signals have no "metric normalized" gate, so if price has
        # clearly turned against entry within the first window minutes, the
        # squeeze/flow obviously did NOT follow through — bail at break-even
        # instead of waiting out the time-stop.
        if (
            not invalidated
            and ("oi_surge" in position.reason or "liq_cascade" in position.reason)
            and age <= timedelta(minutes=ri.momentum_window_minutes)
        ):
            adverse_pct = -_favorable_pct(position, price)  # >0 = price moved against us
            if adverse_pct >= ri.momentum_reversal_pct:
                invalidated = True
                why = (
                    f"price reversed {adverse_pct * 100:.2f}% against entry within "
                    f"{age.total_seconds() / 60:.0f}m (no follow-through on squeeze)"
                )
        if invalidated:
            events.append(
                ExitEvent(
                    kind="REASON_INVALIDATED",
                    fraction_closed=position.open_fraction,
                    description=why,
                )
            )

    return events
