"""Runtime patch for signal visibility before virtual-position acceptance.

The live entrypoint installs this module before the bot starts. It keeps trade
signal Telegram delivery tied to ``engine.signals.evaluate`` output rather than
to successful virtual-position creation. Risk gates still prevent duplicate
virtual positions; they only stop being silent Telegram suppressors.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from crypto_flow_bot import main as main_mod
from crypto_flow_bot.engine.models import Alert, Direction, Snapshot, utcnow
from crypto_flow_bot.notify.telegram import format_entry_alert

log = logging.getLogger(__name__)

_BLOCKED_REASON_LABELS = {
    "conflicting_signals": "conflicting LONG/SHORT signals on the same snapshot",
    "position_open": "same-direction virtual position is already open",
    "opposite_open": "opposite-direction virtual position is already open",
    "cooldown": "symbol/direction is in alert cooldown",
    "post_exit_cooldown": "symbol/direction is in post-exit cooldown",
    "max_concurrent": "max concurrent virtual positions reached",
    "max_per_direction_group": "correlation-group per-direction cap reached",
}


def _format_signal_only_alert(candidate: Any, blocked_reason: str, cfg: Any) -> Alert:
    sym = cfg.notifier.pretty_names.get(candidate.symbol, candidate.symbol)
    arrow = "🟢" if candidate.direction is Direction.LONG else "🔴"
    side = "LONG" if candidate.direction is Direction.LONG else "SHORT"
    strong_tag = " 🔥 <b>STRONG</b>" if candidate.is_strong else ""
    rule_lines = "\n".join(f"  • {r.description}" for r in candidate.fired_rules)
    downgrade_lines = "\n".join(
        f"  ↘ {d.description}" for d in candidate.entry_downgrades
    )
    reason_label = _BLOCKED_REASON_LABELS.get(blocked_reason, blocked_reason)
    text = (
        f"{arrow} <b>{side} {sym}</b> signal @ <code>{candidate.snapshot.price:g}</code>{strong_tag}\n"
        f"<i>{candidate.signal_id}</i>\n"
        f"\n<b>Why:</b>\n{rule_lines}\n"
        + (
            f"\n<b>Downgrades:</b>\n{downgrade_lines}\n"
            if candidate.entry_downgrades
            else ""
        )
        + f"\n<b>Virtual position:</b> not opened — {reason_label}"
    )
    return Alert(
        kind="SIGNAL_BLOCKED",
        symbol=candidate.symbol,
        ts=utcnow(),
        text=text,
        direction=candidate.direction,
        payload={
            "signal_id": candidate.signal_id,
            "blocked_reason": blocked_reason,
            "reason": candidate.reason_label,
            "fired_rules": [r.name for r in candidate.fired_rules],
            "confluence_window_rules": sorted(candidate.confluence_window_rules),
        },
    )


async def _send_signal_only_alert(self: Any, candidate: Any, blocked_reason: str) -> None:
    alert = _format_signal_only_alert(candidate, blocked_reason, self.cfg)
    try:
        await self.notifier.send(alert.text)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning(
            "telegram SIGNAL send failed for %s %s signal_id=%s blocked_reason=%s: %s",
            candidate.symbol,
            candidate.direction.value,
            candidate.signal_id,
            blocked_reason,
            e,
        )
        await self._safe_log_write(
            "write_alert",
            lambda alert=alert: self.logger.write_alert(alert),
            symbol=candidate.symbol,
            signal_id=candidate.signal_id,
            blocked_reason=blocked_reason,
            send_status="failed_to_send",
        )
        return
    await self._safe_log_write(
        "write_alert",
        lambda alert=alert: self.logger.write_alert(alert),
        symbol=candidate.symbol,
        signal_id=candidate.signal_id,
        blocked_reason=blocked_reason,
        send_status="sent",
    )


def _entry_blocked_reason(self: Any, candidate: Any) -> str | None:
    """Return the first virtual-position gate that blocks this candidate.

    Active virtual positions are checked before cooldown so logs show the most
    actionable reason when both are true after an entry alert.
    """
    if self.state.open_for(candidate.symbol, candidate.direction) is not None:
        return "position_open"
    if self.state.open_for(candidate.symbol, candidate.direction.opposite) is not None:
        return "opposite_open"
    cd = self.state.cooldown_remaining_seconds(
        candidate.symbol, candidate.direction, self.cfg.alert_cooldown_seconds
    )
    if cd > 0:
        return "cooldown"
    risk = self.cfg.risk
    post_exit = self.state.post_exit_cooldown_remaining_seconds(
        candidate.symbol, candidate.direction, risk.post_exit_cooldown_seconds
    )
    if post_exit > 0:
        return "post_exit_cooldown"
    open_now = self.state.open_positions()
    if len(open_now) >= risk.max_concurrent_positions:
        return "max_concurrent"
    if risk.max_per_direction is not None:
        group = main_mod._correlation_group_for(candidate.symbol, risk.correlated_groups)
        if group is not None:
            same_dir_in_group = sum(
                1
                for p in open_now
                if p.direction == candidate.direction and p.symbol in group
            )
            if same_dir_in_group >= risk.max_per_direction:
                return "max_per_direction_group"
    return None


def _maybe_log_skip(self: Any, candidate: Any, reason: str) -> None:
    interval = self.cfg.risk.skip_log_interval_seconds
    if self.state.should_log_skip(candidate.symbol, candidate.direction, reason, interval):
        main_mod.log.info(
            "signal blocked %s %s signal_id=%s reason=%s rules=%s",
            candidate.symbol,
            candidate.direction.value,
            candidate.signal_id,
            reason,
            ",".join(r.name for r in candidate.fired_rules),
        )


async def _handle_entry_signals_locked(self: Any, snap: Snapshot) -> None:
    if not hasattr(self, "_decision_summary"):
        self._decision_summary = main_mod.DecisionSummary()
    candidates = main_mod.evaluate(snap, self.cfg, cache=self.confluence_cache)
    self._decision_summary.total_candidates += len(candidates)
    self._decision_summary.long_candidates += sum(1 for c in candidates if c.direction is Direction.LONG)
    self._decision_summary.short_candidates += sum(1 for c in candidates if c.direction is Direction.SHORT)
    for c in candidates:
        for d in c.entry_downgrades:
            self._decision_summary.count_downgrade(d.name)
    self._collect_data_health(snap)

    if len({c.direction for c in candidates}) >= 2:
        main_mod.log.info(
            "skipping %s — conflicting signals: %s",
            snap.symbol,
            " | ".join(
                f"{c.direction.value}:{','.join(r.name for r in c.fired_rules)}"
                for c in candidates
            ),
        )
        for c in candidates:
            self._decision_summary.count_blocked("conflicting_signals")
            await self._safe_log_write(
                "write_blocked",
                lambda c=c, snap=snap: self.logger.write_blocked(
                    signal_id=c.signal_id,
                    symbol=c.symbol,
                    direction=c.direction,
                    blocked_reason="conflicting_signals",
                    fired_rules=[r.name for r in c.fired_rules],
                    confluence_window_rules=list(c.confluence_window_rules),
                    snapshot_ts=snap.ts,
                ),
                symbol=c.symbol,
                signal_id=c.signal_id,
            )
            await self._send_signal_only_alert(c, "conflicting_signals")
        return

    for candidate in candidates:
        blocked_reason = self._entry_blocked_reason(candidate)
        if blocked_reason is not None:
            self._decision_summary.count_blocked(blocked_reason)
            self._maybe_log_skip(candidate, blocked_reason)
            await self._safe_log_write(
                "write_blocked",
                lambda candidate=candidate, blocked_reason=blocked_reason, snap=snap: self.logger.write_blocked(
                    signal_id=candidate.signal_id,
                    symbol=candidate.symbol,
                    direction=candidate.direction,
                    blocked_reason=blocked_reason,
                    fired_rules=[r.name for r in candidate.fired_rules],
                    confluence_window_rules=list(candidate.confluence_window_rules),
                    snapshot_ts=snap.ts,
                ),
                symbol=candidate.symbol,
                signal_id=candidate.signal_id,
            )
            await self._send_signal_only_alert(candidate, blocked_reason)
            continue

        position = self.state.open_from_signal(candidate, self.cfg)
        self._decision_summary.accepted_signals += 1
        alert = format_entry_alert(candidate, position, self.cfg)
        wrote_pos = await self._safe_log_write(
            "write_position",
            lambda position=position: self.logger.write_position(position),
            symbol=position.symbol,
            position_id=position.id,
        )
        if not wrote_pos:
            self.state.positions.pop(position.id, None)
            continue
        self.state.mark_alerted(candidate.symbol, candidate.direction)
        self.state.save()
        try:
            await self.notifier.send(alert.text)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            main_mod.log.warning(
                "telegram ENTRY send failed for %s %s signal_id=%s: %s",
                candidate.symbol,
                candidate.direction.value,
                candidate.signal_id,
                e,
            )
            await self._safe_log_write(
                "write_alert",
                lambda alert=alert: self.logger.write_alert(alert),
                symbol=candidate.symbol,
                signal_id=candidate.signal_id,
                send_status="failed_to_send",
            )
            continue
        await self._safe_log_write(
            "write_alert",
            lambda alert=alert: self.logger.write_alert(alert),
            symbol=candidate.symbol,
            signal_id=candidate.signal_id,
            send_status="sent",
        )


async def _liq_fast_loop(self: Any) -> None:
    while not self._stop.is_set():
        for symbol in self.cfg.symbols:
            sig = self.cfg.signals.for_symbol(symbol)
            if not sig.liq_cascade.enabled:
                continue
            long_liq, short_liq = self.liq_stream.totals(symbol)
            thr = sig.liq_cascade.usd_threshold
            crossed: list[Direction] = []
            if long_liq >= thr:
                crossed.append(Direction.LONG)
            if short_liq >= thr:
                crossed.append(Direction.SHORT)
            if not crossed:
                continue
            try:
                snap = await main_mod.build_snapshot(
                    self.client,
                    self.liq_stream,
                    symbol,
                    oi_window_minutes=sig.oi_surge.window_minutes,
                    slope_window_bars=sig.trend_filter.slope_window_bars,
                    slope_window_bars_4h=sig.trend_filter.slope_window_bars_4h,
                    cvd_window_bars=sig.taker_confirmation.cvd_window_bars,
                    oi_quality_epsilon_pct=sig.oi_surge.quality_epsilon_pct,
                    timeframe_short=sig.timeframe_short,
                    predicted_funding_cap=sig.predicted_funding.funding_cap,
                    predicted_funding_interest_clamp_abs=sig.predicted_funding.interest_clamp_abs,
                    regime_enabled=sig.regime.enabled,
                    regime_timeframe=sig.regime.timeframe,
                    regime_adx_period=sig.regime.adx_period,
                    ema_period=sig.trend_filter.ema_period,
                    atr_period=sig.trend_filter.atr_period,
                    regime_cfg=sig.regime,
                )
            except Exception as e:
                main_mod.log.warning("liq fast-path snapshot for %s failed: %s", symbol, e)
                continue
            self._augment_with_funding_stats(snap)
            self._last_full_snapshot[symbol] = snap
            await self._safe_log_write(
                "write_snapshot",
                lambda snap=snap: self.logger.write_snapshot(snap),
                symbol=symbol,
            )
            await self._handle_entry_signals(snap)
        await self._sleep(self.cfg.liq_fast_check_interval_seconds)


def install_trade_signal_visibility_patch() -> None:
    if getattr(main_mod.Bot, "_trade_signal_visibility_patch_installed", False):
        return
    setattr(main_mod.Bot, "_send_signal_only_alert", _send_signal_only_alert)
    setattr(main_mod.Bot, "_entry_blocked_reason", _entry_blocked_reason)
    setattr(main_mod.Bot, "_maybe_log_skip", _maybe_log_skip)
    setattr(main_mod.Bot, "_handle_entry_signals_locked", _handle_entry_signals_locked)
    setattr(main_mod.Bot, "_liq_fast_loop", _liq_fast_loop)
    setattr(main_mod.Bot, "_trade_signal_visibility_patch_installed", True)
