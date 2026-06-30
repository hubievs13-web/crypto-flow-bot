"""Runtime patch for trade-signal visibility before virtual entries open."""

from __future__ import annotations

import asyncio
import copy
import logging
from typing import Any

from crypto_flow_bot.engine.models import Alert, Direction, Snapshot, utcnow

from crypto_flow_bot import main as main_mod

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


def _cache_state(cache: Any) -> Any:
    if cache is None or not hasattr(cache, "_fires"):
        return None
    return copy.deepcopy(cache._fires)


def _restore_cache_state(cache: Any, state: Any) -> None:
    if cache is not None and state is not None and hasattr(cache, "_fires"):
        cache._fires = state


def _format_signal_only_alert(candidate: Any, blocked_reason: str, cfg: Any) -> Alert:
    sym = cfg.notifier.pretty_names.get(candidate.symbol, candidate.symbol)
    arrow = "🟢" if candidate.direction is Direction.LONG else "🔴"
    side = "LONG" if candidate.direction is Direction.LONG else "SHORT"
    strong_tag = " 🔥 <b>STRONG</b>" if candidate.is_strong else ""
    rule_lines = "\n".join(f"  • {r.description}" for r in candidate.fired_rules)
    downgrade_lines = "\n".join(f"  ↘ {d.description}" for d in candidate.entry_downgrades)
    text = (
        f"{arrow} <b>{side} {sym}</b> signal @ <code>{candidate.snapshot.price:g}</code>{strong_tag}\n"
        f"<i>{candidate.signal_id}</i>\n"
        f"\n<b>Why:</b>\n{rule_lines}\n"
        + (f"\n<b>Downgrades:</b>\n{downgrade_lines}\n" if candidate.entry_downgrades else "")
        + "\n<b>Virtual position:</b> not opened — "
        + _BLOCKED_REASON_LABELS.get(blocked_reason, blocked_reason)
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
    if risk.max_per_direction is None:
        return None
    group = main_mod._correlation_group_for(candidate.symbol, risk.correlated_groups)
    if group is None:
        return None
    same_dir_in_group = sum(
        1 for p in open_now if p.direction == candidate.direction and p.symbol in group
    )
    return "max_per_direction_group" if same_dir_in_group >= risk.max_per_direction else None


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


async def _write_blocked_and_signal(self: Any, candidate: Any, reason: str, snap: Snapshot) -> None:
    self._decision_summary.count_blocked(reason)
    self._maybe_log_skip(candidate, reason)
    await self._safe_log_write(
        "write_blocked",
        lambda candidate=candidate, reason=reason, snap=snap: self.logger.write_blocked(
            signal_id=candidate.signal_id,
            symbol=candidate.symbol,
            direction=candidate.direction,
            blocked_reason=reason,
            fired_rules=[r.name for r in candidate.fired_rules],
            confluence_window_rules=list(candidate.confluence_window_rules),
            snapshot_ts=snap.ts,
        ),
        symbol=candidate.symbol,
        signal_id=candidate.signal_id,
    )
    await self._send_signal_only_alert(candidate, reason)


async def _handle_entry_signals_locked(self: Any, snap: Snapshot) -> None:
    if not hasattr(self, "_decision_summary"):
        self._decision_summary = main_mod.DecisionSummary()

    cache_before = _cache_state(self.confluence_cache)
    candidates = main_mod.evaluate(snap, self.cfg, cache=self.confluence_cache)
    candidate_dirs = {c.direction for c in candidates}
    block_reasons = [self._entry_blocked_reason(c) for c in candidates]
    should_handle_blocked = len(candidate_dirs) >= 2 or any(r is not None for r in block_reasons)

    if not should_handle_blocked:
        _restore_cache_state(self.confluence_cache, cache_before)
        original = self._signal_visibility_original_handle_entry
        await original(snap)
        return

    self._decision_summary.total_candidates += len(candidates)
    self._decision_summary.long_candidates += sum(1 for c in candidates if c.direction is Direction.LONG)
    self._decision_summary.short_candidates += sum(1 for c in candidates if c.direction is Direction.SHORT)
    for candidate in candidates:
        for downgrade in candidate.entry_downgrades:
            self._decision_summary.count_downgrade(downgrade.name)
    self._collect_data_health(snap)

    if len(candidate_dirs) >= 2:
        main_mod.log.info(
            "skipping %s — conflicting signals: %s",
            snap.symbol,
            " | ".join(
                f"{c.direction.value}:{','.join(r.name for r in c.fired_rules)}" for c in candidates
            ),
        )
        for candidate in candidates:
            await self._write_blocked_and_signal(candidate, "conflicting_signals", snap)
        return

    for candidate, reason in zip(candidates, block_reasons, strict=False):
        if reason is not None:
            await self._write_blocked_and_signal(candidate, reason, snap)


async def _liq_fast_loop(self: Any) -> None:
    while not self._stop.is_set():
        for symbol in self.cfg.symbols:
            sig = self.cfg.signals.for_symbol(symbol)
            if not sig.liq_cascade.enabled:
                continue
            long_liq, short_liq = self.liq_stream.totals(symbol)
            thr = sig.liq_cascade.usd_threshold
            if long_liq < thr and short_liq < thr:
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
    patch_attrs = {
        "_signal_visibility_original_handle_entry": main_mod.Bot._handle_entry_signals_locked,
        "_send_signal_only_alert": _send_signal_only_alert,
        "_entry_blocked_reason": _entry_blocked_reason,
        "_maybe_log_skip": _maybe_log_skip,
        "_write_blocked_and_signal": _write_blocked_and_signal,
        "_handle_entry_signals_locked": _handle_entry_signals_locked,
        "_liq_fast_loop": _liq_fast_loop,
        "_trade_signal_visibility_patch_installed": True,
    }
    for name, value in patch_attrs.items():
        setattr(main_mod.Bot, name, value)
