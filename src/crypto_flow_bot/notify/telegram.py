"""Telegram notifier — async sender + alert formatters + /start command handler."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from html import escape
from typing import Any

import httpx

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.exits import ExitEvent
from crypto_flow_bot.engine.models import Alert, Direction, Position, utcnow
from crypto_flow_bot.engine.signals import SignalCandidate

log = logging.getLogger(__name__)

_CHAT_ID_SPLIT_RE = re.compile(r"[\s,;]+")


@dataclass(frozen=True)
class TelegramDeliveryFailure:
    chat_id: str
    reason: str


class TelegramDeliveryError(RuntimeError):
    """Raised after a broadcast attempts every chat and at least one send fails."""

    def __init__(self, failures: list[TelegramDeliveryFailure], success_count: int) -> None:
        self.failures = failures
        self.success_count = success_count
        failed = ", ".join(f"{failure.chat_id} ({failure.reason})" for failure in failures)
        success_label = "chat" if success_count == 1 else "chats"
        super().__init__(
            f"telegram delivery failed for {len(failures)} chat(s): {failed}; "
            f"{success_count} {success_label} succeeded"
        )


def _short_tf_label(cfg: Config) -> str:
    """User-facing short timeframe label from the active config."""
    return cfg.signals.timeframe_short


def _mask_telegram_token(value: Any) -> str:
    """Mask Telegram bot tokens in URL-like strings for safe logging."""
    if value is None:
        return ""
    masked = str(value)
    return re.sub(r"/bot[^/]+/", "/bot***/", masked)


def normalize_chat_ids(chat_ids: str | Iterable[str]) -> list[str]:
    """Normalize Railway/env chat-id input into a stable, de-duplicated broadcast list.

    Railway values are often pasted as comma-separated, newline-separated, semicolon-
    separated, or plain-space-separated strings. Accept all of those forms so adding
    another recipient does not silently turn two IDs into one invalid Telegram chat.
    """
    raw_values = [chat_ids] if isinstance(chat_ids, str) else chat_ids
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for candidate in _CHAT_ID_SPLIT_RE.split(str(raw).strip()):
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
    return normalized


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_ids: list[str], http: httpx.AsyncClient | None = None) -> None:
        self.token = bot_token
        self.chat_ids = normalize_chat_ids(chat_ids)
        if not self.chat_ids:
            raise ValueError("at least one Telegram chat id is required")
        log.info("telegram notifier configured for %d broadcast chat(s)", len(self.chat_ids))
        self._http = http or httpx.AsyncClient(timeout=10.0)
        self._owns_http = http is None
        self._update_offset: int = 0

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def send(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        failures: list[TelegramDeliveryFailure] = []
        success_count = 0
        for chat_id in self.chat_ids:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            try:
                log.debug("telegram send request: %s", _mask_telegram_token(url))
                r = await self._http.post(url, json=payload)
                if r.status_code == 200:
                    success_count += 1
                    continue
                reason = f"{r.status_code} {r.text}"
                failures.append(TelegramDeliveryFailure(chat_id=chat_id, reason=reason))
                log.warning("telegram send to %s failed: %s", chat_id, reason)
            except (TimeoutError, httpx.HTTPError) as e:
                reason = _mask_telegram_token(e)
                failures.append(TelegramDeliveryFailure(chat_id=chat_id, reason=reason))
                log.warning("telegram send to %s errored: %s", chat_id, reason)
        if failures:
            raise TelegramDeliveryError(failures, success_count)

    async def send_to(self, chat_id: str, text: str) -> None:
        """Send a message to a specific chat (used for /start replies)."""
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            log.debug("telegram send_to request: %s", _mask_telegram_token(url))
            r = await self._http.post(url, json=payload)
            if r.status_code != 200:
                log.warning("telegram send_to %s failed: %s %s", chat_id, r.status_code, r.text)
        except (TimeoutError, httpx.HTTPError) as e:
            log.warning("telegram send_to %s errored: %s", chat_id, _mask_telegram_token(e))

    async def clear_pending_updates(self) -> None:
        """Drop any /start messages queued up before the bot started.

        Without this, every restart would re-reply to old /start commands.
        """
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        try:
            log.debug("telegram clear_pending_updates request: %s", _mask_telegram_token(url))
            r = await self._http.get(url, params={"timeout": 0}, timeout=10.0)
            if r.status_code != 200:
                return
            results = r.json().get("result", [])
        except (TimeoutError, httpx.HTTPError) as e:
            log.debug("clear_pending_updates errored: %s", _mask_telegram_token(e))
            return
        if results:
            self._update_offset = results[-1]["update_id"] + 1
            log.info("dropped %d pending Telegram updates on startup", len(results))

    async def poll_commands(self, cfg: Config) -> None:
        """Poll for incoming messages and handle /start command."""
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params: dict[str, int | str] = {"timeout": 0, "allowed_updates": "message"}
        if self._update_offset:
            params["offset"] = self._update_offset
        try:
            log.debug("telegram poll_commands request: %s", _mask_telegram_token(url))
            r = await self._http.get(url, params=params, timeout=15.0)
            if r.status_code != 200:
                log.debug("getUpdates returned %s", r.status_code)
                return
            data = r.json()
        except (TimeoutError, httpx.HTTPError) as e:
            log.debug("getUpdates errored: %s", _mask_telegram_token(e))
            return

        for update in data.get("result", []):
            self._update_offset = update["update_id"] + 1
            message = update.get("message")
            if not message:
                continue
            text = (message.get("text") or "").strip()
            chat_id = str(message["chat"]["id"])
            if text == "/start":
                log.info("received /start from chat %s", chat_id)
                greeting = format_greeting(cfg, subscribed=chat_id in self.chat_ids, chat_id=chat_id)
                await self.send_to(chat_id, greeting)


def _pretty(symbol: str, cfg: Config) -> str:
    return cfg.notifier.pretty_names.get(symbol, symbol)


def format_entry_alert(candidate: SignalCandidate, position: Position, cfg: Config) -> Alert:
    sym = _pretty(candidate.symbol, cfg)
    arrow = "🟢" if candidate.direction is Direction.LONG else "🔴"
    side = "LONG" if candidate.direction is Direction.LONG else "SHORT"
    strong_tag = " 🔥 <b>STRONG</b>" if candidate.is_strong else ""
    rule_lines = "\n".join(f"  • {r.description}" for r in candidate.fired_rules)
    downgrade_lines = "\n".join(
        f"  ↘ {d.description}" for d in candidate.entry_downgrades
    )
    tp_lines = "\n".join(
        f"  TP{i + 1} ({lvl.fraction * 100:.0f}%): <code>{position.entry_price * (1 + position.direction.sign * lvl.pct):g}</code>  ({lvl.pct * 100:+.2f}%)"
        for i, lvl in enumerate(position.tp_levels)
    )
    # SL distance derived from the actual position (so ATR-sized stops display correctly).
    sl_pct = 0.0
    if position.entry_price > 0:
        sl_pct = (
            (position.stop_loss_price - position.entry_price) / position.entry_price * position.direction.sign
        )
    trailing_activate_pct = cfg.exits.trailing.activate_at_pct
    if (
        position.entry_atr_1h is not None
        and cfg.exits.trailing.activate_at_atr_mult is not None
        and position.entry_price > 0
    ):
        trailing_activate_pct = (
            cfg.exits.trailing.activate_at_atr_mult * position.entry_atr_1h / position.entry_price
        )

    text = (
        f"{arrow} <b>{side} {sym}</b> @ <code>{position.entry_price:g}</code>{strong_tag}\n"
        f"<i>{position.id}</i>\n"
        f"\n<b>Why:</b>\n{rule_lines}\n"
        + (
            f"\n<b>Downgrades:</b>\n{downgrade_lines}\n"
            if candidate.entry_downgrades
            else ""
        )
        + f"\n<b>Plan:</b>\n"
        f"  SL: <code>{position.stop_loss_price:g}</code>  ({sl_pct * 100:+.2f}%)\n"
        f"{tp_lines}\n"
        f"  Trailing: " + (
            f"after {trailing_activate_pct * 100:.2f}% lock {cfg.exits.trailing.lock_in_pct * 100:+.2f}%"
            if cfg.exits.trailing.enabled
            else "off"
        ) + "\n"
        f"  Time stop: {cfg.exits.time_stop_minutes} min"
    )
    return Alert(
        kind="ENTRY",
        symbol=candidate.symbol,
        ts=utcnow(),
        text=text,
        direction=candidate.direction,
        position_id=position.id,
        payload={"reason": position.reason, "metrics": position.reason_metric_at_entry},
    )


def format_exit_alert(position: Position, ev: ExitEvent, snap_price: float, cfg: Config) -> Alert:
    sym = _pretty(position.symbol, cfg)
    side = "LONG" if position.direction is Direction.LONG else "SHORT"
    if ev.kind == "TP_HIT":
        head = f"🟡 TP hit on {side} {sym} — fix {ev.fraction_closed * 100:.0f}%"
    elif ev.kind == "SL_HIT":
        head = f"🔴 SL hit on {side} {sym} — close all"
    elif ev.kind == "TIME_STOP":
        head = f"⏰ Time stop on {side} {sym} — close all"
    elif ev.kind == "REASON_INVALIDATED":
        # Momentum-reversal invalidation triggers a small-loss exit, not BE;
        # the funding/LSR normalization path usually fires near BE.
        if "reversed" in ev.description:
            head = f"❎ No follow-through on {side} {sym} — close early"
        else:
            head = f"❎ Reason invalidated on {side} {sym} — close at break-even"
    elif ev.kind == "TRAILING_MOVE":
        head = f"🟦 Trailing move on {side} {sym}"
    elif ev.kind == "EXIT_REGIME_INVALIDATED":
        head = f"🚫 Regime invalidated on {side} {sym} — close all"
    elif ev.kind == "EXIT_OPPOSITE_SIGNAL":
        head = f"↔️ Opposite signal on {side} {sym} — close all"
    else:
        head = f"ℹ️ {ev.kind} on {side} {sym}"

    pnl_pct = (snap_price - position.entry_price) / position.entry_price * position.direction.sign * 100
    text = (
        f"{head}\n"
        f"<i>{position.id}</i>\n"
        f"  Entry: <code>{position.entry_price:g}</code>  Now: <code>{snap_price:g}</code>  "
        f"PnL: <b>{pnl_pct:+.2f}%</b>\n"
        f"  {ev.description}"
    )
    if ev.new_stop_loss_price is not None:
        text += f"\n  New SL: <code>{ev.new_stop_loss_price:g}</code>"
    return Alert(
        kind=ev.kind,
        symbol=position.symbol,
        ts=utcnow(),
        text=text,
        direction=position.direction,
        position_id=position.id,
        payload={"price": snap_price, "fraction_closed": ev.fraction_closed},
    )


def _format_top_reasons(reasons: dict[str, int], top_n: int = 5) -> str:
    if not reasons:
        return "none"
    ordered = sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))
    shown = ordered[:top_n]
    base = ", ".join(f"{k}={v}" for k, v in shown)
    hidden = len(ordered) - len(shown)
    return f"{base}, +{hidden} more" if hidden > 0 else base


def format_heartbeat(open_count: int, watched: list[str], decision_summary=None) -> Alert:
    decision_summary = decision_summary or {}
    text = (
        "🟢 <b>crypto-flow-bot heartbeat</b>\n"
        f"  Watching: {', '.join(watched)}\n"
        f"  Open virtual positions: {open_count}"
    )
    if hasattr(decision_summary, "total_candidates"):
        total = decision_summary.total_candidates
        text += (
            "\n\n<b>15m decision summary:</b>"
            f"\n  candidates: {total} | accepted: {decision_summary.accepted_signals} | blocked: {decision_summary.rejected_or_blocked}"
            f"\n  sides: long {decision_summary.long_candidates} / short {decision_summary.short_candidates}"
            f"\n  top blocks: {_format_top_reasons(decision_summary.blocked_counts_by_reason)}"
            f"\n  downgrades: {_format_top_reasons(decision_summary.downgrade_counts_by_reason)}"
            f"\n  exits: {_format_top_reasons(decision_summary.exit_counts_by_reason)}"
            f"\n  data missing: {_format_top_reasons(decision_summary.missing_data_counts_by_reason)}"
            f"\n  data stale: {_format_top_reasons(decision_summary.stale_data_counts_by_reason)}"
        )
    return Alert(kind="HEARTBEAT", symbol="*", ts=utcnow(), text=text)


def format_startup(cfg: Config, version: str) -> Alert:
    pretty = ", ".join(cfg.notifier.pretty_names.get(s, s) for s in cfg.symbols)
    # Per-symbol thresholds: each pair has its own funding/LSR/OI/liq cuts after
    # PR #8/#9 and they often diverge from the global defaults. Showing the
    # global block alone is misleading, so render one block per watched symbol.
    blocks: list[str] = []
    for sym in cfg.symbols:
        s = cfg.signals.for_symbol(sym)
        rules: list[str] = []
        if s.funding_extreme.enabled:
            rules.append(
                f"funding ≥ {s.funding_extreme.long_overheated_above * 100:+.3f}% / "
                f"≤ {s.funding_extreme.short_overheated_below * 100:+.3f}%"
            )
        if s.lsr_extreme.enabled:
            rules.append(
                f"top L/S ≥ {s.lsr_extreme.long_heavy_above:.2f} or ≤ "
                f"{s.lsr_extreme.short_heavy_below:.2f}"
            )
        if s.oi_surge.enabled:
            rules.append(
                f"OI Δ ≥ {s.oi_surge.pct_change_threshold * 100:.1f}% / "
                f"{s.oi_surge.window_minutes}min"
            )
        if s.liq_cascade.enabled:
            rules.append(
                f"one-sided liq ≥ ${s.liq_cascade.usd_threshold / 1e6:.0f}M / "
                f"{s.liq_cascade.window_minutes}min"
            )
        rules_text = "; ".join(rules) if rules else "no rules enabled"
        blocks.append(f"  • <b>{cfg.notifier.pretty_names.get(sym, sym)}</b>: {rules_text}")
    rules_block = "\n".join(blocks)

    # Exits: SL/TP are ATR-based when atr_sizing is on (the default). The fixed
    # stop_loss_pct is only used as a fallback when ATR isn't available, so do
    # not advertise it as the active behavior.
    atr = cfg.exits.atr_sizing
    if atr.enabled:
        tp_mults = " / ".join(f"{m:g}×ATR" for m in atr.tp_atr_mults)
        exits_text = (
            f"<b>Exits:</b> SL {atr.sl_atr_mult:g}×ATR({_short_tf_label(cfg)}) · "
            f"TP ladder {tp_mults} (STRONG → last TP {atr.strong_last_tp_mult:g}×ATR) · "
            f"time stop {cfg.exits.time_stop_minutes}min"
        )
        if atr.fallback_to_pct:
            exits_text += f"\n  <i>fallback when ATR missing: SL {cfg.exits.stop_loss_pct * 100:.2f}%</i>"
    else:
        exits_text = (
            f"<b>Exits:</b> SL {cfg.exits.stop_loss_pct * 100:.2f}% · "
            f"TP ladder {len(cfg.exits.take_profit_levels)} steps · "
            f"time stop {cfg.exits.time_stop_minutes}min"
        )

    text = (
        f"🤖 <b>crypto-flow-bot v{version} started</b>\n"
        f"  Watching: {pretty}\n"
        f"  Poll: every {cfg.poll_interval_seconds}s\n"
        f"\n<b>Signal rules (per symbol):</b>\n{rules_block}\n"
        f"\n{exits_text}"
    )
    return Alert(kind="STARTUP", symbol="*", ts=utcnow(), text=text)


def format_greeting(cfg: Config, *, subscribed: bool | None = None, chat_id: str | None = None) -> str:
    """Welcome message shown when a user sends /start to the bot."""
    pretty = ", ".join(cfg.notifier.pretty_names.get(s, s) for s in cfg.symbols)
    subscription_text = ""
    if subscribed is True:
        subscription_text = "\n\n✅ <b>This chat is subscribed to broadcast signals.</b>"
    elif subscribed is False:
        chat_hint = f" Add <code>{escape(chat_id)}</code> to Railway TELEGRAM_CHAT_IDS." if chat_id else ""
        subscription_text = (
            "\n\n⚠️ <b>This chat is not in TELEGRAM_CHAT_IDS.</b>"
            " /start replies work here, but entry/exit/heartbeat broadcasts only go to configured chat IDs."
            f"{chat_hint}"
        )
    return (
        "👋 <b>Welcome to crypto-flow-bot!</b>\n\n"
        "I watch Binance USD-M futures flow data and send you trade signals "
        "with full SL / TP / trailing / time-stop / reason-invalidation exits.\n\n"
        f"<b>Currently watching:</b> {pretty}\n\n"
        "<b>What you'll receive:</b>\n"
        "  🟢 LONG / 🔴 SHORT entry signals (with SL + TP ladder)\n"
        "  🟡 TP hits — partial profit-take\n"
        "  🔴 SL hits — close all\n"
        "  🟦 Trailing moves — SL tightens after a favorable move\n"
        "  ⏰ Time stops — close after the configured timeout\n"
        "  ❎ Reason invalidated — close at break-even\n\n"
        "<b>Important:</b> the bot does NOT execute trades. It only suggests "
        "entries and exits — you place the orders yourself on the exchange.\n\n"
        "<i>Signals are statistical heuristics, not guaranteed wins. "
        "Backtest and paper-trade before risking real capital.</i>"
        f"{subscription_text}"
    )
