"""Telegram notifier — async sender + alert formatters + /start command handler."""

from __future__ import annotations

import logging

import httpx

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.exits import ExitEvent
from crypto_flow_bot.engine.models import Alert, Direction, Position, utcnow
from crypto_flow_bot.engine.signals import SignalCandidate

log = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_ids: list[str], http: httpx.AsyncClient | None = None) -> None:
        self.token = bot_token
        self.chat_ids = chat_ids
        self._http = http or httpx.AsyncClient(timeout=10.0)
        self._owns_http = http is None
        self._update_offset: int = 0

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def send(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        for chat_id in self.chat_ids:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            try:
                r = await self._http.post(url, json=payload)
                if r.status_code != 200:
                    log.warning("telegram send to %s failed: %s %s", chat_id, r.status_code, r.text)
            except (TimeoutError, httpx.HTTPError) as e:
                log.warning("telegram send to %s errored: %s", chat_id, e)

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
            r = await self._http.post(url, json=payload)
            if r.status_code != 200:
                log.warning("telegram send_to %s failed: %s %s", chat_id, r.status_code, r.text)
        except (TimeoutError, httpx.HTTPError) as e:
            log.warning("telegram send_to %s errored: %s", chat_id, e)

    async def clear_pending_updates(self) -> None:
        """Drop any /start messages queued up before the bot started.

        Without this, every restart would re-reply to old /start commands.
        """
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        try:
            r = await self._http.get(url, params={"timeout": 0}, timeout=10.0)
            if r.status_code != 200:
                return
            results = r.json().get("result", [])
        except (TimeoutError, httpx.HTTPError) as e:
            log.debug("clear_pending_updates errored: %s", e)
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
            r = await self._http.get(url, params=params, timeout=15.0)
            if r.status_code != 200:
                log.debug("getUpdates returned %s", r.status_code)
                return
            data = r.json()
        except (TimeoutError, httpx.HTTPError) as e:
            log.debug("getUpdates errored: %s", e)
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
                greeting = format_greeting(cfg)
                await self.send_to(chat_id, greeting)


def _pretty(symbol: str, cfg: Config) -> str:
    return cfg.notifier.pretty_names.get(symbol, symbol)


def format_entry_alert(candidate: SignalCandidate, position: Position, cfg: Config) -> Alert:
    sym = _pretty(candidate.symbol, cfg)
    arrow = "🟢" if candidate.direction is Direction.LONG else "🔴"
    side = "LONG" if candidate.direction is Direction.LONG else "SHORT"
    rule_lines = "\n".join(f"  • {r.description}" for r in candidate.fired_rules)
    tp_lines = "\n".join(
        f"  TP{i + 1} ({lvl.fraction * 100:.0f}%): <code>{position.entry_price * (1 + position.direction.sign * lvl.pct):g}</code>  ({lvl.pct * 100:+.2f}%)"
        for i, lvl in enumerate(position.tp_levels)
    )
    # SL distance derived from the actual position (so ATR-sized stops display correctly).
    sl_pct = (position.stop_loss_price - position.entry_price) / position.entry_price * position.direction.sign
    text = (
        f"{arrow} <b>{side} {sym}</b> @ <code>{position.entry_price:g}</code>\n"
        f"<i>{position.id}</i>\n"
        f"\n<b>Why:</b>\n{rule_lines}\n"
        f"\n<b>Plan:</b>\n"
        f"  SL: <code>{position.stop_loss_price:g}</code>  ({sl_pct * 100:+.2f}%)\n"
        f"{tp_lines}\n"
        f"  Trailing: " + (
            f"after {cfg.exits.trailing.activate_at_pct * 100:.2f}% lock {cfg.exits.trailing.lock_in_pct * 100:+.2f}%"
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
        head = f"❎ Reason invalidated on {side} {sym} — close at break-even"
    elif ev.kind == "TRAILING_MOVE":
        head = f"🟦 Trailing move on {side} {sym}"
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


def format_heartbeat(open_count: int, watched: list[str]) -> Alert:
    text = (
        "🟢 <b>crypto-flow-bot heartbeat</b>\n"
        f"  Watching: {', '.join(watched)}\n"
        f"  Open virtual positions: {open_count}"
    )
    return Alert(kind="HEARTBEAT", symbol="*", ts=utcnow(), text=text)


def format_startup(cfg: Config, version: str) -> Alert:
    pretty = ", ".join(cfg.notifier.pretty_names.get(s, s) for s in cfg.symbols)
    rules = []
    s = cfg.signals
    if s.funding_extreme.enabled:
        rules.append(
            f"funding ≥ {s.funding_extreme.long_overheated_above * 100:+.3f}% / "
            f"≤ {s.funding_extreme.short_overheated_below * 100:+.3f}%"
        )
    if s.oi_surge.enabled:
        rules.append(
            f"OI Δ ≥ {s.oi_surge.pct_change_threshold * 100:.1f}% / {s.oi_surge.window_minutes}min"
        )
    if s.lsr_extreme.enabled:
        rules.append(
            f"top L/S ≥ {s.lsr_extreme.long_heavy_above:.2f} or ≤ {s.lsr_extreme.short_heavy_below:.2f}"
        )
    if s.liq_cascade.enabled:
        rules.append(
            f"one-sided liq ≥ ${s.liq_cascade.usd_threshold / 1e6:.0f}M / "
            f"{s.liq_cascade.window_minutes}min"
        )
    rules_block = "\n".join(f"  • {r}" for r in rules) if rules else "  (no rules enabled)"
    text = (
        f"🤖 <b>crypto-flow-bot v{version} started</b>\n"
        f"  Watching: {pretty}\n"
        f"  Poll: every {cfg.poll_interval_seconds}s\n"
        f"\n<b>Signal rules:</b>\n{rules_block}\n"
        f"\n<b>Exits:</b> SL {cfg.exits.stop_loss_pct * 100:.2f}% · "
        f"TP ladder {len(cfg.exits.take_profit_levels)} steps · "
        f"time stop {cfg.exits.time_stop_minutes}min"
    )
    return Alert(kind="STARTUP", symbol="*", ts=utcnow(), text=text)


def format_greeting(cfg: Config) -> str:
    """Welcome message shown when a user sends /start to the bot."""
    pretty = ", ".join(cfg.notifier.pretty_names.get(s, s) for s in cfg.symbols)
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
    )
