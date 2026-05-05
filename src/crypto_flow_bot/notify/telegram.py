"""Telegram notifier — async sender + alert formatters."""

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
    text = (
        f"{arrow} <b>{side} {sym}</b> @ <code>{position.entry_price:g}</code>\n"
        f"<i>{position.id}</i>\n"
        f"\n<b>Why:</b>\n{rule_lines}\n"
        f"\n<b>Plan:</b>\n"
        f"  SL: <code>{position.stop_loss_price:g}</code>  ({-cfg.exits.stop_loss_pct * 100:.2f}%)\n"
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
