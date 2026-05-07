"""Main async loop: poll snapshots, evaluate signals/exits, dispatch alerts."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from datetime import UTC, datetime, timedelta

import httpx
from dotenv import load_dotenv

from crypto_flow_bot import __version__
from crypto_flow_bot.config import Config, load_config
from crypto_flow_bot.data.binance import BinanceClient, build_snapshot
from crypto_flow_bot.data.liquidations import LiquidationStream
from crypto_flow_bot.engine.exits import evaluate_exit
from crypto_flow_bot.engine.models import Snapshot
from crypto_flow_bot.engine.signals import evaluate
from crypto_flow_bot.engine.state import StateStore
from crypto_flow_bot.log.store import JsonlLogger
from crypto_flow_bot.notify.stats import (
    compute_stats,
    compute_symbol_stats,
    format_stats_digest,
    is_past_weekly_send_time,
    read_latest_positions,
)
from crypto_flow_bot.notify.telegram import (
    TelegramNotifier,
    format_entry_alert,
    format_exit_alert,
    format_heartbeat,
    format_startup,
)

log = logging.getLogger(__name__)


def _setup_logging() -> None:
    level = os.environ.get("CRYPTO_FLOW_BOT_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _read_env() -> tuple[str, list[str]]:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids_raw = os.environ.get("TELEGRAM_CHAT_IDS", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env var is required")
    if not chat_ids_raw:
        raise RuntimeError("TELEGRAM_CHAT_IDS env var is required (comma-separated chat ids)")
    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    return token, chat_ids


class Bot:
    def __init__(
        self,
        cfg: Config,
        client: BinanceClient,
        liq_stream: LiquidationStream,
        notifier: TelegramNotifier,
        state: StateStore,
        logger: JsonlLogger,
    ) -> None:
        self.cfg = cfg
        self.client = client
        self.liq_stream = liq_stream
        self.notifier = notifier
        self.state = state
        self.logger = logger
        self._stop = asyncio.Event()
        self._last_heartbeat = datetime.now(tz=UTC)

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        self.liq_stream.start()
        try:
            await asyncio.gather(
                self._poll_loop(),
                self._exit_loop(),
                self._heartbeat_loop(),
                self._commands_loop(),
                self._stats_digest_loop(),
            )
        finally:
            await self.liq_stream.stop()

    # ---------- loops ----------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            for symbol in self.cfg.symbols:
                try:
                    snap = await build_snapshot(
                        self.client,
                        self.liq_stream,
                        symbol,
                        oi_window_minutes=self.cfg.signals.oi_surge.window_minutes,
                    )
                except Exception as e:
                    log.warning("snapshot for %s failed: %s", symbol, e)
                    continue
                await self.logger.write_snapshot(snap)
                await self._handle_entry_signals(snap)
            await self._sleep(self.cfg.poll_interval_seconds)

    async def _exit_loop(self) -> None:
        # We need fresh prices to check SL/TP. Reuse the latest mark price call (cheap).
        while not self._stop.is_set():
            for pos in list(self.state.open_positions()):
                try:
                    price = await self.client.latest_price(pos.symbol)
                except Exception as e:
                    log.warning("price for %s failed: %s", pos.symbol, e)
                    continue
                # Build a slim snapshot for evaluate_exit (only price + previously seen metrics).
                snap = Snapshot(symbol=pos.symbol, ts=datetime.now(tz=UTC), price=price)
                events = evaluate_exit(pos, snap, self.cfg)
                for ev in events:
                    await self._handle_exit_event(pos, ev, price)
            self.state.save()
            await self._sleep(self.cfg.exit_check_interval_seconds)

    async def _heartbeat_loop(self) -> None:
        """Two heartbeat tracks:

        1. *Chatty* periodic heartbeat (every `heartbeat_minutes`) — only when
           `silent_when_idle: false`. Useful while debugging.
        2. *Daily liveness* ping (once per UTC day, at `daily_liveness_hour_utc`)
           — fires regardless of `silent_when_idle`. Lets us notice a silent
           dead bot quickly instead of finding out via missing alerts later.
        """
        while not self._stop.is_set():
            await self._sleep(60)
            now = datetime.now(tz=UTC)

            # Daily liveness ping (always-on).
            liveness_hour = self.cfg.notifier.daily_liveness_hour_utc
            today_key = now.strftime("%Y-%m-%d")
            if (
                liveness_hour is not None
                and now.hour >= liveness_hour
                and self.state.last_liveness_ping_date != today_key
            ):
                hb = format_heartbeat(len(self.state.open_positions()), self.cfg.symbols)
                await self.notifier.send(hb.text)
                await self.logger.write_alert(hb)
                self._last_heartbeat = now
                self.state.last_liveness_ping_date = today_key
                self.state.save()
                continue

            # Chatty periodic heartbeat (only when not silent).
            if self.cfg.notifier.silent_when_idle:
                continue
            if now - self._last_heartbeat >= timedelta(minutes=self.cfg.notifier.heartbeat_minutes):
                self._last_heartbeat = now
                hb = format_heartbeat(len(self.state.open_positions()), self.cfg.symbols)
                await self.notifier.send(hb.text)
                await self.logger.write_alert(hb)

    async def _commands_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.notifier.poll_commands(self.cfg)
            except Exception as e:  # never let this loop crash the bot
                log.warning("command polling errored: %s", e)
            await self._sleep(self.cfg.notifier.command_poll_interval_seconds)

    async def _stats_digest_loop(self) -> None:
        """Send a weekly summary once per ISO week, on or after the configured
        weekday/hour (UTC).

        We fire as soon as we are past the configured "send time" of the
        current ISO week AND we haven't sent a digest tagged with this
        week's key yet. This means a Fly redeploy or short outage that
        spans the exact send-minute does not cause us to miss the week
        — the digest goes out the next minute the bot is alive.
        """
        cfg = self.cfg.stats
        if not cfg.enabled:
            return
        while not self._stop.is_set():
            await self._sleep(60)
            now = datetime.now(tz=UTC)
            iso_year, iso_week, _ = now.isocalendar()
            week_key = f"{iso_year}-W{iso_week:02d}"
            if self.state.last_stats_digest_week == week_key:
                continue
            if not is_past_weekly_send_time(now, cfg.weekday, cfg.hour_utc):
                continue
            try:
                positions = read_latest_positions(self.logger.positions_path)
                stats = compute_stats(positions, now=now, window_days=cfg.window_days)
                per_symbol = compute_symbol_stats(positions, now=now, window_days=cfg.window_days)
                # Count unique positions in window for the header.
                cutoff = now - timedelta(days=cfg.window_days)
                total_unique = sum(
                    1
                    for p in positions
                    if (entry := p.get("entry_ts"))
                    and datetime.fromisoformat(entry) >= cutoff
                )
                text = format_stats_digest(
                    stats,
                    window_days=cfg.window_days,
                    total_positions=total_unique,
                    per_symbol=per_symbol,
                )
                await self.notifier.send(text)
            except Exception as e:
                log.exception("stats digest failed: %s", e)
                continue
            self.state.last_stats_digest_week = week_key
            self.state.save()

    # ---------- entry / exit handling ----------

    async def _handle_entry_signals(self, snap: Snapshot) -> None:
        for candidate in evaluate(snap, self.cfg):
            cd = self.state.cooldown_remaining_seconds(
                candidate.symbol, candidate.direction, self.cfg.alert_cooldown_seconds
            )
            if cd > 0:
                log.debug("cooldown active for %s %s: %.0fs left", candidate.symbol, candidate.direction.value, cd)
                continue
            existing = self.state.open_for(candidate.symbol, candidate.direction)
            if existing is not None:
                log.debug("position already open for %s %s; skip", candidate.symbol, candidate.direction.value)
                continue
            if self.state.open_for(candidate.symbol, candidate.direction.opposite) is not None:
                log.info(
                    "skipping new %s entry for %s — opposite-side position already open",
                    candidate.direction.value, candidate.symbol,
                )
                continue
            # Risk #2 — daily-loss circuit breaker.
            if self.state.daily_loss_cap_reached(self.cfg):
                log.info(
                    "skipping new %s entry for %s — daily-loss cap reached (%d SLs today)",
                    candidate.direction.value, candidate.symbol, self.state.losses_today_count,
                )
                continue
            # Risk #1 — global concurrency cap (and optional per-direction cap).
            risk = self.cfg.risk
            open_now = self.state.open_positions()
            if len(open_now) >= risk.max_concurrent_positions:
                log.info(
                    "skipping new %s entry for %s — at max_concurrent_positions=%d",
                    candidate.direction.value, candidate.symbol, risk.max_concurrent_positions,
                )
                continue
            if risk.max_per_direction is not None:
                same_dir = sum(1 for p in open_now if p.direction == candidate.direction)
                if same_dir >= risk.max_per_direction:
                    log.info(
                        "skipping new %s entry for %s — at max_per_direction=%d",
                        candidate.direction.value, candidate.symbol, risk.max_per_direction,
                    )
                    continue
            position = self.state.open_from_signal(candidate, self.cfg)
            self.state.mark_alerted(candidate.symbol, candidate.direction)
            self.state.save()
            alert = format_entry_alert(candidate, position, self.cfg)
            await self.notifier.send(alert.text)
            await self.logger.write_alert(alert)
            await self.logger.write_position(position)

    async def _handle_exit_event(self, position, ev, price: float) -> None:  # type: ignore[no-untyped-def]
        cfg = self.cfg
        if ev.kind == "TRAILING_MOVE":
            if ev.new_stop_loss_price is not None:
                position.stop_loss_price = ev.new_stop_loss_price
        elif ev.fraction_closed > 0:
            self.state.close_position(position, price, reason=ev.kind, fraction=ev.fraction_closed)
            # Daily-loss circuit breaker tally.
            self.state.record_loss_if_today(ev.kind)
        alert = format_exit_alert(position, ev, price, cfg)
        await self.notifier.send(alert.text)
        await self.logger.write_alert(alert)
        await self.logger.write_position(position)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            return


async def amain() -> None:
    load_dotenv()
    _setup_logging()
    cfg = load_config()
    token, chat_ids = _read_env()

    http = httpx.AsyncClient(timeout=10.0)
    binance = BinanceClient(http=httpx.AsyncClient(base_url="https://fapi.binance.com", timeout=10.0))
    liq = LiquidationStream(
        window_minutes=cfg.signals.liq_cascade.window_minutes,
        exchanges=cfg.liquidations.exchanges,
        symbols=cfg.symbols,
    )
    log.info(
        "liquidation aggregator enabled for: %s",
        ", ".join(liq.configured_exchanges) or "<none>",
    )
    notifier = TelegramNotifier(token, chat_ids, http=http)
    state = StateStore()
    logger = JsonlLogger()

    bot = Bot(cfg, binance, liq, notifier, state, logger)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # add_signal_handler isn't implemented on Windows.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, bot.request_stop)

    log.info(
        "crypto-flow-bot started; watching %s every %ds",
        ", ".join(cfg.symbols), cfg.poll_interval_seconds,
    )
    # Drop any /start messages queued up before this run so we don't double-reply.
    await notifier.clear_pending_updates()
    if cfg.notifier.send_startup_message:
        startup = format_startup(cfg, __version__)
        try:
            await notifier.send(startup.text)
            await logger.write_alert(startup)
        except Exception as e:
            log.warning("startup notification failed: %s", e)
    try:
        await bot.run()
    finally:
        await binance.aclose()
        await notifier.aclose()
        await http.aclose()
        state.save()
        log.info("crypto-flow-bot stopped")


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
