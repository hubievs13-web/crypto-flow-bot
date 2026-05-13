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
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import ConfluenceCache, evaluate
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
    # httpx's default INFO logger emits every request URL ("HTTP Request: GET
    # https://api.telegram.org/bot<TOKEN>/getUpdates?... 200 OK"). The Telegram
    # bot token is part of the URL path, so every getUpdates poll (every 5s by
    # default) leaks the token into stdout — and on Fly.io into the public log
    # stream readable by anyone with the deploy token. Cap httpx at WARNING so
    # only failures surface. Network errors already produce app-level warnings
    # via `notify.telegram` / `data.binance`.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _read_env() -> tuple[str, list[str]]:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids_raw = os.environ.get("TELEGRAM_CHAT_IDS", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env var is required")
    if not chat_ids_raw:
        raise RuntimeError("TELEGRAM_CHAT_IDS env var is required (comma-separated chat ids)")
    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    return token, chat_ids


def _correlation_group_for(symbol: str, groups: list[list[str]]) -> list[str] | None:
    """Return the first correlation group that contains `symbol`, or None.

    Used by the entry-side per-direction cap so that the limit is enforced
    *within* a group of correlated symbols (e.g. BTCUSDT/ETHUSDT) instead
    of globally. Symbols not listed in any group are not subject to the
    cap at all.
    """
    for g in groups:
        if symbol in g:
            return g
    return None


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
        # Cache of the most recent full snapshot per symbol. Used by the
        # exit-loop so reason_invalidation can see funding/LSR/OI without
        # re-fetching them every few seconds. Updated by `_poll_loop`.
        self._last_full_snapshot: dict[str, Snapshot] = {}
        # Serializes _handle_entry_signals so the slow `_poll_loop` and the
        # fast `_liq_fast_loop` cannot both pass the cooldown check on the
        # same (symbol, direction) and double-fire an alert.
        self._entry_lock = asyncio.Lock()
        # Rolling per-(symbol, direction) rule-fire history for the
        # cross-snapshot confluence gate (funding-needs-confirmation, STRONG).
        self.confluence_cache = ConfluenceCache(
            window_minutes=cfg.signals.confluence_window_minutes,
        )

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        self.liq_stream.start()
        try:
            await asyncio.gather(
                self._poll_loop(),
                self._exit_loop(),
                self._liq_fast_loop(),
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
                # Per-symbol overrides resolution: oi_surge.window_minutes
                # may differ per symbol. Use the resolved value when fetching
                # the OI history window.
                sig = self.cfg.signals.for_symbol(symbol)
                try:
                    snap = await build_snapshot(
                        self.client,
                        self.liq_stream,
                        symbol,
                        oi_window_minutes=sig.oi_surge.window_minutes,
                    )
                except Exception as e:
                    log.warning("snapshot for %s failed: %s", symbol, e)
                    continue
                self._last_full_snapshot[symbol] = snap
                await self.logger.write_snapshot(snap)
                await self._handle_entry_signals(snap)
            await self._sleep(self.cfg.poll_interval_seconds)

    async def _liq_fast_loop(self) -> None:
        """Real-time liquidation-cascade detector.

        The main `_poll_loop` runs every `poll_interval_seconds` (60s by
        default). For funding / OI / LSR that is fine — those metrics refresh
        on Binance every 5min-8h. Liquidations, however, stream in real-time
        via the WS aggregator (`LiquidationStream`), so the 60s gate added up
        to 0-60s of artificial detection lag for the highest-velocity signal
        type.

        This loop reads the aggregator's in-memory rolling window every
        `liq_fast_check_interval_seconds` (5s default) — that part is a
        pure dict lookup, no network. When the rolling total for a symbol
        crosses the per-symbol `liq_cascade.usd_threshold` for at least one
        direction that is not already on cooldown, we do exactly the same
        `build_snapshot` + `_handle_entry_signals` that `_poll_loop` does,
        immediately. After a real fire, `mark_alerted` engages the
        `alert_cooldown_seconds` cooldown, which short-circuits subsequent
        iterations until the rolling window decays or cooldown expires.
        """
        while not self._stop.is_set():
            for symbol in self.cfg.symbols:
                sig = self.cfg.signals.for_symbol(symbol)
                if not sig.liq_cascade.enabled:
                    continue
                long_liq, short_liq = self.liq_stream.totals(symbol)
                thr = sig.liq_cascade.usd_threshold
                # Mirror engine.signals.evaluate: a long-side flush triggers
                # a LONG candidate (bounce), short-side a SHORT (squeeze).
                crossed: list[Direction] = []
                if long_liq >= thr:
                    crossed.append(Direction.LONG)
                if short_liq >= thr:
                    crossed.append(Direction.SHORT)
                if not crossed:
                    continue
                # Skip the snapshot fetch if every crossed direction is
                # already on cooldown — the entry handler would just
                # discard the candidate anyway, and `build_snapshot` is a
                # ~250ms REST roundtrip we don't want to repeat every 5s
                # while the 2h cooldown is ticking down.
                if all(
                    self.state.cooldown_remaining_seconds(
                        symbol, d, self.cfg.alert_cooldown_seconds
                    ) > 0
                    for d in crossed
                ):
                    continue
                try:
                    snap = await build_snapshot(
                        self.client,
                        self.liq_stream,
                        symbol,
                        oi_window_minutes=sig.oi_surge.window_minutes,
                    )
                except Exception as e:
                    log.warning("liq fast-path snapshot for %s failed: %s", symbol, e)
                    continue
                self._last_full_snapshot[symbol] = snap
                await self.logger.write_snapshot(snap)
                await self._handle_entry_signals(snap)
            await self._sleep(self.cfg.liq_fast_check_interval_seconds)

    async def _exit_loop(self) -> None:
        # Fetch a fresh price each cycle (cheap REST call) and combine it with
        # the metrics from the latest full poll-loop snapshot for this symbol.
        # Without the cached metrics, reason_invalidation for funding/LSR can
        # never fire (their checks require those fields on the snapshot).
        while not self._stop.is_set():
            for pos in list(self.state.open_positions()):
                try:
                    price = await self.client.latest_price(pos.symbol)
                except Exception as e:
                    log.warning("price for %s failed: %s", pos.symbol, e)
                    continue
                snap = self._build_exit_snapshot(pos.symbol, price)
                events = evaluate_exit(pos, snap, self.cfg)
                for ev in events:
                    await self._handle_exit_event(pos, ev, price)
            self.state.save()
            await self._sleep(self.cfg.exit_check_interval_seconds)

    def _build_exit_snapshot(self, symbol: str, price: float) -> Snapshot:
        """Compose a Snapshot for the exit-loop.

        Uses the most recent full snapshot's metrics (funding/LSR/OI/liq) so
        reason_invalidation in `evaluate_exit` can see them. Falls back to a
        price-only snapshot when no full snapshot has been recorded yet (e.g.
        right after process start).
        """
        now = datetime.now(tz=UTC)
        last = self._last_full_snapshot.get(symbol)
        if last is None:
            return Snapshot(symbol=symbol, ts=now, price=price)
        return Snapshot(
            symbol=symbol,
            ts=now,
            price=price,
            funding_rate=last.funding_rate,
            open_interest_usd=last.open_interest_usd,
            open_interest_change_pct_window=last.open_interest_change_pct_window,
            long_short_ratio=last.long_short_ratio,
            long_liquidations_usd_window=last.long_liquidations_usd_window,
            short_liquidations_usd_window=last.short_liquidations_usd_window,
            price_change_pct_1h=last.price_change_pct_1h,
            ema50_1h=last.ema50_1h,
            atr_1h=last.atr_1h,
        )

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
                stats = compute_stats(
                    positions, now=now, window_days=cfg.window_days, fees=cfg.fees,
                )
                per_symbol = compute_symbol_stats(
                    positions, now=now, window_days=cfg.window_days, fees=cfg.fees,
                )
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
                    fees=cfg.fees,
                )
                await self.notifier.send(text)
            except Exception as e:
                log.exception("stats digest failed: %s", e)
                continue
            self.state.last_stats_digest_week = week_key
            self.state.save()

    # ---------- entry / exit handling ----------

    async def _handle_entry_signals(self, snap: Snapshot) -> None:
        async with self._entry_lock:
            await self._handle_entry_signals_locked(snap)

    async def _handle_entry_signals_locked(self, snap: Snapshot) -> None:
        candidates = evaluate(snap, self.cfg, cache=self.confluence_cache)
        # Conflict policy: if rules fired in *both* directions on the same
        # snapshot, skip the whole symbol this tick. The OR-logic means the
        # market is sending mixed signals and the previous "open LONG first,
        # then block SHORT as opposite-side" behavior arbitrarily picked one.
        # Skipping is safer until we have a confluence score (Phase 2).
        if len({c.direction for c in candidates}) >= 2:
            log.info(
                "skipping %s — conflicting signals: %s",
                snap.symbol,
                " | ".join(
                    f"{c.direction.value}:{','.join(r.name for r in c.fired_rules)}"
                    for c in candidates
                ),
            )
            for c in candidates:
                await self.logger.write_blocked(
                    signal_id=c.signal_id,
                    symbol=c.symbol,
                    direction=c.direction,
                    blocked_reason="conflicting_signals",
                    fired_rules=[r.name for r in c.fired_rules],
                    confluence_window_rules=list(c.confluence_window_rules),
                    snapshot_ts=snap.ts,
                )
            return
        for candidate in candidates:
            blocked_reason = self._entry_blocked_reason(candidate)
            if blocked_reason is not None:
                self._maybe_log_skip(candidate, blocked_reason)
                await self.logger.write_blocked(
                    signal_id=candidate.signal_id,
                    symbol=candidate.symbol,
                    direction=candidate.direction,
                    blocked_reason=blocked_reason,
                    fired_rules=[r.name for r in candidate.fired_rules],
                    confluence_window_rules=list(candidate.confluence_window_rules),
                    snapshot_ts=snap.ts,
                )
                continue
            position = self.state.open_from_signal(candidate, self.cfg)
            self.state.mark_alerted(candidate.symbol, candidate.direction)
            self.state.save()
            alert = format_entry_alert(candidate, position, self.cfg)
            await self.notifier.send(alert.text)
            await self.logger.write_alert(alert)
            await self.logger.write_position(position)

    def _entry_blocked_reason(self, candidate) -> str | None:  # type: ignore[no-untyped-def]
        """Run the risk gates in order and return the first blocking reason.

        Reason tokens are stable, machine-readable strings (`cooldown`,
        `position_open`, `opposite_open`, `post_exit_cooldown`,
        `max_concurrent`, `max_per_direction_group`). They are written to
        `blocked.jsonl` and also drive the in-memory log-dedup keys.
        """
        cd = self.state.cooldown_remaining_seconds(
            candidate.symbol, candidate.direction, self.cfg.alert_cooldown_seconds
        )
        if cd > 0:
            return "cooldown"
        if self.state.open_for(candidate.symbol, candidate.direction) is not None:
            return "position_open"
        if self.state.open_for(candidate.symbol, candidate.direction.opposite) is not None:
            return "opposite_open"
        risk = self.cfg.risk
        post_exit = self.state.post_exit_cooldown_remaining_seconds(
            candidate.symbol, candidate.direction, risk.post_exit_cooldown_seconds
        )
        if post_exit > 0:
            return "post_exit_cooldown"
        open_now = self.state.open_positions()
        if len(open_now) >= risk.max_concurrent_positions:
            return "max_concurrent"
        # Per-direction cap is enforced only *within* a correlation group.
        # Symbols outside every group are unrestricted by this gate (e.g.
        # SOLUSDT can sit alongside a BTCUSDT LONG when the only group is
        # [BTCUSDT, ETHUSDT]).
        if risk.max_per_direction is not None:
            group = _correlation_group_for(candidate.symbol, risk.correlated_groups)
            if group is not None:
                same_dir_in_group = sum(
                    1
                    for p in open_now
                    if p.direction == candidate.direction and p.symbol in group
                )
                if same_dir_in_group >= risk.max_per_direction:
                    return "max_per_direction_group"
        return None

    def _maybe_log_skip(self, candidate, reason: str) -> None:  # type: ignore[no-untyped-def]
        """Emit a deduplicated INFO log for a blocked entry candidate.

        `cooldown` and `position_open` are noisy and stay at DEBUG — they
        repeat every poll cycle while the state persists and carry no new
        information. The other reasons go through StateStore.should_log_skip
        which suppresses repeats of the same (symbol, direction, reason)
        within `risk.skip_log_interval_seconds` (default 30 min).
        """
        if reason in ("cooldown", "position_open"):
            log.debug(
                "skip %s %s signal_id=%s reason=%s",
                candidate.symbol, candidate.direction.value,
                candidate.signal_id, reason,
            )
            return
        interval = self.cfg.risk.skip_log_interval_seconds
        if self.state.should_log_skip(
            candidate.symbol, candidate.direction, reason, interval,
        ):
            log.info(
                "skip %s %s signal_id=%s reason=%s rules=%s",
                candidate.symbol, candidate.direction.value,
                candidate.signal_id, reason,
                ",".join(r.name for r in candidate.fired_rules),
            )

    async def _handle_exit_event(self, position, ev, price: float) -> None:  # type: ignore[no-untyped-def]
        cfg = self.cfg
        if ev.kind == "TRAILING_MOVE":
            if ev.new_stop_loss_price is not None:
                position.stop_loss_price = ev.new_stop_loss_price
        elif ev.fraction_closed > 0:
            self.state.close_position(position, price, reason=ev.kind, fraction=ev.fraction_closed)
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
