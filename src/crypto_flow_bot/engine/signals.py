"""Entry signal evaluation.

A `SignalCandidate` is produced when one or more rules fire on a snapshot.
Multiple rules for the same direction are merged into a single candidate
(carrying the list of rules fired *now* and the union of rules seen in a
short rolling window — the *confluence window*).

The confluence window lets a slow trigger (e.g. `funding_extreme`, which can
stay elevated for hours) team up with a faster trigger (e.g. `liq_cascade` or
`oi_surge`) that arrived a few minutes earlier. The signal engine is
otherwise stateless; the optional `ConfluenceCache` holds the rolling
per-(symbol, direction) history of rule fires for cross-snapshot confluence.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config, FreshnessCfg, FundingExtremeCfg
from crypto_flow_bot.engine.models import Direction, Snapshot

log = logging.getLogger(__name__)

# Rule names that are NOT allowed to open a position alone (no other rule in
# the confluence window). When `funding_extreme_requires_confirmation` is on
# in config, a candidate built only from these rules is dropped.
CONFIRMATION_REQUIRED_RULES: frozenset[str] = frozenset({"funding_extreme"})


def _metric_is_stale(
    *,
    snap_ts: datetime,
    metric_ts: datetime | None,
    max_age_seconds: int,
) -> bool:
    """Return True when the metric is older than `max_age_seconds` at snap_ts.

    `max_age_seconds <= 0` disables the gate for that metric (always fresh).
    A missing `metric_ts` is treated as "fresh" -- the rule's own None-check
    for the metric value already handles the not-yet-populated case.
    """
    if max_age_seconds <= 0 or metric_ts is None:
        return False
    age = (snap_ts - metric_ts).total_seconds()
    return age > max_age_seconds


def _evaluate_funding_extreme(
    snap: Snapshot,
    cfg: FundingExtremeCfg,
) -> tuple[Direction, FiredRule] | None:
    """Decide whether `funding_extreme` fires on this snapshot.

    Two-mode logic:
        - "fixed": the original behavior. Cross the static threshold and fire.
        - "auto": prefer the per-symbol distributional view. The snapshot
          carries `funding_rate_zscore` / `funding_rate_percentile` already
          computed by the bot's poll loop (via FundingHistoryCache). When at
          least one of those is populated we honor them and ignore the static
          thresholds. When the cache hasn't yet collected `min_history_points`
          observations both fields are None and we fall back to the fixed
          thresholds -- the bot keeps emitting alerts even on day one after
          a fresh deploy.

    Returns:
        (Direction, FiredRule) when the rule fires, None otherwise.
    """
    if snap.funding_rate is None:
        return None
    f = snap.funding_rate

    if cfg.mode == "auto":
        z = snap.funding_rate_zscore
        p = snap.funding_rate_percentile
        if z is not None or p is not None:
            # SHORT side: longs are overheated (high positive funding).
            short_hit: list[str] = []
            if z is not None and z >= cfg.zscore_high_abs:
                short_hit.append(f"z {z:+.2f}")
            if p is not None and p >= cfg.pct_high:
                short_hit.append(f"p{p * 100:.0f} / {cfg.pct_lookback_days}d")
            if short_hit:
                desc = (
                    f"funding {f * 100:+.3f}% extreme ({', '.join(short_hit)})"
                )
                return Direction.SHORT, FiredRule(name="funding_extreme", description=desc)

            # LONG side: shorts are overheated (deeply negative funding).
            long_hit: list[str] = []
            if z is not None and z <= -cfg.zscore_high_abs:
                long_hit.append(f"z {z:+.2f}")
            if p is not None and p <= cfg.pct_low:
                long_hit.append(f"p{p * 100:.0f} / {cfg.pct_lookback_days}d")
            if long_hit:
                desc = (
                    f"funding {f * 100:+.3f}% extreme ({', '.join(long_hit)})"
                )
                return Direction.LONG, FiredRule(name="funding_extreme", description=desc)

            # Auto path computed stats and they didn't cross -- do NOT
            # silently fall back to fixed thresholds. That would let the
            # rule fire when the percentile rank says "normal".
            return None

        # Auto mode but cache cold -- fall through to fixed-threshold logic.

    # Fixed-threshold path (legacy behavior; also the cold-start fallback).
    if f >= cfg.long_overheated_above:
        return Direction.SHORT, FiredRule(
            name="funding_extreme",
            description=f"funding {f * 100:+.3f}% (longs overheated)",
        )
    if f <= cfg.short_overheated_below:
        return Direction.LONG, FiredRule(
            name="funding_extreme",
            description=f"funding {f * 100:+.3f}% (shorts overheated)",
        )
    return None


def _check_freshness(
    snap: Snapshot,
    fresh: FreshnessCfg,
) -> dict[str, bool]:
    """Compute per-rule freshness verdicts. True = stale (skip the rule)."""
    if not fresh.enabled:
        return {"funding_extreme": False, "oi_surge": False, "lsr_extreme": False}
    return {
        "funding_extreme": _metric_is_stale(
            snap_ts=snap.ts,
            metric_ts=snap.funding_rate_ts,
            max_age_seconds=fresh.funding_max_age_seconds,
        ),
        "oi_surge": _metric_is_stale(
            snap_ts=snap.ts,
            metric_ts=snap.open_interest_ts,
            max_age_seconds=fresh.open_interest_max_age_seconds,
        ),
        "lsr_extreme": _metric_is_stale(
            snap_ts=snap.ts,
            metric_ts=snap.long_short_ratio_ts,
            max_age_seconds=fresh.long_short_ratio_max_age_seconds,
        ),
    }


@dataclass
class FiredRule:
    name: str
    description: str  # human-readable, e.g. "funding +0.13%"


class ConfluenceCache:
    """Bounded rolling history of rule fires per (symbol, direction).

    Holds entries no older than `window_minutes`. Used by `evaluate` to
    decide whether a funding-only snapshot has a confirming rule in the
    recent past (entry allowed) or stands alone (entry blocked when the
    funding-confirmation gate is on).

    Lives in memory only — restart wipes it, which is acceptable: the
    cache catches up on the next snapshot.
    """

    def __init__(self, window_minutes: int) -> None:
        self.window_minutes = window_minutes
        self._fires: dict[tuple[str, Direction], deque[tuple[datetime, str]]] = defaultdict(deque)

    def _evict_old(self, key: tuple[str, Direction], now: datetime) -> None:
        if self.window_minutes <= 0:
            return
        cutoff = now - timedelta(minutes=self.window_minutes)
        q = self._fires[key]
        while q and q[0][0] < cutoff:
            q.popleft()

    def record(self, symbol: str, direction: Direction, rule_name: str, now: datetime) -> None:
        key = (symbol, direction)
        self._fires[key].append((now, rule_name))
        self._evict_old(key, now)

    def recent_names(self, symbol: str, direction: Direction, now: datetime) -> set[str]:
        key = (symbol, direction)
        self._evict_old(key, now)
        return {name for _, name in self._fires.get(key, ())}


@dataclass
class SignalCandidate:
    symbol: str
    direction: Direction
    fired_rules: list[FiredRule]  # rules fired on the *current* snapshot
    snapshot: Snapshot
    # Union of rule names fired on this snapshot AND any prior snapshot
    # within the configured confluence window for this (symbol, direction).
    # Always at least equal to {r.name for r in fired_rules}.
    confluence_window_rules: set[str] = field(default_factory=set)
    # Stable id for this candidate. Same id rides through the entry alert
    # / blocked-event log / position lifecycle so rows in different jsonl
    # files can be joined.
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    strong_override: bool | None = None

    @property
    def reason_label(self) -> str:
        return "+".join(r.name for r in self.fired_rules)

    @property
    def is_strong(self) -> bool:
        """Confluence: 2+ distinct *non-funding* rules fired in the same
        direction within the confluence window.

        Why exclude funding_extreme: funding is a slow context indicator
        (active for hours/days) — pairing it with one fast trigger is the
        normal entry condition, not a confluence bonus. STRONG should mark
        genuinely rare moments when two *fast* triggers agree.
        """
        if self.strong_override is not None:
            return self.strong_override
        non_funding = self.confluence_window_rules - CONFIRMATION_REQUIRED_RULES
        return len(non_funding) >= 2


def evaluate(
    snap: Snapshot,
    cfg: Config,
    cache: ConfluenceCache | None = None,
    now: datetime | None = None,
) -> list[SignalCandidate]:
    """Return zero or more entry candidates for this snapshot.

    At most one candidate per direction. When `cache` is provided, it is
    updated with the rules fired on this snapshot and used to compute the
    confluence-window set on returned candidates. When `cache` is None,
    confluence is snapshot-only (legacy behavior).
    """
    long_rules: list[FiredRule] = []
    short_rules: list[FiredRule] = []

    # Per-symbol override resolution: BTC/ETH/SOL want different thresholds
    # (a single global value either spams alts or starves majors).
    sig = cfg.signals.for_symbol(snap.symbol)

    # Per-metric freshness verdicts. A stale metric short-circuits the
    # matching rule below regardless of the underlying value.
    stale = _check_freshness(snap, sig.freshness)
    if any(stale.values()):
        stale_names = sorted(k for k, v in stale.items() if v)
        log.info(
            "freshness gate: skipping stale rules for %s: %s",
            snap.symbol,
            ",".join(stale_names),
        )

    if sig.funding_extreme.enabled and snap.funding_rate is not None and not stale["funding_extreme"]:
        funding_fired = _evaluate_funding_extreme(snap, sig.funding_extreme)
        if funding_fired is not None:
            direction, rule = funding_fired
            if direction is Direction.SHORT:
                short_rules.append(rule)
            else:
                long_rules.append(rule)

    if (
        sig.oi_surge.enabled
        and snap.open_interest_change_pct_window is not None
        and abs(snap.open_interest_change_pct_window) >= sig.oi_surge.pct_change_threshold
        and not stale["oi_surge"]
    ):
        oi_pct = snap.open_interest_change_pct_window
        # OI direction alone is ambiguous (longs and shorts both grow OI).
        # When require_price_aligned is True (default) we cross-check with the
        # 1h price-change to identify *which* side opened the new positions:
        #   OI ↑ + price ↑ -> fresh longs  -> LONG
        #   OI ↑ + price ↓ -> fresh shorts -> SHORT
        #   OI ↓ + price ↑ -> short squeeze (skip — already in motion)
        #   OI ↓ + price ↓ -> long capitulation (skip — too late)
        # Without alignment requirement we fall back to OI sign alone (noisy).
        if sig.oi_surge.require_price_aligned:
            quality = snap.oi_quality
            price_pct = snap.price_change_pct_1h
            if sig.oi_surge.require_healthy and (
                quality is None or quality.startswith("dangerous_")
            ):
                log.info("oi_surge: skipping %s due to oi_quality=%s", snap.symbol, quality)
            elif quality == "healthy_short" and price_pct is not None:
                short_rules.append(
                    FiredRule(
                        name="oi_surge",
                        description=f"OI +{oi_pct * 100:.1f}% + price {price_pct * 100:+.2f}% / 1h (fresh longs)",
                    )
                )
            elif quality == "healthy_long" and price_pct is not None:
                long_rules.append(
                    FiredRule(
                        name="oi_surge",
                        description=f"OI +{oi_pct * 100:.1f}% + price {price_pct * 100:+.2f}% / 1h (fresh shorts)",
                    )
                )
            elif (
                not sig.oi_surge.require_healthy
                and price_pct is not None
                and oi_pct > 0
            ):
                if price_pct > 0:
                    long_rules.append(
                        FiredRule(
                            name="oi_surge",
                            description=f"OI +{oi_pct * 100:.1f}% + price {price_pct * 100:+.2f}% / 1h (fresh longs)",
                        )
                    )
                elif price_pct < 0:
                    short_rules.append(
                        FiredRule(
                            name="oi_surge",
                            description=f"OI +{oi_pct * 100:.1f}% + price {price_pct * 100:+.2f}% / 1h (fresh shorts)",
                        )
                    )
        else:
            if oi_pct > 0:
                long_rules.append(
                    FiredRule(name="oi_surge", description=f"OI +{oi_pct * 100:.1f}% / window (fresh longs)")
                )
            else:
                short_rules.append(
                    FiredRule(name="oi_surge", description=f"OI {oi_pct * 100:.1f}% / window (fresh shorts)")
                )

    if sig.lsr_extreme.enabled and snap.long_short_ratio is not None and not stale["lsr_extreme"]:
        lsr = snap.long_short_ratio
        if lsr >= sig.lsr_extreme.long_heavy_above:
            short_rules.append(
                FiredRule(name="lsr_extreme", description=f"top-trader L/S {lsr:.2f} (longs crowded)")
            )
        elif lsr <= sig.lsr_extreme.short_heavy_below:
            long_rules.append(
                FiredRule(name="lsr_extreme", description=f"top-trader L/S {lsr:.2f} (shorts crowded)")
            )

    if sig.liq_cascade.enabled:
        # Counters are already aggregated across every enabled exchange.
        long_liq = snap.long_liquidations_usd_window
        short_liq = snap.short_liquidations_usd_window
        thr = sig.liq_cascade.usd_threshold
        if long_liq >= thr:
            # longs were flushed -> often a bounce / squeeze up
            long_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"long liqs ${long_liq / 1e6:.0f}M (flush)",
                )
            )
        if short_liq >= thr:
            short_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"short liqs ${short_liq / 1e6:.0f}M (squeeze)",
                )
            )

    if now is None:
        now = snap.ts if snap.ts is not None else datetime.now(tz=UTC)

    out: list[SignalCandidate] = []
    for direction, rules in (
        (Direction.LONG, long_rules),
        (Direction.SHORT, short_rules),
    ):
        if not rules:
            continue

        # Update the rolling confluence cache with rules fired on this snap.
        if cache is not None:
            for r in rules:
                cache.record(snap.symbol, direction, r.name, now)

        # Window-wide rule-name set always includes the rules fired now.
        window_names: set[str] = {r.name for r in rules}
        if cache is not None:
            window_names |= cache.recent_names(snap.symbol, direction, now)

        # Funding-needs-confirmation gate: a candidate built only from rules
        # in CONFIRMATION_REQUIRED_RULES (e.g. funding_extreme alone) is
        # dropped unless another rule has fired in the same direction within
        # the confluence window.
        if sig.funding_extreme_requires_confirmation:
            confirming = window_names - CONFIRMATION_REQUIRED_RULES
            if not confirming:
                # Every rule in window needs a confirming partner; we don't
                # have one yet. Drop the candidate.
                continue

        out.append(
            SignalCandidate(
                symbol=snap.symbol,
                direction=direction,
                fired_rules=rules,
                snapshot=snap,
                confluence_window_rules=window_names,
            )
        )
    # ── taker confirmation gate (PR 3, #12-#14) ─────────────────────────
    # Downgrades is_strong on a candidate whose side does not have taker
    # aggression confirmation on the last closed 1h bar. Cold-start
    # (taker_buy_dominance_1h is None) is a pass-through. liq_cascade is
    # exempt -- a cascade IS aggression by definition.
    if sig.taker_confirmation.enabled:
        for cand in out:
            if any(r.name == "liq_cascade" for r in cand.fired_rules):
                continue
            buy_dominance = cand.snapshot.taker_buy_dominance_1h
            if buy_dominance is None:
                continue
            side_dominance = buy_dominance if cand.direction is Direction.LONG else 1.0 - buy_dominance
            if side_dominance >= sig.taker_confirmation.dominance_threshold:
                continue
            cand.strong_override = False
            cand.fired_rules.append(
                FiredRule(
                    name="taker_confirmation",
                    description=f"taker n/c ({side_dominance * 100:.1f}%)",
                )
            )

    # ── trend / slope alignment gate (PR 4, #5, #6) ─────────────────────
    # 4h trend, 1h slope and 4h slope each downgrade is_strong on a
    # contradicting candidate, and (when their hard_block_* knob is on)
    # drop the candidate entirely. Rules listed in tf.exempt_rules
    # (e.g. liq_cascade) are skipped. Missing data is always pass-through.
    tf = sig.trend_filter
    if not tf.enabled:
        return out
    kept: list[SignalCandidate] = []
    for cand in out:
        if any(r.name in tf.exempt_rules for r in cand.fired_rules):
            kept.append(cand)
            continue
        drop = False
        if tf.require_4h_alignment and cand.snapshot.ema50_4h is not None:
            miss = (
                cand.direction is Direction.LONG and cand.snapshot.price <= cand.snapshot.ema50_4h
            ) or (
                cand.direction is Direction.SHORT and cand.snapshot.price >= cand.snapshot.ema50_4h
            )
            if miss:
                cand.strong_override = False
                cand.fired_rules.append(FiredRule(name="trend_4h", description="trend_4h n/a"))
                if tf.hard_block_on_4h:
                    drop = True
        if tf.require_1h_slope_alignment and cand.snapshot.ema50_slope_1h is not None:
            s1 = cand.snapshot.ema50_slope_1h
            if abs(s1) >= tf.slope_min_abs:
                miss = (cand.direction is Direction.LONG and s1 <= 0) or (
                    cand.direction is Direction.SHORT and s1 >= 0
                )
                if miss:
                    cand.strong_override = False
                    cand.fired_rules.append(
                        FiredRule(
                            name="slope_1h",
                            description=f"slope_1h n/a ({s1 * 100:+.2f}%/{tf.slope_window_bars}h)",
                        )
                    )
                    if tf.hard_block_on_slope:
                        drop = True
        if tf.require_4h_slope_alignment and cand.snapshot.ema50_slope_4h is not None:
            s4 = cand.snapshot.ema50_slope_4h
            if abs(s4) >= tf.slope_min_abs:
                miss = (cand.direction is Direction.LONG and s4 <= 0) or (
                    cand.direction is Direction.SHORT and s4 >= 0
                )
                if miss:
                    cand.strong_override = False
                    cand.fired_rules.append(
                        FiredRule(
                            name="slope_4h",
                            description=f"slope_4h n/a ({s4 * 100:+.2f}%/{tf.slope_window_bars * 4}h)",
                        )
                    )
                    if tf.hard_block_on_slope:
                        drop = True
        if not drop:
            kept.append(cand)
    return kept
