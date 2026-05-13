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

import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot

# Rule names that are NOT allowed to open a position alone (no other rule in
# the confluence window). When `funding_extreme_requires_confirmation` is on
# in config, a candidate built only from these rules is dropped.
CONFIRMATION_REQUIRED_RULES: frozenset[str] = frozenset({"funding_extreme"})


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

    if sig.funding_extreme.enabled and snap.funding_rate is not None:
        f = snap.funding_rate
        if f >= sig.funding_extreme.long_overheated_above:
            short_rules.append(
                FiredRule(name="funding_extreme", description=f"funding {f * 100:+.3f}% (longs overheated)")
            )
        elif f <= sig.funding_extreme.short_overheated_below:
            long_rules.append(
                FiredRule(name="funding_extreme", description=f"funding {f * 100:+.3f}% (shorts overheated)")
            )

    if (
        sig.oi_surge.enabled
        and snap.open_interest_change_pct_window is not None
        and abs(snap.open_interest_change_pct_window) >= sig.oi_surge.pct_change_threshold
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
            price_pct = snap.price_change_pct_1h
            if price_pct is not None and oi_pct > 0:
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

    if sig.lsr_extreme.enabled and snap.long_short_ratio is not None:
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

    # 1h EMA trend filter: drop counter-trend signals when the larger trend is clear.
    # Liquidation cascades against the trend often *are* the highest-EV setups
    # (long flush in uptrend, short squeeze in downtrend), so we exempt them.
    tf = sig.trend_filter
    if tf.enabled and tf.require_alignment and snap.ema50_1h is not None:
        if snap.price > snap.ema50_1h:
            short_rules = [r for r in short_rules if r.name == "liq_cascade"]
        elif snap.price < snap.ema50_1h:
            long_rules = [r for r in long_rules if r.name == "liq_cascade"]

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
    return out
