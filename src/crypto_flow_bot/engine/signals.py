"""Entry signal evaluation.

A `SignalCandidate` is produced when one or more rules fire on a snapshot.
Multiple rules for the same direction are merged into a single candidate
(carrying a list of fired rule names so the alert text can mention all of them).
"""

from __future__ import annotations

from dataclasses import dataclass

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot


@dataclass
class FiredRule:
    name: str
    description: str  # human-readable, e.g. "funding +0.13%"


@dataclass
class SignalCandidate:
    symbol: str
    direction: Direction
    fired_rules: list[FiredRule]
    snapshot: Snapshot

    @property
    def reason_label(self) -> str:
        return "+".join(r.name for r in self.fired_rules)


def evaluate(snap: Snapshot, cfg: Config) -> list[SignalCandidate]:
    """Return zero or more entry candidates for this snapshot. One per direction at most."""
    long_rules: list[FiredRule] = []
    short_rules: list[FiredRule] = []

    sig = cfg.signals

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
        # We don't have a separate price-change window — use OI-direction as the
        # primary tell. If require_price_aligned is False, fire purely on OI;
        # otherwise we use OI direction itself (positive = fresh longs, negative
        # = fresh shorts) — this captures "fresh flow" without needing a
        # second price-change series.
        if oi_pct > 0:
            # Fresh longs entering: this is *trend confirmation*, not a contrarian signal.
            # Without price-change confirmation we treat it as a mild long bias.
            if not sig.oi_surge.require_price_aligned:
                long_rules.append(
                    FiredRule(name="oi_surge", description=f"OI +{oi_pct * 100:.1f}% / window (fresh longs)")
                )
        else:
            if not sig.oi_surge.require_price_aligned:
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
        thr = sig.liq_cascade.usd_threshold
        if snap.long_liquidations_usd_window >= thr:
            # longs were flushed -> often a bounce / squeeze up
            long_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"long liqs ${snap.long_liquidations_usd_window / 1e6:.1f}M (flush)",
                )
            )
        if snap.short_liquidations_usd_window >= thr:
            short_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"short liqs ${snap.short_liquidations_usd_window / 1e6:.1f}M (squeeze)",
                )
            )

    out: list[SignalCandidate] = []
    if long_rules:
        out.append(SignalCandidate(symbol=snap.symbol, direction=Direction.LONG, fired_rules=long_rules, snapshot=snap))
    if short_rules:
        out.append(SignalCandidate(symbol=snap.symbol, direction=Direction.SHORT, fired_rules=short_rules, snapshot=snap))
    return out
