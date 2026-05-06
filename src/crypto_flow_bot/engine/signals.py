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
        # Prefer cross-exchange aggregated values (Coinglass) when available;
        # fall back to Binance-only WS counters when not. The threshold also
        # adapts: aggregated numbers are typically 3-5x larger than Binance-only.
        using_aggregated = (
            snap.aggregated_long_liquidations_usd is not None
            and snap.aggregated_short_liquidations_usd is not None
        )
        if using_aggregated:
            long_liq = snap.aggregated_long_liquidations_usd or 0.0
            short_liq = snap.aggregated_short_liquidations_usd or 0.0
            thr = sig.liq_cascade.coinglass_aggregated_threshold
            tag = "aggregated"
        else:
            long_liq = snap.long_liquidations_usd_window
            short_liq = snap.short_liquidations_usd_window
            thr = sig.liq_cascade.usd_threshold
            tag = "binance"
        if long_liq >= thr:
            # longs were flushed -> often a bounce / squeeze up
            long_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"long liqs ${long_liq / 1e6:.0f}M ({tag}, flush)",
                )
            )
        if short_liq >= thr:
            short_rules.append(
                FiredRule(
                    name="liq_cascade",
                    description=f"short liqs ${short_liq / 1e6:.0f}M ({tag}, squeeze)",
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

    out: list[SignalCandidate] = []
    if long_rules:
        out.append(SignalCandidate(symbol=snap.symbol, direction=Direction.LONG, fired_rules=long_rules, snapshot=snap))
    if short_rules:
        out.append(SignalCandidate(symbol=snap.symbol, direction=Direction.SHORT, fired_rules=short_rules, snapshot=snap))
    return out
