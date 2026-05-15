"""YAML config loader with sensible defaults."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator


class FundingExtremeCfg(BaseModel):
    """Funding-rate extreme detection.

    Two modes:
        - "fixed": original behavior. Fires when funding crosses the
          `long_overheated_above` / `short_overheated_below` thresholds.
          Thresholds are necessarily calibrated against a single observation
          window and drift out of date as the regime shifts.
        - "auto": uses `FundingHistoryCache` to evaluate the current funding
          against the symbol's own recent distribution. Fires when EITHER
          the z-score (`zscore_lookback_days` window) crosses
          `zscore_high_abs`, OR the percentile rank (`pct_lookback_days`
          window) crosses `pct_high` / `pct_low`. Falls back to the fixed
          thresholds while the cache has fewer than `min_history_points`
          observations (cold start on a freshly-deployed symbol).

    The fixed thresholds are kept around as a fallback even in "auto" mode
    so the bot still emits alerts on the very first day after deploy.
    """

    enabled: bool = True
    long_overheated_above: float = 0.0008
    short_overheated_below: float = -0.0005

    # "auto" -- prefer percentile/z-score; "fixed" -- only thresholds.
    mode: str = "auto"

    # z-score knobs
    zscore_lookback_days: int = 14
    zscore_high_abs: float = 2.0  # |z| >= this fires

    # Percentile knobs (rank in [0, 1])
    pct_lookback_days: int = 30
    pct_high: float = 0.95  # rank >= 0.95 -> "longs overheated" (SHORT)
    pct_low: float = 0.05   # rank <= 0.05 -> "shorts overheated" (LONG)

    # Minimum stored observations before "auto" engages. 8h cycles, so
    # 20 ≈ 6.7 days of history -- enough to compute a meaningful mean/std.
    min_history_points: int = 20


class OiSurgeCfg(BaseModel):
    enabled: bool = True
    window_minutes: int = 60
    pct_change_threshold: float = 0.05
    require_price_aligned: bool = True
    require_healthy: bool = True
    quality_epsilon_pct: float = 0.0005


class TakerConfirmationCfg(BaseModel):
    enabled: bool = True
    dominance_threshold: float = 0.55
    cvd_window_bars: int = 6
    cvd_alignment_required: bool = False


class LsrExtremeCfg(BaseModel):
    enabled: bool = True
    long_heavy_above: float = 2.5
    short_heavy_below: float = 0.6


class LiqCascadeCfg(BaseModel):
    enabled: bool = True
    window_minutes: int = 5
    # Threshold for the rolling-window aggregated liquidation USD across
    # every enabled exchange. config.yaml typically overrides this; the
    # in-code default is a Binance-only baseline that keeps tests simple.
    usd_threshold: float = 50_000_000


class LiquidationsCfg(BaseModel):
    """Multi-exchange liquidation aggregator.

    Public WebSockets only — no API keys, no rate limits. Each exchange
    runs in its own task and reconnects independently with backoff, so a
    single exchange outage does not affect the others.
    """

    exchanges: list[str] = Field(default_factory=lambda: ["binance", "bybit"])


class TrendFilterCfg(BaseModel):
    """Trend/slope alignment gates for candidate strength."""

    enabled: bool = True
    ema_period: int = 50                 # EMA period for both 1h and 4h derivatives.
    require_alignment: bool = True       # Legacy 1h EMA-side gate.
    exempt_rules: list[str] = Field(default_factory=lambda: ["liq_cascade"])
    require_4h_alignment: bool = True
    require_1h_slope_alignment: bool = True
    require_4h_slope_alignment: bool = False
    slope_window_bars: int = 6
    slope_min_abs: float = 0.0005
    hard_block_on_4h: bool = False
    hard_block_on_slope: bool = False


class FreshnessCfg(BaseModel):
    """Stale-data protection. Drops rules whose underlying upstream metric
    is older than the configured threshold at evaluation time.

    Each Binance metric has its own natural refresh cadence:
        - funding rate (premiumIndex)     -- streamed, but the *value* changes
          slowly (every 8h funding cycle, with intra-cycle drift)
        - open interest (/fapi/v1/openInterest) -- updated continuously
        - top long/short ratio            -- 5-minute buckets

    The thresholds below are wall-clock ages relative to the snapshot's own
    `ts`. When a REST call fails and the snapshot carries the previous-cycle
    metric, the corresponding `*_ts` is older than the current poll and the
    matching rule is skipped. This is intentionally orthogonal to the
    `tenacity` retry layer in `BinanceClient`: retries reduce the probability
    of stale-leak, freshness gates handle the case where retries exhaust.

    Set `enabled: false` to disable the gate entirely (legacy behavior).
    Set an individual `*_max_age_seconds: 0` to allow any age for that metric.
    """

    enabled: bool = True
    # Funding and OI are real-time REST endpoints -- 2x poll interval is generous.
    funding_max_age_seconds: int = 120
    open_interest_max_age_seconds: int = 120
    # LSR buckets close every 5 minutes upstream; allow up to 10min before
    # we treat the value as stale.
    long_short_ratio_max_age_seconds: int = 600


class SymbolOverridesCfg(BaseModel):
    """Per-symbol threshold overrides. Any field set here replaces the global
    default *only for that symbol*. Fields left as None inherit the global.

    BTC/ETH/SOL have very different "normal" funding/LSR/OI/liq profiles, so a
    single threshold either spams alts or starves majors. Per-symbol blocks
    let you tune each pair independently.
    """

    funding_extreme: FundingExtremeCfg | None = None
    oi_surge: OiSurgeCfg | None = None
    lsr_extreme: LsrExtremeCfg | None = None
    liq_cascade: LiqCascadeCfg | None = None
    trend_filter: TrendFilterCfg | None = None


class SignalsCfg(BaseModel):
    funding_extreme: FundingExtremeCfg = Field(default_factory=FundingExtremeCfg)
    oi_surge: OiSurgeCfg = Field(default_factory=OiSurgeCfg)
    lsr_extreme: LsrExtremeCfg = Field(default_factory=LsrExtremeCfg)
    liq_cascade: LiqCascadeCfg = Field(default_factory=LiqCascadeCfg)
    taker_confirmation: TakerConfirmationCfg = Field(default_factory=TakerConfirmationCfg)
    trend_filter: TrendFilterCfg = Field(default_factory=TrendFilterCfg)
    freshness: FreshnessCfg = Field(default_factory=FreshnessCfg)

    # Confluence window (minutes): a rule that fired on any snapshot within
    # this many minutes back counts toward the confluence set for the
    # *current* candidate's (symbol, direction). 0 means snapshot-only
    # confluence (legacy behavior). Default 30 lets a slow trigger like
    # `funding_extreme` team up with a fast trigger like `liq_cascade` that
    # arrived a few minutes earlier.
    confluence_window_minutes: int = 30

    # When True, a snapshot that fires ONLY `funding_extreme` (with no other
    # rule in the confluence window) does NOT produce a candidate. Funding
    # alone showed net-negative PnL across 23 closed trades in our logs.
    # Set False to revert to funding-only entries (legacy behavior).
    funding_extreme_requires_confirmation: bool = True

    # Optional per-symbol overrides keyed by symbol (e.g. "BTCUSDT").
    per_symbol: dict[str, SymbolOverridesCfg] = Field(default_factory=dict)

    def for_symbol(self, symbol: str) -> SignalsCfg:
        """Return a SignalsCfg with per-symbol overrides applied.

        Returns self unchanged when no overrides are configured for `symbol`.
        Otherwise returns a shallow copy with each non-None override field
        replacing the matching global subconfig. Trend filter, per_symbol,
        and the confluence flags are not overrideable (no use case yet).
        """
        ov = self.per_symbol.get(symbol)
        if ov is None:
            return self
        return SignalsCfg(
            funding_extreme=ov.funding_extreme or self.funding_extreme,
            oi_surge=ov.oi_surge or self.oi_surge,
            lsr_extreme=ov.lsr_extreme or self.lsr_extreme,
            liq_cascade=ov.liq_cascade or self.liq_cascade,
            taker_confirmation=self.taker_confirmation,
            trend_filter=ov.trend_filter or self.trend_filter,
            freshness=self.freshness,
            confluence_window_minutes=self.confluence_window_minutes,
            funding_extreme_requires_confirmation=self.funding_extreme_requires_confirmation,
            per_symbol=self.per_symbol,
        )


class TpLevel(BaseModel):
    pct: float
    fraction: float


class TrailingCfg(BaseModel):
    enabled: bool = True
    # ATR-based activation: when set AND the position recorded an entry-time
    # ATR(1h), trailing activates once favorable excursion reaches
    # `activate_at_atr_mult * entry_atr / entry_price`. Set to None to use
    # only the fixed `activate_at_pct` below.
    activate_at_atr_mult: float | None = 1.0
    # Fallback fixed-pct activation. Used when ATR is unavailable at entry
    # time, or when `activate_at_atr_mult` is None.
    activate_at_pct: float = 0.015
    # >0 means lock in at least this much profit when trailing engages.
    # 0.0 = move SL to break-even only.
    lock_in_pct: float = 0.005


class ReasonInvalidationCfg(BaseModel):
    """Close a position once the original entry thesis has materially cooled.

    Both funding and LSR gates now use a *retracement* rule instead of a fixed
    absolute band: a position is invalidated once the metric has moved
    `retrace_pct` of the way back toward neutral (zero for funding, 1.0 for
    LSR) from the *entry-time* value. This adapts to per-symbol thresholds
    automatically — with the previous absolute band a position entered at
    funding=+0.003% would re-invalidate within seconds because |funding| was
    already inside the 0.02% normalization band at entry.

    Setting `funding_normalized_retrace_pct: 0.5` means: if entry funding
    was +0.010%, invalidate once funding drops to +0.005% or below (or flips
    sign entirely). 0.0 disables the gate; 1.0 requires a full return to zero.
    """

    enabled: bool = True

    # Fractional retracement back toward neutral required to invalidate.
    funding_normalized_retrace_pct: float = 0.5
    lsr_normalized_retrace_pct: float = 0.5

    # For point-in-time triggers (oi_surge, liq_cascade) there's no "metric
    # back to normal" gate — but we can still bail out at break-even if the
    # price has clearly reversed against the trade in the first window
    # minutes. This stops us bleeding into the time-stop on broken setups.
    momentum_reversal_pct: float = 0.005     # exit at BE if price moves this far against entry
    momentum_window_minutes: int = 60        # ...within this window after entry


class AtrSizingCfg(BaseModel):
    """Volatility-adaptive SL/TP sizing using 1h ATR.

    When enabled and ATR is available, SL/TP are placed N×ATR from entry instead
    of using the fixed `stop_loss_pct` / `take_profit_levels` percentages. This
    auto-adapts to each symbol's current volatility.
    """

    enabled: bool = True
    sl_atr_mult: float = 1.5             # SL distance = N × ATR(14, 1h)
    tp_atr_mults: list[float] = Field(
        default_factory=lambda: [1.5, 3.0]  # TP1, TP2 distances as ATR multiples
    )
    # When the entry candidate has 2+ rules in confluence (`is_strong`), the
    # final TP multiplier is replaced with this wider value to let the runner
    # capture more of the move. Earlier TPs stay the same (still secure profit).
    strong_last_tp_mult: float = 4.0
    fallback_to_pct: bool = True         # if ATR not available, use the % values below


class ExitsCfg(BaseModel):
    stop_loss_pct: float = 0.015
    take_profit_levels: list[TpLevel] = Field(
        default_factory=lambda: [TpLevel(pct=0.015, fraction=0.5), TpLevel(pct=0.030, fraction=0.5)]
    )
    trailing: TrailingCfg = Field(default_factory=TrailingCfg)
    time_stop_minutes: int = 480
    reason_invalidation: ReasonInvalidationCfg = Field(default_factory=ReasonInvalidationCfg)
    atr_sizing: AtrSizingCfg = Field(default_factory=AtrSizingCfg)

    @field_validator("take_profit_levels")
    @classmethod
    def _check_tp_fractions(cls, v: list[TpLevel]) -> list[TpLevel]:
        total = sum(level.fraction for level in v)
        if total - 1e-9 > 1.0:
            raise ValueError(f"take_profit_levels fractions sum to {total:.3f}, must be <= 1.0")
        return v


class RiskCfg(BaseModel):
    """Top-level risk-control limits applied before opening any new position.

    These are independent of signal strength — they only enforce dispersion
    and bound drawdown.
    """

    # Hard cap on simultaneously open virtual positions across all symbols.
    # Crypto majors are highly correlated, so 3 simultaneous LONGs on
    # BTC+ETH+SOL is effectively 3x exposure on the same beta.
    max_concurrent_positions: int = 2

    # Optional per-direction cap applied **only** within a correlation group
    # listed in `correlated_groups`. If set (e.g. 1) at most that many LONGs
    # (and that many SHORTs) may be open simultaneously *within the same
    # group*. Symbols outside every group are not affected. None disables.
    max_per_direction: int | None = None

    # Groups of highly correlated symbols. `max_per_direction` is enforced
    # per group, not globally — so e.g. with `[["BTCUSDT", "ETHUSDT"]]` and
    # `max_per_direction: 1`, you can hold at most one LONG between BTC and
    # ETH while SOLUSDT (outside any group) is unrestricted. Empty list
    # disables the per-group logic entirely (max_per_direction has no
    # effect when no group matches a candidate's symbol).
    correlated_groups: list[list[str]] = Field(default_factory=list)

    # Cooldown (seconds) after a virtual position fully closes on a given
    # (symbol, direction). Blocks an immediate re-entry on the same side
    # when the metric is still oscillating around its threshold. Observed
    # in logs as <30-minute re-entries that repeated the same losing setup.
    # 0 disables.
    post_exit_cooldown_seconds: int = 7200  # 2h

    # How often the entry path is allowed to emit the same skip-reason log
    # line for the same (symbol, direction). Stops `skipping ... at
    # max_concurrent_positions` from filling stdout every poll cycle when a
    # condition persists for hours. The reason key still updates whenever
    # the actual gate changes (e.g. cooldown -> max_concurrent), so state
    # transitions are visible immediately.
    skip_log_interval_seconds: int = 1800  # 30 min


class NotifierCfg(BaseModel):
    pretty_names: dict[str, str] = Field(default_factory=dict)
    send_startup_message: bool = True
    silent_when_idle: bool = True
    heartbeat_minutes: int = 60
    # Even when `silent_when_idle: true` suppresses the chatty heartbeat, we
    # still want a single liveness ping per day so a silent dead bot is
    # noticed quickly. Send at this UTC hour. Set to None to disable.
    daily_liveness_hour_utc: int | None = 8
    # How often to poll Telegram getUpdates for incoming /start commands.
    command_poll_interval_seconds: int = 5


class FeesCfg(BaseModel):
    """Per-fill cost model applied when computing realized PnL in the digest.

    A position incurs one fill on entry plus one fill per partial close (each
    TP level that hit + the final close on the remaining size), summing to two
    "full" fills' worth of cost. Both numbers below are unit fractions of
    notional (0.0005 = 0.05% = 5 bps).

    Defaults model Binance USD-M futures *taker* (0.05% commission) plus a
    conservative slippage estimate of ~2 bps per fill. Set `enabled: false` to
    revert to the previous fee-free PnL math.
    """

    enabled: bool = True
    commission_per_fill: float = 0.0005   # 0.05% per fill (taker on Binance USD-M)
    slippage_per_fill: float = 0.0002     # 0.02% adverse fill, conservative


class StatsCfg(BaseModel):
    """Weekly stats digest. Sends a summary of the last `window_days` of signals
    every week at the configured weekday/hour (UTC)."""

    enabled: bool = True
    weekday: int = 0       # 0=Mon, 1=Tue, ..., 6=Sun
    hour_utc: int = 12     # send at this hour (UTC)
    window_days: int = 7   # rolling window of positions to summarize
    fees: FeesCfg = Field(default_factory=FeesCfg)


class Config(BaseModel):
    symbols: list[str]
    poll_interval_seconds: int = 60
    exit_check_interval_seconds: int = 5
    # Real-time liquidation-cascade detector cadence. The main poll cycle
    # runs every `poll_interval_seconds` (60s) and is fine for funding /
    # open-interest / LSR data (those metrics only refresh every 5min-8h
    # upstream). Liquidations, however, stream in real-time over the WS
    # aggregator. To shorten the gap between a real cascade and the alert,
    # a dedicated short-interval loop polls the aggregator's in-memory
    # window every `liq_fast_check_interval_seconds` and fast-paths a
    # snapshot + alert when the per-symbol usd_threshold is crossed.
    liq_fast_check_interval_seconds: int = 5
    alert_cooldown_seconds: int = 1800
    signals: SignalsCfg = Field(default_factory=SignalsCfg)
    exits: ExitsCfg = Field(default_factory=ExitsCfg)
    notifier: NotifierCfg = Field(default_factory=NotifierCfg)
    liquidations: LiquidationsCfg = Field(default_factory=LiquidationsCfg)
    stats: StatsCfg = Field(default_factory=StatsCfg)
    risk: RiskCfg = Field(default_factory=RiskCfg)


def load_config(path: str | os.PathLike[str] | None = None) -> Config:
    """Load YAML config from `path` or from CRYPTO_FLOW_BOT_CONFIG / ./config.yaml."""
    if path is None:
        path = os.environ.get("CRYPTO_FLOW_BOT_CONFIG", "config.yaml")
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Config not found: {p.resolve()}")
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return Config.model_validate(raw)
