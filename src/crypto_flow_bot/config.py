"""YAML config loader with sensible defaults."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator


class FundingExtremeCfg(BaseModel):
    enabled: bool = True
    long_overheated_above: float = 0.0008
    short_overheated_below: float = -0.0005


class OiSurgeCfg(BaseModel):
    enabled: bool = True
    window_minutes: int = 60
    pct_change_threshold: float = 0.05
    require_price_aligned: bool = True


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
    """1h EMA-based trend filter. Blocks signals against the larger trend."""

    enabled: bool = True
    ema_period: int = 50         # EMA on 1h closes (we always pull 50 bars)
    require_alignment: bool = True  # if true, drop SHORT signals above EMA / LONG signals below


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


class SignalsCfg(BaseModel):
    funding_extreme: FundingExtremeCfg = Field(default_factory=FundingExtremeCfg)
    oi_surge: OiSurgeCfg = Field(default_factory=OiSurgeCfg)
    lsr_extreme: LsrExtremeCfg = Field(default_factory=LsrExtremeCfg)
    liq_cascade: LiqCascadeCfg = Field(default_factory=LiqCascadeCfg)
    trend_filter: TrendFilterCfg = Field(default_factory=TrendFilterCfg)
    # Optional per-symbol overrides keyed by symbol (e.g. "BTCUSDT").
    per_symbol: dict[str, SymbolOverridesCfg] = Field(default_factory=dict)

    def for_symbol(self, symbol: str) -> SignalsCfg:
        """Return a SignalsCfg with per-symbol overrides applied.

        Returns self unchanged when no overrides are configured for `symbol`.
        Otherwise returns a shallow copy with each non-None override field
        replacing the matching global subconfig. Trend filter and per_symbol
        themselves are not overrideable (no use case yet).
        """
        ov = self.per_symbol.get(symbol)
        if ov is None:
            return self
        return SignalsCfg(
            funding_extreme=ov.funding_extreme or self.funding_extreme,
            oi_surge=ov.oi_surge or self.oi_surge,
            lsr_extreme=ov.lsr_extreme or self.lsr_extreme,
            liq_cascade=ov.liq_cascade or self.liq_cascade,
            trend_filter=self.trend_filter,
            per_symbol=self.per_symbol,
        )


class TpLevel(BaseModel):
    pct: float
    fraction: float


class TrailingCfg(BaseModel):
    enabled: bool = True
    activate_at_pct: float = 0.015
    # >0 means lock in at least this much profit when trailing engages.
    # 0.0 = move SL to break-even only.
    lock_in_pct: float = 0.005


class ReasonInvalidationCfg(BaseModel):
    enabled: bool = True
    funding_normalized_below_abs: float = 0.0002
    lsr_normalized_band: tuple[float, float] = (0.85, 1.15)
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
    time_stop_minutes: int = 240
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

    # Optional per-direction cap. If set (e.g. 1) at most that many LONGs and
    # at most that many SHORTs may be open at the same time. None disables.
    max_per_direction: int | None = None

    # Daily loss circuit breaker: after this many SL_HIT exits in the current
    # UTC day, refuse to open new positions until UTC midnight. Set to None
    # to disable.
    max_daily_losses: int | None = 3


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


class StatsCfg(BaseModel):
    """Weekly stats digest. Sends a summary of the last `window_days` of signals
    every week at the configured weekday/hour (UTC)."""

    enabled: bool = True
    weekday: int = 0       # 0=Mon, 1=Tue, ..., 6=Sun
    hour_utc: int = 12     # send at this hour (UTC)
    window_days: int = 7   # rolling window of positions to summarize


class Config(BaseModel):
    symbols: list[str]
    poll_interval_seconds: int = 60
    exit_check_interval_seconds: int = 5
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
