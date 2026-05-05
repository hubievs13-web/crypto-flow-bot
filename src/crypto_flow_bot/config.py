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
    usd_threshold: float = 50_000_000


class SignalsCfg(BaseModel):
    funding_extreme: FundingExtremeCfg = Field(default_factory=FundingExtremeCfg)
    oi_surge: OiSurgeCfg = Field(default_factory=OiSurgeCfg)
    lsr_extreme: LsrExtremeCfg = Field(default_factory=LsrExtremeCfg)
    liq_cascade: LiqCascadeCfg = Field(default_factory=LiqCascadeCfg)


class TpLevel(BaseModel):
    pct: float
    fraction: float


class TrailingCfg(BaseModel):
    enabled: bool = True
    activate_at_pct: float = 0.015
    lock_in_pct: float = 0.0


class ReasonInvalidationCfg(BaseModel):
    enabled: bool = True
    funding_normalized_below_abs: float = 0.0002
    lsr_normalized_band: tuple[float, float] = (0.85, 1.15)


class ExitsCfg(BaseModel):
    stop_loss_pct: float = 0.015
    take_profit_levels: list[TpLevel] = Field(
        default_factory=lambda: [TpLevel(pct=0.015, fraction=0.5), TpLevel(pct=0.030, fraction=0.5)]
    )
    trailing: TrailingCfg = Field(default_factory=TrailingCfg)
    time_stop_minutes: int = 240
    reason_invalidation: ReasonInvalidationCfg = Field(default_factory=ReasonInvalidationCfg)

    @field_validator("take_profit_levels")
    @classmethod
    def _check_tp_fractions(cls, v: list[TpLevel]) -> list[TpLevel]:
        total = sum(level.fraction for level in v)
        if total - 1e-9 > 1.0:
            raise ValueError(f"take_profit_levels fractions sum to {total:.3f}, must be <= 1.0")
        return v


class NotifierCfg(BaseModel):
    pretty_names: dict[str, str] = Field(default_factory=dict)
    silent_when_idle: bool = True
    heartbeat_minutes: int = 60


class Config(BaseModel):
    symbols: list[str]
    poll_interval_seconds: int = 60
    exit_check_interval_seconds: int = 5
    alert_cooldown_seconds: int = 1800
    signals: SignalsCfg = Field(default_factory=SignalsCfg)
    exits: ExitsCfg = Field(default_factory=ExitsCfg)
    notifier: NotifierCfg = Field(default_factory=NotifierCfg)


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
