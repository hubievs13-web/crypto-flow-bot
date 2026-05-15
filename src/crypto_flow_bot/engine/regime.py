"""1h market-regime helpers (observation-only in this PR)."""

from __future__ import annotations

from crypto_flow_bot.config import RegimeCfg


def _rma(values: list[float], period: int) -> list[float]:
    if not values or period <= 0:
        return []
    seed = sum(values[:period]) / period
    out = [seed]
    prev = seed
    alpha = 1.0 / period
    for v in values[period:]:
        prev = (1 - alpha) * prev + alpha * v
        out.append(prev)
    return out


def compute_adx(klines: list[list], period: int = 14) -> float | None:
    if period <= 0 or len(klines) < period * 2 + 2:
        return None
    closed = klines[:-1]
    if len(closed) < period * 2 + 1:
        return None
    try:
        highs = [float(k[2]) for k in closed]
        lows = [float(k[3]) for k in closed]
        closes = [float(k[4]) for k in closed]
    except (IndexError, TypeError, ValueError):
        return None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus = up if up > down and up > 0 else 0.0
        minus = down if down > up and down > 0 else 0.0
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
        plus_dm.append(plus)
        minus_dm.append(minus)
    if len(trs) < period:
        return None
    tr_rma = _rma(trs, period)
    plus_rma = _rma(plus_dm, period)
    minus_rma = _rma(minus_dm, period)
    if not tr_rma:
        return None
    dx: list[float] = []
    for trv, pdm, mdm in zip(tr_rma, plus_rma, minus_rma, strict=False):
        if trv <= 0:
            continue
        plus_di = 100.0 * pdm / trv
        minus_di = 100.0 * mdm / trv
        denom = plus_di + minus_di
        if denom <= 0:
            dx.append(0.0)
        else:
            dx.append(100.0 * abs(plus_di - minus_di) / denom)
    adx_series = _rma(dx, period)
    return adx_series[-1] if adx_series else None


def classify_regime(adx: float | None, atr_pct: float | None, cfg: RegimeCfg) -> str | None:
    if adx is None or atr_pct is None:
        return None
    # Strong trend needs both directional strength (ADX) and expansion (ATR%).
    if adx >= cfg.trend_adx_threshold and atr_pct >= cfg.trend_atr_pct_threshold:
        return "trend_strong"
    if adx >= cfg.trend_adx_threshold:
        return "trend_weak"
    # Low ADX + compressed ATR implies range; low ADX with larger ATR is chop.
    if adx < cfg.range_adx_threshold and atr_pct < cfg.range_atr_pct_threshold:
        return "range"
    if adx < cfg.range_adx_threshold:
        return "chop"
    return None
