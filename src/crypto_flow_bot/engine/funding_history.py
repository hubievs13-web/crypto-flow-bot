"""Rolling per-symbol cache of historical funding rates.

Powers the `auto` mode of the `funding_extreme` rule: instead of hard-coded
thresholds calibrated against a single observation day, we compare the
current funding rate against the symbol's own recent distribution
(z-score over `zscore_lookback_days`, percentile over `pct_lookback_days`).

The cache is purely in-memory. At bot startup the poll loop populates it
with ~30 days of historical funding via `BinanceClient.funding_rate_history`,
and each subsequent snapshot pushes the freshly-fetched value with
`update()`. Restarting the bot drops the cache; the next startup re-backfills.

Funding cycles are 8h on Binance USD-M, so 30 days ≈ 90 points and 14 days
≈ 42 points — both comfortable sample sizes for the statistics we run.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from datetime import datetime, timedelta


class FundingHistoryCache:
    """Per-symbol bounded history of (timestamp, funding_rate) pairs.

    Internal storage is `deque(maxlen=max_points)` so the memory footprint
    is O(max_points × symbols) and old entries naturally evict. All public
    methods are pure functions of the stored data — there is no background
    task or persistence.

    Threading model: the bot is single-threaded asyncio, so we don't take a
    lock. If we ever go multi-threaded, wrap each public method in a Lock.
    """

    def __init__(self, max_points: int = 1000) -> None:
        # 1000 × 8h = ~333 days of headroom per symbol; backfill caps at 1000.
        self._max_points = max_points
        self._series: dict[str, deque[tuple[datetime, float]]] = defaultdict(
            lambda: deque(maxlen=max_points)
        )

    # ─── ingestion ─────────────────────────────────────────────────────────

    def backfill(self, symbol: str, points: list[tuple[datetime, float]]) -> int:
        """Replace the symbol's stored history with the given points.

        Called once per symbol at startup. We sort defensively (Binance
        returns oldest-first but we don't want to depend on that contract)
        and keep only the last `max_points` after sorting. Returns the
        count actually stored.
        """
        sorted_pts = sorted(points, key=lambda t: t[0])
        # Trim to capacity before assigning so the deque doesn't have to
        # absorb evictions one-by-one.
        if len(sorted_pts) > self._max_points:
            sorted_pts = sorted_pts[-self._max_points :]
        self._series[symbol] = deque(sorted_pts, maxlen=self._max_points)
        return len(sorted_pts)

    def update(self, symbol: str, ts: datetime, rate: float) -> None:
        """Append a single observation.

        Dedupes against the most recent entry: if `ts` is older than (or
        equal to) the last stored timestamp we drop it silently. This makes
        the cache robust to the bot polling faster than Binance updates the
        published funding rate (the same value re-arrives every 60s while
        the 8h cycle is open).
        """
        series = self._series[symbol]
        if series and ts <= series[-1][0]:
            return
        series.append((ts, rate))

    # ─── statistics ────────────────────────────────────────────────────────

    def points_within(self, symbol: str, now: datetime, days: int) -> list[float]:
        """Funding rates within the trailing `days`-window relative to `now`."""
        series = self._series.get(symbol)
        if not series:
            return []
        cutoff = now - timedelta(days=days)
        return [rate for ts, rate in series if ts >= cutoff]

    def zscore(
        self,
        symbol: str,
        value: float,
        now: datetime,
        lookback_days: int,
        min_points: int,
    ) -> float | None:
        """Standard-score of `value` against the trailing window.

        Returns None when:
            - the window contains fewer than `min_points` observations, OR
            - the window has zero variance (degenerate flat history).
        """
        window = self.points_within(symbol, now, lookback_days)
        if len(window) < min_points:
            return None
        mean = sum(window) / len(window)
        # Population variance is fine here -- we're not doing inferential
        # statistics, just normalizing a single observation.
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        if variance <= 0:
            return None
        return (value - mean) / math.sqrt(variance)

    def percentile_rank(
        self,
        symbol: str,
        value: float,
        now: datetime,
        lookback_days: int,
        min_points: int,
    ) -> float | None:
        """Percentile rank of `value` against the trailing window, in [0, 1].

        Uses the simple "fraction of stored values <= value" definition.
        Returns None when the window has fewer than `min_points` observations.
        """
        window = self.points_within(symbol, now, lookback_days)
        if len(window) < min_points:
            return None
        leq = sum(1 for x in window if x <= value)
        return leq / len(window)

    # ─── introspection (handy for tests + future Telegram diagnostics) ────

    def size(self, symbol: str) -> int:
        return len(self._series.get(symbol, ()))
