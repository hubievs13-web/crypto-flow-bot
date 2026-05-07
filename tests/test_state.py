"""Tests for state persistence — ensures new fields round-trip through save/load."""

from datetime import UTC, datetime

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore


def _cfg() -> Config:
    return Config(symbols=["BTCUSDT"])


def _snap(price: float = 100.0, atr: float | None = 1.0) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price, atr_1h=atr)


def test_last_liveness_ping_date_round_trips(tmp_path):
    """Daily liveness state field must persist across restarts."""
    store = StateStore(path=tmp_path)
    assert store.last_liveness_ping_date is None
    store.last_liveness_ping_date = "2025-11-04"
    store.save()
    fresh = StateStore(path=tmp_path)
    assert fresh.last_liveness_ping_date == "2025-11-04"


def test_position_strong_flag_round_trips(tmp_path):
    """STRONG (confluence) flag must persist across restarts."""
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[
            FiredRule(name="funding_extreme", description="x"),
            FiredRule(name="lsr_extreme", description="y"),
        ],
        snapshot=_snap(),
    )
    pos = store.open_from_signal(candidate, cfg)
    assert pos.strong is True
    store.save()
    fresh = StateStore(path=tmp_path)
    reloaded = list(fresh.positions.values())[0]
    assert reloaded.strong is True


def test_single_rule_position_not_strong(tmp_path):
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="funding_extreme", description="x")],
        snapshot=_snap(),
    )
    pos = store.open_from_signal(candidate, cfg)
    assert pos.strong is False


# ─── Risk: daily-loss circuit breaker (item #2) ─────────────────────────────

from datetime import timedelta  # noqa: E402

from crypto_flow_bot.config import RiskCfg  # noqa: E402


def test_record_loss_if_today_ignores_non_sl_events(tmp_path):
    s = StateStore(path=tmp_path)
    s.record_loss_if_today("TP_HIT")
    s.record_loss_if_today("TIME_STOP")
    s.record_loss_if_today("REASON_INVALIDATED")
    assert s.losses_today_count == 0


def test_record_loss_if_today_counts_sl_hit(tmp_path):
    s = StateStore(path=tmp_path)
    s.record_loss_if_today("SL_HIT")
    s.record_loss_if_today("SL_HIT")
    assert s.losses_today_count == 2
    assert s.losses_today_date is not None


def test_loss_counter_resets_on_new_utc_day(tmp_path):
    s = StateStore(path=tmp_path)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    s.record_loss_if_today("SL_HIT", now=yesterday)
    s.record_loss_if_today("SL_HIT", now=yesterday)
    assert s.losses_today_count == 2
    # New UTC day -> first SL resets the counter to 1.
    s.record_loss_if_today("SL_HIT", now=datetime.now(tz=UTC))
    assert s.losses_today_count == 1


def test_daily_loss_cap_reached(tmp_path):
    s = StateStore(path=tmp_path)
    cfg = Config(symbols=["BTCUSDT"], risk=RiskCfg(max_daily_losses=3))
    assert s.daily_loss_cap_reached(cfg) is False
    for _ in range(2):
        s.record_loss_if_today("SL_HIT")
    assert s.daily_loss_cap_reached(cfg) is False  # only 2 < 3
    s.record_loss_if_today("SL_HIT")
    assert s.daily_loss_cap_reached(cfg) is True   # 3 >= 3


def test_daily_loss_cap_disabled_when_none(tmp_path):
    s = StateStore(path=tmp_path)
    cfg = Config(symbols=["BTCUSDT"], risk=RiskCfg(max_daily_losses=None))
    for _ in range(10):
        s.record_loss_if_today("SL_HIT")
    assert s.daily_loss_cap_reached(cfg) is False


def test_loss_counter_round_trips(tmp_path):
    s = StateStore(path=tmp_path)
    s.record_loss_if_today("SL_HIT")
    s.record_loss_if_today("SL_HIT")
    s.save()
    fresh = StateStore(path=tmp_path)
    assert fresh.losses_today_count == 2
    assert fresh.losses_today_date == s.losses_today_date
