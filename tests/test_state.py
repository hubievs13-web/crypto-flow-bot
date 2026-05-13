"""Tests for state persistence and post-exit cooldown bookkeeping."""

from datetime import UTC, datetime, timedelta

from crypto_flow_bot.config import Config
from crypto_flow_bot.engine.models import Direction, Snapshot
from crypto_flow_bot.engine.signals import FiredRule, SignalCandidate
from crypto_flow_bot.engine.state import StateStore


def _cfg() -> Config:
    return Config(symbols=["BTCUSDT"])


def _snap(price: float = 100.0, atr: float | None = 1.0) -> Snapshot:
    return Snapshot(symbol="BTCUSDT", ts=datetime.now(tz=UTC), price=price, atr_1h=atr)


def _candidate(rule_names: list[str], window_rules: set[str] | None = None) -> SignalCandidate:
    return SignalCandidate(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        fired_rules=[FiredRule(name=n, description=n) for n in rule_names],
        snapshot=_snap(),
        confluence_window_rules=window_rules if window_rules is not None else set(rule_names),
    )


def test_last_liveness_ping_date_round_trips(tmp_path):
    """Daily liveness state field must persist across restarts."""
    store = StateStore(path=tmp_path)
    assert store.last_liveness_ping_date is None
    store.last_liveness_ping_date = "2025-11-04"
    store.save()
    fresh = StateStore(path=tmp_path)
    assert fresh.last_liveness_ping_date == "2025-11-04"


def test_position_strong_flag_requires_two_non_funding_rules(tmp_path):
    """STRONG must mark 2+ *non-funding* rules in the confluence window.

    Funding alone (or funding+1 fast rule) is the regular entry condition;
    only two genuinely fast triggers in agreement (e.g. OI surge + liq
    cascade) earn the STRONG marker.
    """
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    # Two non-funding rules in window -> STRONG.
    pos_strong = store.open_from_signal(
        _candidate(["oi_surge", "liq_cascade"]), cfg,
    )
    assert pos_strong.strong is True
    # funding + lsr (one non-funding) -> regular entry, not strong.
    pos_regular = store.open_from_signal(
        _candidate(
            ["funding_extreme"],
            window_rules={"funding_extreme", "lsr_extreme"},
        ),
        cfg,
    )
    assert pos_regular.strong is False

    store.save()
    fresh = StateStore(path=tmp_path)
    by_id = {p.id: p for p in fresh.positions.values()}
    assert by_id[pos_strong.id].strong is True
    assert by_id[pos_regular.id].strong is False


def test_single_rule_position_not_strong(tmp_path):
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    pos = store.open_from_signal(_candidate(["lsr_extreme"]), cfg)
    assert pos.strong is False


def test_signal_id_and_entry_atr_round_trip(tmp_path):
    """signal_id and entry_atr_1h must survive save/load so blocked.jsonl and
    positions.jsonl rows can still be joined and so trailing keeps working
    after a restart."""
    store = StateStore(path=tmp_path)
    cfg = _cfg()
    candidate = SignalCandidate(
        symbol="BTCUSDT", direction=Direction.LONG,
        fired_rules=[FiredRule(name="lsr_extreme", description="x")],
        snapshot=_snap(atr=1.23),
        signal_id="abcd1234",
    )
    pos = store.open_from_signal(candidate, cfg)
    assert pos.signal_id == "abcd1234"
    assert pos.entry_atr_1h == 1.23
    store.save()
    fresh = StateStore(path=tmp_path)
    reloaded = next(iter(fresh.positions.values()))
    assert reloaded.signal_id == "abcd1234"
    assert reloaded.entry_atr_1h == 1.23


# ─── Post-exit cooldown ────────────────────────────────────────────────────

def test_post_exit_cooldown_starts_on_full_close(tmp_path):
    """After a position fully closes, the per-(symbol, direction) cooldown
    must report >0 remaining seconds until the configured window elapses."""
    s = StateStore(path=tmp_path)
    cfg = _cfg()
    pos = s.open_from_signal(_candidate(["lsr_extreme"]), cfg)
    # Before close: no cooldown.
    assert s.post_exit_cooldown_remaining_seconds(
        "BTCUSDT", Direction.LONG, cooldown_seconds=7200,
    ) == 0.0
    s.close_position(pos, price=100.0, reason="TIME_STOP")
    remaining = s.post_exit_cooldown_remaining_seconds(
        "BTCUSDT", Direction.LONG, cooldown_seconds=7200,
    )
    # We just closed; the full window should still be ahead of us.
    assert remaining > 7100.0
    # The opposite direction was never closed, so no cooldown there.
    assert s.post_exit_cooldown_remaining_seconds(
        "BTCUSDT", Direction.SHORT, cooldown_seconds=7200,
    ) == 0.0


def test_post_exit_cooldown_zero_disables_gate(tmp_path):
    s = StateStore(path=tmp_path)
    cfg = _cfg()
    pos = s.open_from_signal(_candidate(["lsr_extreme"]), cfg)
    s.close_position(pos, price=100.0, reason="SL_HIT")
    assert s.post_exit_cooldown_remaining_seconds(
        "BTCUSDT", Direction.LONG, cooldown_seconds=0,
    ) == 0.0


def test_post_exit_cooldown_expires_after_window(tmp_path):
    """An old close (well past the window) must report 0 remaining."""
    s = StateStore(path=tmp_path)
    cfg = _cfg()
    pos = s.open_from_signal(_candidate(["lsr_extreme"]), cfg)
    s.close_position(pos, price=100.0, reason="TIME_STOP")
    # Manually back-date the close timestamp.
    s.last_close_ts[("BTCUSDT", Direction.LONG)] = datetime.now(tz=UTC) - timedelta(hours=10)
    assert s.post_exit_cooldown_remaining_seconds(
        "BTCUSDT", Direction.LONG, cooldown_seconds=7200,
    ) == 0.0


def test_last_close_ts_round_trips(tmp_path):
    """last_close_ts must survive process restarts so the cooldown still
    applies after a Fly redeploy."""
    s = StateStore(path=tmp_path)
    cfg = _cfg()
    pos = s.open_from_signal(_candidate(["lsr_extreme"]), cfg)
    s.close_position(pos, price=100.0, reason="TIME_STOP")
    s.save()
    fresh = StateStore(path=tmp_path)
    assert ("BTCUSDT", Direction.LONG) in fresh.last_close_ts


# ─── Log dedup ─────────────────────────────────────────────────────────────

def test_should_log_skip_first_time_passes(tmp_path):
    s = StateStore(path=tmp_path)
    assert s.should_log_skip(
        "BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=1800,
    ) is True


def test_should_log_skip_suppresses_repeats_within_interval(tmp_path):
    s = StateStore(path=tmp_path)
    s.should_log_skip("BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=1800)
    assert s.should_log_skip(
        "BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=1800,
    ) is False


def test_should_log_skip_emits_when_reason_key_changes(tmp_path):
    """A different reason on the same (symbol, direction) is a state
    transition and must be visible in stdout immediately."""
    s = StateStore(path=tmp_path)
    s.should_log_skip("BTCUSDT", Direction.LONG, "cooldown", interval_seconds=1800)
    assert s.should_log_skip(
        "BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=1800,
    ) is True


def test_should_log_skip_zero_interval_always_passes(tmp_path):
    s = StateStore(path=tmp_path)
    s.should_log_skip("BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=0)
    assert s.should_log_skip(
        "BTCUSDT", Direction.LONG, "max_concurrent", interval_seconds=0,
    ) is True
