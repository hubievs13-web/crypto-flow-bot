from crypto_flow_bot.config import RegimeCfg
from crypto_flow_bot.engine.regime import classify_regime, compute_adx


def _bars(n: int, drift: float) -> list[list]:
    out=[]
    p=100.0
    for i in range(n):
        p += drift
        out.append([i, str(p-0.5), str(p+1.0), str(p-1.0), str(p), '0', i, '0', 0, '0', '0', '0'])
    return out


def test_compute_adx_trend_and_flat():
    up = compute_adx(_bars(60, 1.0))
    down = compute_adx(_bars(60, -1.0))
    flat = compute_adx(_bars(60, 0.0))
    assert up is not None and up > 20
    assert down is not None and down > 20
    assert flat is not None and flat <= 20


def test_classify_regime_buckets_and_none():
    cfg = RegimeCfg()
    assert classify_regime(30, 0.02, cfg) == 'trend_strong'
    assert classify_regime(30, 0.012, cfg) == 'trend_weak'
    assert classify_regime(10, 0.005, cfg) == 'range'
    assert classify_regime(10, 0.02, cfg) == 'chop'
    assert classify_regime(None, 0.01, cfg) is None
