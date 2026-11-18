"""
tests/test_f13a_trap.py - F13a.4/F13a.5 (Plan v7.4)

1) TRAP: condicion, prioridad PANIC > TRAP > TREND > VOLATILE, alias NEUTRAL.
2) Histeresis 4 velas + transicion RANGE->TRAP.
3) Mapeo de senales: TRAP -> RANGE (5-estado) y NEUTRAL (alias).
4) regime_changes_per_month: 15/5 si >3/mes, 30/10 si no.
"""
import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import learning_db as ldb  # noqa: E402
import learning_engine as le  # noqa: E402
from signals_v5 import SignalGenerator  # noqa: E402

D = 86400_000


def _feats(**kw):
    base = {'adx_15m': 15.0, 'er_15m': 0.1,
            'ema7_ema25_spread_pct': 0.0, 'price_rel_ema50_pct': 0.0,
            'atr_pct_15m': 0.5, 'atr_ratio_1m_15m': 0.1, 'roc_4v_pct': 0.0,
            'funding_rate_8h': 0.06, 'oi_delta_1h_pct': 16.0}
    base.update(kw)
    return base


def _test_trap_condicion():
    rc = le.RegimeClassifier()
    assert rc.clasificar_inst(_feats()) == 'TRAP'
    assert rc.clasificar_inst(_feats(funding_rate_8h=0.04)) == 'RANGE'
    assert rc.clasificar_inst(_feats(oi_delta_1h_pct=14.0)) == 'RANGE'
    assert rc.clasificar_inst(_feats(funding_rate_8h=None)) == 'RANGE'
    assert rc.clasificar_inst(_feats(oi_delta_1h_pct=None)) == 'RANGE'
    assert rc.clasificar_inst(_feats(atr_ratio_1m_15m=2.0)) == 'PANIC'
    trend = _feats(adx_15m=30.0, er_15m=0.5,
                   ema7_ema25_spread_pct=0.5, price_rel_ema50_pct=0.5)
    assert rc.clasificar_inst(trend) == 'TRAP'
    trend_no_trap = dict(trend, funding_rate_8h=0.01, oi_delta_1h_pct=1.0)
    assert rc.clasificar_inst(trend_no_trap) == 'TREND_UP'
    print('  [OK] F13a.4: TRAP condicion + prioridad PANIC > TRAP > TREND')


def _test_alias_y_senales():
    assert le.RegimeClassifier.to_alias('TRAP') == 'NEUTRAL'
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.learning_engine = SimpleNamespace(
        regime_cls=SimpleNamespace(regimen_actual={'XUSDT': 'TRAP'}))
    assert sg._regimen_para_senales('XUSDT') == 'RANGE'
    sg.learning_engine.regime_cls.regimen_actual['XUSDT'] = 'TREND_UP'
    assert sg._regimen_para_senales('XUSDT') == 'TREND_UP'
    print('  [OK] F13a.4: TRAP->NEUTRAL alias y TRAP->RANGE en senales')


def _test_histeresis():
    rc = le.RegimeClassifier(min_velas=4)
    rc.regimen_actual['XUSDT'] = 'RANGE'
    rc._regimen_desde['XUSDT'] = le._now_ms() - 3600_000 - 1000
    res = None
    for _ in range(4):
        res = rc.actualizar('XUSDT', _feats())
    assert res['regimen'] == 'TRAP', res
    assert res['transicion'] is not None, res
    assert res['transicion'][0] == 'RANGE' and res['transicion'][1] == 'TRAP'
    print('  [OK] F13a.4: histeresis 4 velas + transicion RANGE->TRAP')


async def _test_regime_changes():
    await ldb.init_learning_db(':memory:')
    ahora = int(time.time() * 1000)
    rows = []
    for dias in (30, 20, 10, 0):
        rows.append({'symbol': 'XUSDT', 'timestamp_15m': ahora - dias * D,
                     'from_regime': 'RANGE', 'to_regime': 'TREND_UP',
                     'adx_15m': 30.0})
    await ldb.guardar_regime_transitions_batch(rows)
    r = await ldb.calcular_regime_changes_per_month()
    assert r['changes'] == 4, r
    assert r['per_month'] == 4.0, r
    assert r['walk_forward'] == '15/5', r
    print('  [OK] F13a.5: 4 cambios/mes -> walk_forward 15/5')
    await ldb.close_learning_db()


async def main():
    _test_trap_condicion()
    _test_alias_y_senales()
    _test_histeresis()
    await _test_regime_changes()
    print('F13a TRAP/F13a.5: PASS')


if __name__ == '__main__':
    asyncio.run(main())
