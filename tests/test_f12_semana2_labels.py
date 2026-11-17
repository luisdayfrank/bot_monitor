"""
tests/test_f12_semana2_labels.py — F12.2 / F12.2-bis (Plan v7.4, Semana 2)

1) _es_exito neto de fees: grid con pnl_real > 0 pero pnl_neto <= 0 NO es
   éxito (p_base=0 -> regla global BLOCK).
2) continuación neta: MFE bruto entre el umbral viejo (k·ATR) y el nuevo
   (k·ATR + fees) NO cuenta como continuación.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import learning_engine as le  # noqa: E402
import learning_db as ldb       # noqa: E402


async def _test_es_exito_neto():
    # 4 grids con pnl_real +0.05 y fees 0.10 -> neto -0.05 (fracaso)
    filas = [{'n_roundtrips': 3, 'pnl_real': 0.05, 'fees_real': 0.10,
              'adx_15m': 30, 'rsi_15m': 50, 'ema200_dist_pct': 0.5,
              'atr_pct_15m': 1.0} for _ in range(4)]
    capturadas = []

    async def _fake_outcomes(**kw):
        return filas

    async def _fake_guardar(cands):
        capturadas.extend(cands)

    orig_o = le.cargar_grid_outcomes
    orig_g = ldb.guardar_entry_rules_batch
    le.cargar_grid_outcomes = _fake_outcomes
    ldb.guardar_entry_rules_batch = _fake_guardar
    try:
        learner = le.PatternLearner()
        await learner.minar_neutral_grids(min_samples=2)
    finally:
        le.cargar_grid_outcomes = orig_o
        ldb.guardar_entry_rules_batch = orig_g

    assert capturadas, 'p_base=0 debe emitir 1 regla global BLOCK'
    assert capturadas[0]['accion'] == 'BLOCK', capturadas[0]
    assert capturadas[0]['win_rate'] == 0.0
    print('  [OK] F12.2: _es_exito neto de fees (pnl bruto>0 / neto<0 = fracaso)')


def _test_continuacion_neta():
    lab = le.BreakoutOutcomeLabeler(atr_significance_mult=1.0, fee_pct=0.05)
    ts = 1794000000000
    snap = {'timestamp_15m': ts, 'price': 100.0, 'atr_pct_15m': 0.5,
            'symbol': 'TESTUSDT', 'regime_confirmado': 'TREND_UP',
            'donchian_high_20': 99.9, 'donchian_low_20': 98.0}
    # MFE bruto 0.55% -> supera 1.0*0.5=0.5 (viejo) pero NO 0.5+0.1=0.6 (nuevo)
    velas = [{'timestamp': ts + i * 900_000, 'high': 100.55, 'low': 99.90}
             for i in range(1, 5)]
    fila = lab.etiquetar(snap, velas, nivel_roto=100.0, direction='LONG')
    assert fila is not None
    assert fila['continuacion'] == 0, f"continuacion={fila['continuacion']} (esperado 0)"
    print('  [OK] F12.2-bis: continuación exige MFE bruto >= k·ATR + fees')


async def main():
    await _test_es_exito_neto()
    _test_continuacion_neta()
    print('F12 semana 2 labels: PASS')


if __name__ == '__main__':
    asyncio.run(main())
