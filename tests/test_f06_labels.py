"""F6 — Tests standalone: labels FIRE/BREAKOUT + normalizacion I68.

Ejecutar en el host del bot:  python tests/test_f06_labels.py
"""
import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from learning_engine import (  # noqa: E402
    FireOutcomeImporter, BreakoutOutcomeLabeler,
    _normalizar_tipo_rechazo, _parse_iso_to_ms,
)
import learning_db as ldb  # noqa: E402

TS = 1794000000000


def test_normalizador():
    assert _normalizar_tipo_rechazo(
        'MFM contradictorio: mfm=-0.117 v') == 'MFM_BAJO'
    assert _normalizar_tipo_rechazo(
        'RSI alto 55.3 para SHORT — momen') == 'RSI_EXTREMO'
    assert _normalizar_tipo_rechazo(
        'ADX sin tendencia (<12): 9.7') == 'ADX_EXTREMO'
    assert _normalizar_tipo_rechazo('ATR bajo: 0.1%') == 'ATR_COMPRIMIDO'
    assert _normalizar_tipo_rechazo('Volumen bajo: 10%') == 'VOLUMEN_BAJO'
    assert _normalizar_tipo_rechazo('MACD explosivo: 1.2%') == 'MACD_EXPLOSIVO'
    assert _normalizar_tipo_rechazo('step_pct 0.082% < min') == 'STEP_PCT_BAJO'
    assert _normalizar_tipo_rechazo('Qty 0.094 < 1.0') == 'QTY_INSUFICIENTE'
    assert _normalizar_tipo_rechazo(
        'SQUEEZE: BB width en p10') == 'COMPRESION_SQUEEZE'
    assert _normalizar_tipo_rechazo(
        'EXPANSION_ATR: x') == 'EXPANSION_ATR'
    assert _normalizar_tipo_rechazo('NEUTRAL_CONVICCION') == 'NEUTRAL_CONVICCION'
    assert _normalizar_tipo_rechazo('') == 'OTRO'
    print('  [OK] I68: normalizador de tipo_rechazo')


def test_parse_iso():
    ms = _parse_iso_to_ms('2026-11-06T00:05:00.548746+00:00')
    assert ms is not None and ms > 0
    assert _parse_iso_to_ms(None) is None
    assert _parse_iso_to_ms('basura') is None
    print('  [OK] _parse_iso_to_ms')


def _le_fake():
    return SimpleNamespace(
        fee_pct=0.05,
        cfg=SimpleNamespace(learning_atr_significance_mult=1.5))


def test_fire_fila():
    imp = FireOutcomeImporter(_le_fake())
    fire = {'id': 1, 'symbol': 'XUSDT', 'direccion': 'LONG', 'precio': 100.0,
            'score': 70, 'timestamp_utc': '2026-11-09T12:00:00+00:00'}
    muestras = [
        {'minutos_desde_disparo': 60, 'precio': 101.0},
        {'minutos_desde_disparo': 120, 'precio': 102.0},
        {'minutos_desde_disparo': 240, 'precio': 99.0},
    ]
    grids = [{'id': 7, 'pnl_real': 1.25}]
    niveles = [{'roundtrips': 2}, {'roundtrips': 0}, {'roundtrips': 1}]
    snap = {'id': 55, 'regime_confirmado': 'TREND_UP', 'atr_pct_15m': 1.0,
            'adx_15m': 30.0, 'rsi_15m': 55.0, 'mfm_15m': 0.2, 'mfm_sma5': 0.3,
            'er_15m': 0.5, 'bb_width_pct_pct': 40.0, 'pct_b': 0.8,
            'atr_ratio_1m_15m': 0.3, 'roc_4v_pct': 1.0,
            'price_rel_ema200_pct': 1.5, 'volume_ratio': 1.8,
            'rsi_7_1m': 60.0, 'wick_upper_pct_1m': 0.1,
            'wick_lower_pct_1m': 0.2, 'body_direction_1m': 1}
    fila = imp._construir_fila(fire, muestras, grids, snap, niveles)
    assert fila is not None
    assert abs(fila['ret_neto_2h_pct'] - 1.90) < 1e-6, fila['ret_neto_2h_pct']
    assert abs(fila['ret_neto_4h_pct'] - (-1.10)) < 1e-6, fila['ret_neto_4h_pct']
    assert abs(fila['mfe_2h_pct'] - 2.0) < 1e-6
    assert abs(fila['mae_2h_pct'] - 0.0) < 1e-6
    assert fila['grid_roundtrips'] == 3
    assert fila['grid_levels_touched'] == 2
    assert fila['grid_pnl_usdt'] == 1.25
    assert fila['significativo_katr'] == 1
    assert fila['rentable_neto_2h'] == 1
    assert fila['rentable_neto_4h'] == 0
    assert fila['wick_pct_1m'] == 0.2  # LONG -> wick_lower
    print('  [OK] F6.3: _construir_fila LONG (ret/mfe/mae/grid)')

    # SHORT espejo
    fire_s = dict(fire)
    fire_s.update({'direccion': 'SHORT'})
    muestras_s = [
        {'minutos_desde_disparo': 60, 'precio': 99.0},
        {'minutos_desde_disparo': 120, 'precio': 98.0},
        {'minutos_desde_disparo': 240, 'precio': 101.0},
    ]
    fila_s = imp._construir_fila(fire_s, muestras_s, grids, snap, niveles)
    assert fila_s is not None
    assert abs(fila_s['ret_neto_2h_pct'] - 1.90) < 1e-6
    assert abs(fila_s['ret_neto_4h_pct'] - (-1.10)) < 1e-6
    assert abs(fila_s['mfe_2h_pct'] - 2.0) < 1e-6
    print('  [OK] F6.3: _construir_fila SHORT')


def test_breakout_labeler():
    lb = BreakoutOutcomeLabeler(atr_significance_mult=2.0)
    snap = {'symbol': 'XUSDT', 'timestamp_15m': TS, 'price': 100.0,
            'atr_pct_15m': 1.0, 'regime_confirmado': 'TREND_DOWN'}

    def v(off, hi, lo):
        return {'timestamp': TS + off, 'high': hi, 'low': lo}

    velas = [v(900000, 99.4, 99.0), v(1800000, 99.3, 98.0),
             v(2700000, 99.4, 97.0), v(3600000, 99.2, 97.5)]
    fila = lb.etiquetar(snap, velas, 99.0, 'SHORT')
    assert fila is not None
    assert fila['continuacion'] == 1 and fila['fakeout'] == 0
    assert fila['minutos_continuacion'] == 30
    velas_f = [v(900000, 100.0, 98.0), v(1800000, 99.0, 97.0),
               v(2700000, 99.0, 97.0), v(3600000, 99.0, 97.0)]
    fila_f = lb.etiquetar(snap, velas_f, 99.0, 'SHORT')
    assert fila_f is not None
    assert fila_f['fakeout'] == 1 and fila_f['continuacion'] == 0
    assert lb.etiquetar(snap, velas[:3], 99.0, 'SHORT') is None
    print('  [OK] F6.4: etiquetar continuacion / fakeout / insuficiente')


async def test_sqlite_roundtrip():
    await ldb.init_learning_db(':memory:')
    try:
        row = {c: None for c in ldb._FIRE_LABEL_COLS}
        row.update({'symbol': 'XUSDT', 'timestamp_fire': TS, 'direction': 'LONG',
                    'precio_entrada': 100.0, 'ret_neto_2h_pct': 1.9,
                    'rentable_neto_2h': 1, 'feature_schema_version': 'v1.1'})
        n1 = await ldb.guardar_fire_labels_batch([row])
        n2 = await ldb.guardar_fire_labels_batch([row])
        cnt = await ldb._run(lambda: ldb._get_conn().execute(
            "SELECT COUNT(*) FROM fire_labels").fetchone()[0])
        assert n1 == 1 and n2 == 1 and cnt == 1, (n1, n2, cnt)

        brow = {c: None for c in ldb._BREAKOUT_LABEL_COLS}
        brow.update({'symbol': 'XUSDT', 'timestamp_15m': TS, 'direction': 'SHORT',
                     'regimen': 'TREND_DOWN', 'nivel_roto': 99.0,
                     'continuacion': 1, 'fakeout': 0,
                     'feature_schema_version': 'v1.1'})
        await ldb.guardar_breakout_labels_batch([brow])

        snap_row = {c: None for c in ldb._SNAPSHOT_COLS}
        snap_row.update({'symbol': 'XUSDT', 'timestamp_15m': TS + 900000,
                         'price': 98.0, 'regime_confirmado': 'TREND_DOWN',
                         'donchian_high_20': 100.0, 'donchian_low_20': 99.0,
                         'feature_schema_version': 'v1.1'})
        await ldb.guardar_snapshots_batch([snap_row])
        cands = await ldb.cargar_snapshots_breakout_candidatos(
            horas=24 * 365, antes_de_ms=TS + 10 ** 9)
        assert any(c['timestamp_15m'] == TS + 900000 for c in cands), cands

        brow2 = dict(brow)
        brow2.update({'timestamp_15m': TS + 900000})
        await ldb.guardar_breakout_labels_batch([brow2])
        cands2 = await ldb.cargar_snapshots_breakout_candidatos(
            horas=24 * 365, antes_de_ms=TS + 10 ** 9)
        assert all(c['timestamp_15m'] != TS + 900000 for c in cands2), cands2
        print('  [OK] SQLite: savers + candidatos excluyen ya etiquetados')
    finally:
        await ldb.close_learning_db()


async def main():
    test_normalizador()
    test_parse_iso()
    test_fire_fila()
    test_breakout_labeler()
    await test_sqlite_roundtrip()
    print('RESULTADO F6: PASS')


if __name__ == '__main__':
    asyncio.run(main())
