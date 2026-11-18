"""
tests/test_f12_semana4_breakout_neto.py - F12.3 (Plan v7.4, Semana 4)

1) etiquetar() calcula ret_neto_2h_pct NETO de fees (LONG y SHORT) y
   rentable_neto_2h; feature_schema_version pasa a v1.2.
2) migracion en DB existente: ALTER idempotente + LABELER v1.2 en schema_versions.
3) round-trip SQLite y minado solo sobre filas v1.2 (legacy NULL excluida).
"""
import asyncio
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import learning_db as ldb  # noqa: E402
from learning_engine import BreakoutOutcomeLabeler  # noqa: E402

TS = 1_700_000_000_000


def _vela(off, hi, lo, close):
    return {'timestamp': TS + off, 'high': hi, 'low': lo, 'close': close}


def _snap(regime):
    return {'symbol': 'XUSDT', 'timestamp_15m': TS, 'price': 100.0,
            'atr_pct_15m': 1.0, 'regime_confirmado': regime}


def _test_long():
    lab = BreakoutOutcomeLabeler(atr_significance_mult=2.0, fee_pct=0.05)
    velas = [
        _vela(900_000, 100.8, 100.1, 100.5),
        _vela(1_800_000, 101.5, 100.4, 101.2),
        _vela(2_700_000, 102.6, 101.0, 102.2),
        _vela(3_600_000, 102.8, 101.8, 102.5),
    ]
    fila = lab.etiquetar(_snap('TREND_UP'), velas, 99.0, 'LONG')
    assert fila is not None
    assert fila['continuacion'] == 1
    assert fila['feature_schema_version'] == 'v1.2', fila['feature_schema_version']
    assert abs(fila['ret_neto_2h_pct'] - 2.40) < 1e-6, fila['ret_neto_2h_pct']
    assert fila['rentable_neto_2h'] == 1
    print('  [OK] F12.3: LONG ret_neto_2h = 2.40 (bruto 2.50 - fees 0.10)')


def _test_short():
    lab = BreakoutOutcomeLabeler(atr_significance_mult=2.0, fee_pct=0.05)
    velas = [
        _vela(900_000, 100.4, 99.6, 99.8),
        _vela(1_800_000, 100.2, 98.9, 99.1),
        _vela(2_700_000, 100.1, 98.2, 98.5),
        _vela(3_600_000, 100.0, 97.8, 97.9),
    ]
    fila = lab.etiquetar(_snap('TREND_DOWN'), velas, 101.0, 'SHORT')
    assert fila is not None
    assert fila['continuacion'] == 1
    assert abs(fila['ret_neto_2h_pct'] - 2.00) < 1e-6, fila['ret_neto_2h_pct']
    assert fila['rentable_neto_2h'] == 1
    print('  [OK] F12.3: SHORT ret_neto_2h = 2.00 (bruto 2.10 - fees 0.10)')


def _test_no_rentable():
    lab = BreakoutOutcomeLabeler(atr_significance_mult=2.0, fee_pct=0.05)
    velas = [
        _vela(900_000, 100.2, 99.8, 99.99),
        _vela(1_800_000, 100.2, 99.8, 99.98),
        _vela(2_700_000, 100.2, 99.8, 99.97),
        _vela(3_600_000, 100.2, 99.8, 99.95),
    ]
    fila = lab.etiquetar(_snap('TREND_UP'), velas, 99.0, 'LONG')
    assert fila is not None
    assert fila['ret_neto_2h_pct'] < 0, fila['ret_neto_2h_pct']
    assert fila['rentable_neto_2h'] == 0
    print('  [OK] F12.3: bruto +0.05% < fees 0.10% -> no rentable')


async def _test_migracion():
    tmp = tempfile.mkdtemp(prefix='f12s4_')
    path = os.path.join(tmp, 'old.db')
    viejo = sqlite3.connect(path)
    viejo.execute(
        "CREATE TABLE breakout_labels ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
        "timestamp_15m INTEGER NOT NULL, direction TEXT NOT NULL, "
        "regimen TEXT NOT NULL, nivel_roto REAL, donchian_high_20 REAL, "
        "donchian_low_20 REAL, er_15m REAL, bb_width_pct_pct REAL, pct_b REAL, "
        "atr_ratio_1m_15m REAL, roc_4v_pct REAL, volume_ratio_1m REAL, "
        "volume_ratio_15m REAL, mfm_sma5 REAL, vwap_24h REAL, "
        "continuacion BOOLEAN, fakeout BOOLEAN, mfe_pct_2h REAL, "
        "mae_pct_2h REAL, minutos_continuacion INTEGER, "
        "nivel_invalidacion REAL, feature_schema_version TEXT DEFAULT 'v1.1', "
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE(symbol, timestamp_15m, direction))")
    viejo.execute(
        "INSERT INTO breakout_labels (symbol, timestamp_15m, direction, regimen) "
        "VALUES ('OLDUSDT', ?, 'LONG', 'TREND_UP')",
        (int(time.time() * 1000),))
    viejo.commit()
    viejo.close()

    await ldb.init_learning_db(path)
    cols = await ldb._run(lambda: [r[1] for r in ldb._get_conn().execute(
        "PRAGMA table_info(breakout_labels)").fetchall()])
    assert 'ret_neto_2h_pct' in cols and 'rentable_neto_2h' in cols, cols
    assert 'ret_neto_4h_pct' not in cols, 'opcion A: sin columna 4h'
    ver = await ldb._run(lambda: ldb._get_conn().execute(
        "SELECT 1 FROM schema_versions WHERE componente='LABELER' "
        "AND version='v1.2' AND activo=1").fetchone())
    assert ver is not None, 'falta schema_versions LABELER v1.2'
    rows = await ldb.cargar_breakout_labels(horas=24 * 30)
    assert all(r['symbol'] != 'OLDUSDT' for r in rows)
    print('  [OK] F12.3: migracion en DB existente (ALTER + LABELER v1.2)')
    await ldb.close_learning_db()


async def _test_roundtrip():
    await ldb.init_learning_db(':memory:')
    now = int(time.time() * 1000)
    base = {c: None for c in ldb._BREAKOUT_LABEL_COLS}
    nuevo = dict(base)
    nuevo.update({'symbol': 'XUSDT', 'timestamp_15m': now, 'direction': 'LONG',
                  'regimen': 'TREND_UP', 'ret_neto_2h_pct': 2.4,
                  'rentable_neto_2h': 1, 'feature_schema_version': 'v1.2'})
    legacy = dict(base)
    legacy.update({'symbol': 'XUSDT', 'timestamp_15m': now - 900_000,
                   'direction': 'LONG', 'regimen': 'TREND_UP',
                   'feature_schema_version': 'v1.1'})
    n = await ldb.guardar_breakout_labels_batch([nuevo, legacy])
    assert n == 2
    rows = await ldb.cargar_breakout_labels(horas=24 * 30)
    assert len(rows) == 1, len(rows)
    r = rows[0]
    assert r['ret_neto_2h_pct'] == 2.4 and r['rentable_neto_2h'] == 1
    assert r['feature_schema_version'] == 'v1.2'
    print('  [OK] F12.3: round-trip v1.2 y minado excluye legacy NULL')
    await ldb.close_learning_db()


async def main():
    _test_long()
    _test_short()
    _test_no_rentable()
    await _test_migracion()
    await _test_roundtrip()
    print('F12 semana 4 breakout neto: PASS')


if __name__ == '__main__':
    asyncio.run(main())
