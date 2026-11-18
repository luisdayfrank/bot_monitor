"""
tests/test_f13a_funding_oi.py - F13a.1/F13a.2 (Plan v7.4)

1) funding: decimal -> %, interpolacion devengada 1h (ultimo settlement).
2) OI: deltas 5m/1h contra la muestra <= ventana; None sin historico.
3) polling por simbolo actualiza micro_features (cliente falso).
4) feature_snapshots v1.2: round-trip + migracion de DB existente.
"""
import asyncio
import sqlite3
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import collector as col  # noqa: E402
import learning_db as ldb  # noqa: E402

H = 3600_000


def _test_funding_formula():
    now = 1_700_000_000_000
    rate, interp = col.calcular_funding_features(0.0001, now + 2 * H, now)
    assert abs(rate - 0.01) < 1e-9, rate
    assert abs(interp - 0.0075) < 1e-9, interp
    rate, interp = col.calcular_funding_features(0.0001, now, now)
    assert abs(rate - 0.01) < 1e-9 and abs(interp - 0.01) < 1e-9
    rate, interp = col.calcular_funding_features(0.0001, now + 8 * H, now)
    assert abs(interp) < 1e-9
    assert col.calcular_funding_features(None, now, now) == (None, None)
    assert col.calcular_funding_features(0.0001, None, now) == (0.01, None)
    assert col.calcular_funding_features(0.0001, 0, now) == (0.01, None)
    print('  [OK] F13a.1: funding decimal->% y devengo 1h interpolado')


def _test_oi_delta():
    t0 = 1_700_000_000_000
    hist = deque([(t0, 100.0), (t0 + 5 * 60_000, 105.0),
                  (t0 + 55 * 60_000, 108.0), (t0 + 60 * 60_000, 110.0)])
    d5 = col.calcular_oi_delta_pct(hist, t0 + 60 * 60_000, 5 * 60_000)
    d60 = col.calcular_oi_delta_pct(hist, t0 + 60 * 60_000, 60 * 60_000)
    esperado5 = round((110.0 - 108.0) / 108.0 * 100.0, 4)
    assert abs(d5 - esperado5) < 1e-9, d5
    assert abs(d60 - 10.0) < 1e-6, d60
    assert col.calcular_oi_delta_pct(deque([(t0, 100.0)]),
                                     t0 + 60 * 60_000, 60 * 60_000) is None
    assert col.calcular_oi_delta_pct(hist, t0 + 60 * 60_000, 3 * H) is None
    print('  [OK] F13a.2: OI delta 5m/1h y None sin historico')


class _FakeClient:
    def futures_mark_price(self, symbol):
        return {'lastFundingRate': '0.0001',
                'nextFundingTime': int(time.time() * 1000) + 2 * H}

    def futures_open_interest(self, symbol):
        return {'openInterest': '105.0'}


async def _test_polling():
    c = object.__new__(col.DataCollector)
    c.client = _FakeClient()
    c.micro_features = {'XUSDT': {'funding_rate_8h': None,
                                  'funding_rate_1h_interp': None,
                                  'oi_delta_5m_pct': None,
                                  'oi_delta_1h_pct': None}}
    c._oi_hist = {'XUSDT': deque([
        (int(time.time() * 1000) - 5 * 60_000, 100.0)])}
    orig = col.CONFIG
    col.CONFIG = SimpleNamespace(symbols=['XUSDT'])
    try:
        await c._poll_funding_rate()
        await c._poll_open_interest()
    finally:
        col.CONFIG = orig
    assert abs(c.micro_features['XUSDT']['funding_rate_8h'] - 0.01) < 1e-9
    assert c.micro_features['XUSDT']['funding_rate_1h_interp'] is not None
    assert abs(c.micro_features['XUSDT']['oi_delta_5m_pct'] - 5.0) < 1e-6
    assert c.micro_features['XUSDT']['oi_delta_1h_pct'] is None
    print('  [OK] F13a.1/2: polling actualiza micro_features por simbolo')


def _test_snapshot_wiring():
    src = (Path(__file__).resolve().parent.parent / 'learning_engine.py').read_text(
        encoding='utf-8')
    for f in ('funding_rate_8h', 'funding_rate_1h_interp',
              'oi_delta_5m_pct', 'oi_delta_1h_pct'):
        assert f in src, f
    assert "'feature_schema_version': 'v1.2'" in src
    print('  [OK] F13a: snapshot wiring v1.2 con 4 features')


async def _test_roundtrip():
    await ldb.init_learning_db(':memory:')
    now = int(time.time() * 1000)
    row = {c: None for c in ldb._SNAPSHOT_COLS}
    row.update({'symbol': 'XUSDT', 'timestamp_15m': now, 'price': 100.0,
                'regime': 'RANGE', 'regime_confirmado': 'RANGE',
                'funding_rate_8h': 0.01, 'funding_rate_1h_interp': 0.0075,
                'oi_delta_5m_pct': 1.2, 'oi_delta_1h_pct': 15.5,
                'feature_schema_version': 'v1.2'})
    n = await ldb.guardar_snapshots_batch([row])
    assert n == 1
    got = await ldb._run(lambda: ldb._get_conn().execute(
        "SELECT funding_rate_8h, funding_rate_1h_interp, oi_delta_5m_pct, "
        "oi_delta_1h_pct, feature_schema_version FROM feature_snapshots "
        "WHERE symbol='XUSDT'").fetchone())
    assert tuple(got) == (0.01, 0.0075, 1.2, 15.5, 'v1.2'), tuple(got)
    print('  [OK] F13a: round-trip feature_snapshots v1.2')
    await ldb.close_learning_db()


async def _test_migracion():
    tmp = tempfile.mkdtemp(prefix='f13a_')
    path = str(Path(tmp) / 'old.db')
    viejo = sqlite3.connect(path)
    viejo.execute(
        "CREATE TABLE feature_snapshots ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, "
        "timestamp_15m INTEGER NOT NULL, price REAL NOT NULL, regime TEXT, "
        "regime_confirmado TEXT, evento_asociado TEXT, score_macro INTEGER, "
        "feature_schema_version TEXT DEFAULT 'v1.1', "
        "UNIQUE(symbol, timestamp_15m))")
    viejo.execute(
        "INSERT INTO feature_snapshots (symbol, timestamp_15m, price) "
        "VALUES ('OLDUSDT', ?, 1.0)", (int(time.time() * 1000),))
    viejo.commit()
    viejo.close()
    await ldb.init_learning_db(path)
    cols = await ldb._run(lambda: [r[1] for r in ldb._get_conn().execute(
        "PRAGMA table_info(feature_snapshots)").fetchall()])
    for f in ('funding_rate_8h', 'funding_rate_1h_interp',
              'oi_delta_5m_pct', 'oi_delta_1h_pct'):
        assert f in cols, (f, cols)
    ver = await ldb._run(lambda: ldb._get_conn().execute(
        "SELECT 1 FROM schema_versions WHERE componente='FEATURES' "
        "AND version='v1.2' AND activo=1").fetchone())
    assert ver is not None
    print('  [OK] F13a: migracion DB existente (ALTER + FEATURES v1.2)')
    await ldb.close_learning_db()


async def main():
    _test_funding_formula()
    _test_oi_delta()
    await _test_polling()
    _test_snapshot_wiring()
    await _test_migracion()
    await _test_roundtrip()
    print('F13a funding/OI: PASS')


if __name__ == '__main__':
    asyncio.run(main())
