"""F11 - Test del esqueleto de backtest (sin estrategia aun)."""
import os
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import backtest_f11 as bt  # noqa: E402

SYMBOLS = ('BNBUSDT', 'XRPUSDT')


def _crear_db(path, dias=90):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE velas_15m (symbol TEXT, timestamp INTEGER, "
            "open REAL, high REAL, low REAL, close REAL, volume REAL, "
            "PRIMARY KEY(symbol, timestamp))")
        ahora = int(time.time() * 1000)
        inicio = ahora - dias * 86400_000
        for sym in SYMBOLS:
            filas = []
            for i in range(dias * bt.CANDLES_DIA_15M):
                ts = inicio + i * bt.STEP_MS
                precio = 100.0 + (i % 100) * 0.01
                filas.append((sym, ts, precio, precio + 0.05,
                              precio - 0.05, precio, 10.0))
            conn.executemany(
                "INSERT INTO velas_15m VALUES (?,?,?,?,?,?,?)", filas)
        conn.commit()
    finally:
        conn.close()


def test_backtest_pendiente_estrategia():
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    try:
        _crear_db(path)
        r = bt.ejecutar(path, list(SYMBOLS), dias=90)
        assert r['estado'] == 'PENDIENTE_ESTRATEGIA', r['estado']
        for sym in SYMBOLS:
            cov = r['simbolos'][sym]['cobertura']
            assert cov['cobertura_pct'] >= 85.0, cov
            assert r['simbolos'][sym]['baseline'] is not None
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    print('  [OK] F11-B7: cobertura OK y estado PENDIENTE_ESTRATEGIA')


def test_backtest_datos_insuficientes():
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    try:
        _crear_db(path, dias=1)
        r = bt.ejecutar(path, list(SYMBOLS), dias=90)
        assert r['estado'] == 'DATOS_INSUFICIENTES', r['estado']
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    print('  [OK] F11-B7: cobertura corta -> DATOS_INSUFICIENTES')


def main():
    test_backtest_pendiente_estrategia()
    test_backtest_datos_insuficientes()
    print('RESULTADO F11-B7: PASS')


if __name__ == '__main__':
    main()
