# -*- coding: utf-8 -*-
"""
PR-B: Backfill de income_eventos (ESCRITURA — separado del diagnóstico read-only).

Crea la tabla income_eventos si no existe (mismo schema que database_v5.init_db)
y rellena los últimos N días desde futures_income: FUNDING_FEE, REALIZED_PNL
y COMMISSION. Idempotente (INSERT IGNORE sobre UNIQUE tran_id+income_type+symbol).

Uso (en la máquina donde corre el bot):
    python backfill_income.py             # 30 días
    python backfill_income.py --dias 60
    python backfill_income.py --testnet
"""
import argparse
import os
import time
from datetime import datetime, timedelta, timezone

import pymysql

DB_HOST = os.getenv("DB_HOST", "database-1.cnqsq0e6cf6z.eu-west-1.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1605584496")
DB_NAME = os.getenv("DB_NAME", "crypto_monitor")

INCOME_TYPES = ('FUNDING_FEE', 'REALIZED_PNL', 'COMMISSION')

DDL = """
CREATE TABLE IF NOT EXISTS income_eventos (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    income_type VARCHAR(24) NOT NULL,
    income DOUBLE NOT NULL,
    asset VARCHAR(16) NOT NULL,
    tran_id BIGINT NOT NULL,
    trade_id VARCHAR(64) NULL,
    timestamp_ms BIGINT NOT NULL,
    info VARCHAR(255) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_income_tran_type (tran_id, income_type, symbol)
)
"""

DDL_IDX = [
    "CREATE INDEX IF NOT EXISTS idx_income_ts ON income_eventos(timestamp_ms)",
    "CREATE INDEX IF NOT EXISTS idx_income_symbol_ts ON income_eventos(symbol, timestamp_ms)",
    "CREATE INDEX IF NOT EXISTS idx_income_type_ts ON income_eventos(income_type, timestamp_ms)",
]

INSERT_SQL = """
INSERT IGNORE INTO income_eventos
(symbol, income_type, income, asset, tran_id, trade_id, timestamp_ms, info)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def main():
    ap = argparse.ArgumentParser(description="PR-B backfill income_eventos (escritura)")
    ap.add_argument("--dias", type=int, default=30)
    ap.add_argument("--testnet", action="store_true")
    args = ap.parse_args()

    # Cliente Binance (misma selección de keys que executor.py:332-336)
    from binance.client import Client
    if args.testnet:
        key = os.getenv("BINANCE_TESTNET_API_KEY", "")
        secret = os.getenv("BINANCE_TESTNET_SECRET", "")
    else:
        key = os.getenv("BINANCE_API_KEY", "")
        secret = os.getenv("BINANCE_API_SECRET", "")
    if not key or not secret:
        print("ERROR: sin API keys en env (BINANCE_API_KEY/BINANCE_API_SECRET "
              "o las *_TESTNET_* con --testnet)")
        return 1
    client = Client(key, secret, testnet=args.testnet)

    conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, connect_timeout=10, read_timeout=60,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            for sql in DDL_IDX:
                try:
                    cur.execute(sql)
                except pymysql.err.InternalError:
                    pass  # índice ya existe
        conn.commit()
        print("[OK] Tabla income_eventos lista")

        ahora = datetime.now(timezone.utc)
        inicio = ahora - timedelta(days=args.dias)

        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM pnl_eventos ORDER BY symbol")
            symbols = [r[0] for r in cur.fetchall()]
        print(f"Símbolos: {symbols}")

        total_insert = 0
        for symbol in symbols:
            for income_type in INCOME_TYPES:
                cursor_ts = int(inicio.timestamp() * 1000)
                ts_fin = int(ahora.timestamp() * 1000)
                filas = []
                while cursor_ts < ts_fin:
                    fin_ventana = min(cursor_ts + 7 * 86400_000, ts_fin)
                    try:
                        income = client.futures_income(
                            incomeType=income_type,
                            symbol=symbol,
                            startTime=cursor_ts,
                            endTime=fin_ventana,
                            limit=1000,
                        )
                    except Exception as e:
                        print(f"  ⚠️ {symbol} {income_type}: {e}")
                        income = []
                    for ev in income or []:
                        filas.append((
                            symbol,
                            ev.get('incomeType', income_type),
                            float(ev.get('income', 0)),
                            ev.get('asset', 'USDT'),
                            int(ev.get('tranId', 0)),
                            str(ev.get('tradeId', '')) or None,
                            int(ev.get('time', 0)),
                            (ev.get('info') or '')[:255],
                        ))
                    cursor_ts = fin_ventana + 1
                    time.sleep(0.2)

                if filas:
                    with conn.cursor() as cur:
                        n = cur.executemany(INSERT_SQL, filas)
                    conn.commit()
                    total_insert += len(filas)
                    print(f"  [OK] {symbol} {income_type}: {len(filas)} eventos "
                          f"(insertados/ignorados: {n})")

        # Resumen final
        with conn.cursor() as cur:
            cur.execute("""
                SELECT income_type, COUNT(*), ROUND(SUM(income), 6)
                FROM income_eventos GROUP BY income_type
            """)
            print("\n── income_eventos tras backfill ──")
            for r in cur.fetchall():
                print(f"  {r[0]}: n={r[1]} sum={r[2]}")
        print(f"\n[OK] Backfill completado: {total_insert} eventos procesados")
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
