#!/usr/bin/env python3
"""Backfill 90d de velas 15m (Binance Futures) para el backtest F11.

Crea/actualiza backtest_data.db con:
  velas_15m(symbol TEXT, timestamp INTEGER ms, open, high, low, close,
            volume, PRIMARY KEY(symbol,timestamp))

Uso:
  python3 backfill_backtest_f11.py \
    --symbols BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,SOLUSDT --days 90

Es operativo: no toca crypto_learning.db, MySQL ni el bot.
"""
import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE_URL = 'https://fapi.binance.com/fapi/v1/klines'
STEP_MS = 15 * 60 * 1000
LIMIT = 1500


def _get_json(url):
    req = urllib.request.Request(
        url, headers={'User-Agent': 'crypto-monitor-f11-backfill/1.0'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def descargar(symbol, start_ms, end_ms, base_url=BASE_URL):
    filas = []
    cursor = start_ms
    while cursor < end_ms:
        params = urllib.parse.urlencode({
            'symbol': symbol,
            'interval': '15m',
            'startTime': cursor,
            'endTime': end_ms,
            'limit': LIMIT,
        })
        try:
            data = _get_json(base_url + '?' + params)
        except urllib.error.HTTPError as e:
            detalle = ''
            try:
                detalle = e.read().decode('utf-8')[:200]
            except Exception:
                pass
            raise RuntimeError(
                f'{symbol}: HTTP {e.code} {e.reason} {detalle}') from e
        except Exception as e:
            raise RuntimeError(f'{symbol}: {e}') from e
        if not data:
            break
        filas.extend(data)
        ultimo = int(data[-1][0])
        if ultimo <= cursor:
            break
        cursor = ultimo + STEP_MS
        time.sleep(0.15)
    return filas


def init_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS velas_15m (
             symbol TEXT NOT NULL,
             timestamp INTEGER NOT NULL,
             open REAL, high REAL, low REAL, close REAL, volume REAL,
             PRIMARY KEY (symbol, timestamp)
           )""")
    conn.commit()
    return conn


def main():
    ap = argparse.ArgumentParser(
        description='Backfill 15m klines para backtest F11')
    ap.add_argument('--db', default='backtest_data.db')
    ap.add_argument('--symbols', required=True)
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--base-url', default=BASE_URL)
    args = ap.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(',')
               if s.strip()]
    if not symbols:
        raise SystemExit('sin simbolos')
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - args.days * 86400 * 1000
    print(f'[F11-BACKFILL] {args.days}d | {start_ms} -> {end_ms} ms')
    print(f'[F11-BACKFILL] DB: {args.db} | simbolos: {len(symbols)}')
    conn = init_db(args.db)
    total = 0
    try:
        for symbol in symbols:
            print(f'  descargando {symbol}...', end='', flush=True)
            filas = descargar(symbol, start_ms, end_ms, args.base_url)
            datos = []
            for k in filas:
                try:
                    datos.append((
                        symbol, int(k[0]), float(k[1]), float(k[2]),
                        float(k[3]), float(k[4]), float(k[5])))
                except (IndexError, TypeError, ValueError):
                    continue
            conn.executemany(
                'INSERT OR REPLACE INTO velas_15m '
                '(symbol,timestamp,open,high,low,close,volume) '
                'VALUES (?,?,?,?,?,?,?)', datos)
            conn.commit()
            total += len(datos)
            if datos:
                ini = datetime.fromtimestamp(
                    datos[0][1] / 1000, timezone.utc).strftime('%Y-%m-%d')
                fin = datetime.fromtimestamp(
                    datos[-1][1] / 1000, timezone.utc).strftime('%Y-%m-%d')
                print(f' {len(datos)} velas ({ini} -> {fin})')
            else:
                print(' 0 velas')
    finally:
        conn.close()
    print(f'[F11-BACKFILL] total insertado: {total}')
    print('[F11-BACKFILL] listo. Ejecutar backtest_f11.py')


if __name__ == '__main__':
    main()
