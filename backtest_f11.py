"""backtest_f11.py - F11/B7: esqueleto de backtest 90d (5 simbolos).

DB de investigacion (separada de produccion):
  tabla velas_15m(symbol TEXT, timestamp INTEGER, open REAL, high REAL,
                  low REAL, close REAL, volume REAL)
  timestamp en ms; una fila por vela de 15m.

El backfill de 90 dias es operativo (fuera de este script). Este script:
  1) valida cobertura/temporalidad por simbolo
  2) calcula baseline buy&hold neto de fees
  3) deja hook simular_learning() (estrategia F11 pendiente de aprobar)
  4) emite JSON con estado: PASS | PENDIENTE_ESTRATEGIA |
     DATOS_INSUFICIENTES | NO_MEJORA
No toca crypto_learning.db ni MySQL.
"""
import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

UMBRAL_MEJORA_PCT = 15.0
CANDLES_DIA_15M = 96
STEP_MS = 15 * 60 * 1000


def cargar_velas(conn, symbol, dias=90):
    desde = int((time.time() - dias * 86400) * 1000)
    return conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM velas_15m "
        "WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp",
        (symbol, desde)).fetchall()


def evaluar_cobertura(velas, dias=90):
    esperadas = dias * CANDLES_DIA_15M
    n = len(velas)
    gaps = 0
    duplicados = 0
    desordenados = 0
    prev = None
    for r in velas:
        ts = r[0]
        if prev is not None:
            if ts == prev:
                duplicados += 1
            elif ts < prev:
                desordenados += 1
            elif (ts - prev) > STEP_MS * 1.5:
                gaps += 1
        prev = ts
    return {
        'n': n,
        'esperadas': esperadas,
        'cobertura_pct': round(100.0 * n / esperadas, 2) if esperadas else 0.0,
        'gaps': gaps,
        'duplicados': duplicados,
        'desordenados': desordenados,
        'primer_ts': velas[0][0] if velas else None,
        'ultimo_ts': velas[-1][0] if velas else None,
    }


def metricas_buy_hold(velas, fee_rate=0.0005):
    closes = [float(r[4]) for r in velas if r[4] is not None]
    if len(closes) < 2 or closes[0] <= 0:
        return None
    bruto = (closes[-1] / closes[0] - 1.0) * 100.0
    fees = fee_rate * 2.0 * 100.0
    neto = bruto - fees
    peak = closes[0]
    max_dd = 0.0
    for c in closes:
        peak = max(peak, c)
        dd = (c / peak - 1.0) * 100.0
        max_dd = min(max_dd, dd)
    return {
        'pnl_neto_pct': round(neto, 4),
        'pnl_bruto_pct': round(bruto, 4),
        'fees_pct': round(fees, 4),
        'max_drawdown_pct': round(max_dd, 4),
        'n_velas': len(closes),
    }


def simular_learning(symbol, velas, fee_rate=0.0005):
    """HOOK F11: implementar aqui la estrategia aprobada.

    Debe retornar un dict con pnl_neto_pct/trades/win_rate o None si la
    estrategia aun no esta definida. El skeleton retorna None.
    """
    return None


def ejecutar(db_path, symbols, dias=90, min_cobertura=0.85,
             fee_rate=0.0005):
    reporte = {
        'estado': 'DATOS_INSUFICIENTES',
        'generado_utc': datetime.now(timezone.utc).isoformat(),
        'db': db_path,
        'dias': dias,
        'min_cobertura_pct': round(min_cobertura * 100.0, 2),
        'umbral_mejora_pct': UMBRAL_MEJORA_PCT,
        'simbolos': {},
        'nota': '',
    }
    if not os.path.exists(db_path):
        reporte['nota'] = 'DB de investigacion no existe; ejecutar backfill'
        return reporte
    conn = sqlite3.connect(db_path)
    try:
        existe = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='velas_15m'").fetchone()
        if not existe:
            reporte['nota'] = 'Falta tabla velas_15m; ejecutar backfill'
            return reporte
        insuficientes = []
        pendientes = []
        for symbol in symbols:
            velas = cargar_velas(conn, symbol, dias)
            cov = evaluar_cobertura(velas, dias)
            base = metricas_buy_hold(velas, fee_rate)
            learn = simular_learning(symbol, velas, fee_rate)
            reporte['simbolos'][symbol] = {
                'cobertura': cov,
                'baseline': base,
                'learning': learn,
            }
            if (base is None or
                    cov['cobertura_pct'] < min_cobertura * 100.0):
                insuficientes.append(symbol)
            if learn is None:
                pendientes.append(symbol)
        if insuficientes:
            reporte['estado'] = 'DATOS_INSUFICIENTES'
            reporte['nota'] = ('Cobertura insuficiente en: '
                               + ', '.join(insuficientes))
            return reporte
        if pendientes:
            reporte['estado'] = 'PENDIENTE_ESTRATEGIA'
            reporte['nota'] = ('Implementar simular_learning() para: '
                               + ', '.join(pendientes))
            return reporte
        mejoras = []
        for symbol in symbols:
            base = reporte['simbolos'][symbol]['baseline']['pnl_neto_pct']
            learn = reporte['simbolos'][symbol]['learning']['pnl_neto_pct']
            mejora = ((learn - base) / max(abs(base), 1e-9)) * 100.0
            reporte['simbolos'][symbol]['mejora_pct'] = round(mejora, 4)
            mejoras.append(mejora)
        if mejoras and min(mejoras) >= UMBRAL_MEJORA_PCT:
            reporte['estado'] = 'PASS'
            reporte['nota'] = 'Mejora >= umbral en todos los simbolos'
        else:
            reporte['estado'] = 'NO_MEJORA'
            reporte['nota'] = 'Mejora insuficiente vs umbral del plan'
        return reporte
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description='Backtest F11 (esqueleto)')
    ap.add_argument('--db', default='backtest_data.db')
    ap.add_argument('--symbols', required=True,
                    help='lista separada por comas (5 simbolos)')
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--min-coverage', type=float, default=0.85)
    ap.add_argument('--fee-rate', type=float, default=0.0005)
    ap.add_argument('--out', default='backtest_f11_report.json')
    args = ap.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(',')
               if s.strip()]
    reporte = ejecutar(args.db, symbols, args.days,
                       args.min_coverage, args.fee_rate)
    with open(args.out, 'w', encoding='utf-8') as fh:
        json.dump(reporte, fh, indent=2, ensure_ascii=False)
    print(f"[F11-B7] estado={reporte['estado']} | {reporte['nota']}")
    for s, d in reporte['simbolos'].items():
        cov = d['cobertura']['cobertura_pct']
        base = (d['baseline'] or {}).get('pnl_neto_pct')
        print(f"  {s}: cobertura={cov}% baseline_neto={base}%")
    print(f"[F11-B7] reporte: {args.out}")
    if reporte['estado'] == 'PASS':
        return 0
    if reporte['estado'] == 'DATOS_INSUFICIENTES':
        return 2
    if reporte['estado'] == 'PENDIENTE_ESTRATEGIA':
        return 3
    return 4


if __name__ == '__main__':
    raise SystemExit(main())
