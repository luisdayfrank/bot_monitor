# -*- coding: utf-8 -*-
"""
PR-B: Diagnóstico de fees vs ganancias (STRICTLY READ-ONLY).

Solo ejecuta SELECT contra MySQL y lecturas de la API de Binance.
CERO INSERT/UPDATE/DELETE (la escritura de income_eventos vive en backfill_income.py).

Uso (en la máquina donde corre el bot):
    python diagnostico_fees.py                # diagnóstico completo (30 días)
    python diagnostico_fees.py --dias 14      # ventana alternativa
    python diagnostico_fees.py --testnet      # usa BINANCE_TESTNET_API_KEY/SECRET

Variables de entorno (mismos nombres/defaults que config.py):
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    BINANCE_API_KEY / BINANCE_API_SECRET   (o las *_TESTNET_* con --testnet)

Output: stdout + JSON local `diagnostico_fees_YYYYMMDD.json` junto al script.
NO escribe en CONFIG.audit_export_dir (ese directorio se limpia tras el envío
del reporte diario — un diagnóstico ahí desaparecería al día siguiente).
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pymysql

# ── Conexión (espejo de config.py:324-329, sin dependencia de pydantic) ──
DB_HOST = os.getenv("DB_HOST", "database-1.cnqsq0e6cf6z.eu-west-1.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1605584496")
DB_NAME = os.getenv("DB_NAME", "crypto_monitor")
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

INCOME_TYPES = ('FUNDING_FEE', 'REALIZED_PNL', 'COMMISSION')


def conectar():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, connect_timeout=DB_CONNECT_TIMEOUT, read_timeout=60,
        cursorclass=pymysql.cursors.DictCursor,
    )


def q(conn, sql, params=()):
    """SELECT único (read-only por diseño de este script)."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fmt(x, nd=4):
    return None if x is None else round(float(x), nd)


# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES DE DIAGNÓSTICO (solo SELECT) — siguiente.txt Paso 2 + deliverable 1
# ═══════════════════════════════════════════════════════════════════════════════

def diag_global(conn, ts_ini, ts_fin):
    """Q1 — respuesta global a ¿fees > ganancias?"""
    rows = q(conn, """
        SELECT
            ROUND(SUM(realized_pnl), 4)                              AS gross,
            ROUND(SUM(commission), 4)                                AS fees,
            ROUND(SUM(realized_pnl) - SUM(commission), 4)            AS net,
            ROUND(SUM(commission) / NULLIF(SUM(realized_pnl), 0), 4) AS fee_ratio,
            COUNT(*)                                                 AS trades,
            COUNT(DISTINCT symbol)                                   AS symbols,
            MIN(FROM_UNIXTIME(timestamp_ms/1000))                    AS primer_trade,
            MAX(FROM_UNIXTIME(timestamp_ms/1000))                    AS ultimo_trade
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
    """, (ts_ini, ts_fin))
    return dict(rows[0]) if rows else {}


def diag_por_dia(conn, ts_ini, ts_fin):
    """Q1b — evolución diaria (deliverable 1)."""
    rows = q(conn, """
        SELECT
            DATE(FROM_UNIXTIME(timestamp_ms/1000))                   AS dia,
            ROUND(SUM(realized_pnl), 4)                              AS gross,
            ROUND(SUM(commission), 4)                                AS fees,
            ROUND(SUM(realized_pnl) - SUM(commission), 4)            AS net,
            COUNT(*)                                                 AS trades
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
        GROUP BY dia
        ORDER BY dia
    """, (ts_ini, ts_fin))
    return [dict(r) for r in rows]


def diag_por_tipo_evento(conn, ts_ini, ts_fin):
    """Q2 — identifica el flujo taker (ABORTO/KILL_SWITCH vs round-trips)."""
    rows = q(conn, """
        SELECT
            tipo_evento,
            COUNT(*)                                                 AS n,
            ROUND(SUM(realized_pnl), 4)                              AS gross,
            ROUND(SUM(commission), 4)                                AS fees,
            ROUND(SUM(realized_pnl) - SUM(commission), 4)            AS net,
            ROUND(SUM(commission) / NULLIF(SUM(realized_pnl), 0), 4) AS fee_ratio
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
        GROUP BY tipo_evento
        ORDER BY fees DESC
    """, (ts_ini, ts_fin))
    return [dict(r) for r in rows]


def diag_por_symbol(conn, ts_ini, ts_fin):
    """Q3 — desglose por símbolo."""
    rows = q(conn, """
        SELECT
            symbol,
            COUNT(*)                                                 AS n,
            ROUND(SUM(realized_pnl), 4)                              AS gross,
            ROUND(SUM(commission), 4)                                AS fees,
            ROUND(SUM(realized_pnl) - SUM(commission), 4)            AS net
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
        GROUP BY symbol
        ORDER BY fees DESC
    """, (ts_ini, ts_fin))
    return [dict(r) for r in rows]


def diag_tasa_efectiva(conn, ts_ini, ts_fin):
    """Q4 — tasa efectiva commission/notional: maker vs taker REAL.

    Interpretación:
      ~0.0002  → mayoría maker sin BNB   |  ~0.00018 → maker con BNB
      ~0.0004  → mixto maker+taker       |  ~0.0005  → mayoría taker sin BNB
      ~0.00045 → mayoría taker con BNB
    """
    rows = q(conn, """
        SELECT
            symbol,
            ROUND(AVG(commission / NULLIF(notional, 0)), 6)  AS tasa_media,
            ROUND(MIN(commission / NULLIF(notional, 0)), 6)  AS tasa_min,
            ROUND(MAX(commission / NULLIF(notional, 0)), 6)  AS tasa_max,
            COUNT(*)                                         AS n_fills
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
          AND notional > 0
        GROUP BY symbol
        ORDER BY tasa_media DESC
    """, (ts_ini, ts_fin))
    return [dict(r) for r in rows]


def diag_commission_asset(conn, ts_ini, ts_fin):
    """Q5 — ¿descuento BNB activo? (commission_asset='BNB' dominante)."""
    rows = q(conn, """
        SELECT
            commission_asset,
            COUNT(*)                        AS n_fills,
            ROUND(SUM(commission), 8)       AS total_commission
        FROM pnl_eventos
        WHERE timestamp_ms >= %s AND timestamp_ms < %s
        GROUP BY commission_asset
    """, (ts_ini, ts_fin))
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# FUNDING (siguiente.txt Paso 3) — vía futures_income, NO está en pnl_eventos
# ═══════════════════════════════════════════════════════════════════════════════

def diag_funding(client, symbols, dias=30):
    """Ventana máx. 7 días por request (límite de Binance) — se itera."""
    ahora = datetime.now(timezone.utc)
    inicio = ahora - timedelta(days=dias)
    total_por_symbol = {}
    total_por_dia = {}
    total_global = 0.0
    n_eventos = 0

    for symbol in symbols:
        cursor_ts = int(inicio.timestamp() * 1000)
        ts_fin = int(ahora.timestamp() * 1000)
        acum_symbol = 0.0

        while cursor_ts < ts_fin:
            fin_ventana = min(cursor_ts + 7 * 86400_000, ts_fin)
            try:
                income = client.futures_income(
                    incomeType='FUNDING_FEE',
                    symbol=symbol,
                    startTime=cursor_ts,
                    endTime=fin_ventana,
                    limit=1000,
                )
            except Exception as e:
                print(f"  ⚠️ funding {symbol}: {e}")
                income = []

            for ev in income or []:
                monto = float(ev.get('income', 0))
                ts_ms = int(ev.get('time', 0))
                dia = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                acum_symbol += monto
                total_por_dia[dia] = total_por_dia.get(dia, 0.0) + monto
                n_eventos += 1

            cursor_ts = fin_ventana + 1
            time.sleep(0.2)  # rate limit

        total_por_symbol[symbol] = round(acum_symbol, 6)
        total_global += acum_symbol

    return {
        'dias': dias,
        'n_eventos': n_eventos,
        'total_global_usdt': round(total_global, 6),
        'por_symbol': total_por_symbol,
        'por_dia': dict(sorted(total_por_dia.items())),
    }


def hacer_client(testnet):
    """Misma selección de keys que executor.py:332-336."""
    try:
        from binance.client import Client
    except ImportError:
        return None, "paquete 'binance' no instalado — funding omitido"
    if testnet:
        key = os.getenv("BINANCE_TESTNET_API_KEY", "")
        secret = os.getenv("BINANCE_TESTNET_SECRET", "")
    else:
        key = os.getenv("BINANCE_API_KEY", "")
        secret = os.getenv("BINANCE_API_SECRET", "")
    if not key or not secret:
        return None, "sin API keys en env — funding omitido"
    return Client(key, secret, testnet=testnet), None


# ═══════════════════════════════════════════════════════════════════════════════
# VEREDICTO: ¿fees > ganancias? (condiciones.txt deliverable 2)
# ═══════════════════════════════════════════════════════════════════════════════

def emitir_veredicto(g):
    ratio = g.get('fee_ratio')
    if ratio is None:
        return ("INCONCLUSO: sin trades en la ventana o gross=0 — "
                "ampliar --dias o verificar filtros")
    if ratio > 1.0:
        return (f"CONFIRMADO: fees > ganancias (fee_ratio={ratio}). "
                "Las comisiones superan el PnL bruto real.")
    if ratio < 0.3:
        return (f"ARTEFACTO: fee_ratio={ratio} (<0.3) — la afirmación "
                "'fees > ganancias' era un artefacto del bug de contabilidad (PR-A). "
                "PR-D/E/F siguen siendo válidos pero la urgencia baja.")
    return (f"PARCIAL: fee_ratio={ratio} (0.3-1.0) — las fees consumen una fracción "
            "importante de las ganancias sin superarlas. PR-D/E/F justificados.")


def interpretar_tasa(t):
    if t is None:
        return "sin datos"
    if t <= 0.00021:
        return "mayoría MAKER sin BNB (0.02%) o con BNB si ~0.00018"
    if t <= 0.00030:
        return "mayoría maker, algo de taker"
    if t <= 0.00042:
        return "MIXTO maker+taker (o taker con BNB ~0.00036)"
    if t <= 0.00047:
        return "mayoría TAKER con BNB (0.05%×0.9)"
    return "mayoría TAKER sin BNB (0.05%)"


def main():
    ap = argparse.ArgumentParser(description="PR-B diagnóstico fees (read-only)")
    ap.add_argument("--dias", type=int, default=30)
    ap.add_argument("--testnet", action="store_true",
                    help="usa BINANCE_TESTNET_API_KEY/SECRET para funding")
    args = ap.parse_args()

    ahora = datetime.now(timezone.utc)
    ts_fin = int(ahora.timestamp() * 1000)
    ts_ini = int((ahora - timedelta(days=args.dias)).timestamp() * 1000)

    print(f"═══ PR-B DIAGNÓSTICO DE FEES ({args.dias} días) ═══")
    print(f"Ventana: {datetime.fromtimestamp(ts_ini/1000, timezone.utc)} → "
          f"{datetime.fromtimestamp(ts_fin/1000, timezone.utc)}")
    print(f"DB: {DB_HOST}/{DB_NAME} (read-only)\n")

    conn = conectar()
    try:
        # ── Condición 1: RDS vivo ──
        print("[OK] Conexión RDS establecida (SELECT 1)")

        g = diag_global(conn, ts_ini, ts_fin)
        print("\n── Q1 GLOBAL ──")
        for k in ('gross', 'fees', 'net', 'fee_ratio', 'trades', 'symbols',
                  'primer_trade', 'ultimo_trade'):
            print(f"  {k:15s}: {g.get(k)}")

        por_dia = diag_por_dia(conn, ts_ini, ts_fin)
        print(f"\n── Q1b POR DÍA ({len(por_dia)} días con trades) ──")
        for r in por_dia:
            print(f"  {r['dia']}: gross={r['gross']:+.4f} fees={r['fees']:.4f} "
                  f"net={r['net']:+.4f} trades={r['trades']}")

        por_tipo = diag_por_tipo_evento(conn, ts_ini, ts_fin)
        print("\n── Q2 POR TIPO DE EVENTO (flujo taker) ──")
        for r in por_tipo:
            print(f"  {r['tipo_evento']:15s}: n={r['n']:5d} gross={r['gross']:+.4f} "
                  f"fees={r['fees']:.4f} net={r['net']:+.4f} fee_ratio={r['fee_ratio']}")

        por_sym = diag_por_symbol(conn, ts_ini, ts_fin)
        print("\n── Q3 POR SÍMBOLO ──")
        for r in por_sym:
            print(f"  {r['symbol']:12s}: n={r['n']:5d} gross={r['gross']:+.4f} "
                  f"fees={r['fees']:.4f} net={r['net']:+.4f}")

        tasas = diag_tasa_efectiva(conn, ts_ini, ts_fin)
        print("\n── Q4 TASA EFECTIVA (commission/notional) ──")
        for r in tasas:
            print(f"  {r['symbol']:12s}: media={r['tasa_media']} "
                  f"[{r['tasa_min']}..{r['tasa_max']}] n={r['n_fills']}")
        t_global = None
        if tasas:
            t_global = q(conn, """
                SELECT ROUND(AVG(commission / NULLIF(notional, 0)), 6) AS t
                FROM pnl_eventos
                WHERE timestamp_ms >= %s AND timestamp_ms < %s AND notional > 0
            """, (ts_ini, ts_fin))[0]['t']
        print(f"  GLOBAL: {t_global} → {interpretar_tasa(t_global)}")

        assets = diag_commission_asset(conn, ts_ini, ts_fin)
        print("\n── Q5 COMMISSION ASSET (¿descuento BNB?) ──")
        for r in assets:
            print(f"  {r['commission_asset']}: n={r['n_fills']} total={r['total_commission']}")
        bnb_activo = any(r['commission_asset'] == 'BNB' for r in assets)
        print(f"  → Descuento BNB detectado: {'SÍ' if bnb_activo else 'NO'}")

        # ── FUNDING (hueco #1) ──
        client, err = hacer_client(args.testnet)
        if client is None:
            funding = {'error': err}
            print(f"\n── FUNDING ──\n  ⚠️ {err}")
        else:
            print("\n── FUNDING (futures_income, 30d) ──")
            symbols = [r['symbol'] for r in por_sym]
            if not symbols:
                # sin trades en la ventana: usar los símbolos históricos de la DB
                symbols = [r['symbol'] for r in q(conn,
                    "SELECT DISTINCT symbol FROM pnl_eventos ORDER BY symbol LIMIT 50", ())]
            funding = diag_funding(client, symbols, dias=args.dias)
            print(f"  eventos={funding['n_eventos']} "
                  f"total_global={funding['total_global_usdt']} USDT")
            print(f"  por_symbol={funding['por_symbol']}")
            if funding['total_global_usdt'] < 0:
                print("  → Estás PAGANDING funding: coste real que hoy nadie contabiliza.")
            elif funding['total_global_usdt'] > 0:
                print("  → Estás COBRANDO funding: el neto real (realized-fees-funding) "
                    "subestima tu rentabilidad.")
            else:
                print("  → Funding neutro: ignorable en el modelo de costes.")

        # ── VEREDICTO ──
        veredicto = emitir_veredicto(g)
        print(f"\n═══ VEREDICTO ¿fees > ganancias? ═══\n  {veredicto}")

        # ── Output JSON local (junto al script; NO en audit_export_dir) ──
        out = {
            'generado': ahora.isoformat(),
            'ventana': {'ts_ini': ts_ini, 'ts_fin': ts_fin, 'dias': args.dias},
            'q1_global': g,
            'q1b_por_dia': por_dia,
            'q2_por_tipo_evento': por_tipo,
            'q3_por_symbol': por_sym,
            'q4_tasa_efectiva': {'por_symbol': tasas, 'global': t_global,
                                 'interpretacion': interpretar_tasa(t_global)},
            'q5_commission_asset': {'filas': assets, 'bnb_descuento': bnb_activo},
            'funding': funding,
            'veredicto': veredicto,
        }
        fname = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             f"diagnostico_fees_{ahora.strftime('%Y%m%d')}.json")
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\n[OK] JSON guardado: {fname}")
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
