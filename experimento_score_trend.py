"""
experimento_score_trend.py

Valida empíricamente si la TENDENCIA del score macro (score_trend_3v) es
predictiva del outcome de los FIREs.

Hipótesis H1:
    FIREs con score subiendo en las últimas 3 velas (score_trend_3v > +15%)
    tienen win rate significativamente MAYOR que FIREs con score estático
    (-5% <= score_trend_3v <= +5%).

Hipótesis H0 (nula):
    No hay diferencia significativa (lift ≈ 1.0, p > 0.05).

Uso:
    python experimento_score_trend.py                # defaults (90 días, 30 muestras)
    python experimento_score_trend.py --horas 180
    python experimento_score_trend.py --min-samples 50 --verbose
    python experimento_score_trend.py --out resultado.json

Solo lectura de crypto_learning.db. NO modifica tablas. Escribe JSON aparte.
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

DB_PATH_DEFAULT = "crypto_learning.db"
OUT_DEFAULT = "experimento_score_trend_resultado.json"

# Umbrales de clasificación de la tendencia (en % de cambio del score en 3 velas)
TREND_UP_THRESHOLD = 15.0
TREND_FLAT_BAND = 5.0
TREND_DOWN_THRESHOLD = -15.0

# Umbral mínimo de score para considerar el snapshot "candidato a FIRE"
# (fallback si no existe estado_maquina='FIRE')
SCORE_FIRE_FALLBACK = 70

# Madurez mínima: el pattern_label debe ser de al menos 4h después del snapshot
MADUREZ_LABEL_HORAS = 4


# ═══════════════════════════════════════════════════════════════════════════
# ESTADÍSTICA (stdlib only)
# ═══════════════════════════════════════════════════════════════════════════

def _phi(x):
    """CDF normal estándar."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _wilson(k, n, z=1.96):
    """Intervalo de Wilson para proporción k/n."""
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denom
    margen = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return max(0.0, centro - margen), min(1.0, centro + margen)


def _chi2_2x2(a, b, c, d):
    """
    p-valor aproximado de chi² para tabla 2x2 con corrección de Yates.
    a,b,c,d = conteos. H0: independencia. H1: grupos difieren.

    Ejemplo: a = wins_din, b = losses_din, c = wins_est, d = losses_est
    """
    n = a + b + c + d
    if n == 0:
        return 1.0
    num = n * (abs(a * d - b * c) - n / 2.0) ** 2
    den = (a + b) * (c + d) * (a + c) * (b + d)
    if den <= 0:
        return 1.0
    chi2 = num / den
    # p-valor aproximado: chi² con 1 grado de libertad → 2*(1-Φ(√chi²))
    z = math.sqrt(chi2)
    return 2.0 * (1.0 - _phi(z))


# ═══════════════════════════════════════════════════════════════════════════
# ACCESO A LA DB
# ═══════════════════════════════════════════════════════════════════════════

def _tabla_existe(conn, tabla):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (tabla,)).fetchone()
    return row is not None


def _columnas(conn, tabla):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({tabla})").fetchall()}


def _inspeccionar_db(conn):
    """Reporta qué tablas/columnas relevantes existen."""
    info = {
        'feature_snapshots': _tabla_existe(conn, 'feature_snapshots'),
        'pattern_labels': _tabla_existe(conn, 'pattern_labels'),
        'fire_labels': _tabla_existe(conn, 'fire_labels'),
    }
    if info['feature_snapshots']:
        cols = _columnas(conn, 'feature_snapshots')
        info['tiene_score_macro'] = 'score_macro' in cols
        info['tiene_estado_maquina'] = 'estado_maquina' in cols
        info['tiene_evento_asociado'] = 'evento_asociado' in cols
        info['tiene_regime_confirmado'] = 'regime_confirmado' in cols
    if info['pattern_labels']:
        cols = _columnas(conn, 'pattern_labels')
        info['tiene_ret_4h_neto'] = 'ret_4h_neto_pct' in cols
        info['tiene_ret_1h'] = 'ret_1h_pct' in cols
        info['tiene_mfe_mae'] = 'mfe_pct' in cols and 'mae_pct' in cols
    return info


# ═══════════════════════════════════════════════════════════════════════════
# CARGA Y RECONSTRUCCIÓN DE score_trend_3v
# ═══════════════════════════════════════════════════════════════════════════

def cargar_snapshots_con_labels(conn, horas, verbose=False):
    """
    Retorna lista de dicts con:
        symbol, timestamp_15m, score_macro, score_trend_3v,
        regime_confirmado, estado_maquina, evento_asociado,
        ret_4h_neto_pct, mfe_pct, mae_pct

    Prioridad:
        1. fire_labels (si existe) — más preciso
        2. feature_snapshots + pattern_labels (fallback)
    """
    ahora_ms = int(time.time() * 1000)
    ts_corte = ahora_ms - horas * 3600_000

    if _tabla_existe(conn, 'fire_labels'):
        cols = _columnas(conn, 'fire_labels')
        if 'ret_neto_4h_pct' in cols and 'score_trend_3v' in cols:
            print("  [FUENTE] fire_labels (ideal)")
            rows = conn.execute("""
                SELECT symbol, timestamp_fire, score_macro,
                       score_trend_3v, regime,
                       ret_neto_4h_pct AS ret_4h_neto_pct,
                       mfe_2h_pct AS mfe_pct, mae_2h_pct AS mae_pct
                FROM fire_labels
                WHERE timestamp_fire >= ? AND ret_neto_4h_pct IS NOT NULL
                ORDER BY symbol, timestamp_fire
            """, (ts_corte,)).fetchall()
            return [dict(r) for r in rows], 'fire_labels'

    # Fallback: feature_snapshots + pattern_labels
    if not _tabla_existe(conn, 'feature_snapshots') or not _tabla_existe(conn, 'pattern_labels'):
        return [], 'sin_tablas'

    fs_cols = _columnas(conn, 'feature_snapshots')
    has_estado = 'estado_maquina' in fs_cols
    has_evento = 'evento_asociado' in fs_cols
    has_regime = 'regime_confirmado' in fs_cols

    pl_cols = _columnas(conn, 'pattern_labels')
    if 'ret_4h_neto_pct' not in pl_cols:
        return [], 'sin_ret_4h_neto'

    # Query base
    select_extra = []
    if has_regime:
        select_extra.append("fs.regime_confirmado")
    else:
        select_extra.append("NULL as regime_confirmado")
    if has_estado:
        select_extra.append("fs.estado_maquina")
    else:
        select_extra.append("NULL as estado_maquina")
    if has_evento:
        select_extra.append("fs.evento_asociado")
    else:
        select_extra.append("NULL as evento_asociado")

    if 'mfe_pct' in pl_cols and 'mae_pct' in pl_cols:
        select_extra.append("pl.mfe_pct, pl.mae_pct")
    else:
        select_extra.append("NULL as mfe_pct, NULL as mae_pct")

    sql = f"""
        SELECT fs.id, fs.symbol, fs.timestamp_15m, fs.score_macro,
               {', '.join(select_extra)},
               pl.ret_4h_neto_pct
        FROM feature_snapshots fs
        JOIN pattern_labels pl ON pl.snapshot_id = fs.id
        WHERE fs.timestamp_15m >= ?
          AND fs.score_macro IS NOT NULL
          AND pl.ret_4h_neto_pct IS NOT NULL
        ORDER BY fs.symbol, fs.timestamp_15m
    """
    filas = [dict(r) for r in conn.execute(sql, (ts_corte,)).fetchall()]

    # Filtrar FIREs reales si hay columna
    # Prioridad: evento_asociado='FIRE' > estado_maquina='FIRE' > score>=70
    if has_evento and any(r.get('evento_asociado') == 'FIRE' for r in filas):
        filas = [r for r in filas if r.get('evento_asociado') == 'FIRE']
        fuente = 'feature_snapshots.evento_asociado=FIRE'
    elif has_estado and any(r.get('estado_maquina') == 'FIRE' for r in filas):
        filas = [r for r in filas if r.get('estado_maquina') == 'FIRE']
        fuente = 'feature_snapshots.estado_maquina=FIRE'
    else:
        filas = [r for r in filas if (r.get('score_macro') or 0) >= SCORE_FIRE_FALLBACK]
        fuente = f'feature_snapshots.score_macro>={SCORE_FIRE_FALLBACK}'

    if verbose:
        print(f"  [FUENTE] {fuente} | {len(filas)} snapshots FIRE con label")

    return filas, fuente


def reconstruir_score_trend_3v(filas):
    """
    Calcula score_trend_3v como (score[i] - score[i-3]) / max(1, |score[i-3]|) * 100.
    Opera POR SÍMBOLO (la tendencia no cruza monedas).

    Muta `filas` in-place añadiendo el campo. Retorna la misma lista.
    """
    por_symbol = defaultdict(list)
    for r in filas:
        por_symbol[r['symbol']].append(r)

    for sym, lista in por_symbol.items():
        lista.sort(key=lambda r: r['timestamp_15m'])
        for i in range(len(lista)):
            if i < 3:
                lista[i]['score_trend_3v'] = None
                continue
            s_i = lista[i]['score_macro']
            s_prev = lista[i - 3]['score_macro']
            if s_i is None or s_prev is None:
                lista[i]['score_trend_3v'] = None
                continue
            denom = max(1.0, abs(s_prev))
            lista[i]['score_trend_3v'] = (s_i - s_prev) / denom * 100.0
    return filas


# ═══════════════════════════════════════════════════════════════════════════
# CLASIFICACIÓN Y ANÁLISIS
# ═══════════════════════════════════════════════════════════════════════════

def clasificar(filas):
    """
    Retorna 3 grupos:
      - 'dinamico_alcista': score_trend_3v >  TREND_UP_THRESHOLD
      - 'estatico':         -TREND_FLAT_BAND <= score_trend_3v <= +TREND_FLAT_BAND
      - 'dinamico_bajista': score_trend_3v <  TREND_DOWN_THRESHOLD
    Descarta filas sin score_trend_3v o sin label.
    """
    din = []
    est = []
    baj = []
    descartados = 0
    for r in filas:
        st = r.get('score_trend_3v')
        lbl = r.get('ret_4h_neto_pct')
        if st is None or lbl is None:
            descartados += 1
            continue
        if st > TREND_UP_THRESHOLD:
            din.append(r)
        elif st < TREND_DOWN_THRESHOLD:
            baj.append(r)
        elif -TREND_FLAT_BAND <= st <= TREND_FLAT_BAND:
            est.append(r)
    return din, est, baj, descartados


def _outcome_stats(grupo, label_key='ret_4h_neto_pct'):
    """Resumen del outcome del grupo."""
    if not grupo:
        return None
    rets = [r[label_key] for r in grupo if r.get(label_key) is not None]
    if not rets:
        return None
    wins = sum(1 for r in rets if r > 0)
    n = len(rets)
    lo, hi = _wilson(wins, n)
    return {
        'n': n,
        'wins': wins,
        'win_rate': wins / n,
        'wilson_lower': lo,
        'wilson_upper': hi,
        'ret_media': sum(rets) / n,
        'ret_mediana': sorted(rets)[n // 2],
        'ret_max': max(rets),
        'ret_min': min(rets),
        'mfe_media': _media(grupo, 'mfe_pct'),
        'mae_media': _media(grupo, 'mae_pct'),
    }


def _media(grupo, key):
    vals = [r[key] for r in grupo if r.get(key) is not None]
    return sum(vals) / len(vals) if vals else None


def analizar_principal(din, est, baj):
    """Compara 'dinámico alcista' vs 'estático'. Retorna dict con conclusión."""
    a = _outcome_stats(din)
    b = _outcome_stats(est)
    c = _outcome_stats(baj)

    resultado = {
        'dinamico_alcista': a,
        'estatico': b,
        'dinamico_bajista': c,
    }

    if not a or not b:
        resultado['veredicto'] = 'INSUFICIENTE'
        resultado['razon'] = 'Faltan datos en uno o ambos grupos'
        return resultado

    # Test 2x2: din vs est
    wins_din = a['wins']; losses_din = a['n'] - a['wins']
    wins_est = b['wins']; losses_est = b['n'] - b['wins']
    p_valor = _chi2_2x2(wins_din, losses_din, wins_est, losses_est)

    lift = a['win_rate'] / b['win_rate'] if b['win_rate'] > 0 else float('inf')

    resultado['lift_alcista_vs_estatico'] = round(lift, 3) if math.isfinite(lift) else 'inf'
    resultado['p_valor_chi2'] = round(p_valor, 4)

    # Veredicto
    if lift >= 1.40 and p_valor < 0.05:
        resultado['veredicto'] = 'FEATURE_VALIOSA'
        resultado['razon'] = (
            f"score_trend_3v > +{TREND_UP_THRESHOLD}% tiene win_rate "
            f"{a['win_rate']:.1%} vs {b['win_rate']:.1%} estatico "
            f"(lift {lift:.2f}x, p={p_valor:.4f}) → AÑADIR feature")
    elif lift >= 1.20 and p_valor < 0.10:
        resultado['veredicto'] = 'PROMETEDORA'
        resultado['razon'] = (
            f"Lift {lift:.2f}x con p={p_valor:.3f}. "
            f"Necesita más datos para confirmar. Continuar acumulando.")
    elif lift <= 1.10 or p_valor > 0.20:
        resultado['veredicto'] = 'NO_VALIOSA'
        resultado['razon'] = (
            f"Sin diferencia significativa (lift {lift:.2f}x, p={p_valor:.4f}). "
            f"El pre-disparo como feature NO aporta información nueva.")
    else:
        resultado['veredicto'] = 'AMBIGUO'
        resultado['razon'] = f"Lift {lift:.2f}x, p={p_valor:.3f}"

    return resultado


def analizar_por_regimen(filas):
    """Bonus: misma comparación segmentada por régimen."""
    regs = defaultdict(list)
    for r in filas:
        regs[r.get('regime_confirmado') or 'UNKNOWN'].append(r)

    resultado = {}
    for reg, grupo in regs.items():
        din, est, _baj, _d = clasificar(grupo)
        if len(din) < 10 or len(est) < 10:
            continue
        a = _outcome_stats(din)
        b = _outcome_stats(est)
        if not a or not b:
            continue
        lift = a['win_rate'] / b['win_rate'] if b['win_rate'] > 0 else float('inf')
        p = _chi2_2x2(a['wins'], a['n'] - a['wins'],
                      b['wins'], b['n'] - b['wins'])
        resultado[reg] = {
            'n_din': a['n'], 'n_est': b['n'],
            'wr_din': round(a['win_rate'], 3),
            'wr_est': round(b['win_rate'], 3),
            'lift': round(lift, 3) if math.isfinite(lift) else 'inf',
            'p_valor': round(p, 4),
        }
    return resultado


# ═══════════════════════════════════════════════════════════════════════════
# REPORTE
# ═══════════════════════════════════════════════════════════════════════════

def _fmt(v, decimals=3):
    if v is None:
        return '  —  '
    if isinstance(v, float):
        return f"{v:.{decimals}f}"
    return str(v)


def reportar(resultado, fuente, inspeccion, out_path):
    print("\n" + "═" * 78)
    print("  EXPERIMENTO: ¿es predictiva la tendencia del score?")
    print("═" * 78)

    print(f"\n[INSPECCIÓN DB]")
    for k, v in inspeccion.items():
        print(f"  {k:30s} = {v}")
    print(f"\n[FUENTE DATOS] {fuente}")
    print(f"[HORAS ANALIZADAS] {resultado.get('horas_analizadas', '?')}")

    print("\n" + "─" * 78)
    print("  RESULTADO PRINCIPAL")
    print("─" * 78)

    grupos = [
        ('🟢 Dinámico alcista  (>+15%)', resultado.get('dinamico_alcista')),
        ('⚪ Estático       (±5%)',     resultado.get('estatico')),
        ('🔴 Dinámico bajista (<-15%)', resultado.get('dinamico_bajista')),
    ]
    for nombre, stats in grupos:
        if not stats:
            print(f"  {nombre:40s} — sin datos")
            continue
        print(f"  {nombre:40s}")
        print(f"    n={stats['n']:5d}  WR={stats['win_rate']:.1%}  "
              f"CI95=[{stats['wilson_lower']:.2f}, {stats['wilson_upper']:.2f}]")
        print(f"    ret_media={stats['ret_media']:+.3f}%  "
              f"ret_mediana={stats['ret_mediana']:+.3f}%  "
              f"MFE={_fmt(stats['mfe_media'])}  MAE={_fmt(stats['mae_media'])}")

    print()
    print(f"  Lift (din/est): {resultado.get('lift_alcista_vs_estatico', '?')}")
    print(f"  p-valor (chi²): {resultado.get('p_valor_chi2', '?')}")

    print()
    print("  VEREDICTO:", resultado.get('veredicto'))
    print("  RAZÓN:", resultado.get('razon'))

    por_reg = resultado.get('por_regimen') or {}
    if por_reg:
        print("\n" + "─" * 78)
        print("  SEGMENTACIÓN POR RÉGIMEN")
        print("─" * 78)
        print(f"  {'Régimen':20s} {'n_din':>6s} {'n_est':>6s} "
              f"{'WR_din':>8s} {'WR_est':>8s} {'lift':>6s} {'p':>8s}")
        for reg, d in sorted(por_reg.items()):
            print(f"  {reg:20s} {d['n_din']:6d} {d['n_est']:6d} "
                  f"{d['wr_din']:8.1%} {d['wr_est']:8.1%} "
                  f"{d['lift']:>6} {d['p_valor']:8.4f}")

    print("\n" + "═" * 78)
    print("  INTERPRETACIÓN")
    print("═" * 78)
    veredicto = resultado.get('veredicto')
    if veredicto == 'FEATURE_VALIOSA':
        print("""
  ✅ AÑADIR score_trend_3v como feature al plan blindado (Fase 1).

  Cómo integrarla:
    1. En SnapshotCollector._extraer_features:
         score_hist = list(self._score_hist[symbol])[-4:]
         if len(score_hist) >= 4:
             denom = max(1.0, abs(score_hist[0]))
             score_trend_3v = (score_hist[-1] - score_hist[0]) / denom * 100
    2. ALTER TABLE feature_snapshots ADD COLUMN score_trend_3v REAL
    3. MINING_FEATURES.append('score_trend_3v')
    4. Backfill: computar desde score_macro histórico (query diff)

  Qué esperar en el learning:
    - minar_bypasses puede descubrir: "rechazos por MFM_BAJO con score_trend_3v>15
      son falsos negativos más probables → bypass válido"
    - minar_fires puede descubrir: "FIREs con tendencia alcista del score tienen
      mejor win rate → priorizar requisitos cuando la tendencia es plana"
        """)
    elif veredicto == 'PROMETEDORA':
        print("""
  🟡 PROMETEDORA pero no concluyente.

  Acción:
    1. NO añadir aún la feature al plan (evitar overfitting).
    2. Correr el experimento de nuevo en 2-3 semanas con más datos.
    3. Añadir logging de score_trend_3v al SnapshotCollector AHORA (barato)
       para acumular histórico mientras decides.
    4. Si en 3 semanas lift >= 1.4 y p < 0.05 → integrar.
        """)
    elif veredicto == 'NO_VALIOSA':
        print("""
  ❌ NO añadir score_trend_3v al plan.

  El pre-disparo como feature NO aporta información nueva que el learning
  pueda explotar. Conclusión coherente con el análisis previo:
    "el pre-disparo como evento es redundante con MUY_CERCA_DEL_UMBRAL".

  Acciones:
    1. Mantener el pre-disparo SOLO como alerta Telegram (con anti-spam 30min).
    2. NO añadir score_trend_3v a feature_snapshots ni a MINING_FEATURES.
    3. Reutilizar las 3-4 horas de Fase 1 en features con más potencial
       (ER, BB width, ATR ratio ya están en el plan).
        """)
    else:
        print(f"  Veredicto: {veredicto}. Razón: {resultado.get('razon')}")

    print(f"\n[RESULTADO GUARDADO] {out_path}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Valida si score_trend_3v es predictiva")
    ap.add_argument('--db', default=DB_PATH_DEFAULT)
    ap.add_argument('--horas', type=int, default=24 * 90)
    ap.add_argument('--min-samples', type=int, default=30)
    ap.add_argument('--out', default=OUT_DEFAULT)
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"❌ DB no encontrada: {args.db}")
        print("   Asegúrate de correr desde el directorio del bot.")
        sys.exit(1)

    print(f"[1/5] Abriendo {args.db} (solo lectura)...")
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    inspeccion = _inspeccionar_db(conn)
    print(f"      {inspeccion}")

    print(f"[2/5] Cargando snapshots FIRE con label (últimas {args.horas}h)...")
    filas, fuente = cargar_snapshots_con_labels(conn, args.horas, args.verbose)
    if not filas:
        print("\n❌ Sin datos suficientes.")
        print("   Posibles causas:")
        print("     · El learning aún no ha etiquetado snapshots (necesita >=4h)")
        print("     · feature_snapshots vacío o pattern_labels vacío")
        print("     · Ningún FIRE con score_macro >= 70 en el período")
        print("\n   Alternativas:")
        print("     · Correr el bot más tiempo para acumular datos")
        print("     · Aumentar --horas (ej: --horas 180)")
        print("     · Revisar manualmente: SELECT COUNT(*) FROM pattern_labels;")
        sys.exit(0)
    print(f"      {len(filas)} snapshots FIRE con outcome 4h")

    print("[3/5] Reconstruyendo score_trend_3v (diff de score en 3 velas)...")
    reconstruir_score_trend_3v(filas)
    validos = sum(1 for f in filas if f.get('score_trend_3v') is not None)
    print(f"      {validos} con tendencia computable ({validos/len(filas)*100:.1f}%)")

    print("[4/5] Clasificando y analizando...")
    din, est, baj, descartados = clasificar(filas)
    if args.verbose:
        print(f"      Dinámicos alcistas: {len(din)}")
        print(f"      Estáticos:          {len(est)}")
        print(f"      Dinámicos bajistas: {len(baj)}")
        print(f"      Descartados:        {descartados}")

    if len(din) < args.min_samples or len(est) < args.min_samples:
        print(f"\n⚠️  Insuficientes muestras: din={len(din)}, est={len(est)}, "
              f"min={args.min_samples}")
        print("   Aumenta --horas o baja --min-samples.")

    resultado = analizar_principal(din, est, baj)
    resultado['horas_analizadas'] = args.horas
    resultado['fuente'] = fuente
    resultado['n_total_con_label'] = len(filas)
    resultado['n_din'] = len(din)
    resultado['n_est'] = len(est)
    resultado['n_baj'] = len(baj)
    resultado['por_regimen'] = analizar_por_regimen(filas)
    resultado['umbrales'] = {
        'trend_up_threshold': TREND_UP_THRESHOLD,
        'trend_flat_band': TREND_FLAT_BAND,
        'trend_down_threshold': TREND_DOWN_THRESHOLD,
    }
    resultado['timestamp'] = datetime.now(timezone.utc).isoformat()

    print("[5/5] Guardando resultado...")
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False, default=str)

    reportar(resultado, fuente, inspeccion, args.out)
    conn.close()


if __name__ == '__main__':
    main()
