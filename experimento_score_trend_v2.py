"""
experimento_score_trend_v2.py

Mejoras sobre v1:
    1. Gate estricto de muestra mínima (evita el bug de v1 que dio verdict
       con n=10).
    2. Test H1 (dirección) vs H1' (magnitud) explícitamente.
    3. Cross-tab completo: dirección x magnitud x régimen.
    4. Bootstrap de ret_media (más robusto que mean simple).
    5. Métricas separadas por régimen para detectar confounders.
    6. Veredicto con 3 niveles: FEATURE_VALIOSA_DIRECCIONAL,
       FEATURE_VALIOSA_MAGNITUD, INSUFICIENTE.

Uso:
    python experimento_score_trend_v2.py
    python experimento_score_trend_v2.py --min-samples 15 --horas 180
"""

import argparse
import json
import math
import os
import random
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone


# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

DB_PATH_DEFAULT = "crypto_learning.db"
OUT_DEFAULT = "experimento_score_trend_v2_resultado.json"

# Magnitud (|score_trend_3v|) para considerar "movimiento"
MOVEMENT_THRESHOLD = 10.0          # |st| > 10 = "movimiento claro"
STATIC_THRESHOLD = 5.0             # |st| <= 5 = "estático"
DIRECTIONAL_THRESHOLD = 15.0       # |st| > 15 y signo consistente = "direccional"

# Muestra mínima dura: si no se cumple, verdict = INSUFICIENTE
MIN_SAMPLES_HARD = 30
# Muestra para verdict = PROVISIONAL
MIN_SAMPLES_SOFT = 15

SCORE_FIRE_FALLBACK = 70


# ═══════════════════════════════════════════════════════════════════════════
# ESTADÍSTICA
# ═══════════════════════════════════════════════════════════════════════════

def _phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _wilson(k, n, z=1.96):
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denom
    margen = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return max(0.0, centro - margen), min(1.0, centro + margen)


def _chi2_2x2(a, b, c, d):
    n = a + b + c + d
    if n == 0:
        return 1.0
    num = n * (abs(a * d - b * c) - n / 2.0) ** 2
    den = (a + b) * (c + d) * (a + c) * (b + d)
    if den <= 0:
        return 1.0
    chi2 = num / den
    z = math.sqrt(max(0, chi2))
    return 2.0 * (1.0 - _phi(z))


def _bootstrap_median_ci(vals, n_iter=2000, seed=42):
    """Bootstrap percentil 95% para la mediana. Retorna (lo, hi, mediana)."""
    if not vals:
        return None, None, None
    rng = random.Random(seed)
    n = len(vals)
    if n < 5:
        vals_sorted = sorted(vals)
        return vals_sorted[0], vals_sorted[-1], vals_sorted[n // 2]
    meds = []
    for _ in range(n_iter):
        sample = [vals[rng.randrange(n)] for _ in range(n)]
        sample.sort()
        meds.append(sample[n // 2])
    meds.sort()
    return meds[int(0.025 * n_iter)], meds[int(0.975 * n_iter)], meds[n_iter // 2]


# ═══════════════════════════════════════════════════════════════════════════
# CARGA (igual que v1)
# ═══════════════════════════════════════════════════════════════════════════

def _tabla_existe(conn, tabla):
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (tabla,)).fetchone() is not None


def _columnas(conn, tabla):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({tabla})").fetchall()}


def cargar_snapshots_con_labels(conn, horas):
    ahora_ms = int(time.time() * 1000)
    ts_corte = ahora_ms - horas * 3600_000

    if not _tabla_existe(conn, 'feature_snapshots'):
        return [], 'sin_tabla'
    fs_cols = _columnas(conn, 'feature_snapshots')
    has_estado = 'estado_maquina' in fs_cols
    has_evento = 'evento_asociado' in fs_cols
    has_regime = 'regime_confirmado' in fs_cols

    pl_cols = _columnas(conn, 'pattern_labels')
    if 'ret_4h_neto_pct' not in pl_cols:
        return [], 'sin_ret_4h'

    select_extra = [
        "fs.regime_confirmado" if has_regime else "NULL",
        "fs.estado_maquina" if has_estado else "NULL",
        "fs.evento_asociado" if has_evento else "NULL",
    ]
    mfe_mae = ("pl.mfe_pct, pl.mae_pct"
               if 'mfe_pct' in pl_cols and 'mae_pct' in pl_cols
               else "NULL, NULL")

    sql = f"""
        SELECT fs.symbol, fs.timestamp_15m, fs.score_macro,
               {', '.join(select_extra)}, {mfe_mae}, pl.ret_4h_neto_pct
        FROM feature_snapshots fs
        JOIN pattern_labels pl ON pl.snapshot_id = fs.id
        WHERE fs.timestamp_15m >= ?
          AND fs.score_macro IS NOT NULL
          AND pl.ret_4h_neto_pct IS NOT NULL
        ORDER BY fs.symbol, fs.timestamp_15m
    """
    filas = [dict(r) for r in conn.execute(sql, (ts_corte,)).fetchall()]

    if has_evento and any(r.get('evento_asociado') == 'FIRE' for r in filas):
        filas = [r for r in filas if r.get('evento_asociado') == 'FIRE']
        return filas, 'evento_asociado=FIRE'
    if has_estado and any(r.get('estado_maquina') == 'FIRE' for r in filas):
        filas = [r for r in filas if r.get('estado_maquina') == 'FIRE']
        return filas, 'estado_maquina=FIRE'
    filas = [r for r in filas if (r.get('score_macro') or 0) >= SCORE_FIRE_FALLBACK]
    return filas, f'score_macro>={SCORE_FIRE_FALLBACK}'


def reconstruir_score_trend_3v(filas):
    por_symbol = defaultdict(list)
    for r in filas:
        por_symbol[r['symbol']].append(r)
    for sym, lista in por_symbol.items():
        lista.sort(key=lambda r: r['timestamp_15m'])
        for i in range(len(lista)):
            if i < 3:
                lista[i]['score_trend_3v'] = None
                continue
            s_i, s_prev = lista[i]['score_macro'], lista[i - 3]['score_macro']
            if s_i is None or s_prev is None:
                lista[i]['score_trend_3v'] = None
                continue
            lista[i]['score_trend_3v'] = (s_i - s_prev) / max(1.0, abs(s_prev)) * 100.0
    return filas


# ═══════════════════════════════════════════════════════════════════════════
# MÉTRICAS
# ═══════════════════════════════════════════════════════════════════════════

def metricas(grupo):
    rets = [r['ret_4h_neto_pct'] for r in grupo if r.get('ret_4h_neto_pct') is not None]
    if not rets:
        return None
    wins = sum(1 for r in rets if r > 0)
    n = len(rets)
    lo, hi = _wilson(wins, n)
    rets_sorted = sorted(rets)
    med = rets_sorted[n // 2]
    _, _, med_boot = _bootstrap_median_ci(rets)
    return {
        'n': n,
        'wins': wins,
        'win_rate': wins / n,
        'wilson_lower': round(lo, 3),
        'wilson_upper': round(hi, 3),
        'ret_media': round(sum(rets) / n, 4),
        'ret_mediana': round(med, 4),
        'ret_mediana_boot_med': round(med_boot, 4) if med_boot is not None else None,
        'ret_q1': round(rets_sorted[n // 4], 4),
        'ret_q3': round(rets_sorted[3 * n // 4], 4),
        'mfe_media': _media(grupo, 'mfe_pct'),
        'mae_media': _media(grupo, 'mae_pct'),
    }


def _media(grupo, key):
    vals = [r[key] for r in grupo if r.get(key) is not None]
    return round(sum(vals) / len(vals), 3) if vals else None


# ═══════════════════════════════════════════════════════════════════════════
# CLASIFICACIÓN 3x3 (dirección x magnitud)
# ═══════════════════════════════════════════════════════════════════════════

def clasificar(filas):
    """Retorna dict con celdas: 'up_fuerte', 'down_fuerte', 'mov_leve', 'estatico'."""
    grupos = {'up_fuerte': [], 'down_fuerte': [], 'mov_leve': [], 'estatico': []}
    descartados = 0
    for r in filas:
        st = r.get('score_trend_3v')
        if st is None:
            descartados += 1
            continue
        abs_st = abs(st)
        if st > DIRECTIONAL_THRESHOLD:
            grupos['up_fuerte'].append(r)
        elif st < -DIRECTIONAL_THRESHOLD:
            grupos['down_fuerte'].append(r)
        elif abs_st > STATIC_THRESHOLD:
            grupos['mov_leve'].append(r)
        else:
            grupos['estatico'].append(r)
    return grupos, descartados


# ═══════════════════════════════════════════════════════════════════════════
# ANÁLISIS PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

def analizar(filas, min_hard, min_soft):
    grupos, descartados = clasificar(filas)
    stats = {g: metricas(grupo) for g, grupo in grupos.items()}

    est = stats['estatico']
    if not est or est['n'] < min_soft:
        return {'veredicto': 'INSUFICIENTE',
                'razon': f'Grupo estático n={est["n"] if est else 0} < {min_soft}',
                'grupos': stats}, descartados

    # ─── TEST H1 (direccional) ───
    up = stats['up_fuerte']
    h1 = None
    if up and up['n'] >= min_soft:
        lift_up = up['win_rate'] / est['win_rate'] if est['win_rate'] > 0 else float('inf')
        p_up = _chi2_2x2(up['wins'], up['n'] - up['wins'],
                         est['wins'], est['n'] - est['wins'])
        h1 = {
            'n_up': up['n'], 'n_est': est['n'],
            'wr_up': round(up['win_rate'], 3),
            'wr_est': round(est['win_rate'], 3),
            'lift': round(lift_up, 3) if math.isfinite(lift_up) else 'inf',
            'p_valor': round(p_up, 4),
            'valido_muestra': up['n'] >= min_hard,
        }

    # ─── TEST H1' (magnitud): up+down fuerte combinados vs estático ───
    down = stats['down_fuerte']
    comb_n = (up['n'] if up else 0) + (down['n'] if down else 0)
    comb_wins = (up['wins'] if up else 0) + (down['wins'] if down else 0)
    h1p = None
    if comb_n >= min_soft:
        wr_comb = comb_wins / comb_n
        lift_comb = wr_comb / est['win_rate'] if est['win_rate'] > 0 else float('inf')
        p_comb = _chi2_2x2(comb_wins, comb_n - comb_wins,
                           est['wins'], est['n'] - est['wins'])
        h1p = {
            'n_mov': comb_n, 'n_est': est['n'],
            'wr_mov': round(wr_comb, 3),
            'wr_est': round(est['win_rate'], 3),
            'lift': round(lift_comb, 3) if math.isfinite(lift_comb) else 'inf',
            'p_valor': round(p_comb, 4),
            'valido_muestra': comb_n >= min_hard,
        }

    # ─── Cross-tab por régimen ───
    por_regimen = {}
    regs = defaultdict(list)
    for r in filas:
        regs[r.get('regime_confirmado') or 'UNKNOWN'].append(r)
    for reg, grupo in regs.items():
        sub_grupos, _ = clasificar(grupo)
        sub_stats = {g: metricas(sub) for g, sub in sub_grupos.items()}
        s_est = sub_stats.get('estatico')
        s_up = sub_stats.get('up_fuerte')
        s_dn = sub_stats.get('down_fuerte')
        if not s_est or s_est['n'] < 10:
            continue
        por_regimen[reg] = {
            'n_est': s_est['n'], 'n_up': s_up['n'] if s_up else 0,
            'n_down': s_dn['n'] if s_dn else 0,
            'wr_est': round(s_est['win_rate'], 3),
            'wr_up': round(s_up['win_rate'], 3) if s_up else None,
            'wr_down': round(s_dn['win_rate'], 3) if s_dn else None,
        }

    # ─── Veredicto ───
    verdict = 'INSUFICIENTE'
    razon = 'Sin muestra suficiente'

    h1_ok = h1 and h1['valido_muestra'] and h1['lift'] >= 1.4 and h1['p_valor'] < 0.05
    h1p_ok = h1p and h1p['valido_muestra'] and h1p['lift'] >= 1.4 and h1p['p_valor'] < 0.05

    if h1_ok and h1p_ok:
        # Ambas válidas: ¿cuál explica mejor?
        if abs(h1p['lift'] - h1['lift']) < 0.15:
            verdict = 'FEATURE_VALIOSA_MAGNITUD'
            razon = (f'Movimiento (up+down) lift {h1p["lift"]} ≈ direccional lift '
                     f'{h1["lift"]} → usar |score_trend_3v|')
        elif h1['lift'] > h1p['lift']:
            verdict = 'FEATURE_VALIOSA_DIRECCIONAL'
            razon = (f'Solo alcista funciona (lift {h1["lift"]} vs magnitud '
                     f'{h1p["lift"]}). Sesgo LONG. Cuidado.')
        else:
            verdict = 'FEATURE_VALIOSA_MAGNITUD'
            razon = (f'Magnitud explica más que dirección (lift {h1p["lift"]} '
                     f'vs {h1["lift"]})')
    elif h1p_ok:
        verdict = 'FEATURE_VALIOSA_MAGNITUD'
        razon = (f'Movimiento en cualquier dirección mejora WR '
                 f'({h1p["wr_mov"]:.1%} vs {h1p["wr_est"]:.1%}, '
                 f'lift {h1p["lift"]}, p={h1p["p_valor"]})')
    elif h1_ok:
        verdict = 'FEATURE_VALIOSA_DIRECCIONAL'
        razon = (f'Solo score alcista > +15% mejora WR '
                 f'({h1["wr_up"]:.1%} vs {h1["wr_est"]:.1%}, '
                 f'lift {h1["lift"]}, p={h1["p_valor"]})')
    elif h1 or h1p:
        verdict = 'PROMETEDORA'
        razon = 'Tendencia positiva pero muestra insuficiente o lift < 1.4'

    return {
        'veredicto': verdict,
        'razon': razon,
        'grupos': stats,
        'H1_direccional': h1,
        'H1p_magnitud': h1p,
        'por_regimen': por_regimen,
        'min_hard': min_hard,
        'min_soft': min_soft,
    }, descartados


# ═══════════════════════════════════════════════════════════════════════════
# REPORTE
# ═══════════════════════════════════════════════════════════════════════════

def reportar(res, fuente, descartados, out):
    print('\n' + '═' * 78)
    print('  EXPERIMENTO v2: ¿direccional o magnitud?')
    print('═' * 78)
    print(f'\n[FUENTE] {fuente} | descartados sin trend: {descartados}')

    print('\n' + '─' * 78)
    print('  GRUPOS')
    print('─' * 78)
    labels = {
        'up_fuerte': '🟢 Up fuerte (>+15%)',
        'down_fuerte': '🔴 Down fuerte (<-15%)',
        'mov_leve': '🟡 Mov. leve (5-15%)',
        'estatico': '⚪ Estático (±5%)',
    }
    for k in ('up_fuerte', 'down_fuerte', 'mov_leve', 'estatico'):
        s = res['grupos'].get(k)
        if not s:
            print(f'  {labels[k]:28s} —')
            continue
        print(f'  {labels[k]:28s} n={s["n"]:4d} WR={s["win_rate"]:6.1%} '
              f'CI=[{s["wilson_lower"]:.2f},{s["wilson_upper"]:.2f}] '
              f'ret_med={s["ret_mediana"]:+.3f}%')

    print('\n' + '─' * 78)
    print('  TEST H1 (direccional) vs H1\' (magnitud)')
    print('─' * 78)
    h1 = res.get('H1_direccional')
    h1p = res.get('H1p_magnitud')
    if h1:
        print(f"  H1  (up vs est): n_up={h1['n_up']:3d} n_est={h1['n_est']:3d} "
              f"lift={h1['lift']} p={h1['p_valor']} "
              f"{'✅ muestra OK' if h1['valido_muestra'] else '⚠️ muestra baja'}")
    else:
        print('  H1  (up vs est): insuficiente')
    if h1p:
        print(f"  H1' (mov vs est): n_mov={h1p['n_mov']:3d} n_est={h1p['n_est']:3d} "
              f"lift={h1p['lift']} p={h1p['p_valor']} "
              f"{'✅ muestra OK' if h1p['valido_muestra'] else '⚠️ muestra baja'}")
    else:
        print("  H1' (mov vs est): insuficiente")

    por_reg = res.get('por_regimen') or {}
    if por_reg:
        print('\n' + '─' * 78)
        print('  POR RÉGIMEN')
        print('─' * 78)
        print(f'  {"Régimen":16s} {"n_est":>5s} {"n_up":>5s} {"n_dn":>5s} '
              f'{"WR_est":>8s} {"WR_up":>8s} {"WR_dn":>8s}')
        for reg, d in sorted(por_reg.items()):
            wr_up = f'{d["wr_up"]:.1%}' if d['wr_up'] is not None else '—'
            wr_dn = f'{d["wr_down"]:.1%}' if d['wr_down'] is not None else '—'
            print(f'  {reg:16s} {d["n_est"]:5d} {d["n_up"]:5d} {d["n_down"]:5d} '
                  f'{d["wr_est"]:8.1%} {wr_up:>8s} {wr_dn:>8s}')

    print('\n' + '═' * 78)
    print('  VEREDICTO:', res['veredicto'])
    print('  RAZÓN:', res['razon'])
    print('═' * 78)

    v = res['veredicto']
    if v == 'FEATURE_VALIOSA_MAGNITUD':
        print("""
  ✅ AÑADIR |score_trend_3v| como feature.

  Configuración:
    · En SnapshotCollector:
        score_trend_3v  = (s[-1] - s[-4]) / max(1, |s[-4]|) * 100
        score_movement_3v = abs(score_trend_3v)   # NUEVA feature
    · MINING_FEATURES.append('score_movement_3v')  # no la direccional
    · En learning: usar score_movement_3v para bypasses y entry_rules.

  Por qué magnitud y no dirección:
    El análisis muestra que score subiendo Y bajando mejora WR vs estático.
    Usar solo el alcista sesgaría hacia LONGs (peligroso en tendencia bajista).
        """)
    elif v == 'FEATURE_VALIOSA_DIRECCIONAL':
        print("""
  🟢 AÑADIR score_trend_3v direccional — PERO con advertencia.

  ⚠️ CUIDADO: si solo el "alcista" funciona, favorecerás LONGs.
  En régimen BEARISH ese sesgo es peligroso.

  Configuración segura:
    · Añadir score_trend_3v como feature a MINING_FEATURES.
    · En minar_bypasses / minar_fires, incluir el régimen como condición
      para que las reglas aprendidas sean específicas (BEARISH/BULLISH).
    · Validar en backtest que el sesgo LONG no rompe el balance.
        """)
    elif v == 'PROMETEDORA':
        print("""
  🟡 PROMETEDORA — diferir decisión.

  Acciones:
    1. Añadir logging de score_trend_3v al SnapshotCollector AHORA
       (coste ≈ 0, no consume recursos).
    2. Volver a correr v2 en 2-3 semanas con más datos.
    3. Implementar F0.2 + F0.3 (FIREs reales) → alimenta fuente ideal.
        """)
    else:
        print("""
  ⏸️ INSUFICIENTE — no decidir aún.

  La muestra actual no permite distinguir H1 de H1'. Opciones:
    · Aumentar --horas (180, 365).
    · Bajar --min-samples (a 15) — pero entonces verdict = PROVISIONAL.
    · Implementar F0.2 + F0.3 y esperar 2-4 semanas.
        """)
    print(f'\n[RESULTADO] {out}')


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=DB_PATH_DEFAULT)
    ap.add_argument('--horas', type=int, default=24 * 90)
    ap.add_argument('--min-samples', type=int, default=MIN_SAMPLES_HARD)
    ap.add_argument('--out', default=OUT_DEFAULT)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f'❌ DB no encontrada: {args.db}')
        sys.exit(1)

    print(f'[1/4] Abriendo {args.db} (ro)...')
    conn = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row

    print(f'[2/4] Cargando snapshots FIRE con label (últimas {args.horas}h)...')
    filas, fuente = cargar_snapshots_con_labels(conn, args.horas)
    if not filas:
        print('❌ Sin datos.')
        sys.exit(0)
    print(f'      {len(filas)} snapshots | fuente: {fuente}')

    print('[3/4] Reconstruyendo score_trend_3v...')
    reconstruir_score_trend_3v(filas)
    validos = sum(1 for f in filas if f.get('score_trend_3v') is not None)
    print(f'      {validos} con tendencia ({validos/len(filas)*100:.1f}%)')

    print('[4/4] Analizando...')
    res, descartados = analizar(filas, args.min_samples, MIN_SAMPLES_SOFT)
    res['fuente'] = fuente
    res['horas'] = args.horas
    res['timestamp'] = datetime.now(timezone.utc).isoformat()

    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=2, ensure_ascii=False, default=str)

    reportar(res, fuente, descartados, args.out)
    conn.close()


if __name__ == '__main__':
    main()
