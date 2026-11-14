"""learning_dashboard.py - F10: agregacion read-only para la pestana Learning.

Consultas sobre crypto_learning.db (SQLite) + config/flags del proceso.
Sin dependencias de FastAPI; server_v5 expone el endpoint.
"""
import os
import time
from datetime import datetime, timedelta, timezone

try:
    import learning_db as ldb
except Exception:
    ldb = None

FLAG_FORCE = 'learning_force_run.flag'
FLAG_REVIEW = 'learning_revisar_guardrails.flag'


def _wilson(k, n, z=1.96):
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denom
    margen = (z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)) / denom
    return max(0.0, centro - margen), min(1.0, centro + margen)


def _dt_ms(value):
    if not value:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(str(value)[:19], fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _vacio(msg):
    return {"estado": "sin_datos", "mensaje": msg, "generado_ms": int(time.time() * 1000),
            "modo": {}, "f95_aplicado": False, "flags": {},
            "resumen": {}, "frescura": {}, "labels_por_dia": [],
            "entry_rules": [], "entry_rules_por_accion": [],
            "p2_count": 0, "b1_promotables": 0, "near_miss_por_tipo": [],
            "guardrails": [], "predictions": [], "patterns": [], "profiles": [],
            "schema": [], "bypass": []}


def _op():
    conn = ldb._get_conn()
    if conn is None:
        raise RuntimeError("learning_db no inicializada")

    def one(sql, args=(), default=0):
        try:
            row = conn.execute(sql, args).fetchone()
            return row[0] if row and row[0] is not None else default
        except Exception:
            return default

    def rows(sql, args=()):
        try:
            return conn.execute(sql, args).fetchall()
        except Exception:
            return []

    resumen = {
        'feature_snapshots': one("SELECT COUNT(*) FROM feature_snapshots"),
        'fire_labels': one("SELECT COUNT(*) FROM fire_labels"),
        'breakout_labels': one("SELECT COUNT(*) FROM breakout_labels"),
        'near_miss_labels': one("SELECT COUNT(*) FROM near_miss_labels"),
        'entry_rules_total': one("SELECT COUNT(*) FROM entry_rules"),
        'entry_rules_activas': one(
            "SELECT COUNT(*) FROM entry_rules WHERE is_active = 1"),
        'entry_rules_shadow': one(
            "SELECT COUNT(*) FROM entry_rules WHERE is_active = 0"),
        'bypass_rules_total': one("SELECT COUNT(*) FROM bypass_rules"),
        'bypass_rules_activas': one(
            "SELECT COUNT(*) FROM bypass_rules WHERE is_active = 1"),
        'learned_patterns': one("SELECT COUNT(*) FROM learned_patterns"),
        'coin_profiles': one("SELECT COUNT(*) FROM coin_profiles_learned"),
        'predictions_log': one("SELECT COUNT(*) FROM predictions_log"),
        'guardrail_events': one("SELECT COUNT(*) FROM guardrail_events"),
        'guardrail_pending': one(
            "SELECT COUNT(*) FROM guardrail_events "
            "WHERE tipo = 'ENTRY_RULE_BLOCK_RATE' "
            "AND revisado_por_humano = 0"),
        'drift_alerts': one("SELECT COUNT(*) FROM drift_alerts"),
        'exit_outcomes': one("SELECT COUNT(*) FROM exit_outcomes"),
        'trend_end_labels': one("SELECT COUNT(*) FROM trend_end_labels"),
        'regime_transitions': one("SELECT COUNT(*) FROM regime_transitions"),
    }

    # Entry rules: fetch all (para B1/P2 y tabla top)
    er = []
    for r in rows(
            "SELECT id,tipo_entrada,accion,symbol,direction,regime,"
            "condition_json,n_occurrences,win_rate,wilson_lower,"
            "lift_vs_baseline,is_active,created_at,updated_at "
            "FROM entry_rules"):
        try:
            item = {
                'id': r[0], 'tipo_entrada': r[1], 'accion': r[2],
                'symbol': r[3], 'direction': r[4], 'regime': r[5],
                'condition_json': r[6], 'n_occurrences': int(r[7] or 0),
                'win_rate': float(r[8] or 0), 'wilson_lower': float(r[9] or 0),
                'lift_vs_baseline': (None if r[10] is None else float(r[10])),
                'is_active': int(r[11] or 0), 'created_at': r[12],
                'updated_at': r[13],
            }
        except Exception:
            continue
        er.append(item)

    por_accion = {}
    p2_count = 0
    b1_promotables = 0
    ahora_ms = int(time.time() * 1000)
    for r in er:
        key = f"{r['tipo_entrada']}|{r['accion']}|{r['is_active']}"
        por_accion[key] = por_accion.get(key, 0) + 1
        if r['condition_json'] == '{}':
            p2_count += 1
        if r['accion'] == 'BLOCK':
            n = r['n_occurrences']
            wins = int(round(r['win_rate'] * n)) if n > 0 else 0
            _lo, hi = _wilson(wins, n) if n > 0 else (0.0, 1.0)
            created = _dt_ms(r['created_at'])
            if (not r['is_active'] and n >= 50 and hi <= 0.35
                    and created and (ahora_ms - created) >= 7 * 86400_000):
                b1_promotables += 1
    er.sort(key=lambda x: (x['is_active'], x['n_occurrences']), reverse=True)
    er_top = er[:100]

    near_tipos = [
        {'tipo_rechazo': r[0], 'n': r[1], 'falsos_negativos': r[2] or 0,
         'bloqueo_correcto': r[3] or 0}
        for r in rows(
            "SELECT tipo_rechazo, COUNT(*), SUM(falso_negativo), "
            "SUM(bloqueo_correcto) FROM near_miss_labels "
            "GROUP BY tipo_rechazo ORDER BY COUNT(*) DESC")
    ]

    guardrails = []
    for r in rows(
            "SELECT timestamp_ms,tipo,severidad,razon,accion_tomada,"
            "modo_antes,modo_despues,revisado_por_humano "
            "FROM guardrail_events ORDER BY timestamp_ms DESC LIMIT 10"):
        guardrails.append({
            'timestamp_ms': r[0], 'tipo': r[1], 'severidad': r[2],
            'razon': r[3], 'accion_tomada': r[4], 'modo_antes': r[5],
            'modo_despues': r[6], 'revisado': int(r[7] or 0),
        })

    predicciones = [
        {'modo': r[0] or '?', 'decision': r[1] or '?', 'n': r[2]}
        for r in rows(
            "SELECT modo, decision_tomada, COUNT(*) FROM predictions_log "
            "GROUP BY modo, decision_tomada ORDER BY COUNT(*) DESC")
    ]

    patrones = [
        {'status': r[0] or '?', 'n': r[1]}
        for r in rows("SELECT status, COUNT(*) FROM learned_patterns "
                      "GROUP BY status ORDER BY COUNT(*) DESC")
    ]

    perfiles = [
        {'symbol': r[0], 'generado_utc': r[1]}
        for r in rows("SELECT symbol, generado_utc FROM coin_profiles_learned "
                      "ORDER BY generado_utc DESC LIMIT 10")
    ]

    schema = [
        {'componente': r[0], 'version': r[1], 'activo': int(r[2] or 0),
         'aplicado_utc': r[3], 'changelog': r[4]}
        for r in rows("SELECT componente, version, activo, aplicado_utc, "
                      "changelog FROM schema_versions "
                      "ORDER BY aplicado_utc DESC LIMIT 12")
    ]

    bypass = [
        {'is_active': int(r[0] or 0), 'n': r[1]}
        for r in rows("SELECT is_active, COUNT(*) FROM bypass_rules "
                      "GROUP BY is_active")
    ]

    bypass_top = []
    for r in rows(
            "SELECT symbol,direction,regime,rejected_filter,condition_json,"
            "n_occurrences,win_rate,wilson_lower,is_active "
            "FROM bypass_rules ORDER BY is_active DESC, "
            "n_occurrences DESC LIMIT 50"):
        bypass_top.append({
            'symbol': r[0], 'direction': r[1], 'regime': r[2],
            'rejected_filter': r[3], 'condition_json': r[4],
            'n_occurrences': int(r[5] or 0),
            'win_rate': float(r[6] or 0),
            'wilson_lower': float(r[7] or 0),
            'is_active': int(r[8] or 0),
        })

    # Labels por dia (14d) desde created_at
    dias = {}
    hoy = datetime.now(timezone.utc).date()
    for i in range(13, -1, -1):
        d = (hoy - timedelta(days=i)).isoformat()
        dias[d] = {'fecha': d, 'fire': 0, 'breakout': 0, 'near_miss': 0}
    for tabla, clave in (('fire_labels', 'fire'),
                         ('breakout_labels', 'breakout'),
                         ('near_miss_labels', 'near_miss')):
        for r in rows(
                f"SELECT substr(created_at,1,10), COUNT(*) FROM {tabla} "
                "WHERE created_at >= date('now','-14 day') GROUP BY 1"):
            if r[0] in dias:
                dias[r[0]][clave] = r[1]
    labels_por_dia = [dias[k] for k in sorted(dias)]

    snap_max = one("SELECT MAX(timestamp_15m) FROM feature_snapshots")
    near_max = one("SELECT MAX(timestamp_inicio) FROM near_miss_labels")
    fire_max = one("SELECT MAX(timestamp_fire) FROM fire_labels")
    brk_max = one("SELECT MAX(timestamp_15m) FROM breakout_labels")
    frescura = {
        'snapshots_max_ms': snap_max,
        'near_miss_max_ms': near_max,
        'fire_max_ms': fire_max,
        'breakout_max_ms': brk_max,
        'snapshots_h': (round((ahora_ms - snap_max) / 3600_000, 1)
                        if snap_max else None),
        'near_miss_h': (round((ahora_ms - near_max) / 3600_000, 1)
                        if near_max else None),
        'entry_updated_max': one(
            "SELECT MAX(updated_at) FROM entry_rules", default=None),
    }

    f95 = bool(one("SELECT 1 FROM schema_versions WHERE "
                   "componente='ENTRY_RULES' AND version='v1.0_f95' "
                   "AND activo=1"))

    return {
        'resumen': resumen,
        'entry_rules': er_top,
        'entry_rules_por_accion': [
            {'key': k, 'n': v} for k, v in sorted(por_accion.items())
        ],
        'p2_count': p2_count,
        'b1_promotables': b1_promotables,
        'near_miss_por_tipo': near_tipos,
        'guardrails': guardrails,
        'predictions': predicciones,
        'patterns': patrones,
        'profiles': perfiles,
        'schema': schema,
        'bypass': bypass,
        'bypass_top': bypass_top,
        'labels_por_dia': labels_por_dia,
        'frescura': frescura,
        'f95_aplicado': f95,
    }


async def resumen(cfg=None):
    """Agrega el estado completo del Learning Engine para el dashboard."""
    if ldb is None:
        return _vacio("learning_db no importable")
    try:
        data = await ldb._run(_op)
        data['estado'] = 'success'
        data['generado_ms'] = int(time.time() * 1000)
        data['modo'] = {
            'apply': getattr(cfg, 'learning_apply_mode', 'OFF') if cfg else '?',
            'entry_rules': (getattr(cfg, 'learning_entry_rules_mode', 'OFF')
                            if cfg else '?'),
            'bypass': getattr(cfg, 'learning_bypass_mode', 'OFF') if cfg else '?',
            'exit': getattr(cfg, 'learning_exit_mode', 'OFF') if cfg else '?',
            'refusal_threshold': getattr(
                cfg, 'learning_refusal_threshold', None) if cfg else None,
        }
        data['flags'] = {
            'force_run': os.path.exists(FLAG_FORCE),
            'revisar_guardrails': os.path.exists(FLAG_REVIEW),
            'force_run_file': FLAG_FORCE,
            'revisar_guardrails_file': FLAG_REVIEW,
        }
        db_path = getattr(cfg, 'learning_db_path', 'crypto_learning.db') if cfg else 'crypto_learning.db'
        try:
            data['db_path'] = db_path
            data['db_mb'] = round(os.path.getsize(db_path) / (1024 * 1024), 1)
        except Exception:
            data['db_path'] = db_path
            data['db_mb'] = None
        return data
    except Exception as e:
        return _vacio(str(e))
