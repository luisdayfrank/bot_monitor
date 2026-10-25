"""
learning_db.py — FASE 0 · Learning Engine para Crypto Monitor V5.7+

Capa de persistencia sobre `crypto_learning.db` (SQLite, aislado de la DB de
trading MySQL/MariaDB). Nada aquí toca la base de datos del executor.

Correcciones aplicadas (ver informe de revisión):
  C-1: DB de trading es MySQL; la copia de velas se hace con un job de
       aplicación (upsert_velas_mirror), nunca con triggers cross-DBMS.
  §3C: columnas de fees / pnl_neto soportadas desde el día 1.

Uso:
    from learning_db import init_learning_db, close_learning_db, guardar_snapshots_batch
    await init_learning_db()
    ...
    await close_learning_db()
"""

import asyncio
import json
import os
import sqlite3
import threading
from typing import Iterable, Optional

_SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learning_schema.sql")

_db_path: Optional[str] = None
_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()          # serializa el acceso desde hilos de to_thread


# ═══════════════════════════════════════════════════════════════════════════
# Conexión
# ═══════════════════════════════════════════════════════════════════════════
def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        raise RuntimeError("learning_db no inicializado — llama a init_learning_db() primero")
    return _conn


async def init_learning_db(db_path: str = "crypto_learning.db"):
    """Abre (o crea) la DB de learning y ejecuta el schema completo."""
    global _db_path, _conn

    def _init():
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=60.0)
        conn.row_factory = sqlite3.Row
        with open(_SCHEMA_PATH, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
        modo = conn.execute("PRAGMA journal_mode").fetchone()[0]
        return conn, modo

    conn, modo = await _run(_init)
    _conn = conn
    _db_path = db_path
    await inicializar_schema_versions()
    print(f"  🧠 [LEARN-DB] crypto_learning.db abierta ({db_path}) | journal_mode={modo}")
    return modo


async def close_learning_db():
    global _conn
    if _conn is not None:
        def _close():
            try:
                _conn.close()
            except Exception:
                pass
        await _run(_close)
        _conn = None
        print("  🧠 [LEARN-DB] cerrada")


def _run(fn):
    """Ejecuta fn() en un hworker thread con el lock de la conexión."""
    def _wrapped():
        with _lock:
            return fn()
    return asyncio.to_thread(_wrapped)


# ═══════════════════════════════════════════════════════════════════════════
# Velas mirror (sync desde MySQL/MariaDB)
# ═══════════════════════════════════════════════════════════════════════════
async def upsert_velas_mirror(symbol: str, velas: Iterable[dict]):
    filas = [(symbol, int(v['timestamp']), v.get('open'), v.get('high'),
              v.get('low'), v.get('close'), v.get('volume')) for v in velas]
    if not filas:
        return 0

    def _op():
        conn = _get_conn()
        conn.executemany(
            """INSERT OR REPLACE INTO velas_15m_mirror
               (symbol, timestamp, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)""", filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_velas_mirror(symbol: str, desde_ms: int, hasta_ms: int):
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT timestamp, open, high, low, close, volume
               FROM velas_15m_mirror
               WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
               ORDER BY timestamp ASC""",
            (symbol, int(desde_ms), int(hasta_ms))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# feature_snapshots
# ═══════════════════════════════════════════════════════════════════════════
_SNAPSHOT_COLS = [
    'symbol', 'timestamp_15m', 'price', 'regime', 'regime_confirmado',
    'rsi_15m', 'adx_15m', 'atr_pct_15m', 'macd_hist', 'macd_hist_prev',
    'mfm_15m', 'mfm_sma5', 'volume_ratio',
    'price_rel_ema50_pct', 'price_rel_ema200_pct', 'ema7_ema25_spread_pct',
    'dist_to_recent_high_pct', 'dist_to_recent_low_pct',
    'ema50_slope_3v', 'adx_slope_3v', 'rsi_divergence',
    'rsi_7_1m', 'wick_upper_pct_1m', 'wick_lower_pct_1m',
    'body_direction_1m', 'mecha_short_1m', 'mecha_long_1m',
    'ema200_4h', 'close_4h', 'hora_sin', 'hora_cos', 'dia_semana',
    'score_macro', 'estado_maquina', 'evento_asociado', 'evento_id',
    'feature_schema_version',
]


async def guardar_snapshots_batch(rows: list):
    """INSERT OR IGNORE por lote — idempotente ante reconexiones."""
    if not rows:
        return 0
    filas = [tuple(r.get(c) for c in _SNAPSHOT_COLS) for r in rows]
    placeholders = ", ".join(["?"] * len(_SNAPSHOT_COLS))
    sql = f"INSERT OR IGNORE INTO feature_snapshots ({', '.join(_SNAPSHOT_COLS)}) VALUES ({placeholders})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_snapshots_sin_label(antes_de_ms: int, limit: int = 500):
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT fs.* FROM feature_snapshots fs
               LEFT JOIN pattern_labels pl ON pl.snapshot_id = fs.id
               WHERE pl.id IS NULL AND fs.timestamp_15m <= ?
               ORDER BY fs.timestamp_15m ASC LIMIT ?""",
            (int(antes_de_ms), int(limit))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# pattern_labels
# ═══════════════════════════════════════════════════════════════════════════
_LABEL_COLS = [
    'snapshot_id', 'symbol', 'timestamp_15m', 'ret_1h_pct', 'ret_4h_pct',
    'ret_8h_pct', 'ret_4h_neto_pct', 'mfe_pct', 'mae_pct',
    'up_excursion_pct', 'down_excursion_pct', 'mfe_minuto', 'mae_minuto',
    'label_class', 'labeler_version', 'labeler_params_json',
]


async def guardar_pattern_labels_batch(rows: list):
    if not rows:
        return 0
    filas = [tuple(r.get(c) for c in _LABEL_COLS) for r in rows]
    placeholders = ", ".join(["?"] * len(_LABEL_COLS))
    sql = f"INSERT OR REPLACE INTO pattern_labels ({', '.join(_LABEL_COLS)}) VALUES ({placeholders})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# learned_patterns / pattern_history
# ═══════════════════════════════════════════════════════════════════════════
async def guardar_learned_patterns_batch(rows: list):
    if not rows:
        return 0
    cols = ['pattern_key', 'symbol', 'regime', 'feature', 'bucket', 'rango_valor',
            'label_objetivo', 'p_local', 'p_baseline', 'lift', 'lift_lower',
            'p_value', 'n', 'n_entero', 'capa', 'status', 'key_format_version',
            'n_ventanas_valido',
            'peso_actual', 'primera_deteccion_utc', 'ultima_validacion_utc',
            'generado_utc']
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO learned_patterns ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def guardar_pattern_history_batch(rows: list):
    if not rows:
        return 0
    cols = ['pattern_key', 'symbol', 'regime', 'feature', 'bucket', 'label_objetivo',
            'lift', 'lift_lower', 'n', 'p_local', 'capa', 'status',
            'ventana_inicio_utc', 'registrado_utc']
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO pattern_history ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_learned_patterns(symbol: str = None, status: str = None):
    def _op():
        conn = _get_conn()
        sql = "SELECT * FROM learned_patterns WHERE 1=1"
        args = []
        if symbol:
            sql += " AND symbol = ?"
            args.append(symbol)
        if status:
            sql += " AND status = ?"
            args.append(status)
        sql += " ORDER BY lift DESC"
        return [dict(r) for r in conn.execute(sql, args).fetchall()]

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# coin_profiles_learned
# ═══════════════════════════════════════════════════════════════════════════
async def publicar_perfil(symbol: str, perfil: dict, generado_utc: str):
    def _op():
        conn = _get_conn()
        conn.execute(
            """INSERT INTO coin_profiles_learned (symbol, perfil_json, generado_utc)
               VALUES (?, ?, ?)
               ON CONFLICT(symbol) DO UPDATE SET
                 perfil_json = excluded.perfil_json,
                 generado_utc = excluded.generado_utc,
                 updated_at = CURRENT_TIMESTAMP""",
            (symbol, json.dumps(perfil, ensure_ascii=False), generado_utc))
        conn.commit()

    return await _run(_op)


async def cargar_perfil(symbol: str):
    def _op():
        conn = _get_conn()
        row = conn.execute(
            "SELECT perfil_json, generado_utc FROM coin_profiles_learned WHERE symbol = ?",
            (symbol,)).fetchone()
        if not row:
            return None
        return {"perfil": json.loads(row["perfil_json"]), "generado_utc": row["generado_utc"]}

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# regime_transitions
# ═══════════════════════════════════════════════════════════════════════════
async def guardar_regime_transitions_batch(rows: list):
    if not rows:
        return 0
    cols = ['symbol', 'timestamp_15m', 'from_regime', 'to_regime', 'adx_15m',
            'adx_slope_3v', 'mfm_sma5', 'mfm_flip', 'volume_ratio', 'rsi_15m',
            'atr_pct_15m', 'precio_antes', 'precio_despues',
            'duracion_regimen_previo_min', 'fue_transicion_real',
            'tiempo_confirmacion_min']
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO regime_transitions ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_transiciones_pendientes(antes_de_ms: int, limit: int = 500):
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT * FROM regime_transitions
               WHERE fue_transicion_real IS NULL AND timestamp_15m <= ?
               ORDER BY timestamp_15m ASC LIMIT ?""",
            (int(antes_de_ms), int(limit))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def marcar_transicion_real(transicion_id: int, fue_real: bool, minutos: int):
    def _op():
        conn = _get_conn()
        conn.execute(
            """UPDATE regime_transitions
               SET fue_transicion_real = ?, tiempo_confirmacion_min = ?
               WHERE id = ?""",
            (1 if fue_real else 0, int(minutos), int(transicion_id)))
        conn.commit()

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# exit_outcomes
# ═══════════════════════════════════════════════════════════════════════════
async def guardar_exit_outcome(row: dict):
    cols = ['posicion_id', 'symbol', 'grid_id', 'tipo', 'direccion',
            'timestamp_entrada', 'precio_entrada', 'qty', 'mfe_pct', 'mfe_minuto',
            'mae_pct', 'mae_minuto', 'timestamp_salida', 'precio_salida',
            'pnl_pct', 'pnl_usdt', 'fees_pct', 'pnl_neto_pct', 'pnl_neto_usdt',
            'capturado_pct_de_mfe', 'left_on_table_pct', 'razon_salida',
            'regime_entrada', 'regime_salida']
    vals = tuple(row.get(c) for c in cols)
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO exit_outcomes ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.execute(sql, vals)
        conn.commit()

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# trend_end_labels
# ═══════════════════════════════════════════════════════════════════════════
async def guardar_trend_end_labels_batch(rows: list):
    if not rows:
        return 0
    cols = ['symbol', 'timestamp_15m', 'regime_actual', 'adx_slope_3v', 'mfm_flip',
            'volume_ratio', 'rsi_divergence', 'distancia_ema50_pct', 'atr_pct_15m',
            'revirtio_2h', 'magnitud_reversion_pct', 'minutos_hasta_reversion',
            'umbral_reversion_pct', 'tenia_posicion_abierta', 'direccion_posicion']
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO trend_end_labels ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# predictions_log
# ═══════════════════════════════════════════════════════════════════════════
async def log_prediccion(row: dict):
    cols = ['symbol', 'timestamp_15m', 'model_version', 'prob_rentable',
            'prob_reversion', 'decision_sugerida', 'score_macro', 'direction',
            'regime', 'decision_tomada', 'modo']
    vals = tuple(row.get(c) for c in cols)
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO predictions_log ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.execute(sql, vals)
        conn.commit()

    return await _run(_op)


async def completar_outcomes_predicciones(antes_de_ms: int, limit: int = 2000):
    """Rellena outcome_* en predictions_log desde pattern_labels (job horario)."""
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT pl.id AS pid, pl.symbol, pl.timestamp_15m, pl.label_class, pl.ret_4h_neto_pct
               FROM pattern_labels pl
               JOIN predictions_log p
                 ON p.symbol = pl.symbol AND p.timestamp_15m = pl.timestamp_15m
               WHERE p.outcome_label IS NULL AND pl.timestamp_15m <= ?
               LIMIT ?""", (int(antes_de_ms), int(limit))).fetchall()
        for r in rows:
            conn.execute(
                """UPDATE predictions_log
                   SET outcome_label = ?, outcome_retorno_pct = ?, outcome_timestamp = ?
                   WHERE symbol = ? AND timestamp_15m = ?""",
                (r["label_class"], r["ret_4h_neto_pct"], int(r["timestamp_15m"]),
                 r["symbol"], int(r["timestamp_15m"])))
        conn.commit()
        return len(rows)

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# models / model_experiments (Fase 9-10, esquema listo)
# ═══════════════════════════════════════════════════════════════════════════
async def guardar_modelo(row: dict):
    def _op():
        conn = _get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO models
               (version, tipo, scope, filepath, features_list, metrics_json,
                umbral_recomendado, promovido)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (row['version'], row['tipo'], row.get('scope'), row.get('filepath'),
             row.get('features_list'), row.get('metrics_json'),
             row.get('umbral_recomendado'), 1 if row.get('promovido') else 0))
        conn.commit()

    return await _run(_op)


async def guardar_experimento(row: dict):
    def _op():
        conn = _get_conn()
        conn.execute(
            """INSERT INTO model_experiments
               (experimento_id, symbol, grupo, model_version, fecha_inicio,
                fecha_fin, metricas_json, promovido)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (row['experimento_id'], row['symbol'], row['grupo'],
             row.get('model_version'), row.get('fecha_inicio'),
             row.get('fecha_fin'), row.get('metrics_json'),
             1 if row.get('promovido') else 0))
        conn.commit()

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# Consultas analíticas (PatternLearner / backfill / backtest)
# ═══════════════════════════════════════════════════════════════════════════
async def cargar_dataset_labels(symbol: str, desde_ms: int, hasta_ms: int):
    """Snapshots + labels para minería de patrones (ventana temporal dada)."""
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT fs.timestamp_15m, fs.regime, fs.regime_confirmado, fs.price,
                      fs.rsi_15m, fs.adx_15m, fs.atr_pct_15m, fs.macd_hist,
                      fs.mfm_15m, fs.mfm_sma5, fs.volume_ratio,
                      fs.price_rel_ema50_pct, fs.price_rel_ema200_pct,
                      fs.ema7_ema25_spread_pct, fs.dist_to_recent_high_pct,
                      fs.dist_to_recent_low_pct, fs.ema50_slope_3v, fs.adx_slope_3v,
                      fs.rsi_divergence, fs.rsi_7_1m, fs.score_macro,
                      pl.label_class, pl.ret_4h_neto_pct
               FROM feature_snapshots fs
               JOIN pattern_labels pl ON pl.snapshot_id = fs.id
               WHERE fs.symbol = ? AND fs.timestamp_15m >= ? AND fs.timestamp_15m <= ?
               ORDER BY fs.timestamp_15m ASC""",
            (symbol, int(desde_ms), int(hasta_ms))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_trend_end_dataset(symbol: str, desde_ms: int, hasta_ms: int):
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT * FROM trend_end_labels
               WHERE symbol = ? AND timestamp_15m >= ? AND timestamp_15m <= ?
               ORDER BY timestamp_15m ASC""",
            (symbol, int(desde_ms), int(hasta_ms))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_snapshots_por_regimen(regimenes, desde_ms: int, hasta_ms: int,
                                       limit: int = 500):
    if isinstance(regimenes, str):
        regimenes = (regimenes,)
    regs = tuple(regimenes)
    ph = ", ".join(["?"] * len(regs))

    def _op():
        conn = _get_conn()
        rows = conn.execute(
            f"""SELECT fs.* FROM feature_snapshots fs
                WHERE fs.regime IN ({ph})
                  AND fs.timestamp_15m >= ? AND fs.timestamp_15m <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM trend_end_labels tel
                      WHERE tel.symbol = fs.symbol
                        AND tel.timestamp_15m = fs.timestamp_15m)
                ORDER BY fs.timestamp_15m ASC LIMIT ?""",
            regs + (int(desde_ms), int(hasta_ms), int(limit))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def contar_snapshots(symbol: str = None):
    def _op():
        conn = _get_conn()
        if symbol:
            return conn.execute(
                "SELECT COUNT(*) FROM feature_snapshots WHERE symbol = ?", (symbol,)).fetchone()[0]
        return conn.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()[0]

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# ANEXO A §2.1 — schema_versions (reproducibilidad)
# ═══════════════════════════════════════════════════════════════════════════
async def inicializar_schema_versions():
    """Inserta las versiones iniciales si no existen (idempotente)."""
    ahora = int(__import__('time').time() * 1000)
    semillas = [
        ('FEATURES', 'v1.0', 'Esquema inicial de features (informe §9 apéndice)'),
        ('LABELER', 'v1.0_ret8h', 'Labels ATR-normalizados, horizonte 8h, neto de fees'),
        ('PATTERNS', 'v1.0', 'Quintiles + decay 125d + Wilson + BH-FDR'),
    ]

    def _op():
        conn = _get_conn()
        for comp, ver, changelog in semillas:
            conn.execute(
                """INSERT OR IGNORE INTO schema_versions
                   (componente, version, aplicado_utc, changelog, activo)
                   VALUES (?, ?, ?, ?, 1)""", (comp, ver, ahora, changelog))
        conn.commit()
        return len(semillas)

    return await _run(_op)


async def registrar_schema_version(componente: str, version: str, changelog: str = ''):
    """Registra una versión nueva y la marca activa (las anteriores quedan inactivas)."""
    ahora = int(__import__('time').time() * 1000)

    def _op():
        conn = _get_conn()
        conn.execute(
            "UPDATE schema_versions SET activo = 0 WHERE componente = ?", (componente,))
        conn.execute(
            """INSERT OR REPLACE INTO schema_versions
               (componente, version, aplicado_utc, changelog, activo)
               VALUES (?, ?, ?, ?, 1)""", (componente, version, ahora, changelog))
        conn.commit()

    return await _run(_op)


# ═══════════════════════════════════════════════════════════════════════════
# ANEXO A §4/§5 — guardrails y drift
# ═══════════════════════════════════════════════════════════════════════════
async def contar_predicciones_periodo(decision: str = None, horas: float = 24.0):
    """Retorna (n_con_decision, n_total) en predictions_log para las últimas N horas."""
    desde = int(__import__('time').time() * 1000) - int(horas * 3600_000)

    def _op():
        conn = _get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM predictions_log WHERE timestamp_15m >= ?",
            (desde,)).fetchone()[0]
        if decision:
            n = conn.execute(
                "SELECT COUNT(*) FROM predictions_log WHERE timestamp_15m >= ? AND decision_tomada = ?",
                (desde, decision)).fetchone()[0]
        else:
            n = total
        return n, total

    return await _run(_op)


async def calcular_precision_rechazos(horas: float = 24.0):
    """
    Precisión de los rechazos del learner: fracción de FIREs rechazados cuyo
    outcome neto fue <= 0 (rechazo acertado). Retorna (precision|None, n).
    """
    desde = int(__import__('time').time() * 1000) - int(horas * 3600_000)

    def _op():
        conn = _get_conn()
        rows = conn.execute(
            """SELECT outcome_retorno_pct FROM predictions_log
               WHERE decision_tomada = 'REJECT'
                 AND outcome_retorno_pct IS NOT NULL
                 AND timestamp_15m >= ?""", (desde,)).fetchall()
        if not rows:
            return None, 0
        aciertos = sum(1 for r in rows if (r[0] or 0) <= 0)
        return aciertos / len(rows), len(rows)

    return await _run(_op)


async def avg_capturado_mfe(dias: float = 7.0):
    """capturado_pct_de_mfe medio de exit_outcomes cerradas en los últimos N días."""
    desde = int(__import__('time').time() * 1000) - int(dias * 86400_000)

    def _op():
        conn = _get_conn()
        row = conn.execute(
            """SELECT AVG(capturado_pct_de_mfe), COUNT(*) FROM exit_outcomes
               WHERE capturado_pct_de_mfe IS NOT NULL AND timestamp_salida >= ?""",
            (desde,)).fetchone()
        return (row[0], row[1]) if row else (None, 0)

    return await _run(_op)


async def guardar_guardrail_event(row: dict):
    cols = ['timestamp_ms', 'tipo', 'severidad', 'razon', 'accion_tomada',
            'modo_antes', 'modo_despues', 'revisado_por_humano']
    vals = tuple(row.get(c) for c in cols)
    ph = ", ".join(["?"] * len(cols))

    def _op():
        conn = _get_conn()
        conn.execute(
            f"INSERT INTO guardrail_events ({', '.join(cols)}) VALUES ({ph})", vals)
        conn.commit()

    return await _run(_op)


async def guardar_drift_alert(symbol: str, drift: dict):
    import json as _json
    ahora = int(__import__('time').time() * 1000)

    def _op():
        conn = _get_conn()
        conn.execute(
            """INSERT INTO drift_alerts
               (symbol, timestamp_ms, psi_por_feature_json, features_con_drift_json,
                acciones_tomadas, revisado_por_humano, created_at_ms)
               VALUES (?, ?, ?, ?, 'ninguna', 0, ?)""",
            (symbol, ahora, _json.dumps(drift.get('psi_por_feature', {})),
             _json.dumps(drift.get('features_con_drift', [])), ahora))
        conn.commit()

    return await _run(_op)


async def cargar_features_ventana(symbol: str, dias: float,
                                  features: tuple = ("rsi_15m", "adx_15m", "atr_pct_15m",
                                                     "mfm_sma5", "volume_ratio",
                                                     "price_rel_ema50_pct")):
    desde = int(__import__('time').time() * 1000) - int(dias * 86400_000)
    cols = ", ".join(features)

    def _op():
        conn = _get_conn()
        rows = conn.execute(
            f"SELECT {cols} FROM feature_snapshots WHERE symbol = ? AND timestamp_15m >= ?",
            (symbol, desde)).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)
