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
import time
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
    # F4.3: Añadir snapshot_id a predictions_log (idempotente para DBs existentes)
    def _add_snapshot_id_col():
        try:
            _conn.execute("ALTER TABLE predictions_log ADD COLUMN snapshot_id INTEGER")
            _conn.commit()
        except Exception:
            pass  # columna ya existe
        try:
            _conn.execute("CREATE INDEX IF NOT EXISTS idx_predlog_snapshot ON predictions_log(snapshot_id)")
            _conn.commit()
        except Exception:
            pass  # indice ya existe
    await _run(_add_snapshot_id_col)
    # P0-2: Añadir direction a near_miss_labels (idempotente para DBs existentes)
    def _add_direction_col():
        try:
            _conn.execute("ALTER TABLE near_miss_labels ADD COLUMN direction TEXT")
            _conn.commit()
        except Exception:
            pass  # columna ya existe
    await _run(_add_direction_col)
    # F1.2: 9 columnas v1.1 en feature_snapshots (idempotente)
    def _add_features_v11_cols():
        cols = [
            'bb_width_pct REAL', 'bb_width_pct_pct REAL', 'pct_b REAL',
            'er_15m REAL', 'donchian_high_20 REAL', 'donchian_low_20 REAL',
            'atr_ratio_1m_15m REAL', 'roc_4v_pct REAL', 'vwap_24h REAL',
        ]
        for col in cols:
            try:
                _conn.execute(f"ALTER TABLE feature_snapshots ADD COLUMN {col}")
            except Exception:
                pass  # columna ya existe
        try:
            _conn.commit()
        except Exception:
            pass
    await _run(_add_features_v11_cols)
    # F1.3 (I44): version activa del componente FEATURES
    try:
        await registrar_schema_version(
            'FEATURES', 'v1.1',
            'F1: BB/%b/ER/Donchian/ATR-ratio/ROC/VWAP + backfill')
    except Exception as e:
        print(f"  [LEARN-DB] F1.2 registro FEATURES v1.1: {e}")
    # 3.2 (convención schema_versions §2.1): registrar el cambio de esquema
    def _register_direction_version():
        try:
            _conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('LABELER', 'v1.1_direction', ?, 'P0-2: columna direction en near_miss_labels', 1)",
                (int(__import__('time').time() * 1000),))
            _conn.commit()
        except Exception:
            pass  # ya registrado / tabla no disponible
    await _run(_register_direction_version)
    # P3-5: purga one-shot de bypass_rules pre-fix (reglas minadas con el bucket
    # colapsado (UNKNOWN, NEUTRAL, DESCONOCIDO) — estadísticamente inválidas).
    # Se ejecuta UNA vez (marcado en schema_versions); el bypass aprende limpio desde cero.
    def _purgar_bypass_rules_legacy():
        try:
            ya = _conn.execute(
                "SELECT 1 FROM schema_versions WHERE componente = 'PATTERNS' AND version = 'v1.1_bypass_purge'"
            ).fetchone()
            if ya:
                return
            n = _conn.execute("SELECT COUNT(*) FROM bypass_rules").fetchone()[0]
            _conn.execute("DELETE FROM bypass_rules")
            _conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('PATTERNS', 'v1.1_bypass_purge', ?, 'P3-5: purga de bypass_rules pre-fix (bucket colapsado)', 1)",
                (int(__import__('time').time() * 1000),))
            _conn.commit()
            print(f"  🧠 [LEARN-DB] P3-5: purgadas {n} bypass_rules inválidas (re-aprendizaje limpio)")
        except Exception as e:
            print(f"  ⚠️ P3-5 purga bypass_rules: {e}")
    await _run(_purgar_bypass_rules_legacy)
    # F0.4: purga one-shot v1.2 — reglas minadas con mineria sesgada
    # (win_rate=1.0 por solo-FN) y condition_json doble-codificado.
    # Re-minado limpio con baseline+lift (F0.1). Una sola vez (schema_versions).
    def _purgar_bypass_rules_v2():
        try:
            ya = _conn.execute(
                "SELECT 1 FROM schema_versions WHERE componente = 'PATTERNS' "
                "AND version = 'v1.2_bypass_purge'"
            ).fetchone()
            if ya:
                return
            n = _conn.execute("SELECT COUNT(*) FROM bypass_rules").fetchone()[0]
            _conn.execute("DELETE FROM bypass_rules")
            _conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('PATTERNS', 'v1.2_bypass_purge', ?, "
                "'F0: re-minado con baseline+lift; reglas previas win_rate=1.0', 1)",
                (int(time.time() * 1000),))
            _conn.commit()
            print(f"  [LEARN-DB] F0.4: purgadas {n} bypass_rules (v1.2)")
        except Exception as e:
            print(f"  [LEARN-DB] F0.4 purga v1.2: {e}")
    await _run(_purgar_bypass_rules_v2)
    # I68: normalizar tipo_rechazo (texto libre -> codigos minables)
    def _normalizar_tipos_rechazo_v13():
        try:
            ya = _conn.execute(
                "SELECT 1 FROM schema_versions WHERE componente = 'PATTERNS' "
                "AND version = 'v1.3_norm_tipos'").fetchone()
            if ya:
                return
            _conn.execute("""
                UPDATE near_miss_labels SET tipo_rechazo = CASE
                  WHEN tipo_rechazo IN ('MFM_BAJO','RSI_EXTREMO','ADX_EXTREMO',
                       'ATR_COMPRIMIDO','VOLUMEN_BAJO','MACD_EXPLOSIVO',
                       'COMPRESION_SQUEEZE','EXPANSION_ATR','STEP_PCT_BAJO',
                       'QTY_INSUFICIENTE','NEUTRAL_CONVICCION','OTRO')
                       THEN tipo_rechazo
                  WHEN tipo_rechazo LIKE '%SQUEEZE%' THEN 'COMPRESION_SQUEEZE'
                  WHEN tipo_rechazo LIKE '%EXPANSION%' THEN 'EXPANSION_ATR'
                  WHEN tipo_rechazo LIKE 'MFM contradictorio%' THEN 'MFM_BAJO'
                  WHEN tipo_rechazo LIKE 'RSI %' THEN 'RSI_EXTREMO'
                  WHEN tipo_rechazo LIKE 'ADX %' THEN 'ADX_EXTREMO'
                  WHEN tipo_rechazo LIKE 'ATR %' THEN 'ATR_COMPRIMIDO'
                  WHEN tipo_rechazo LIKE 'Volumen bajo%' THEN 'VOLUMEN_BAJO'
                  WHEN tipo_rechazo LIKE 'MACD%' THEN 'MACD_EXPLOSIVO'
                  WHEN tipo_rechazo LIKE 'step_pct%' THEN 'STEP_PCT_BAJO'
                  WHEN tipo_rechazo LIKE 'Qty%' THEN 'QTY_INSUFICIENTE'
                  ELSE 'OTRO' END
            """)
            _conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('PATTERNS', 'v1.3_norm_tipos', ?, "
                "'I68: codigos limpios de tipo_rechazo para mineria', 1)",
                (int(time.time() * 1000),))
            _conn.commit()
            print("  [LEARN-DB] I68: tipo_rechazo normalizado (v1.3)")
        except Exception as e:
            print(f"  [LEARN-DB] I68 norm_tipos: {e}")
    await _run(_normalizar_tipos_rechazo_v13)
    # F9.5: marcador de cierre de gaps P1/P2/P3 (punto 7 del diff F9).
    # Solo inserta una FILA de datos en schema_versions (sin schema change).
    def _registrar_f95():
        try:
            _conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('ENTRY_RULES', 'v1.0_f95', ?, "
                "'F9.5: P1 matcher unificado; P2 p_base=0 -> BLOCK; "
                "P3 score real FIRE', 1)",
                (int(time.time() * 1000),))
            _conn.commit()
        except Exception as e:
            print(f"  [LEARN-DB] F9.5 marker: {e}")
    await _run(_registrar_f95)
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


async def cargar_bb_width_hist(limit: int = 30):
    """F5.1/I79-A: ultimos N bb_width_pct por simbolo (warm-start del histograma).

    Retorna dict symbol -> lista de bb_width_pct (orden ASC, mas viejo primero).
    """
    def _op():
        conn = _get_conn()
        syms = [r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM feature_snapshots "
            "WHERE bb_width_pct IS NOT NULL").fetchall()]
        out = {}
        for sym in syms:
            rows = conn.execute(
                "SELECT bb_width_pct FROM feature_snapshots "
                "WHERE symbol = ? AND bb_width_pct IS NOT NULL "
                "ORDER BY timestamp_15m DESC LIMIT ?",
                (sym, int(limit))).fetchall()
            vals = [float(r[0]) for r in reversed(rows)]
            if vals:
                out[sym] = vals
        return out
    return await _run(_op)


async def backfill_features_v1_1():
    """F1.4: rellena columnas v1.1 en snapshots v1.0 desde velas_15m_mirror.

    One-shot (marcador FEATURES/v1.1_backfill). Rellena 5/9 columnas:
    bb_width_pct, pct_b, er_15m, donchian_high_20, donchian_low_20.
    (atr_ratio/roc/bb_pct_pct/vwap requieren 1m o ventana historica -> NULL)
    """
    def _op():
        conn = _get_conn()
        ya = conn.execute(
            "SELECT 1 FROM schema_versions WHERE componente = 'FEATURES' "
            "AND version = 'v1.1_backfill'").fetchone()
        if ya:
            return 0
        rows = conn.execute(
            "SELECT id, symbol, timestamp_15m FROM feature_snapshots "
            "WHERE feature_schema_version = 'v1.0' "
            "ORDER BY symbol, timestamp_15m").fetchall()
        if not rows:
            conn.execute(
                "INSERT OR IGNORE INTO schema_versions "
                "(componente, version, aplicado_utc, changelog, activo) "
                "VALUES ('FEATURES','v1.1_backfill',?, 'F1.4: sin filas v1.0', 1)",
                (int(time.time() * 1000),))
            conn.commit()
            return 0
        por_sym = {}
        for r in rows:
            por_sym.setdefault(r['symbol'], []).append(r)
        total = 0
        for sym, filas_sym in por_sym.items():
            velas = conn.execute(
                "SELECT timestamp, open, high, low, close, volume "
                "FROM velas_15m_mirror WHERE symbol = ? ORDER BY timestamp",
                (sym,)).fetchall()
            if len(velas) < 20:
                continue
            idx_by_ts = {v['timestamp']: i for i, v in enumerate(velas)}
            updates = []
            for r in filas_sym:
                i = idx_by_ts.get(r['timestamp_15m'])
                if i is None or i < 19:
                    continue
                ventana = velas[i - 19:i + 1]
                closes = [float(v['close']) for v in ventana]
                sma = sum(closes) / len(closes)
                std = (sum((c - sma) ** 2 for c in closes) / len(closes)) ** 0.5
                bb_w = (4.0 * std / sma * 100.0) if sma > 0 else None
                pct_b = (((closes[-1] - (sma - 2.0 * std)) / (4.0 * std))
                         if std > 0 else 0.5)
                er_num = abs(closes[-1] - closes[0])
                er_den = sum(abs(closes[j] - closes[j - 1])
                             for j in range(1, len(closes)))
                er = (er_num / er_den) if er_den > 0 else 0.0
                updates.append((
                    bb_w, pct_b, er,
                    max(float(v['high']) for v in ventana),
                    min(float(v['low']) for v in ventana),
                    r['id']))
            if updates:
                conn.executemany(
                    "UPDATE feature_snapshots SET bb_width_pct = ?, pct_b = ?, "
                    "er_15m = ?, donchian_high_20 = ?, donchian_low_20 = ?, "
                    "feature_schema_version = 'v1.1' WHERE id = ?", updates)
                total += len(updates)
        conn.execute(
            "INSERT OR IGNORE INTO schema_versions "
            "(componente, version, aplicado_utc, changelog, activo) "
            "VALUES ('FEATURES','v1.1_backfill',?, "
            "'F1.4: backfill desde velas_15m_mirror', 1)",
            (int(time.time() * 1000),))
        conn.commit()
        return total

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
    # F1.1 v1.1
    'bb_width_pct', 'bb_width_pct_pct', 'pct_b', 'er_15m',
    'donchian_high_20', 'donchian_low_20', 'atr_ratio_1m_15m',
    'roc_4v_pct', 'vwap_24h',
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


# ═════════════════════════════════════════════════════════════════════════════
# near_miss_labels — import desde MySQL (F1.6)
# ═════════════════════════════════════════════════════════════════════════════
async def guardar_near_miss_labels_batch(rows: list):
    if not rows:
        return 0
    cols = ['seguimiento_id', 'symbol', 'timestamp_inicio', 'timestamp_fin',
            'regime', 'direction', 'tipo_rechazo', 'adx_15m', 'rsi_15m', 'ema200_dist_pct',
            'ret_neto_2h_pct', 'ret_neto_8h_pct', 'mfe_neto_pct', 'mae_neto_pct',
            'breakeven_pct', 'katr_umbral', 'rentable_neto_2h', 'rentable_neto_8h',
            'bloqueo_correcto', 'falso_negativo', 'significativo_katr',
            'costo_oportunidad_usdt']
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO near_miss_labels ({', '.join(cols)}) VALUES ({ph})"

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


_FIRE_LABEL_COLS = [
    'fire_evento_id', 'snapshot_id', 'symbol', 'timestamp_fire', 'direction',
    'regime', 'score_macro', 'adx_15m', 'rsi_15m', 'mfm_15m', 'atr_pct_15m',
    'er_15m', 'bb_width_pct_pct', 'pct_b', 'atr_ratio_1m_15m', 'roc_4v_pct',
    'ema200_dist_pct', 'volume_ratio', 'rsi_7_1m', 'wick_pct_1m', 'body_dir_1m',
    'precio_entrada', 'precio_salida_2h', 'precio_salida_4h',
    'ret_neto_2h_pct', 'ret_neto_4h_pct', 'mfe_2h_pct', 'mae_2h_pct',
    'grid_levels_touched', 'grid_roundtrips', 'grid_pnl_usdt',
    'rentable_neto_2h', 'rentable_neto_4h', 'significativo_katr',
    'feature_schema_version',
]


async def guardar_fire_labels_batch(rows: list):
    """F6.3: INSERT OR IGNORE batch en fire_labels."""
    if not rows:
        return 0
    filas = [tuple(r.get(c) for c in _FIRE_LABEL_COLS) for r in rows]
    ph = ", ".join(["?"] * len(_FIRE_LABEL_COLS))
    sql = (f"INSERT OR IGNORE INTO fire_labels "
           f"({', '.join(_FIRE_LABEL_COLS)}) VALUES ({ph})")

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_fire_labels(horas: int = 24 * 90):
    """F7.2: fire_labels recientes para minar_fires."""
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        rows = conn.execute(
            "SELECT * FROM fire_labels WHERE timestamp_fire >= ? "
            "ORDER BY timestamp_fire DESC", (ts_corte,)).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


_BREAKOUT_LABEL_COLS = [
    'symbol', 'timestamp_15m', 'direction', 'regimen', 'nivel_roto',
    'donchian_high_20', 'donchian_low_20', 'er_15m', 'bb_width_pct_pct',
    'pct_b', 'atr_ratio_1m_15m', 'roc_4v_pct', 'volume_ratio_1m',
    'volume_ratio_15m', 'mfm_sma5', 'vwap_24h', 'continuacion', 'fakeout',
    'mfe_pct_2h', 'mae_pct_2h', 'minutos_continuacion', 'nivel_invalidacion',
    'feature_schema_version',
]


async def guardar_breakout_labels_batch(rows: list):
    """F6.4: INSERT OR IGNORE batch en breakout_labels."""
    if not rows:
        return 0
    filas = [tuple(r.get(c) for c in _BREAKOUT_LABEL_COLS) for r in rows]
    ph = ", ".join(["?"] * len(_BREAKOUT_LABEL_COLS))
    sql = (f"INSERT OR IGNORE INTO breakout_labels "
           f"({', '.join(_BREAKOUT_LABEL_COLS)}) VALUES ({ph})")

    def _op():
        conn = _get_conn()
        conn.executemany(sql, filas)
        conn.commit()
        return len(filas)

    return await _run(_op)


async def cargar_breakout_labels(horas: int = 24 * 90):
    """F7.4: breakout_labels recientes para minar_breakouts."""
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        rows = conn.execute(
            "SELECT * FROM breakout_labels WHERE timestamp_15m >= ? "
            "ORDER BY timestamp_15m DESC", (ts_corte,)).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_snapshots_breakout_candidatos(horas: int = 24 * 7,
                                                antes_de_ms: int = 0,
                                                limit: int = 400):
    """F6.4: snapshots TREND/PANIC con cierre fuera de Donchian20 sin label."""
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        rows = conn.execute(
            "SELECT fs.* FROM feature_snapshots fs "
            "LEFT JOIN breakout_labels bl ON bl.symbol = fs.symbol "
            "AND bl.timestamp_15m = fs.timestamp_15m "
            "WHERE fs.timestamp_15m >= ? AND fs.timestamp_15m <= ? "
            "AND fs.regime_confirmado IN ('TREND_UP','TREND_DOWN','PANIC') "
            "AND fs.donchian_high_20 IS NOT NULL "
            "AND fs.donchian_low_20 IS NOT NULL AND bl.id IS NULL "
            "ORDER BY fs.timestamp_15m ASC LIMIT ?",
            (ts_corte, int(antes_de_ms), int(limit))).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_snapshot_cercano(symbol: str, ts_ms: int,
                                  ventana_ms: int = 45 * 60_000):
    """F6.3/I82: snapshot mas cercano hacia atras (ventana acotada)."""
    def _op():
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM feature_snapshots WHERE symbol = ? "
            "AND timestamp_15m <= ? AND timestamp_15m >= ? "
            "ORDER BY timestamp_15m DESC LIMIT 1",
            (symbol, int(ts_ms), int(ts_ms - ventana_ms))).fetchone()
        return dict(row) if row else None

    return await _run(_op)


async def contar_near_miss_labels():
    """Retorna (total, ultimo_timestamp_ms) para health monitor NMI."""
    def _op():
        conn = _get_conn()
        row = conn.execute(
            "SELECT COUNT(*) as total, MAX(timestamp_inicio) as ultimo_ts FROM near_miss_labels"
        ).fetchone()
        return row[0], row[1]

    return await _run(_op)


async def cargar_near_miss_labels_falsos_negativos():
    """Carga near_miss_labels con falso_negativo=1 para minar reglas de bypass."""
    def _op():
        conn = _get_conn()
        cursor = conn.execute("""
            SELECT * FROM near_miss_labels
            WHERE falso_negativo = 1
            ORDER BY timestamp_inicio DESC
        """)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_near_miss_labels_todas(horas: int = 24 * 90):
    """F0.1: TODOS los labels (FN + OK), no solo falsos_negativos.

    Sin contramuestras (OK) la mineria daba win_rate=1.0 siempre (Bug #1).
    """
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        rows = conn.execute(
            "SELECT * FROM near_miss_labels WHERE timestamp_inicio >= ? "
            "ORDER BY timestamp_inicio DESC", (ts_corte,)).fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def guardar_bypass_rules_batch(rows: list):
    """Upsert reglas de bypass en SQLite (UNIQUE: symbol, direction, regime, rejected_filter)."""
    if not rows:
        return 0
    cols = ['symbol', 'direction', 'regime', 'rejected_filter', 'condition_json',
            'n_occurrences', 'win_rate', 'wilson_lower', 'is_active']
    ph = ", ".join(["?"] * len(cols))
    sql = f"""INSERT INTO bypass_rules ({', '.join(cols)}) VALUES ({ph})
              ON CONFLICT(symbol, direction, regime, rejected_filter) DO UPDATE SET
                condition_json=excluded.condition_json,
                n_occurrences=excluded.n_occurrences,
                win_rate=excluded.win_rate,
                wilson_lower=excluded.wilson_lower,
                is_active=excluded.is_active,
                updated_at=CURRENT_TIMESTAMP"""

    def _op():
        conn = _get_conn()
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
        conn.commit()
        return len(rows)

    return await _run(_op)


async def cargar_bypass_rules_activas():
    """Carga reglas de bypass activas (is_active=1) para cache RAM."""
    def _op():
        conn = _get_conn()
        cursor = conn.execute("""
            SELECT * FROM bypass_rules WHERE is_active = 1
        """)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_bypass_rules_todas():
    """Carga todas las reglas de bypass (activas e inactivas) para evaluar promotor/demotor."""
    def _op():
        conn = _get_conn()
        cursor = conn.execute("SELECT * FROM bypass_rules")
        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


_ENTRY_RULE_COLS = [
    'tipo_entrada', 'accion', 'symbol', 'direction', 'regime',
    'condition_json', 'score_bonus', 'n_occurrences', 'win_rate',
    'wilson_lower', 'lift_vs_baseline', 'is_active',
]


async def guardar_entry_rules_batch(rows: list):
    """F7.1: upsert de entry_rules (globales symbol NULL incluidos).

    I87: SQLite trata NULL como distinto en UNIQUE(...), asi que el upsert
    se hace con SELECT + UPDATE/INSERT comparando COALESCE(...).
    """
    if not rows:
        return 0

    def _op():
        conn = _get_conn()
        ph = ", ".join(["?"] * len(_ENTRY_RULE_COLS))
        sql_ins = (f"INSERT INTO entry_rules ({', '.join(_ENTRY_RULE_COLS)}) "
                   f"VALUES ({ph})")
        n = 0
        for r in rows:
            found = conn.execute(
                "SELECT id FROM entry_rules WHERE tipo_entrada = ? "
                "AND COALESCE(symbol,'') = COALESCE(?,'') "
                "AND COALESCE(direction,'') = COALESCE(?,'') "
                "AND COALESCE(regime,'') = COALESCE(?,'') "
                "AND condition_json = ?",
                (r.get('tipo_entrada'), r.get('symbol'), r.get('direction'),
                 r.get('regime'), r.get('condition_json'))).fetchone()
            if found:
                conn.execute(
                    "UPDATE entry_rules SET accion=?, score_bonus=?, "
                    "n_occurrences=?, win_rate=?, wilson_lower=?, "
                    "lift_vs_baseline=?, is_active=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE id=?",
                    (r.get('accion'), r.get('score_bonus') or 0,
                     r.get('n_occurrences'), r.get('win_rate'),
                     r.get('wilson_lower'), r.get('lift_vs_baseline'),
                     1 if r.get('is_active') else 0, found[0]))
            else:
                conn.execute(sql_ins, tuple(r.get(c) for c in _ENTRY_RULE_COLS))
            n += 1
        conn.commit()
        return n

    return await _run(_op)


async def cargar_entry_rules_activas():
    """F7.5: entry_rules activas para cache RAM (F8 las consume)."""
    def _op():
        conn = _get_conn()
        rows = conn.execute(
            "SELECT * FROM entry_rules WHERE is_active = 1").fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def cargar_entry_rules_todas():
    """F9.3: todas las entry_rules (ACTIVE y SHADOW) para promocion/demote."""
    def _op():
        conn = _get_conn()
        rows = conn.execute("SELECT * FROM entry_rules").fetchall()
        return [dict(r) for r in rows]

    return await _run(_op)


async def update_entry_rule_estado(rule_id, is_active):
    """F9.3: cambia is_active de una entry_rule por id."""
    def _op():
        conn = _get_conn()
        conn.execute(
            "UPDATE entry_rules SET is_active = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (1 if is_active else 0, int(rule_id)))
        conn.commit()

    return await _run(_op)


async def demote_all_entry_rules():
    """F9.1: ACTIVE -> SHADOW en todas las entry_rules. Retorna rowcount."""
    def _op():
        conn = _get_conn()
        cur = conn.execute(
            "UPDATE entry_rules SET is_active = 0, "
            "updated_at = CURRENT_TIMESTAMP WHERE is_active = 1")
        conn.commit()
        return cur.rowcount

    return await _run(_op)


async def contar_entry_rule_blocks(horas: float = 24):
    """F9.1: (n_block, n_total) de intentos FIRE; delega en MySQL.

    Import lazy para evitar dependencia circular learning_db <-> database_v5.
    """
    from database_v5 import contar_entry_rule_blocks as _contar_mysql
    return await _contar_mysql(horas=horas)


async def ultimo_guardrail_entry_rules(horas: float = 24 * 7) -> int:
    """F9.1/I-F9-4: timestamp_ms del ultimo guardrail de entry_rules (o 0)."""
    desde = int(time.time() * 1000) - int(horas * 3600_000)

    def _op():
        conn = _get_conn()
        row = conn.execute(
            "SELECT MAX(timestamp_ms) FROM guardrail_events "
            "WHERE tipo = 'ENTRY_RULE_BLOCK_RATE' AND timestamp_ms >= ?",
            (desde,)).fetchone()
        return int(row[0]) if row and row[0] else 0

    return await _run(_op)


async def f95_aplicado() -> bool:
    """F9.5: True si el marcador ENTRY_RULES v1.0_f95 esta registrado."""
    def _op():
        conn = _get_conn()
        row = conn.execute(
            "SELECT 1 FROM schema_versions "
            "WHERE componente = 'ENTRY_RULES' AND version = 'v1.0_f95' "
            "AND activo = 1").fetchone()
        return bool(row)

    return await _run(_op)


async def guardar_bypass_rule_estado(symbol, direction, regime, rejected_filter, is_active, wilson_lower, n_occurrences):
    """Actualiza is_active de una regla de bypass individual."""
    def _op():
        conn = _get_conn()
        if symbol is None:
            conn.execute("""
                UPDATE bypass_rules
                SET is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE symbol IS NULL AND direction = ? AND regime = ? AND rejected_filter = ?
            """, (1 if is_active else 0, direction, regime, rejected_filter))
        else:
            conn.execute("""
                UPDATE bypass_rules
                SET is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = ? AND direction = ? AND regime = ? AND rejected_filter = ?
            """, (1 if is_active else 0, symbol, direction, regime, rejected_filter))
        conn.commit()

    return await _run(_op)


def _wilson(k: float, n: float, z: float = 1.96):
    """F0.1 (I32-A): Wilson local. learning_db no puede importar
    learning_engine (circular: learning_engine importa learning_db)."""
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denom
    margen = (z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)) / denom
    return max(0.0, centro - margen), min(1.0, centro + margen)


async def cargar_bypass_rule_stats(rejected_filter: str, regime: str,
                                   direction: str = None):
    """F0.1 (I2): stats COMPLETAS FN vs OK (antes solo FN -> win_rate=1.0
    y AVG(wilson_lower) sobre columna inexistente -> error tragado).
    direction=None agrega todas las direcciones (compat con caller actual)."""
    def _op():
        conn = _get_conn()
        sql = (
            "SELECT "
            "COUNT(*) as n, "
            "SUM(CASE WHEN falso_negativo = 1 THEN 1 ELSE 0 END) as n_fn, "
            "SUM(CASE WHEN bloqueo_correcto = 1 THEN 1 ELSE 0 END) as n_ok "
            "FROM near_miss_labels "
            "WHERE tipo_rechazo = ? AND regime = ?"
        )
        params = [rejected_filter, regime]
        if direction is not None:
            sql += " AND direction = ?"
            params.append(direction)
        row = conn.execute(sql, params).fetchone()
        if not row or row[0] < 10:
            return None
        n = row[0]
        n_fn = row[1] or 0
        lo, hi = _wilson(n_fn, n)
        return {
            'n': n, 'n_fn': n_fn, 'n_ok': row[2] or 0,
            'win_rate': n_fn / n,
            'wilson_lower': lo, 'wilson_upper': hi,
        }

    return await _run(_op)


async def calcular_base_rates_reales(symbol: str = None, horas: int = 24 * 30):
    """
    F3.7: base_rates reales desde feature_snapshots JOIN pattern_labels.
    Distribución real de labels por régimen (no promedio de p_baseline).
    """
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        where_symbol = "AND fs.symbol = ?" if symbol else ""
        params = [ts_corte]
        if symbol:
            params.append(symbol)
        cursor = conn.execute(f"""
            SELECT
                fs.regime as regime,
                pl.label_class as label,
                COUNT(*) as n
            FROM feature_snapshots fs
            JOIN pattern_labels pl ON pl.snapshot_id = fs.id
            WHERE fs.timestamp_15m >= ?
            {where_symbol}
            GROUP BY fs.regime, pl.label_class
        """, params)
        rows = cursor.fetchall()
        result = {}
        for r in rows:
            regime = r[0] or 'UNKNOWN'
            label = r[1] or 'UNKNOWN'
            n = r[2] or 0
            result.setdefault(regime, {})[label] = n
        # Convertir a probabilidades
        base_rates = {}
        for regime, labels in result.items():
            total = sum(labels.values())
            if total > 0:
                for label, n in labels.items():
                    base_rates[f"{regime}|{label}"] = round(n / total, 4)
        return base_rates

    return await _run(_op)


async def contar_snapshots_ventana(symbol: str, horas: int = 24 * 7):
    """Cuenta feature_snapshots para un símbolo en ventana de horas."""
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - horas * 3600_000
        cursor = conn.execute(
            "SELECT COUNT(*) FROM feature_snapshots WHERE symbol = ? AND timestamp_15m >= ?",
            (symbol, ts_corte))
        return cursor.fetchone()[0] or 0

    return await _run(_op)


async def calcular_fnr_ventana(horas: int = 24 * 7):
    """
    Calcula False Negative Ratio en ventana de horas.
    FNR = FN / (FN + TP) donde:
      FN = falso_negativo = 1 en near_miss_labels
      TP = bloqueo_correcto = 1 en near_miss_labels
    """
    def _op():
        conn = _get_conn()
        cursor = conn.execute("""
            SELECT
                SUM(CASE WHEN falso_negativo = 1 THEN 1 ELSE 0 END) as fn,
                SUM(CASE WHEN bloqueo_correcto = 1 THEN 1 ELSE 0 END) as tp
            FROM near_miss_labels
            WHERE timestamp_inicio >= ?
        """, (int(time.time() * 1000) - horas * 3600_000,))
        row = cursor.fetchone()
        fn = row[0] or 0
        tp = row[1] or 0
        total = fn + tp
        fnr = fn / total if total > 0 else 0.0
        return fnr, fn, tp, total

    return await _run(_op)


async def contar_predicciones_fire_periodo(horas: float = 24.0):
    """Cuenta FIREs en predictions_log en ventana de horas."""
    def _op():
        conn = _get_conn()
        ts_corte = int(time.time() * 1000) - int(horas * 3600_000)
        cursor = conn.execute("""
            SELECT COUNT(*) as n_fire
            FROM predictions_log
            WHERE decision_tomada = 'FIRE' AND timestamp_15m >= ?
        """, (ts_corte,))
        row = cursor.fetchone()
        return row[0] or 0

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
    """Rellena outcome_* en predictions_log desde pattern_labels (job horario).
    F4.3: Join por snapshot_id (no por symbol+timestamp) para evitar ambiguedad."""
    def _op():
        conn = _get_conn()
        # Primero: poblar snapshot_id en predictions_log pendientes (match por symbol+timestamp)
        conn.execute("""
            UPDATE predictions_log
            SET snapshot_id = (
                SELECT fs.id FROM feature_snapshots fs
                WHERE fs.symbol = predictions_log.symbol
                  AND fs.timestamp_15m = predictions_log.timestamp_15m
                  AND predictions_log.snapshot_id IS NULL
            )
            WHERE snapshot_id IS NULL
        """)
        # Luego: completar outcomes via join directo en snapshot_id
        rows = conn.execute(
            """SELECT pl.id AS pid, p.id AS lid, pl.label_class, pl.ret_4h_neto_pct
               FROM pattern_labels pl
               JOIN predictions_log p ON p.snapshot_id = pl.snapshot_id
               WHERE p.outcome_label IS NULL AND pl.timestamp_15m <= ?
               LIMIT ?""", (int(antes_de_ms), int(limit))).fetchall()
        for r in rows:
            conn.execute(
                """UPDATE predictions_log
                   SET outcome_label = ?, outcome_retorno_pct = ?, outcome_timestamp = ?
                   WHERE id = ?""",
                (r["label_class"], r["ret_4h_neto_pct"], int(r["pid"]),
                 r["lid"]))
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
