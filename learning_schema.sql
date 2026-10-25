-- ============================================================================
-- learning_schema.sql — Esquema de crypto_learning.db (Learning Engine)
-- FASE 0 · Crypto Monitor V5.7+ — 11 tablas + 1 mirror
--
-- Correcciones aplicadas respecto al documento original:
--   C-1: se añade velas_15m_mirror (copia desde MySQL/MariaDB para poder
--        calcular outcomes; el doc original asumía SQLite como DB de trading).
--   §3C: columnas fees_pct / pnl_neto_pct desde el día 1 (todo neto de fees).
--   §3B: pattern_labels guarda excursiones direccionales up/down (sin leak).
--   C-2: razon_salida ampliado con NEUTRAL_GRID_ABORT.
--   Fase 10: model_experiments incluida (11 tablas, no 10).
-- ============================================================================

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=60000;
PRAGMA cache_size=-64000;
PRAGMA temp_store=MEMORY;
PRAGMA mmap_size=268435456;

-- ----------------------------------------------------------------------------
-- Mirror de velas 15m (fuente: crypto_monitor.db en MySQL/MariaDB).
-- Job de sync a nivel de aplicación (no se puede hacer trigger cross-DBMS).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS velas_15m_mirror (
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,           -- ms UTC, cierre de la vela
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY (symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_v15_ts ON velas_15m_mirror(timestamp);

-- ----------------------------------------------------------------------------
-- ANEXO A §2.1: versionado de esquema/lógica — reproducibilidad
-- Cualquier cambio en extracción de features o etiquetado OBLIGA a insertar
-- una fila nueva aquí (y actualizar feature_schema_version / labeler_version).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    componente TEXT NOT NULL,        -- FEATURES | LABELER | PATTERNS | MODEL
    version TEXT NOT NULL,           -- 'v1.0', 'v1.1', 'v2.0'
    aplicado_utc INTEGER NOT NULL,   -- epoch_ms (§2.4: timestamps de análisis en ms)
    changelog TEXT,
    activo BOOLEAN DEFAULT 1,
    UNIQUE(componente, version)
);
CREATE INDEX IF NOT EXISTS idx_sv_componente ON schema_versions(componente, activo);

-- ----------------------------------------------------------------------------
-- ANEXO A §5.4: alertas de drift (PSI) — informativas, sin auto-acción
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS drift_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    psi_por_feature_json TEXT,
    features_con_drift_json TEXT,
    acciones_tomadas TEXT DEFAULT 'ninguna',   -- 'ninguna' | 'suspender_patrones' | 'rollback'
    revisado_por_humano BOOLEAN DEFAULT 0,
    created_at_ms INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_drift_symbol_ts ON drift_alerts(symbol, timestamp_ms);

-- ----------------------------------------------------------------------------
-- ANEXO A §4: eventos de guardrail (rollback automático) — auditoría
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS guardrail_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    tipo TEXT NOT NULL,              -- REJECT_RATIO | PRECISION_DROP | EXIT_CAPTURA | DRIFT
    severidad TEXT,                  -- CRITICO | ALTO | MEDIO
    razon TEXT NOT NULL,
    accion_tomada TEXT,              -- ROLLBACK_A_SHADOW | ROLLBACK_EXIT_OFF | ALERTA
    modo_antes TEXT,
    modo_despues TEXT,
    revisado_por_humano BOOLEAN DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ge_ts ON guardrail_events(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ge_tipo ON guardrail_events(tipo, timestamp_ms);

-- ----------------------------------------------------------------------------
-- 3.1 feature_snapshots — corazón del sistema (captura por vela 15m)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_15m INTEGER NOT NULL,
    price REAL NOT NULL,
    regime TEXT,                          -- RANGE|TREND_UP|TREND_DOWN|VOLATILE|UNKNOWN
    regime_confirmado TEXT,               -- tras histéresis (>=N velas iguales)

    -- Features 15m crudas
    rsi_15m REAL,
    adx_15m REAL,
    atr_pct_15m REAL,
    macd_hist REAL,
    macd_hist_prev REAL,
    mfm_15m REAL,
    mfm_sma5 REAL,
    volume_ratio REAL,                    -- volume / volume_sma20

    -- Features derivadas
    price_rel_ema50_pct REAL,
    price_rel_ema200_pct REAL,
    ema7_ema25_spread_pct REAL,
    dist_to_recent_high_pct REAL,
    dist_to_recent_low_pct REAL,
    ema50_slope_3v REAL,
    adx_slope_3v REAL,
    rsi_divergence INTEGER,               -- 0|1

    -- Contexto micro 1m
    rsi_7_1m REAL,
    wick_upper_pct_1m REAL,
    wick_lower_pct_1m REAL,
    body_direction_1m INTEGER,
    mecha_short_1m INTEGER,
    mecha_long_1m INTEGER,

    -- Contexto macro 4h
    ema200_4h REAL,
    close_4h REAL,

    -- Features temporales (UTC, ver corrección §3B del informe)
    hora_sin REAL,
    hora_cos REAL,
    dia_semana INTEGER,

    -- Meta
    score_macro INTEGER,
    estado_maquina TEXT,
    evento_asociado TEXT,                 -- NULL | FIRE | NEAR_MISS | RECHAZADO
    evento_id INTEGER,

    feature_schema_version TEXT DEFAULT 'v1.0',   -- ANEXO A §2.2

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp_15m)
);
CREATE INDEX IF NOT EXISTS idx_fs_symbol_ts ON feature_snapshots(symbol, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_fs_ts ON feature_snapshots(timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_fs_regime ON feature_snapshots(symbol, regime);
CREATE INDEX IF NOT EXISTS idx_fs_evento ON feature_snapshots(evento_asociado);

-- ----------------------------------------------------------------------------
-- 3.2 pattern_labels — outcome etiquetado de cada snapshot
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pattern_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    timestamp_15m INTEGER NOT NULL,

    -- Retornos futuros (brutos y netos de fees)
    ret_1h_pct REAL,
    ret_4h_pct REAL,
    ret_8h_pct REAL,
    ret_4h_neto_pct REAL,                 -- corregido §3C: neto de fees+slippage

    -- Excursiones direccionales (corregido §3B: sin sesgo LONG/SHORT)
    mfe_pct REAL,                         -- máx. excursion favorable arriba (up)
    mae_pct REAL,                         -- máx. excursion favorable abajo (dn)
    up_excursion_pct REAL,
    down_excursion_pct REAL,
    mfe_minuto INTEGER,
    mae_minuto INTEGER,

    label_class TEXT,                     -- LONG_STRONG|SHORT_STRONG|NEUTRAL_RANGE|WHIPSAW|MIXED

    labeler_version TEXT DEFAULT 'v1.0_ret8h',    -- ANEXO A §2.2
    labeler_params_json TEXT,                     -- {"umbral_k_atr": 1.5, "ventana_h": 8, "fee_pct": 0.05}

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (snapshot_id) REFERENCES feature_snapshots(id)
);
CREATE INDEX IF NOT EXISTS idx_pl_symbol_ts ON pattern_labels(symbol, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_pl_label ON pattern_labels(label_class);

-- ----------------------------------------------------------------------------
-- 3.3 learned_patterns — patrones minados por régimen, memoria 3 capas
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS learned_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    regime TEXT NOT NULL,
    feature TEXT NOT NULL,
    bucket TEXT NOT NULL,                 -- Q1..Q5
    rango_valor TEXT,
    label_objetivo TEXT NOT NULL,
    p_local REAL,
    p_baseline REAL,
    lift REAL,
    lift_lower REAL,                      -- corregido §3A: límite inferior (Wilson)
    p_value REAL,                         -- corregido §3A: para control FDR
    n REAL,                               -- n efectivo (ponderado)
    n_entero INTEGER,

    capa TEXT DEFAULT 'SHORT',
    status TEXT DEFAULT 'ACTIVE',         -- ACTIVE|SUSPENDED|DEPRECATED|STABLE
    key_format_version TEXT DEFAULT 'v1_raw',   -- ANEXO A §2.3: clave legible determinística
    n_ventanas_valido INTEGER DEFAULT 1,
    peso_actual REAL DEFAULT 1.0,
    primera_deteccion_utc TEXT,
    ultima_validacion_utc TEXT,
    generado_utc TEXT,

    UNIQUE(pattern_key, capa, generado_utc)
);
CREATE INDEX IF NOT EXISTS idx_lp_symbol_regime ON learned_patterns(symbol, regime, capa);
CREATE INDEX IF NOT EXISTS idx_lp_status ON learned_patterns(status);
CREATE INDEX IF NOT EXISTS idx_lp_lift ON learned_patterns(lift DESC);

-- ----------------------------------------------------------------------------
-- 3.4 pattern_history — memoria histórica acumulativa (nunca se borra)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pattern_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    regime TEXT NOT NULL,
    feature TEXT NOT NULL,
    bucket TEXT NOT NULL,
    label_objetivo TEXT NOT NULL,
    lift REAL,
    lift_lower REAL,
    n REAL,
    p_local REAL,
    capa TEXT,
    status TEXT,
    ventana_inicio_utc TEXT,
    registrado_utc TEXT
);
CREATE INDEX IF NOT EXISTS idx_ph_key ON pattern_history(pattern_key, registrado_utc);
CREATE INDEX IF NOT EXISTS idx_ph_symbol ON pattern_history(symbol, registrado_utc);

-- ----------------------------------------------------------------------------
-- 3.5 coin_profiles_learned — perfil runtime por moneda
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coin_profiles_learned (
    symbol TEXT PRIMARY KEY,
    perfil_json TEXT NOT NULL,
    generado_utc TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3.6 regime_transitions (Pilar 3)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS regime_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_15m INTEGER NOT NULL,
    from_regime TEXT NOT NULL,
    to_regime TEXT NOT NULL,

    adx_15m REAL,
    adx_slope_3v REAL,
    mfm_sma5 REAL,
    mfm_flip BOOLEAN,
    volume_ratio REAL,
    rsi_15m REAL,
    atr_pct_15m REAL,
    precio_antes REAL,
    precio_despues REAL,
    duracion_regimen_previo_min INTEGER,

    fue_transicion_real BOOLEAN,
    tiempo_confirmacion_min INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_rt_symbol_ts ON regime_transitions(symbol, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_rt_transition ON regime_transitions(from_regime, to_regime);

-- ----------------------------------------------------------------------------
-- 3.7 exit_outcomes (Pilar 4) — con fees y neto (corrección §3C)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exit_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    posicion_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    grid_id INTEGER,
    tipo TEXT NOT NULL,                   -- REAL | VIRTUAL
    direccion TEXT NOT NULL,              -- LONG|SHORT

    timestamp_entrada INTEGER NOT NULL,
    precio_entrada REAL NOT NULL,
    qty REAL,

    mfe_pct REAL,
    mfe_minuto INTEGER,
    mae_pct REAL,
    mae_minuto INTEGER,

    timestamp_salida INTEGER,
    precio_salida REAL,
    pnl_pct REAL,
    pnl_usdt REAL,
    fees_pct REAL,                        -- corregido §3C
    pnl_neto_pct REAL,                    -- corregido §3C
    pnl_neto_usdt REAL,

    capturado_pct_de_mfe REAL,
    left_on_table_pct REAL,
    razon_salida TEXT,                    -- TAKE_PROFIT|KILL_SWITCH|TIMEOUT|TREND_END|DINAMICA|NEUTRAL_GRID_ABORT|DESCONOCIDA

    regime_entrada TEXT,
    regime_salida TEXT,
    model_schema_version TEXT DEFAULT 'v1.0',   -- ANEXO A §9 Fase 7

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_eo_symbol ON exit_outcomes(symbol, timestamp_entrada);
CREATE INDEX IF NOT EXISTS idx_eo_pos ON exit_outcomes(posicion_id);
CREATE INDEX IF NOT EXISTS idx_eo_razon ON exit_outcomes(razon_salida);

-- ----------------------------------------------------------------------------
-- 3.8 trend_end_labels (Pilar 4)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trend_end_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_15m INTEGER NOT NULL,
    regime_actual TEXT NOT NULL,

    adx_slope_3v REAL,
    mfm_flip BOOLEAN,
    volume_ratio REAL,
    rsi_divergence BOOLEAN,
    distancia_ema50_pct REAL,
    atr_pct_15m REAL,

    revirtio_2h BOOLEAN,
    magnitud_reversion_pct REAL,
    minutos_hasta_reversion INTEGER,
    umbral_reversion_pct REAL,            -- corregido §3B: umbral ATR-normalizado usado

    tenia_posicion_abierta BOOLEAN,
    direccion_posicion TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp_15m)
);
CREATE INDEX IF NOT EXISTS idx_tel_symbol_ts ON trend_end_labels(symbol, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_tel_regime ON trend_end_labels(regime_actual);

-- ----------------------------------------------------------------------------
-- 3.9 predictions_log — observabilidad online
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predictions_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_15m INTEGER NOT NULL,
    model_version TEXT NOT NULL,

    prob_rentable REAL,
    prob_reversion REAL,
    decision_sugerida TEXT,               -- FIRE|RECHAZAR|MANTENER|CERRAR

    score_macro INTEGER,
    direction TEXT,
    regime TEXT,

    decision_tomada TEXT,                 -- FIRE|REJECT|IGNORADA
    modo TEXT,                            -- SHADOW|ACTIVE
    model_schema_version TEXT DEFAULT 'v1.0',   -- ANEXO A §2.2

    outcome_label TEXT,
    outcome_retorno_pct REAL,
    outcome_timestamp INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predlog_symbol_ts ON predictions_log(symbol, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_predlog_modo ON predictions_log(modo);
CREATE INDEX IF NOT EXISTS idx_predlog_decision ON predictions_log(decision_tomada);

-- ----------------------------------------------------------------------------
-- 3.10 models — registro versionado de modelos (Fase 9+)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT UNIQUE NOT NULL,
    tipo TEXT NOT NULL,                   -- PATTERNS | LGBM | GLOBAL | PER_COIN
    scope TEXT,
    filepath TEXT,
    features_list TEXT,
    metrics_json TEXT,
    umbral_recomendado REAL,
    promovido BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    promoted_at TIMESTAMP,
    deprecated_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_models_version ON models(version);
CREATE INDEX IF NOT EXISTS idx_models_promovido ON models(promovido, tipo);

-- ----------------------------------------------------------------------------
-- ANEXO A §2.5: índices adicionales (consultas del learner y reportes)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fs_regime_evento ON feature_snapshots(regime, evento_asociado);
CREATE INDEX IF NOT EXISTS idx_fs_score ON feature_snapshots(symbol, score_macro, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_pl_ventana ON pattern_labels(symbol, label_class, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_predlog_modo_version ON predictions_log(modo, model_version, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_predlog_decision_ts ON predictions_log(decision_tomada, timestamp_15m);
CREATE INDEX IF NOT EXISTS idx_eo_timestamp_salida ON exit_outcomes(timestamp_salida);
CREATE INDEX IF NOT EXISTS idx_eo_razon_ts ON exit_outcomes(razon_salida, timestamp_salida);
CREATE INDEX IF NOT EXISTS idx_lp_capa_status ON learned_patterns(capa, status, peso_actual DESC);

-- ----------------------------------------------------------------------------
-- Fase 10: model_experiments — A/B framework (tabla 11)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_experiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experimento_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    grupo TEXT NOT NULL,                  -- A_CONTROL | B_EXPERIMENTAL
    model_version TEXT,
    fecha_inicio TEXT,
    fecha_fin TEXT,
    metricas_json TEXT,
    promovido BOOLEAN DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_me_exp ON model_experiments(experimento_id, symbol);
