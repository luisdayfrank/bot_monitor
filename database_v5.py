import aiosqlite
import asyncio
from config import CONFIG
from datetime import datetime, timedelta
import pytz
import json

# ─── Conexión persistente singleton ───
_db_conn = None
_db_lock = asyncio.Lock()

# ─── Helpers de zona horaria ───
def get_tz():
    """Obtiene la zona horaria configurada."""
    try:
        return pytz.timezone(CONFIG.timezone)
    except Exception:
        return pytz.UTC

def utc_to_local(dt_utc):
    """Convierte datetime UTC a hora local configurada."""
    if dt_utc is None:
        return None
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=pytz.UTC)
    return dt_utc.astimezone(get_tz())

def local_to_utc(dt_local):
    """Convierte datetime local a UTC."""
    if dt_local is None:
        return None
    if dt_local.tzinfo is None:
        dt_local = get_tz().localize(dt_local)
    return dt_local.astimezone(pytz.UTC)

def now_local():
    """Obtiene la hora actual en zona horaria local."""
    return datetime.now(get_tz())

def now_utc():
    """Obtiene la hora actual en UTC."""
    return datetime.now(pytz.UTC)

async def _get_db():
    """Obtiene o crea la conexión persistente a SQLite."""
    global _db_conn
    if _db_conn is None:
        _db_conn = await aiosqlite.connect(CONFIG.db_path, timeout=30.0)
        # Activar WAL mode para mejor concurrencia (lectura/escritura simultánea)
        await _db_conn.execute("PRAGMA journal_mode=WAL")
        await _db_conn.execute("PRAGMA synchronous=NORMAL")
        await _db_conn.execute("PRAGMA busy_timeout=60000")  # 60s de espera antes de "database is locked"
        await _db_conn.commit()
    return _db_conn

async def close_db():
    """Cierra la conexión persistente."""
    global _db_conn
    if _db_conn:
        await _db_conn.close()
        _db_conn = None

async def _execute_with_retry(sql, params=(), max_retries=10):
    """Ejecuta SQL con retry automático ante 'database is locked'."""
    db = await _get_db()
    for attempt in range(max_retries):
        try:
            async with _db_lock:
                await db.execute(sql, params)
                await db.commit()
            return True
        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                wait = 0.5 * (2 ** attempt)  # 0.5, 1.0, 2.0, 4.0, 8.0s
                print(f"  ⚠️ DB locked (intento {attempt+1}/{max_retries}), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ DB error definitivo: {e}")
                raise
    return False

async def init_db():
    """Inicializa las tablas de la base de datos."""
    db = await _get_db()
    async with _db_lock:
        # Tabla 1M
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_1m (
                symbol TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Tabla 15M
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_15m (
                symbol TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Tabla 4H
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_4h (
                symbol TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Alertas
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                tipo TEXT,
                direccion TEXT,
                score INTEGER,
                mensaje TEXT,
                params_json TEXT,
                precio REAL
            )
        """)
        # Precios vivo
        await db.execute("""
            CREATE TABLE IF NOT EXISTS precios_vivo (
                symbol TEXT PRIMARY KEY,
                precio REAL,
                actualizado TEXT
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 4.5: TABLAS DE AUDITORÍA EXTERNA
        # ═══════════════════════════════════════════════════════════════════════════════

        # Eventos del bot para auditoría (FIRE, RECHAZADO, CAMBIO_ESTADO, etc.)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,                    -- Fecha local (YYYY-MM-DD) para agrupar
                symbol TEXT,
                timestamp_utc TEXT,            -- Timestamp exacto en UTC
                timestamp_local TEXT,          -- Timestamp en hora local del usuario
                tipo TEXT,                     -- FIRE, RECHAZADO, CAMBIO_ESTADO, ARMED, etc.
                direccion TEXT,                -- SHORT, LONG, o NULL
                precio REAL,
                contexto_json TEXT,            -- JSON con todo el contexto (1m, 15m, 4h)
                grid_params_json TEXT,         -- JSON con parámetros del grid (si aplica)
                score INTEGER,                 -- Score del disparo o macro
                rechazos_json TEXT,            -- JSON array con motivos de rechazo
                estado_maquina TEXT            -- Estado de la máquina después del evento
            )
        """)

        # Muestras de precio post-disparo para seguimiento
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_muestras_post (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER,
                symbol TEXT,
                timestamp_utc TEXT,
                timestamp_local TEXT,
                precio REAL,
                minutos_desde_disparo INTEGER,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # Eventos significativos post-disparo (picos, valles, entradas/salidas de grid)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_eventos_post (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER,
                symbol TEXT,
                timestamp_utc TEXT,
                timestamp_local TEXT,
                tipo_evento TEXT,              -- MINIMO_ABSOLUTO, MAXIMO_ABSOLUTO,
                                              -- PRIMERA_VEZ_EN_GRID, PRIMERA_VEZ_FUERA_RANGO
                precio REAL,
                distancia_desde_entrada_pct REAL,
                grid_rentable_aqui INTEGER,    -- 0/1
                nota TEXT,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # Velas 15m posteriores al disparo
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_velas_post (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER,
                symbol TEXT,
                timestamp_utc TEXT,
                timestamp_local TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                minutos_desde_disparo INTEGER,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5.1: TABLA PARA PERSISTENCIA DE DISPAROS ACTIVOS (sobrevive reinicios)
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_disparos_activos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                precio_entrada REAL NOT NULL,
                direccion TEXT NOT NULL,
                grid_params_json TEXT,
                maximo_visto REAL,
                minimo_visto REAL,
                primera_vez_en_grid_json TEXT,      -- JSON con {timestamp, precio, minutos_desde}
                primera_vez_fuera_rango_json TEXT,  -- JSON con {timestamp, precio, minutos_desde, direccion}
                muestras_guardadas INTEGER DEFAULT 0,
                horas_seguimiento INTEGER,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # V5.7: TABLA PARA SEGUIMIENTOS VIRTUALES POST NEAR-MISS
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS near_miss_seguimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp_inicio REAL NOT NULL,
                timestamp_fin REAL,
                precio_inicio REAL NOT NULL,
                direccion_nm TEXT NOT NULL,
                score INTEGER NOT NULL,
                umbral INTEGER NOT NULL,
                filtros_rechazo TEXT,
                muestras_json TEXT,
                precio_max REAL,
                precio_min REAL,
                precio_fin REAL,
                movimiento_pct REAL,
                direccion_real TEXT,
                acerto_bot INTEGER,
                hubiera_sido_rentable INTEGER,
                notas TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Índices para consultas rápidas
        await db.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_fecha ON auditoria_eventos(fecha)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_symbol ON auditoria_eventos(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_tipo ON auditoria_eventos(tipo)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_muestras_evento ON auditoria_muestras_post(evento_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_eventos_post_evento ON auditoria_eventos_post(evento_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_velas_post_evento ON auditoria_velas_post(evento_id)")
        # FASE 5.1: Índice para disparos activos
        await db.execute("CREATE INDEX IF NOT EXISTS idx_disparos_activos_symbol ON auditoria_disparos_activos(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_disparos_activos_evento ON auditoria_disparos_activos(evento_id)")
        # V5.7: Índices para near-miss seguimientos
        await db.execute("CREATE INDEX IF NOT EXISTS idx_nm_symbol ON near_miss_seguimientos(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_nm_timestamp ON near_miss_seguimientos(timestamp_inicio)")

        # ═══════════════════════════════════════════════════════════════════════════════
        # V5.9.2: TABLAS DE GRID NEUTRAL (simulación virtual)
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_estados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp_inicio REAL NOT NULL,
                timestamp_fin REAL,
                estado TEXT NOT NULL DEFAULT 'ACTIVO',
                direccion TEXT,
                precio_entrada REAL,
                grid_params_json TEXT,
                evento_auditoria_id INTEGER,
                FOREIGN KEY (evento_auditoria_id) REFERENCES auditoria_eventos(id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_simulaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                timestamp_inicio REAL NOT NULL,
                timestamp_fin REAL,
                precio_inicio REAL NOT NULL,
                precio_fin REAL,
                pnl_bruto REAL DEFAULT 0.0,
                pnl_neto REAL DEFAULT 0.0,
                fees_totales REAL DEFAULT 0.0,
                slippage_total REAL DEFAULT 0.0,
                trades_completados INTEGER DEFAULT 0,
                trades_kill_switch INTEGER DEFAULT 0,
                posiciones_abiertas_json TEXT,
                max_posiciones_simultaneas INTEGER DEFAULT 0,
                posiciones_atrapadas_json TEXT,
                estado TEXT NOT NULL DEFAULT 'SIMULANDO',
                FOREIGN KEY (grid_id) REFERENCES grid_estados(id)
            )
        """)

        # Índices para grids
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_estados_symbol ON grid_estados(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_estados_estado ON grid_estados(estado)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_sim_grid_id ON grid_simulaciones(grid_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_sim_estado ON grid_simulaciones(estado)")

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 0: TABLAS DE EJECUCIÓN REAL (Testnet / Real)
        # ═══════════════════════════════════════════════════════════════════════════════

        # Grid de ejecución real (testnet o real)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_ejecuciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'ACTIVO',
                trading_mode TEXT NOT NULL,
                capital_asignado REAL NOT NULL,
                apalancamiento_usado INTEGER NOT NULL,
                precio_entrada REAL NOT NULL,
                grid_params_json TEXT,
                pnl_real REAL DEFAULT 0,
                fees_real REAL DEFAULT 0,
                razon_cierre TEXT,
                timestamp_inicio INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            )
        """)

        # Órdenes individuales en Binance (para recuperación post-crash)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_ejecucion_ordenes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_ejecucion_id INTEGER NOT NULL,
                binance_order_id TEXT,
                client_order_id TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                tipo_orden TEXT NOT NULL,
                price REAL,
                quantity REAL,
                quantity_filled REAL DEFAULT 0,
                status TEXT DEFAULT 'NEW',
                FOREIGN KEY (grid_ejecucion_id) REFERENCES grid_ejecuciones(id)
            )
        """)

        # Fills reales (parciales o completos)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_ejecucion_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_ejecucion_id INTEGER NOT NULL,
                orden_id INTEGER NOT NULL,
                binance_trade_id TEXT,
                price REAL,
                qty REAL,
                commission REAL,
                commission_asset TEXT,
                realized_pnl REAL,
                timestamp TIMESTAMP,
                FOREIGN KEY (orden_id) REFERENCES grid_ejecucion_ordenes(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # CR16: TABLA PARA TRACKING PROACTIVO DE FILLS
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fills_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_ejecucion_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                binance_trade_id TEXT NOT NULL UNIQUE,
                binance_order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                commission REAL NOT NULL,
                commission_asset TEXT,
                realized_pnl REAL,
                timestamp_ms INTEGER NOT NULL,
                procesado INTEGER DEFAULT 0,  -- 0=no, 1=sí
                posicion_id TEXT,  -- Link a PosicionReal
                fecha_procesamiento TIMESTAMP,
                FOREIGN KEY (grid_ejecucion_id) REFERENCES grid_ejecuciones(id)
            )
        """)


        # Índices para ejecución real
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_grid ON fills_tracking(grid_ejecucion_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_order ON fills_tracking(binance_order_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_procesado ON fills_tracking(procesado)")

        # ═══════════════════════════════════════════════════════════════════════════════
        # CR2: TABLA PARA EVENTOS DE PnL (cada trade genera un evento)
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pnl_eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_ejecucion_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                tipo_evento TEXT NOT NULL,
                side TEXT NOT NULL,
                binance_trade_id TEXT NOT NULL UNIQUE,
                binance_order_id TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                commission REAL NOT NULL,
                commission_asset TEXT,
                realized_pnl REAL NOT NULL,
                notional REAL NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                timestamp_local TEXT,
                procesado INTEGER DEFAULT 1,
                FOREIGN KEY (grid_ejecucion_id) REFERENCES grid_ejecuciones(id)
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_pnl_grid ON pnl_eventos(grid_ejecucion_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pnl_symbol ON pnl_eventos(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pnl_timestamp ON pnl_eventos(timestamp_ms)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_ejec_symbol ON grid_ejecuciones(symbol)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_grid_ejec_estado ON grid_ejecuciones(estado)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ordenes_client_id ON grid_ejecucion_ordenes(client_order_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ordenes_grid_id ON grid_ejecucion_ordenes(grid_ejecucion_id)")

        await db.commit()

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.4: MIGRACIÓN SILENCIOSA DE COLUMNAS FALTANTES (compatibilidad DB antigua)
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _migrar_columnas_faltantes():
        """Añade columnas faltantes en tablas existentes (idempotente)."""
        columnas_necesarias = {
            'grid_estados': [
                ('evento_auditoria_id', 'INTEGER'),
            ],
            'near_miss_seguimientos': [
                ('filtros_rechazo', 'TEXT'),
                ('muestras_json', 'TEXT'),
                ('precio_max', 'REAL'),
                ('precio_min', 'REAL'),
            ],
            'grid_ejecuciones': [            
                ('timestamp_inicio', 'INTEGER'),
            ],
        }

        for tabla, columnas in columnas_necesarias.items():
            try:
                cursor = await db.execute(f"PRAGMA table_info({tabla})")
                rows = await cursor.fetchall()
                columnas_existentes = {r[1] for r in rows}  # r[1] = name

                for col, tipo in columnas:
                    if col not in columnas_existentes:
                        await db.execute(f"ALTER TABLE {tabla} ADD COLUMN {col} {tipo}")
                        print(f"  [MIGRACION] Columna '{col}' ({tipo}) añadida a '{tabla}'")
                await db.commit()
            except Exception as e:
                print(f"  ⚠️ [MIGRACION] Error verificando {tabla}: {e}")

    await _migrar_columnas_faltantes()

    print("🗄️ Base de datos inicializada (WAL mode) + Auditoría + Disparos + Near-miss + V5.9.2 Grid Neutral + F1.4 Migración")

# Mapeo seguro de timeframes a tablas (evita SQL injection)
_TABLAS_VELAS = {
    '1m': 'velas_1m',
    '15m': 'velas_15m',
    '4h': 'velas_4h',
}

async def insertar_vela(symbol: str, tf: str, vela: dict):
    tabla = _TABLAS_VELAS.get(tf)
    if not tabla:
        print(f"  ⚠️ [DB] Timeframe inválido '{tf}' para {symbol}, ignorando vela")
        return

    await _execute_with_retry(f"""
        INSERT OR REPLACE INTO {tabla}
        (symbol, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (symbol, vela['timestamp'], vela['open'], vela['high'],
          vela['low'], vela['close'], vela['volume']))

async def insertar_velas_batch(symbol: str, tf: str, velas: list):
    """Inserta múltiples velas en una sola transacción (batch)."""
    tabla = _TABLAS_VELAS.get(tf)
    if not tabla:
        print(f"  ⚠️ [DB] Timeframe inválido '{tf}' para batch insert")
        return

    if not velas:
        return

    db = await _get_db()
    async with _db_lock:
        try:
            await db.executemany(
                f"""
                INSERT OR REPLACE INTO {tabla}
                (symbol, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [(symbol, v['timestamp'], v['open'], v['high'], v['low'], v['close'], v['volume']) 
                 for v in velas]
            )
            await db.commit()
            print(f"  💾 [DB] Batch insert: {len(velas)} velas {tf} para {symbol}")
        except Exception as e:
            print(f"  ⚠️ [DB] Error en batch insert {tf} {symbol}: {e}")

async def cargar_velas_historicas(symbol: str, tf: str, limit: int):
    tabla = _TABLAS_VELAS.get(tf)
    if not tabla:
        print(f"  ⚠️ [DB] Timeframe inválido '{tf}' para {symbol}, no se pueden cargar velas")
        return []
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            f"SELECT * FROM {tabla} WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?",
            (symbol, limit)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in reversed(rows)]

async def guardar_alerta(symbol, tipo, direccion, score, mensaje, params_json, precio):
    await _execute_with_retry("""
        INSERT INTO alertas (symbol, timestamp, tipo, direccion, score, mensaje, params_json, precio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (symbol, datetime.utcnow().isoformat(), tipo, direccion, score, mensaje, params_json, precio))
    print(f"  💾 Alerta guardada: {symbol} {tipo} {direccion}")

async def actualizar_precio_vivo(symbol: str, precio: float):
    await _execute_with_retry("""
        INSERT OR REPLACE INTO precios_vivo (symbol, precio, actualizado)
        VALUES (?, ?, ?)
    """, (symbol, precio, datetime.utcnow().isoformat()))

# ═══════════════════════════════════════════════════════════════════════════════
# F1.1: ACTUALIZACIÓN BATCH DE PRECIOS VIVO (reduce I/O drásticamente)
# ═══════════════════════════════════════════════════════════════════════════════
async def actualizar_precios_vivo_batch(precios: dict):
    """Actualiza múltiples precios en una sola transacción (batch)."""
    if not precios:
        return
    db = await _get_db()
    ts = datetime.utcnow().isoformat()
    async with _db_lock:
        try:
            await db.executemany(
                "INSERT OR REPLACE INTO precios_vivo (symbol, precio, actualizado) VALUES (?, ?, ?)",
                [(symbol, precio, ts) for symbol, precio in precios.items()]
            )
            await db.commit()
        except Exception as e:
            print(f"  ⚠️ Error en batch precios vivo: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 4.5: FUNCIONES DE AUDITORÍA
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_evento_auditoria(symbol, timestamp_utc, tipo, direccion=None, precio=None,
                                  contexto_json=None, grid_params_json=None, score=None,
                                  rechazos_json=None, estado_maquina=None):
    """Guarda un evento del bot para auditoría externa.

    Returns:
        int: ID del evento insertado, o None si falla.
    """
    fecha_local = utc_to_local(timestamp_utc).strftime("%Y-%m-%d") if timestamp_utc else now_local().strftime("%Y-%m-%d")
    timestamp_local = utc_to_local(timestamp_utc).isoformat() if timestamp_utc else now_local().isoformat()
    ts_utc_str = timestamp_utc.isoformat() if timestamp_utc else now_utc().isoformat()

    db = await _get_db()

    # ─── FASE 4.5 FIX #7: Insert + SELECT atómico con retry ───
    for attempt in range(5):
        try:
            async with _db_lock:
                await db.execute("""
                    INSERT INTO auditoria_eventos
                    (fecha, symbol, timestamp_utc, timestamp_local, tipo, direccion, precio,
                     contexto_json, grid_params_json, score, rechazos_json, estado_maquina)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (fecha_local, symbol, ts_utc_str, timestamp_local,
                        tipo, direccion, precio, contexto_json, grid_params_json, score,
                        rechazos_json, estado_maquina))

                # Obtener ID dentro de la misma transacción
                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                await db.commit()

                if row and row[0]:
                    return row[0]
                else:
                    # Fallback: buscar por timestamp exacto
                    cursor2 = await db.execute(
                        "SELECT id FROM auditoria_eventos WHERE symbol = ? AND timestamp_utc = ? ORDER BY id DESC LIMIT 1",
                        (symbol, ts_utc_str)
                    )
                    row2 = await cursor2.fetchone()
                    return row2[0] if row2 else None

        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️ DB locked (auditoría intento {attempt+1}/5), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error definitivo guardando evento auditoría: {e}")
                return None

    return None

async def guardar_muestra_post(evento_id, symbol, timestamp_utc, precio, minutos_desde_disparo):
    """Guarda una muestra de precio post-disparo."""
    timestamp_local = utc_to_local(timestamp_utc).isoformat() if timestamp_utc else now_local().isoformat()

    await _execute_with_retry("""
        INSERT INTO auditoria_muestras_post
        (evento_id, symbol, timestamp_utc, timestamp_local, precio, minutos_desde_disparo)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (evento_id, symbol, timestamp_utc.isoformat() if timestamp_utc else now_utc().isoformat(),
            timestamp_local, precio, minutos_desde_disparo))

async def guardar_evento_post(evento_id, symbol, timestamp_utc, tipo_evento, precio,
                               distancia_desde_entrada_pct=None, grid_rentable_aqui=None, nota=None):
    """Guarda un evento significativo post-disparo."""
    timestamp_local = utc_to_local(timestamp_utc).isoformat() if timestamp_utc else now_local().isoformat()

    await _execute_with_retry("""
        INSERT INTO auditoria_eventos_post
        (evento_id, symbol, timestamp_utc, timestamp_local, tipo_evento, precio,
         distancia_desde_entrada_pct, grid_rentable_aqui, nota)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (evento_id, symbol, timestamp_utc.isoformat() if timestamp_utc else now_utc().isoformat(),
            timestamp_local, tipo_evento, precio, distancia_desde_entrada_pct,
            1 if grid_rentable_aqui else 0, nota))

async def guardar_vela_post(evento_id, symbol, timestamp_utc, open_p, high, low, close, volume,
                            minutos_desde_disparo):
    """Guarda una vela 15m posterior al disparo."""
    timestamp_local = utc_to_local(timestamp_utc).isoformat() if timestamp_utc else now_local().isoformat()

    await _execute_with_retry("""
        INSERT INTO auditoria_velas_post
        (evento_id, symbol, timestamp_utc, timestamp_local, open, high, low, close, volume, minutos_desde_disparo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (evento_id, symbol, timestamp_utc.isoformat() if timestamp_utc else now_utc().isoformat(),
            timestamp_local, open_p, high, low, close, volume, minutos_desde_disparo))

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 5.1: FUNCIONES DE PERSISTENCIA DE DISPAROS ACTIVOS
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_disparo_activo(evento_id, symbol, timestamp_utc, precio_entrada, direccion,
                                  grid_params_json, maximo_visto, minimo_visto,
                                  primera_vez_en_grid_json, primera_vez_fuera_rango_json,
                                  muestras_guardadas, horas_seguimiento):
    """Guarda o actualiza un disparo activo en la base de datos para persistencia."""
    await _execute_with_retry("""
        INSERT OR REPLACE INTO auditoria_disparos_activos
        (evento_id, symbol, timestamp_utc, precio_entrada, direccion, grid_params_json,
         maximo_visto, minimo_visto, primera_vez_en_grid_json, primera_vez_fuera_rango_json,
         muestras_guardadas, horas_seguimiento)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (evento_id, symbol, timestamp_utc.isoformat() if timestamp_utc else now_utc().isoformat(),
            precio_entrada, direccion, grid_params_json,
            maximo_visto, minimo_visto,
            primera_vez_en_grid_json, primera_vez_fuera_rango_json,
            muestras_guardadas, horas_seguimiento))

async def cargar_disparos_activos():
    """Carga todos los disparos activos persistentes (útil al reiniciar el bot)."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_disparos_activos
            ORDER BY timestamp_utc ASC
        """)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def actualizar_disparo_activo(evento_id, maximo_visto=None, minimo_visto=None,
                                      primera_vez_en_grid_json=None, primera_vez_fuera_rango_json=None,
                                      muestras_guardadas=None):
    """Actualiza campos de un disparo activo existente."""
    campos = []
    valores = []
    if maximo_visto is not None:
        campos.append("maximo_visto = ?")
        valores.append(maximo_visto)
    if minimo_visto is not None:
        campos.append("minimo_visto = ?")
        valores.append(minimo_visto)
    if primera_vez_en_grid_json is not None:
        campos.append("primera_vez_en_grid_json = ?")
        valores.append(primera_vez_en_grid_json)
    if primera_vez_fuera_rango_json is not None:
        campos.append("primera_vez_fuera_rango_json = ?")
        valores.append(primera_vez_fuera_rango_json)
    if muestras_guardadas is not None:
        campos.append("muestras_guardadas = ?")
        valores.append(muestras_guardadas)

    if not campos:
        return

    valores.append(evento_id)
    sql = f"UPDATE auditoria_disparos_activos SET {', '.join(campos)} WHERE evento_id = ?"
    await _execute_with_retry(sql, tuple(valores))

async def eliminar_disparo_activo(evento_id):
    """Elimina un disparo activo (cuando el seguimiento termina)."""
    await _execute_with_retry(
        "DELETE FROM auditoria_disparos_activos WHERE evento_id = ?",
        (evento_id,)
    )

async def cargar_eventos_dia(fecha_local: str):
    """Carga todos los eventos de auditoría de un día específico (fecha local)."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_eventos
            WHERE fecha = ?
            ORDER BY timestamp_utc ASC
        """, (fecha_local,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_muestras_post_evento(evento_id: int):
    """Carga las muestras post-disparo de un evento específico."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_muestras_post
            WHERE evento_id = ?
            ORDER BY minutos_desde_disparo ASC
        """, (evento_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_eventos_post_evento(evento_id: int):
    """Carga los eventos significativos post-disparo de un evento específico."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_eventos_post
            WHERE evento_id = ?
            ORDER BY timestamp_utc ASC
        """, (evento_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_velas_post_evento(evento_id: int):
    """Carga las velas 15m posteriores de un evento específico."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_velas_post
            WHERE evento_id = ?
            ORDER BY minutos_desde_disparo ASC
        """, (evento_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# ═══════════════════════════════════════════════════════════════════════════════
# V5.7: FUNCIONES DE SEGUIMIENTO VIRTUAL POST NEAR-MISS
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_near_miss_seguimiento(symbol, timestamp_inicio, precio_inicio, direccion_nm,
                                         score, umbral, filtros_rechazo=None):
    """Inicia un seguimiento virtual post near-miss. Retorna el ID del seguimiento."""
    db = await _get_db()
    for attempt in range(5):
        try:
            async with _db_lock:
                await db.execute("""
                    INSERT INTO near_miss_seguimientos
                    (symbol, timestamp_inicio, precio_inicio, direccion_nm, score, umbral, filtros_rechazo)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (symbol, timestamp_inicio, precio_inicio, direccion_nm, score, umbral,
                        json.dumps(filtros_rechazo) if filtros_rechazo else None))

                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                await db.commit()

                if row and row[0]:
                    return row[0]
                else:
                    cursor2 = await db.execute(
                        "SELECT id FROM near_miss_seguimientos WHERE symbol = ? AND timestamp_inicio = ? ORDER BY id DESC LIMIT 1",
                        (symbol, timestamp_inicio)
                    )
                    row2 = await cursor2.fetchone()
                    return row2[0] if row2 else None

        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️ DB locked (near-miss seguimiento intento {attempt+1}/5), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error guardando near-miss seguimiento: {e}")
                return None
    return None

async def actualizar_near_miss_muestras(seguimiento_id, muestras_json, precio_max=None,
                                         precio_min=None, precio_fin=None):
    """Actualiza las muestras y precios extremos de un seguimiento activo."""
    campos = ["muestras_json = ?"]
    valores = [json.dumps(muestras_json)]
    if precio_max is not None:
        campos.append("precio_max = ?")
        valores.append(precio_max)
    if precio_min is not None:
        campos.append("precio_min = ?")
        valores.append(precio_min)
    if precio_fin is not None:
        campos.append("precio_fin = ?")
        valores.append(precio_fin)

    valores.append(seguimiento_id)
    sql = f"UPDATE near_miss_seguimientos SET {', '.join(campos)} WHERE id = ?"
    await _execute_with_retry(sql, tuple(valores))

async def finalizar_near_miss_seguimiento(seguimiento_id, timestamp_fin, precio_fin,
                                           movimiento_pct, direccion_real, acerto_bot,
                                           hubiera_sido_rentable, notas=None):
    """Finaliza un seguimiento virtual con los resultados de la evaluación."""
    await _execute_with_retry("""
        UPDATE near_miss_seguimientos
        SET timestamp_fin = ?, precio_fin = ?, movimiento_pct = ?,
            direccion_real = ?, acerto_bot = ?, hubiera_sido_rentable = ?, notas = ?
        WHERE id = ?
    """, (timestamp_fin, precio_fin, movimiento_pct, direccion_real,
            1 if acerto_bot else 0, 1 if hubiera_sido_rentable else 0, notas, seguimiento_id))

async def cargar_near_miss_seguimientos_dia(fecha_local: str):
    """Carga todos los seguimientos de un día específico (fecha local)."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        # Convertir fecha local a rango de timestamps UTC para la consulta
        tz = get_tz()
        fecha_inicio = datetime.strptime(fecha_local, "%Y-%m-%d")
        fecha_inicio = tz.localize(fecha_inicio)
        fecha_fin = fecha_inicio + timedelta(days=1)

        ts_inicio = fecha_inicio.astimezone(pytz.UTC).timestamp()
        ts_fin = fecha_fin.astimezone(pytz.UTC).timestamp()

        cursor = await db.execute("""
            SELECT * FROM near_miss_seguimientos
            WHERE timestamp_inicio >= ? AND timestamp_inicio < ?
            ORDER BY timestamp_inicio ASC
        """, (ts_inicio, ts_fin))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_near_miss_seguimientos_activos():
    """Carga seguimientos que aún no han finalizado (timestamp_fin IS NULL)."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM near_miss_seguimientos
            WHERE timestamp_fin IS NULL
            ORDER BY timestamp_inicio ASC
        """)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# ═══════════════════════════════════════════════════════════════════════════════
# V5.9.2: FUNCIONES DE GRID NEUTRAL (Simulación Virtual)
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_grid_estado(symbol, timestamp_inicio, estado='ACTIVO', direccion='NEUTRAL',
                               precio_entrada=None, grid_params_json=None, evento_auditoria_id=None):
    """Crea un nuevo grid estado. Retorna el ID del grid."""
    db = await _get_db()
    for attempt in range(5):
        try:
            async with _db_lock:
                await db.execute("""
                    INSERT INTO grid_estados
                    (symbol, timestamp_inicio, estado, direccion, precio_entrada, grid_params_json, evento_auditoria_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (symbol, timestamp_inicio, estado, direccion, precio_entrada, grid_params_json, evento_auditoria_id))

                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                await db.commit()

                if row and row[0]:
                    return row[0]
                else:
                    cursor2 = await db.execute(
                        "SELECT id FROM grid_estados WHERE symbol = ? AND timestamp_inicio = ? ORDER BY id DESC LIMIT 1",
                        (symbol, timestamp_inicio)
                    )
                    row2 = await cursor2.fetchone()
                    return row2[0] if row2 else None

        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️ DB locked (grid estado intento {attempt+1}/5), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error guardando grid estado: {e}")
                return None
    return None


async def actualizar_grid_estado(grid_id, estado=None, timestamp_fin=None):
    """Actualiza el estado de un grid existente."""
    campos = []
    valores = []
    if estado is not None:
        campos.append("estado = ?")
        valores.append(estado)
    if timestamp_fin is not None:
        campos.append("timestamp_fin = ?")
        valores.append(timestamp_fin)

    if not campos:
        return

    valores.append(grid_id)
    sql = f"UPDATE grid_estados SET {', '.join(campos)} WHERE id = ?"
    await _execute_with_retry(sql, tuple(valores))


async def guardar_grid_simulacion(grid_id, symbol, timestamp_inicio, precio_inicio,
                                   posiciones_abiertas_json=None, estado='SIMULANDO'):
    """Crea una nueva simulación de grid. Retorna el ID de la simulación."""
    db = await _get_db()
    for attempt in range(5):
        try:
            async with _db_lock:
                await db.execute("""
                    INSERT INTO grid_simulaciones
                    (grid_id, symbol, timestamp_inicio, precio_inicio, posiciones_abiertas_json, estado)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (grid_id, symbol, timestamp_inicio, precio_inicio, posiciones_abiertas_json, estado))

                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                await db.commit()

                if row and row[0]:
                    return row[0]
                else:
                    cursor2 = await db.execute(
                        "SELECT id FROM grid_simulaciones WHERE grid_id = ? AND timestamp_inicio = ? ORDER BY id DESC LIMIT 1",
                        (grid_id, timestamp_inicio)
                    )
                    row2 = await cursor2.fetchone()
                    return row2[0] if row2 else None

        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️ DB locked (grid sim intento {attempt+1}/5), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error guardando grid simulación: {e}")
                return None
    return None


async def actualizar_grid_simulacion(grid_id, estado=None, timestamp_fin=None, precio_fin=None,
                                      pnl_bruto=None, pnl_neto=None, fees_totales=None,
                                      slippage_total=None, trades_completados=None,
                                      trades_kill_switch=None, posiciones_abiertas_json=None,
                                      posiciones_atrapadas_json=None):
    """Actualiza una simulación de grid existente."""
    campos = []
    valores = []
    if estado is not None:
        campos.append("estado = ?")
        valores.append(estado)
    if timestamp_fin is not None:
        campos.append("timestamp_fin = ?")
        valores.append(timestamp_fin)
    if precio_fin is not None:
        campos.append("precio_fin = ?")
        valores.append(precio_fin)
    if pnl_bruto is not None:
        campos.append("pnl_bruto = ?")
        valores.append(pnl_bruto)
    if pnl_neto is not None:
        campos.append("pnl_neto = ?")
        valores.append(pnl_neto)
    if fees_totales is not None:
        campos.append("fees_totales = ?")
        valores.append(fees_totales)
    if slippage_total is not None:
        campos.append("slippage_total = ?")
        valores.append(slippage_total)
    if trades_completados is not None:
        campos.append("trades_completados = ?")
        valores.append(trades_completados)
    if trades_kill_switch is not None:
        campos.append("trades_kill_switch = ?")
        valores.append(trades_kill_switch)
    if posiciones_abiertas_json is not None:
        campos.append("posiciones_abiertas_json = ?")
        valores.append(posiciones_abiertas_json)
    if posiciones_atrapadas_json is not None:
        campos.append("posiciones_atrapadas_json = ?")
        valores.append(posiciones_atrapadas_json)

    if not campos:
        return

    valores.append(grid_id)
    sql = f"UPDATE grid_simulaciones SET {', '.join(campos)} WHERE grid_id = ?"
    await _execute_with_retry(sql, tuple(valores))


async def cargar_grid_activo(symbol):
    """Carga el grid activo más reciente para un símbolo."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM grid_estados
            WHERE symbol = ? AND estado = 'ACTIVO'
            ORDER BY timestamp_inicio DESC LIMIT 1
        """, (symbol,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def cargar_simulacion_activa(grid_id):
    """Carga la simulación activa para un grid_id."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM grid_simulaciones
            WHERE grid_id = ? AND estado = 'SIMULANDO'
            ORDER BY timestamp_inicio DESC LIMIT 1
        """, (grid_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def cargar_grids_huerfanos(timeout_seg):
    """Carga grids ACTIVOS sin simulación activa durante más de timeout_seg."""
    db = await _get_db()
    ahora = int(datetime.now(pytz.UTC).timestamp())
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT g.id, g.symbol, g.timestamp_inicio
            FROM grid_estados g
            WHERE g.estado = 'ACTIVO'
            AND (
                NOT EXISTS (
                    SELECT 1 FROM grid_simulaciones s
                    WHERE s.grid_id = g.id AND s.estado = 'SIMULANDO'
                )
                OR EXISTS (
                    SELECT 1 FROM grid_simulaciones s2
                    WHERE s2.grid_id = g.id AND s2.estado = 'SIMULANDO'
                    AND (? - s2.timestamp_inicio) > ?
                )
            )
        """, (ahora, timeout_seg))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def forzar_aborto_grid_huerfano(grid_id, sim_id=None):
    """Marca un grid huérfano y su simulación como ABORTADO."""
    db = await _get_db()
    async with _db_lock:
        await db.execute(
            "UPDATE grid_estados SET estado = 'ABORTADO', timestamp_fin = ? WHERE id = ?",
            (int(datetime.now(pytz.UTC).timestamp()), grid_id)
        )
        if sim_id:
            await db.execute(
                "UPDATE grid_simulaciones SET estado = 'ABORTADO', timestamp_fin = ? WHERE id = ?",
                (int(datetime.now(pytz.UTC).timestamp()), sim_id)
            )
        await db.commit()


async def guardar_grid_estado_atomico(symbol, timestamp_inicio, precio_inicio, grid_params_json,
                                       posiciones_abiertas_json, evento_auditoria_id=None,
                                       direccion="NEUTRAL"):
    """Transacción atómica: grid_estado + grid_simulacion en una sola transacción."""
    db = await _get_db()
    ts = int(datetime.now(pytz.UTC).timestamp())
    for attempt in range(5):
        try:
            async with _db_lock:
                # 1. Insertar grid_estado
                await db.execute("""
                    INSERT INTO grid_estados
                    (symbol, timestamp_inicio, estado, direccion, precio_entrada, grid_params_json, evento_auditoria_id)
                    VALUES (?, ?, 'ACTIVO', ?, ?, ?, ?)
                """, (symbol, timestamp_inicio, direccion, precio_inicio, grid_params_json, evento_auditoria_id))

                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                grid_id = row[0] if row else None

                if not grid_id:
                    await db.rollback()
                    return None

                # 2. Insertar grid_simulacion
                await db.execute("""
                    INSERT INTO grid_simulaciones
                    (grid_id, symbol, timestamp_inicio, precio_inicio, posiciones_abiertas_json, estado)
                    VALUES (?, ?, ?, ?, ?, 'SIMULANDO')
                """, (grid_id, symbol, timestamp_inicio, precio_inicio, posiciones_abiertas_json))

                cursor2 = await db.execute("SELECT last_insert_rowid()")
                row2 = await cursor2.fetchone()
                sim_id = row2[0] if row2 else None

                await db.commit()
                return {"grid_id": grid_id, "sim_id": sim_id}

        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️ DB locked (transacción atómica intento {attempt+1}/5), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error transacción atómica grid: {e}")
                try:
                    await db.rollback()
                except:
                    pass
                return None
    return None


async def cargar_todos_grids_activos():
    """Carga todos los grids activos con su simulación."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT g.id as grid_id, g.symbol, g.timestamp_inicio, g.estado as grid_estado,
                   g.precio_entrada, g.grid_params_json,
                   s.id as sim_id, s.posiciones_abiertas_json, s.posiciones_atrapadas_json,
                   s.pnl_neto, s.estado as sim_estado
            FROM grid_estados g
            LEFT JOIN grid_simulaciones s ON g.id = s.grid_id
            WHERE g.estado = 'ACTIVO'
        """)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def actualizar_posiciones_abiertas(grid_id, posiciones_json, posiciones_atrapadas_json=None):
    """Actualiza las posiciones abiertas de una simulación."""
    campos = ["posiciones_abiertas_json = ?"]
    valores = [posiciones_json]
    if posiciones_atrapadas_json is not None:
        campos.append("posiciones_atrapadas_json = ?")
        valores.append(posiciones_atrapadas_json)
    valores.append(grid_id)
    sql = f"UPDATE grid_simulaciones SET {', '.join(campos)} WHERE grid_id = ?"
    await _execute_with_retry(sql, tuple(valores))

# ═══════════════════════════════════════════════════════════════════════════════
# FASE 0: FUNCIONES DE EJECUCIÓN REAL
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_grid_ejecucion(symbol, direction, trading_mode, capital_asignado,
                                  apalancamiento_usado, precio_entrada, grid_params_json=None,
                                  timestamp_inicio=None):
    """Guarda un grid de ejecución real. Retorna el ID."""
    db = await _get_db()
    for attempt in range(5):
        try:
            async with _db_lock:
                await db.execute("""
                    INSERT INTO grid_ejecuciones
                    (symbol, direction, estado, trading_mode, capital_asignado,
                     apalancamiento_usado, precio_entrada, grid_params_json, timestamp_inicio)
                    VALUES (?, ?, 'ACTIVO', ?, ?, ?, ?, ?, ?)
                """, (symbol, direction, trading_mode, capital_asignado,
                      apalancamiento_usado, precio_entrada, grid_params_json,
                      timestamp_inicio or int(datetime.now(pytz.UTC).timestamp())))

                cursor = await db.execute("SELECT last_insert_rowid()")
                row = await cursor.fetchone()
                await db.commit()
                return row[0] if row else None
        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < 4:
                wait = 0.1 * (2 ** attempt)
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ Error guardando grid ejecución: {e}")
                return None
    return None


async def guardar_orden_ejecucion(grid_ejecucion_id, binance_order_id, client_order_id,
                                   symbol, side, tipo_orden, price, quantity):
    """Guarda una orden de ejecución real."""
    await _execute_with_retry("""
        INSERT INTO grid_ejecucion_ordenes
        (grid_ejecucion_id, binance_order_id, client_order_id, symbol, side, tipo_orden, price, quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (grid_ejecucion_id, binance_order_id, client_order_id, symbol, side, tipo_orden, price, quantity))


async def actualizar_orden_fill(orden_id, binance_trade_id, price, qty, commission,
                                 commission_asset, realized_pnl, timestamp):
    """Actualiza un fill de una orden."""
    await _execute_with_retry("""
        UPDATE grid_ejecucion_ordenes
        SET quantity_filled = quantity_filled + ?,
            status = CASE WHEN quantity_filled + ? >= quantity THEN 'FILLED' ELSE 'PARTIALLY_FILLED' END
        WHERE id = ?
    """, (qty, qty, orden_id))

    await _execute_with_retry("""
        INSERT INTO grid_ejecucion_fills
        (grid_ejecucion_id, orden_id, binance_trade_id, price, qty, commission,
         commission_asset, realized_pnl, timestamp)
        VALUES (
            (SELECT grid_ejecucion_id FROM grid_ejecucion_ordenes WHERE id = ?),
            ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (orden_id, orden_id, binance_trade_id, price, qty, commission,
          commission_asset, realized_pnl, timestamp))


async def cargar_grid_ejecuciones_activos():
    """Carga grids de ejecución en estado ACTIVO."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM grid_ejecuciones WHERE estado = 'ACTIVO' ORDER BY created_at ASC
        """)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def cargar_ordenes_por_grid(grid_ejecucion_id):
    """Carga órdenes de un grid de ejecución."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM grid_ejecucion_ordenes WHERE grid_ejecucion_id = ? ORDER BY id ASC
        """, (grid_ejecucion_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def actualizar_grid_ejecucion_cierre(grid_id, estado, pnl_real, fees_real, razon_cierre):
    """Cierra un grid de ejecución."""
    await _execute_with_retry("""
        UPDATE grid_ejecuciones
        SET estado = ?, pnl_real = ?, fees_real = ?, razon_cierre = ?, closed_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (estado, pnl_real, fees_real, razon_cierre, grid_id))


# ═══════════════════════════════════════════════════════════════════════════════
# CR16: FUNCIONES DE ACCESO A FILLS (TRACKING PROACTIVO)
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_fill_tracking(grid_ejecucion_id, symbol,
                                 binance_trade_id, binance_order_id,
                                 side, price, qty,
                                 commission, commission_asset,
                                 realized_pnl, timestamp_ms):
    """Guarda un fill detectado para procesamiento posterior."""
    await _execute_with_retry("""
        INSERT OR IGNORE INTO fills_tracking
        (grid_ejecucion_id, symbol, binance_trade_id, binance_order_id,
         side, price, qty, commission, commission_asset, realized_pnl,
         timestamp_ms, procesado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (grid_ejecucion_id, symbol, binance_trade_id, binance_order_id,
          side, price, qty, commission, commission_asset, realized_pnl,
          timestamp_ms))


async def cargar_fills_sin_procesar(grid_ejecucion_id):
    """Carga fills no procesados de un grid."""
    db = await _get_db()
    async with _db_lock:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM fills_tracking
            WHERE grid_ejecucion_id = ? AND procesado = 0
            ORDER BY timestamp_ms ASC
        """, (grid_ejecucion_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def marcar_fill_procesado(fill_id, posicion_id):
    """Marca un fill como procesado y vinculado a una posición."""
    await _execute_with_retry("""
        UPDATE fills_tracking
        SET procesado = 1, posicion_id = ?, fecha_procesamiento = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (posicion_id, fill_id))


async def obtener_ultimo_trade_timestamp(symbol):
    """Obtiene el timestamp del último trade procesado para un símbolo."""
    db = await _get_db()
    async with _db_lock:
        cursor = await db.execute("""
            SELECT MAX(timestamp_ms) FROM fills_tracking WHERE symbol = ?
        """, (symbol,))
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 0

# ═══════════════════════════════════════════════════════════════════════════════
# CR2: FUNCIONES DE ACCESO A EVENTOS DE PnL
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_pnl_evento(grid_ejecucion_id: int, symbol: str, tipo_evento: str,
                               side: str, binance_trade_id: str, binance_order_id: str,
                               price: float, qty: float, commission: float,
                               commission_asset: str, realized_pnl: float,
                               notional: float, timestamp_ms: int):
    """Guarda un evento de PnL inmediatamente al procesar un trade."""
    timestamp_local = datetime.now(get_tz()).isoformat()

    await _execute_with_retry("""
        INSERT OR IGNORE INTO pnl_eventos
        (grid_ejecucion_id, symbol, tipo_evento, side, binance_trade_id,
         binance_order_id, price, qty, commission, commission_asset,
         realized_pnl, notional, timestamp_ms, timestamp_local)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (grid_ejecucion_id, symbol, tipo_evento, side, binance_trade_id,
          binance_order_id, price, qty, commission, commission_asset,
          realized_pnl, notional, timestamp_ms, timestamp_local))


async def calcular_pnl_acumulado(grid_ejecucion_id: int) -> dict:
    """Calcula PnL acumulado desde la base de datos (fuente de verdad)."""
    db = await _get_db()
    async with _db_lock:
        cursor = await db.execute("""
            SELECT COALESCE(SUM(realized_pnl), 0) as pnl_total,
                   COALESCE(SUM(commission), 0) as fees_total,
                   COUNT(*) as total_trades,
                   SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as trades_ganadores,
                   SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as trades_perdedores
            FROM pnl_eventos
            WHERE grid_ejecucion_id = ?
        """, (grid_ejecucion_id,))
        row = await cursor.fetchone()

        return {
            'pnl_real': float(row[0]),
            'fees_real': float(row[1]),
            'total_trades': int(row[2]),
            'trades_ganadores': int(row[3]),
            'trades_perdedores': int(row[4])
        }


async def obtener_pnl_por_tipo(grid_ejecucion_id: int) -> dict:
    """Desglose de PnL por tipo de evento."""
    db = await _get_db()
    async with _db_lock:
        cursor = await db.execute("""
            SELECT tipo_evento, COALESCE(SUM(realized_pnl), 0), COUNT(*)
            FROM pnl_eventos
            WHERE grid_ejecucion_id = ?
            GROUP BY tipo_evento
        """, (grid_ejecucion_id,))
        rows = await cursor.fetchall()
        return {row[0]: {'pnl': float(row[1]), 'count': int(row[2])} for row in rows}
