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
        await _db_conn.execute("PRAGMA busy_timeout=10000")  # 10s de espera antes de "database is locked"
        await _db_conn.commit()
    return _db_conn

async def close_db():
    """Cierra la conexión persistente."""
    global _db_conn
    if _db_conn:
        await _db_conn.close()
        _db_conn = None

async def _execute_with_retry(sql, params=(), max_retries=5):
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
                wait = 0.1 * (2 ** attempt)  # 0.1, 0.2, 0.4, 0.8, 1.6s
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

        await db.commit()
    print("🗄️ Base de datos inicializada (WAL mode activado) + Tablas auditoría + Disparos activos persistentes + Near-miss seguimientos")

async def insertar_vela(symbol: str, tf: str, vela: dict):
    tabla = f"velas_{tf}"
    await _execute_with_retry(f"""
        INSERT OR REPLACE INTO {tabla}
        (symbol, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (symbol, vela['timestamp'], vela['open'], vela['high'],
          vela['low'], vela['close'], vela['volume']))

async def cargar_velas_historicas(symbol: str, tf: str, limit: int):
    tabla = f"velas_{tf}"
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
