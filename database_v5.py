"""
database_v5.py — Capa de persistencia V5 (adaptada a MySQL / MariaDB).

Capa de persistencia sobre MySQL/MariaDB mediante aiomysql (driver async).
Para NO romper la estructura ni las ~50 funciones existentes, se
introduce una delgada capa de compatibilidad (_CompatConnection / Row) que
expone una interfaz idéntica a la que el código ya utilizaba:

    db.execute(sql, params)   -> cursor con .fetchall() / .fetchone()
    db.executemany(sql, seq)  -> cursor
    db.commit() / db.rollback() / db.close()
    db.row_factory = Row      -> filas con acceso por índice y por nombre
                                 (dict(row) sigue funcionando)

La capa traduce, de forma centralizada y segura:
    • placeholders  '?'          -> '%s'   (paramstyle de PyMySQL/aiomysql)
    • '%' literales (LIKE, DATE_FORMAT) -> '%%'  (para no interferir con el
      formateo del driver; conserva la parametrización real = sin inyección SQL)
    • INSERT OR REPLACE INTO     -> REPLACE INTO
    • INSERT OR IGNORE INTO      -> INSERT IGNORE INTO
    • SELECT last_insert_rowid() -> SELECT LAST_INSERT_ID()
    • PRAGMA table_info(x)       -> consulta a information_schema
    • CREATE INDEX IF NOT EXISTS -> portabilidad MySQL/MariaDB

De este modo las funciones de negocio permanecen intactas.

V6 (pool de conexiones): la conexión única persistente se sustituyó por un
pool (min 2 / max 8). La API pública no cambia:
    • `async with _db_lock:` sigue serializando las transacciones
      multi-sentencia (INSERT + SELECT LAST_INSERT_ID(), batches con varios
      statements, etc.) y además vincula una conexión del pool al bloque.
    • El resto de operaciones (una sentencia: lecturas y escrituras sueltas)
      usan el pool directamente, sin pasar por el lock global.
    • `_get_db()` conserva la interfaz histórica (execute/executemany/commit/
      rollback/close) sobre el pool.
"""

import asyncio
import re
import time
import warnings
from contextlib import asynccontextmanager

import aiomysql
import pymysql

from config import CONFIG
from datetime import datetime, timedelta
import pytz
import json

# Silenciar WARNINGS inofensivos del driver (CREATE TABLE IF NOT EXISTS cuando
# la tabla ya existe, INSERT IGNORE ante duplicados). Son avisos, no errores:
# las violaciones reales de integridad siguen lanzando excepciones.
warnings.filterwarnings("ignore", message=".*already exists.*")
warnings.filterwarnings("ignore", message=".*Duplicate entry.*")
warnings.filterwarnings("ignore", message=".*PRIMARY KEY.*")

# ─── Pool de conexiones (V6) ───
_pool_q = None                     # asyncio.Queue con conexiones libres
_pool_live = set()                 # conexiones creadas (para cierre total)
_pool_crear_lock = asyncio.Lock()  # protege creación/cierre del pool
_devoluciones_pendientes = []      # devoluciones diferidas (release síncrono)
_POOL_MIN = 2                      # conexiones iniciales calientes
_POOL_MAX = 8                      # conexiones simultáneas máximas


# ═══════════════════════════════════════════════════════════════════════════════
# Capa de compatibilidad MySQL/MariaDB (interfaz unificada de acceso a datos)
# ═══════════════════════════════════════════════════════════════════════════════

class Row:
    """Fila con acceso por índice (row[0]) y por nombre (row['col']).

    Filas con acceso por índice y por nombre, tal y como las usaba el
    código: admite `dict(row)`, `row[i]` y `row['nombre']`.
    """
    __slots__ = ("_cols", "_vals")

    def __init__(self, cols, vals):
        self._cols = tuple(cols)
        self._vals = tuple(vals)

    def keys(self):
        return list(self._cols)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._vals[key]
        try:
            idx = self._cols.index(key)
        except ValueError:
            raise KeyError(key)
        return self._vals[idx]

    def __len__(self):
        return len(self._vals)

    def __iter__(self):
        return iter(self._vals)

    def __repr__(self):
        return "<Row %s>" % dict(zip(self._cols, self._vals))


class _CompatCursor:
    """Cursor precargado: los resultados se materializan al ejecutar."""

    def __init__(self, cols, rows, rowcount=-1, lastrowid=None):
        self._cols = tuple(cols)
        self._rows = list(rows)
        self._pos = 0
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    @property
    def description(self):
        """DB-API 2.0: None sin resultset, si no secuencia de 7-tuplas por columna."""
        if not self._cols:
            return None
        return tuple((c, None, None, None, None, None, None) for c in self._cols)

    def _mk(self, r):
        return Row(self._cols, r)

    async def fetchall(self):
        out = [self._mk(r) for r in self._rows[self._pos:]]
        self._pos = len(self._rows)
        return out

    async def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        r = self._rows[self._pos]
        self._pos += 1
        return self._mk(r)

    async def fetchmany(self, size=1):
        chunk = self._rows[self._pos:self._pos + size]
        self._pos += len(chunk)
        return [self._mk(r) for r in chunk]

    async def close(self):
        return None


# Traducciones SQL centralizadas (sintaxis heredada -> MySQL/MariaDB).
_REPLACEMENTS = [
    (re.compile(r"INSERT\s+OR\s+REPLACE\s+INTO", re.I), "REPLACE INTO"),
    (re.compile(r"INSERT\s+OR\s+IGNORE\s+INTO", re.I), "INSERT IGNORE INTO"),
    (re.compile(r"last_insert_rowid\s*\(\s*\)", re.I), "LAST_INSERT_ID()"),
]

_PRAGMA_TABLE_INFO = re.compile(r"^\s*PRAGMA\s+table_info\s*\(\s*([A-Za-z0-9_]+)\s*\)\s*$", re.I)


def _translate_sql(sql: str) -> str:
    """Convierte SQL con placeholders '?' y sintaxis heredada a formato MySQL."""
    # 1. Funciones / palabras clave heredadas.
    for pat, rep in _REPLACEMENTS:
        sql = pat.sub(rep, sql)
    # 2. Portabilidad de índices: MySQL no soporta IF NOT EXISTS en CREATE INDEX.
    if re.match(r"\s*CREATE\s+(UNIQUE\s+)?INDEX", sql, re.I):
        sql = re.sub(r"\s+IF\s+NOT\s+EXISTS", "", sql, count=1, flags=re.I)
    # 3. Escapar '%' literales (LIKE '%x%', DATE_FORMAT '%Y-%m-%d') para que el
    #    formateo %-style del driver no los interprete.
    sql = sql.replace("%", "%%")
    # 4. Placeholders qmark -> %s (paramstyle de PyMySQL).
    sql = sql.replace("?", "%s")
    return sql


class _CompatConnection:
    """Conexión persistente con la interfaz unificada de acceso a datos."""

    def __init__(self, raw):
        self._raw = raw
        self._op_lock = asyncio.Lock()   # serializa cada operación de red
        self.row_factory = Row           # aceptado por compatibilidad

    # ---- vida de la conexión ----
    def _alive(self):
        return not getattr(self._raw, "closed", False)

    async def _ping(self):
        async with self._op_lock:
            await self._raw.ping(reconnect=True)

    # ---- ejecución ----
    async def execute(self, sql, params=()):
        m = _PRAGMA_TABLE_INFO.match(sql)
        if m:
            return await self._pragma_table_info(m.group(1))
        is_create_index = bool(re.match(r"\s*CREATE\s+(UNIQUE\s+)?INDEX", sql, re.I))
        sql2 = _translate_sql(sql)
        args = tuple(params) if params is not None else ()
        async with self._op_lock:
            cur = await self._raw.cursor()
            try:
                await cur.execute(sql2, args)
                cols, rows = await self._drain(cur)
                rc = cur.rowcount
                lrid = cur.lastrowid
            except Exception as e:
                # CREATE INDEX idempotente: si el índice ya existe, no es error.
                if is_create_index and _is_missing_index(e):
                    cols, rows, rc, lrid = (), [], 0, None
                else:
                    raise
            finally:
                await cur.close()
        return _CompatCursor(cols, rows, rowcount=rc, lastrowid=lrid)

    async def executemany(self, sql, seq):
        sql2 = _translate_sql(sql)
        args = [tuple(p) for p in seq]
        async with self._op_lock:
            cur = await self._raw.cursor()
            try:
                await cur.executemany(sql2, args)
                cols, rows = await self._drain(cur)
                rc = cur.rowcount
                lrid = cur.lastrowid
            finally:
                await cur.close()
        return _CompatCursor(cols, rows, rowcount=rc, lastrowid=lrid)

    async def _drain(self, cur):
        desc = cur.description
        cols = tuple(d[0] for d in desc) if desc else tuple()
        rows = []
        if desc:
            fetched = await cur.fetchall()
            if fetched:
                rows = [tuple(r) for r in fetched]
        return cols, rows

    async def _pragma_table_info(self, tabla):
        """Sustituto de PRAGMA table_info(tabla) usando information_schema."""
        sql = ("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
               "EXTRA FROM information_schema.COLUMNS "
               "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
               "ORDER BY ORDINAL_POSITION")
        async with self._op_lock:
            cur = await self._raw.cursor()
            try:
                await cur.execute(sql, (tabla,))
                fetched = (await cur.fetchall()) or []
            finally:
                await cur.close()
        cols = ("cid", "name", "type", "notnull", "dflt_value", "pk")
        rows = []
        for i, r in enumerate(fetched):
            # Formato heredado: r[1] == nombre de columna.
            rows.append((i, r[0], r[1], 0 if r[2] == "YES" else 1, r[3], 0))
        return _CompatCursor(cols, rows)

    # ---- transacción ----
    async def commit(self):
        async with self._op_lock:
            await self._raw.commit()

    async def rollback(self):
        async with self._op_lock:
            await self._raw.rollback()

    async def close(self):
        async with self._op_lock:
            await self._raw.ensure_closed()


async def _crear_conexion_raw():
    """Crea una conexión MySQL/MariaDB cruda con la sesión configurada."""
    raw = await aiomysql.connect(
        host=CONFIG.db_host,
        port=CONFIG.db_port,
        user=CONFIG.db_user,
        password=CONFIG.db_password,
        db=CONFIG.db_name,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=CONFIG.db_connect_timeout,
    )
    # Ajustes de sesión: comportamiento equivalente al esquema original.
    async with raw.cursor() as cur:
        # utf8mb4 completo
        await cur.execute("SET NAMES utf8mb4")
        # No aplicar restricciones de FOREIGN KEY (no se aplicaban antes).
        await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        # Zona horaria UTC: FROM_UNIXTIME() devuelve UTC, igual que las
        # conversiones de tiempo heredadas. Preserva la semántica original.
        await cur.execute("SET time_zone = '+00:00'")
        # Modo permisivo (sin STRICT_*): admite coerciones laxas y evita
        # errores por tipos laxos heredados.
        await cur.execute("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
    return raw


async def _crear_conexion():
    """Crea una conexión MySQL/MariaDB con la sesión configurada."""
    return _CompatConnection(await _crear_conexion_raw())


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


# ═══════════════════════════════════════════════════════════════════════════
# Pool de conexiones (V6): sustituye a la conexión única persistente
# ═══════════════════════════════════════════════════════════════════════════

async def _pool_checkout():
    """Obtiene una conexión cruda del pool (creando el pool si hace falta).

    Las conexiones muertas (timeout del servidor, caídas de red) se descartan
    y se sustituyen automáticamente por conexiones nuevas.
    """
    global _pool_q
    # Drenar devoluciones diferidas (compat con release() síncrono del guard).
    while _devoluciones_pendientes:
        await _pool_devolver(_devoluciones_pendientes.pop(0))
    while True:
        if _pool_q is None:
            async with _pool_crear_lock:
                if _pool_q is None:
                    _pool_q = asyncio.Queue()
                    # Precalentar el mínimo de conexiones calientes.
                    for _ in range(_POOL_MIN):
                        raw = await _crear_conexion_raw()
                        _pool_live.add(raw)
                        _pool_q.put_nowait(raw)
        raw = None
        try:
            raw = _pool_q.get_nowait()
        except asyncio.QueueEmpty:
            async with _pool_crear_lock:
                if len(_pool_live) < _POOL_MAX:
                    raw = await _crear_conexion_raw()
                    _pool_live.add(raw)
            if raw is None:
                # Pool saturado: esperar una liberación (con timeout para
                # revalidar por si hubo descartes de conexiones muertas).
                try:
                    raw = await asyncio.wait_for(_pool_q.get(), 10.0)
                except asyncio.TimeoutError:
                    continue
        if raw is None:
            continue
        if raw.closed:
            # Conexión muerta: descartarla; el bucle creará su sustituta.
            _pool_live.discard(raw)
            try:
                await raw.ensure_closed()
            except Exception:
                pass
            continue
        # Ping solo a las conexiones que llevan rato ociosas (evita zombies
        # tras el wait_timeout del servidor) sin penalizar las calientes.
        if time.time() - getattr(raw, "_ts_devuelta", 0.0) > 30:
            try:
                await raw.ping(reconnect=False)
            except Exception:
                _pool_live.discard(raw)
                try:
                    await raw.ensure_closed()
                except Exception:
                    pass
                continue
        return raw


async def _pool_devolver(raw, descartar=False):
    """Devuelve una conexión al pool tras hacer rollback de lo no confirmado."""
    if raw is None:
        return
    if descartar or raw.closed:
        try:
            await raw.ensure_closed()
        except Exception:
            pass
    else:
        try:
            await raw.rollback()
            raw._ts_devuelta = time.time()
        except Exception:
            try:
                await raw.ensure_closed()
            except Exception:
                pass
    # Se devuelve siempre (aunque esté cerrada) para despertar a quien espere
    # una conexión; el siguiente checkout la descarta y crea su sustituta.
    if _pool_q is not None:
        _pool_q.put_nowait(raw)


@asynccontextmanager
async def _conexion():
    """Reserva una conexión del pool para un bloque de operaciones.

    Dentro de `async with _db_lock` reutiliza la conexión vinculada al bloque
    (todas las operaciones comparten la misma transacción). Fuera de un bloque,
    toma una conexión propia del pool y la devuelve al terminar, haciendo
    rollback de lo que no se haya confirmado explícitamente.
    """
    vinculada = _db_lock._conn_de_tarea_actual()
    if vinculada is not None:
        yield vinculada
        return
    raw = await _pool_checkout()
    conn = _CompatConnection(raw)
    descartar = False
    try:
        yield conn
    except Exception as e:
        descartar = _is_transient(e)
        raise
    finally:
        await _pool_devolver(raw, descartar=descartar)


class _TxGuard:
    """`async with _db_lock:` que serializa transacciones multi-sentencia.

    Mantiene la semántica del asyncio.Lock original (un solo bloque activo a la
    vez, no reentrante) y además vincula una conexión del pool al bloque: todas
    las operaciones hechas durante el bloque usan esa misma conexión y, por
    tanto, la misma transacción. Al salir se hace rollback de lo no confirmado
    y la conexión vuelve al pool.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner = None
        self._conn = None

    # ── API de asyncio.Lock (compatibilidad) ──
    def locked(self):
        return self._lock.locked()

    async def acquire(self):
        await self._lock.acquire()
        try:
            conn = _CompatConnection(await _pool_checkout())
        except BaseException:
            self._lock.release()
            raise
        self._owner = asyncio.current_task()
        self._conn = conn
        return True

    def release(self):
        conn, self._conn = self._conn, None
        self._owner = None
        self._lock.release()
        if conn is not None:
            # release() es síncrono: la devolución al pool se hace en segundo plano.
            try:
                asyncio.get_running_loop().create_task(_pool_devolver(conn._raw))
            except Exception:
                _devoluciones_pendientes.append(conn._raw)

    # ── Uso normal ──
    async def __aenter__(self):
        await self.acquire()
        return None

    async def __aexit__(self, exc_type, exc, tb):
        conn, self._conn = self._conn, None
        self._owner = None
        try:
            if conn is not None:
                await _pool_devolver(
                    conn._raw,
                    descartar=exc_type is not None and _is_transient(exc),
                )
        finally:
            self._lock.release()
        return False

    def _conn_de_tarea_actual(self):
        """Conexión vinculada al bloque si la tarea actual lo está ejecutando."""
        if self._owner is not None and self._owner is asyncio.current_task():
            return self._conn
        return None


_db_lock = _TxGuard()          # serializa transacciones multi-sentencia


_RE_LAST_INSERT_ID = re.compile(
    r"^\s*SELECT\s+LAST_INSERT_ID\s*\(\s*\)\s*;?\s*$", re.I)


class _DbHandle:
    """Manejador con la interfaz de la conexión histórica, respaldado por el pool.

    • Dentro de `async with _db_lock` usa la conexión vinculada al bloque: los
      INSERT + SELECT LAST_INSERT_ID() siguen siendo atómicos.
    • Fuera de un bloque, cada operación usa una conexión del pool y las
      escrituras de una sentencia se confirman automáticamente.
    """

    row_factory = Row

    def __init__(self):
        self._lastrowid = None

    def _vinculada(self):
        return _db_lock._conn_de_tarea_actual()

    async def execute(self, sql, params=()):
        conn = self._vinculada()
        if conn is not None:
            cur = await conn.execute(sql, params)
            if cur.lastrowid:
                self._lastrowid = cur.lastrowid
            return cur
        # Compat: SELECT LAST_INSERT_ID() sin transacción abierta devuelve el
        # último ID observado por este manejador.
        if _RE_LAST_INSERT_ID.match(sql or ""):
            return _CompatCursor(("LAST_INSERT_ID()",),
                                 [(self._lastrowid or 0,)])
        async with _conexion() as db:
            cur = await db.execute(sql, params)
            if cur.lastrowid:
                self._lastrowid = cur.lastrowid
            if not cur._cols:
                await db.commit()   # escritura de una sentencia
            return cur

    async def executemany(self, sql, seq):
        conn = self._vinculada()
        if conn is not None:
            cur = await conn.executemany(sql, seq)
            if cur.lastrowid:
                self._lastrowid = cur.lastrowid
            return cur
        async with _conexion() as db:
            cur = await db.executemany(sql, seq)
            if cur.lastrowid:
                self._lastrowid = cur.lastrowid
            await db.commit()
            return cur

    async def commit(self):
        # Fuera de un bloque cada operación ya se confirma por sí misma.
        conn = self._vinculada()
        if conn is not None:
            await conn.commit()

    async def rollback(self):
        conn = self._vinculada()
        if conn is not None:
            await conn.rollback()

    async def close(self):
        # La conexión vuelve al pool al terminar cada operación / bloque.
        return None


_db_handle = _DbHandle()


async def _get_db():
    """Obtiene el manejador de base de datos (misma interfaz que antes).

    Desde la V6 no existe una única conexión persistente: las operaciones usan
    un pool de conexiones (ver `_DbHandle` y `_conexion`).
    """
    return _db_handle


async def close_db():
    """Cierra el pool de conexiones."""
    global _pool_q
    async with _pool_crear_lock:
        pendientes = list(_devoluciones_pendientes)
        _devoluciones_pendientes.clear()
        vivas = list(_pool_live)
        _pool_live.clear()
        q, _pool_q = _pool_q, None
    for raw in vivas + pendientes:
        try:
            await raw.ensure_closed()
        except Exception:
            pass
    if q is not None:
        while not q.empty():
            q.get_nowait()


# Códigos / mensajes transitorios que se reintentan.
_TRANSIENT_ERRNOS = {1205, 1213, 2002, 2003, 2006, 2013, 2014, 2055}
_TRANSIENT_MSGS = (
    "database is locked", "deadlock", "lock wait", "lost connection",
    "connection refused", "server has gone away", "broken pipe",
    "can't connect", "connection reset", "connection aborted",
)


def _is_transient(e: Exception) -> bool:
    if isinstance(e, (pymysql.err.OperationalError, pymysql.err.InternalError)):
        if e.args and isinstance(e.args[0], int) and e.args[0] in _TRANSIENT_ERRNOS:
            return True
    msg = str(e).lower()
    return any(m in msg for m in _TRANSIENT_MSGS)


def _is_missing_index(e: Exception) -> bool:
    msg = str(e).lower()
    return ("duplicate key name" in msg or "already exists" in msg
            or (e.args and isinstance(e.args[0], int) and e.args[0] == 1061))


async def _execute_with_retry(sql, params=(), max_retries=10, seq=None):
    """Ejecuta SQL (una sentencia) con retry automático ante fallos transitorios.

    V6: usa una conexión del pool por intento y NO toma `_db_lock`: una sentencia
    individual ya es atómica, así que las escrituras de una sentencia y las
    lecturas ya no se serializan con el resto del bot. Las transacciones
    multi-sentencia siguen usando `async with _db_lock`.
    Pasando `seq=[...]` ejecuta `executemany` en lugar de `execute`.
    """
    for attempt in range(max_retries):
        try:
            async with _conexion() as db:
                if seq is not None:
                    await db.executemany(sql, seq)
                else:
                    await db.execute(sql, params)
                await db.commit()
            return True
        except Exception as e:
            if _is_transient(e) and attempt < max_retries - 1:
                wait = 0.5 * (2 ** attempt)  # 0.5, 1.0, 2.0, 4.0, 8.0s
                print(f"  ⚠️ DB transitorio (intento {attempt+1}/{max_retries}), esperando {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"  ❌ DB error definitivo: {e}")
                raise
    return False


async def init_db():
    """Inicializa las tablas de la base de datos (MySQL / MariaDB)."""
    db = await _get_db()
    async with _db_lock:
        # Tabla 1M
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_1m (
                symbol VARCHAR(32),
                timestamp BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Tabla 15M
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_15m (
                symbol VARCHAR(32),
                timestamp BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Tabla 4H
        await db.execute("""
            CREATE TABLE IF NOT EXISTS velas_4h (
                symbol VARCHAR(32),
                timestamp BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        # Alertas
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(32),
                timestamp VARCHAR(40),
                tipo VARCHAR(32),
                direccion VARCHAR(16),
                score INT,
                mensaje TEXT,
                params_json TEXT,
                precio DOUBLE
            )
        """)
        # Precios vivo
        await db.execute("""
            CREATE TABLE IF NOT EXISTS precios_vivo (
                symbol VARCHAR(32) PRIMARY KEY,
                precio DOUBLE,
                actualizado VARCHAR(40)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 4.5: TABLAS DE AUDITORÍA EXTERNA
        # ═══════════════════════════════════════════════════════════════════════════════

        # Eventos del bot para auditoría (FIRE, RECHAZADO, CAMBIO_ESTADO, etc.)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_eventos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                fecha VARCHAR(16),
                symbol VARCHAR(32),
                timestamp_utc VARCHAR(40),
                timestamp_local VARCHAR(40),
                tipo VARCHAR(32),
                direccion VARCHAR(16),
                precio DOUBLE,
                contexto_json TEXT,
                grid_params_json TEXT,
                score INT,
                rechazos_json TEXT,
                estado_maquina VARCHAR(32)
            )
        """)

        # Muestras de precio post-disparo para seguimiento
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_muestras_post (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                evento_id BIGINT,
                symbol VARCHAR(32),
                timestamp_utc VARCHAR(40),
                timestamp_local VARCHAR(40),
                precio DOUBLE,
                minutos_desde_disparo INT,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # Eventos significativos post-disparo (picos, valles, entradas/salidas de grid)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_eventos_post (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                evento_id BIGINT,
                symbol VARCHAR(32),
                timestamp_utc VARCHAR(40),
                timestamp_local VARCHAR(40),
                tipo_evento VARCHAR(48),
                precio DOUBLE,
                distancia_desde_entrada_pct DOUBLE,
                grid_rentable_aqui INT,
                nota TEXT,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # Velas 15m posteriores al disparo
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_velas_post (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                evento_id BIGINT,
                symbol VARCHAR(32),
                timestamp_utc VARCHAR(40),
                timestamp_local VARCHAR(40),
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                minutos_desde_disparo INT,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5.1: TABLA PARA PERSISTENCIA DE DISPAROS ACTIVOS (sobrevive reinicios)
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_disparos_activos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                evento_id BIGINT NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                timestamp_utc VARCHAR(40) NOT NULL,
                precio_entrada DOUBLE NOT NULL,
                direccion VARCHAR(16) NOT NULL,
                grid_params_json TEXT,
                maximo_visto DOUBLE,
                minimo_visto DOUBLE,
                primera_vez_en_grid_json TEXT,
                primera_vez_fuera_rango_json TEXT,
                muestras_guardadas INT DEFAULT 0,
                horas_seguimiento INT,
                FOREIGN KEY (evento_id) REFERENCES auditoria_eventos(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # V5.7: TABLA PARA SEGUIMIENTOS VIRTUALES POST NEAR-MISS
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS near_miss_seguimientos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(32) NOT NULL,
                timestamp_inicio DOUBLE NOT NULL,
                timestamp_fin DOUBLE,
                precio_inicio DOUBLE NOT NULL,
                direccion_nm VARCHAR(16) NOT NULL,
                score INT NOT NULL,
                umbral INT NOT NULL,
                filtros_rechazo TEXT,
                muestras_json TEXT,
                precio_max DOUBLE,
                precio_min DOUBLE,
                precio_fin DOUBLE,
                movimiento_pct DOUBLE,
                direccion_real VARCHAR(16),
                acerto_bot INT,
                hubiera_sido_rentable INT,
                notas TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(32) NOT NULL,
                timestamp_inicio DOUBLE NOT NULL,
                timestamp_fin DOUBLE,
                estado VARCHAR(24) NOT NULL DEFAULT 'ACTIVO',
                direccion VARCHAR(16),
                precio_entrada DOUBLE,
                grid_params_json TEXT,
                evento_auditoria_id BIGINT,
                FOREIGN KEY (evento_auditoria_id) REFERENCES auditoria_eventos(id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_simulaciones (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_id BIGINT NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                timestamp_inicio DOUBLE NOT NULL,
                timestamp_fin DOUBLE,
                precio_inicio DOUBLE NOT NULL,
                precio_fin DOUBLE,
                pnl_bruto DOUBLE DEFAULT 0.0,
                pnl_neto DOUBLE DEFAULT 0.0,
                fees_totales DOUBLE DEFAULT 0.0,
                slippage_total DOUBLE DEFAULT 0.0,
                trades_completados INT DEFAULT 0,
                trades_kill_switch INT DEFAULT 0,
                posiciones_abiertas_json TEXT,
                max_posiciones_simultaneas INT DEFAULT 0,
                posiciones_atrapadas_json TEXT,
                estado VARCHAR(24) NOT NULL DEFAULT 'SIMULANDO',
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
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(32) NOT NULL,
                direction VARCHAR(16) NOT NULL,
                estado VARCHAR(24) NOT NULL DEFAULT 'ACTIVO',
                trading_mode VARCHAR(24) NOT NULL,
                capital_asignado DOUBLE NOT NULL,
                apalancamiento_usado INT NOT NULL,
                precio_entrada DOUBLE NOT NULL,
                grid_params_json TEXT,
                pnl_real DOUBLE DEFAULT 0,
                fees_real DOUBLE DEFAULT 0,
                razon_cierre VARCHAR(255),
                timestamp_inicio BIGINT,
                engine_version INT DEFAULT 1,
                order_id_counter INT DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                closed_at DATETIME NULL DEFAULT NULL
            )
        """)

        # Órdenes individuales en Binance (para recuperación post-crash)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_ejecucion_ordenes (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                binance_order_id VARCHAR(64),
                client_order_id VARCHAR(64) NOT NULL UNIQUE,
                symbol VARCHAR(32) NOT NULL,
                side VARCHAR(8) NOT NULL,
                tipo_orden VARCHAR(24) NOT NULL,
                price DOUBLE,
                quantity DOUBLE,
                quantity_filled DOUBLE DEFAULT 0,
                status VARCHAR(24) DEFAULT 'NEW',
                FOREIGN KEY (grid_ejecucion_id) REFERENCES grid_ejecuciones(id)
            )
        """)

        # Fills reales (parciales o completos)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_ejecucion_fills (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                orden_id BIGINT NOT NULL,
                binance_trade_id VARCHAR(64),
                price DOUBLE,
                qty DOUBLE,
                commission DOUBLE,
                commission_asset VARCHAR(16),
                realized_pnl DOUBLE,
                timestamp VARCHAR(40),
                FOREIGN KEY (orden_id) REFERENCES grid_ejecucion_ordenes(id)
            )
        """)

        # ═══════════════════════════════════════════════════════════════════════════════
        # CR16: TABLA PARA TRACKING PROACTIVO DE FILLS
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fills_tracking (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                binance_trade_id VARCHAR(64) NOT NULL UNIQUE,
                binance_order_id VARCHAR(64) NOT NULL,
                side VARCHAR(8) NOT NULL,
                price DOUBLE NOT NULL,
                qty DOUBLE NOT NULL,
                commission DOUBLE NOT NULL,
                commission_asset VARCHAR(16),
                realized_pnl DOUBLE,
                timestamp_ms BIGINT NOT NULL,
                procesado INT DEFAULT 0,
                posicion_id VARCHAR(64),
                fecha_procesamiento DATETIME NULL DEFAULT NULL,
                FOREIGN KEY (grid_ejecucion_id) REFERENCES grid_ejecuciones(id)
            )
        """)

        # Índices para ejecución real
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_grid ON fills_tracking(grid_ejecucion_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_order ON fills_tracking(binance_order_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_fills_procesado ON fills_tracking(procesado)")

        # ═══════════════════════════════════════════════════════════════════════════════
        # V8: TABLAS DEL MOTOR DE NIVELES
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_niveles (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                level_index INT NOT NULL,
                price DOUBLE NOT NULL,
                side VARCHAR(8) NOT NULL,
                position_side VARCHAR(16) NOT NULL DEFAULT 'LONG',
                quantity DOUBLE NOT NULL,
                state VARCHAR(24) NOT NULL DEFAULT 'PENDING',
                is_gap INT DEFAULT 0,
                order_id VARCHAR(64),
                binance_order_id VARCHAR(64),
                filled_qty DOUBLE DEFAULT 0,
                last_placed_at_ms BIGINT,
                version INT DEFAULT 1,
                roundtrips INT DEFAULT 0,
                updated_at BIGINT,
                UNIQUE(grid_ejecucion_id, level_index)
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_niveles_grid ON grid_niveles(grid_ejecucion_id)")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS grid_engine_snapshots (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                level_map_json TEXT NOT NULL,
                coverage_report_json TEXT,
                created_at BIGINT DEFAULT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_grid ON grid_engine_snapshots(grid_ejecucion_id, timestamp_ms)")

        # ═══════════════════════════════════════════════════════════════════════════════
        # CR2: TABLA PARA EVENTOS DE PnL (cada trade genera un evento)
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pnl_eventos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                grid_ejecucion_id BIGINT NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                tipo_evento VARCHAR(32) NOT NULL,
                side VARCHAR(8) NOT NULL,
                binance_trade_id VARCHAR(64) NOT NULL UNIQUE,
                binance_order_id VARCHAR(64) NOT NULL,
                price DOUBLE NOT NULL,
                qty DOUBLE NOT NULL,
                commission DOUBLE NOT NULL,
                commission_asset VARCHAR(16),
                realized_pnl DOUBLE NOT NULL,
                notional DOUBLE NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                timestamp_local VARCHAR(40),
                procesado INT DEFAULT 1,
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

        # ═══════════════════════════════════════════════════════════════════════════════
        # PR-B: TABLA DE INCOME DE BINANCE (funding / realized / commission)
        # Fuente: endpoint futures_income. pnl_eventos NO cubre funding — esta tabla sí.
        # UNIQUE (tran_id, income_type, symbol): Binance puede reusar tranId entre
        # símbolos, y un mismo trade genera REALIZED_PNL y COMMISSION con el mismo
        # tranId. La tripleta es la clave natural (idempotencia con INSERT IGNORE).
        # ═══════════════════════════════════════════════════════════════════════════════
        await db.execute("""
            CREATE TABLE IF NOT EXISTS income_eventos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(32) NOT NULL,
                income_type VARCHAR(24) NOT NULL,
                income DOUBLE NOT NULL,
                asset VARCHAR(16) NOT NULL,
                tran_id BIGINT NOT NULL,
                trade_id VARCHAR(64) NULL,
                timestamp_ms BIGINT NOT NULL,
                info VARCHAR(255) NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_income_tran_type (tran_id, income_type, symbol)
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_income_ts ON income_eventos(timestamp_ms)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_income_symbol_ts ON income_eventos(symbol, timestamp_ms)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_income_type_ts ON income_eventos(income_type, timestamp_ms)")

        await db.commit()

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.4: MIGRACIÓN SILENCIOSA DE COLUMNAS FALTANTES (compatibilidad DB antigua)
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _migrar_columnas_faltantes():
        """Añade columnas faltantes en tablas existentes (idempotente)."""
        columnas_necesarias = {
            'grid_estados': [
                ('evento_auditoria_id', 'BIGINT'),
            ],
            'near_miss_seguimientos': [
                ('filtros_rechazo', 'TEXT'),
                ('muestras_json', 'TEXT'),
                ('precio_max', 'DOUBLE'),
                ('precio_min', 'DOUBLE'),
            ],
            'grid_ejecuciones': [
                ('timestamp_inicio', 'BIGINT'),
                ('engine_version', 'INT DEFAULT 1'),      # V8: versión del motor
                ('order_id_counter', 'INT DEFAULT 0'),       # V8: contador de IDs
            ],
            'grid_niveles': [
                ('roundtrips', 'INT DEFAULT 0'),          # V8 FASE 4: ciclos por nivel
            ],
        }

        for tabla, columnas in columnas_necesarias.items():
            try:
                cursor = await db.execute(f"SHOW COLUMNS FROM {tabla}")
                rows = await cursor.fetchall()
                columnas_existentes = {r[0] for r in rows}  # r[0] = Field

                for col, tipo in columnas:
                    if col not in columnas_existentes:
                        await db.execute(f"ALTER TABLE {tabla} ADD COLUMN {col} {tipo}")
                        print(f"  [MIGRACION] Columna '{col}' ({tipo}) añadida a '{tabla}'")
                await db.commit()
            except Exception as e:
                print(f"  ⚠️ [MIGRACION] Error verificando {tabla}: {e}")

    await _migrar_columnas_faltantes()

    print("🗄️ Base de datos MySQL/MariaDB inicializada + Auditoría + Disparos + Near-miss + V5.9.2 Grid Neutral + F1.4 Migración")

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
        REPLACE INTO {tabla}
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

    try:
        await _execute_with_retry(
            f"""
            REPLACE INTO {tabla}
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            seq=[(symbol, v['timestamp'], v['open'], v['high'], v['low'], v['close'], v['volume'])
                 for v in velas]
        )
        print(f"  💾 [DB] Batch insert: {len(velas)} velas {tf} para {symbol}")
    except Exception as e:
        print(f"  ⚠️ [DB] Error en batch insert {tf} {symbol}: {e}")

async def cargar_velas_historicas(symbol: str, tf: str, limit: int):
    tabla = _TABLAS_VELAS.get(tf)
    if not tabla:
        print(f"  ⚠️ [DB] Timeframe inválido '{tf}' para {symbol}, no se pueden cargar velas")
        return []
    async with _conexion() as db:
        db.row_factory = Row
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
        REPLACE INTO precios_vivo (symbol, precio, actualizado)
        VALUES (?, ?, ?)
    """, (symbol, precio, datetime.utcnow().isoformat()))

# ═══════════════════════════════════════════════════════════════════════════════
# F1.1: ACTUALIZACIÓN BATCH DE PRECIOS VIVO (reduce I/O drásticamente)
# ═══════════════════════════════════════════════════════════════════════════════
async def actualizar_precios_vivo_batch(precios: dict):
    """Actualiza múltiples precios en una sola transacción (batch)."""
    if not precios:
        return
    ts = datetime.utcnow().isoformat()
    try:
        await _execute_with_retry(
            "REPLACE INTO precios_vivo (symbol, precio, actualizado) VALUES (?, ?, ?)",
            seq=[(symbol, precio, ts) for symbol, precio in precios.items()],
        )
    except Exception as e:
        print(f"  ⚠️ Error en batch precios vivo: {e}")

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
                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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
        REPLACE INTO auditoria_disparos_activos
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
    async with _conexion() as db:
        db.row_factory = Row
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
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_eventos
            WHERE fecha = ?
            ORDER BY timestamp_utc ASC
        """, (fecha_local,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_muestras_post_evento(evento_id: int):
    """Carga las muestras post-disparo de un evento específico."""
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_muestras_post
            WHERE evento_id = ?
            ORDER BY minutos_desde_disparo ASC
        """, (evento_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_eventos_post_evento(evento_id: int):
    """Carga los eventos significativos post-disparo de un evento específico."""
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM auditoria_eventos_post
            WHERE evento_id = ?
            ORDER BY timestamp_utc ASC
        """, (evento_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def cargar_velas_post_evento(evento_id: int):
    """Carga las velas 15m posteriores de un evento específico."""
    async with _conexion() as db:
        db.row_factory = Row
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

                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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
    async with _conexion() as db:
        db.row_factory = Row
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
    async with _conexion() as db:
        db.row_factory = Row
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

                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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

                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM grid_estados
            WHERE symbol = ? AND estado = 'ACTIVO'
            ORDER BY timestamp_inicio DESC LIMIT 1
        """, (symbol,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def cargar_simulacion_activa(grid_id):
    """Carga la simulación activa para un grid_id."""
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM grid_simulaciones
            WHERE grid_id = ? AND estado = 'SIMULANDO'
            ORDER BY timestamp_inicio DESC LIMIT 1
        """, (grid_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def cargar_grids_huerfanos(timeout_seg):
    """Carga grids ACTIVOS sin simulación activa durante más de timeout_seg."""
    ahora = int(datetime.now(pytz.UTC).timestamp())
    async with _conexion() as db:
        db.row_factory = Row
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

                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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

                cursor2 = await db.execute("SELECT LAST_INSERT_ID()")
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
    async with _conexion() as db:
        db.row_factory = Row
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

                cursor = await db.execute("SELECT LAST_INSERT_ID()")
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
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT * FROM grid_ejecuciones WHERE estado = 'ACTIVO' ORDER BY created_at ASC
        """)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def cargar_ordenes_por_grid(grid_ejecucion_id):
    """Carga órdenes de un grid de ejecución."""
    async with _conexion() as db:
        db.row_factory = Row
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
        INSERT IGNORE INTO fills_tracking
        (grid_ejecucion_id, symbol, binance_trade_id, binance_order_id,
         side, price, qty, commission, commission_asset, realized_pnl,
         timestamp_ms, procesado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (grid_ejecucion_id, symbol, binance_trade_id, binance_order_id,
          side, price, qty, commission, commission_asset, realized_pnl,
          timestamp_ms))


async def cargar_fills_sin_procesar(grid_ejecucion_id):
    """Carga fills no procesados de un grid."""
    async with _conexion() as db:
        db.row_factory = Row
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
    async with _conexion() as db:
        cursor = await db.execute("""
            SELECT MAX(timestamp_ms) FROM fills_tracking WHERE symbol = ?
        """, (symbol,))
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 0

# ═══════════════════════════════════════════════════════════════════════════════
# PR-B: FUNCIONES DE INCOME DE BINANCE (funding / realized / commission)
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_income_batch(rows: list) -> int:
    """Inserta eventos de income de Binance de forma idempotente (INSERT IGNORE).

    rows: dicts con symbol, income_type, income, asset, tran_id,
    trade_id, timestamp_ms, info. Devuelve el nº de filas intentadas.
    """
    if not rows:
        return 0
    cols = ('symbol', 'income_type', 'income', 'asset', 'tran_id',
            'trade_id', 'timestamp_ms', 'info')
    filas = [tuple(r.get(c) for c in cols) for r in rows]
    ph = ", ".join(["?"] * len(cols))
    sql = f"INSERT IGNORE INTO income_eventos ({', '.join(cols)}) VALUES ({ph})"
    try:
        await _execute_with_retry(sql, seq=filas)
        return len(filas)
    except Exception as e:
        print(f"  ⚠️ Error guardando income_eventos: {e}")
        return 0


async def cargar_watermark_income(symbol: str, income_type: str) -> int:
    """Último timestamp_ms persistido para (symbol, income_type). 0 si no hay datos."""
    async with _conexion() as db:
        cursor = await db.execute(
            "SELECT COALESCE(MAX(timestamp_ms), 0) FROM income_eventos "
            "WHERE symbol = ? AND income_type = ?",
            (symbol, income_type)
        )
        row = await cursor.fetchone()
        return int(row[0] or 0) if row else 0


async def calcular_resumen_funding_dia(fecha_local: str) -> dict:
    """Total de FUNDING_FEE del día (UTC) agrupado por symbol. PR-B hueco #1."""
    tz = get_tz()
    fecha_inicio = tz.localize(datetime.strptime(fecha_local, "%Y-%m-%d"))
    fecha_fin = fecha_inicio + timedelta(days=1)
    ts_inicio = int(fecha_inicio.astimezone(pytz.UTC).timestamp() * 1000)
    ts_fin = int(fecha_fin.astimezone(pytz.UTC).timestamp() * 1000)

    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute("""
            SELECT symbol, COALESCE(SUM(income), 0) AS funding
            FROM income_eventos
            WHERE income_type = 'FUNDING_FEE'
              AND timestamp_ms >= ? AND timestamp_ms < ?
            GROUP BY symbol
        """, (ts_inicio, ts_fin))
        rows = await cursor.fetchall()
    return {r['symbol']: float(r['funding']) for r in rows}

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
        INSERT IGNORE INTO pnl_eventos
        (grid_ejecucion_id, symbol, tipo_evento, side, binance_trade_id,
         binance_order_id, price, qty, commission, commission_asset,
         realized_pnl, notional, timestamp_ms, timestamp_local)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (grid_ejecucion_id, symbol, tipo_evento, side, binance_trade_id,
          binance_order_id, price, qty, commission, commission_asset,
          realized_pnl, notional, timestamp_ms, timestamp_local))


async def calcular_pnl_acumulado(grid_ejecucion_id: int) -> dict:
    """Calcula PnL acumulado desde la base de datos (fuente de verdad)."""
    async with _conexion() as db:
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

        # FIX: SUM() sobre conjunto vacío devuelve NULL en MySQL (grid sin trades).
        # Los COALESCE del SQL no cubren los CASE. Normalizar aquí.
        return {
            'pnl_real': float(row[0] or 0),
            'fees_real': float(row[1] or 0),
            'total_trades': int(row[2] or 0),
            'trades_ganadores': int(row[3] or 0),
            'trades_perdedores': int(row[4] or 0)
        }


async def obtener_pnl_por_tipo(grid_ejecucion_id: int) -> dict:
    """Desglose de PnL por tipo de evento."""
    async with _conexion() as db:
        cursor = await db.execute("""
            SELECT tipo_evento, COALESCE(SUM(realized_pnl), 0), COUNT(*)
            FROM pnl_eventos
            WHERE grid_ejecucion_id = ?
            GROUP BY tipo_evento
        """, (grid_ejecucion_id,))
        rows = await cursor.fetchall()
        return {row[0]: {'pnl': float(row[1]), 'count': int(row[2])} for row in rows}


# ════════════════════════════════════════════════════════════════════════════════
# FIX F (Bloque 4): RESUMEN FINANCIERO DESDE pnl_eventos (fuente de verdad)
# ════════════════════════════════════════════════════════════════════════════════

async def calcular_resumen_financiero_dia(fecha_local: str, symbol: str = None) -> dict:
    """
    Calcula el resumen financiero del día desde pnl_eventos (fuente de verdad).

    Fixes aplicados (cuidado con los bugs.txt):
      - Bug #1: Abortos contados desde UNA sola fuente (pnl_eventos),
        no duplicados con grid_ejecuciones.
      - Bug #2: round_trips EXCLUYE cierres F45 (client_order_id LIKE '%_PC_%').
      - Bug #3: Filtro opcional por symbol para resumen por moneda.

    Returns:
        dict con pnl_neto_usdt, fees_usdt, pnl_ex_abortos, costo_abortos_usdt,
        round_trips, abortos, ratio_win_por_aborto, profit_factor,
        fees_sobre_ganancias_pct.
    """
    try:
        async with _conexion() as db:
            db.row_factory = Row

            # Rango de timestamps UTC para la fecha local
            tz = get_tz()
            fecha_inicio = datetime.strptime(fecha_local, "%Y-%m-%d")
            fecha_inicio = tz.localize(fecha_inicio)
            fecha_fin = fecha_inicio + timedelta(days=1)
            ts_inicio = int(fecha_inicio.astimezone(pytz.UTC).timestamp() * 1000)
            ts_fin = int(fecha_fin.astimezone(pytz.UTC).timestamp() * 1000)

            # Cláusula WHERE base + filtro opcional por símbolo
            where_clause = "WHERE timestamp_ms >= ? AND timestamp_ms < ?"
            params = [ts_inicio, ts_fin]
            if symbol:
                where_clause += " AND symbol = ?"
                params.append(symbol)
            params_t = tuple(params)

            # PnL y fees reales (todas las operaciones del día)
            cursor = await db.execute(
                f"SELECT COALESCE(SUM(realized_pnl),0), COALESCE(SUM(commission),0) "
                f"FROM pnl_eventos {where_clause}",
                params_t)
            row = await cursor.fetchone()
            pnl_total = float(row[0] or 0)
            fees_total = float(row[1] or 0)

            # Costo específico de abortos (tipo_evento='ABORTO' o 'KILL_SWITCH')
            cursor = await db.execute(
                f"SELECT COALESCE(SUM(realized_pnl),0) FROM pnl_eventos "
                f"{where_clause} AND tipo_evento IN ('ABORTO','KILL_SWITCH')",
                params_t)
            pnl_abortos = float((await cursor.fetchone())[0] or 0)

            # Round trips completados (excluir F45 - cierres de par en grid vivo)
            cursor = await db.execute(
                f"SELECT COUNT(*) FROM pnl_eventos "
                f"{where_clause} AND tipo_evento = 'FILL_SALIDA' "
                f"AND binance_order_id NOT IN ("
                f"    SELECT binance_order_id FROM grid_ejecucion_ordenes "
                f"    WHERE client_order_id LIKE '%_PC_%'"
                f")",
                params_t)
            round_trips = int((await cursor.fetchone())[0] or 0)

            # Abortos: UNA sola fuente (pnl_eventos) — Fix Bug #1
            cursor = await db.execute(
                f"SELECT COUNT(*) FROM pnl_eventos "
                f"{where_clause} AND tipo_evento IN ('ABORTO','KILL_SWITCH')",
                params_t)
            abortos = int((await cursor.fetchone())[0] or 0)

        return {
            'pnl_neto_usdt': pnl_total,
            'fees_usdt': fees_total,
            'pnl_ex_abortos': pnl_total - pnl_abortos,
            'costo_abortos_usdt': pnl_abortos,
            'round_trips': round_trips,
            'abortos': abortos,
            'ratio_win_por_aborto': round_trips / max(1, abortos),
            'profit_factor': pnl_total / fees_total if fees_total > 0 else None,
            'fees_sobre_ganancias_pct': fees_total / max(pnl_total, 0.01) * 100,
        }
    except Exception as e_db:
        vacio = {
            'pnl_neto_usdt': 0, 'fees_usdt': 0,
            'pnl_ex_abortos': 0, 'costo_abortos_usdt': 0,
            'round_trips': 0, 'abortos': 0,
            'ratio_win_por_aborto': 0, 'profit_factor': None,
            'fees_sobre_ganancias_pct': 0,
        }
        print(f"  ?? [FIN-DIA] Error calculando resumen financiero "
              f"({fecha_local}, symbol={symbol}): {e_db}")
        print(f"     REPORTE SE ENVIARÁ CON MÉTRICAS EN CERO — revisar conectividad DB")
        return vacio


async def calcular_resumen_financiero_dia_por_symbol(fecha_local: str) -> dict:
    """
    Una sola query con GROUP BY symbol. Retorna {symbol: resumen_dict}.
    Alternativa N+1-friendly a iterar calcular_resumen_financiero_dia(symbol=...).
    """
    try:
        async with _conexion() as db:
            db.row_factory = Row
            tz = get_tz()
            fecha_inicio = tz.localize(datetime.strptime(fecha_local, "%Y-%m-%d"))
            fecha_fin = fecha_inicio + timedelta(days=1)
            ts_inicio = int(fecha_inicio.astimezone(pytz.UTC).timestamp() * 1000)
            ts_fin = int(fecha_fin.astimezone(pytz.UTC).timestamp() * 1000)

            cursor = await db.execute("""
                SELECT symbol,
                       COALESCE(SUM(realized_pnl), 0) as pnl,
                       COALESCE(SUM(commission), 0) as fees,
                       SUM(CASE WHEN tipo_evento = 'FILL_SALIDA'
                                AND binance_order_id NOT IN (
                                    SELECT binance_order_id FROM grid_ejecucion_ordenes
                                    WHERE client_order_id LIKE '%_PC_%')
                                THEN 1 ELSE 0 END) as rt,
                       SUM(CASE WHEN tipo_evento IN ('ABORTO','KILL_SWITCH') THEN 1 ELSE 0 END) as ab,
                       COALESCE(SUM(CASE WHEN tipo_evento IN ('ABORTO','KILL_SWITCH')
                                         THEN realized_pnl ELSE 0 END), 0) as ab_pnl
                FROM pnl_eventos
                WHERE timestamp_ms >= ? AND timestamp_ms < ?
                GROUP BY symbol
            """, (ts_inicio, ts_fin))
            rows = await cursor.fetchall()

        out = {}
        for r in rows:
            pnl = float(r[1] or 0); fees = float(r[2] or 0)
            rt = int(r[3] or 0); ab = int(r[4] or 0); ab_pnl = float(r[5] or 0)
            out[r[0]] = {
                'pnl_neto_usdt': pnl, 'fees_usdt': fees,
                'pnl_ex_abortos': pnl - ab_pnl, 'costo_abortos_usdt': ab_pnl,
                'round_trips': rt, 'abortos': ab,
                'ratio_win_por_aborto': rt / max(1, ab),
                'profit_factor': pnl / fees if fees > 0 else None,
                'fees_sobre_ganancias_pct': fees / max(pnl, 0.01) * 100,
            }
        return out
    except Exception as e_db:
        print(f"  ?? [FIN-DIA] Error calculando resumen financiero por symbol "
              f"({fecha_local}): {e_db}")
        return {}


def resumen_financiero_vacio() -> dict:
    """Retorna un resumen financiero con ceros para símbolos sin actividad."""
    return {
        'pnl_neto_usdt': 0, 'fees_usdt': 0,
        'pnl_ex_abortos': 0, 'costo_abortos_usdt': 0,
        'round_trips': 0, 'abortos': 0,
        'ratio_win_por_aborto': 0, 'profit_factor': None,
        'fees_sobre_ganancias_pct': 0,
    }




# ═══════════════════════════════════════════════════════════════════════════════
# V8: FUNCIONES DEL MOTOR DE NIVELES
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_niveles_grid(grid_id: int, rows: list):
    """Guarda o actualiza niveles de un grid."""
    db = await _get_db()
    async with _db_lock:
        for row in rows:
            await db.execute("""
                REPLACE INTO grid_niveles
                (grid_ejecucion_id, symbol, level_index, price, side, position_side,
                 quantity, state, is_gap, order_id, binance_order_id, filled_qty,
                 last_placed_at_ms, version, roundtrips, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                grid_id, row['symbol'], row['level_index'], row['price'],
                row['side'], row.get('position_side', 'LONG'), row['quantity'],
                row['state'], row['is_gap'], row.get('order_id'),
                row.get('binance_order_id'), row.get('filled_qty', 0),
                row.get('last_placed_at_ms'), row.get('version', 1),
                row.get('roundtrips', 0),
                int(time.time())
            ))
        await db.commit()

async def cargar_niveles_grid(grid_id: int) -> list:
    """Carga niveles de un grid desde DB."""
    async with _conexion() as db:
        db.row_factory = Row
        cursor = await db.execute(
            "SELECT * FROM grid_niveles WHERE grid_ejecucion_id = ? ORDER BY level_index",
            (grid_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def actualizar_nivel_grid(grid_id: int, level_index: int, **campos):
    """Actualiza campos específicos de un nivel."""
    sets = [f"{k} = ?" for k in campos.keys()]
    vals = list(campos.values())
    vals.extend([int(time.time()), grid_id, level_index])

    await _execute_with_retry(
        f"UPDATE grid_niveles SET {', '.join(sets)}, updated_at = ? WHERE grid_ejecucion_id = ? AND level_index = ?",
        tuple(vals)
    )

async def actualizar_grid_engine_version(grid_id: int, version: int):
    """Actualiza la versión del engine de un grid."""
    await _execute_with_retry(
        "UPDATE grid_ejecuciones SET engine_version = ? WHERE id = ?",
        (version, grid_id)
    )

async def actualizar_order_id_counter(grid_id: int, counter: int):
    """Actualiza el contador de order IDs para un grid."""
    await _execute_with_retry(
        "UPDATE grid_ejecuciones SET order_id_counter = ? WHERE id = ?",
        (counter, grid_id)
    )

async def cargar_order_id_counter(grid_id: int) -> int:
    """Carga el contador de order IDs para un grid."""
    async with _conexion() as db:
        cursor = await db.execute(
            "SELECT order_id_counter FROM grid_ejecuciones WHERE id = ?",
            (grid_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

async def guardar_snapshot_engine(grid_id: int, level_map_json: str,
                                   coverage_report_json: str = None):
    """Guarda snapshot del engine para debugging."""
    await _execute_with_retry("""
        INSERT INTO grid_engine_snapshots
        (grid_ejecucion_id, timestamp_ms, level_map_json, coverage_report_json, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (grid_id, int(time.time() * 1000), level_map_json, coverage_report_json, int(time.time())))

async def obtener_tamano_db_mb():
    """Tamaño aproximado de la base de datos (MB) desde information_schema."""
    async with _conexion() as db:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(data_length + index_length), 0) "
            "FROM information_schema.TABLES WHERE table_schema = DATABASE()"
        )
        row = await cursor.fetchone()
        return float(row[0] or 0) / (1024 * 1024)
