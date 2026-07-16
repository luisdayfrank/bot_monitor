import asyncio
import json
import logging
import numpy as np
import os
import re
from datetime import datetime, timedelta
import time
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database_v5 import (
    guardar_alerta, _get_db, now_local, get_tz, calcular_pnl_acumulado, 
    obtener_pnl_por_tipo, cargar_grid_ejecuciones_activos
)
from notifier_v5 import Notifier
from config import CONFIG

REGISTRY_PATH = "coins_registry.json"

# ─── Rate limiting simple en memoria ───
_rate_limit_cache = {}


def _check_rate_limit(key: str, seconds: int = 3) -> bool:
    """Retorna True si permite la acción, False si está rate-limited."""
    now = time.time()
    last = _rate_limit_cache.get(key, 0)
    if now - last < seconds:
        return False
    _rate_limit_cache[key] = now
    return True


def _sanitize_symbol(symbol: str) -> str:
    """Sanitiza símbolo: solo letras mayúsculas y números, máx 20 chars."""
    if not symbol or not isinstance(symbol, str):
        raise HTTPException(status_code=400, detail="Símbolo inválido")
    s = symbol.upper().strip()
    if not re.match(r'^[A-Z0-9]{1,20}$', s):
        raise HTTPException(status_code=400, detail=f"Símbolo '{symbol}' contiene caracteres inválidos")
    return s


def _load_registry():
    """Carga el registro de monedas desde disco."""
    if not os.path.exists(REGISTRY_PATH):
        return {}
    try:
        with open(REGISTRY_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_registry(registry):
    """Guarda el registro de monedas a disco."""
    with open(REGISTRY_PATH, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('crypto_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('server')


class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def safe_json_dumps(obj):
    import json as _json
    return _json.dumps(obj, cls=NumpyJSONEncoder)


def _sanitize_numpy(obj):
    if obj is None:
        return None
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _sanitize_numpy(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_numpy(v) for v in obj]
    return obj


app = FastAPI(title="Crypto Monitor V6.2 — Multi-Timeframe Sniper Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("web/static", exist_ok=True)
app.mount("/ui", StaticFiles(directory="web/static", html=True), name="static")


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self._connection_count = 0

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        self._connection_count += 1
        logger.info(f"[UI] Navegador conectado. Total activas: {len(self.active_connections)} | Histórico: {self._connection_count}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"[UI] Navegador desconectado. Restantes: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                dead.append(connection)
        for d in dead:
            self.disconnect(d)

    async def broadcast_json(self, payload: dict):
        await self.broadcast(safe_json_dumps(payload))


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            msg = await asyncio.wait_for(websocket.receive_text(), timeout=60)
            try:
                data = json.loads(msg)
                if data.get("action") == "ping":
                    await websocket.send_text(safe_json_dumps({"msg_type": "pong", "timestamp": time.time()}))
            except json.JSONDecodeError:
                pass
    except asyncio.TimeoutError:
        logger.info("[UI] WebSocket inactivo 60s, cerrando...")
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@app.get("/api/velas/{symbol}")
async def get_velas(symbol: str, request: Request, tf: str = "15m"):
    symbol = _sanitize_symbol(symbol)
    buffers_map = {
        '1m': getattr(request.app.state, 'buffers_1m', {}),
        '15m': getattr(request.app.state, 'buffers_15m', {}),
        '4h': getattr(request.app.state, 'buffers_4h', {})
    }
    buffers = buffers_map.get(tf, {})

    if symbol not in buffers:
        return {"velas": [], "symbol": symbol, "tf": tf, "count": 0}

    df = buffers[symbol].copy()
    if df.empty:
        return {"velas": [], "symbol": symbol, "tf": tf, "count": 0}

    cols = ['timestamp', 'open', 'high', 'low', 'close']
    for c in cols:
        if c not in df.columns:
            return {"velas": [], "symbol": symbol, "tf": tf, "count": 0, "error": f"Missing column {c}"}

    df = df[cols].copy()
    df['time'] = df['timestamp'] // 1000
    records = df.to_dict('records')
    return {
        "velas": records,
        "symbol": symbol,
        "tf": tf,
        "count": len(records),
        "last_time": records[-1]['time'] if records else None
    }


@app.get("/api/estado/{symbol}")
async def get_estado(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    estados = getattr(request.app.state, 'signal_states', {})
    if symbol in estados:
        s = estados[symbol]
        return _sanitize_numpy({
            "symbol": symbol,
            "estado": s.estado,
            "direccion_filtro": s.direccion_filtro,
            "velas_confirmacion": s.velas_confirmacion,
            "ultimo_score": s.ultimo_score,
            "ultimo_disparo": s.ultimo_disparo_timestamp_15m,
            "filtro_macro_aprobado": s.filtro_macro_aprobado,
            "circuit_breaker_activo": getattr(s, 'circuit_breaker_activo', False),
            "circuit_breaker_hasta": getattr(s, 'circuit_breaker_hasta', 0),
            "moneda_pausada": getattr(s, 'moneda_pausada', False),
            "moneda_pausada_manual": getattr(s, 'moneda_pausada_manual', False),
            "neutral_grid_timestamp": getattr(s, 'neutral_grid_timestamp', 0),
            "moneda_pausada_razon": getattr(s, 'moneda_pausada_razon', None),
            "timestamp": int(time.time() * 1000)
        })
    return {"symbol": symbol, "estado": "UNKNOWN", "timestamp": int(time.time() * 1000)}


@app.get("/api/estados")
async def get_estados(request: Request):
    estados = getattr(request.app.state, 'signal_states', {})
    result = []
    for symbol, s in estados.items():
        result.append(_sanitize_numpy({
            "symbol": symbol,
            "estado": s.estado,
            "direccion_filtro": s.direccion_filtro,
            "ultimo_score": s.ultimo_score,
            "ultimo_disparo": s.ultimo_disparo_timestamp_15m,
            "filtro_macro_aprobado": s.filtro_macro_aprobado,
            "circuit_breaker_activo": getattr(s, 'circuit_breaker_activo', False),
            "moneda_pausada": getattr(s, 'moneda_pausada', False),
            "moneda_pausada_manual": getattr(s, 'moneda_pausada_manual', False),
        }))
    return {"estados": result, "count": len(result), "timestamp": int(time.time() * 1000)}


@app.get("/api/precios")
async def get_precios(request: Request):
    precios = getattr(request.app.state, 'precios_vivo', {})
    return {
        "precios": {k: round(v, 4) for k, v in precios.items()},
        "count": len(precios),
        "timestamp": int(time.time() * 1000)
    }


@app.get("/api/precios/{symbol}")
async def get_precio_symbol(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    precios = getattr(request.app.state, 'precios_vivo', {})
    return {
        "symbol": symbol,
        "precio": round(precios.get(symbol), 4) if symbol in precios else None,
        "timestamp": int(time.time() * 1000)
    }


@app.get("/api/indicadores/{symbol}")
async def get_indicadores(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    i1m = getattr(request.app.state, 'indicadores_1m', {}).get(symbol, {})
    i15m = getattr(request.app.state, 'indicadores_15m', {}).get(symbol, {})
    i4h = getattr(request.app.state, 'indicadores_4h', {}).get(symbol, {})
    precio = getattr(request.app.state, 'precios_vivo', {}).get(symbol)
    estado = getattr(request.app.state, 'signal_states', {}).get(symbol)

    return _sanitize_numpy({
        "symbol": symbol,
        "precio": precio,
        "indicadores_1m": {
            "rsi_7": i1m.get("rsi_7"),
            "ema_300": i1m.get("ema_300"),
            "wick_upper_pct": i1m.get("wick_upper_pct"),
            "wick_lower_pct": i1m.get("wick_lower_pct"),
            "body_direction": i1m.get("body_direction"),
            "close": i1m.get("close"),
            "high": i1m.get("high"),
            "low": i1m.get("low"),
            "atr_1m": i1m.get("atr_1m"),
            "mecha_valida_short": i1m.get("mecha_valida_short"),
            "mecha_valida_long": i1m.get("mecha_valida_long"),
            "ema300_distancia_pct": i1m.get("ema300_distancia_pct"),
            "volume": i1m.get("volume"),
            "volume_sma20": i1m.get("volume_sma20"),
        },
        "indicadores_15m": {
            "rsi": i15m.get("rsi"),
            "adx": i15m.get("adx"),
            "macd_hist": i15m.get("macd_hist"),
            "atr": i15m.get("atr"),
            "ema200_15m": i15m.get("ema200_15m"),
            "volume": i15m.get("volume"),
            "volume_sma20": i15m.get("volume_sma20"),
            "recent_high": i15m.get("recent_high"),
            "recent_low": i15m.get("recent_low"),
        },
        "indicadores_4h": {
            "ema200_4h": i4h.get("ema200_4h"),
            "close": i4h.get("close"),
        },
        "estado": {
            "estado": estado.estado if estado else "UNKNOWN",
            "direccion": estado.direccion_filtro if estado else None,
            "score": estado.ultimo_score if estado else 0,
            "filtro_aprobado": estado.filtro_macro_aprobado if estado else False,
            "velas_confirmacion": estado.velas_confirmacion if estado else 0,
            "circuit_breaker_activo": getattr(estado, 'circuit_breaker_activo', False) if estado else False,
            "moneda_pausada": getattr(estado, 'moneda_pausada', False) if estado else False,
            "moneda_pausada_manual": getattr(estado, 'moneda_pausada_manual', False) if estado else False,
            "moneda_pausada_razon": getattr(estado, 'moneda_pausada_razon', None) if estado else None,
        },
        "timestamp": int(time.time() * 1000)
    })


@app.get("/api/snapshot")
async def get_snapshot(request: Request):
    precios = getattr(request.app.state, 'precios_vivo', {})
    estados = getattr(request.app.state, 'signal_states', {})
    indicadores_1m = getattr(request.app.state, 'indicadores_1m', {})
    indicadores_15m = getattr(request.app.state, 'indicadores_15m', {})
    indicadores_4h = getattr(request.app.state, 'indicadores_4h', {})

    registry = _load_registry()

    result = {}
    for symbol in registry.keys():
        if not registry.get(symbol, {}).get('active', False):
            continue
        st = estados.get(symbol)
        i1m = indicadores_1m.get(symbol, {})
        i15m = indicadores_15m.get(symbol, {})
        i4h = indicadores_4h.get(symbol, {})

        result[symbol] = {
            "precio": precios.get(symbol),
            "estado": {
                "estado": st.estado if st else "SIN_DATOS",
                "direccion_filtro": st.direccion_filtro if st else None,
                "score": st.ultimo_score if st else 0,
                "filtro_aprobado": st.filtro_macro_aprobado if st else False,
                "velas_confirmacion": st.velas_confirmacion if st else 0,
                "circuit_breaker_activo": getattr(st, 'circuit_breaker_activo', False) if st else False,
                "moneda_pausada": getattr(st, 'moneda_pausada', False) if st else False,
                "moneda_pausada_manual": getattr(st, 'moneda_pausada_manual', False) if st else False,
            },
            "indicadores_1m": {
                "rsi_7": i1m.get("rsi_7"),
                "ema_300": i1m.get("ema_300"),
                "wick_upper_pct": i1m.get("wick_upper_pct"),
                "wick_lower_pct": i1m.get("wick_lower_pct"),
                "body_direction": i1m.get("body_direction"),
                "close": i1m.get("close"),
                "high": i1m.get("high"),
                "low": i1m.get("low"),
                "atr_1m": i1m.get("atr_1m"),
                "mecha_valida_short": i1m.get("mecha_valida_short"),
                "mecha_valida_long": i1m.get("mecha_valida_long"),
                "ema300_distancia_pct": i1m.get("ema300_distancia_pct"),
                "volume": i1m.get("volume"),
                "volume_sma20": i1m.get("volume_sma20"),
            },
            "indicadores_15m": {
                "rsi": i15m.get("rsi"),
                "adx": i15m.get("adx"),
                "macd_hist": i15m.get("macd_hist"),
                "atr": i15m.get("atr"),
                "ema200_15m": i15m.get("ema200_15m"),
                "volume": i15m.get("volume"),
                "volume_sma20": i15m.get("volume_sma20"),
                "recent_high": i15m.get("recent_high"),
                "recent_low": i15m.get("recent_low"),
            },
            "indicadores_4h": {
                "ema200_4h": i4h.get("ema200_4h"),
                "close": i4h.get("close"),
            },
        }

    return _sanitize_numpy({
        "snapshot": result,
        "coin_config": {
            symbol: {"active": data.get("active", 0), "category": data.get("category", "unknown")}
            for symbol, data in registry.items()
        },
        "symbols_count": len(result),
        "active_count": sum(1 for d in registry.values() if d.get("active", 0) == 1),
        "timestamp": int(time.time() * 1000)
    })


# -------------------------------------------------------------------------------
# V6.2: ENDPOINT DE ESTADÍSTICAS DESDE LA BASE DE DATOS (CORREGIDO Y ALINEADO)
# -------------------------------------------------------------------------------

@app.get("/api/stats/summary")
async def get_stats_summary(request: Request):
    """Obtiene un resumen de rendimiento usando la base de datos de auditoría.

    V6.2: Métricas separadas para near-misses en curso vs finalizados.
    PnL REAL desde grid_ejecuciones (no simulaciones legacy).
    Agregadas métricas de executor y circuit breaker.
    FIX: ultimo_evento_info ahora se asigna correctamente.
    """
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")
        ts_inicio_dia = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        # ─── 1. Fires hoy ───
        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?",
            (hoy_local,)
        )
        fires_hoy = (await cursor.fetchone())[0]

        # ─── 2. Near Misses: totales, en curso, finalizados ───
        cursor = await db.execute(
            "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ?",
            (ts_inicio_dia,)
        )
        total_near_miss = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL",
            (ts_inicio_dia,)
        )
        finalizados_hoy = (await cursor.fetchone())[0]

        seguimientos_activos = total_near_miss - finalizados_hoy

        aciertos = 0
        if finalizados_hoy > 0:
            cursor = await db.execute(
                "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND acerto_bot = 1",
                (ts_inicio_dia,)
            )
            aciertos = (await cursor.fetchone())[0]

        # Win Rate REAL: solo sobre finalizados
        if finalizados_hoy > 0:
            win_rate_val = round((aciertos / finalizados_hoy * 100), 2)
            win_rate_estado = "CALCULABLE"
        else:
            win_rate_val = None
            win_rate_estado = "PENDIENTE"

        # ─── 3. Grids REALES (executor) ───
        cursor = await db.execute(
            "SELECT count(*) FROM grid_ejecuciones WHERE date(datetime(timestamp_inicio, 'unixepoch')) = ?",
            (hoy_local,)
        )
        grids_total_hoy = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT count(*) FROM grid_ejecuciones WHERE estado = 'ACTIVO'",
            ()
        )
        grids_reales_activos = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COALESCE(SUM(pnl_real), 0), COALESCE(SUM(fees_real), 0) FROM grid_ejecuciones WHERE estado = 'CERRADO' AND date(datetime(timestamp_inicio, 'unixepoch')) = ?",
            (hoy_local,)
        )
        row = await cursor.fetchone()
        pnl_real_hoy = float(row[0]) if row and row[0] is not None else 0.0
        fees_real_hoy = float(row[1]) if row and row[1] is not None else 0.0

        cursor = await db.execute(
            "SELECT COALESCE(SUM(pnl_real), 0) FROM grid_ejecuciones WHERE estado = 'ACTIVO'",
            ()
        )
        row_activo = await cursor.fetchone()
        pnl_real_activo = float(row_activo[0]) if row_activo and row_activo[0] is not None else 0.0

        # ─── 4. Circuit Breakers hoy ───
        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'CIRCUIT_BREAKER' AND fecha = ?",
            (hoy_local,)
        )
        cb_hoy = (await cursor.fetchone())[0]

        # ─── 5. Último evento importante (FIXED) ───
        cursor = await db.execute(
            "SELECT tipo, symbol, timestamp_utc FROM auditoria_eventos WHERE fecha = ? AND tipo IN ('FIRE', 'CIRCUIT_BREAKER') ORDER BY timestamp_utc DESC LIMIT 1",
            (hoy_local,)
        )
        ultimo_evento = await cursor.fetchone()
        ultimo_evento_info = None
        if ultimo_evento:
            tipo_ev, sym_ev, ts_ev = ultimo_evento
            if isinstance(ts_ev, str):
                ts_ev = datetime.fromisoformat(ts_ev.replace('Z', '+00:00'))
                if ts_ev.tzinfo is None:
                    ts_ev = ts_ev.replace(tzinfo=pytz.UTC)
            if isinstance(ts_ev, datetime) and ts_ev.tzinfo is None:
                ts_ev = ts_ev.replace(tzinfo=pytz.UTC)
            mins_ago = int((datetime.now(pytz.UTC).timestamp() - ts_ev.timestamp()) / 60) if ts_ev else None
            # FIX: asignar el dict correctamente
            ultimo_evento_info = {"tipo": tipo_ev, "symbol": sym_ev, "minutos_ago": mins_ago}

        # ─── 6. Rechazados hoy ───
        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ?",
            (hoy_local,)
        )
        rechazados_hoy = (await cursor.fetchone())[0]

        return {
            "estado": "success",
            "fires_hoy": fires_hoy,
            "rechazados_hoy": rechazados_hoy,
            "total_near_miss": total_near_miss,
            "finalizados_hoy": finalizados_hoy,
            "seguimientos_activos": seguimientos_activos,
            "aciertos_bot": aciertos,
            "win_rate_rechazos": win_rate_val,
            "win_rate_estado": win_rate_estado,
            "grids_total_hoy": grids_total_hoy,
            "grids_reales_activos": grids_reales_activos,
            "pnl_real_hoy": round(pnl_real_hoy, 4),
            "fees_real_hoy": round(fees_real_hoy, 4),
            "pnl_real_activo": round(pnl_real_activo, 4),
            "cb_hoy": cb_hoy,
            "ultimo_evento": ultimo_evento_info,
            "mensaje": f"{seguimientos_activos} seguimiento(s) en curso (2h). Win Rate calculable sobre {finalizados_hoy} finalizados.",
        }

    except Exception as e:
        logger.error(f"Error obteniendo stats: {e}")
        return {"error": str(e), "estado": "failed"}


@app.get("/api/stats/per-coin")
async def get_stats_per_coin(request: Request):
    """Ranking de rendimiento por moneda. V6.2: Enriquecido con más métricas."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")
        ts_inicio_dia = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        result = []
        registry = _load_registry()
        symbols = [s for s, d in registry.items() if d.get("active", 0) == 1]

        for sym in symbols:
            # Fires
            cursor = await db.execute(
                "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ? AND symbol = ?",
                (hoy_local, sym)
            )
            fires = (await cursor.fetchone())[0]

            # Rechazados
            cursor = await db.execute(
                "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ? AND symbol = ?",
                (hoy_local, sym)
            )
            rechazos = (await cursor.fetchone())[0]

            # Near-misses finalizados y aciertos
            cursor = await db.execute(
                "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND symbol = ?",
                (ts_inicio_dia, sym)
            )
            nm_finalizados = (await cursor.fetchone())[0]

            aciertos = 0
            if nm_finalizados > 0:
                cursor = await db.execute(
                    "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND acerto_bot = 1 AND symbol = ?",
                    (ts_inicio_dia, sym)
                )
                aciertos = (await cursor.fetchone())[0]

            # PnL real grids
            cursor = await db.execute(
                "SELECT COALESCE(SUM(pnl_real), 0) FROM grid_ejecuciones WHERE symbol = ? AND date(datetime(timestamp_inicio, 'unixepoch')) = ?",
                (sym, hoy_local)
            )
            pnl = float((await cursor.fetchone())[0] or 0)

            # V6.2: Mejor/peor trade del día
            cursor = await db.execute(
                """
                SELECT MAX(realized_pnl), MIN(realized_pnl), COUNT(*)
                FROM pnl_eventos 
                WHERE symbol = ? AND date(datetime(timestamp_ms/1000, 'unixepoch')) = ?
                """,
                (sym, hoy_local)
            )
            row_trade = await cursor.fetchone()
            mejor_trade = float(row_trade[0]) if row_trade and row_trade[0] is not None else None
            peor_trade = float(row_trade[1]) if row_trade and row_trade[1] is not None else None
            total_trades = int(row_trade[2]) if row_trade and row_trade[2] is not None else 0

            # V6.2: Último fire timestamp
            cursor = await db.execute(
                "SELECT MAX(timestamp_utc) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ? AND symbol = ?",
                (hoy_local, sym)
            )
            row_last_fire = await cursor.fetchone()
            ultimo_fire_ts = row_last_fire[0] if row_last_fire and row_last_fire[0] else None

            # V6.2: Streak (últimos 5 near-misses finalizados)
            cursor = await db.execute(
                "SELECT acerto_bot FROM near_miss_seguimientos WHERE symbol = ? AND timestamp_fin IS NOT NULL ORDER BY timestamp_fin DESC LIMIT 5",
                (sym,)
            )
            streak_rows = await cursor.fetchall()
            streak = 0
            for r in streak_rows:
                if r[0] == 1:
                    streak += 1
                else:
                    break

            # Estado actual
            estados = getattr(request.app.state, 'signal_states', {})
            st = estados.get(sym)
            estado_actual = st.estado if st else "UNKNOWN"
            pausada = getattr(st, 'moneda_pausada_manual', False) if st else False

            result.append({
                "symbol": sym,
                "fires": fires,
                "rechazos": rechazos,
                "nm_finalizados": nm_finalizados,
                "nm_aciertos": aciertos,
                "win_rate": round((aciertos / nm_finalizados * 100), 1) if nm_finalizados > 0 else None,
                "pnl_real": round(pnl, 4),
                "estado": estado_actual,
                "pausada_manual": pausada,
                # V6.2 nuevos
                "mejor_trade": round(mejor_trade, 4) if mejor_trade is not None else None,
                "peor_trade": round(peor_trade, 4) if peor_trade is not None else None,
                "total_trades": total_trades,
                "ultimo_fire": ultimo_fire_ts,
                "streak_aciertos": streak,
            })

        result.sort(key=lambda x: x["pnl_real"], reverse=True)
        return {"estado": "success", "data": result}

    except Exception as e:
        logger.error(f"Error per-coin stats: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/funnel")
async def get_stats_funnel(request: Request):
    """Funnel de conversión: MONITOREO -> ARMED -> FIRE. V6.2: Enriquecido con latencias."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?",
            (hoy_local,)
        )
        fires = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'CAMBIO_ESTADO' AND fecha = ? AND estado_maquina LIKE '%ARMED%'",
            (hoy_local,)
        )
        armed = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ?",
            (hoy_local,)
        )
        rechazados = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'NEUTRAL_GRID_INICIADO' AND fecha = ?",
            (hoy_local,)
        )
        neutral_grids = (await cursor.fetchone())[0]

        monitoreo_filtro_ok = armed + rechazados

        # V6.2: Tiempo promedio en ARMED antes de FIRE
        cursor = await db.execute(
            """
            SELECT AVG(
                (SELECT timestamp_utc FROM auditoria_eventos a2 
                 WHERE a2.symbol = a1.symbol AND a2.tipo = 'FIRE' AND a2.timestamp_utc > a1.timestamp_utc 
                 ORDER BY a2.timestamp_utc ASC LIMIT 1) - a1.timestamp_utc
            )
            FROM auditoria_eventos a1
            WHERE a1.tipo = 'CAMBIO_ESTADO' AND a1.fecha = ? AND a1.estado_maquina LIKE '%ARMED%'
            """,
            (hoy_local,)
        )
        row_latency = await cursor.fetchone()
        avg_latency_fire = float(row_latency[0]) if row_latency and row_latency[0] is not None else None

        # V6.2: Tiempo promedio en ARMED antes de RECHAZO
        cursor = await db.execute(
            """
            SELECT AVG(
                (SELECT timestamp_utc FROM auditoria_eventos a2 
                 WHERE a2.symbol = a1.symbol AND a2.tipo = 'RECHAZADO' AND a2.timestamp_utc > a1.timestamp_utc 
                 ORDER BY a2.timestamp_utc ASC LIMIT 1) - a1.timestamp_utc
            )
            FROM auditoria_eventos a1
            WHERE a1.tipo = 'CAMBIO_ESTADO' AND a1.fecha = ? AND a1.estado_maquina LIKE '%ARMED%'
            """,
            (hoy_local,)
        )
        row_latency_r = await cursor.fetchone()
        avg_latency_reject = float(row_latency_r[0]) if row_latency_r and row_latency_r[0] is not None else None

        return {
            "estado": "success",
            "funnel": {
                "monitoreo_filtro_ok": monitoreo_filtro_ok,
                "armed": armed,
                "fires": fires,
                "neutral_grids": neutral_grids,
                "rechazados": rechazados,
            },
            "conversiones": {
                "monitoreo_to_armed_pct": round((armed / monitoreo_filtro_ok * 100), 1) if monitoreo_filtro_ok > 0 else 0,
                "armed_to_fire_pct": round((fires / armed * 100), 1) if armed > 0 else 0,
                "fire_vs_neutral_pct": round((fires / (fires + neutral_grids) * 100), 1) if (fires + neutral_grids) > 0 else 0,
            },
            "latencias": {
                "avg_seg_armed_to_fire": round(avg_latency_fire, 1) if avg_latency_fire is not None else None,
                "avg_seg_armed_to_reject": round(avg_latency_reject, 1) if avg_latency_reject is not None else None,
            }
        }

    except Exception as e:
        logger.error(f"Error funnel stats: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/rechazos")
async def get_stats_rechazos(request: Request):
    """Rechazos por causa principal."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        cursor = await db.execute(
            "SELECT rechazos_json FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ?",
            (hoy_local,)
        )
        rows = await cursor.fetchall()

        causas = {}
        total = 0
        for row in rows:
            msg = row[0] or ""
            if msg:
                try:
                    rechazos_list = json.loads(msg)
                    msg = "; ".join(rechazos_list) if rechazos_list else ""
                except:
                    pass
            causa = "OTRO"
            if msg:
                if "ADX" in msg.upper():
                    causa = "ADX"
                elif "VOLUMEN" in msg.upper() or "VOLUME" in msg.upper():
                    causa = "VOLUMEN"
                elif "RSI" in msg.upper():
                    causa = "RSI"
                elif "MFM" in msg.upper():
                    causa = "MFM"
                elif "MACD" in msg.upper():
                    causa = "MACD"
                elif "ATR" in msg.upper():
                    causa = "ATR"
                elif "DIRECCION" in msg.upper() or "NEUTRAL" in msg.upper():
                    causa = "DIRECCION_NEUTRAL"
                elif "CAPITAL" in msg.upper() or "NOTIONAL" in msg.upper():
                    causa = "CAPITAL"
                else:
                    parts = msg.split(':')
                    if len(parts) > 1:
                        causa = parts[0].strip().upper()[:20]
                    else:
                        causa = msg.split()[0].upper()[:20] if msg.split() else "OTRO"
            causas[causa] = causas.get(causa, 0) + 1
            total += 1

        lista_causas = [{"causa": k, "count": v, "pct": round(v/total*100, 1)} for k, v in sorted(causas.items(), key=lambda x: x[1], reverse=True)]

        return {
            "estado": "success",
            "total_rechazos": total,
            "causas": lista_causas,
        }

    except Exception as e:
        logger.error(f"Error rechazos stats: {e}")
        return {"estado": "failed", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# V6.2: NUEVOS ENDPOINTS ANALÍTICOS AVANZADOS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/stats/equity")
async def get_stats_equity(request: Request, dias: int = 1):
    """Curva de equity: PnL acumulado por hora."""
    try:
        db = await _get_db()
        desde = datetime.now(pytz.UTC) - timedelta(days=dias)
        ts_desde = int(desde.timestamp() * 1000)

        cursor = await db.execute(
            """
            SELECT 
                strftime('%Y-%m-%d %H:00:00', datetime(timestamp_ms/1000, 'unixepoch')) as hora,
                SUM(realized_pnl) as pnl_hora,
                COUNT(*) as trades_hora
            FROM pnl_eventos
            WHERE timestamp_ms >= ?
            GROUP BY hora
            ORDER BY hora ASC
            """,
            (ts_desde,)
        )
        rows = await cursor.fetchall()

        equity = 0.0
        result = []
        for row in rows:
            equity += float(row[1] or 0)
            result.append({
                "hora": row[0],
                "pnl_hora": round(float(row[1] or 0), 4),
                "equity": round(equity, 4),
                "trades": int(row[2])
            })

        return {"estado": "success", "dias": dias, "data": result}
    except Exception as e:
        logger.error(f"Error equity stats: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/latency")
async def get_stats_latency(request: Request):
    """Tiempo promedio que pasa una moneda en ARMED antes de decisión."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        cursor = await db.execute(
            """
            SELECT a1.symbol, a1.timestamp_utc as t_armed,
                   (SELECT timestamp_utc FROM auditoria_eventos a2 
                    WHERE a2.symbol = a1.symbol AND a2.tipo IN ('FIRE', 'RECHAZADO') 
                    AND a2.timestamp_utc > a1.timestamp_utc 
                    ORDER BY a2.timestamp_utc ASC LIMIT 1) as t_decision
            FROM auditoria_eventos a1
            WHERE a1.tipo = 'CAMBIO_ESTADO' AND a1.fecha = ? AND a1.estado_maquina LIKE '%ARMED%'
            """,
            (hoy_local,)
        )
        rows = await cursor.fetchall()

        latencias = []
        for row in rows:
            if row[2]:
                try:
                    t1 = datetime.fromisoformat(str(row[1]).replace('Z', '+00:00')) if isinstance(row[1], str) else row[1]
                    t2 = datetime.fromisoformat(str(row[2]).replace('Z', '+00:00')) if isinstance(row[2], str) else row[2]
                    if hasattr(t1, 'timestamp') and hasattr(t2, 'timestamp'):
                        delta = (t2.timestamp() - t1.timestamp())
                        latencias.append({"symbol": row[0], "segundos": round(delta, 1)})
                except Exception:
                    pass

        if latencias:
            avg = sum(l["segundos"] for l in latencias) / len(latencias)
            max_l = max(l["segundos"] for l in latencias)
            min_l = min(l["segundos"] for l in latencias)
        else:
            avg = max_l = min_l = None

        return {
            "estado": "success",
            "count": len(latencias),
            "avg_segundos": round(avg, 1) if avg is not None else None,
            "max_segundos": round(max_l, 1) if max_l is not None else None,
            "min_segundos": round(min_l, 1) if min_l is not None else None,
            "detalle": latencias[:20]  # Top 20 para no saturar
        }
    except Exception as e:
        logger.error(f"Error latency stats: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/filter-efficiency")
async def get_stats_filter_efficiency(request: Request):
    """Eficiencia por filtro: ¿cuántos rechazos de cada tipo resultaron en near-miss acertado?"""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        # Para cada near-miss acertado, buscar el rechazo previo más cercano de la misma moneda
        cursor = await db.execute(
            """
            SELECT nm.symbol, nm.acerto_bot, nm.timestamp_inicio, ae.rechazos_json
            FROM near_miss_seguimientos nm
            LEFT JOIN auditoria_eventos ae ON ae.symbol = nm.symbol 
                AND ae.tipo = 'RECHAZADO' 
                AND ae.timestamp_utc < datetime(nm.timestamp_inicio, 'unixepoch')
            WHERE nm.timestamp_fin IS NOT NULL AND date(datetime(nm.timestamp_inicio, 'unixepoch')) = ?
            ORDER BY nm.timestamp_inicio ASC
            """,
            (hoy_local,)
        )
        rows = await cursor.fetchall()

        # Agrupar por causa de rechazo
        eficiencia = {}
        for row in rows:
            rechazos_json = row[3] or ""
            acerto = row[1]
            try:
                rechazos = json.loads(rechazos_json) if rechazos_json else []
            except:
                rechazos = [rechazos_json]

            for r in rechazos:
                causa = "OTRO"
                r_str = str(r).upper()
                if "ADX" in r_str: causa = "ADX"
                elif "VOLUMEN" in r_str or "VOLUME" in r_str: causa = "VOLUMEN"
                elif "RSI" in r_str: causa = "RSI"
                elif "MFM" in r_str: causa = "MFM"
                elif "MACD" in r_str: causa = "MACD"
                elif "ATR" in r_str: causa = "ATR"
                elif "DIRECCION" in r_str or "NEUTRAL" in r_str: causa = "DIRECCION_NEUTRAL"
                elif "CAPITAL" in r_str or "NOTIONAL" in r_str: causa = "CAPITAL"

                if causa not in eficiencia:
                    eficiencia[causa] = {"total": 0, "acertados": 0, "fallados": 0}
                eficiencia[causa]["total"] += 1
                if acerto == 1:
                    eficiencia[causa]["acertados"] += 1
                else:
                    eficiencia[causa]["fallados"] += 1

        result = []
        for causa, data in sorted(eficiencia.items(), key=lambda x: x[1]["total"], reverse=True):
            result.append({
                "causa": causa,
                "total": data["total"],
                "acertados": data["acertados"],
                "fallados": data["fallados"],
                "precision": round((data["acertados"] / data["total"] * 100), 1) if data["total"] > 0 else 0
            })

        return {"estado": "success", "data": result}
    except Exception as e:
        logger.error(f"Error filter efficiency: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/score-distribution")
async def get_stats_score_distribution(request: Request):
    """Distribución de scores en FIRE vs RECHAZADO."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        cursor = await db.execute(
            "SELECT score, tipo FROM auditoria_eventos WHERE fecha = ? AND tipo IN ('FIRE', 'RECHAZADO') AND score IS NOT NULL",
            (hoy_local,)
        )
        rows = await cursor.fetchall()

        fire_scores = [r[0] for r in rows if r[1] == 'FIRE' and r[0] is not None]
        reject_scores = [r[0] for r in rows if r[1] == 'RECHAZADO' and r[0] is not None]

        def buckets(scores):
            if not scores:
                return {}
            b = {"0-30": 0, "31-50": 0, "51-70": 0, "71-85": 0, "86-100": 0}
            for s in scores:
                if s <= 30: b["0-30"] += 1
                elif s <= 50: b["31-50"] += 1
                elif s <= 70: b["51-70"] += 1
                elif s <= 85: b["71-85"] += 1
                else: b["86-100"] += 1
            return b

        return {
            "estado": "success",
            "fire": {"count": len(fire_scores), "avg": round(sum(fire_scores)/len(fire_scores), 1) if fire_scores else None, "buckets": buckets(fire_scores)},
            "rechazados": {"count": len(reject_scores), "avg": round(sum(reject_scores)/len(reject_scores), 1) if reject_scores else None, "buckets": buckets(reject_scores)}
        }
    except Exception as e:
        logger.error(f"Error score distribution: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/direction-pnl")
async def get_stats_direction_pnl(request: Request, dias: int = 1):
    """PnL por dirección (LONG vs SHORT)."""
    try:
        db = await _get_db()
        desde = datetime.now(pytz.UTC) - timedelta(days=dias)
        ts_desde = int(desde.timestamp())

        cursor = await db.execute(
            """
            SELECT direction, COALESCE(SUM(pnl_real), 0), COALESCE(SUM(fees_real), 0), COUNT(*)
            FROM grid_ejecuciones
            WHERE timestamp_inicio >= ? AND estado = 'CERRADO'
            GROUP BY direction
            """,
            (ts_desde,)
        )
        rows = await cursor.fetchall()

        result = {}
        for row in rows:
            result[row[0]] = {
                "pnl": round(float(row[1]), 4),
                "fees": round(float(row[2]), 4),
                "grids": int(row[3])
            }

        return {"estado": "success", "dias": dias, "data": result}
    except Exception as e:
        logger.error(f"Error direction pnl: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/drawdown")
async def get_stats_drawdown(request: Request, dias: int = 7):
    """Drawdown máximo y actual basado en curva de equity."""
    try:
        db = await _get_db()
        desde = datetime.now(pytz.UTC) - timedelta(days=dias)
        ts_desde = int(desde.timestamp() * 1000)

        cursor = await db.execute(
            """
            SELECT timestamp_ms, realized_pnl 
            FROM pnl_eventos 
            WHERE timestamp_ms >= ?
            ORDER BY timestamp_ms ASC
            """,
            (ts_desde,)
        )
        rows = await cursor.fetchall()

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        dd_series = []
        for row in rows:
            equity += float(row[1] or 0)
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
            dd_series.append({"ts": row[0], "equity": round(equity, 4), "drawdown": round(dd, 4)})

        return {
            "estado": "success",
            "dias": dias,
            "max_drawdown": round(max_dd, 4),
            "current_drawdown": round(dd_series[-1]["drawdown"], 4) if dd_series else 0.0,
            "current_equity": round(equity, 4),
            "peak_equity": round(peak, 4),
            "series": dd_series
        }
    except Exception as e:
        logger.error(f"Error drawdown stats: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/risk-reward")
async def get_stats_risk_reward(request: Request, dias: int = 7):
    """Ratio riesgo/beneficio promedio de trades."""
    try:
        db = await _get_db()
        desde = datetime.now(pytz.UTC) - timedelta(days=dias)
        ts_desde = int(desde.timestamp() * 1000)

        cursor = await db.execute(
            """
            SELECT 
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) as avg_win,
                AVG(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) END) as avg_loss,
                COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) as wins,
                COUNT(CASE WHEN realized_pnl < 0 THEN 1 END) as losses
            FROM pnl_eventos
            WHERE timestamp_ms >= ?
            """,
            (ts_desde,)
        )
        row = await cursor.fetchone()

        avg_win = float(row[0]) if row and row[0] is not None else 0
        avg_loss = float(row[1]) if row and row[1] is not None else 0
        wins = int(row[2]) if row and row[2] is not None else 0
        losses = int(row[3]) if row and row[3] is not None else 0

        ratio = round(avg_win / avg_loss, 2) if avg_loss > 0 else None

        return {
            "estado": "success",
            "dias": dias,
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            "wins": wins,
            "losses": losses,
            "risk_reward_ratio": ratio,
            "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
        }
    except Exception as e:
        logger.error(f"Error risk reward: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/stats/missed-opportunities")
async def get_stats_missed_opportunities(request: Request):
    """Oportunidades perdidas rentables: near-misses donde hubiera_sido_rentable=1 pero el bot rechazó."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        cursor = await db.execute(
            """
            SELECT symbol, score, umbral, movimiento_pct, direccion_nm, filtros_rechazo
            FROM near_miss_seguimientos
            WHERE hubiera_sido_rentable = 1 AND date(datetime(timestamp_inicio, 'unixepoch')) = ?
            ORDER BY movimiento_pct DESC
            """,
            (hoy_local,)
        )
        rows = await cursor.fetchall()

        result = []
        for row in rows:
            filtros = []
            try:
                filtros = json.loads(row[5]) if row[5] else []
            except:
                filtros = [str(row[5])] if row[5] else []
            result.append({
                "symbol": row[0],
                "score": row[1],
                "umbral": row[2],
                "movimiento_pct": round(float(row[3]), 2) if row[3] is not None else None,
                "direccion": row[4],
                "filtros_rechazo": filtros
            })

        # Agrupar por causa principal de rechazo
        por_filtro = {}
        for r in result:
            for f in r["filtros_rechazo"]:
                causa = "OTRO"
                f_str = str(f).upper()
                if "ADX" in f_str: causa = "ADX"
                elif "VOLUMEN" in f_str or "VOLUME" in f_str: causa = "VOLUMEN"
                elif "RSI" in f_str: causa = "RSI"
                elif "MFM" in f_str: causa = "MFM"
                elif "MACD" in f_str: causa = "MACD"
                elif "ATR" in f_str: causa = "ATR"
                elif "DIRECCION" in f_str or "NEUTRAL" in f_str: causa = "DIRECCION_NEUTRAL"
                por_filtro[causa] = por_filtro.get(causa, 0) + 1

        return {
            "estado": "success",
            "total": len(result),
            "oportunidades": result[:50],  # Limitar
            "por_filtro": [{"causa": k, "count": v} for k, v in sorted(por_filtro.items(), key=lambda x: x[1], reverse=True)]
        }
    except Exception as e:
        logger.error(f"Error missed opportunities: {e}")
        return {"estado": "failed", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# V6.2: ENDPOINT SALUD DEL SISTEMA
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/system/health")
async def get_system_health(request: Request):
    """Estado de salud del bot: executor, cola, websockets, etc."""
    try:
        executor = getattr(request.app.state, 'executor', None)
        signal_generator = getattr(request.app.state, 'signal_generator', None)

        health = {
            "estado": "success",
            "timestamp": int(time.time() * 1000),
            "executor": {
                "disponible": executor is not None,
                "grids_activos": len(executor._grids) if executor else 0,
                "cola_size": executor.queue.qsize() if executor and hasattr(executor, 'queue') else 0,
            },
            "signal_generator": {
                "disponible": signal_generator is not None,
                "monedas_monitoreadas": len(signal_generator.states) if signal_generator else 0,
            },
            "websocket": {
                "conexiones_activas": len(manager.active_connections),
                "conexiones_historicas": manager._connection_count,
            },
            "precios_vivo": {
                "monedas_con_precio": len(getattr(request.app.state, 'precios_vivo', {})),
            }
        }
        return health
    except Exception as e:
        logger.error(f"Error health check: {e}")
        return {"estado": "failed", "error": str(e)}


# -------------------------------------------------------------------------------
# V4.2: ENDPOINTS REST PARA PAUSA MANUAL
# -------------------------------------------------------------------------------

@app.post("/api/pause/{symbol}")
async def pause_symbol(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    signal_generator = getattr(request.app.state, 'signal_generator', None)
    if not signal_generator:
        return {"error": "SignalGenerator no disponible", "symbol": symbol}

    if symbol not in signal_generator.states:
        return {"error": f"Símbolo {symbol} no encontrado", "symbol": symbol}

    success = signal_generator.pausar_moneda_manual(symbol, "REST API")
    return {
        "symbol": symbol,
        "paused": success,
        "manual_pause": signal_generator.states[symbol].moneda_pausada_manual,
        "auto_pause": signal_generator.states[symbol].moneda_pausada,
        "timestamp": int(time.time() * 1000)
    }


@app.post("/api/resume/{symbol}")
async def resume_symbol(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    signal_generator = getattr(request.app.state, 'signal_generator', None)
    if not signal_generator:
        return {"error": "SignalGenerator no disponible", "symbol": symbol}

    if symbol not in signal_generator.states:
        return {"error": f"Símbolo {symbol} no encontrado", "symbol": symbol}

    success = signal_generator.reanudar_moneda_manual(symbol)
    return {
        "symbol": symbol,
        "resumed": success,
        "manual_pause": signal_generator.states[symbol].moneda_pausada_manual,
        "auto_pause": signal_generator.states[symbol].moneda_pausada,
        "timestamp": int(time.time() * 1000)
    }


@app.post("/api/pause_all")
async def pause_all(request: Request):
    signal_generator = getattr(request.app.state, 'signal_generator', None)
    if not signal_generator:
        return {"error": "SignalGenerator no disponible"}

    pausadas = signal_generator.pausar_todas_manual("REST API /pause_all")
    return {
        "paused": pausadas,
        "count": len(pausadas),
        "timestamp": int(time.time() * 1000)
    }


@app.post("/api/resume_all")
async def resume_all(request: Request):
    signal_generator = getattr(request.app.state, 'signal_generator', None)
    if not signal_generator:
        return {"error": "SignalGenerator no disponible"}

    reanudadas = signal_generator.reanudar_todas_manual()
    return {
        "resumed": reanudadas,
        "count": len(reanudadas),
        "timestamp": int(time.time() * 1000)
    }


@app.get("/api/paused")
async def get_paused(request: Request):
    signal_generator = getattr(request.app.state, 'signal_generator', None)
    if not signal_generator:
        return {"error": "SignalGenerator no disponible"}

    pausadas = signal_generator.get_monedas_pausadas()
    return {
        "paused": pausadas,
        "count": len(pausadas),
        "timestamp": int(time.time() * 1000)
    }

# -------------------------------------------------------------------------------
# V5.9.2: ENDPOINT GRID NEUTRAL (CORREGIDO CON FALLBACK SEGURO)
# -------------------------------------------------------------------------------

@app.get("/api/grid-neutral/{symbol}")
async def get_grid_neutral(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    executor = getattr(request.app.state, 'executor', None)

    if not executor:
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Executor no disponible",
            "timestamp": int(time.time() * 1000)
        }

    grid_state = executor._grids.get(symbol)
    if not grid_state or getattr(grid_state, 'grid_mode', 'DIRECTIONAL') != 'NEUTRAL':
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Sin grid neutral activo",
            "timestamp": int(time.time() * 1000)
        }

    # Fallback seguro: si no existe get_estado_grid, leer atributos directamente
    estado = None
    try:
        if hasattr(executor, 'get_estado_grid'):
            estado = executor.get_estado_grid(grid_state)
        else:
            # Lectura directa de atributos conocidos
            estado = {
                'grid_id': getattr(grid_state, 'grid_id', 'unknown'),
                'niveles': getattr(grid_state, 'grid_count', 0),
                'niveles_buy': getattr(grid_state, 'niveles_buy', 0),
                'niveles_sell': getattr(grid_state, 'niveles_sell', 0),
                'posiciones_abiertas': getattr(grid_state, 'posiciones_abiertas', 0),
                'posiciones_atrapadas': getattr(grid_state, 'posiciones_atrapadas', 0),
                'posiciones_vencidas': getattr(grid_state, 'posiciones_vencidas', 0),
                'trades_completados': getattr(grid_state, 'trades_completados', 0),
                'trades_kill_switch': getattr(grid_state, 'trades_kill_switch', 0),
                'pnl_neto': getattr(grid_state, 'pnl_real', 0.0),
                'pnl_bruto': getattr(grid_state, 'pnl_real', 0.0),
                'fees_totales': getattr(grid_state, 'fees_real', 0.0),
                'max_posiciones_simultaneas': getattr(grid_state, 'max_posiciones_simultaneas', 0),
                'posicion_neta': getattr(grid_state, 'posicion_neta', 0.0),
                'ordenes_tp_pendientes': getattr(grid_state, 'ordenes_tp_pendientes', 0),
                'ultimo_tick_segundos_ago': getattr(grid_state, 'ultimo_tick', 0),
            }
    except Exception as e:
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Error leyendo estado del executor",
            "error": str(e),
            "timestamp": int(time.time() * 1000)
        }

    if not estado:
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Sin grid neutral activo",
            "timestamp": int(time.time() * 1000)
        }

    return {
        "symbol": symbol,
        "grid_activo": True,
        "grid_id": estado.get('grid_id'),
        "niveles": estado.get('niveles'),
        "niveles_buy": estado.get('niveles_buy'),
        "niveles_sell": estado.get('niveles_sell'),
        "posiciones_abiertas": estado.get('posiciones_abiertas'),
        "posiciones_atrapadas": estado.get('posiciones_atrapadas'),
        "posiciones_vencidas": estado.get('posiciones_vencidas'),
        "trades_completados": estado.get('trades_completados'),
        "trades_kill_switch": estado.get('trades_kill_switch'),
        "pnl_neto": estado.get('pnl_neto'),
        "pnl_bruto": estado.get('pnl_bruto'),
        "fees_totales": estado.get('fees_totales'),
        "max_posiciones_simultaneas": estado.get('max_posiciones_simultaneas'),
        "posicion_neta": estado.get('posicion_neta'),
        "ordenes_tp_pendientes": estado.get('ordenes_tp_pendientes'),
        "ultimo_tick_minutos": estado.get('ultimo_tick_segundos_ago', 0) // 60,
        "timestamp": int(time.time() * 1000)
    }


# -------------------------------------------------------------------------------
# CR2: ENDPOINT PnL EN TIEMPO REAL (MEJORADO)
# -------------------------------------------------------------------------------

@app.get("/api/grid/{symbol}/pnl")
async def get_grid_pnl(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)
    executor = getattr(request.app.state, 'executor', None)
    if not executor or symbol not in executor._grids:
        raise HTTPException(status_code=404, detail="Grid no activo")

    state = executor._grids[symbol]

    pnl_ram = float(state.pnl_real)
    fees_ram = float(state.fees_real)
    pnl_db = await calcular_pnl_acumulado(state.grid_id)
    desglose = await obtener_pnl_por_tipo(state.grid_id)

    # V6.2: Agregar posiciones abiertas individuales si están disponibles
    posiciones = []
    if hasattr(state, 'posiciones') and state.posiciones:
        for pos in state.posiciones:
            posiciones.append({
                "id": getattr(pos, 'id', 'unknown'),
                "side": getattr(pos, 'side', 'UNKNOWN'),
                "entry": getattr(pos, 'entry_price', 0),
                "qty": getattr(pos, 'qty', 0),
                "pnl_unrealized": getattr(pos, 'pnl_unrealized', 0),
                "minutos_abierta": getattr(pos, 'minutos_abierta', 0),
            })

    return {
        'symbol': symbol,
        'grid_id': state.grid_id,
        'pnl_ram': pnl_ram,
        'pnl_db': pnl_db['pnl_real'],
        'fees_ram': fees_ram,
        'fees_db': pnl_db['fees_real'],
        'discrepancia': abs(pnl_ram - pnl_db['pnl_real']),
        'total_trades': pnl_db['total_trades'],
        'trades_ganadores': pnl_db['trades_ganadores'],
        'trades_perdedores': pnl_db['trades_perdedores'],
        'desglose': desglose,
        'posicion_neta': float(state.posicion_neta),
        'posiciones_abiertas': state.grid_state.contar_posiciones_abiertas() if state.grid_state else 0,
        'posiciones_detalle': posiciones,
        'timestamp': int(time.time() * 1000)
    }


@app.get("/api/coin-registry")
async def get_coin_registry():
    registry = _load_registry()
    return {
        "registry": registry,
        "total": len(registry),
        "active_count": sum(1 for d in registry.values() if d.get("active", 0) == 1),
        "categories": list(set(d.get("category", "unknown") for d in registry.values())),
        "timestamp": int(time.time() * 1000)
    }


@app.post("/api/toggle-coin")
async def toggle_coin(request: Request):
    try:
        data = await request.json()
        symbol = data.get("symbol")
        active = data.get("active")

        if symbol is None or active is None:
            return {"error": "Faltan 'symbol' o 'active'", "success": False}

        symbol = _sanitize_symbol(symbol)
        registry = _load_registry()
        if symbol not in registry:
            return {"error": f"{symbol} no existe en el registro", "success": False}

        registry[symbol]["active"] = 1 if active else 0
        _save_registry(registry)

        status_text = "ACTIVADA" if active else "DESACTIVADA"
        msg_text = (
            "📋 <b>Registro actualizado</b>\n"
            + f"{symbol}: {'✅' if active else '❌'} {status_text}\n"
            + "🔄 Ejecuta <code>/restart</code> para aplicar cambios."
        )
        _notifier_rest = getattr(request.app.state, 'notifier', None)
        if _notifier_rest:
            await _notifier_rest.enviar_telegram(msg_text)
        else:
            print("  [API] Notifier no disponible en app.state")

        await manager.broadcast_json({
            "msg_type": "coin_config_update",
            "coin_config": {symbol: {"active": active}}
        })

        return {
            "symbol": symbol,
            "active": active,
            "success": True,
            "needs_restart": True,
            "message": "Cambio guardado. Ejecuta /restart para aplicar."
        }

    except Exception as e:
        return {"error": str(e), "success": False}


@app.post("/api/toggle-all-coins")
async def toggle_all_coins(request: Request):
    try:
        data = await request.json()
        active = data.get("active")
        category = data.get("category")

        if active is None:
            return {"error": "Falta 'active'", "success": False}

        registry = _load_registry()
        changed = []

        for symbol, data_coin in registry.items():
            if category and data_coin.get("category") != category:
                continue
            if data_coin.get("active", 0) != (1 if active else 0):
                data_coin["active"] = 1 if active else 0
                changed.append(symbol)

        _save_registry(registry)

        cat_msg = f" en categoría '{category}'" if category else ""
        status_text = "ACTIVADAS" if active else "DESACTIVADAS"
        msg_text = (
            f"📋 <b>Todas {status_text}{cat_msg}</b>\n"
            + f"Monedas afectadas: {len(changed)}\n"
            + "🔄 Ejecuta <code>/restart</code> para aplicar cambios."
        )
        _notifier_rest = getattr(request.app.state, 'notifier', None)
        if _notifier_rest:
            await _notifier_rest.enviar_telegram(msg_text)
        else:
            print("  [API] Notifier no disponible en app.state")

        update_payload = {symbol: {"active": active} for symbol in changed}
        await manager.broadcast_json({
            "msg_type": "coin_config_update",
            "coin_config": update_payload
        })

        return {
            "changed": changed,
            "count": len(changed),
            "active": active,
            "success": True,
            "needs_restart": True
        }

    except Exception as e:
        return {"error": str(e), "success": False}


@app.post("/api/force-fire/{symbol}")
async def force_fire(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)

    # V6.2: Rate limiting
    if not _check_rate_limit(f"force_fire:{symbol}", seconds=3):
        return {"success": False, "error": "Rate limit: espera 3 segundos entre force-fire"}

    executor = getattr(request.app.state, 'executor', None)
    if not executor:
        return {"success": False, "error": "Executor no disponible (modo SIMULACION?)"}

    precios = getattr(request.app.state, 'precios_vivo', {})
    price = precios.get(symbol, 0)
    if not price:
        return {"success": False, "error": f"Sin precio vivo para {symbol}"}

    try:
        body = await request.json()
        direction = str(body.get('direction', 'LONG')).upper()
    except Exception:
        direction = 'LONG'

    if direction not in ('LONG', 'SHORT'):
        return {"success": False, "error": "direction debe ser LONG o SHORT"}

    signal_generator = getattr(request.app.state, 'signal_generator', None)
    params = None
    rechazos = []
    modo_fallback = False

    if signal_generator:
        state = signal_generator.states.get(symbol)
        i15 = signal_generator.indicadores_15m.get(symbol, {})
        i4h = signal_generator.indicadores_4h.get(symbol, {})

        atr = i15.get('atr', price * 0.01)
        if not atr or atr <= 0:
            atr = price * 0.01

        params, rechazos = signal_generator.calcular_parametros_grid_blindado(
            price=price, direction=direction, atr=atr,
            i15=i15, i4h=i4h, symbol=symbol, state=state
        )

    if not params:
        print(f"  [FORCE_FIRE] {symbol} Motor blindado rechazó: {rechazos}. Usando fallback forzado.")
        modo_fallback = True

        if price < 1.0:
            rango_pct = 0.30
            grid_count = 7
        elif price < 10.0:
            rango_pct = 0.20
            grid_count = 7
        else:
            rango_pct = 0.15
            grid_count = 7

        rango_total = price * rango_pct * 2
        lower = max(price - (rango_total / 2), price * 0.001)
        upper = price + (rango_total / 2)

        step_usdt = rango_total / grid_count
        step_pct = (step_usdt / price) * 100 if price > 0 else 0

        capital = 100.0
        leverage = 5
        poder_total = capital * leverage
        notional_por_orden = poder_total / grid_count
        qty_por_orden = notional_por_orden / price if price > 0 else 0

        fee_rate = 0.0005
        breakeven = (2 * fee_rate + 2 * 0.0005) * 100

        params = {
            'direction': direction,
            'upper_limit': round(float(upper), 6),
            'lower_limit': round(float(lower), 6),
            'grid_count': grid_count,
            'step_usdt': round(float(step_usdt), 6),
            'step_pct': round(float(step_pct), 3),
            'breakeven_pct': round(breakeven, 3),
            'capital_sugerido': capital,
            'apalancamiento_sugerido': leverage,
            'notional_por_orden': round(notional_por_orden, 2),
            'qty_por_orden': round(qty_por_orden, 4),
            'margen_sobre_breakeven': round(step_pct - breakeven, 3),
            'rentable': True,
            'posicion_en_rango': 0.5,
            'recent_high': round(float(upper), 4),
            'recent_low': round(float(lower), 4),
            'auto_compressed': False,
            'posicion_extrema': False,
            'atr_seguro': round(rango_total / 4, 6),
            'rango_mult': 4.0,
        }
        print(f"  [FORCE_FIRE] {symbol} Fallback: {grid_count} grids | "
              f"[{params['lower_limit']}, {params['upper_limit']}] | "
              f"Step:{step_pct:.2f}% | Qty:{qty_por_orden:.2f}")

    if 'qty_por_orden' not in params or params['qty_por_orden'] <= 0:
        params['qty_por_orden'] = max(1.0, 5.0 / price) if price > 0 else 0.1

    print(f"  [FORCE_FIRE] {symbol} {'[FALLBACK]' if modo_fallback else '[BLINDADO]'} "
          f"Enviando grid {direction} al executor | {params['grid_count']} niveles | "
          f"Rango: {params['lower_limit']} - {params['upper_limit']}")
    await executor.queue.put({
        'tipo': 'CREAR_GRID',
        'symbol': symbol,
        'direction': direction,
        'params': params,
        'price': price
    })

    icon = "🟢" if direction == 'LONG' else "🔴"
    return {
        "success": True,
        "symbol": symbol,
        "price": price,
        "direction": direction,
        "message": f"{icon} Grid {direction} forzado en {symbol} @ ${price}"
    }


@app.post("/api/force-close/{symbol}")
async def force_close(symbol: str, request: Request):
    symbol = _sanitize_symbol(symbol)

    # V6.2: Rate limiting
    if not _check_rate_limit(f"force_close:{symbol}", seconds=3):
        return {"success": False, "error": "Rate limit: espera 3 segundos entre force-close"}

    executor = getattr(request.app.state, 'executor', None)
    if not executor:
        return {"success": False, "error": "Executor no disponible (modo SIMULACION?)"}

    await executor.queue.put({
        'tipo': 'ABORTAR_GRID',
        'symbol': symbol,
        'razon': 'force_close_manual'
    })

    return {
        "success": True,
        "symbol": symbol,
        "message": f"🛑 Aborto forzado solicitado para {symbol}"
    }


async def orquestador_eventos(queue_eventos: asyncio.Queue, precios_vivo: dict,
                               indicadores_1m: dict, indicadores_15m: dict,
                               indicadores_4h: dict, signal_states: dict,
                               notifier=None):
    _notifier = notifier
    async def escuchar_alertas():
        while True:
            evento = await queue_eventos.get()
            print(f"  [ORQUESTADOR] Evento recibido: {evento.get('tipo', 'UNKNOWN')} {evento.get('symbol', 'UNKNOWN')}")
            try:
                await guardar_alerta(
                    symbol=evento.get('symbol'), tipo=evento.get('tipo'), 
                    direccion=evento.get('direction'),
                    score=evento.get('score'), mensaje=str(evento.get('rechazos', [])),
                    params_json=safe_json_dumps(evento.get('params') or {}), 
                    precio=evento.get('price')
                )
                print(f"  [ORQUESTADOR] Alerta guardada en DB: {evento.get('tipo')} {evento.get('symbol')}")
            except Exception as e:
                logger.exception(f"Error guardando alerta en DB: {e}")
                print(f"  ⚠️ [ORQUESTADOR] ERROR guardando alerta DB: {e}")

            try:
                await _notifier.procesar_alerta(evento)
            except Exception as e:
                logger.exception(f"ERROR notificando alerta {evento.get('tipo')} para {evento.get('symbol')}: {e}")
                print(f"  ⚠️ [ORQUESTADOR] ERROR alerta {evento.get('tipo')} {evento.get('symbol')}: {e}")
                try:
                    await _notifier.enviar_telegram(
                        f"⚠️ <b>Error procesando alerta {evento.get('tipo')}</b> para {evento.get('symbol')}\n"
                        f"<code>{str(e)[:200]}</code>"
                    )
                except Exception:
                    pass

            await manager.broadcast_json({
                "msg_type": "alerta",
                "symbol": evento["symbol"],
                "payload": evento
            })

    async def empujar_telemetria():
        ciclo = 0
        while True:
            ciclo += 1
            t0 = time.time()

            if not precios_vivo:
                if ciclo % 5 == 0:
                    logger.info("[RAM VACÍA] Esperando markPrice de Binance...")
                await asyncio.sleep(1)
                continue

            if ciclo % 10 == 0:
                precios_str = {k: f"${v:.4f}" for k, v in list(precios_vivo.items())[:3]}
                logger.info(f"[TELEMETRÍA] Precios: {precios_str} | "
                           f"1m: {len(indicadores_1m)} | 15m: {len(indicadores_15m)} | "
                           f"4h: {len(indicadores_4h)} | UI: {len(manager.active_connections)}")

            for symbol, precio in list(precios_vivo.items()):
                st = signal_states.get(symbol)
                i1m = indicadores_1m.get(symbol, {})
                i15m = indicadores_15m.get(symbol, {})
                i4h = indicadores_4h.get(symbol, {})

                payload_consolidado = {
                    "msg_type": "snapshot",
                    "symbol": symbol,
                    "payload": {
                        "precio": precio,
                        "indicadores_1m": {
                            "rsi_7": i1m.get("rsi_7"),
                            "ema_300": i1m.get("ema_300"),
                            "wick_upper_pct": i1m.get("wick_upper_pct"),
                            "wick_lower_pct": i1m.get("wick_lower_pct"),
                            "body_direction": i1m.get("body_direction"),
                            "close": i1m.get("close"),
                            "high": i1m.get("high"),
                            "low": i1m.get("low"),
                            "atr_1m": i1m.get("atr_1m"),
                            "mecha_valida_short": i1m.get("mecha_valida_short"),
                            "mecha_valida_long": i1m.get("mecha_valida_long"),
                            "ema300_distancia_pct": i1m.get("ema300_distancia_pct"),
                            "volume": i1m.get("volume"),
                            "volume_sma20": i1m.get("volume_sma20"),
                        },
                        "indicadores_15m": {
                            "rsi": i15m.get("rsi"),
                            "adx": i15m.get("adx"),
                            "macd_hist": i15m.get("macd_hist"),
                            "atr": i15m.get("atr"),
                            "ema200_15m": i15m.get("ema200_15m"),
                            "volume": i15m.get("volume"),
                            "volume_sma20": i15m.get("volume_sma20"),
                            "recent_high": i15m.get("recent_high"),
                            "recent_low": i15m.get("recent_low"),
                        },
                        "indicadores_4h": {
                            "ema200_4h": i4h.get("ema200_4h"),
                            "close": i4h.get("close"),
                        },
                        "estado": {
                            "estado": st.estado if st else "UNKNOWN",
                            "direccion": st.direccion_filtro if st else None,
                            "score": st.ultimo_score if st else 0,
                            "filtro_aprobado": st.filtro_macro_aprobado if st else False,
                            "velas_confirmacion": st.velas_confirmacion if st else 0,
                            "circuit_breaker_activo": getattr(st, 'circuit_breaker_activo', False) if st else False,
                            "moneda_pausada": getattr(st, 'moneda_pausada', False) if st else False,
                            "moneda_pausada_manual": getattr(st, 'moneda_pausada_manual', False) if st else False,
                        }
                    }
                }
                try:
                    await manager.broadcast_json(payload_consolidado)
                except Exception as e:
                    logger.error(f"[BROADCAST] Error enviando snapshot {symbol}: {e}")
                    logger.debug(f"[BROADCAST] Payload problemático: {safe_json_dumps(payload_consolidado)[:500]}")

            elapsed = time.time() - t0
            sleep_time = max(0, 1.0 - elapsed)
            await asyncio.sleep(sleep_time)

    await asyncio.gather(escuchar_alertas(), empujar_telemetria())


# -------------------------------------------------------------------------------
# V6.1: ENDPOINTS ANALÍTICOS (Adaptados y alineados a grid_ejecuciones REAL)
# -------------------------------------------------------------------------------

@app.get("/api/stats/extended")
async def get_stats_extended(request: Request):
    """KPIs globales agregados de toda la base de datos. V6.2: Alineado a grid_ejecuciones."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")
        ts_inicio_dia = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?", (hoy_local,))
        fires_hoy = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ?", (hoy_local,))
        rechazados_hoy = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'ARMED' AND fecha = ?", (hoy_local,))
        armed_hoy = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ?", (ts_inicio_dia,))
        nm_total = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL", (ts_inicio_dia,))
        nm_finalizados = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND acerto_bot = 1", (ts_inicio_dia,))
        nm_acertados = (await cursor.fetchone())[0]

        # V6.2 FIX: Leer desde grid_ejecuciones (real), no grid_estados (legacy)
        cursor = await db.execute(
            "SELECT count(*) FROM grid_ejecuciones WHERE date(datetime(timestamp_inicio, 'unixepoch')) = ?",
            (hoy_local,)
        )
        grids_total = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COALESCE(SUM(pnl_real), 0), COALESCE(SUM(fees_real), 0) FROM grid_ejecuciones WHERE date(datetime(timestamp_inicio, 'unixepoch')) = ?",
            (hoy_local,)
        )
        row = await cursor.fetchone()
        pnl_total = row[0] or 0
        fees_total = row[1] or 0
        ks_total = 0  # TODO: agregar campo trades_kill_switch a grid_ejecuciones si se necesita

        cursor = await db.execute("SELECT AVG(score) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?", (hoy_local,))
        avg_score_fire = (await cursor.fetchone())[0]

        return {
            "estado": "success",
            "hoy": hoy_local,
            "fires_hoy": fires_hoy,
            "rechazados_hoy": rechazados_hoy,
            "armed_hoy": armed_hoy,
            "near_miss_total": nm_total,
            "near_miss_finalizados": nm_finalizados,
            "near_miss_acertados": nm_acertados,
            "near_miss_win_rate": round((nm_acertados / nm_finalizados * 100), 1) if nm_finalizados > 0 else None,
            "grids_total": grids_total,
            "pnl_total": round(pnl_total, 4),
            "fees_total": round(fees_total, 4),
            "kill_switches_total": ks_total,
            "avg_score_fire": round(avg_score_fire, 1) if avg_score_fire else None,
        }
    except Exception as e:
        logger.error(f"Error en stats extended: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/auditoria")
async def get_auditoria(request: Request, symbol: str = None, fecha_inicio: str = None, fecha_fin: str = None, tipos: str = None):
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
            symbol = _sanitize_symbol(symbol)
            where_clauses.append("symbol = ?")
            params.append(symbol)
        if fecha_inicio and fecha_fin:
            where_clauses.append("fecha BETWEEN ? AND ?")
            params.append(fecha_inicio)
            params.append(fecha_fin)
        elif fecha_inicio:
            where_clauses.append("fecha = ?")
            params.append(fecha_inicio)

        if tipos:
            tipo_list = [t.strip() for t in tipos.split(',')]
            placeholders = ','.join('?' * len(tipo_list))
            where_clauses.append(f"tipo IN ({placeholders})")
            params.extend(tipo_list)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM auditoria_eventos {where_sql} ORDER BY timestamp_utc DESC LIMIT 500"
        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return {"estado": "success", "count": len(result), "data": result}
    except Exception as e:
        return {"estado": "failed", "error": str(e)}


@app.get("/api/near-misses")
async def get_near_misses(request: Request, symbol: str = None, fecha_inicio: str = None, fecha_fin: str = None, estado: str = "todos"):
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
            symbol = _sanitize_symbol(symbol)
            where_clauses.append("symbol = ?")
            params.append(symbol)

        if fecha_inicio:
            try:
                dt_ini = datetime.strptime(fecha_inicio, "%Y-%m-%d")
                dt_ini = pytz.timezone(CONFIG.timezone).localize(dt_ini)
                ts_ini = dt_ini.astimezone(pytz.UTC).timestamp()
                where_clauses.append("timestamp_inicio >= ?")
                params.append(ts_ini)
            except:
                pass

        if fecha_fin:
            try:
                dt_fin = datetime.strptime(fecha_fin, "%Y-%m-%d") + timedelta(days=1)
                dt_fin = pytz.timezone(CONFIG.timezone).localize(dt_fin)
                ts_fin = dt_fin.astimezone(pytz.UTC).timestamp()
                where_clauses.append("timestamp_inicio < ?")
                params.append(ts_fin)
            except:
                pass

        if estado == "finalizados":
            where_clauses.append("timestamp_fin IS NOT NULL")
        elif estado == "en_curso":
            where_clauses.append("timestamp_fin IS NULL")

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM near_miss_seguimientos {where_sql} ORDER BY timestamp_inicio DESC LIMIT 500"
        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return {"estado": "success", "count": len(result), "data": result}
    except Exception as e:
        return {"estado": "failed", "error": str(e)}


# -------------------------------------------------------------------------------
# V6.2: /api/grids MIGRADO A grid_ejecuciones (REAL) con compatibilidad legacy
# -------------------------------------------------------------------------------

@app.get("/api/grids")
async def get_grids(request: Request, symbol: str = None, estado_grid: str = None):
    """Grid ejecuciones reales + PnL agregado. Legacy grid_estados disponible vía /api/grids/legacy."""
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
            symbol = _sanitize_symbol(symbol)
            where_clauses.append("g.symbol = ?")
            params.append(symbol)
        if estado_grid and estado_grid != "todos":
            where_clauses.append("g.estado = ?")
            params.append(estado_grid)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT 
                g.id AS grid_id, g.symbol, g.timestamp_inicio, g.timestamp_fin,
                g.estado, g.direction, g.precio_entrada, g.grid_params_json,
                g.pnl_real, g.fees_real, g.razon_cierre, g.trading_mode,
                COALESCE(SUM(p.realized_pnl), 0) as pnl_verificado,
                COUNT(p.id) as total_fills
            FROM grid_ejecuciones g
            LEFT JOIN pnl_eventos p ON g.id = p.grid_ejecucion_id
            {where_sql}
            GROUP BY g.id
            ORDER BY g.timestamp_inicio DESC
            LIMIT 500
        """
        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return {"estado": "success", "count": len(result), "data": result, "fuente": "grid_ejecuciones (real)"}
    except Exception as e:
        logger.error(f"Error grids: {e}")
        return {"estado": "failed", "error": str(e)}


@app.get("/api/grids/legacy")
async def get_grids_legacy(request: Request, symbol: str = None, estado_grid: str = None):
    """Endpoint legacy para grid_estados + grid_simulaciones (solo referencia)."""
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
            symbol = _sanitize_symbol(symbol)
            where_clauses.append("g.symbol = ?")
            params.append(symbol)
        if estado_grid and estado_grid != "todos":
            where_clauses.append("g.estado = ?")
            params.append(estado_grid)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT 
                g.id AS grid_id, g.symbol, g.timestamp_inicio, g.timestamp_fin,
                g.estado AS grid_estado, g.direccion, g.precio_entrada, g.grid_params_json,
                s.id AS sim_id, s.precio_inicio, s.precio_fin, s.pnl_bruto,
                s.pnl_neto, s.fees_totales, s.slippage_total, s.trades_completados,
                s.trades_kill_switch, s.posiciones_abiertas_json, s.posiciones_atrapadas_json,
                s.estado AS sim_estado
            FROM grid_estados g
            LEFT JOIN grid_simulaciones s ON g.id = s.grid_id
            {where_sql}
            ORDER BY g.timestamp_inicio DESC
            LIMIT 500
        """
        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        return {"estado": "success", "count": len(result), "data": result, "fuente": "grid_estados (legacy)"}
    except Exception as e:
        return {"estado": "failed", "error": str(e)}


@app.get("/api/velas/range")
async def get_velas_range(symbol: str, request: Request, tf: str = "1m", t_ini_ms: int = 0, t_fin_ms: int = 0):
    """Velas en un rango de tiempo para graficar trayectorias. FIX: datetime correcto."""
    try:
        db = await _get_db()
        tabla = f"velas_{tf}"
        cursor = await db.execute(
            f"SELECT timestamp, open, high, low, close, volume FROM {tabla} WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
            (symbol, t_ini_ms, t_fin_ms)
        )
        rows = await cursor.fetchall()
        records = []
        for row in rows:
            records.append({
                "timestamp": row[0], "open": row[1], "high": row[2],
                "low": row[3], "close": row[4], "volume": row[5]
            })
        return {"velas": records, "symbol": symbol, "tf": tf, "count": len(records)}
    except Exception as e:
        return {"estado": "failed", "error": str(e), "velas": []}
