import asyncio
import json
import logging
import numpy as np
import os
import datetime
import time
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database_v5 import guardar_alerta, _get_db, now_local, get_tz
from notifier_v5 import Notifier
from config import CONFIG

REGISTRY_PATH = "coins_registry.json"

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

app = FastAPI(title="Crypto Monitor V5.7 — Multi-Timeframe Sniper Dashboard")

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
# FASE 5 FIX: El notifier se inyecta desde main.py vía app.state
# notifier = Notifier()  ? ELIMINADO (era instancia huérfana sin signal_generator)


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
    precios = getattr(request.app.state, 'precios_vivo', {})
    return {
        "symbol": symbol,
        "precio": round(precios.get(symbol), 4) if symbol in precios else None,
        "timestamp": int(time.time() * 1000)
    }


@app.get("/api/indicadores/{symbol}")
async def get_indicadores(symbol: str, request: Request):
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
            "direccion_filtro": estado.direccion_filtro if estado else None,
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
# V5.7: ENDPOINT DE ESTADÍSTICAS DESDE LA BASE DE DATOS
# -------------------------------------------------------------------------------

@app.get("/api/stats/summary")
async def get_stats_summary(request: Request):
    """Obtiene un resumen de rendimiento usando la base de datos de auditoría.

    V5.7 FIX: Win Rate calculado SOLO sobre seguimientos FINALIZADOS del día,
    no sobre el total histórico. Esto evita que seguimientos en curso (2h)
    distorsionen la métrica mostrando 0% win rate falsamente.
    """
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")

        # 1. Conteo de alertas FIRE de hoy (usando columna fecha en hora local)
        cursor = await db.execute(
            "SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?",
            (hoy_local,)
        )
        fires_hoy = (await cursor.fetchone())[0]

        # F4.2: Usar UTC directamente para evitar desfase zona horaria
        ts_inicio_dia = datetime.datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        # 2. Conteo de Near Misses INICIADOS HOY (no toda la historia)
        cursor = await db.execute(
            "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ?",
            (ts_inicio_dia,)
        )
        total_near_miss = (await cursor.fetchone())[0]

        # 3. V5.7 FIX: Win Rate SOLO sobre seguimientos FINALIZADOS del día
        # Los seguimientos en curso (timestamp_fin IS NULL) NO se cuentan
        # como fracasos, evitando distorsión del 0% win rate
        cursor = await db.execute(
            "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL",
            (ts_inicio_dia,)
        )
        finalizados_hoy = (await cursor.fetchone())[0]

        aciertos = 0
        if finalizados_hoy > 0:
            cursor = await db.execute(
                "SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND acerto_bot = 1",
                (ts_inicio_dia,)
            )
            aciertos = (await cursor.fetchone())[0]

        # Win Rate REAL: Aciertos / Seguimientos Terminados (no total)
        win_rate = (aciertos / finalizados_hoy * 100) if finalizados_hoy > 0 else 0.0

        # Métricas adicionales para transparencia
        seguimientos_activos = total_near_miss - finalizados_hoy

        # FIX 0.3: Win Rate solo calculable cuando hay seguimientos finalizados.
        # Si no hay finalizados, mostrar "PENDIENTE" en lugar de 0.0 para evitar
        # interpretacion erronea ("el bot acierta 0%" cuando en realidad es
        # "aun no hay datos suficientes").
        if finalizados_hoy > 0:
            win_rate_val = round(win_rate, 2)
            win_rate_estado = "CALCULABLE"
        else:
            win_rate_val = None
            win_rate_estado = "PENDIENTE"

        return {
            "fires_hoy": fires_hoy,
            "total_near_miss": total_near_miss,
            "finalizados_hoy": finalizados_hoy,
            "seguimientos_activos": seguimientos_activos,
            "aciertos_bot": aciertos,
            "win_rate_rechazos": win_rate_val,
            "win_rate_estado": win_rate_estado,
            "mensaje": f"{seguimientos_activos} seguimiento(s) en curso (2h). "
                       f"Win Rate calculable cuando finalicen.",
            "estado": "success"
        }
    except Exception as e:
        logger.error(f"Error obteniendo stats: {e}")
        return {"error": str(e), "estado": "failed"}


# -------------------------------------------------------------------------------
# V4.2: NUEVOS ENDPOINTS REST PARA PAUSA MANUAL
# -------------------------------------------------------------------------------

@app.post("/api/pause/{symbol}")
async def pause_symbol(symbol: str, request: Request):
    """Pausa manualmente una moneda."""
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
    """Reanuda manualmente una moneda pausada."""
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
    """Pausa todas las monedas manualmente."""
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
    """Reanuda todas las monedas pausadas manualmente."""
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
    """Lista todas las monedas pausadas con detalles."""
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
# V5.9.2: ENDPOINT GRID NEUTRAL
# -------------------------------------------------------------------------------

@app.get("/api/grid-neutral/{symbol}")
async def get_grid_neutral(symbol: str, request: Request):
    """Retorna el estado de la simulación grid neutral para un símbolo."""
    grid_simulator = getattr(request.app.state, 'grid_simulator', None)

    if not grid_simulator:
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Grid simulator no disponible",
            "timestamp": int(time.time() * 1000)
        }

    try:
        estado = grid_simulator.get_estado_simulacion(symbol)
    except Exception as e:
        return {
            "symbol": symbol,
            "grid_activo": False,
            "mensaje": "Error interno del simulador",
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
        "sim_id": estado.get('sim_id'),
        "niveles": estado.get('niveles'),
        "posiciones_abiertas": estado.get('posiciones_abiertas'),
        "posiciones_atrapadas": estado.get('posiciones_atrapadas'),
        "posiciones_vencidas": estado.get('posiciones_vencidas'),
        "trades_completados": estado.get('trades_completados'),
        "trades_kill_switch": estado.get('trades_kill_switch'),
        "pnl_neto": estado.get('pnl_neto'),
        "pnl_bruto": estado.get('pnl_bruto'),
        "fees_totales": estado.get('fees_totales'),
        "slippage_total": estado.get('slippage_total'),
        "max_posiciones_simultaneas": estado.get('max_posiciones_simultaneas'),
        "ultimo_tick_minutos": estado.get('ultimo_tick_segundos_ago', 0) // 60,
        "timestamp": int(time.time() * 1000)
    }


# -------------------------------------------------------------------------------
# ENDPOINTS PARA COIN REGISTRY (Toggle desde dashboard)
# -------------------------------------------------------------------------------

@app.get("/api/coin-registry")
async def get_coin_registry():
    """Retorna el registro completo de monedas (activas y inactivas)."""
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
    """Activa o desactiva una moneda individual. Cambia el JSON en disco."""
    try:
        data = await request.json()
        symbol = data.get("symbol")
        active = data.get("active")

        if symbol is None or active is None:
            return {"error": "Faltan 'symbol' o 'active'", "success": False}

        registry = _load_registry()
        if symbol not in registry:
            return {"error": f"{symbol} no existe en el registro", "success": False}

        registry[symbol]["active"] = 1 if active else 0
        _save_registry(registry)

        status_text = "ACTIVADA" if active else "DESACTIVADA"
        msg_text = (
            "?? <b>Registro actualizado</b>\n"
            + f"{symbol}: {'?' if active else '?'} {status_text}\n"
            + "?? Ejecuta <code>/restart</code> para aplicar cambios."
        )
        # FASE 5 FIX: Usar notifier desde app.state (instancia real de main.py)
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
    """Activa o desactiva TODAS las monedas."""
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
            f"?? <b>Todas {status_text}{cat_msg}</b>\n"
            + f"Monedas afectadas: {len(changed)}\n"
            + "?? Ejecuta <code>/restart</code> para aplicar cambios."
        )
        # FASE 5 FIX: Usar notifier desde app.state (instancia real de main.py)
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


async def orquestador_eventos(queue_eventos: asyncio.Queue, precios_vivo: dict,
                               indicadores_1m: dict, indicadores_15m: dict,
                               indicadores_4h: dict, signal_states: dict,
                               notifier=None):
    # FASE 5 FIX: Usar el notifier pasado desde main.py (ya no hay global)
    _notifier = notifier
    async def escuchar_alertas():
        while True:
            evento = await queue_eventos.get()
            # FASE 6 FIX: Confirmar en consola que el evento llegó al orquestador
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
                print(f"  ? [ORQUESTADOR] ERROR guardando alerta DB: {e}")

            try:
                await _notifier.procesar_alerta(evento)
            except Exception as e:
                # FASE 3 FIX: Traceback completo + notificación de fallback
                logger.exception(f"ERROR notificando alerta {evento.get('tipo')} para {evento.get('symbol')}: {e}")
                print(f"  ? [ORQUESTADOR] ERROR alerta {evento.get('tipo')} {evento.get('symbol')}: {e}")
                try:
                    await _notifier.enviar_telegram(
                        f"?? <b>Error procesando alerta {evento.get('tipo')}</b> para {evento.get('symbol')}\n"
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
# V6.1: ENDPOINTS ANALÍTICOS (Adaptados de main_dashboard.py)
# -------------------------------------------------------------------------------

@app.get("/api/stats/extended")
async def get_stats_extended(request: Request):
    """KPIs globales agregados de toda la base de datos."""
    try:
        db = await _get_db()
        hoy_local = now_local().strftime("%Y-%m-%d")
        ts_inicio_dia = datetime.datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

        # Fires hoy
        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'FIRE' AND fecha = ?", (hoy_local,))
        fires_hoy = (await cursor.fetchone())[0]

        # Rechazados hoy
        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'RECHAZADO' AND fecha = ?", (hoy_local,))
        rechazados_hoy = (await cursor.fetchone())[0]

        # Armed hoy
        cursor = await db.execute("SELECT count(*) FROM auditoria_eventos WHERE tipo = 'ARMED' AND fecha = ?", (hoy_local,))
        armed_hoy = (await cursor.fetchone())[0]

        # Near-misses totales hoy
        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ?", (ts_inicio_dia,))
        nm_total = (await cursor.fetchone())[0]

        # Near-misses finalizados hoy
        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL", (ts_inicio_dia,))
        nm_finalizados = (await cursor.fetchone())[0]

        # Near-misses acertados hoy
        cursor = await db.execute("SELECT count(*) FROM near_miss_seguimientos WHERE timestamp_inicio >= ? AND timestamp_fin IS NOT NULL AND acerto_bot = 1", (ts_inicio_dia,))
        nm_acertados = (await cursor.fetchone())[0]

        # Grids totales hoy
        cursor = await db.execute("SELECT count(*) FROM grid_estados WHERE date(datetime(timestamp_inicio, 'unixepoch')) = ?", (hoy_local,))
        grids_total = (await cursor.fetchone())[0]

        # PnL acumulado grids hoy
        cursor = await db.execute("SELECT COALESCE(SUM(pnl_neto), 0), COALESCE(SUM(fees_totales), 0), COALESCE(SUM(trades_kill_switch), 0) FROM grid_simulaciones WHERE date(datetime(timestamp_inicio, 'unixepoch')) = ?", (hoy_local,))
        row = await cursor.fetchone()
        pnl_total = row[0] or 0
        fees_total = row[1] or 0
        ks_total = row[2] or 0

        # Score promedio FIRE hoy
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
    """Eventos de auditoria_eventos con filtros."""
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
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
    """Seguimientos de near_miss_seguimientos."""
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
            where_clauses.append("symbol = ?")
            params.append(symbol)

        # Convertir fechas a timestamps si se proporcionan
        if fecha_inicio:
            try:
                dt_ini = datetime.datetime.strptime(fecha_inicio, "%Y-%m-%d")
                dt_ini = pytz.timezone(CONFIG.timezone).localize(dt_ini)
                ts_ini = dt_ini.astimezone(pytz.UTC).timestamp()
                where_clauses.append("timestamp_inicio >= ?")
                params.append(ts_ini)
            except:
                pass

        if fecha_fin:
            try:
                dt_fin = datetime.datetime.strptime(fecha_fin, "%Y-%m-%d") + datetime.timedelta(days=1)
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


@app.get("/api/grids")
async def get_grids(request: Request, symbol: str = None, estado_grid: str = None):
    """Grid estados + simulaciones unidos."""
    try:
        db = await _get_db()
        params = []
        where_clauses = []

        if symbol:
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
        return {"estado": "success", "count": len(result), "data": result}
    except Exception as e:
        return {"estado": "failed", "error": str(e)}


@app.get("/api/velas/range")
async def get_velas_range(symbol: str, request: Request, tf: str = "1m", t_ini_ms: int = 0, t_fin_ms: int = 0):
    """Velas en un rango de tiempo para graficar trayectorias."""
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