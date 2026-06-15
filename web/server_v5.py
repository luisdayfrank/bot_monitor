import asyncio
import json
import logging
import numpy as np

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

import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from database_v5 import guardar_alerta
from notifier import Notifier
from config import CONFIG

app = FastAPI(title="Crypto Monitor V5.1 — Multi-Timeframe Sniper Dashboard")

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
notifier = Notifier()


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
            # V4.2: Exponer estado de pausa
            "moneda_pausada": getattr(s, 'moneda_pausada', False),
            "moneda_pausada_manual": getattr(s, 'moneda_pausada_manual', False),
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
            # V4.2: Exponer estado de pausa
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
            # V4.2: Estado de pausa
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

    result = {}
    for symbol in CONFIG.symbols:
        st = estados.get(symbol)
        i1m = indicadores_1m.get(symbol, {})
        i15m = indicadores_15m.get(symbol, {})
        i4h = indicadores_4h.get(symbol, {})

        result[symbol] = {
            "precio": precios.get(symbol),
            "estado": {
                "estado": st.estado if st else "UNKNOWN",
                "direccion_filtro": st.direccion_filtro if st else None,
                "score": st.ultimo_score if st else 0,
                "filtro_aprobado": st.filtro_macro_aprobado if st else False,
                "velas_confirmacion": st.velas_confirmacion if st else 0,
                "circuit_breaker_activo": getattr(st, 'circuit_breaker_activo', False) if st else False,
                # V4.2: Estado de pausa
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
        "symbols_count": len(result),
        "timestamp": int(time.time() * 1000)
    })


# ═══════════════════════════════════════════════════════════════════════════════
# V4.2: NUEVOS ENDPOINTS REST PARA PAUSA MANUAL
# ═══════════════════════════════════════════════════════════════════════════════

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


async def orquestador_eventos(queue_eventos: asyncio.Queue, precios_vivo: dict,
                               indicadores_1m: dict, indicadores_15m: dict,
                               indicadores_4h: dict, signal_states: dict):
    async def escuchar_alertas():
        while True:
            evento = await queue_eventos.get()
            try:
                await guardar_alerta(
                    symbol=evento['symbol'], tipo=evento['tipo'], direccion=evento['direction'],
                    score=evento['score'], mensaje=str(evento.get('rechazos', [])),
                    params_json=safe_json_dumps(evento.get('params', {})), precio=evento['price']
                )
            except Exception as e:
                logger.warning(f"Error guardando alerta: {e}")

            try:
                await notifier.procesar_alerta(evento)
            except Exception as e:
                logger.warning(f"Error notificando alerta: {e}")

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
                            # V4.2: Estado de pausa en broadcast
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
