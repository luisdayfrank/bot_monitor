import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Optional
from collections import deque
import websockets
import pandas as pd
from binance.client import Client

from config import CONFIG
from database_v5 import insertar_vela, insertar_velas_batch, actualizar_precio_vivo, actualizar_precios_vivo_batch


def extraer_precio_combinado(data: dict) -> tuple[Optional[float], Optional[str]]:
    """Parseo robusto para combined streams de Binance Futures"""
    if not isinstance(data, dict):
        return None, None

    payload = data.get('data', data)
    symbol = payload.get('s', '')

    if not symbol and 'stream' in data:
        symbol = data['stream'].split('@')[0].upper()

    if not symbol:
        return None, None

    precio_str = payload.get('p')
    if not precio_str and 'k' in payload:
        precio_str = payload['k'].get('c')

    if precio_str:
        try:
            precio = float(precio_str)
            if precio > 0:
                return precio, symbol.upper()
        except ValueError:
            pass

    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# F13a.1/F13a.2 (plan v7) — microestructura: funding y open interest
# ═══════════════════════════════════════════════════════════════════════════════
def calcular_funding_features(last_funding_rate, next_funding_time_ms, ahora_ms):
    """Devuelve (funding_rate_8h, funding_rate_1h_interp) en % (decimal->%).

    F13a.1: `funding_rate_1h_interp` = devengo desde el ultimo settlement:
        rate_8h * horas_desde_ultimo / 8   (acotado a [0, 8] horas)
    Binance entrega `lastFundingRate` en decimal; se convierte a %.
    """
    if last_funding_rate is None:
        return None, None
    try:
        rate = float(last_funding_rate) * 100.0
        ahora_ms = int(ahora_ms)
    except (TypeError, ValueError):
        return None, None
    try:
        next_ms = int(next_funding_time_ms)
    except (TypeError, ValueError):
        return round(rate, 6), None
    if next_ms <= 0:
        # Sin nextFundingTime no se puede devengar; el rate 8h sigue siendo valido
        return round(rate, 6), None
    horas_hasta = (next_ms - ahora_ms) / 3600_000.0
    horas_hasta = max(0.0, min(8.0, horas_hasta))
    horas_desde = 8.0 - horas_hasta
    return round(rate, 6), round(rate * (horas_desde / 8.0), 6)


def calcular_oi_delta_pct(hist, ahora_ms, ventana_ms, tolerancia_ms=5 * 60_000):
    """F13a.2: % de cambio de OI contra la muestra <= (ahora - ventana).

    `hist` es un deque de (timestamp_ms, open_interest) ordenado. Si no hay
    muestra suficientemente antigua (o el hueco supera la tolerancia), None.
    """
    if len(hist) < 2:
        return None
    objetivo = int(ahora_ms) - int(ventana_ms)
    candidatos = [x for x in hist if x[0] <= objetivo]
    if not candidatos:
        return None
    ts_prev, oi_prev = candidatos[-1]
    ts_actual, oi_actual = hist[-1]
    if ts_actual <= ts_prev or objetivo - ts_prev > tolerancia_ms or oi_prev <= 0:
        return None
    return round((oi_actual - oi_prev) / oi_prev * 100.0, 4)


class DataCollector:
    def __init__(self, queue_velas: asyncio.Queue, precios_vivo: Dict[str, float]):
        self.queue = queue_velas
        self.precios_vivo = precios_vivo
        self.client = Client(CONFIG.binance_api_key, CONFIG.binance_api_secret)

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 2: Buffers como deque nativo en vez de pd.DataFrame
        # Mucho más eficiente para append/tail. Solo convertimos a DataFrame
        # cuando el engine lo necesita (cold start o reconstrucción).
        # ═══════════════════════════════════════════════════════════════════════════════
        self._buffers_1m_raw: Dict[str, deque] = {s: deque(maxlen=CONFIG.max_velas_1m) for s in CONFIG.symbols}
        self._buffers_15m_raw: Dict[str, deque] = {s: deque(maxlen=CONFIG.max_velas_15m) for s in CONFIG.symbols}
        self._buffers_4h_raw: Dict[str, deque] = {s: deque(maxlen=CONFIG.max_velas_4h) for s in CONFIG.symbols}

        # DataFrames expuestos al engine (lazy: se reconstruyen solo cuando se necesitan)
        self.buffers_1m: Dict[str, pd.DataFrame] = {}
        self.buffers_15m: Dict[str, pd.DataFrame] = {}
        self.buffers_4h: Dict[str, pd.DataFrame] = {}

        self._shutdown = asyncio.Event()
        self._primer_precio_recibido = False

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 2: Deduplicación robusta con contador de secuencia
        # En vez de solo timestamp, guardamos (timestamp, close) para detectar
        # duplicados reales incluso si el timestamp coincide.
        # ═══════════════════════════════════════════════════════════════════════════════
        self._ultimas_velas_procesadas: Dict[str, Dict[str, tuple]] = {
            s: {'1m': (0, 0.0), '15m': (0, 0.0), '4h': (0, 0.0)} for s in CONFIG.symbols
        }

        # ═══════════════════════════════════════════════════════════════════════════════
        # F1.1 + F1.6: Buffer batch para precios vivo (elimina asfixia de I/O y memory leak)
        # Reemplaza el semáforo + create_task por acumulación en RAM con flush periódico.
        # ═══════════════════════════════════════════════════════════════════════════════
        self._precio_buffer: Dict[str, float] = {}  # symbol -> precio más reciente
        self._precio_buffer_lock = asyncio.Lock()
        self._precio_flush_event = asyncio.Event()
        self._precio_flush_task: Optional[asyncio.Task] = None
        self._precio_buffer_max_size = 100
        self._precio_flush_interval = 5.0  # segundos

        # F13a.1/F13a.2: microestructura (funding 5 min / OI 1 min)
        self.micro_features: Dict[str, dict] = {
            s: {'funding_rate_8h': None, 'funding_rate_1h_interp': None,
                'oi_delta_5m_pct': None, 'oi_delta_1h_pct': None}
            for s in CONFIG.symbols
        }
        self._oi_hist: Dict[str, deque] = {
            s: deque(maxlen=70) for s in CONFIG.symbols
        }
        self._micro_tasks: list = []

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.1: Helpers del buffer batch de precios
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _buffer_precio(self, symbol: str, precio: float):
        """Acumula precio en buffer RAM. El flush periódico persiste en MySQL/MariaDB."""
        if not CONFIG.guardar_precios_vivo:
            return
        async with self._precio_buffer_lock:
            self._precio_buffer[symbol] = precio
            if len(self._precio_buffer) >= self._precio_buffer_max_size:
                self._precio_flush_event.set()

    async def _flush_precios_loop(self):
        """Loop background que flushea precios cada N segundos o cuando el buffer está lleno."""
        while not self._shutdown.is_set():
            try:
                await asyncio.wait_for(
                    self._precio_flush_event.wait(),
                    timeout=self._precio_flush_interval
                )
            except asyncio.TimeoutError:
                pass

            self._precio_flush_event.clear()
            await self._flush_precios_buffer()

        # Flush final al salir
        await self._flush_precios_buffer()

    async def _flush_precios_buffer(self):
        """Persiste el buffer de precios en MySQL/MariaDB de forma batch (reduce I/O 95%)."""
        async with self._precio_buffer_lock:
            if not self._precio_buffer:
                return
            buffer_copy = dict(self._precio_buffer)
            self._precio_buffer.clear()

        try:
            await actualizar_precios_vivo_batch(buffer_copy)
        except Exception as e:
            print(f"  ⚠️ Error en flush batch de precios: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 2: Helpers para conversión lazy deque → DataFrame
    # ═══════════════════════════════════════════════════════════════════════════════
    def _rebuild_df(self, symbol: str, tf: str) -> pd.DataFrame:
        """Reconstruye DataFrame desde deque raw. Llamado solo cuando el engine lo necesita."""
        raw_map = {
            '1m': self._buffers_1m_raw,
            '15m': self._buffers_15m_raw,
            '4h': self._buffers_4h_raw,
        }
        buf = raw_map.get(tf, {}).get(symbol)
        if not buf:
            return pd.DataFrame()
        return pd.DataFrame(list(buf))

    def _sync_buffers_to_engine(self):
        """Sincroniza todos los buffers raw a DataFrames para el engine.
        Llamado una vez al final del cold start y opcionalmente en reconexión."""
        for symbol in CONFIG.symbols:
            self.buffers_1m[symbol] = self._rebuild_df(symbol, '1m')
            self.buffers_15m[symbol] = self._rebuild_df(symbol, '15m')
            self.buffers_4h[symbol] = self._rebuild_df(symbol, '4h')

    async def cold_start(self):
        print("🧊 COLD START: Descargando histórico REST...")

        for symbol in CONFIG.symbols:
            # ─── 1M ───
            try:
                klines = await asyncio.to_thread(
                    self.client.get_historical_klines,
                    symbol, Client.KLINE_INTERVAL_1MINUTE,
                    f"{CONFIG.max_velas_1m} minutes ago UTC"
                )
                velas = []
                for k in klines:
                    vela = {
                        'timestamp': k[0], 'open': float(k[1]), 'high': float(k[2]),
                        'low': float(k[3]), 'close': float(k[4]), 'volume': float(k[5])
                    }
                    velas.append(vela)
                    self._buffers_1m_raw[symbol].append(vela)

                # Batch insert: todas las velas en una sola transacción
                await insertar_velas_batch(symbol, '1m', velas)

                # FASE 2: Deduplicación con (timestamp, close)
                if velas:
                    last = velas[-1]
                    self._ultimas_velas_procesadas[symbol]['1m'] = (int(last['timestamp']), float(last['close']))
                    await self.queue.put(('1m', symbol, last))
                    print(f"  ✅ {symbol} 1M: {len(velas)} velas")
                    print(f"  📥 {symbol} 1M: última vela inyectada")

            except Exception as e:
                print(f"  ❌ {symbol} 1M error: {e}")

            await asyncio.sleep(0.5)

            # ─── 15M ───
            try:
                klines = await asyncio.to_thread(
                    self.client.get_historical_klines,
                    symbol, Client.KLINE_INTERVAL_15MINUTE,
                    f"{CONFIG.max_velas_15m * 15} minutes ago UTC"
                )
                velas = []
                for k in klines:
                    vela = {
                        'timestamp': k[0], 'open': float(k[1]), 'high': float(k[2]),
                        'low': float(k[3]), 'close': float(k[4]), 'volume': float(k[5])
                    }
                    velas.append(vela)
                    self._buffers_15m_raw[symbol].append(vela)

                # Batch insert: todas las velas en una sola transacción
                await insertar_velas_batch(symbol, '15m', velas)

                if velas:
                    last = velas[-1]
                    self._ultimas_velas_procesadas[symbol]['15m'] = (int(last['timestamp']), float(last['close']))
                    await self.queue.put(('15m', symbol, last))
                    print(f"  ✅ {symbol} 15M: {len(velas)} velas")
                    print(f"  📥 {symbol} 15M: última vela inyectada")

            except Exception as e:
                print(f"  ❌ {symbol} 15M error: {e}")

            await asyncio.sleep(0.5)

            # ─── 4H ───
            try:
                klines = await asyncio.to_thread(
                    self.client.get_historical_klines,
                    symbol, Client.KLINE_INTERVAL_4HOUR,
                    f"{CONFIG.max_velas_4h * 4} hours ago UTC"
                )
                velas = []
                for k in klines:
                    vela = {
                        'timestamp': k[0], 'open': float(k[1]), 'high': float(k[2]),
                        'low': float(k[3]), 'close': float(k[4]), 'volume': float(k[5])
                    }
                    velas.append(vela)
                    self._buffers_4h_raw[symbol].append(vela)

                # Batch insert: todas las velas en una sola transacción
                await insertar_velas_batch(symbol, '4h', velas)

                if velas:
                    last = velas[-1]
                    self._ultimas_velas_procesadas[symbol]['4h'] = (int(last['timestamp']), float(last['close']))
                    await self.queue.put(('4h', symbol, last))
                    print(f"  ✅ {symbol} 4H: {len(velas)} velas")
                    print(f"  📥 {symbol} 4H: última vela inyectada")

            except Exception as e:
                print(f"  ❌ {symbol} 4H error: {e}")

            # Precio spot
            try:
                ticker = self.client.futures_symbol_ticker(symbol=symbol)
                precio_spot = float(ticker['price'])
                self.precios_vivo[symbol] = precio_spot
                # F1.1: Usar buffer en lugar de write directo a DB
                await self._buffer_precio(symbol, precio_spot)
                print(f"  💰 {symbol} Precio REST: ${precio_spot:.4f}")
            except Exception as e:
                print(f"  ⚠️ {symbol} Precio REST error: {e}")

            await asyncio.sleep(0.5)

        # FASE 2: Sincronizar buffers raw → DataFrames para el engine
        self._sync_buffers_to_engine()
        print("🧊 Cold Start completado.")

    # ── F13a.1/F13a.2: polling de microestructura ─────────────────────────
    async def _poll_funding_rate(self):
        """F13a.1: funding rate cada 5 min (todas las monedas)."""
        ahora_ms = int(time.time() * 1000)
        for symbol in CONFIG.symbols:
            try:
                info = await asyncio.to_thread(
                    self.client.futures_mark_price, symbol=symbol)
                rate, interp = calcular_funding_features(
                    info.get('lastFundingRate'),
                    info.get('nextFundingTime'), ahora_ms)
                if rate is None:
                    continue
                self.micro_features[symbol]['funding_rate_8h'] = rate
                self.micro_features[symbol]['funding_rate_1h_interp'] = interp
            except Exception as e:
                print(f"  ⚠️ [F13a] funding {symbol}: {e}")

    async def _poll_open_interest(self):
        """F13a.2: open interest cada 1 min (todas las monedas)."""
        ahora_ms = int(time.time() * 1000)
        for symbol in CONFIG.symbols:
            try:
                info = await asyncio.to_thread(
                    self.client.futures_open_interest, symbol=symbol)
                oi = float(info.get('openInterest') or 0)
                if oi <= 0:
                    continue
                hist = self._oi_hist[symbol]
                hist.append((ahora_ms, oi))
                self.micro_features[symbol]['oi_delta_5m_pct'] = (
                    calcular_oi_delta_pct(hist, ahora_ms, 5 * 60_000))
                self.micro_features[symbol]['oi_delta_1h_pct'] = (
                    calcular_oi_delta_pct(hist, ahora_ms, 60 * 60_000))
            except Exception as e:
                print(f"  ⚠️ [F13a] OI {symbol}: {e}")

    async def _loop_funding(self):
        while not self._shutdown.is_set():
            t0 = time.perf_counter()
            await self._poll_funding_rate()
            dur_ms = (time.perf_counter() - t0) * 1000.0
            if dur_ms > 1000:
                print(f"  ⚠️ [F13a] funding poll lento: {dur_ms:.0f}ms")
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass

    async def _loop_open_interest(self):
        while not self._shutdown.is_set():
            t0 = time.perf_counter()
            await self._poll_open_interest()
            dur_ms = (time.perf_counter() - t0) * 1000.0
            if dur_ms > 1000:
                print(f"  ⚠️ [F13a] OI poll lento: {dur_ms:.0f}ms")
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    def build_ws_url(self) -> str:
        streams = []
        for s in CONFIG.symbols:
            s_low = s.lower()
            streams.append(f"{s_low}@kline_1m")
            streams.append(f"{s_low}@kline_15m")
            streams.append(f"{s_low}@kline_4h")
            streams.append(f"{s_low}@markPrice@1s")
        return f"wss://fstream.binance.com/market/stream?streams={'/'.join(streams)}"

    async def run(self):
        url = self.build_ws_url()
        intento = 0

        # F1.1: Iniciar loop de flush de precios en background
        self._precio_flush_task = asyncio.create_task(self._flush_precios_loop())

        # F13a: polling de microestructura (no bloquea el WS)
        self._micro_tasks = [
            asyncio.create_task(self._loop_funding()),
            asyncio.create_task(self._loop_open_interest()),
        ]

        while not self._shutdown.is_set():
            print(f"📡 Conectando a: {url[:90]}...")

            try:
                async with websockets.connect(
                    url, 
                    ping_interval=20, 
                    ping_timeout=10, 
                    close_timeout=5
                ) as ws:
                    print(f"✅ WS conectado ({len(CONFIG.symbols)} monedas × 4 streams = {len(CONFIG.symbols)*4} total)")

                    ws_start_time = time.time()
                    REFRESH_INTERVAL = 43200  # 12 horas

                    ultimo_msg = time.time()
                    intento = 0
                    msg_count = 0
                    klines_recibidas = 0
                    markprice_recibidos = 0

                    while not self._shutdown.is_set():
                        try:
                            if time.time() - ws_start_time > REFRESH_INTERVAL:
                                print("⏰ Refresh programado de 12h. Cerrando WS limpio...")
                                break

                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            ultimo_msg = time.time()
                            data = json.loads(msg)
                            msg_count += 1

                            if msg_count <= 3:
                                stream_name = data.get('stream', 'UNKNOWN')
                                has_data = 'data' in data
                                data_keys = list(data.get('data', {}).keys())[:5]
                                print(f"  📨 MSG #{msg_count} | Stream: {stream_name} | Keys: {data_keys}")
                            elif msg_count == 4:
                                print(f"  📨 ... ({klines_recibidas} klines, {markprice_recibidos} markPrices hasta ahora)")

                            # 1. Precio en vivo (markPrice@1s)
                            precio, symbol_parsed = extraer_precio_combinado(data)
                            if precio and symbol_parsed:
                                self.precios_vivo[symbol_parsed] = precio
                                # F1.1 + F1.6: Buffer en RAM (no create_task ni write directo)
                                await self._buffer_precio(symbol_parsed, precio)
                                markprice_recibidos += 1
                                if not self._primer_precio_recibido:
                                    print(f"  🎯 PRIMER PRECIO WS: {symbol_parsed} @ ${precio:.4f}")
                                    self._primer_precio_recibido = True

                            # 2. Velas cerradas (kline con x == True)
                            payload = data.get('data', data)
                            stream = data.get('stream', '')

                            if not payload.get('k', {}).get('x'):
                                continue

                            k = payload['k']
                            symbol = k['s'].upper()
                            tf = None

                            if '@kline_1m' in stream:
                                tf = '1m'
                            elif '@kline_15m' in stream:
                                tf = '15m'
                            elif '@kline_4h' in stream:
                                tf = '4h'
                            else:
                                continue

                            timestamp = int(k['t'])
                            close_val = float(k['c'])
                            klines_recibidas += 1

                            # ═══════════════════════════════════════════════════════════════════════════════
                            # FASE 2: Deduplicación robusta con (timestamp, close)
                            # Si timestamp es igual pero close diferente → vela diferente (raro pero posible)
                            # Si ambos coinciden → duplicado real
                            # ═══════════════════════════════════════════════════════════════════════════════
                            ultima_ts, ultima_close = self._ultimas_velas_procesadas.get(symbol, {}).get(tf, (0, 0.0))
                            if timestamp == ultima_ts and abs(close_val - ultima_close) < 0.0001:
                                continue  # Duplicado exacto
                            if timestamp < ultima_ts:
                                continue  # Vela antigua (desordenada)

                            self._ultimas_velas_procesadas[symbol][tf] = (timestamp, close_val)

                            vela = {
                                'timestamp': timestamp,
                                'open': float(k['o']),
                                'high': float(k['h']),
                                'low': float(k['l']),
                                'close': close_val,
                                'volume': float(k['v'])
                            }

                            # FASE 2: Append a deque raw (O(1) amortizado) + lazy sync a DataFrame
                            if tf == '1m':
                                self._buffers_1m_raw[symbol].append(vela)
                                self.buffers_1m[symbol] = self._rebuild_df(symbol, '1m')
                                await insertar_vela(symbol, '1m', vela)
                                await self.queue.put(('1m', symbol, vela))
                                print(f"  🕯️ Vela 1M: {symbol} C=${vela['close']:.4f} H=${vela['high']:.4f} L=${vela['low']:.4f}")

                            elif tf == '15m':
                                self._buffers_15m_raw[symbol].append(vela)
                                self.buffers_15m[symbol] = self._rebuild_df(symbol, '15m')
                                await insertar_vela(symbol, '15m', vela)
                                await self.queue.put(('15m', symbol, vela))
                                print(f"  🕯️ Vela 15M: {symbol} C=${vela['close']:.4f}")

                            elif tf == '4h':
                                self._buffers_4h_raw[symbol].append(vela)
                                self.buffers_4h[symbol] = self._rebuild_df(symbol, '4h')
                                await insertar_vela(symbol, '4h', vela)
                                await self.queue.put(('4h', symbol, vela))
                                print(f"  🕯️ Vela 4H: {symbol} C=${vela['close']:.4f}")

                        except asyncio.TimeoutError:
                            tiempo_sin_mensaje = time.time() - ultimo_msg
                            if tiempo_sin_mensaje > 45:
                                print(f"⏱️ Silencio {tiempo_sin_mensaje:.0f}s. Reconectando...")
                                break
                            continue

            except Exception as e:
                intento += 1
                delay = min(5 * (2 ** min(intento, 5)), 120)
                print(f"⚠️ WS error: {type(e).__name__}: {e}")
                print(f"   Reconectando en {delay}s... (intento #{intento})")
                await asyncio.sleep(delay)

            if time.time() - ultimo_msg > 900:
                print("🧊 Hueco largo de datos. Re-hidratando...")
                await self.cold_start()
                # Re-sincronizar buffers raw → DataFrames para el engine
                self._sync_buffers_to_engine()
                print("🧊 Buffers re-sincronizados tras re-hidratación")

    def stop(self):
        self._shutdown.set()
        # F1.1: Cancelar flush loop de forma segura
        if self._precio_flush_task and not self._precio_flush_task.done():
            self._precio_flush_task.cancel()
        # F13a: cancelar polling de microestructura
        for t in getattr(self, '_micro_tasks', []):
            if t and not t.done():
                t.cancel()
