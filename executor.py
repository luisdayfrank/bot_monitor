"""
executor.py — PLAN 6.3 FASES 1-4 + FIXES V7.1 + CR3 CIERRE ATÓMICO + CR2 PnL COMPLETO
Motor de ejecución real de grids en Binance Futures (Testnet / Real).
Flujo: recibe mensajes por cola, coloca órdenes LIMIT, trackea fills, maneja aborto.
Arquitectura: 100% autónomo — lógica operativa interna (PosicionReal + GridState).
GridSimulator eliminado completamente. No quedan dependencias externas.
CR3 FIX: Cierre atómico con máquina de estados y verificación de estado real en Binance.
CR2 FIX: Arquitectura de PnL completa — cada trade persiste realizedPnl en DB.
V7.1: Grid atómico — locks, anti-duplicación, validación defensiva de niveles, sanidad periódica.
FASE 1-8 FIX: LONG/SHORT son ciudadanos de primera clase — pipeline unificada de fills.
"""

import asyncio
import json
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Set
from binance.client import Client
from binance.exceptions import BinanceAPIException

from config import CONFIG
from database_v5 import (
    guardar_grid_ejecucion,
    guardar_orden_ejecucion,
    actualizar_orden_fill,
    actualizar_grid_ejecucion_cierre,
    # CR16: Tracking proactivo de fills
    guardar_fill_tracking,
    cargar_fills_sin_procesar,
    marcar_fill_procesado,
    obtener_ultimo_trade_timestamp,
    _execute_with_retry,
    _get_db,
    _db_lock,
    # CR2: Arquitectura de PnL completa
    guardar_pnl_evento,
    calcular_pnl_acumulado,
    obtener_pnl_por_tipo,
    cargar_grid_ejecuciones_activos,
    cargar_ordenes_por_grid,
)


# ═══════════════════════════════════════════════════════════════════════════════
# FASE 1 PLAN 6.3: POSICION REAL Y GRID STATE (Lógica heredada del helper)
# ═══════════════════════════════════════════════════════════════════════════════

class PosicionReal:
    """
    Representa una posición real abierta en Binance Futures.
    Hereda la lógica de SimPosicion del helper, pero añade campos
    necesarios para operar en Binance real (binance_order_id, etc.).
    """
    _contador = 0

    def __init__(self, tipo: str, nivel_precio: float, precio_ejecucion: float,
                 qty: float, fee_pagada: float, timestamp_apertura: int,
                 binance_order_id: str = None, filled_qty: float = None,
                 original_qty: float = None):
        PosicionReal._contador += 1
        self.id = f"pos_{PosicionReal._contador:03d}"
        self.tipo = tipo  # 'LONG' | 'SHORT'
        self.nivel_precio = Decimal(str(nivel_precio))
        self.precio_ejecucion = Decimal(str(precio_ejecucion))
        self.qty = Decimal(str(qty))
        self.fee_pagada = Decimal(str(fee_pagada))
        self.timestamp_apertura = timestamp_apertura
        self.binance_order_id = binance_order_id
        self.orden_cierre_id = None
        self.filled_qty = Decimal(str(filled_qty)) if filled_qty is not None else self.qty
        self.original_qty = Decimal(str(original_qty)) if original_qty is not None else self.qty
        self.estado = 'ABIERTA'
        self.pnl_cierre = Decimal('0')

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tipo": self.tipo,
            "nivel_precio": float(self.nivel_precio),
            "precio_ejecucion": float(self.precio_ejecucion),
            "qty": float(self.qty),
            "fee_pagada": float(self.fee_pagada),
            "timestamp_apertura": self.timestamp_apertura,
            "binance_order_id": self.binance_order_id,
            "orden_cierre_id": self.orden_cierre_id,
            "filled_qty": float(self.filled_qty),
            "original_qty": float(self.original_qty),
            "estado": self.estado,
            "pnl_cierre": float(self.pnl_cierre),
        }


class GridState:
    """
    Estado operativo del grid en el executor.
    Hereda la lógica de SimState del helper, adaptado para Binance real.
    """
    def __init__(self, grid_id: int, symbol: str, niveles: List[float],
                 qty_por_orden: float, fee_rate: float, precio_inicio: float,
                 timestamp_inicio: int):
        self.grid_id = grid_id
        self.symbol = symbol
        self.niveles = [Decimal(str(n)) for n in niveles]
        self.qty_por_orden = Decimal(str(qty_por_orden))
        self.fee_rate = Decimal(str(fee_rate))
        self.precio_inicio = Decimal(str(precio_inicio))
        self.timestamp_inicio = timestamp_inicio

        self.posiciones: List[PosicionReal] = []
        self.posiciones_atrapadas: List[PosicionReal] = []
        self.pnl_bruto = Decimal('0.0')
        self.pnl_neto = Decimal('0.0')
        self.fees_totales = Decimal('0.0')
        self.trades_completados = 0
        self.trades_kill_switch = 0
        self.max_posiciones_simultaneas = 0

        self.ultimo_tick_ts = timestamp_inicio
        self.activa = True

        self.niveles_buy: List[Decimal] = []
        self.niveles_sell: List[Decimal] = []
        self.precio_referencia = Decimal(str(precio_inicio))

        self.ordenes_reales: Dict[str, str] = {}
        self.ordenes_tp_pendientes: Dict[str, str] = {}

    def contar_posiciones_abiertas(self) -> int:
        return sum(1 for p in self.posiciones if p.estado == 'ABIERTA')

    def posiciones_abiertas_list(self) -> List[PosicionReal]:
        return [p for p in self.posiciones if p.estado == 'ABIERTA']

    def to_dict(self) -> dict:
        return {
            "grid_id": self.grid_id,
            "symbol": self.symbol,
            "niveles": [float(n) for n in self.niveles],
            "qty_por_orden": float(self.qty_por_orden),
            "fee_rate": float(self.fee_rate),
            "precio_inicio": float(self.precio_inicio),
            "timestamp_inicio": self.timestamp_inicio,
            "pnl_bruto": float(self.pnl_bruto),
            "pnl_neto": float(self.pnl_neto),
            "fees_totales": float(self.fees_totales),
            "trades_completados": self.trades_completados,
            "trades_kill_switch": self.trades_kill_switch,
            "max_posiciones_simultaneas": self.max_posiciones_simultaneas,
            "posiciones_abiertas": [p.to_dict() for p in self.posiciones_abiertas_list()],
            "posiciones_atrapadas": [p.to_dict() for p in self.posiciones_atrapadas],
            "activa": self.activa,
            "ultimo_tick_ts": self.ultimo_tick_ts,
            "ordenes_reales": self.ordenes_reales,
            "ordenes_tp_pendientes": self.ordenes_tp_pendientes,
        }


class GridExecutionState:
    """Estado en memoria de un grid de ejecución real."""

    def __init__(self, grid_id: int, symbol: str, direction: str,
                 capital: float, leverage: int, precio_entrada: float,
                 niveles: List[float], qty_por_orden: float):
        self.grid_id = grid_id
        self.symbol = symbol
        self.direction = direction
        self.capital = Decimal(str(capital))
        self.leverage = leverage
        self.precio_entrada = Decimal(str(precio_entrada))
        self.niveles = [Decimal(str(n)) for n in niveles]
        self.qty_por_orden = Decimal(str(qty_por_orden))

        self.ordenes: Dict[str, dict] = {}  # clientOrderId -> metadata
        self.posicion_neta = Decimal('0')
        self.pnl_real = Decimal('0')
        self.fees_real = Decimal('0')
        self.activa = True
        self.cerrando = False
        self.timestamp_inicio = time.time()

        self.pares_abiertos = []

        # ═══ FASE 2 PLAN 6.3: GridState propio del executor (autónomo) ═══
        self.grid_state = None
        self.grid_mode = direction

    def init_grid_state(self, niveles_buy: List[float] = None, niveles_sell: List[float] = None):
        """Inicializa el GridState propio del executor."""
        fee_rate = getattr(CONFIG, 'grid_neutral_sim_fee_rate', 0.0005)
        self.grid_state = GridState(
            grid_id=self.grid_id,
            symbol=self.symbol,
            niveles=[float(n) for n in self.niveles],
            qty_por_orden=float(self.qty_por_orden),
            fee_rate=fee_rate,
            precio_inicio=float(self.precio_entrada),
            timestamp_inicio=int(time.time())
        )
        if niveles_buy:
            self.grid_state.niveles_buy = [Decimal(str(n)) for n in niveles_buy]
        if niveles_sell:
            self.grid_state.niveles_sell = [Decimal(str(n)) for n in niveles_sell]
        self.grid_state.precio_referencia = Decimal(str(self.precio_entrada))


# ═══════════════════════════════════════════════════════════════════════════════
# CR3 FIX: MÁQUINA DE ESTADOS DE CIERRE ATÓMICO
# ═══════════════════════════════════════════════════════════════════════════════

class CierreState:
    """
    CR3 FIX: Máquina de estados para el proceso de cierre.
    Garantiza que cada paso se completa antes de pasar al siguiente.
    
    Estados:
        INICIADO → CANCELANDO_ORDENES → ORDENES_CANCELADAS → CERRANDO_POSICION
        → POSICION_CERRADA → VERIFICANDO → COMPLETADO
        
        En cualquier fallo: REINTENTO → o FALLIDO si se agotan intentos.
    """
    
    ESTADOS = [
        'INICIADO',
        'CANCELANDO_ORDENES',
        'ORDENES_CANCELADAS',
        'CERRANDO_POSICION',
        'POSICION_CERRADA',
        'VERIFICANDO',
        'COMPLETADO',
        'FALLIDO'
    ]
    
    def __init__(self, grid_id: int, symbol: str, razon: str, timestamp_inicio: int):
        self.grid_id = grid_id
        self.symbol = symbol
        self.razon = razon
        self.timestamp_inicio = timestamp_inicio
        self.estado = 'INICIADO'
        self.intentos = 0
        self.MAX_INTENTOS = 5
        
        # Tracking de cada paso
        self.ordenes_canceladas = False
        self.ordenes_canceladas_count = 0
        self.ordenes_fallidas = []
        
        self.posicion_cerrada = False
        self.posicion_cierre_order_id = None
        self.posicion_cierre_qty = 0.0
        self.posicion_cierre_precio = 0.0
        
        self.verificacion_ok = False
        self.posicion_final = None
        self.ordenes_restantes = None
        
        # Métricas
        self.timestamp_completado = None
        self.duracion_total = 0

    def puede_reintentar(self) -> bool:
        return self.intentos < self.MAX_INTENTOS and self.estado != 'COMPLETADO'
    
    def avanzar(self, nuevo_estado: str):
        if nuevo_estado in self.ESTADOS:
            self.estado = nuevo_estado
            print(f"  [CIERRE] {self.symbol} {self.razon} → {nuevo_estado} "
                  f"(intento {self.intentos + 1}/{self.MAX_INTENTOS})")
    
    def fallar(self, detalle: str):
        self.intentos += 1
        if self.intentos >= self.MAX_INTENTOS:
            self.estado = 'FALLIDO'
            print(f"  🚨 [CIERRE] {self.symbol} {self.razon} → FALLIDO después de "
                  f"{self.MAX_INTENTOS} intentos: {detalle}")
        else:
            print(f"  ⚠️ [CIERRE] {self.symbol} {self.razon} → Reintento "
                  f"{self.intentos}/{self.MAX_INTENTOS}: {detalle}")
    
    def completar(self):
        self.estado = 'COMPLETADO'
        self.timestamp_completado = int(time.time())
        self.duracion_total = self.timestamp_completado - self.timestamp_inicio
        print(f"  ✅ [CIERRE] {self.symbol} {self.razon} → COMPLETADO en "
              f"{self.duracion_total}s")


class GridExecutor:
    """
    Executor de grids reales en Binance Futures.
    Recibe mensajes por cola asyncio y ejecuta operaciones reales.
    FASE 3: Lógica de grid neutral 100% autónoma — sin llamadas al helper.
    V7.1: Grid atómico con locks, rollback y validación defensiva.
    """

    def __init__(self, precios_vivo: dict, signal_states: dict):
        self.precios_vivo = precios_vivo
        self.signal_states = signal_states
        self.queue: asyncio.Queue = asyncio.Queue()
        self.notifier = None  # Inyectado desde fuera si existe

        # FASE 4 PLAN 6.3: Executor 100% autónomo. Sin helper externo.

        # Cliente Binance (testnet o real)
        is_testnet = CONFIG.trading_mode == 'TESTNET'
        api_key = CONFIG.binance_testnet_api_key if is_testnet else CONFIG.binance_api_key
        api_secret = CONFIG.binance_testnet_secret if is_testnet else CONFIG.binance_api_secret
        self.client = Client(api_key, api_secret, testnet=is_testnet)

        if not api_key or not api_secret:
            print(f"  ❌ [EXECUTOR] ERROR: API Key o Secret vacíos. Define las variables de entorno.")
            print(f"     Requeridas: BINANCE_TESTNET_API_KEY + BINANCE_TESTNET_SECRET (para testnet)")
            self.client = None
        else:
            self.client = Client(api_key, api_secret, testnet=is_testnet)

        self._shutdown = asyncio.Event()
        self._rate_limiter = asyncio.Semaphore(CONFIG.trading_rate_limit_rps)
        self._exchange_info: Dict[str, dict] = {}
        self._grids: Dict[str, GridExecutionState] = {}  # symbol -> state
        self._symbol_leverage: Dict[str, int] = {}  # Leverage fijado por símbolo
        # V7.1 FASE 1: Lock atómico por símbolo para prevenir condiciones de carrera
        self._grid_creation_locks: Dict[str, asyncio.Lock] = {}
        # V7.1 FASE 4: Debounce interno para evitar spam de CREAR_GRID
        self._grid_pending_creation: Dict[str, float] = {}
        # HEDGE MODE: Detección y helpers
        self._hedge_mode: bool = False  # Se detecta al arrancar
        # P5 FIX: Órdenes que el propio bot canceló (E.2/E.3/E.5/kill switch).
        # El monitor reactivo las reconoce y deja de reportarlas como
        # "cancelada externamente" (ruido que ensuciaba el log y la auditoría).
        self._cancelaciones_propias: Set[str] = set()

    # ═══════════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _detectar_modo_posicion(self):
        """Detecta si la cuenta está en HEDGE MODE o ONE-WAY."""
        try:
            # Método 1: Intentar obtener position mode
            try:
                res = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_position_mode
                ))
                # FIX: Binance devuelve 'true'/'false' como STRING, no bool.
                # res.get(...) directo evaluaría 'false' como truthy → detección errónea.
                self._hedge_mode = str(res.get('dualSidePosition', 'false')).lower() == 'true'
            except Exception as e:
                print(f"  ⚠️ [HEDGE] No se pudo obtener position mode: {e}")
                # Fallback: intentar crear una orden de prueba con positionSide
                self._hedge_mode = False

            modo_str = "HEDGE" if self._hedge_mode else "ONE-WAY"
            print(f"  [EXECUTOR] Modo de posición detectado: {modo_str}")

            if self._hedge_mode:
                print(f"  [HEDGE] Grids direccionales usarán positionSide LONG/SHORT")
                print(f"  [HEDGE] Grid neutral habilitado")
            else:
                print(f"  [HEDGE] Modo ONE-WAY detectado. Grid neutral NO funcionará.")

        except Exception as e:
            print(f"  ⚠️ [HEDGE] Error detectando modo de posición: {e}")
            self._hedge_mode = False

    def _get_position_amt_hedge(self, position_list: list, side: str) -> Decimal:
        """
        En HEDGE MODE, retorna la posición del lado específico.
        En ONE-WAY, retorna positionAmt del primer elemento.
        """
        if not position_list:
            return Decimal('0')
        # FIX: filtrar por lado SOLO si realmente estamos en HEDGE MODE.
        # En ONE-WAY Binance también incluye positionSide='BOTH' en la respuesta;
        # filtrar por 'LONG'/'SHORT' ahí devolvería 0 siempre (bug del plan original).
        if self._hedge_mode:
            for p in position_list:
                if p.get('positionSide') == side:
                    return Decimal(str(p.get('positionAmt', 0)))
            return Decimal('0')
        # ONE-WAY: positionAmt es la posición neta
        return Decimal(str(position_list[0].get('positionAmt', 0)))

    def _get_position_neta_hedge(self, position_list: list) -> Decimal:
        """
        NEUTRAL en HEDGE MODE: posición neta = pierna LONG + pierna SHORT.
        (En hedge, positionAmt de la pierna SHORT viene negativo, así que la suma
        directa da la neta correcta: +0.5 LONG y -0.3 SHORT → +0.2 neto.)
        En ONE-WAY devuelve position[0] igual que antes.
        """
        if not position_list:
            return Decimal('0')
        if self._hedge_mode:
            long_amt, short_amt = Decimal('0'), Decimal('0')
            for p in position_list:
                ps = p.get('positionSide')
                if ps == 'LONG':
                    long_amt = Decimal(str(p.get('positionAmt', 0)))
                elif ps == 'SHORT':
                    short_amt = Decimal(str(p.get('positionAmt', 0)))
            return long_amt + short_amt
        return Decimal(str(position_list[0].get('positionAmt', 0)))

    def _sincronizar_signal_grid_neutral_activo(self, symbol: str):
        """
        SYNC FIX: Alinear la máquina de señales cuando el executor tiene un grid
        NEUTRAL activo creado por una vía que la bypassa (force_fire) o
        recuperado post-reinicio. Idempotente: si ya está en NEUTRAL_GRID no toca nada.
        """
        st = self.signal_states.get(symbol)
        if st is None:
            return
        if st.estado != 'NEUTRAL_GRID':
            st._prev_estado = st.estado
            st.estado = 'NEUTRAL_GRID'
            st.neutral_grid_timestamp = int(time.time())
            print(f"  🔄 [SYNC] {symbol} SignalState -> NEUTRAL_GRID (executor tiene grid activo)")

    def _sincronizar_signal_grid_neutral_cerrado(self, symbol: str):
        """
        SYNC FIX: Liberar la máquina de señales cuando un grid NEUTRAL se cierra
        desde el executor (kill switch, límite de pérdida, aborto, fantasma).
        Idempotente: no pisa el reset propio de signals (que ya pone MONITOREO
        antes de enviar ABORTAR_GRID).
        """
        st = self.signal_states.get(symbol)
        if st is None:
            return
        if st.estado == 'NEUTRAL_GRID':
            st._prev_estado = 'NEUTRAL_GRID'
            st.estado = 'MONITOREO'
            st.neutral_grid_timestamp = 0
            st.grid_params_neutral = None
            st.filtro_macro_aprobado = False
            st.direccion_filtro = None
            print(f"  🔄 [SYNC] {symbol} SignalState -> MONITOREO (grid neutral cerrado por executor)")

    async def run(self):
        """Loop principal del executor."""
        print("  [EXECUTOR] Iniciando GridExecutor...")
        print(f"  [EXECUTOR] Modo: {CONFIG.trading_mode} | Capital: ${CONFIG.trading_capital_max_usdt}")

        await self._cargar_exchange_info()
        await self._detectar_modo_posicion()  # ← FASE A
        await self._recuperar_grids_activos()
        await self._barrido_huerfanas_global()  # P7: barrido global al arranque

        # Tarea de monitoreo periódico
        asyncio.create_task(self._monitoring_loop())

        while not self._shutdown.is_set():
            try:
                msg = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._procesar_mensaje(msg)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  ❌ [EXECUTOR] Error procesando mensaje: {e}")

    def stop(self):
        self._shutdown.set()

    # ═══════════════════════════════════════════════════════════════════════════════
    # EXCHANGE INFO Y VALIDACIÓN
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cargar_exchange_info(self):
        """Carga y cachea información de precisión por símbolo, incluyendo maxPrice/minPrice."""
        try:
            info = await self._api_call(
                asyncio.to_thread(self.client.futures_exchange_info)
            )
            for s in info['symbols']:
                if s['symbol'] in CONFIG.symbols:
                    # Buscar filtros por tipo, no por índice (el orden varía por símbolo)
                    lot_size = next((f for f in s['filters'] if f['filterType'] == 'LOT_SIZE'), {})
                    price_filter = next((f for f in s['filters'] if f['filterType'] == 'PRICE_FILTER'), {})
                    min_notional = next((f for f in s['filters'] if f['filterType'] == 'MIN_NOTIONAL'), {})

                    self._exchange_info[s['symbol']] = {
                        'stepSize': float(lot_size.get('stepSize', 0.001)),
                        'tickSize': float(price_filter.get('tickSize', 0.01)),
                        'minNotional': float(min_notional.get('notional', min_notional.get('minNotional', 5.0))),
                        'minPrice': float(price_filter.get('minPrice', 0.0)),
                        'maxPrice': float(price_filter.get('maxPrice', 999999.0))
                    }

            print(f"  [EXECUTOR] ExchangeInfo cargado para {len(self._exchange_info)} símbolos")
        except Exception as e:
            print(f"  ⚠️ [EXECUTOR] Error cargando exchangeInfo: {e}")

    def _get_symbol_info(self, symbol: str) -> dict:
        return self._exchange_info.get(symbol, {
            'stepSize': 0.001, 'tickSize': 0.01, 'minNotional': 5.0,
            'minPrice': 0.0, 'maxPrice': 999999.0
        })

    def _validar_y_redondear_precio(self, precio: float, symbol: str) -> Optional[float]:
        """Valida y redondea un precio según tick_size, minPrice y maxPrice de Binance."""
        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))
        min_price = info.get('minPrice', 0.0)
        max_price = info.get('maxPrice', 999999.0)
        
        precio_d = Decimal(str(precio))
        # Redondear al tick_size (hacia abajo para no exceder)
        ticks = int(precio_d / tick_size)
        precio_redondeado = float(ticks * tick_size)
        
        if precio_redondeado <= 0 or precio_redondeado < min_price or precio_redondeado > max_price:
            print(f"  ❌ [EXECUTOR] {symbol} Precio inválido: {precio_redondeado} (tick:{tick_size}, min:{min_price}, max:{max_price})")
            return None
        return precio_redondeado

    # ═══════════════════════════════════════════════════════════════════════════════
    # RATE LIMITER
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _api_call(self, coro):
        """Wrapper con rate limiting, retry y contador global de requests."""
        async with self._rate_limiter:
            # ─── Rate limit global por minuto ───
            ahora = time.time()
            if not hasattr(self, '_request_timestamps'):
                self._request_timestamps = []
            # Limpiar timestamps mayores a 60s
            self._request_timestamps = [t for t in self._request_timestamps if ahora - t < 60]
            self._request_timestamps.append(ahora)

            reqs_por_minuto = len(self._request_timestamps)
            if reqs_por_minuto >= 900:  # 90% de 1000, margen de seguridad
                print(f"  ⚠️ [EXECUTOR] Rate limit cercano: {reqs_por_minuto}/min. Pausando 1s...")
                await asyncio.sleep(1)

            if reqs_por_minuto % 100 == 0 and reqs_por_minuto > 0:
                print(f"  [EXECUTOR] Requests este minuto: {reqs_por_minuto}/1000")

            for intento in range(3):
                try:
                    return await coro
                except BinanceAPIException as e:
                    if e.code == -1003:  # Rate limit
                        wait = 2 ** intento
                        print(f"  ⚠️ [EXECUTOR] Rate limit, esperando {wait}s...")
                        await asyncio.sleep(wait)
                    else:
                        raise
            raise Exception("Max retries en API call")

    # ═══════════════════════════════════════════════════════════════════════════════
    # APALANCAMIENTO ADAPTATIVO
    # ═══════════════════════════════════════════════════════════════════════════════

    def _calcular_leverage_adaptativo(self, symbol: str, capital: float,
                                       grid_count: int, price: float) -> Optional[int]:
        """
        Itera de apalancamiento_min a apalancamiento_max.
        Retorna el primer leverage que hace que notional_final >= minNotional * margen.
        Retorna None si ninguno alcanza.
        """
        info = self._get_symbol_info(symbol)
        step_size = Decimal(str(info['stepSize']))
        min_notional = Decimal(str(info['minNotional']))
        margen = Decimal(str(CONFIG.trading_notional_margen_seguridad))
        precio_d = Decimal(str(price))

        for lev in range(CONFIG.trading_apalancamiento_min,
                         CONFIG.trading_apalancamiento_max + 1):
            notional_total = Decimal(str(capital)) * lev
            notional_orden = notional_total / grid_count

            qty_raw = notional_orden / precio_d
            # Redondear hacia abajo al step_size
            steps = int(qty_raw / step_size)
            qty = steps * step_size

            if qty <= 0:
                continue

            notional_final = qty * precio_d

            if notional_final >= min_notional * margen:
                print(f"  [EXECUTOR] {symbol} Leverage adaptativo: {lev}x | "
                      f"Notional/orden: {float(notional_final):.2f} USDT | "
                      f"Qty: {float(qty)}")
                return lev

        print(f"  ❌ [EXECUTOR] {symbol} No se encontró leverage viable. "
              f"Max probado: {CONFIG.trading_apalancamiento_max}x")
        return None

    # ═══════════════════════════════════════════════════════════════════════════════
    # HELPERS NUEVOS FASE 3.2
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cambiar_leverage(self, symbol: str, leverage: int):
        """Cambia leverage en Binance y cachea el valor."""
        try:
            await self._api_call(asyncio.to_thread(
                self.client.futures_change_leverage,
                symbol=symbol, leverage=leverage
            ))
            self._symbol_leverage[symbol] = leverage
        except Exception as e:
            print(f"  ❌ [EXECUTOR] {symbol} Error cambiando leverage: {e}")
            raise

    def _generar_niveles(self, params: dict, price: float, symbol: str) -> List[float]:
        """Genera niveles de grid alineados a tick_size, evitando duplicados."""
        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        lower = Decimal(str(params['lower_limit']))
        upper = Decimal(str(params['upper_limit']))
        grid_count = int(params['grid_count'])

        rango = upper - lower
        step = rango / (grid_count - 1) if grid_count > 1 else rango
        niveles = []
        vistos = set()
        for i in range(grid_count):
            nivel = lower + step * i
            ticks = int(nivel / tick_size)
            nivel = ticks * tick_size
            nivel_f = float(nivel)
            if nivel_f in vistos:
                continue
            vistos.add(nivel_f)
            niveles.append(nivel_f)

        return niveles

    def _filtrar_niveles_por_limites_binance(self, niveles: List[float], symbol: str) -> List[float]:
        """
        FASE 4.6: Filtra niveles que excedan maxPrice o estén por debajo de minPrice
        según los límites de Binance Futures para el símbolo.
        Evita errores -4016: 'Limit price can't be higher than X'.
        """
        info = self._get_symbol_info(symbol)
        min_price = info.get('minPrice', 0.0)
        max_price = info.get('maxPrice', 999999.0)

        niveles_filtrados = []
        rechazados = 0
        for n in niveles:
            if n < min_price:
                rechazados += 1
                continue
            if n > max_price:
                rechazados += 1
                continue
            niveles_filtrados.append(n)

        if rechazados > 0:
            print(f"  [EXECUTOR] {symbol} {rechazados} nivel(es) filtrado(s) por limites Binance "
                  f"[min:{min_price}, max:{max_price}]")

        return niveles_filtrados

    # ═══════════════════════════════════════════════════════════════════════════════
    # V7.1 FASE 1: VERIFICACIÓN PRE-CREACIÓN DE GRIDS HUÉRFANOS EN BINANCE
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _verificar_grid_existente_en_binance(self, symbol: str) -> bool:
        """
        V7.1 FASE 1: Consulta si hay órdenes abiertas con prefix 'CM' en Binance.
        Retorna True si detecta un grid huérfano (órdenes abiertas con nuestro prefix).
        """
        try:
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders, symbol=symbol
            ))
            huérfanas = [o for o in open_orders if o.get('clientOrderId', '').startswith('CM')]
            if huérfanas:
                print(f"  🚨 [V7.1] {symbol} Detectadas {len(huérfanas)} órdenes huérfanas con prefix CM")
                return True
            return False
        except Exception as e:
            print(f"  ⚠️ [V7.1] {symbol} Error verificando órdenes huérfanas: {e}")
            return False

    async def _cancelar_ordenes_huérfanas(self, symbol: str, huérfanas: List[dict]) -> bool:
        """
        V7.1 FASE 1 + PARCHE B: Cancela órdenes huérfanas y verifica éxito.
        Retorna True solo si TODAS fueron canceladas y Binance confirma 0 restantes.
        Retorna False si persiste alguna → el llamador debe abortar creación.
        """
        print(f"  [V7.1] {symbol} Cancelando {len(huérfanas)} órdenes huérfanas...")
        fallos = 0
        for o in huérfanas:
            oid = o.get('orderId')
            if not oid:
                print(f"  ⚠️ [V7.1] {symbol} Orden huérfana sin orderId, ignorando")
                continue
            try:
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_order,
                    symbol=symbol,
                    orderId=oid
                ))
                print(f"  [V7.1] {symbol} Orden huérfana {oid} cancelada")
            except Exception as e:
                fallos += 1
                print(f"  ⚠️ [V7.1] {symbol} No se pudo cancelar orden {oid}: {e}")

        # Si hubo fallos, verificar post-cancelación antes de declarar éxito
        if fallos > 0:
            print(f"  [V7.1] {symbol} {fallos} cancelación(es) fallaron. Verificando estado...")
            await asyncio.sleep(1.0)  # Más tiempo para propagación
            try:
                open_orders = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_open_orders, symbol=symbol
                ))
                restantes = [o for o in (open_orders or []) if o.get('clientOrderId', '').startswith('CM')]
                if restantes:
                    print(f"  🚨 [V7.1] {symbol} {len(restantes)} órdenes huérfanas PERSISTEN tras cancelación.")
                    return False
                print(f"  [V7.1] {symbol} Verificación OK: 0 órdenes huérfanas restantes.")
            except Exception as e_ver:
                print(f"  ⚠️ [V7.1] {symbol} Error verificando post-cancelación: {e_ver}")
                return False  # Si no podemos verificar, asumimos lo peor
        else:
            await asyncio.sleep(0.5)  # Espera original si todo fue limpio

        return True

    async def _barrido_huerfanas_global(self):
        """
        P7 FIX: Barrido GLOBAL de órdenes huérfanas al arranque, independiente de la
        recuperación de grids. Antes el barrido solo corría por símbolo recuperado:
        si un grid quedaba fuera de la DB (rollback incompleto, cierre manual de DB,
        crash entre estados) sus órdenes vivas quedaban invisibles y el siguiente
        grid del mismo símbolo duplicaba precios.

        Lógica: consulta TODAS las órdenes abiertas de la cuenta (1 sola llamada),
        filtra las de este bot (clientOrderId prefix 'CM'), extrae el grid_id de
        cada una y cancela las que no pertenezcan a un grid ACTIVO en DB.
        Es seguro porque la DB se escribe ANTES de enviar el batch (creación),
        así que toda orden legítima tiene su grid ACTIVO registrado.
        """
        try:
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders
            ))
            if not open_orders:
                print("  [P7] Barrido global: sin órdenes abiertas en la cuenta")
                return

            grids_db = await cargar_grid_ejecuciones_activos()
            ids_activos = {str(g['id']) for g in grids_db}

            huerfanas_por_symbol = {}
            for o in open_orders:
                cid = o.get('clientOrderId', '') or ''
                if not cid.startswith('CM'):
                    continue  # no es de este bot
                # Extraer grid_id: formatos CM{id}_..., CM{id}_BUY_..., CM{id}_TP_..., etc.
                gid = ''
                for ch in cid[2:]:
                    if ch.isdigit():
                        gid += ch
                    else:
                        break
                if not gid or gid in ids_activos:
                    continue  # pertenece a un grid activo (o no parseable → no tocar)
                huerfanas_por_symbol.setdefault(o['symbol'], []).append(o)

            if not huerfanas_por_symbol:
                print(f"  [P7] Barrido global: {len(open_orders)} órdenes abiertas, todas de grids activos")
                return

            for symbol, lista in huerfanas_por_symbol.items():
                print(f"  🧹 [P7] {symbol} {len(lista)} órdenes huérfanas GLOBALES "
                      f"(grid_id no activo en DB). Cancelando...")
                await self._cancelar_ordenes_huérfanas(symbol, lista)

        except Exception as e:
            # Nunca abortar el arranque por el barrido
            print(f"  ⚠️ [P7] Error en barrido global de huérfanas: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # V7.1 FASE 3: VALIDACIÓN DEFENSIVA DE INTEGRIDAD DE NIVELES
    # ═══════════════════════════════════════════════════════════════════════════════

    def _validar_integridad_niveles(self, niveles: List[float], symbol: str, grid_count_esperado: int) -> bool:
        """
        V7.1 FASE 3: Rechaza grids donde el rango es tan pequeño que los niveles colapsan.
        Reglas:
        - Si len(niveles) < grid_count_esperado * 0.6 → rechazar
        - Si max - min < tick_size * 3 → rechazar
        - Si hay niveles con distancia < tick_size * 1.5 → rechazar
        """
        if len(niveles) < 3:
            return False

        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        # Regla 1: Más del 40% de niveles colapsaron
        if len(niveles) < grid_count_esperado * 0.6:
            print(f"  ❌ [V7.1] {symbol} Niveles colapsados: {len(niveles)}/{grid_count_esperado} "
                  f"únicos (< 60% esperado)")
            return False

        # Regla 2: Rango total menor a 3 ticks
        rango = Decimal(str(max(niveles))) - Decimal(str(min(niveles)))
        if rango < tick_size * 3:
            print(f"  ❌ [V7.1] {symbol} Rango insuficiente: {float(rango):.4f} < {float(tick_size * 3):.4f} (3 ticks)")
            return False

        # Regla 3: Distancia mínima entre niveles consecutivos
        niveles_ordenados = sorted(niveles)
        for i in range(1, len(niveles_ordenados)):
            dist = Decimal(str(niveles_ordenados[i])) - Decimal(str(niveles_ordenados[i-1]))
            if dist < tick_size * Decimal('1.5') and dist > 0:
                print(f"  ❌ [V7.1] {symbol} Niveles demasiado cercanos: "
                      f"{niveles_ordenados[i-1]} ↔ {niveles_ordenados[i]} "
                      f"(dist {float(dist):.4f} < {float(tick_size * Decimal('1.5')):.4f})")
                return False

        return True

    # ═══════════════════════════════════════════════════════════════════════════════
    # V7.1 FASE 2: ENVÍO ATÓMICO DE ÓRDENES (TODO-O-NADA CON ROLLBACK)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _enviar_ordenes_batch(self, symbol: str, ordenes: List[dict], grid_id: int):
        """
        V7.1 FASE 2: Envía órdenes una por una. Si una falla, cancela las anteriores y retorna [].
        Garantiza 0 órdenes huérfanas en caso de fallo parcial.
        """
        if not ordenes:
            return []

        # E.1 FIX: Deduplicar por (side, price) antes de enviar
        ordenes_unicas = []
        vistas = {}  # (side, price) -> orden
        descartadas = 0
        for o in ordenes:
            key = (o['side'], Decimal(str(o['price'])))
            if key in vistas:
                descartadas += 1
                continue
            vistas[key] = o
            ordenes_unicas.append(o)

        if descartadas > 0:
            print(f"  [E.1] {symbol} {descartadas} orden(es) duplicadas descartadas antes de envío")

        ordenes = ordenes_unicas
        enviadas = []  # Lista of dicts  # Lista de dicts {'orderId': str, 'clientOrderId': str}
        order_ids_guardados = []

        for idx, ord in enumerate(ordenes):
            try:
                # FIX HEDGE-BATCH: pasar positionSide si la orden lo trae.
                # El parche hedge lo añadía al dict pero aquí se reconstruía la
                # llamada con kwargs explícitos y se perdía → error -4061 en hedge.
                kwargs_orden = {
                    'symbol': ord['symbol'],
                    'side': ord['side'],
                    'type': ord['type'],
                    'quantity': ord['quantity'],
                    'price': ord['price'],
                    'timeInForce': ord['timeInForce'],
                    'newClientOrderId': ord['newClientOrderId']
                }
                if 'positionSide' in ord:
                    kwargs_orden['positionSide'] = ord['positionSide']
                res = await self._api_call(asyncio.to_thread(
                    self.client.futures_create_order,
                    **kwargs_orden
                ))

                if 'orderId' not in res:
                    print(f"  ❌ [V7.1] {symbol} Orden {idx} sin orderId en respuesta: {res}")
                    raise Exception(f"Respuesta sin orderId: {res}")

                await guardar_orden_ejecucion(
                    grid_ejecucion_id=grid_id,
                    binance_order_id=str(res['orderId']),
                    client_order_id=ord['newClientOrderId'],
                    symbol=symbol,
                    side=ord['side'],
                    tipo_orden='ENTRY',
                    price=ord['price'],
                    quantity=ord['quantity']
                )
                enviadas.append({'orderId': str(res['orderId']), 'clientOrderId': ord['newClientOrderId']})
                order_ids_guardados.append(res['orderId'])

                # Rate limit micro-pausa entre órdenes
                if idx < len(ordenes) - 1:
                    await asyncio.sleep(0.05)

            except Exception as e:
                print(f"  ❌ [V7.1] {symbol} Orden {idx} ({ord['newClientOrderId']}) falló: {e}. "
                      f"Cancelando {len(enviadas)} órdenes previas...")

                # ROLLBACK: Cancelar todas las órdenes ya enviadas de este grid
                rollback_fallos = 0
                for env in enviadas:
                    try:
                        await self._api_call(asyncio.to_thread(
                            self.client.futures_cancel_order,
                            symbol=symbol,
                            orderId=env['orderId']
                        ))
                        print(f"  [V7.1] Rollback: orden {env['orderId']} cancelada")
                    except Exception as e_cancel:
                        rollback_fallos += 1
                        print(f"  ⚠️ [V7.1] Rollback falló para {env['orderId']}: {e_cancel}")

                print(f"  🛑 [V7.1] {symbol} Grid abortado. Rollback: {len(enviadas)} canceladas, "
                      f"{rollback_fallos} fallos de cancelación")
                return []  # Lista vacía = fallo total

        print(f"  ✅ [V7.1] {symbol} Batch atómico exitoso: {len(order_ids_guardados)}/{len(ordenes)} órdenes")
        return order_ids_guardados

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 2 PLAN 6.3: LÓGICA OPERATIVA INTERNA (Emparejamiento FIFO autónomo)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _emparejar_posiciones(self, state: GridExecutionState, timestamp: int):
        """
        FASE 2.1: Emparejamiento FIFO de posiciones LONG y SHORT.
        El LONG más antiguo se empareja primero con el SHORT más antiguo
        que esté en nivel superior, evitando ocultar pérdidas.
        """
        if not state.grid_state:
            return
        
        gs = state.grid_state
        
        # Separar y ordenar por timestamp (FIFO)
        longs_abiertas = [p for p in gs.posiciones if p.estado == 'ABIERTA' and p.tipo == 'LONG']
        shorts_abiertas = [p for p in gs.posiciones if p.estado == 'ABIERTA' and p.tipo == 'SHORT']
        
        longs_abiertas.sort(key=lambda p: p.timestamp_apertura)
        shorts_abiertas.sort(key=lambda p: p.timestamp_apertura)
        
        for long_pos in longs_abiertas:
            if long_pos.estado != 'ABIERTA':
                continue
            
            for short_pos in shorts_abiertas:
                if short_pos.estado != 'ABIERTA':
                    continue
                
                # El SHORT debe estar en nivel superior al LONG (take-profit válido)
                # y haberse abierto después o al mismo tiempo que el LONG
                if (short_pos.nivel_precio > long_pos.nivel_precio and
                    short_pos.timestamp_apertura >= long_pos.timestamp_apertura):
                    
                    # Cerrar el par
                    diferencia_niveles = short_pos.nivel_precio - long_pos.nivel_precio
                    pnl_bruto = diferencia_niveles * long_pos.qty
                    fee_par = (long_pos.fee_pagada + short_pos.fee_pagada)
                    pnl_neto = pnl_bruto - fee_par
                    
                    long_pos.estado = 'CERRADA'
                    long_pos.pnl_cierre = pnl_neto / Decimal('2')
                    short_pos.estado = 'CERRADA'
                    short_pos.pnl_cierre = pnl_neto / Decimal('2')
                    
                    gs.pnl_bruto += pnl_bruto
                    gs.pnl_neto = gs.pnl_bruto - gs.fees_totales
                    gs.trades_completados += 1
                    
                    # Cancelar órdenes de take-profit pendientes si existen
                    await self._cancelar_tp_si_existe(state, long_pos)
                    await self._cancelar_tp_si_existe(state, short_pos)
                    
                    print(f"  [EXECUTOR] {state.symbol} PAR CERRADO FIFO | "
                          f"LONG ${float(long_pos.nivel_precio):.4f} → SELL ${float(short_pos.nivel_precio):.4f} | "
                          f"Diff: {float(diferencia_niveles):.4f} | PnL: {float(pnl_neto):+.4f}")
                    break  # Solo emparejar una vez por LONG

    def _registrar_cancelacion_propia(self, order_id):
        """P5: Marca una orden como cancelada por el propio bot (anti falsos 'cancelada externamente')."""
        try:
            if order_id is None:
                return
            # Cota de seguridad para que el set no crezca sin límite en sesiones largas
            if len(self._cancelaciones_propias) > 2000:
                self._cancelaciones_propias.clear()
            self._cancelaciones_propias.add(str(order_id))
        except Exception:
            pass  # Nunca romper el flujo por telemetría

    async def _cancelar_tp_si_existe(self, state: GridExecutionState, pos: PosicionReal):
        """Cancela la orden de take-profit de una posición si aún está pendiente."""
        if pos.orden_cierre_id and state.grid_state:
            try:
                self._registrar_cancelacion_propia(pos.orden_cierre_id)  # P5
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_order,
                    symbol=state.symbol,
                    orderId=pos.orden_cierre_id
                ))
                print(f"  [EXECUTOR] {state.symbol} TP {pos.orden_cierre_id} cancelado")
            except Exception as e:
                print(f"  ⚠️ [EXECUTOR] Error cancelando TP {pos.orden_cierre_id}: {e}")
            finally:
                # Limpiar tracking SIEMPRE, incluso si la cancelación falla
                if pos.id in state.grid_state.ordenes_tp_pendientes:
                    del state.grid_state.ordenes_tp_pendientes[pos.id]

    async def _on_fill_real(self, state: GridExecutionState, side: str, price: float,
                      qty: float, fee: float, timestamp: int,
                      binance_order_id: str, binance_trade_id: str = None) -> str:
        """
        FASE 2.2: El executor registra el fill directamente en su estado.
        Reemplaza a GridSimulator.on_fill().
        """
        if not state.grid_state or not state.grid_state.activa:
            print(f"  ⚠️ [EXECUTOR] {state.symbol} Fill ignorado: grid no activo")
            return None
        if timestamp is None:
            timestamp = int(time.time())
        
        gs = state.grid_state
        
        precio_d = Decimal(str(price))
        qty_d = Decimal(str(qty))
        fee_d = Decimal(str(fee))
        notional = qty_d * precio_d
        
        # E.5 FIX: Verificar si ya existe posición para este binance_order_id (fills parciales)
        pos_existente = None
        if binance_order_id and gs.posiciones:
            for p in gs.posiciones:
                if p.binance_order_id == binance_order_id and p.estado == 'ABIERTA':
                    pos_existente = p
                    break

        if pos_existente:
            # E.5: Acumular fill parcial en posición existente
            pos_existente.qty += qty_d
            pos_existente.filled_qty += qty_d
            pos_existente.fee_pagada += fee_d
            pos_id = pos_existente.id
            print(f"  [E.5] {state.symbol} Fill parcial acumulado en {pos_id} | "
                  f"Qty total: {float(pos_existente.qty):.4f} | Nuevo fill: {float(qty_d):.4f}")

            # P5 FIX: Ya NO se cancela el TP en cada fill parcial. Antes se cancelaba
            # y recreaba hasta 20 veces por orden (churn de API, ventanas sin
            # protección, errores -2011 y falsos "cancelada externamente"). Ahora el
            # TP se recrea UNA sola vez, con qty acumulada completa (P1), desde
            # _procesar_fills_pendientes cuando la orden de entrada termina de llenarse.
        else:
            # Crear nueva posición (fill completo o primera parte parcial)
            pos = PosicionReal(
                tipo='LONG' if side == 'BUY' else 'SHORT',
                nivel_precio=float(price),
                precio_ejecucion=float(price),
                qty=float(qty),
                fee_pagada=float(fee),
                timestamp_apertura=timestamp,
                binance_order_id=binance_order_id,
                filled_qty=float(qty)
            )

            gs.posiciones.append(pos)
            pos_id = pos.id

        gs.fees_totales += fee_d

        # Actualizar posicion_neta del executor (para compatibilidad con código existente)
        if side == 'BUY':
            state.posicion_neta += qty_d
        else:
            state.posicion_neta -= qty_d

        n_abiertas = gs.contar_posiciones_abiertas()
        gs.max_posiciones_simultaneas = max(gs.max_posiciones_simultaneas, n_abiertas)
        gs.precio_referencia = precio_d

        # MEJORA 3: Registrar mapeo de orden real a posición
        gs.ordenes_reales[binance_order_id] = pos_id

        # Auto-emparejar inmediatamente si hay par posible
        await self._emparejar_posiciones(state, timestamp)
        
        # FIX FASE 1: pos no definida cuando pos_existente es True
        # Usar pos_id que se definió en ambas ramas (if/else)
        print(f"  [EXECUTOR] {state.symbol} {side} real registrado @ ${price:.4f} | "
              f"Pos: {pos_id} | Qty: {qty} | Fee: ${fee:.4f} | "
              f"Posiciones abiertas: {n_abiertas}")
        
        return pos_id

    async def _on_fill_cierre_neutral(self, state: GridExecutionState, side: str, price: float,
                                      qty: float, fee: float, timestamp: int,
                                      binance_order_id: str) -> Optional[str]:
        """
        P2 FIX: Procesa el fill de una orden de CIERRE (TAKE_PROFIT neutral / CIERRE)
        SIN crear una posición nueva. Antes, todo fill NEUTRAL pasaba por _on_fill_real
        y el fill del TP creaba una PosicionReal fantasma (ej: pos_006 BUY 81.9 cuando
        en realidad era el TP cerrando pos_005), corrompiendo toda la contabilidad.

        Lógica:
        1. Localiza la posición que la orden estaba cerrando (match exacto por
           orden_cierre_id; fallback: la ABIERTA más antigua del lado contrario).
        2. Ajusta posicion_neta y fees SIEMPRE (Binance ya reflejó el cierre).
        3. Reduce la posición; si queda en cero la marca CERRADA y contabiliza PnL.
           Soporta fills parciales del TP (la posición queda ABIERTA con qty menor).
        """
        gs = state.grid_state
        if not gs:
            return None
        if timestamp is None:
            timestamp = int(time.time())

        qty_d = Decimal(str(qty))
        fee_d = Decimal(str(fee))
        precio_d = Decimal(str(price))

        # 1. Localizar posición objetivo
        pos = None
        for p in gs.posiciones:
            if (p.estado == 'ABIERTA' and p.orden_cierre_id
                    and str(p.orden_cierre_id) == str(binance_order_id)):
                pos = p
                break
        if pos is None:
            # Fallback: la posición ABIERTA más antigua del lado que este fill cierra
            tipo_objetivo = 'SHORT' if side == 'BUY' else 'LONG'
            candidatas = [p for p in gs.posiciones
                          if p.estado == 'ABIERTA' and p.tipo == tipo_objetivo]
            candidatas.sort(key=lambda p: p.timestamp_apertura)
            if candidatas:
                pos = candidatas[0]

        # 2. Ajustar neta y fees SIEMPRE (el cierre ya ocurrió en Binance)
        if side == 'BUY':
            state.posicion_neta += qty_d
        else:
            state.posicion_neta -= qty_d
        gs.fees_totales += fee_d

        if pos is None:
            # No hay posición interna que matchear (ej: ya cerrada por emparejamiento).
            # Solo ajustamos la neta para no divergir de Binance.
            print(f"  [P2] {state.symbol} Fill de cierre {side} @ ${price:.4f} sin posición "
                  f"ABIERTA que matchear (orden {binance_order_id}). Solo ajuste de neta.")
            return None

        # 3. Reducir posición y contabilizar PnL del tramo cerrado
        if pos.tipo == 'SHORT':
            pnl_tramo = (pos.precio_ejecucion - precio_d) * qty_d
        else:
            pnl_tramo = (precio_d - pos.precio_ejecucion) * qty_d
        gs.pnl_bruto += pnl_tramo
        gs.pnl_neto = gs.pnl_bruto - gs.fees_totales
        pos.pnl_cierre += pnl_tramo - fee_d

        pos.qty -= qty_d
        pos.fee_pagada += fee_d
        gs.ordenes_reales[str(binance_order_id)] = pos.id

        if pos.qty <= Decimal('0.00000001'):
            pos.qty = Decimal('0')
            pos.estado = 'CERRADA'
            # Limpiar tracking del TP consumido
            if (pos.id in gs.ordenes_tp_pendientes
                    and gs.ordenes_tp_pendientes[pos.id] == str(binance_order_id)):
                del gs.ordenes_tp_pendientes[pos.id]
            pos.orden_cierre_id = None
            gs.trades_completados += 1
            print(f"  ✅ [P2] {state.symbol} Pos {pos.id} CERRADA por fill {side} @ ${price:.4f} | "
                  f"Qty: {qty} | PnL tramo: {float(pnl_tramo):+.4f} | Fee: ${fee:.4f}")
        else:
            print(f"  [P2] {state.symbol} Pos {pos.id} reducida por fill {side} @ ${price:.4f} | "
                  f"Qty restante: {float(pos.qty):.4f} | PnL tramo: {float(pnl_tramo):+.4f}")

        return pos.id

    async def _evaluar_kill_switch(self, state: GridExecutionState,
                             precio_actual: float, timestamp: int) -> List[dict]:
        """
        FASE 2.3: El executor evalúa condiciones de kill switch directamente.
        Reemplaza a GridSimulator.poll().
        """
        if not state.grid_state or not state.grid_state.activa:
            return []
        
        gs = state.grid_state
        acciones = []
        
        # 1. Emparejar posiciones FIFO primero (si hay LONG + SHORT abiertas)
        await self._emparejar_posiciones(state, timestamp)
        
        # 2. Verificar timeout de posiciones abiertas
        timeout_seg = CONFIG.grid_neutral_posicion_timeout_min * 60
        
        for pos in gs.posiciones:
            if pos.estado != 'ABIERTA':
                continue
            
            tiempo_abierta = timestamp - pos.timestamp_apertura
            if tiempo_abierta > timeout_seg:
                acciones.append({
                    'tipo': 'KILL_SWITCH',
                    'pos_id': pos.id,
                    'pos_tipo': pos.tipo,
                    'qty': float(pos.qty),
                    'razon': 'timeout_posicion',
                    'binance_order_id': pos.binance_order_id
                })
                pos.estado = 'PENDIENTE_CIERRE'
                print(f"  [EXECUTOR] {state.symbol} Pos {pos.id} vencida: "
                      f"{tiempo_abierta}s > {timeout_seg}s")
        
        # 3. Verificar max posiciones simultáneas
        abiertas = gs.contar_posiciones_abiertas()
        if abiertas > CONFIG.grid_neutral_sim_max_posiciones:
            # Cerrar la más antigua
            mas_antigua = min(
                (p for p in gs.posiciones if p.estado == 'ABIERTA'),
                key=lambda p: p.timestamp_apertura,
                default=None
            )
            if mas_antigua:
                acciones.append({
                    'tipo': 'KILL_SWITCH',
                    'pos_id': mas_antigua.id,
                    'pos_tipo': mas_antigua.tipo,
                    'qty': float(mas_antigua.qty),
                    'razon': 'max_posiciones',
                    'binance_order_id': mas_antigua.binance_order_id
                })
                mas_antigua.estado = 'PENDIENTE_CIERRE'
                print(f"  [EXECUTOR] {state.symbol} Max posiciones ({abiertas}), "
                      f"cerrando {mas_antigua.id}")
        
        # 4. Calcular PnL acumulado en RAM
        gs.pnl_neto = gs.pnl_bruto - gs.fees_totales
        
        return acciones

    def _cerrar_posicion_por_id(self, state: GridExecutionState, pos_id: str,
                                precio_cierre: float, fee_cierre: float = 0.0,
                                binance_order_id: str = None) -> bool:
        """
        FASE 2.4: El executor cierra una posición por ID usando precio real.
        Reemplaza a GridSimulator.close_position_by_id().
        """
        if not state.grid_state:
            return False
        
        gs = state.grid_state
        
        for pos in gs.posiciones:
            if pos.id == pos_id and pos.estado in ('ABIERTA', 'PENDIENTE_CIERRE'):
                # Calcular PnL
                if pos.tipo == 'LONG':
                    pnl = (precio_cierre - float(pos.precio_ejecucion)) * float(pos.qty)
                else:
                    pnl = (float(pos.precio_ejecucion) - precio_cierre) * float(pos.qty)
                
                pos.estado = 'CERRADA'
                pos.pnl_cierre = Decimal(str(pnl))
                pos.orden_cierre_id = binance_order_id
                
                gs.pnl_bruto += Decimal(str(pnl))
                gs.fees_totales += Decimal(str(fee_cierre))
                gs.pnl_neto = gs.pnl_bruto - gs.fees_totales
                gs.trades_kill_switch += 1
                
                # Actualizar posicion_neta del executor
                if pos.tipo == 'LONG':
                    state.posicion_neta -= pos.qty
                else:
                    state.posicion_neta += pos.qty
                
                # Limpiar de órdenes pendientes
                if pos.id in gs.ordenes_tp_pendientes:
                    del gs.ordenes_tp_pendientes[pos.id]
                
                print(f"  [EXECUTOR] {state.symbol} Pos {pos_id} cerrada real "
                      f"@ ${precio_cierre:.4f} | PnL:{pnl:+.4f} | "
                      f"Fee cierre: ${fee_cierre:.4f}")
                return True
        
        print(f"  [EXECUTOR] {state.symbol} Pos {pos_id} no encontrada para cierre")
        return False

    async def _cerrar_grid_total(self, state: GridExecutionState, precio_final: float) -> dict:
        """
        FASE 2.5: El executor cierra todo el grid y retorna resumen.
        Reemplaza a GridSimulator.close_sim_state().
        """
        if not state.grid_state:
            return {}
        
        gs = state.grid_state
        gs.activa = False
        
        precio_d = Decimal(str(precio_final))
        
        # Liquidar posiciones pendientes o abiertas
        for pos in gs.posiciones:
            if pos.estado in ('ABIERTA', 'PENDIENTE_CIERRE'):
                # Calcular PnL forzado
                if pos.tipo == 'LONG':
                    pnl = (precio_final - float(pos.precio_ejecucion)) * float(pos.qty)
                else:
                    pnl = (float(pos.precio_ejecucion) - precio_final) * float(pos.qty)
                
                pos.estado = 'CERRADA_FORZADA'
                pos.pnl_cierre = Decimal(str(pnl))
                gs.pnl_bruto += Decimal(str(pnl))
                
                # Actualizar posicion_neta
                if pos.tipo == 'LONG':
                    state.posicion_neta -= pos.qty
                else:
                    state.posicion_neta += pos.qty
        
        gs.pnl_neto = gs.pnl_bruto - gs.fees_totales
        
        # Cancelar todas las órdenes de take-profit pendientes
        for pos_id, tp_order_id in list(gs.ordenes_tp_pendientes.items()):
            try:
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_order,
                    symbol=state.symbol,
                    orderId=tp_order_id
                ))
            except Exception as e:
                print(f"  ⚠️ [EXECUTOR] Error cancelando TP {tp_order_id}: {e}")
        
        gs.ordenes_tp_pendientes.clear()
        
        resumen = {
            'pnl_bruto': float(gs.pnl_bruto),
            'pnl_neto': float(gs.pnl_neto),
            'fees_totales': float(gs.fees_totales),
            'trades_completados': gs.trades_completados,
            'trades_kill_switch': gs.trades_kill_switch,
            'posiciones_atrapadas': len(gs.posiciones_atrapadas),
            'posiciones_total': len(gs.posiciones),
            'posiciones_cerradas': sum(1 for p in gs.posiciones if p.estado == 'CERRADA'),
            'posiciones_forzadas': sum(1 for p in gs.posiciones if p.estado == 'CERRADA_FORZADA'),
        }
        
        print(f"  [EXECUTOR] {state.symbol} Grid cerrado total | "
              f"PnL Bruto: {resumen['pnl_bruto']:+.4f} | "
              f"PnL Neto: {resumen['pnl_neto']:+.4f} | "
              f"Trades: {resumen['trades_completados']} | "
              f"KS: {resumen['trades_kill_switch']}")
        
        return resumen

    def _calcular_pnl_posicion(self, pos: PosicionReal, precio_cierre: float) -> float:
        """
        FASE 2.6: Cálculo de PnL de una posición.
        Reemplaza a GridSimulator._calcular_pnl().
        """
        if pos.tipo == 'LONG':
            return (precio_cierre - float(pos.precio_ejecucion)) * float(pos.qty)
        else:
            return (float(pos.precio_ejecucion) - precio_cierre) * float(pos.qty)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 7: DASHBOARD Y ESTADO PÚBLICO UNIFICADO
    # ═══════════════════════════════════════════════════════════════════════════════

    def get_estado_grid(self, state: GridExecutionState) -> Optional[dict]:
        """
        FASE 2.7 / FASE 7: Retorna el estado actual del grid para el dashboard.
        Unificado para NEUTRAL y DIRECCIONAL.
        """
        if not state or not state.activa:
            return None
        
        # MODO NEUTRAL: lógica existente
        if state.grid_mode == 'NEUTRAL' and state.grid_state:
            gs = state.grid_state
            pos_abiertas = gs.posiciones_abiertas_list()
            pos_vencidas = sum(
                1 for p in pos_abiertas
                if (int(time.time()) - p.timestamp_apertura) > CONFIG.grid_neutral_posicion_timeout_min * 60
            )
            return {
                'grid_id': gs.grid_id,
                'symbol': gs.symbol,
                'activa': gs.activa,
                'niveles': len(gs.niveles),
                'niveles_buy': len(gs.niveles_buy),
                'niveles_sell': len(gs.niveles_sell),
                'posiciones_abiertas': len(pos_abiertas),
                'posiciones_atrapadas': len(gs.posiciones_atrapadas),
                'posiciones_vencidas': pos_vencidas,
                'trades_completados': gs.trades_completados,
                'trades_kill_switch': gs.trades_kill_switch,
                'pnl_neto': float(gs.pnl_neto),
                'pnl_bruto': float(gs.pnl_bruto),
                'fees_totales': float(gs.fees_totales),
                'max_posiciones_simultaneas': gs.max_posiciones_simultaneas,
                'ultimo_tick_segundos_ago': int(time.time()) - gs.ultimo_tick_ts,
                'timestamp_inicio': gs.timestamp_inicio,
                'posicion_neta': float(state.posicion_neta),
                'ordenes_tp_pendientes': len(gs.ordenes_tp_pendientes),
                'grid_mode': state.grid_mode,
            }
        
        # MODO DIRECCIONAL: datos desde GridExecutionState directamente
        niveles_buy = len([n for n in state.niveles if n < state.precio_entrada]) if state.grid_mode == 'LONG' else 0
        niveles_sell = len([n for n in state.niveles if n > state.precio_entrada]) if state.grid_mode == 'SHORT' else 0
        return {
            'grid_id': state.grid_id,
            'symbol': state.symbol,
            'activa': state.activa,
            'niveles': len(state.niveles),
            'niveles_buy': niveles_buy,
            'niveles_sell': niveles_sell,
            'posiciones_abiertas': 1 if float(state.posicion_neta) != 0 else 0,
            'posiciones_atrapadas': 0,
            'posiciones_vencidas': 0,
            'trades_completados': 0,
            'trades_kill_switch': 0,
            'pnl_neto': float(state.pnl_real),
            'pnl_bruto': float(state.pnl_real) + float(state.fees_real),
            'fees_totales': float(state.fees_real),
            'max_posiciones_simultaneas': 0,
            'ultimo_tick_segundos_ago': int(time.time()) - int(state.timestamp_inicio),
            'timestamp_inicio': int(state.timestamp_inicio),
            'posicion_neta': float(state.posicion_neta),
            'hedge_mode': self._hedge_mode,
            'posicion_lado': state.grid_mode if self._hedge_mode else 'NETA',
            'ordenes_tp_pendientes': len([o for o in state.ordenes.values() if o.get('tipo') == 'TAKE_PROFIT']),
            'grid_mode': state.grid_mode,
        }

    # ═══════════════════════════════════════════════════════════════════════════════
    # CREAR GRID LONG (FASE 1) — V7.1 CON LOCK ATÓMICO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_direccional(self, symbol: str, direction: str, params: dict, price: float):
        """
        Crea un grid LONG o SHORT real en Binance Futures.
        direction: 'LONG' o 'SHORT'
        V7.1: Protegido con asyncio.Lock y verificación de huérfanas.
        """
        # V7.1 FASE 1: Lock atómico por símbolo
        lock = self._grid_creation_locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            print(f"  [EXECUTOR] >>> SOLICITUD RECIBIDA: {symbol} {direction} @ ${price:.4f}")
            print(f"  [EXECUTOR] Params: grids={params.get('grid_count')}, range=[{params.get('lower_limit')}, {params.get('upper_limit')}], step_pct={params.get('step_pct')}%")
            
            # Doble verificación DENTRO del lock
            if symbol in self._grids and self._grids[symbol].activa:
                print(f"  ⚠️ [EXECUTOR] {symbol} Ya hay grid activo, rechazando nuevo")
                return

            # V7.1 FASE 1: Verificación CRÍTICA de órdenes huérfanas en Binance
            tiene_huerfanas = await self._verificar_grid_existente_en_binance(symbol)
            if tiene_huerfanas:
                # Intentar recuperar órdenes huérfanas exactas y cancelarlas
                try:
                    open_orders = await self._api_call(asyncio.to_thread(
                        self.client.futures_get_open_orders, symbol=symbol
                    ))
                    huerfanas = [o for o in (open_orders or []) if o.get('clientOrderId', '').startswith('CM')]
                    if huerfanas:
                        limpieza_ok = await self._cancelar_ordenes_huérfanas(symbol, huerfanas)
                        if not limpieza_ok:
                            print(f"  🚨 [V7.1] {symbol} CREACIÓN ABORTADA: huérfanas persistentes tras limpieza.")
                            return
                except Exception as e:
                    print(f"  ⚠️ [V7.1] {symbol} Error limpiando huérfanas: {e}")
                    return

            # 1. Calcular leverage adaptativo
            leverage = self._calcular_leverage_adaptativo(
                symbol, CONFIG.trading_capital_max_usdt,
                params['grid_count'], price
            )
            if not leverage:
                await self._notificar_rechazo(symbol, "No alcanza notional mínimo ni con max leverage")
                return

            # 2. Verificar leverage existente
            if symbol in self._symbol_leverage and self._symbol_leverage[symbol] != leverage:
                leverage = self._symbol_leverage[symbol]
                print(f"  [EXECUTOR] {symbol} Usando leverage existente: {leverage}x")

            # 3. Cambiar leverage en Binance
            try:
                await self._cambiar_leverage(symbol, leverage)
            except Exception:
                return

            # 4. Generar niveles y qty
            niveles = self._generar_niveles(params, price, symbol)
            niveles = self._filtrar_niveles_por_limites_binance(niveles, symbol)
            if len(niveles) < 3:
                print(f"  ❌ [EXECUTOR] {symbol} Grid {direction} rechazado: solo {len(niveles)} niveles únicos")
                await self._notificar_rechazo(symbol, f"Grid inválido: {len(niveles)} niveles únicos")
                return

            # V7.1 FASE 3: Validar integridad de niveles
            if not self._validar_integridad_niveles(niveles, symbol, int(params['grid_count'])):
                await self._notificar_rechazo(symbol, "Niveles colapsados por tick_size")
                return

            info = self._get_symbol_info(symbol)
            step_size = Decimal(str(info['stepSize']))
            notional_total = Decimal(str(CONFIG.trading_capital_max_usdt)) * leverage
            notional_orden = notional_total / int(params['grid_count'])
            qty_raw = notional_orden / Decimal(str(price))
            steps = int(qty_raw / step_size)
            qty = float(steps * step_size)

            # FASE 2: Persistir niveles reales y qty en params para recuperación post-crash
            params['niveles'] = [float(n) for n in niveles]
            params['qty_por_orden'] = qty

            # 5. Guardar grid en SQLite
            grid_id = await guardar_grid_ejecucion(
                symbol=symbol, direction=direction, trading_mode=CONFIG.trading_mode,
                capital_asignado=CONFIG.trading_capital_max_usdt,
                apalancamiento_usado=leverage, precio_entrada=price,
                grid_params_json=json.dumps(params),
                timestamp_inicio=int(time.time())
            )
            if not grid_id:
                print(f"  ❌ [EXECUTOR] {symbol} No se pudo guardar grid en DB")
                return

            # 6. Generar órdenes LIMIT
            ordenes = []
            timestamp = int(time.time() * 1000)
            side = 'BUY' if direction == 'LONG' else 'SELL'
            for idx, nivel in enumerate(niveles):
                if direction == 'LONG':
                    if nivel >= price * 0.999:
                        continue
                else:  # SHORT
                    if nivel <= price * 1.001:
                        continue

                precio_validado = self._validar_y_redondear_precio(nivel, symbol)
                if precio_validado is None:
                    continue  # Saltar este nivel, no abortar todo el grid
                
                client_order_id = f"CM{grid_id}_{idx}_{timestamp}"
                orden = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'LIMIT',
                    'quantity': str(qty),
                    'price': str(precio_validado),
                    'timeInForce': 'GTC',
                    'newClientOrderId': client_order_id
                }
                # FASE B: HEDGE MODE requiere positionSide en direccionales
                if self._hedge_mode:
                    orden['positionSide'] = direction  # 'LONG' o 'SHORT'
                ordenes.append(orden)

            # 7. Enviar en batches (V7.1 FASE 2: atómico con rollback)
            order_ids_guardados = await self._enviar_ordenes_batch(symbol, ordenes, grid_id)

            # 8. Verificar que se guardaron órdenes
            if not order_ids_guardados:
                print(f"  ❌ [EXECUTOR] {symbol} Grid {direction} FALLÓ: 0 órdenes guardadas")
                # V7.1: Marcar DB como abortado si el batch falló
                await actualizar_grid_ejecucion_cierre(
                    grid_id=grid_id, estado='ABORTADO', pnl_real=0, fees_real=0,
                    razon_cierre='grid_incompleto_batch_rechazado'
                )
                return

            # 9. Crear estado en RAM
            state = GridExecutionState(
                grid_id=grid_id, symbol=symbol, direction=direction,
                capital=CONFIG.trading_capital_max_usdt, leverage=leverage,
                precio_entrada=price, niveles=niveles, qty_por_orden=qty
            )
            self._grids[symbol] = state

            print(f"  ✅ [EXECUTOR] {symbol} Grid {direction} creado | ID:{grid_id} | "
                  f"Leverage:{leverage}x | Órdenes:{len(order_ids_guardados)}")

    async def _crear_grid_long(self, symbol: str, params: dict, price: float):
        await self._crear_grid_direccional(symbol, 'LONG', params, price)

    async def _crear_grid_short(self, symbol: str, params: dict, price: float):
        await self._crear_grid_direccional(symbol, 'SHORT', params, price)

    # ═══════════════════════════════════════════════════════════════════════════════
    # CREAR GRID NEUTRAL (FASE 2 — Autónomo, sin helper) — V7.1 CON LOCK ATÓMICO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_neutral(self, symbol: str, params: dict, price: float):
        """Crea un grid neutral usando lógica interna del executor (FASE 2). V7.1: Lock atómico."""
        # V7.1 FASE 1: Lock atómico por símbolo
        lock = self._grid_creation_locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            if symbol in self._grids and self._grids[symbol].activa:
                print(f"  ⚠️ [EXECUTOR] {symbol} Ya hay grid activo")
                return

            # V7.1 FASE 1: Verificación CRÍTICA de órdenes huérfanas
            tiene_huerfanas = await self._verificar_grid_existente_en_binance(symbol)
            if tiene_huerfanas:
                try:
                    open_orders = await self._api_call(asyncio.to_thread(
                        self.client.futures_get_open_orders, symbol=symbol
                    ))
                    huerfanas = [o for o in (open_orders or []) if o.get('clientOrderId', '').startswith('CM')]
                    if huerfanas:
                        limpieza_ok = await self._cancelar_ordenes_huérfanas(symbol, huerfanas)
                        if not limpieza_ok:
                            print(f"  🚨 [V7.1] {symbol} CREACIÓN NEUTRAL ABORTADA: huérfanas persistentes.")
                            return
                except Exception as e:
                    print(f"  ⚠️ [V7.1] {symbol} Error limpiando huérfanas: {e}")
                    return

            # 1. Calcular leverage (igual que directional)
            leverage = self._calcular_leverage_adaptativo(
                symbol, CONFIG.trading_capital_max_usdt,
                params['grid_count'], price
            )
            if not leverage:
                await self._notificar_rechazo(symbol, "No alcanza notional mínimo ni con max leverage")
                return

            # 2. Verificar leverage existente
            if symbol in self._symbol_leverage and self._symbol_leverage[symbol] != leverage:
                leverage = self._symbol_leverage[symbol]
                print(f"  [EXECUTOR] {symbol} Usando leverage existente: {leverage}x")

            # 3. Cambiar leverage en Binance
            try:
                await self._cambiar_leverage(symbol, leverage)
            except Exception:
                return

            # 4. Generar niveles (igual que directional pero para ambos lados)
            niveles = self._generar_niveles(params, price, symbol)
            niveles = self._filtrar_niveles_por_limites_binance(niveles, symbol)

            # VALIDACIÓN mínima de niveles
            if len(niveles) < 3:
                print(f"  ❌ [EXECUTOR] {symbol} Grid NEUTRAL rechazado: solo {len(niveles)} niveles únicos")
                await self._notificar_rechazo(symbol, f"Grid inválido: {len(niveles)} niveles únicos")
                return

            # V7.1 FASE 3: Validar integridad de niveles
            if not self._validar_integridad_niveles(niveles, symbol, int(params['grid_count'])):
                await self._notificar_rechazo(symbol, "Niveles colapsados por tick_size")
                return

            # 5. Separar niveles BUY (debajo) y SELL (encima)
            niveles_buy = [n for n in niveles if n < price * 0.9995]
            niveles_sell = [n for n in niveles if n > price * 1.0005]

            if len(niveles_buy) < 1 or len(niveles_sell) < 1:
                print(f"  ❌ [EXECUTOR] {symbol} Grid NEUTRAL rechazado: BUY={len(niveles_buy)} SELL={len(niveles_sell)}")
                await self._notificar_rechazo(symbol, f"Grid neutral inválido: {len(niveles_buy)} buy, {len(niveles_sell)} sell niveles")
                return

            # 6. Calcular qty
            info = self._get_symbol_info(symbol)
            step_size = Decimal(str(info['stepSize']))
            notional_total = Decimal(str(CONFIG.trading_capital_max_usdt)) * leverage
            notional_orden = notional_total / int(params['grid_count'])
            qty_raw = notional_orden / Decimal(str(price))
            steps = int(qty_raw / step_size)
            qty = float(steps * step_size)

            # FASE 2: Persistir niveles reales y qty en params para recuperación post-crash
            params['niveles'] = [float(n) for n in niveles]
            params['qty_por_orden'] = qty

            # 7. Guardar grid en DB (igual)
            grid_id = await guardar_grid_ejecucion(
                symbol=symbol, direction='NEUTRAL', trading_mode=CONFIG.trading_mode,
                capital_asignado=CONFIG.trading_capital_max_usdt,
                apalancamiento_usado=leverage, precio_entrada=price,
                grid_params_json=json.dumps(params),
                timestamp_inicio=int(time.time())
            )

            if not grid_id:
                print(f"  ❌ [EXECUTOR] {symbol} No se pudo guardar grid en DB")
                return

            # 8. Crear estado del executor
            state = GridExecutionState(
                grid_id=grid_id, symbol=symbol, direction='NEUTRAL',
                capital=CONFIG.trading_capital_max_usdt, leverage=leverage,
                precio_entrada=price, niveles=niveles, qty_por_orden=qty
            )

            # 9. INICIAR GridState propio del executor (FASE 2)
            state.init_grid_state(niveles_buy=niveles_buy, niveles_sell=niveles_sell)

            # 10. Colocar órdenes LIMIT reales: BUY debajo, SELL encima
            ordenes = []
            timestamp = int(time.time() * 1000)
            for idx, nivel in enumerate(niveles_buy):
                precio_validado = self._validar_y_redondear_precio(nivel, symbol)
                if precio_validado is None:
                    continue  # Saltar este nivel, no abortar todo el grid
                
                orden_buy = {
                    'symbol': symbol, 'side': 'BUY', 'type': 'LIMIT',
                    'quantity': str(qty), 'price': str(precio_validado),
                    'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_BUY_{idx}_{timestamp}"
                }
                # FASE B.2 (complemento): HEDGE MODE — BUY abre/cierra lado LONG
                if self._hedge_mode:
                    orden_buy['positionSide'] = 'LONG'
                ordenes.append(orden_buy)
            for idx, nivel in enumerate(niveles_sell):
                precio_validado = self._validar_y_redondear_precio(nivel, symbol)
                if precio_validado is None:
                    continue  # Saltar este nivel, no abortar todo el grid

                orden_sell = {
                    'symbol': symbol, 'side': 'SELL', 'type': 'LIMIT',
                    'quantity': str(qty), 'price': str(precio_validado),
                    'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_SELL_{idx}_{timestamp}"
                }
                # FASE B.2 (complemento): HEDGE MODE — SELL abre/cierra lado SHORT
                if self._hedge_mode:
                    orden_sell['positionSide'] = 'SHORT'
                ordenes.append(orden_sell)

            # 11. Enviar batches (V7.1 FASE 2: atómico con rollback)
            order_ids_guardados = await self._enviar_ordenes_batch(symbol, ordenes, grid_id)

            # FIX V7.1: Verificar integridad del grid
            if len(order_ids_guardados) < len(ordenes):
                print(f"  ❌ [EXECUTOR] {symbol} Grid NEUTRAL INCOMPLETO: {len(order_ids_guardados)}/{len(ordenes)} órdenes. Abortando.")
                # Cancelar las que quedaron
                for oid in order_ids_guardados:
                    try:
                        await self._api_call(asyncio.to_thread(self.client.futures_cancel_order, symbol=symbol, orderId=oid))
                    except Exception:
                        pass
                # Marcar DB como abortado
                await actualizar_grid_ejecucion_cierre(
                    grid_id=grid_id, estado='ABORTADO', pnl_real=0, fees_real=0,
                    razon_cierre='grid_incompleto_batch_rechazado'
                )
                return

            self._grids[symbol] = state
            # SYNC FIX: alinear máquina de señales (force_fire la bypassa)
            self._sincronizar_signal_grid_neutral_activo(symbol)

            print(f"  ✅ [EXECUTOR] {symbol} Grid NEUTRAL creado | BUY:{len(niveles_buy)} SELL:{len(niveles_sell)} | Órdenes:{len(order_ids_guardados)}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # MONITOREO PERIÓDICO — V7.1 FASE 4: SANIDAD PERIÓDICA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _monitoring_loop(self):
        """Monitorea grids activos cada N segundos. V7.1: Incluye verificación de integridad."""
        ciclo = 0
        while not self._shutdown.is_set():
            await asyncio.sleep(CONFIG.trading_polling_interval_seg)
            ciclo += 1

            # P6 FIX: armadura por ciclo — NINGUNA excepción debe matar esta tarea:
            # es la única que pollea fills, gestiona TPs y evalúa kill switches.
            try:
                # FASE 3: Verificar límite de pérdida diaria
                await self._verificar_limite_perdida()

                for symbol in list(self._grids.keys()):
                    try:
                        # P6 FIX: usar .get() — _abortar_grid (otra tarea) puede eliminar
                        # el símbolo entre el snapshot de keys y este acceso. Antes:
                        # self._grids[symbol] FUERA del try → KeyError que mataba la tarea
                        # de monitoreo y dejaba al executor SIN monitoreo el resto del día
                        # (caso real: LDOUSDT 11:45 — Task-16 murió y nunca se reinició).
                        st = self._grids.get(symbol)
                        if st is None or not st.activa or st.cerrando:
                            continue
                        if st.activa and not st.cerrando:
                            # FASE 3 FIX: Auto-cleanup si grid terminó naturalmente
                            # FASE E.3: En HEDGE MODE, verificar posición del lado específico
                            pos_actual = float(st.posicion_neta)
                            if self._hedge_mode and st.grid_mode in ('LONG', 'SHORT'):
                                try:
                                    position = await self._api_call(asyncio.to_thread(
                                        self.client.futures_position_information,  # typo del plan corregido
                                        symbol=symbol
                                    ))
                                    pos_actual = float(self._get_position_amt_hedge(position, st.grid_mode))
                                except Exception:
                                    pass  # Fallback a posicion_neta

                            if pos_actual == 0 and len(st.pares_abiertos) == 0:
                                open_orders = await self._api_call(asyncio.to_thread(
                                    self.client.futures_get_open_orders, symbol=symbol
                                ))
                                if not open_orders:
                                    print(f"  [EXECUTOR] {symbol} Grid terminado naturalmente (posición 0, sin órdenes). Limpiando...")
                                    await actualizar_grid_ejecucion_cierre(
                                        grid_id=st.grid_id,
                                        estado='CERRADO',
                                        pnl_real=float(st.pnl_real),
                                        fees_real=float(st.fees_real),
                                        razon_cierre='grid_completado_natural'
                                    )
                                    st.activa = False
                                    del self._grids[symbol]
                                    if st.grid_mode == 'NEUTRAL':
                                        self._sincronizar_signal_grid_neutral_cerrado(symbol)
                                    continue

                            print(f"  [EXECUTOR] {symbol} Monitoreo | Pos: {float(st.posicion_neta):.4f} | PnL: {float(st.pnl_real):+.4f} | Fees: {float(st.fees_real):.4f}")
                        await self._monitorear_grid(symbol)

                        # V7.1 FASE 4: Verificación de integridad cada 3 ciclos (~30s)
                        if ciclo % 3 == 0:
                            await self._verificar_integridad_grid(symbol, st)
                    except Exception as e:
                        print(f"  ❌ [EXECUTOR] Error monitoreando {symbol}: {e}")

            except Exception as e_loop:
                print(f"  🚨 [P6] Error en ciclo de monitoreo #{ciclo} (la tarea sobrevive): {e_loop}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # V7.1 FASE 4: VERIFICACIÓN DE INTEGRIDAD DEL GRID
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _verificar_integridad_grid(self, symbol: str, state: GridExecutionState):
        """
        V7.1 FASE 4 / FASE 8: Detecta grids fantasmas, duplicación de órdenes, o grids huérfanos.
        Se ejecuta cada ~30 segundos desde _monitoring_loop.
        Funciona para NEUTRAL y DIRECCIONAL.
        """
        if not state.activa:
            return
        if state.grid_mode == 'NEUTRAL' and (not state.grid_state or not state.grid_state.activa):
            return

        try:
            # 1. Consultar órdenes abiertas en Binance
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders, symbol=symbol
            ))

            # 2. Filtrar órdenes de este grid por prefix
            prefix = f"CM{state.grid_id}"
            ordenes_nuestras = [o for o in open_orders if o.get('clientOrderId', '').startswith(prefix)]
            total_open = len(open_orders)

            # 3. Grid fantasma: 0 órdenes nuestras pero grid dice activo
            if len(ordenes_nuestras) == 0 and total_open == 0:
                # Verificar si hay posición real
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information, symbol=symbol
                ))
                # FIX HEDGE-NET: leer pierna correcta (direccional) o neta de ambas
                # piernas (neutral). Antes position[0] podía dar falso "grid fantasma".
                if self._hedge_mode and state.grid_mode in ('LONG', 'SHORT'):
                    pos_amt = self._get_position_amt_hedge(position, state.grid_mode)
                else:
                    pos_amt = self._get_position_neta_hedge(position)
                if abs(pos_amt) < Decimal('0.0001'):
                    print(f"  🚨 [V7.1] {symbol} Grid fantasma detectado (0 órdenes, 0 posición). Marcando cerrado.")
                    await actualizar_grid_ejecucion_cierre(
                        grid_id=state.grid_id,
                        estado='CERRADO',
                        pnl_real=float(state.pnl_real),
                        fees_real=float(state.fees_real),
                        razon_cierre='grid_fantasma_detectado_integridad'
                    )
                    state.activa = False
                    if symbol in self._grids:
                        del self._grids[symbol]
                    if state.grid_mode == 'NEUTRAL':
                        self._sincronizar_signal_grid_neutral_cerrado(symbol)
                    return

            # 4. Duplicación detectada: más órdenes de las esperadas
            # (esto solo detecta órdenes de OTROS grids CM, no del actual)
            ordenes_otros_cm = [o for o in open_orders if o.get('clientOrderId', '').startswith('CM') and not o.get('clientOrderId', '').startswith(prefix)]
            if len(ordenes_otros_cm) > 0:
                print(f"  🚨 [V7.1] {symbol} Detectadas {len(ordenes_otros_cm)} órdenes de OTROS grids CM. Posible duplicación.")
                # Cancelar órdenes ajenas para evitar interferencia
                for o in ordenes_otros_cm:
                    oid = o.get('orderId')
                    if not oid:
                        print(f"  ⚠️ [V7.1] {symbol} Orden ajena sin orderId, ignorando")
                        continue
                    try:
                        await self._api_call(asyncio.to_thread(
                            self.client.futures_cancel_order,
                            symbol=symbol,
                            orderId=oid
                        ))
                        print(f"  [V7.1] {symbol} Orden ajena {oid} cancelada")
                    except Exception as e:
                        print(f"  ⚠️ [V7.1] No se pudo cancelar orden ajena {o['orderId']}: {e}")

        except Exception as e:
            print(f"  ⚠️ [V7.1] {symbol} Error en verificación de integridad: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3 PLAN 6.3: REEMPLAZAR LLAMADAS AL HELPER POR LÓGICA INTERNA
    # ═══════════════════════════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════════════════════════
    # CR16 FASE 2: POLLING PROACTIVO DE FILLS REALES
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _poll_fills_proactivo(self, symbol: str, state: GridExecutionState) -> List[dict]:
        """
        Consulta PROACTIVAMENTE futures_account_trades() para descubrir fills
        que pudieron haber ocurrido entre ciclos de monitoreo.

        CR16 FIX: No esperar a que la orden desaparezca de open_orders.
        Ir directamente a la fuente: los trades de Binance.

        Args:
            symbol: Símbolo a consultar
            state: Estado del grid

        Returns:
            List[dict]: Nuevos fills detectados, no procesados
        """
        if not state.activa:
            return []
        if state.grid_mode == 'NEUTRAL' and (not state.grid_state or not state.grid_state.activa):
            return []

        # 1. Obtener timestamp del último trade conocido
        ultimo_ts = await obtener_ultimo_trade_timestamp(symbol)

        # P2-A FIX: piso temporal = nacimiento del grid (en ms).
        # Ningún fill anterior al grid puede pertenecerle. Sin este piso, el primer
        # poll adoptaba trades de grids anteriores (incidente ARBUSDT 2026-07-17).
        # Los fills perdidos por downtime se recuperan por la vía reactiva
        # (_monitorear_ordenes_abiertas), no por este poll.
        grid_inicio_ms = int(getattr(state, 'timestamp_inicio', 0) or 0) * 1000

        # 2. Consultar trades recientes en Binance
        try:
            # Si hay último timestamp, pedir desde ahí. Si no, desde el nacimiento del grid.
            if ultimo_ts > 0:
                # +1ms para no repetir; nunca por debajo del nacimiento del grid
                start_time = max(ultimo_ts + 1, grid_inicio_ms)
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol,
                    startTime=start_time,
                    limit=100
                ))
            elif grid_inicio_ms > 0:
                # P2-A: sin watermark previo → arrancar desde el nacimiento del grid,
                # no desde el historial completo de la cuenta
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol,
                    startTime=grid_inicio_ms,
                    limit=100
                ))
            else:
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol,
                    limit=100
                ))
        except Exception as e:
            print(f"  ⚠️ [CR16] {symbol} Error consultando trades: {e}")
            return []

        if not trades:
            return []

        # Cargar órdenes de DB una sola vez (fuera del loop)
        db_ordenes = await cargar_ordenes_por_grid(state.grid_id)
        ordenes_ids = {str(o['binance_order_id']) for o in db_ordenes}

        nuevos_fills = []

        for trade in trades:
            trade_id = str(trade['id'])
            order_id = str(trade['orderId'])

            # Guardar en tracking (idempotente)
            await guardar_fill_tracking(
                grid_ejecucion_id=state.grid_id,
                symbol=symbol,
                binance_trade_id=trade_id,
                binance_order_id=order_id,
                side=trade['side'],
                price=float(trade['price']),
                qty=float(trade['qty']),
                commission=float(trade['commission']),
                commission_asset=trade['commissionAsset'],
                realized_pnl=float(trade.get('realizedPnl', 0)),
                timestamp_ms=trade['time']
            )

            if order_id not in ordenes_ids:
                # Fill de una orden que NO tenemos trackeada → intentar sincronizar desde Binance
                print(f"  🚨 [CR16] {symbol} Fill de orden desconocida: {order_id}. Intentando sincronizar...")
                try:
                    order_info = await self._api_call(asyncio.to_thread(
                        self.client.futures_get_order,
                        symbol=symbol,
                        orderId=int(order_id)
                    ))
                    if order_info and order_info.get('orderId'):
                        sync_coid = order_info.get('clientOrderId', f"SYNC_{order_id}")
                        # P2-A FIX: solo adoptar órdenes con prefijo propio ('CM').
                        # Órdenes ajenas (manuales, otro bot) no se adoptan al grid:
                        # se reportan como ANOMALIA y el fill queda SIN_ORDEN en el
                        # procesamiento posterior (no reintenta, no contamina estado).
                        if not str(sync_coid).startswith('CM'):
                            print(f"  ⚠️ [CR16] {symbol} Orden {order_id} ajena "
                                  f"(clientOrderId='{sync_coid}'). No se adopta.")
                            nuevos_fills.append({
                                'tipo': 'ANOMALIA',
                                'trade': trade,
                                'razon': 'orden_ajena_sin_prefijo'
                            })
                            continue
                        # FIX FASE 2: Verificar si ya existe para evitar UNIQUE constraint
                        existe = any(str(o.get('client_order_id')) == sync_coid for o in db_ordenes)
                        if not existe:
                            await guardar_orden_ejecucion(
                                grid_ejecucion_id=state.grid_id,
                                binance_order_id=str(order_info['orderId']),
                                client_order_id=sync_coid,
                                symbol=symbol,
                                side=order_info['side'],
                                tipo_orden='ENTRY',
                                price=float(order_info.get('price', 0)),
                                quantity=float(order_info.get('origQty', 0))
                            )
                            # FIX FASE 2: Agregar al set para que fills parciales de la misma orden no re-intenten
                            ordenes_ids.add(order_id)
                            print(f"  [CR16] {symbol} Orden {order_id} sincronizada desde Binance. Se procesará en siguiente ciclo.")
                        else:
                            print(f"  [CR16] {symbol} Orden {order_id} ya existe en DB, skip sincronización.")
                            # FIX FASE 2: Aún así agregar al set para evitar re-intentos
                            ordenes_ids.add(order_id)
                        nuevos_fills.append({
                            'tipo': 'NUEVO_FILL',
                            'trade': trade,
                            'order_id': order_id
                        })
                        continue
                except Exception as e_sync:
                    print(f"  ⚠️ [CR16] {symbol} No se pudo sincronizar orden {order_id}: {e_sync}")

                nuevos_fills.append({
                    'tipo': 'ANOMALIA',
                    'trade': trade,
                    'razon': 'orden_no_trackeada'
                })
            else:
                # Verificar si ya fue procesado
                orden = next((o for o in db_ordenes if str(o['binance_order_id']) == order_id), None)
                if orden and orden['status'] == 'FILLED':
                    # Ya procesado, ignorar
                    continue

                # Nuevo fill detectado
                nuevos_fills.append({
                    'tipo': 'NUEVO_FILL',
                    'trade': trade,
                    'order_id': order_id
                })

        if nuevos_fills:
            print(f"  [CR16] {symbol} {len(nuevos_fills)} fills nuevos detectados "
                  f"(desde {len(trades)} trades consultados)")

        return nuevos_fills


    async def _procesar_fills_pendientes(self, symbol: str, state: GridExecutionState):
        """
        Procesa fills que están en tracking pero no han sido aplicados al estado.
        CR16 FIX: Pipeline de procesamiento garantizado.
        FASE 2 FIX: Bifurcación limpia entre NEUTRAL y DIRECCIONAL.
        FASE 3 FIX: Atómico por fill — marca como procesado INMEDIATAMENTE después
        de actualizar el estado RAM, ANTES de side-effects (TP, PnL). Esto garantiza
        que un fill nunca se reprocese, incluso si TP o PnL fallan.
        """
        if not state.activa:
            return

        fills = await cargar_fills_sin_procesar(state.grid_id)
        if not fills:
            return

        # Cargar órdenes de DB una sola vez
        db_ordenes = await cargar_ordenes_por_grid(state.grid_id)

        for fill in fills:
            fill_id = fill['id']
            procesado_ok = False
            try:
                # ─── VALIDACIÓN DEFENSIVA DE CAMPOS CRÍTICOS ───
                ts_raw = fill.get('timestamp_ms')
                try:
                    ts = int(ts_raw or 0) // 1000
                except (TypeError, ValueError):
                    ts = 0

                price = fill.get('price')
                qty = fill.get('qty')
                commission = fill.get('commission')
                side = fill.get('side')
                binance_order_id = fill.get('binance_order_id')
                binance_trade_id = fill.get('binance_trade_id')

                if any(v is None for v in [price, qty, commission, side, binance_order_id, binance_trade_id]):
                    print(f"  ⚠️ [FASE 3] {symbol} Fill {fill_id} con datos faltantes, marcando como ERROR")
                    await marcar_fill_procesado(fill_id, 'DATOS_FALTANTES')
                    procesado_ok = True
                    continue

                # ─── PASO 0: Identificar orden ───
                orden = next((o for o in db_ordenes if str(o['binance_order_id']) == str(binance_order_id)), None)

                if not orden:
                    print(f"  ⚠️ [CR16] Fill {fill_id} sin orden en DB (order_id={binance_order_id}), marcando como SIN_ORDEN")
                    await marcar_fill_procesado(fill_id, 'SIN_ORDEN')
                    procesado_ok = True
                    continue

                if orden['status'] == 'FILLED':
                    await marcar_fill_procesado(fill_id, 'YA_PROCESADO')
                    procesado_ok = True
                    continue

                # ─── PASO 1: Actualizar estado RAM ───
                pos_id = None
                if state.grid_mode == 'NEUTRAL' and state.grid_state:
                    # P2 FIX: Solo los fills de ENTRY crean/acumulan posiciones.
                    # Los fills de TAKE_PROFIT/CIERRE reducen o cierran la posición
                    # que estaban cerrando — nunca crean posiciones fantasma.
                    if orden['tipo_orden'] == 'ENTRY':
                        pos_id = await self._on_fill_real(
                            state=state,
                            side=side,
                            price=price,
                            qty=qty,
                            fee=commission,
                            timestamp=ts,
                            binance_order_id=str(binance_order_id),
                            binance_trade_id=str(binance_trade_id)
                        )
                    else:
                        pos_id = await self._on_fill_cierre_neutral(
                            state=state,
                            side=side,
                            price=price,
                            qty=qty,
                            fee=commission,
                            timestamp=ts,
                            binance_order_id=str(binance_order_id)
                        )
                else:
                    pos_id = await self._procesar_fill_direccional(state, fill, orden)

                # ─── PASO 2: MARCAR COMO PROCESADO (garantía anti-bucle infinito) ───
                await marcar_fill_procesado(fill_id, 'PROCESADO')
                procesado_ok = True

                # ─── PASO 3: Side-effects secundarios (fallan → log, no se repiten) ───
                try:
                    if orden['tipo_orden'] == 'ENTRY':
                        # FIX FASE 5: Verificar que la posición sigue ABIERTA antes de colocar TP
                        # (puede haber sido cerrada por emparejamiento FIFO en _on_fill_real)
                        posicion_aun_abierta = False
                        if state.grid_mode == 'NEUTRAL' and state.grid_state:
                            posicion_aun_abierta = any(
                                p.id == pos_id and p.estado == 'ABIERTA'
                                for p in state.grid_state.posiciones
                            )
                        elif state.grid_mode in ('LONG', 'SHORT') and hasattr(state, 'posiciones_direccionales'):
                            posicion_aun_abierta = any(
                                p.id == pos_id and p.estado == 'ABIERTA'
                                for p in state.posiciones_direccionales
                            )

                        if state.grid_mode == 'NEUTRAL' and state.grid_state and pos_id and posicion_aun_abierta:
                            # P5 FIX: No cancelar/recrear el TP en cada fill parcial.
                            # Solo (a) colocarlo si la posición aún no tiene, o
                            # (b) recrearlo con qty acumulada completa (P1) cuando la
                            # orden de entrada terminó de llenarse (o casi: >=99.9%).
                            pos_p5 = next((p for p in state.grid_state.posiciones
                                           if p.id == pos_id and p.estado == 'ABIERTA'), None)
                            qty_orden_p5 = Decimal(str(orden.get('quantity', 0) or 0))
                            orden_completa_p5 = bool(
                                pos_p5 is not None and qty_orden_p5 > 0
                                and pos_p5.qty >= qty_orden_p5 * Decimal('0.999')
                            )
                            if pos_p5 is not None and pos_p5.orden_cierre_id and not orden_completa_p5:
                                print(f"  [P5] {symbol} Fill parcial: TP existente de {pos_id} se mantiene "
                                      f"(qty {float(pos_p5.qty):.4f}/{float(qty_orden_p5):.4f})")
                            else:
                                if pos_p5 is not None and pos_p5.orden_cierre_id:
                                    # Recrear con qty completa → cancelar TP viejo primero
                                    await self._cancelar_tp_si_existe(state, pos_p5)
                                    pos_p5.orden_cierre_id = None  # Si el recreate falla, E.6 lo detecta
                                await self._colocar_take_profit_neutral(symbol, state, orden, fill, side, pos_id)
                        elif state.grid_mode in ('LONG', 'SHORT') and posicion_aun_abierta:
                            fill['pos_id'] = pos_id
                            await self._colocar_take_profit(symbol, state, orden, fill)
                        else:
                            print(f"  [FASE 5] {symbol} Pos {pos_id} ya cerrada, skip TP.")

                    tipo_evento = 'FILL_ENTRADA' if orden['tipo_orden'] == 'ENTRY' else 'FILL_SALIDA' if orden['tipo_orden'] in ('TAKE_PROFIT', 'CIERRE') else 'FILL_DESCONOCIDO'
                    await self._procesar_trade_con_pnl(state, fill, tipo_evento)

                    await actualizar_orden_fill(
                        orden_id=orden['id'],
                        binance_trade_id=str(binance_trade_id),
                        price=price,
                        qty=qty,
                        commission=commission,
                        commission_asset=fill.get('commission_asset', ''),
                        realized_pnl=float(fill.get('realized_pnl', 0)),
                        timestamp=datetime.fromtimestamp(ts).isoformat() if ts > 0 else datetime.utcnow().isoformat()
                    )
                except Exception as e_side:
                    print(f"  ⚠️ [FASE 3] {symbol} Fill {fill_id} side-effect falló (TP/PnL/orden) pero YA MARCADO como procesado: {e_side}")

                print(f"  [CR16] {symbol} Fill procesado: {side} @ ${price:.4f} | Modo: {state.grid_mode} | Qty: {qty}")

            except Exception as e:
                print(f"  ❌ [FASE 3] {symbol} Error procesando fill {fill_id}: {e}")
            finally:
                # CRÍTICO: Si no se marcó como procesado, marcarlo como ERROR para romper loop infinito
                if not procesado_ok:
                    try:
                        await marcar_fill_procesado(fill_id, 'ERROR_LOOP')
                        print(f"  [FASE 3] {symbol} Fill {fill_id} marcado como ERROR_LOOP para evitar reintento infinito")
                    except Exception as e2:
                        print(f"  🚨 [FASE 3] {symbol} CRÍTICO: No se pudo marcar fill {fill_id} como ERROR_LOOP: {e2}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 2: PROCESAMIENTO DE FILLS DIRECCIONALES
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _procesar_fill_direccional(self, state: GridExecutionState, fill: dict, orden: dict) -> str:
        """
        E.4 FIX: Procesa un fill para grid LONG/SHORT actualizando posición neta y PnL real.
        Crea PosicionReal para tracking de TP. Retorna pos_id.
        Usa realized_pnl de Binance (ya calculado por reducción de posición).
        """
        side = fill['side']
        price = float(fill['price'])
        qty = float(fill['qty'])
        commission = float(fill['commission'])
        realized_pnl = float(fill.get('realized_pnl', 0))
        ts_raw = fill.get('timestamp_ms')
        try:
            ts = int(ts_raw or 0) // 1000
        except (TypeError, ValueError):
            ts = 0

        # Actualizar posición neta (modelo acumulativo de Binance Futures)
        if side == 'BUY':
            state.posicion_neta += Decimal(str(qty))
        else:
            state.posicion_neta -= Decimal(str(qty))

        # Actualizar fees y PnL real
        state.fees_real += Decimal(str(commission))
        if realized_pnl != 0:
            state.pnl_real += Decimal(str(realized_pnl))

        # E.4 FIX: Crear PosicionReal para tracking de TP
        pos = PosicionReal(
            tipo='LONG' if side == 'BUY' else 'SHORT',
            nivel_precio=price,
            precio_ejecucion=price,
            qty=qty,
            fee_pagada=commission,
            timestamp_apertura=ts,
            binance_order_id=str(fill.get('binance_order_id', '')),
            filled_qty=qty
        )

        if not hasattr(state, 'posiciones_direccionales'):
            state.posiciones_direccionales = []
        state.posiciones_direccionales.append(pos)

        # Tracking en memoria para referencia rápida
        state.ordenes[fill['binance_order_id']] = {
            'side': side,
            'price': price,
            'qty': qty,
            'filled': True,
            'realized_pnl': realized_pnl,
            'timestamp': ts,
            'pos_id': pos.id  # ← E.4 FIX
        }

        print(f"  [EXECUTOR] {state.symbol} Fill {state.grid_mode}: {side} {qty} @ ${price:.4f} | "
              f"Pos: {pos.id} | Pos neta: {float(state.posicion_neta):+.4f}")

        return pos.id  # ← E.4 FIX

    async def _monitorear_ordenes_abiertas(self, symbol: str, state: GridExecutionState):
        """
        CR16: Monitoreo reactivo como fallback.
        Solo detecta órdenes que cambiaron de estado sin ser vistas por el poll proactivo.
        """
        # 1. Consultar órdenes abiertas
        open_orders = await self._api_call(asyncio.to_thread(
            self.client.futures_get_open_orders, symbol=symbol
        ))
        open_ids = {str(o['orderId']) for o in open_orders}

        # 2. Cargar nuestras órdenes de DB
        db_ordenes = await cargar_ordenes_por_grid(state.grid_id)

        # 3. Detectar órdenes que deberían estar abiertas pero no lo están
        for orden in db_ordenes:
            if orden['status'] != 'NEW':
                continue  # Ya procesada

            orden_id_raw = orden.get('binance_order_id')
            if not orden_id_raw or str(orden_id_raw).lower() == 'none':
                print(f"  ⚠️ [CR16] {symbol} Orden DB {orden['id']} sin binance_order_id válido, saltando")
                continue
            orden_id = str(orden_id_raw)

            if orden_id not in open_ids:
                # La orden desapareció de abiertas → puede estar FILLED o CANCELED
                # Consultar estado exacto
                try:
                    order_info = await self._api_call(asyncio.to_thread(
                        self.client.futures_get_order,
                        symbol=symbol,
                        orderId=orden_id
                    ))

                    # FASE 6: Proteger acceso si Binance devuelve respuesta inesperada
                    if order_info.get('status') == 'FILLED':
                        # Este fill debería haber sido capturado por el poll proactivo
                        # Si llegamos aquí, el poll proactivo falló → anomalía
                        print(f"  🚨 [CR16] {symbol} Orden {orden_id} FILLED no detectada "
                              f"por poll proactivo")

                        # Procesar de todas formas (doble seguridad)
                        await self._procesar_fill_desde_orden_info(state, order_info)

                    elif order_info['status'] == 'CANCELED':
                        # P5 FIX: si la canceló el propio bot (E.2/E.3/E.5/kill switch),
                        # no es una anomalía — log suave y limpieza del registro.
                        if orden_id in self._cancelaciones_propias:
                            print(f"  [P5] {symbol} Orden {orden_id} cancelada por el bot (propia, ignorada)")
                            self._cancelaciones_propias.discard(orden_id)
                        else:
                            print(f"  [CR16] {symbol} Orden {orden_id} cancelada externamente")
                        # Actualizar DB
                        await _execute_with_retry(
                            "UPDATE grid_ejecucion_ordenes SET status = 'CANCELED' WHERE id = ?",
                            (orden['id'],)
                        )

                except Exception as e:
                    print(f"  ⚠️ [CR16] {symbol} Error consultando orden {orden_id}: {e}")


    async def _procesar_fill_desde_orden_info(self, state: GridExecutionState, order_info: dict):
        """
        Procesa un fill a partir de la info de la orden (fallback del poll proactivo).
        """
        # Consultar trades de esta orden
        try:
            trades = await self._api_call(asyncio.to_thread(
                self.client.futures_account_trades,
                symbol=state.symbol,
                orderId=order_info['orderId']
            ))

            if not trades:
                return

            # Procesar el último trade (el que completó la orden)
            trade = trades[-1]

            # Guardar en tracking
            await guardar_fill_tracking(
                grid_ejecucion_id=state.grid_id,
                symbol=state.symbol,
                binance_trade_id=trade['id'],
                binance_order_id=str(order_info['orderId']),
                side=trade['side'],
                price=float(trade['price']),
                qty=float(trade['qty']),
                commission=float(trade['commission']),
                commission_asset=trade['commissionAsset'],
                realized_pnl=float(trade.get('realizedPnl', 0)),
                timestamp_ms=trade['time']
            )

            # Procesar fills pendientes (incluirá este)
            await self._procesar_fills_pendientes(state.symbol, state)

        except Exception as e:
            print(f"  ⚠️ [CR16] Error procesando fill fallback: {e}")


    async def _monitorear_grid(self, symbol: str):
        """
        CR16 FIX: Monitoreo proactivo de fills + reactivo como fallback.
        FASE 4 FIX: Kill switch adaptativo por modo.

        Flujo:
        1. Poll proactivo de trades (descubre fills nuevos)
        2. Procesar fills pendientes (aplica al estado)
        3. Monitoreo reactivo de órdenes abiertas (detecta cambios de estado)
        4. Evaluar kill switch
        5. Reconciliar con Binance
        6. Reconciliar PnL con DB
        """
        state = self._grids[symbol]

        if state.cerrando:
            return

        # ─── SYNC: Sincronizar órdenes abiertas de Binance con DB local ───
        try:
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders, symbol=symbol
            ))
            db_ordenes = await cargar_ordenes_por_grid(state.grid_id)
            db_order_ids = {str(o['binance_order_id']): o for o in db_ordenes if o.get('binance_order_id')}

            for o in (open_orders or []):
                oid = str(o.get('orderId', ''))
                if oid and oid not in db_order_ids and o.get('clientOrderId', '').startswith('CM'):
                    sync_coid = o.get('clientOrderId', f"SYNC_{oid}")
                    # FIX FASE 4: Verificar client_order_id contra DB para evitar UNIQUE constraint
                    # (db_order_ids es dict key=binance_order_id; el UNIQUE está en client_order_id)
                    existe_coid = any(str(db_o.get('client_order_id')) == sync_coid for db_o in db_ordenes)
                    if not existe_coid:
                        await guardar_orden_ejecucion(
                            grid_ejecucion_id=state.grid_id,
                            binance_order_id=oid,
                            client_order_id=sync_coid,
                            symbol=symbol,
                            side=o['side'],
                            tipo_orden='ENTRY',
                            price=float(o.get('price', 0)),
                            quantity=float(o.get('origQty', 0))
                        )
                        # FIX FASE 4: Registrar en el dict para no re-sincronizar en este ciclo
                        db_order_ids[oid] = o
                        print(f"  [SYNC] {symbol} Orden huérfana {oid} sincronizada.")
                    else:
                        print(f"  [SYNC] {symbol} Orden {oid} ya en DB (client_order_id={sync_coid}), skip.")
        except Exception as e_sync:
            print(f"  ⚠️ [SYNC] {symbol} Error sincronizando órdenes: {e_sync}")
            
        # ═══════════════════════════════════════════════════════════════════
        # CR16 PASO 1: POLL PROACTIVO DE TRADES
        # ═══════════════════════════════════════════════════════════════════
        nuevos_fills = await self._poll_fills_proactivo(symbol, state)

        # ═══════════════════════════════════════════════════════════════════
        # CR16 PASO 2: PROCESAR FILLS PENDIENTES
        # ═══════════════════════════════════════════════════════════════════
        await self._procesar_fills_pendientes(symbol, state)

        # ═══════════════════════════════════════════════════════════════════
        # FASE 6: RECONCILIACIÓN RÁPIDA DE POSICIÓN NETA vs BINANCE
        # Se ejecuta cada ciclo de monitoreo. Si diverge >1%, fuerza sync.
        # P6 FIX: movido DESPUÉS del poll+procesamiento de fills — antes corría
        # ANTES del poll y el guard P3 no veía los fills recién llegados
        # (doble conteo transitorio LDO: sync a -267 + 8 fills = -396).
        # ═══════════════════════════════════════════════════════════════════
        try:
            # P3 FIX: Si hay fills pendientes de procesar, Binance ya los refleja
            # pero el estado interno aún no. Forzar sync aquí y luego aplicar esos
            # fills en el pipeline CR16 (corre justo después) produce DOBLE CONTEO
            # (caso real: ARB sync a -2252.2 + fill re-procesado = -3378.3, que
            # gatilló un aborto de emergencia con pérdida real). Omitir el sync
            # este ciclo: el pipeline deja la neta correcta y el próximo ciclo
            # la reconciliación corre limpia.
            fills_pend_sync = await cargar_fills_sin_procesar(state.grid_id)
            if fills_pend_sync:
                print(f"  [P3] {symbol} Sync FASE 6 omitido: {len(fills_pend_sync)} "
                      f"fills pendientes de procesar (anti doble conteo)")
            else:
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information,
                    symbol=symbol
                ))
                if position and len(position) > 0:
                    # FIX HEDGE-NET: leer la posición correcta según modo y tipo de grid.
                    # Antes position[0] leía solo UNA pierna en HEDGE → falsas divergencias
                    # y reseteo de posicion_neta en grids SHORT direccionales y NEUTRAL.
                    if self._hedge_mode and state.grid_mode in ('LONG', 'SHORT'):
                        pos_amt_real = self._get_position_amt_hedge(position, state.grid_mode)
                    else:
                        pos_amt_real = self._get_position_neta_hedge(position)
                    pos_amt_interno = state.posicion_neta
                    if pos_amt_real != 0 and abs(pos_amt_interno) > Decimal('0.0001'):
                        divergencia_pct = abs((pos_amt_real - pos_amt_interno) / pos_amt_real) * 100
                        if divergencia_pct > Decimal('1.0'):
                            # P9 FIX: solo DETECTAR — NO forzar sync aquí. Binance puede
                            # ir por delante del poll de fills (trades aún no consultados);
                            # sincronizar ahora produce doble conteo cuando el poll entrega
                            # esos fills (caso real: ARB sync a 39.5 + fills pendientes =
                            # interno -17.0). La única autoridad de sync es
                            # _reconciliar_con_binance (PASO 5), que pollea trades
                            # recientes ANTES de comparar → no puede doble-contar.
                            # Si la divergencia es real, PASO 5 la corrige en este ciclo.
                            print(f"  🚨 [P9] {symbol} DIVERGENCIA POSICIÓN detectada: "
                                  f"Binance={float(pos_amt_real):.4f} vs "
                                  f"Interno={float(pos_amt_interno):.4f} "
                                  f"({float(divergencia_pct):.2f}%). Sin sync forzado, "
                                  f"PASO 5 reconcilia.")
                    elif pos_amt_real == 0 and abs(pos_amt_interno) > Decimal('0.0001'):
                        print(f"  🚨 [P9] {symbol} Posición real=0 pero interno={float(pos_amt_interno):.4f}. "
                              f"Sin sync forzado, PASO 5 reconcilia.")
        except Exception as e_rec:
            # FASE 6: No bloquear monitoreo si reconciliación falla
            print(f"  ⚠️ [FASE 6] {symbol} Error reconciliando posición: {e_rec}")

        # ═══════════════════════════════════════════════════════════════════
        # CR16 PASO 3: MONITOREO REACTIVO (fallback)
        # Detecta órdenes que cambiaron de estado sin que las viéramos
        # ═══════════════════════════════════════════════════════════════════
        await self._monitorear_ordenes_abiertas(symbol, state)

        # ═══════════════════════════════════════════════════════════════════
        # PASO 4: EVALUAR KILL SWITCH
        # ═══════════════════════════════════════════════════════════════════
        if state.grid_mode == 'NEUTRAL' and state.grid_state:
            precio_actual = self.precios_vivo.get(symbol, 0)
            if precio_actual > 0:
                acciones = await self._evaluar_kill_switch(
                    state=state,
                    precio_actual=precio_actual,
                    timestamp=int(time.time())
                )
                for accion in acciones:
                    if accion['tipo'] == 'KILL_SWITCH':
                        await self._ejecutar_kill_switch_real(
                            symbol=symbol, state=state, **accion
                        )

        elif state.grid_mode in ('LONG', 'SHORT'):
            # FASE 4: Kill switch direccional
            await self._evaluar_kill_switch_direccional(symbol, state)

        # ═══════════════════════════════════════════════════════════════════
        # PASO 5: RECONCILIACIÓN
        # ═══════════════════════════════════════════════════════════════════
        await self._reconciliar_con_binance(state)

        # ═══════════════════════════════════════════════════════════════════
        # CR2 PASO 6: RECONCILIAR PnL CON DB
        # ═══════════════════════════════════════════════════════════════════
        await self._reconciliar_pnl(state)

        # E.6 FIX: Fallback — verificar que cada posición abierta tenga TP
        if state.grid_mode == 'NEUTRAL' and state.grid_state:
            for pos in state.grid_state.posiciones:
                if pos.estado == 'ABIERTA' and not pos.orden_cierre_id:
                    print(f"  🚨 [E.6] {symbol} Pos {pos.id} ABIERTA sin TP. Intentando recrear...")
                    try:
                        db = await _get_db()
                        async with _db_lock:
                            cursor = await db.execute(
                                "SELECT * FROM fills_tracking WHERE binance_order_id = ? AND procesado = 1 LIMIT 1",
                                (pos.binance_order_id,)
                            )
                            row = await cursor.fetchone()
                            if row:
                                fill_fallback = dict(row)
                                ordenes_db = await cargar_ordenes_por_grid(state.grid_id)
                                orden_padre = next((
                                    o for o in ordenes_db
                                    if str(o['binance_order_id']) == pos.binance_order_id
                                ), None)
                                if orden_padre:
                                    await self._colocar_take_profit_neutral(
                                        symbol, state, orden_padre, fill_fallback,
                                        'BUY' if pos.tipo == 'LONG' else 'SELL',
                                        pos.id
                                    )
                    except Exception as e:
                        print(f"  ⚠️ [E.6] {symbol} Error fallback TP para {pos.id}: {e}")

        elif state.grid_mode in ('LONG', 'SHORT') and hasattr(state, 'posiciones_direccionales'):
            for pos in state.posiciones_direccionales:
                if pos.estado == 'ABIERTA' and not pos.orden_cierre_id:
                    print(f"  🚨 [E.6] {symbol} Pos direccional {pos.id} ABIERTA sin TP. Intentando recrear...")
                    try:
                        db = await _get_db()
                        async with _db_lock:
                            cursor = await db.execute(
                                "SELECT * FROM fills_tracking WHERE binance_order_id = ? AND procesado = 1 LIMIT 1",
                                (pos.binance_order_id,)
                            )
                            row = await cursor.fetchone()
                            if row:
                                fill_fallback = dict(row)
                                ordenes_db = await cargar_ordenes_por_grid(state.grid_id)
                                orden_padre = next((
                                    o for o in ordenes_db
                                    if str(o['binance_order_id']) == pos.binance_order_id
                                ), None)
                                if orden_padre:
                                    fill_fallback['pos_id'] = pos.id
                                    await self._colocar_take_profit(symbol, state, orden_padre, fill_fallback)
                    except Exception as e:
                        print(f"  ⚠️ [E.6] {symbol} Error fallback TP para {pos.id}: {e}")


    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 4: KILL SWITCH ADAPTATIVO POR MODO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _evaluar_kill_switch_direccional(self, symbol: str, state: GridExecutionState):
        """
        FASE 4.2: Para direccionales: cierra el grid completo si:
        1. Hay posición abierta pero no hay órdenes de entrada ni TP pendientes (grid agotado)
        2. Timeout desde creación del grid (configurable)
        3. PnL unrealizado excede umbral negativo (opcional, futuro)
        """
        if not state.activa or state.cerrando:
            return

        # Timeout global del grid (no por posición, ya que es posición neta)
        tiempo_vida = int(time.time()) - int(state.timestamp_inicio)
        timeout_grid = getattr(CONFIG, 'grid_direccional_timeout_min', 120) * 60  # default 2h
        
        # FASE E.2: En HEDGE MODE, leer posición del lado específico
        if self._hedge_mode and state.grid_mode in ('LONG', 'SHORT'):
            try:
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information,
                    symbol=symbol
                ))
                pos_actual = float(self._get_position_amt_hedge(position, state.grid_mode))
            except Exception:
                pos_actual = float(state.posicion_neta)
        else:
            pos_actual = float(state.posicion_neta)

        if tiempo_vida > timeout_grid and pos_actual != 0:
            print(f"  🛑 [EXECUTOR] {symbol} Grid {state.grid_mode} vencido: {tiempo_vida}s > {timeout_grid}s")
            await self._abortar_grid(symbol, f'timeout_direccional_{tiempo_vida}s')
            return

        # Si posición neta es 0 pero aún hay órdenes abiertas, es normal (esperando fills)
        # Si posición neta es 0 y NO hay órdenes, el auto-cleanup de _monitoring_loop se encargará


    async def _reconciliar_con_binance(self, state: GridExecutionState):
        """
        CR16: Reconciliación completa del estado interno vs Binance.
        Detecta fills perdidos, posiciones huérfanas, órdenes fantasmas.
        FASE 5: Unificada para NEUTRAL y DIRECCIONAL.
        """
        if not state.activa:
            return
        if state.grid_mode == 'NEUTRAL' and (not state.grid_state or not state.grid_state.activa):
            return

        try:
            # 1. Posición real en Binance
            position = await self._api_call(asyncio.to_thread(
                self.client.futures_position_information,
                symbol=state.symbol
            ))

            # FASE E.1: Usar helper HEDGE para leer posición del lado correcto
            # FIX HEDGE-NET: NEUTRAL en hedge debe sumar AMBAS piernas, no position[0]
            if self._hedge_mode and state.grid_mode in ('LONG', 'SHORT'):
                pos_amt_real = self._get_position_amt_hedge(position, state.grid_mode)
            else:
                pos_amt_real = self._get_position_neta_hedge(position)
            pos_amt_interno = state.posicion_neta

            # 2. Trades recientes (últimos 5 minutos) para detectar fills perdidos
            # P2-A FIX: nunca por debajo del nacimiento del grid
            cinco_min_atras = int((time.time() - 300) * 1000)
            grid_inicio_ms_rec = int(getattr(state, 'timestamp_inicio', 0) or 0) * 1000
            start_time_rec = max(cinco_min_atras, grid_inicio_ms_rec) if grid_inicio_ms_rec > 0 else cinco_min_atras
            trades_recientes = await self._api_call(asyncio.to_thread(
                self.client.futures_account_trades,
                symbol=state.symbol,
                startTime=start_time_rec
            ))

            # Guardar todos los trades recientes en tracking
            for trade in (trades_recientes or []):
                await guardar_fill_tracking(
                    grid_ejecucion_id=state.grid_id,
                    symbol=state.symbol,
                    binance_trade_id=trade['id'],
                    binance_order_id=str(trade['orderId']),
                    side=trade['side'],
                    price=float(trade['price']),
                    qty=float(trade['qty']),
                    commission=float(trade['commission']),
                    commission_asset=trade['commissionAsset'],
                    realized_pnl=float(trade.get('realizedPnl', 0)),
                    timestamp_ms=trade['time']
                )

            # Procesar cualquier fill nuevo
            await self._procesar_fills_pendientes(state.symbol, state)

            # 3. Verificar discrepancia de posición
            # P6 FIX: re-leer posicion_neta DESPUÉS del procesamiento de fills de
            # este mismo paso (antes se comparaba contra el valor capturado al
            # entrar → falsos "posición huérfana detectada" y syncs innecesarios,
            # ej: LDO -85 vs -138 e INJ 5.6 vs 17.8 del log del día).
            pos_amt_interno = state.posicion_neta
            if abs(pos_amt_real - pos_amt_interno) > Decimal('0.0001'):
                print(f"  🚨 [CR16 RECONCILIACIÓN] {state.symbol} "
                      f"Posición: Binance={float(pos_amt_real):.4f} vs "
                      f"Interno={float(pos_amt_interno):.4f}")

                # Corrección: ajustar posicion_neta al valor real
                # PERO primero investigar por qué divergió
                fills_no_procesados = await cargar_fills_sin_procesar(state.grid_id)
                if fills_no_procesados:
                    print(f"   → {len(fills_no_procesados)} fills sin procesar, aplicando...")
                    await self._procesar_fills_pendientes(state.symbol, state)
                else:
                    print(f"   → Sin fills pendientes, posición huérfana detectada")
                    # Ajustar forzosamente (último recurso)
                    state.posicion_neta = pos_amt_real

            # 4. Órdenes abiertas en Binance vs tracking interno
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders,
                symbol=state.symbol
            ))
            open_orders = open_orders or []  # ← PARCHE C: Guarda defensiva

            # PARCHE C: Construir set de TODAS nuestras órdenes (entrada + TP) por prefix
            prefix = f"CM{state.grid_id}"
            nuestras_ordenes_ids = set()
            for o in open_orders:
                cid = o.get('clientOrderId', '')
                if cid.startswith(prefix):
                    nuestras_ordenes_ids.add(str(o['orderId']))

            # FASE 5: Reconciliación de órdenes solo para NEUTRAL (tiene tracking de TP)
            if state.grid_mode == 'NEUTRAL' and state.grid_state:
                open_ids_binance = {str(o['orderId']) for o in open_orders}
                open_ids_interno = set(state.grid_state.ordenes_tp_pendientes.values())

                # PARCHE C: Solo reportar/logs si la orden NO es nuestra (ni entrada ni TP)
                for oid in open_ids_binance - open_ids_interno:
                    if oid not in nuestras_ordenes_ids:
                        # Es una orden que no reconocemos como nuestra → posible huérfana ajena
                        print(f"  🚨 [CR16] {state.symbol} Orden ajena/huérfana en Binance: {oid}")
                    # Si ES nuestra orden de entrada, NO reportar (no es huérfana, es normal)

                # Órdenes que creímos abiertas pero Binance no las tiene
                for pos_id, oid in list(state.grid_state.ordenes_tp_pendientes.items()):
                    if oid not in open_ids_binance:
                        print(f"  ⚠️ [CR16] {state.symbol} TP {oid} desaparecido, limpiando tracking")
                        del state.grid_state.ordenes_tp_pendientes[pos_id]

                # PARCHE C.2: Cancelar órdenes ajenas con prefix CM detectadas en reconciliación
                for oid in open_ids_binance - open_ids_interno:
                    if oid not in nuestras_ordenes_ids:
                        # Buscar la orden en open_orders para verificar su origen
                        orden_ajena = next(
                            (o for o in open_orders if str(o.get('orderId')) == oid), None
                        )
                        if orden_ajena:
                            cid = orden_ajena.get('clientOrderId', '')
                            # Solo actuar si es una orden de OTRO grid (prefix CM pero no CM{grid_id})
                            if cid.startswith('CM') and not cid.startswith(prefix):
                                print(f"  🚨 [CR16] {state.symbol} Orden ajena {oid} ({cid}) detectada. Cancelando...")
                                try:
                                    await self._api_call(asyncio.to_thread(
                                        self.client.futures_cancel_order,
                                        symbol=state.symbol,
                                        orderId=oid
                                    ))
                                    print(f"  ✅ [CR16] {state.symbol} Orden ajena {oid} cancelada desde reconciliación")
                                except Exception as e:
                                    # Si falla (ej: ya fue cancelada por otro grid), no propagar
                                    print(f"  ⚠️ [CR16] {state.symbol} No se pudo cancelar orden ajena {oid}: {e}")
                            elif not cid.startswith('CM'):
                                # Orden de origen externo (no del bot) — solo log silencioso
                                print(f"  [CR16] {state.symbol} Orden externa no-CM: {oid}")

        except Exception as e:
            print(f"  ⚠️ [CR16] Error en reconciliación: {e}")


    async def _reportar_metricas_cr16(self, state: GridExecutionState):
        """Reporta métricas específicas del fix CR16."""
        if not state.grid_state:
            return

        # Contar fills por método de detección
        db = await _get_db()
        async with _db_lock:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM fills_tracking
                WHERE grid_ejecucion_id = ?
            """, (state.grid_id,))
            total_fills = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM fills_tracking
                WHERE grid_ejecucion_id = ? AND procesado = 1
            """, (state.grid_id,))
            fills_procesados = (await cursor.fetchone())[0]

        print(f"  [CR16 METRICS] {state.symbol} "
              f"Fills totales: {total_fills} | "
              f"Procesados: {fills_procesados} | "
              f"Pendientes: {total_fills - fills_procesados}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # CR2 FIX: PROCESAR TRADE CON PnL Y RECONCILIACIÓN
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _procesar_trade_con_pnl(self, state: GridExecutionState, trade: dict,
                                        tipo_evento: str) -> dict:
        """
        CR2 FIX: Procesa un trade de Binance extrayendo y persistiendo el PnL real.
        Cada trade genera un evento de PnL que se persiste inmediatamente en DB.

        FASE 1 FIX: Normaliza mapeo de claves entre API Binance (orderId, id, time)
        y filas de SQLite (binance_order_id, binance_trade_id, timestamp_ms).
        Evita KeyError cuando el trade proviene de fills_tracking (DB).
        """
        # ─── Normalización defensiva de claves (API vs DB) ───
        price_raw = trade.get('price')
        qty_raw = trade.get('qty')
        side_raw = trade.get('side')
        commission_raw = trade.get('commission')

        # Identificadores con fallback API → DB
        binance_trade_id = str(trade.get('binance_trade_id') or trade.get('id') or 'UNKNOWN')
        binance_order_id = str(trade.get('binance_order_id') or trade.get('orderId') or 'UNKNOWN')
        timestamp_ms = trade.get('timestamp_ms') or trade.get('time') or 0
        commission_asset = trade.get('commissionAsset') or trade.get('commission_asset') or ''
        realized_pnl = float(trade.get('realizedPnl') or trade.get('realized_pnl') or 0)

        # Validación mínima: si faltan datos críticos, abortar sin excepción
        if any(v is None for v in [price_raw, qty_raw, side_raw, commission_raw]):
            print(f"  ⚠️ [PnL] {state.symbol} Trade descartado: datos críticos faltantes "
                  f"(price={price_raw}, qty={qty_raw}, side={side_raw}, comm={commission_raw})")
            return {'realized_pnl': 0, 'commission': 0, 'notional': 0, 'descartado': True}

        if binance_trade_id == 'UNKNOWN' or binance_order_id == 'UNKNOWN':
            print(f"  ⚠️ [PnL] {state.symbol} Trade con IDs desconocidos: "
                  f"trade_id={binance_trade_id}, order_id={binance_order_id}. "
                  f"Se procesa PnL pero trazabilidad limitada.")

        price = float(price_raw)
        qty = float(qty_raw)
        commission = float(commission_raw)
        notional = price * qty

        # Persistir inmediatamente en DB (fuente de verdad)
        await guardar_pnl_evento(
            grid_ejecucion_id=state.grid_id,
            symbol=state.symbol,
            tipo_evento=tipo_evento,
            side=side_raw,
            binance_trade_id=binance_trade_id,
            binance_order_id=binance_order_id,
            price=price,
            qty=qty,
            commission=commission,
            commission_asset=commission_asset,
            realized_pnl=realized_pnl,
            notional=notional,
            timestamp_ms=int(timestamp_ms)
        )

        # Actualizar estado en RAM (para respuesta rápida)
        state.fees_real += Decimal(str(commission))

        # El realized_pnl solo se suma al estado si es un evento de cierre
        # (Las entradas tienen realized_pnl = 0, los cierres tienen el PnL real)
        if realized_pnl != 0:
            state.pnl_real += Decimal(str(realized_pnl))
            print(f"  [PnL] {state.symbol} {tipo_evento} | "
                  f"Realized PnL: {realized_pnl:+.4f} | "
                  f"Commission: {commission:.4f} | "
                  f"Trade: {binance_trade_id}")

        return {
            'realized_pnl': realized_pnl,
            'commission': commission,
            'notional': notional
        }

    async def _reconciliar_pnl(self, state: GridExecutionState):
        """
        CR2 FIX: Reconcilia el PnL en RAM con la base de datos (fuente de verdad).
        Detecta discrepancias y corrige el estado interno.
        """
        if not state.grid_state:
            return

        # Calcular PnL desde DB (fuente de verdad)
        pnl_db = await calcular_pnl_acumulado(state.grid_id)

        pnl_db_val = Decimal(str(pnl_db['pnl_real']))
        fees_db_val = Decimal(str(pnl_db['fees_real']))

        pnl_ram = state.pnl_real
        fees_ram = state.fees_real

        # Detectar discrepancia
        discrepancia_pnl = abs(pnl_db_val - pnl_ram)
        discrepancia_fees = abs(fees_db_val - fees_ram)

        if discrepancia_pnl > Decimal('0.0001') or discrepancia_fees > Decimal('0.0001'):
            print(f"  🚨 [PnL RECONCILIACIÓN] {state.symbol} Discrepancia detectada:")
            print(f"     PnL RAM: {float(pnl_ram):.4f} | DB: {float(pnl_db_val):.4f} | Diff: {float(discrepancia_pnl):.4f}")
            print(f"     Fees RAM: {float(fees_ram):.4f} | DB: {float(fees_db_val):.4f} | Diff: {float(discrepancia_fees):.4f}")

            # Corregir: DB gana
            state.pnl_real = pnl_db_val
            state.fees_real = fees_db_val

            print(f"  ✅ [PnL RECONCILIACIÓN] {state.symbol} Estado corregido desde DB")

        # Actualizar GridState con PnL reconciliado
        state.grid_state.pnl_bruto = pnl_db_val + fees_db_val
        state.grid_state.pnl_neto = pnl_db_val
        state.grid_state.fees_totales = fees_db_val

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3: TAKE-PROFIT PARA DIRECCIONALES (INTEGRACIÓN COMPLETA)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _colocar_take_profit(self, symbol: str, state: GridExecutionState,
                                orden_entry: dict, fill: dict):
        """Coloca take-profit para un fill de entrada (LONG o SHORT).
        E.3 FIX: Recibe fill para usar qty real y precio de ejecución, reduceOnly.
        """
        if not state.niveles:
            print(f"  ⚠️ [E.3] {symbol} Sin niveles disponibles, no se puede colocar TP")
            return

        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        entry_price = Decimal(str(fill.get('price', orden_entry['price'])))
        fill_qty = float(fill.get('qty', orden_entry.get('quantity', 0)))
        if fill_qty <= 0:
            print(f"  ⚠️ [E.3] {symbol} Fill qty inválido ({fill_qty}), abortando TP")
            return

        binance_order_id = str(fill.get('binance_order_id', orden_entry.get('binance_order_id', '')))

        if state.direction == 'LONG':
            siguiente_nivel = None
            for nivel in state.niveles:
                if Decimal(str(nivel)) > entry_price * Decimal('1.0001'):
                    siguiente_nivel = nivel
                    break
            if not siguiente_nivel:
                print(f"  ⚠️ [E.3] {symbol} LONG: No hay nivel superior a {float(entry_price):.4f} para TP. "
                      f"Niveles: {[float(n) for n in state.niveles]}")
                return
            side = 'SELL'
        else:
            siguiente_nivel = None
            for nivel in reversed(state.niveles):
                if Decimal(str(nivel)) < entry_price * Decimal('0.9999'):
                    siguiente_nivel = nivel
                    break
            if not siguiente_nivel:
                print(f"  ⚠️ [E.3] {symbol} SHORT: No hay nivel inferior a {float(entry_price):.4f} para TP. "
                      f"Niveles: {[float(n) for n in state.niveles]}")
                return
            side = 'BUY'

        ticks = int(Decimal(str(siguiente_nivel)) / tick_size)
        tp_price = float(ticks * tick_size)

        # E.3 FIX: Verificar si ya existe TP para este orden (fills parciales)
        qty_total = fill_qty
        tp_previo = None
        if binance_order_id:
            for cid, meta in list(state.ordenes.items()):
                if (meta.get('tipo') == 'TAKE_PROFIT' and
                    meta.get('orden_padre') == binance_order_id):
                    tp_previo = meta
                    break

            if tp_previo:
                print(f"  [E.3] {symbol} TP existente para orden {binance_order_id}. "
                      f"Actualizando qty: {tp_previo.get('qty', 0)} + {fill_qty}")
                try:
                    self._registrar_cancelacion_propia(tp_previo['binance_order_id'])  # P5
                    await self._api_call(asyncio.to_thread(
                        self.client.futures_cancel_order,
                        symbol=symbol,
                        orderId=tp_previo['binance_order_id']
                    ))
                    print(f"  [E.3] {symbol} TP anterior cancelado para recrear con qty acumulada")
                    for k in list(state.ordenes.keys()):
                        if state.ordenes[k].get('binance_order_id') == tp_previo['binance_order_id']:
                            del state.ordenes[k]
                except Exception as e:
                    print(f"  ⚠️ [E.3] {symbol} Error cancelando TP anterior: {e}")
                qty_total = float(tp_previo.get('qty', 0)) + fill_qty

        client_order_id = f"CM{state.grid_id}_TP_{binance_order_id}_{int(time.time()*1000)}"

        try:
            orden_tp = {
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'quantity': qty_total,
                'price': tp_price,
                'timeInForce': 'GTC',
                'newClientOrderId': client_order_id
            }
            # FASE C.1: HEDGE MODE requiere positionSide en TP
            # P1-A FIX: en HEDGE MODE Binance rechaza reduceOnly (-1106).
            # Hedge → solo positionSide; ONE-WAY → solo reduceOnly.
            if self._hedge_mode:
                orden_tp['positionSide'] = state.direction  # 'LONG' o 'SHORT'
            else:
                orden_tp['reduceOnly'] = True
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                **orden_tp
            ))

            await guardar_orden_ejecucion(
                grid_ejecucion_id=state.grid_id,
                binance_order_id=str(res['orderId']),
                client_order_id=client_order_id,
                symbol=symbol, side=side, tipo_orden='TAKE_PROFIT',
                price=tp_price, quantity=qty_total
            )

            state.ordenes[client_order_id] = {
                'side': side, 'price': tp_price, 'qty': qty_total,
                'tipo': 'TAKE_PROFIT', 'binance_order_id': str(res['orderId']),
                'orden_padre': binance_order_id
            }

            # E.3 FIX: Asociar TP a posición si se pasó pos_id
            pos_id = fill.get('pos_id')
            if pos_id and hasattr(state, 'posiciones_direccionales'):
                for pos in state.posiciones_direccionales:
                    if pos.id == pos_id and pos.estado == 'ABIERTA':
                        pos.orden_cierre_id = str(res['orderId'])
                        print(f"  [E.3] {symbol} TP asociado a posición {pos_id}")
                        break

            print(f"  ✅ [E.3] {symbol} TP {side} reduceOnly @ ${tp_price} | Qty:{qty_total} | Origen:{binance_order_id}")

        except Exception as e:
            print(f"  ⚠️ [E.3] {symbol} Error colocando TP {side} @ ${tp_price}: {e}")

    async def _colocar_take_profit_neutral(self, symbol: str, state: GridExecutionState,
                                            orden_entry: dict, fill: dict, side_filled: str, pos_id: str):
        """Cuando se ejecuta un BUY, coloca SELL take-profit. Cuando SELL, coloca BUY.
        E.2 FIX: reduceOnly, cancela orden de entrada conflictiva, usa qty del fill.
        """
        gs = state.grid_state if state else None
        if not gs:
            print(f"  ⚠️ [E.2] {symbol} Sin grid_state, no se puede colocar TP neutral")
            return

        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))
        precio_entry = Decimal(str(fill.get('price', orden_entry['price'])))

        # P8 FIX: El TP va al nivel INMEDIATO de la escalera completa del grid
        # (niveles_buy + niveles_sell ordenados), no al primer nivel del lado
        # contrario del centro. Antes solo se miraban niveles_sell (para LONG) o
        # niveles_buy (para SHORT): dos posiciones distintas podían recibir TP al
        # MISMO precio (caso real LDO: pos_001 y pos_003 ambas con TP @ 0.3753,
        # visto por el usuario como "órdenes duplicadas") y los niveles
        # intermedios quedaban muertos para el round-trip.
        escalera = sorted(set(list(gs.niveles_buy or []) + list(gs.niveles_sell or [])))
        if not escalera:
            print(f"  ⚠️ [E.2] {symbol} Escalera de niveles vacía, no se puede colocar TP")
            return

        if side_filled == 'BUY':
            candidatos = [n for n in escalera if n > precio_entry * Decimal('1.0001')]
            if not candidatos:
                print(f"  ⚠️ [E.2] {symbol} No hay nivel superior a {float(precio_entry):.4f} para TP de LONG")
                return
            siguiente = min(candidatos)
            tp_side = 'SELL'
        else:
            candidatos = [n for n in escalera if n < precio_entry * Decimal('0.9999')]
            if not candidatos:
                print(f"  ⚠️ [E.2] {symbol} No hay nivel inferior a {float(precio_entry):.4f} para TP de SHORT")
                return
            siguiente = max(candidatos)
            tp_side = 'BUY'

        ticks = int(siguiente / tick_size)
        tp_price = float(ticks * tick_size)
        # P1 FIX: El TP debe cubrir la posición ACUMULADA completa, no solo el
        # último fill parcial. Antes: qty = fill['qty'] → con 20 fills parciales
        # el TP quedaba de ~1/20 de la posición real (pos_004 ARB: LONG 1122.2
        # con TP de 39.8). Buscar la posición por pos_id y usar su qty total.
        qty = 0.0
        if pos_id and gs:
            pos_p1 = next((p for p in gs.posiciones if p.id == pos_id), None)
            if pos_p1 is not None and pos_p1.qty > 0:
                qty = float(pos_p1.qty)
        if qty <= 0:
            # Fallback: comportamiento original (qty del fill o de la orden)
            qty = float(fill.get('qty', orden_entry.get('quantity', 0)))
        if qty <= 0:
            print(f"  ⚠️ [E.2] {symbol} qty inválida ({qty}), abortando TP")
            return

        # E.2 FIX: Cancelar orden de entrada conflictiva en ese precio+side
        prefix = f"CM{state.grid_id}"
        try:
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders, symbol=symbol
            ))
            open_orders = open_orders or []
            conflicto = next((
                o for o in open_orders
                if o.get('side') == tp_side
                and abs(float(o.get('price', 0)) - tp_price) < float(tick_size) * 0.5
                and o.get('clientOrderId', '').startswith(prefix)
                and '_TP_' not in o.get('clientOrderId', '')
            ), None)
            if conflicto:
                print(f"  [E.2] {symbol} Cancelando orden de entrada {conflicto['side']} @ ${tp_price} "
                      f"(conflicto con TP) | ID: {conflicto['orderId']}")
                self._registrar_cancelacion_propia(conflicto['orderId'])  # P5
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_order,
                    symbol=symbol,
                    orderId=conflicto['orderId']
                ))
                for cid, meta in list(state.ordenes.items()):
                    if meta.get('binance_order_id') == str(conflicto['orderId']):
                        del state.ordenes[cid]
                        break
        except Exception as e:
            print(f"  ⚠️ [E.2] {symbol} Error buscando/cancelando orden conflictiva: {e}")

        client_id = f"CM{state.grid_id}_TP_{int(time.time()*1000)}"

        try:
            # FIX FASE 3: Verificar si hay posición abierta del lado correcto antes de reduceOnly
            # En modo ONE-WAY, reduceOnly falla si no hay posición abierta para reducir.
            puede_reduce = False
            if state.grid_state:
                pos_abierta = next((p for p in state.grid_state.posiciones
                                    if p.estado == 'ABIERTA' and p.tipo == ('LONG' if tp_side == 'SELL' else 'SHORT')), None)
                if pos_abierta:
                    puede_reduce = True
                else:
                    print(f"  [E.2] {symbol} No hay posición {'LONG' if tp_side == 'SELL' else 'SHORT'} abierta para TP {tp_side}. Skip.")

            if not puede_reduce:
                print(f"  ⚠️ [FASE 3] {symbol} TP {tp_side} @ ${tp_price} omitido: sin posición abierta")
                return

            orden_tp = {
                'symbol': symbol,
                'side': tp_side,
                'type': 'LIMIT',
                'quantity': qty,
                'price': tp_price,
                'timeInForce': 'GTC',
                'newClientOrderId': client_id
            }
            # FASE C.2: HEDGE MODE — el TP cierra la posición del lado opuesto
            # Si tp_side es SELL, cerramos LONG. Si BUY, cerramos SHORT.
            # P1-A FIX: en HEDGE MODE Binance rechaza reduceOnly (-1106).
            if self._hedge_mode:
                orden_tp['positionSide'] = 'LONG' if tp_side == 'SELL' else 'SHORT'
            else:
                orden_tp['reduceOnly'] = True
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                **orden_tp
            ))

            tp_order_id = str(res['orderId'])
            await guardar_orden_ejecucion(
                grid_ejecucion_id=state.grid_id,
                binance_order_id=tp_order_id,
                client_order_id=client_id,
                symbol=symbol, side=tp_side, tipo_orden='TAKE_PROFIT',
                price=tp_price, quantity=qty
            )

            gs.ordenes_tp_pendientes[pos_id] = tp_order_id

            for pos in gs.posiciones:
                if pos.id == pos_id:
                    pos.orden_cierre_id = tp_order_id
                    break

            print(f"  ✅ [E.2] {symbol} TP {tp_side} reduceOnly @ ${tp_price} | Qty:{qty} | Pos:{pos_id}")
        except Exception as e:
            print(f"  ⚠️ [E.2] {symbol} Error TP neutral {tp_side} reduceOnly @ ${tp_price}: {e}")

    async def _ejecutar_kill_switch_real(self, symbol: str, state: GridExecutionState,
                                          pos_id: str, pos_tipo: str, qty: float, 
                                          razon: str, binance_order_id: str = None,
                                          tipo: str = None):
        """
        CR3 FIX: Kill switch que garantiza cierre completo.
        P1-C FIX: acepta 'tipo' (la acción KILL_SWITCH lo incluye); sin él el
        llamador con **accion lanzaba TypeError y abortaba todo el monitoreo.
        
        Si la razón implica cierre total del grid, delega al proceso atómico.
        Si es cierre de posición individual, ejecuta MARKET con verificación.
        """
        # Si es cierre total del grid, usar el proceso atómico
        if razon in ('grid_completo', 'aborto_manual', 'limite_perdida_diaria', 
                     'grid_incompleto_batch_rechazado', 'recuperacion_post_crash_ya_terminado'):
            print(f"  🛑 [CIERRE] Kill Switch delega a cierre atómico | Razón: {razon}")
            await self._abortar_grid(symbol, razon)
            return
        
        print(f"  🛑 [CIERRE] Kill Switch {pos_tipo} {pos_id} | Razón: {razon}")

        # FASE 3: Cancelar take-profit de la posición ANTES de cerrar
        if state.grid_state:
            for pos in state.grid_state.posiciones:
                if pos.id == pos_id and pos.orden_cierre_id:
                    try:
                        self._registrar_cancelacion_propia(pos.orden_cierre_id)  # P5
                        await self._api_call(asyncio.to_thread(
                            self.client.futures_cancel_order,
                            symbol=symbol, orderId=pos.orden_cierre_id
                        ))
                        print(f"  [CIERRE] TP {pos.orden_cierre_id} cancelado antes de kill switch")
                    except Exception as e:
                        print(f"  ⚠️ [CIERRE] Error cancelando TP: {e}")
                    break

        # Cerrar con MARKET order (reduceOnly)
        side_cierre = 'SELL' if pos_tipo == 'LONG' else 'BUY'
        order_id = None

        try:
            orden_cierre = {
                'symbol': symbol,
                'side': side_cierre,
                'type': 'MARKET',
                'quantity': float(qty)
            }
            # FASE D.1: HEDGE MODE — cerrar posición del lado específico
            # P1-A FIX: en HEDGE MODE Binance rechaza reduceOnly (-1106).
            if self._hedge_mode:
                orden_cierre['positionSide'] = pos_tipo  # 'LONG' o 'SHORT'
            else:
                orden_cierre['reduceOnly'] = True
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                **orden_cierre
            ))
            order_id = str(res.get('orderId', ''))
            print(f"  ✅ [CIERRE] Kill Switch ejecutado: {side_cierre} {qty} | Order:{order_id}")

            # Esperar propagación del fill
            await asyncio.sleep(0.3)

            # Consultar trades para obtener precio real y commission
            if order_id:
                # P10 FIX: el endpoint de trades puede tardar unos segundos en
                # reflejar la ejecución MARKET. Antes se declaraba huérfana de
                # inmediato y se abortaba el GRID COMPLETO cuando la posición ya
                # estaba cerrada (2 grids muertos por lag de API en el log de 24h).
                # Reintentar antes de escalar.
                trades = []
                for intento_p10 in range(3):
                    trades = await self._api_call(asyncio.to_thread(
                        self.client.futures_account_trades,
                        symbol=symbol, orderId=order_id
                    ))
                    if trades:
                        break
                    print(f"  [P10] {symbol} Trades del kill switch aún no visibles "
                          f"(intento {intento_p10+1}/3). Reintentando...")
                    await asyncio.sleep(3)
                if trades:
                    # CR2 FIX: Procesar cada trade con PnL
                    for trade in trades:
                        await self._procesar_trade_con_pnl(state, trade, 'KILL_SWITCH')

                    total_qty = sum(float(t['qty']) for t in trades)
                    avg_price = sum(float(t['price']) * float(t['qty']) for t in trades) / total_qty if total_qty > 0 else 0
                    total_commission = sum(float(t['commission']) for t in trades)
                    total_realized_pnl = sum(float(t.get('realizedPnl', 0)) for t in trades)

                    # FASE 3: Cerrar posición en el estado propio del executor
                    self._cerrar_posicion_por_id(
                        state=state,
                        pos_id=pos_id,
                        precio_cierre=avg_price,
                        fee_cierre=total_commission,
                        binance_order_id=order_id
                    )

                    # P4 FIX: Registrar la orden de cierre en DB y marcarla FILLED.
                    # Antes no se guardaba → CR16 detectaba el fill como SIN_ORDEN
                    # y la auditoría/reconciliación de PnL quedaba coja. Al marcarla
                    # FILLED de inmediato, el pipeline la ignora (YA_PROCESADO) porque
                    # el kill switch ya procesó trades, PnL y cierre de posición aquí.
                    try:
                        await guardar_orden_ejecucion(
                            grid_ejecucion_id=state.grid_id,
                            binance_order_id=order_id,
                            client_order_id=f"CM{state.grid_id}_KS_{pos_id}_{int(time.time()*1000)}",
                            symbol=symbol,
                            side=side_cierre,
                            tipo_orden='CIERRE',
                            price=avg_price,
                            quantity=float(qty)
                        )
                        await _execute_with_retry(
                            "UPDATE grid_ejecucion_ordenes SET status = 'FILLED', quantity_filled = ? WHERE binance_order_id = ?",
                            (float(total_qty), order_id)
                        )
                    except Exception as e_p4:
                        print(f"  ⚠️ [P4] {symbol} No se pudo registrar orden de kill switch en DB: {e_p4}")

                    # CR2: PnL y fees ya actualizados por _procesar_trade_con_pnl

                    print(f"  [CIERRE] Pos {pos_id} cerrada @ ${avg_price:.4f} | "
                          f"PnL:{total_realized_pnl:+.4f} | Fee:{total_commission:.4f}")
                else:
                    print(f"  🚨 [CIERRE] Kill Switch sin trades confirmados — posible huérfana")
                    # CR3: Si no hay trades, verificar con el proceso atómico
                    await self._abortar_grid(symbol, f"kill_switch_sin_trades:{pos_id}")
                    return

        except Exception as e:
            print(f"  ❌ [CIERRE] Kill Switch falló: {e}")
            # CR3: Si falla el kill switch individual, forzar cierre completo del grid
            await self._abortar_grid(symbol, f"kill_switch_fallido:{e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # CR3 FIX: CIERRE ATÓMICO CON MÁQUINA DE ESTADOS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _abortar_grid(self, symbol: str, razon: str):
        """
        CR3 FIX: Cierre atómico con máquina de estados y verificación.
        
        Flujo garantizado:
        1. Cancelar TODAS las órdenes abiertas (con verificación loop)
        2. Cerrar posición con MARKET (con verificación de fill)
        3. Verificar que posición = 0 en Binance
        4. Verificar que no quedan órdenes abiertas
        5. Solo entonces: actualizar estado interno y DB
        """
        if symbol not in self._grids:
            return

        state = self._grids[symbol]
        if state.cerrando:
            print(f"  [CIERRE] {symbol} Ya está en proceso de cierre")
            return

        state.cerrando = True
        
        # CR3: Crear máquina de estados del cierre
        cierre = CierreState(
            grid_id=state.grid_id,
            symbol=symbol,
            razon=razon,
            timestamp_inicio=int(time.time())
        )
        
        print(f"  🛑 [CIERRE] {symbol} Iniciando cierre atómico... Razón: {razon}")

        try:
            # ═══════════════════════════════════════════════════════════════════
            # PASO 1: CANCELAR ÓRDENES ABIERTAS (con verificación loop)
            # ═══════════════════════════════════════════════════════════════════
            await self._paso_cancelar_ordenes(state, cierre)
            
            if cierre.estado == 'FALLIDO':
                await self._manejar_cierre_fallido(state, cierre)
                return
            
            # ═══════════════════════════════════════════════════════════════════
            # PASO 2: CERRAR POSICIÓN CON MARKET (con verificación de fill)
            # ═══════════════════════════════════════════════════════════════════
            await self._paso_cerrar_posicion(state, cierre)
            
            if cierre.estado == 'FALLIDO':
                await self._manejar_cierre_fallido(state, cierre)
                return
            
            # ═══════════════════════════════════════════════════════════════════
            # PASO 3: VERIFICAR POSICIÓN = 0 (loop con timeout)
            # ═══════════════════════════════════════════════════════════════════
            await self._paso_verificar_posicion(state, cierre)
            
            if cierre.estado == 'FALLIDO':
                await self._manejar_cierre_fallido(state, cierre)
                return
            
            # ═══════════════════════════════════════════════════════════════════
            # PASO 4: VERIFICAR SIN ÓRDENES ABIERTAS
            # ═══════════════════════════════════════════════════════════════════
            await self._paso_verificar_ordenes(state, cierre)
            
            if cierre.estado == 'FALLIDO':
                await self._manejar_cierre_fallido(state, cierre)
                return
            
            # ═══════════════════════════════════════════════════════════════════
            # PASO 5: COMPLETAR CIERRE (solo si todo verificado)
            # ═══════════════════════════════════════════════════════════════════
            await self._paso_completar_cierre(state, cierre)
            
        except Exception as e:
            print(f"  🚨 [CIERRE] {symbol} Error inesperado: {e}")
            cierre.fallar(f"Excepción: {e}")
            await self._manejar_cierre_fallido(state, cierre)

    # ═══════════════════════════════════════════════════════════════════════════════
    # CR3: PASOS DEL CIERRE ATÓMICO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _paso_cancelar_ordenes(self, state: GridExecutionState, cierre: CierreState):
        """Cancela todas las órdenes y verifica que no queden pendientes."""
        cierre.avanzar('CANCELANDO_ORDENES')
        
        while cierre.puede_reintentar():
            try:
                # Cancelar todas las órdenes abiertas
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_all_open_orders,
                    symbol=cierre.symbol
                ))
                
                await asyncio.sleep(0.5)  # Esperar propagación
                
                # Verificar que no quedan órdenes
                open_orders = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_open_orders,
                    symbol=cierre.symbol
                ))
                
                cierre.ordenes_restantes = len(open_orders)
                
                if not open_orders:
                    cierre.ordenes_canceladas = True
                    cierre.avanzar('ORDENES_CANCELADAS')
                    print(f"  ✅ [CIERRE] {cierre.symbol} Órdenes canceladas (0 pendientes)")
                    return
                
                # Si quedan órdenes, intentar cancelar una por una
                cierre.ordenes_fallidas = [o['orderId'] for o in open_orders]
                print(f"  ⚠️ [CIERRE] {cierre.symbol} Quedan {len(open_orders)} órdenes: {cierre.ordenes_fallidas}")
                
                for o in open_orders:
                    try:
                        await self._api_call(asyncio.to_thread(
                            self.client.futures_cancel_order,
                            symbol=cierre.symbol,
                            orderId=o['orderId']
                        ))
                        cierre.ordenes_canceladas_count += 1
                    except Exception as e_one:
                        print(f"  ❌ [CIERRE] No se pudo cancelar orden {o['orderId']}: {e_one}")
                
                cierre.fallar(f"{len(open_orders)} órdenes persisten")
                await asyncio.sleep(1)  # Esperar antes de reintentar
                
            except Exception as e:
                cierre.fallar(f"Error cancelando órdenes: {e}")
        
        # Si llegamos aquí, agotamos reintentos
        cierre.avanzar('FALLIDO')

    async def _paso_cerrar_posicion(self, state: GridExecutionState, cierre: CierreState):
        """Cierra la posición con MARKET y verifica el fill."""
        cierre.avanzar('CERRANDO_POSICION')
        
        while cierre.puede_reintentar():
            try:
                # Consultar posición actual
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information,
                    symbol=cierre.symbol
                ))
                
                if not position or len(position) == 0:
                    cierre.posicion_cerrada = True
                    print(f"  ✅ [CIERRE] {cierre.symbol} Sin posición que cerrar")
                    cierre.avanzar('POSICION_CERRADA')
                    return

                # FASE D.3 (corregida): leer posición del lado correcto según modo.
                # El plan original usaba `side` antes de definirla (NameError).
                # Se deriva el lado desde state.grid_mode; en NEUTRAL+HEDGE se toma
                # el lado con mayor posición (el otro se cierra en la siguiente iteración).
                if self._hedge_mode:
                    if state.grid_mode in ('LONG', 'SHORT'):
                        pos_amt = self._get_position_amt_hedge(position, state.grid_mode)
                    else:
                        amt_long = self._get_position_amt_hedge(position, 'LONG')
                        amt_short = self._get_position_amt_hedge(position, 'SHORT')
                        if abs(amt_long) >= abs(amt_short):
                            pos_amt = abs(amt_long)   # positivo → LONG
                        else:
                            pos_amt = -abs(amt_short)  # negativo → SHORT
                else:
                    pos_amt = Decimal(str(position[0].get('positionAmt', 0)))

                if abs(pos_amt) < Decimal('0.0001'):
                    cierre.posicion_cerrada = True
                    print(f"  ✅ [CIERRE] {cierre.symbol} Posición ya es 0")
                    cierre.avanzar('POSICION_CERRADA')
                    return

                # Calcular lado y cantidad
                side = 'SELL' if pos_amt > 0 else 'BUY'
                qty = float(abs(pos_amt))

                # Enviar orden MARKET de cierre
                orden_cierre = {
                    'symbol': cierre.symbol,
                    'side': side,
                    'type': 'MARKET',
                    'quantity': qty
                }
                # FASE D.2: HEDGE MODE — determinar lado de la posición a cerrar
                # P1-A FIX: en HEDGE MODE Binance rechaza reduceOnly (-1106).
                if self._hedge_mode:
                    # side=SELL cierra LONG, side=BUY cierra SHORT
                    pos_side = 'LONG' if side == 'SELL' else 'SHORT'
                    orden_cierre['positionSide'] = pos_side
                else:
                    orden_cierre['reduceOnly'] = True
                res_cierre = await self._api_call(asyncio.to_thread(
                    self.client.futures_create_order,
                    **orden_cierre
                ))
                
                cierre.posicion_cierre_order_id = str(res_cierre.get('orderId', ''))
                cierre.posicion_cierre_qty = qty
                
                print(f"  ✅ [CIERRE] {cierre.symbol} Orden MARKET enviada: "
                      f"{side} {qty} | Order: {cierre.posicion_cierre_order_id}")
                
                # Esperar y verificar fill
                await asyncio.sleep(0.5)
                
                # Consultar trades de esta orden
                if cierre.posicion_cierre_order_id:
                    trades = await self._api_call(asyncio.to_thread(
                        self.client.futures_account_trades,
                        symbol=cierre.symbol,
                        orderId=cierre.posicion_cierre_order_id
                    ))

                    if trades:
                        # CR2 FIX: Procesar cada trade con PnL
                        for trade in trades:
                            await self._procesar_trade_con_pnl(state, trade, 'ABORTO')

                        total_qty = sum(float(t['qty']) for t in trades)
                        avg_price = sum(float(t['price']) * float(t['qty']) for t in trades) / total_qty if total_qty > 0 else 0
                        total_commission = sum(float(t['commission']) for t in trades)
                        total_pnl = sum(float(t.get('realizedPnl', 0)) for t in trades)

                        cierre.posicion_cierre_precio = avg_price
                        # CR2: PnL y fees ya actualizados por _procesar_trade_con_pnl

                        print(f"  ✅ [CIERRE] {cierre.symbol} Posición cerrada @ ${avg_price:.4f} | "
                              f"PnL: {total_pnl:+.4f} | Fee: {total_commission:.4f}")

                        # FASE D.3 (complemento NEUTRAL+HEDGE): un grid neutral puede tener
                        # posición en AMBOS lados. Si el otro lado sigue abierto, continuar
                        # el bucle para cerrarlo en la siguiente iteración.
                        if self._hedge_mode and state.grid_mode not in ('LONG', 'SHORT'):
                            try:
                                position_recheck = await self._api_call(asyncio.to_thread(
                                    self.client.futures_position_information,
                                    symbol=cierre.symbol
                                ))
                                rest_long = abs(self._get_position_amt_hedge(position_recheck, 'LONG'))
                                rest_short = abs(self._get_position_amt_hedge(position_recheck, 'SHORT'))
                                if rest_long > Decimal('0.0001') or rest_short > Decimal('0.0001'):
                                    print(f"  [CIERRE] {cierre.symbol} HEDGE+NEUTRAL: queda otro lado abierto "
                                          f"(LONG:{float(rest_long):.4f} SHORT:{float(rest_short):.4f}). Cerrando...")
                                    continue
                            except Exception as e_re:
                                print(f"  ⚠️ [CIERRE] {cierre.symbol} Error re-chequeando lados HEDGE: {e_re}")

                        cierre.posicion_cerrada = True
                        cierre.avanzar('POSICION_CERRADA')
                        return
                    else:
                        cierre.fallar("Orden MARKET enviada pero sin trades confirmados")
                else:
                    cierre.fallar("No se obtuvo orderId del cierre")
                    
            except Exception as e:
                cierre.fallar(f"Error cerrando posición: {e}")
        
        cierre.avanzar('FALLIDO')

    async def _paso_verificar_posicion(self, state: GridExecutionState, cierre: CierreState):
        """Verifica que la posición real en Binance sea 0."""
        cierre.avanzar('VERIFICANDO')
        
        max_verificaciones = 25  # 5 segundos máximo (0.2s * 25)
        
        for i in range(max_verificaciones):
            await asyncio.sleep(0.2)
            
            try:
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information,
                    symbol=cierre.symbol
                ))
                
                if not position or len(position) == 0:
                    cierre.verificacion_ok = True
                    cierre.posicion_final = 0
                    print(f"  ✅ [CIERRE] {cierre.symbol} Posición verificada: 0")
                    return
                
                # P1-B FIX HEDGE: leer la pierna correcta según el modo del grid;
                # position[0] a ciegas puede ser la pierna equivocada en HEDGE MODE.
                if self._hedge_mode and state.grid_mode in ('LONG', 'SHORT'):
                    pos_amt_check = self._get_position_amt_hedge(position, state.grid_mode)
                elif self._hedge_mode:
                    pos_amt_check = self._get_position_neta_hedge(position)
                else:
                    pos_amt_check = Decimal(str(position[0].get('positionAmt', 0)))
                cierre.posicion_final = float(pos_amt_check)
                
                if abs(pos_amt_check) < Decimal('0.0001'):
                    cierre.verificacion_ok = True
                    print(f"  ✅ [CIERRE] {cierre.symbol} Posición confirmada en 0 "
                          f"({i+1}/{max_verificaciones} intentos)")
                    return
                
                # Si aún hay posición, reintentar cierre
                if i == max_verificaciones - 1:
                    cierre.fallar(f"Posición persistente: {float(pos_amt_check):.4f}")
                    cierre.avanzar('FALLIDO')
                    return
                
            except Exception as e:
                if i == max_verificaciones - 1:
                    cierre.fallar(f"Error verificando posición: {e}")
                    cierre.avanzar('FALLIDO')
                    return
        
        cierre.avanzar('FALLIDO')

    async def _paso_verificar_ordenes(self, state: GridExecutionState, cierre: CierreState):
        """Verifica que no queden órdenes abiertas."""
        try:
            open_orders = await self._api_call(asyncio.to_thread(
                self.client.futures_get_open_orders,
                symbol=cierre.symbol
            ))
            
            if open_orders:
                # Órdenes fantasmas detectadas
                ids_fantasmas = [o['orderId'] for o in open_orders]
                print(f"  🚨 [CIERRE] {cierre.symbol} ÓRDENES FANTASMAS detectadas: {ids_fantasmas}")
                
                # Intentar cancelar de nuevo
                for o in open_orders:
                    try:
                        await self._api_call(asyncio.to_thread(
                            self.client.futures_cancel_order,
                            symbol=cierre.symbol,
                            orderId=o['orderId']
                        ))
                    except Exception:
                        pass
                
                cierre.fallar(f"Órdenes fantasmas: {ids_fantasmas}")
                cierre.avanzar('FALLIDO')
                return
            
            print(f"  ✅ [CIERRE] {cierre.symbol} Sin órdenes abiertas confirmado")
            
        except Exception as e:
            cierre.fallar(f"Error verificando órdenes: {e}")
            cierre.avanzar('FALLIDO')
            return

    async def _paso_completar_cierre(self, state: GridExecutionState, cierre: CierreState):
        """Completa el cierre solo si todas las verificaciones pasaron."""
        
        # Cerrar GridState interno
        precio_actual = self.precios_vivo.get(cierre.symbol, 0)
        if state.grid_state and precio_actual > 0:
            resumen = await self._cerrar_grid_total(state, precio_actual)
            
            pnl_real = float(state.pnl_real)
            pnl_interno = resumen.get('pnl_neto', 0)
            
            print(f"  [CIERRE] {cierre.symbol} Resumen: PnL Real={pnl_real:+.4f} | "
                  f"Interno={pnl_interno:+.4f}")
            
            # Notificar
            if self.notifier:
                try:
                    await self.notifier.enviar_telegram(
                        f"🏁 <b>Grid Cerrado — {cierre.symbol}</b>\\n"
                        f"Razón: {cierre.razon}\\n"
                        f"📊 <b>Real (Binance):</b> {pnl_real:+.4f} USDT\\n"
                        f"🤖 <b>Interno:</b> {pnl_interno:+.4f} USDT\\n"
                        f"Trades: {resumen.get('trades_completados', 0)} | "
                        f"KS: {resumen.get('trades_kill_switch', 0)} | "
                        f"Duración: {cierre.duracion_total}s"
                    )
                except Exception as e:
                    print(f"  ⚠️ [CIERRE] Error notificando: {e}")
        
        # Guardar en DB
        pnl_final = float(state.pnl_real)
        fees_final = float(state.fees_real)
        
        await actualizar_grid_ejecucion_cierre(
            grid_id=state.grid_id,
            estado='CERRADO',
            pnl_real=pnl_final,
            fees_real=fees_final,
            razon_cierre=cierre.razon
        )
        
        # Guardar métricas del cierre
        await self._guardar_metricas_cierre(cierre)
        
        # Limpiar estado
        state.activa = False
        if cierre.symbol in self._grids:
            del self._grids[cierre.symbol]
        
        # SYNC FIX: liberar máquina de señales si el grid cerrado era neutral
        if state.grid_mode == 'NEUTRAL':
            self._sincronizar_signal_grid_neutral_cerrado(cierre.symbol)
        
        cierre.completar()

    async def _manejar_cierre_fallido(self, state: GridExecutionState, cierre: CierreState):
        """Maneja un cierre que no pudo completarse."""
        
        print(f"  🚨 [CIERRE] {cierre.symbol} CIERRE FALLIDO — Estado: {cierre.estado}")
        
        # Alerta crítica
        if self.notifier:
            try:
                await self.notifier.enviar_telegram(
                    f"🚨 <b>CIERRE FALLIDO — {cierre.symbol}</b>\\n"
                    f"Razón original: {cierre.razon}\\n"
                    f"Estado final: {cierre.estado}\\n"
                    f"Intentos: {cierre.intentos}/{cierre.MAX_INTENTOS}\\n"
                    f"Órdenes restantes: {cierre.ordenes_restantes or 'N/A'}\\n"
                    f"Posición final: {cierre.posicion_final or 'N/A'}\\n\\n"
                    f"<b>⚠️ INTERVENCIÓN MANUAL REQUERIDA</b>\\n"
                    f"Verificar en Binance: posición y órdenes abiertas."
                )
            except Exception as e:
                print(f"  ⚠️ Error enviando alerta: {e}")
        
        # Marcar en DB como FALLIDO para seguimiento
        await actualizar_grid_ejecucion_cierre(
            grid_id=state.grid_id,
            estado='CIERRE_FALLIDO',
            pnl_real=float(state.pnl_real),
            fees_real=float(state.fees_real),
            razon_cierre=f"{cierre.razon} | FALLIDO: {cierre.estado}"
        )
        
        # NO eliminar del diccionario para mantener referencia
        # state.activa = False  # ← NO, mantener para investigación
        state.cerrando = False  # Permitir reintentar manualmente
        
        print(f"  [CIERRE] {cierre.symbol} Grid marcado como CIERRE_FALLIDO. "
              f"NO se eliminó de memoria para investigación.")

    async def _guardar_metricas_cierre(self, cierre: CierreState):
        """Guarda métricas del proceso de cierre para análisis posterior."""
        print(f"  [CIERRE METRICS] {cierre.symbol}: "
              f"Duración={cierre.duracion_total}s | "
              f"Intentos={cierre.intentos} | "
              f"Órdenes canceladas={cierre.ordenes_canceladas_count} | "
              f"Posición cerrada={cierre.posicion_cerrada}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3 PLAN 6.3: LÍMITE DE PÉRDIDA DIARIA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _verificar_limite_perdida(self):
        """FASE 3: Aborta todos los grids si el PnL diario (cerrado + activo) supera el límite."""
        LIMITE_PERDIDA = -100
        try:
            from database_v5 import _get_db
            db = await _get_db()

            # PnL de grids CERRADOS hoy (FIX: usar date() de SQLite, no string ISO)
            cursor = await db.execute(
                "SELECT COALESCE(SUM(pnl_real), 0) FROM grid_ejecuciones "
                "WHERE estado = 'CERRADO' AND date(closed_at) = date('now')"
            )
            row = await cursor.fetchone()
            pnl_cerrado = float(row[0]) if row and row[0] is not None else 0.0

            # PnL de grids ACTIVOS en RAM (suma posiciones abiertas actuales)
            pnl_activo = 0.0
            for sym, st in self._grids.items():
                if st.activa:
                    pnl_activo += float(st.pnl_real)

            pnl_total = pnl_cerrado + pnl_activo

            # Log silencioso cada ciclo para debug (solo si hay actividad)
            if pnl_cerrado != 0 or pnl_activo != 0:
                print(f"  [EXECUTOR] PnL diario check | Cerrado: {pnl_cerrado:+.4f} | Activo: {pnl_activo:+.4f} | Total: {pnl_total:+.4f}")

            if pnl_total < LIMITE_PERDIDA:
                print(f"  🛑 [EXECUTOR] LÍMITE DE PÉRDIDA ALCANZADO: {pnl_total:.4f} USDT. Cerrando TODO.")
                if self.notifier:
                    try:
                        await self.notifier.enviar_telegram(
                            f"🛑 <b>LÍMITE DE PÉRDIDA DIARIO ALCANZADO</b>\\n"
                            f"PnL total: <code>{pnl_total:.4f} USDT</code>\\n"
                            f"(Cerrado: {pnl_cerrado:.4f} | Activo: {pnl_activo:.4f})\\n"
                            f"Abortando todos los grids activos..."
                        )
                    except Exception as e:
                        print(f"  ⚠️ [EXECUTOR] Error notificando límite: {e}")

                for sym in list(self._grids.keys()):
                    await self._abortar_grid(sym, 'limite_perdida_diaria')

        except Exception as e:
            print(f"  ⚠️ [EXECUTOR] Error verificando límite de pérdida: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # CR3 FIX: RECUPERACIÓN POST-CRASH CON VERIFICACIÓN
    # FASE 6: Adaptativa para direccionales (sin grid_state)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _recuperar_grids_activos(self):
        """CR3 FIX: Recuperar con verificación de que el grid sigue coherente.
        FASE 6: Direccionales no reconstruyen grid_state."""
        grids_db = await cargar_grid_ejecuciones_activos()
        if not grids_db:
            print("  [EXECUTOR] No hay grids activos para recuperar")
            return

        for grid in grids_db:
            symbol = grid['symbol']
            print(f"  [EXECUTOR] Recuperando grid {symbol} (ID:{grid['id']})...")

            try:
                # CR3: Verificar si el grid sigue activo en Binance
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information,
                    symbol=symbol
                ))
                
                open_orders = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_open_orders,
                    symbol=symbol
                ))
                
                # FASE F.1: Usar helper HEDGE para leer posición del lado correcto
                # FIX HEDGE-NET: NEUTRAL en hedge recupera neta de AMBAS piernas
                if self._hedge_mode and grid['direction'] in ('LONG', 'SHORT'):
                    pos_amt = self._get_position_amt_hedge(position, grid['direction'])
                else:
                    pos_amt = self._get_position_neta_hedge(position)
                tiene_posicion = abs(pos_amt) > Decimal('0.0001')
                tiene_ordenes = len(open_orders) > 0
                
                if not tiene_posicion and not tiene_ordenes:
                    # Grid ya terminó naturalmente, marcar como cerrado
                    print(f"  [EXECUTOR] {symbol} Grid ya terminado (pos 0, sin órdenes). Marcando CERRADO.")
                    await actualizar_grid_ejecucion_cierre(
                        grid_id=grid['id'],
                        estado='CERRADO',
                        pnl_real=0,
                        fees_real=0,
                        razon_cierre='recuperacion_post_crash_ya_terminado'
                    )
                    continue
                
                # Reconstruir parámetros desde JSON
                params = json.loads(grid['grid_params_json']) if grid.get('grid_params_json') else {}
                niveles = params.get('niveles', [])
                qty = params.get('qty_por_orden', 0)

                # FASE 2: Fallback — si niveles no persistieron (grids antiguos), regenerar desde límites
                if not niveles and params.get('lower_limit') is not None and params.get('upper_limit') is not None:
                    try:
                        lower = float(params['lower_limit'])
                        upper = float(params['upper_limit'])
                        grid_count = int(params.get('grid_count', 7))
                        info = self._get_symbol_info(symbol)
                        tick_size = Decimal(str(info['tickSize']))
                        rango = Decimal(str(upper)) - Decimal(str(lower))
                        step = rango / (grid_count - 1) if grid_count > 1 else rango
                        niveles_regen = []
                        vistos = set()
                        for i in range(grid_count):
                            nivel = lower + float(step) * i
                            ticks = int(Decimal(str(nivel)) / tick_size)
                            nivel = float(ticks * tick_size)
                            if nivel not in vistos:
                                vistos.add(nivel)
                                niveles_regen.append(nivel)
                        niveles = niveles_regen
                        print(f"  [FASE 2] {symbol} Niveles regenerados desde límites post-crash: {len(niveles)} niveles")
                    except Exception as e_regen:
                        print(f"  ⚠️ [FASE 2] {symbol} No se pudieron regenerar niveles: {e_regen}")

                # Reconstruir estado
                state = GridExecutionState(
                    grid_id=grid['id'], symbol=symbol, direction=grid['direction'],
                    capital=grid['capital_asignado'], leverage=grid['apalancamiento_usado'],
                    precio_entrada=grid['precio_entrada'],
                    niveles=niveles, qty_por_orden=qty
                )
                state.pares_abiertos = []  # FIX: Inicializar atributo faltante en recuperación
                
                # FASE 2 / FASE 6: Reconstruir grid_state SOLO para grids neutral
                if grid['direction'] == 'NEUTRAL':
                    state.grid_mode = 'NEUTRAL'
                    try:
                        params = json.loads(grid['grid_params_json']) if grid.get('grid_params_json') else {}
                        niveles_rec = params.get('niveles', niveles)
                        price_rec = grid['precio_entrada']
                        niveles_buy_rec = [n for n in niveles_rec if n < price_rec * 0.9995]
                        niveles_sell_rec = [n for n in niveles_rec if n > price_rec * 1.0005]
                        state.init_grid_state(niveles_buy=niveles_buy_rec, niveles_sell=niveles_sell_rec)
                        print(f"  [EXECUTOR] {symbol} GridState neutral reconstruido post-crash")
                    except Exception as e:
                        print(f"  ⚠️ [EXECUTOR] {symbol} Error reconstruyendo grid_state: {e}")
                else:
                    # FASE 6: Direccionales — NO reconstruir grid_state
                    state.grid_mode = grid['direction']
                    print(f"  [EXECUTOR] {symbol} Grid direccional recuperado sin grid_state")
                
                # Sincronizar posición real
                state.posicion_neta = pos_amt
                # FASE F.2: En HEDGE, la posición neta es la del lado específico
                print(f"  [EXECUTOR] {symbol} Posición recuperada: {float(pos_amt):.4f} (lado: {grid['direction']})")

                db_ordenes = await cargar_ordenes_por_grid(grid['id'])
                for orden in db_ordenes:
                    if orden['status'] == 'NEW':
                        vivo = any(str(o['orderId']) == orden['binance_order_id']
                                  for o in open_orders)
                        if vivo:
                            state.ordenes[orden['client_order_id']] = orden

                self._grids[symbol] = state
                self._symbol_leverage[symbol] = grid['apalancamiento_usado']
                print(f"  ✅ [EXECUTOR] {symbol} Recuperado | {len(state.ordenes)} órdenes activas | "
                      f"Posición: {float(state.posicion_neta):.4f}")
                # SYNC FIX: post-reinicio la máquina vuelve a MONITOREO; realinear si es neutral
                if grid['direction'] == 'NEUTRAL':
                    self._sincronizar_signal_grid_neutral_activo(symbol)

            except Exception as e:
                print(f"  ❌ [EXECUTOR] Error recuperando {symbol}: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # PROCESAR MENSAJES DE LA COLA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _procesar_mensaje(self, msg: dict):
        tipo = msg.get('tipo')

        if tipo == 'CREAR_GRID':
            direction = msg.get('direction')
            symbol = msg.get('symbol')

            # V7.1 FASE 4: Debounce SIMÉTRICO — registrar ANTES de cualquier operación
            if symbol:
                ahora = time.time()
                if symbol in self._grid_pending_creation:
                    tiempo_desde_ultimo = ahora - self._grid_pending_creation[symbol]
                    if tiempo_desde_ultimo < 60:
                        print(f"  ⚠️ [V7.1] {symbol} Debounce activo: {tiempo_desde_ultimo:.0f}s < 60s. Ignorando CREAR_GRID.")
                        return
                self._grid_pending_creation[symbol] = ahora   # ← REGISTRADO AQUÍ

            if direction == 'LONG':
                await self._crear_grid_long(
                    msg['symbol'], msg['params'], msg['price']
                )
            elif direction == 'SHORT':
                await self._crear_grid_short(
                    msg['symbol'], msg['params'], msg['price']
                )
            elif direction == 'NEUTRAL':
                await self._crear_grid_neutral(
                    msg['symbol'], msg['params'], msg['price']
                )
        elif tipo == 'ABORTAR_GRID':
            await self._abortar_grid(msg['symbol'], msg.get('razon', 'aborto_manual'))

    async def _notificar_rechazo(self, symbol: str, razon: str):
        print(f"  ❌ [EXECUTOR] {symbol} Grid rechazado: {razon}")
