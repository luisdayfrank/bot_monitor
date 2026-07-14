"""
executor.py — PLAN 6.3 FASES 1-4 + FIXES V7.1 + CR3 CIERRE ATÓMICO + CR2 PnL COMPLETO
Motor de ejecución real de grids en Binance Futures (Testnet / Real).
Flujo: recibe mensajes por cola, coloca órdenes LIMIT, trackea fills, maneja aborto.
Arquitectura: 100% autónomo — lógica operativa interna (PosicionReal + GridState).
GridSimulator eliminado completamente. No quedan dependencias externas.
CR3 FIX: Cierre atómico con máquina de estados y verificación de estado real en Binance.
CR2 FIX: Arquitectura de PnL completa — cada trade persiste realizedPnl en DB.
"""

import asyncio
import json
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional
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

    # ═══════════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ═══════════════════════════════════════════════════════════════════════════════

    async def run(self):
        """Loop principal del executor."""
        print("  [EXECUTOR] Iniciando GridExecutor...")
        print(f"  [EXECUTOR] Modo: {CONFIG.trading_mode} | Capital: ${CONFIG.trading_capital_max_usdt}")

        await self._cargar_exchange_info()
        await self._recuperar_grids_activos()

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

    async def _enviar_ordenes_batch(self, symbol: str, ordenes: List[dict], grid_id: int):
        """Envía órdenes en batches, con fallback individual."""
        batch_size = CONFIG.trading_batch_max_ordenes
        order_ids_guardados = []

        for i in range(0, len(ordenes), batch_size):
            batch = ordenes[i:i+batch_size]
            try:
                if hasattr(self.client, 'futures_place_batch_order'):
                    resultados = await self._api_call(asyncio.to_thread(
                        self.client.futures_place_batch_order,
                        batchOrders=batch
                    ))
                else:
                    resultados = []
                    for ord in batch:
                        try:
                            res = await self._api_call(asyncio.to_thread(
                                self.client.futures_create_order,
                                symbol=ord['symbol'],
                                side=ord['side'],
                                type=ord['type'],
                                quantity=ord['quantity'],
                                price=ord['price'],
                                timeInForce=ord['timeInForce'],
                                newClientOrderId=ord['newClientOrderId']
                            ))
                            resultados.append(res)
                        except Exception as e2:
                            print(f"  ❌ [EXECUTOR] Fallback individual falló: {e2}")

                for j, res in enumerate(resultados):
                    if 'orderId' in res:
                        await guardar_orden_ejecucion(
                            grid_ejecucion_id=grid_id,
                            binance_order_id=str(res['orderId']),
                            client_order_id=batch[j]['newClientOrderId'],
                            symbol=symbol,
                            side=batch[j]['side'],
                            tipo_orden='ENTRY',
                            price=batch[j]['price'],
                            quantity=batch[j]['quantity']
                        )
                        order_ids_guardados.append(res['orderId'])
                    else:
                        print(f"  ⚠️ [EXECUTOR] Orden rechazada en batch: {res}")
            except Exception as e:
                print(f"  ❌ [EXECUTOR] Error en batch completo: {e}")
                for ord in batch:
                    try:
                        res = await self._api_call(asyncio.to_thread(
                            self.client.futures_create_order,
                            symbol=ord['symbol'],
                            side=ord['side'],
                            type=ord['type'],
                            quantity=ord['quantity'],
                            price=ord['price'],
                            timeInForce=ord['timeInForce'],
                            newClientOrderId=ord['newClientOrderId']
                        ))
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
                        order_ids_guardados.append(res['orderId'])
                    except Exception as e2:
                        print(f"  ❌ [EXECUTOR] Fallback final falló: {e2}")

            if i + batch_size < len(ordenes):
                await asyncio.sleep(0.1)

        # Verificar si hubo rechazos en el batch
        rechazos_batch = [r for r in resultados if r.get('code')]
        if rechazos_batch:
            print(f"  ❌ [EXECUTOR] {symbol} Batch con rechazos: {len(rechazos_batch)} órdenes. ABORTANDO grid.")
            # Cancelar órdenes que sí pasaron para no dejar grid roto
            for oid in order_ids_guardados:
                try:
                    await self._api_call(asyncio.to_thread(
                        self.client.futures_cancel_order, symbol=symbol, orderId=oid
                    ))
                except Exception:
                    pass
            return []  # Lista vacía = fallo total

        return order_ids_guardados

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 2 PLAN 6.3: LÓGICA OPERATIVA INTERNA (Emparejamiento FIFO autónomo)
    # ═══════════════════════════════════════════════════════════════════════════════

    def _emparejar_posiciones(self, state: GridExecutionState, timestamp: int):
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
                    self._cancelar_tp_si_existe(state, long_pos)
                    self._cancelar_tp_si_existe(state, short_pos)
                    
                    print(f"  [EXECUTOR] {state.symbol} PAR CERRADO FIFO | "
                          f"LONG ${float(long_pos.nivel_precio):.4f} → SELL ${float(short_pos.nivel_precio):.4f} | "
                          f"Diff: {float(diferencia_niveles):.4f} | PnL: {float(pnl_neto):+.4f}")
                    break  # Solo emparejar una vez por LONG

    def _cancelar_tp_si_existe(self, state: GridExecutionState, pos: PosicionReal):
        """Cancela la orden de take-profit de una posición si aún está pendiente."""
        if pos.orden_cierre_id and state.grid_state:
            try:
                self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_order,
                    symbol=state.symbol,
                    orderId=pos.orden_cierre_id
                ))
                print(f"  [EXECUTOR] {state.symbol} TP cancelado para pos {pos.id}")
            except Exception as e:
                print(f"  ⚠️ [EXECUTOR] Error cancelando TP {pos.orden_cierre_id}: {e}")
            # Limpiar del tracking
            if pos.id in state.grid_state.ordenes_tp_pendientes:
                del state.grid_state.ordenes_tp_pendientes[pos.id]

    def _on_fill_real(self, state: GridExecutionState, side: str, price: float,
                      qty: float, fee: float, timestamp: int,
                      binance_order_id: str, binance_trade_id: str = None) -> str:
        """
        FASE 2.2: El executor registra el fill directamente en su estado.
        Reemplaza a GridSimulator.on_fill().
        """
        if not state.grid_state or not state.grid_state.activa:
            print(f"  ⚠️ [EXECUTOR] {state.symbol} Fill ignorado: grid no activo")
            return None
        
        gs = state.grid_state
        
        precio_d = Decimal(str(price))
        qty_d = Decimal(str(qty))
        fee_d = Decimal(str(fee))
        notional = qty_d * precio_d
        
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
        gs.ordenes_reales[binance_order_id] = pos.id
        
        # Auto-emparejar inmediatamente si hay par posible
        self._emparejar_posiciones(state, timestamp)
        
        print(f"  [EXECUTOR] {state.symbol} {side} real registrado @ ${price:.4f} | "
              f"Pos: {pos.id} | Qty: {qty} | Fee: ${fee:.4f} | "
              f"Posiciones abiertas: {n_abiertas}")
        
        return pos.id

    def _evaluar_kill_switch(self, state: GridExecutionState,
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
        self._emparejar_posiciones(state, timestamp)
        
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

    def _cerrar_grid_total(self, state: GridExecutionState, precio_final: float) -> dict:
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
                self._api_call(asyncio.to_thread(
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

    def get_estado_grid(self, state: GridExecutionState) -> Optional[dict]:
        """
        FASE 2.7: Retorna el estado actual del grid para el dashboard.
        Reemplaza a GridSimulator.get_estado_simulacion().
        """
        if not state or not state.activa or not state.grid_state:
            return None
        
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
        }

    # ═══════════════════════════════════════════════════════════════════════════════
    # CREAR GRID LONG (FASE 1)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_direccional(self, symbol: str, direction: str, params: dict, price: float):
        """
        Crea un grid LONG o SHORT real en Binance Futures.
        direction: 'LONG' o 'SHORT'
        """
        print(f"  [EXECUTOR] >>> SOLICITUD RECIBIDA: {symbol} {direction} @ ${price:.4f}")
        print(f"  [EXECUTOR] Params: grids={params.get('grid_count')}, range=[{params.get('lower_limit')}, {params.get('upper_limit')}], step_pct={params.get('step_pct')}%")
        
        if symbol in self._grids and self._grids[symbol].activa:
            print(f"  ⚠️ [EXECUTOR] {symbol} Ya hay grid activo, rechazando nuevo")
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

        info = self._get_symbol_info(symbol)
        step_size = Decimal(str(info['stepSize']))
        notional_total = Decimal(str(CONFIG.trading_capital_max_usdt)) * leverage
        notional_orden = notional_total / int(params['grid_count'])
        qty_raw = notional_orden / Decimal(str(price))
        steps = int(qty_raw / step_size)
        qty = float(steps * step_size)

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
            ordenes.append({
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'quantity': str(qty),
                'price': str(precio_validado),
                'timeInForce': 'GTC',
                'newClientOrderId': client_order_id
            })

        # 7. Enviar en batches
        order_ids_guardados = await self._enviar_ordenes_batch(symbol, ordenes, grid_id)

        # 8. Verificar que se guardaron órdenes
        if not order_ids_guardados:
            print(f"  ❌ [EXECUTOR] {symbol} Grid {direction} FALLÓ: 0 órdenes guardadas")
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
    # CREAR GRID NEUTRAL (FASE 2 — Autónomo, sin helper)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_neutral(self, symbol: str, params: dict, price: float):
        """Crea un grid neutral usando lógica interna del executor (FASE 2)."""
        if symbol in self._grids and self._grids[symbol].activa:
            print(f"  ⚠️ [EXECUTOR] {symbol} Ya hay grid activo")
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
            
            ordenes.append({
                'symbol': symbol, 'side': 'BUY', 'type': 'LIMIT',
                'quantity': str(qty), 'price': str(precio_validado),
                'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_BUY_{idx}_{timestamp}"
            })
        for idx, nivel in enumerate(niveles_sell):
            precio_validado = self._validar_y_redondear_precio(nivel, symbol)
            if precio_validado is None:
                continue  # Saltar este nivel, no abortar todo el grid
            
            ordenes.append({
                'symbol': symbol, 'side': 'SELL', 'type': 'LIMIT',
                'quantity': str(qty), 'price': str(precio_validado),
                'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_SELL_{idx}_{timestamp}"
            })

        # 11. Enviar batches (igual que directional)
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
        print(f"  ✅ [EXECUTOR] {symbol} Grid NEUTRAL creado | BUY:{len(niveles_buy)} SELL:{len(niveles_sell)} | Órdenes:{len(order_ids_guardados)}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # MONITOREO PERIÓDICO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _monitoring_loop(self):
        """Monitorea grids activos cada N segundos."""
        while not self._shutdown.is_set():
            await asyncio.sleep(CONFIG.trading_polling_interval_seg)

            # FASE 3: Verificar límite de pérdida diaria
            await self._verificar_limite_perdida()

            for symbol in list(self._grids.keys()):
                if not self._grids[symbol].activa or self._grids[symbol].cerrando:
                    continue
                try:
                    st = self._grids[symbol]
                    if st.activa and not st.cerrando:
                        # FASE 3 FIX: Auto-cleanup si grid terminó naturalmente
                        if float(st.posicion_neta) == 0 and len(st.pares_abiertos) == 0:
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
                                continue

                        print(f"  [EXECUTOR] {symbol} Monitoreo | Pos: {float(st.posicion_neta):.4f} | PnL: {float(st.pnl_real):+.4f} | Fees: {float(st.fees_real):.4f}")
                    await self._monitorear_grid(symbol)
                except Exception as e:
                    print(f"  ❌ [EXECUTOR] Error monitoreando {symbol}: {e}")

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
        if not state.grid_state or not state.grid_state.activa:
            return []

        # 1. Obtener timestamp del último trade conocido
        ultimo_ts = await obtener_ultimo_trade_timestamp(symbol)

        # 2. Consultar trades recientes en Binance
        try:
            # Si hay último timestamp, pedir desde ahí. Si no, últimos 100 trades.
            if ultimo_ts > 0:
                # Convertir ms a hora aproximada para startTime
                start_time = ultimo_ts + 1  # +1ms para no repetir
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol,
                    startTime=start_time,
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
                # Fill de una orden que NO tenemos trackeada → anomalía
                print(f"  🚨 [CR16] {symbol} Fill de orden desconocida: {order_id}")
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
        """
        if not state.grid_state:
            return

        fills = await cargar_fills_sin_procesar(state.grid_id)

        # Cargar órdenes de DB una sola vez
        db_ordenes = await cargar_ordenes_por_grid(state.grid_id)

        for fill in fills:
            # Buscar la orden correspondiente en nuestra DB
            orden = next((o for o in db_ordenes if str(o['binance_order_id']) == fill['binance_order_id']), None)

            if not orden:
                print(f"  ⚠️ [CR16] Fill {fill['id']} sin orden en DB, saltando")
                await marcar_fill_procesado(fill['id'], 'SIN_ORDEN')
                continue

            # Verificar si ya fue procesado en el estado
            if orden['status'] == 'FILLED':
                await marcar_fill_procesado(fill['id'], 'YA_PROCESADO')
                continue

            # Procesar el fill
            side = fill['side']
            price = fill['price']
            qty = fill['qty']
            commission = fill['commission']
            ts = fill['timestamp_ms'] // 1000  # Convertir a segundos

            # Registrar en estado interno
            pos_id = self._on_fill_real(
                state=state,
                side=side,
                price=price,
                qty=qty,
                fee=commission,
                timestamp=ts,
                binance_order_id=fill['binance_order_id'],
                binance_trade_id=fill['binance_trade_id']
            )

            # CR2 FIX: Procesar PnL del trade
            tipo_evento = 'FILL_ENTRADA' if orden['tipo_orden'] == 'ENTRY' else 'FILL_SALIDA' if orden['tipo_orden'] == 'TAKE_PROFIT' else 'FILL_DESCONOCIDO'
            await self._procesar_trade_con_pnl(state, fill, tipo_evento)

            # Actualizar orden en DB
            await actualizar_orden_fill(
                orden_id=orden['id'],
                binance_trade_id=fill['binance_trade_id'],
                price=price,
                qty=qty,
                commission=commission,
                commission_asset=fill['commission_asset'],
                realized_pnl=fill['realized_pnl'],
                timestamp=datetime.fromtimestamp(ts).isoformat()
            )

            # Marcar fill como procesado
            await marcar_fill_procesado(fill['id'], pos_id or 'PROCESADO')

            # Colocar take-profit
            if state.grid_mode == 'NEUTRAL' and pos_id:
                await self._colocar_take_profit_neutral(
                    symbol, state, orden, side, pos_id
                )
            elif orden['tipo_orden'] == 'ENTRY':
                await self._colocar_take_profit(symbol, state, orden)

            print(f"  [CR16] {symbol} Fill procesado: {side} @ ${price:.4f} | "
                  f"Pos: {pos_id} | Qty: {qty}")


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

            orden_id = str(orden['binance_order_id'])

            if orden_id not in open_ids:
                # La orden desapareció de abiertas → puede estar FILLED o CANCELED
                # Consultar estado exacto
                try:
                    order_info = await self._api_call(asyncio.to_thread(
                        self.client.futures_get_order,
                        symbol=symbol,
                        orderId=orden_id
                    ))

                    if order_info['status'] == 'FILLED':
                        # Este fill debería haber sido capturado por el poll proactivo
                        # Si llegamos aquí, el poll proactivo falló → anomalía
                        print(f"  🚨 [CR16] {symbol} Orden {orden_id} FILLED no detectada "
                              f"por poll proactivo")

                        # Procesar de todas formas (doble seguridad)
                        await self._procesar_fill_desde_orden_info(state, order_info)

                    elif order_info['status'] == 'CANCELED':
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

        # ═══════════════════════════════════════════════════════════════════
        # CR16 PASO 1: POLL PROACTIVO DE TRADES
        # ═══════════════════════════════════════════════════════════════════
        nuevos_fills = await self._poll_fills_proactivo(symbol, state)

        # ═══════════════════════════════════════════════════════════════════
        # CR16 PASO 2: PROCESAR FILLS PENDIENTES
        # ═══════════════════════════════════════════════════════════════════
        await self._procesar_fills_pendientes(symbol, state)

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
                acciones = self._evaluar_kill_switch(
                    state=state,
                    precio_actual=precio_actual,
                    timestamp=int(time.time())
                )
                for accion in acciones:
                    if accion['tipo'] == 'KILL_SWITCH':
                        await self._ejecutar_kill_switch_real(
                            symbol=symbol, state=state, **accion
                        )

        # ═══════════════════════════════════════════════════════════════════
        # PASO 5: RECONCILIACIÓN
        # ═══════════════════════════════════════════════════════════════════
        await self._reconciliar_con_binance(state)

        # ═══════════════════════════════════════════════════════════════════
        # CR2 PASO 6: RECONCILIAR PnL CON DB
        # ═══════════════════════════════════════════════════════════════════
        await self._reconciliar_pnl(state)


    async def _reconciliar_con_binance(self, state: GridExecutionState):
        """
        CR16: Reconciliación completa del estado interno vs Binance.
        Detecta fills perdidos, posiciones huérfanas, órdenes fantasmas.
        """
        if not state.grid_state or not state.grid_state.activa:
            return

        try:
            # 1. Posición real en Binance
            position = await self._api_call(asyncio.to_thread(
                self.client.futures_position_information,
                symbol=state.symbol
            ))

            pos_amt_real = Decimal(str(position[0].get('positionAmt', 0))) if position else Decimal('0')
            pos_amt_interno = state.posicion_neta

            # 2. Trades recientes (últimos 5 minutos) para detectar fills perdidos
            cinco_min_atras = int((time.time() - 300) * 1000)
            trades_recientes = await self._api_call(asyncio.to_thread(
                self.client.futures_account_trades,
                symbol=state.symbol,
                startTime=cinco_min_atras
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

            open_ids_binance = {str(o['orderId']) for o in open_orders}
            open_ids_interno = set(state.grid_state.ordenes_tp_pendientes.values())

            # Órdenes en Binance que no tenemos trackeadas
            for oid in open_ids_binance - open_ids_interno:
                print(f"  🚨 [CR16] {state.symbol} Orden huérfana en Binance: {oid}")

            # Órdenes que creímos abiertas pero Binance no las tiene
            for pos_id, oid in list(state.grid_state.ordenes_tp_pendientes.items()):
                if oid not in open_ids_binance:
                    print(f"  ⚠️ [CR16] {state.symbol} TP {oid} desaparecido, limpiando tracking")
                    del state.grid_state.ordenes_tp_pendientes[pos_id]

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
        """
        price = float(trade['price'])
        qty = float(trade['qty'])
        commission = float(trade['commission'])
        realized_pnl = float(trade.get('realizedPnl', 0))
        notional = price * qty

        # Persistir inmediatamente en DB (fuente de verdad)
        await guardar_pnl_evento(
            grid_ejecucion_id=state.grid_id,
            symbol=state.symbol,
            tipo_evento=tipo_evento,
            side=trade['side'],
            binance_trade_id=trade['id'],
            binance_order_id=str(trade['orderId']),
            price=price,
            qty=qty,
            commission=commission,
            commission_asset=trade.get('commissionAsset', ''),
            realized_pnl=realized_pnl,
            notional=notional,
            timestamp_ms=trade['time']
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
                  f"Trade: {trade['id']}")

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

    async def _colocar_take_profit(self, symbol: str, state: GridExecutionState,
                                    orden_entry: dict):
        """Coloca take-profit para un fill de entrada (LONG o SHORT)."""
        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        entry_price = Decimal(str(orden_entry['price']))

        if state.direction == 'LONG':
            # Encontrar nivel superior para LONG
            siguiente_nivel = None
            for nivel in state.niveles:
                if Decimal(str(nivel)) > entry_price * Decimal('1.0001'):
                    siguiente_nivel = nivel
                    break
            if not siguiente_nivel:
                return
            side = 'SELL'
        else:  # SHORT
            # Encontrar nivel inferior para SHORT
            siguiente_nivel = None
            for nivel in reversed(state.niveles):
                if Decimal(str(nivel)) < entry_price * Decimal('0.9999'):
                    siguiente_nivel = nivel
                    break
            if not siguiente_nivel:
                return
            side = 'BUY'

        ticks = int(Decimal(str(siguiente_nivel)) / tick_size)
        tp_price = float(ticks * tick_size)
        qty = orden_entry['quantity']

        client_order_id = f"CM{state.grid_id}_TP_{int(time.time()*1000)}"

        try:
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                symbol=symbol, side=side, type='LIMIT',
                quantity=qty, price=tp_price,
                timeInForce='GTC', newClientOrderId=client_order_id
            ))
            await guardar_orden_ejecucion(
                grid_ejecucion_id=state.grid_id,
                binance_order_id=str(res['orderId']),
                client_order_id=client_order_id,
                symbol=symbol, side=side, tipo_orden='TAKE_PROFIT',
                price=tp_price, quantity=qty
            )
            print(f"  ✅ [EXECUTOR] {symbol} Take-profit {side} colocado @ ${tp_price} | Qty:{qty}")
        except Exception as e:
            print(f"  ❌ [EXECUTOR] {symbol} Error colocando take-profit: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # TAKE-PROFIT NEUTRAL Y KILL SWITCH (FASE 3 — Autónomo)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _colocar_take_profit_neutral(self, symbol: str, state: GridExecutionState,
                                            orden_entry: dict, side_filled: str, pos_id: str):
        """Cuando se ejecuta un BUY, coloca SELL take-profit. Cuando SELL, coloca BUY."""
        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        gs = state.grid_state
        precio_entry = Decimal(str(orden_entry['price']))

        if side_filled == 'BUY':
            # Colocar SELL en el siguiente nivel superior (de niveles_sell)
            candidatos = [n for n in gs.niveles_sell if n > precio_entry * Decimal('1.0001')]
            if not candidatos:
                return
            siguiente = min(candidatos)
            tp_side = 'SELL'
        else:
            # Colocar BUY en el siguiente nivel inferior (de niveles_buy)
            candidatos = [n for n in gs.niveles_buy if n < precio_entry * Decimal('0.9999')]
            if not candidatos:
                return
            siguiente = max(candidatos)
            tp_side = 'BUY'

        # Redondear a tick_size
        ticks = int(siguiente / tick_size)
        tp_price = float(ticks * tick_size)
        qty = orden_entry['quantity']

        client_id = f"CM{state.grid_id}_TP_{int(time.time()*1000)}"

        try:
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                symbol=symbol, side=tp_side, type='LIMIT',
                quantity=qty, price=tp_price,
                timeInForce='GTC', newClientOrderId=client_id
            ))
            
            # FASE 3: Trackear orden TP en el estado propio
            tp_order_id = str(res['orderId'])
            gs.ordenes_tp_pendientes[pos_id] = tp_order_id
            
            # Actualizar PosicionReal con el ID de cierre
            for pos in gs.posiciones:
                if pos.id == pos_id:
                    pos.orden_cierre_id = tp_order_id
                    break
            
            await guardar_orden_ejecucion(
                grid_ejecucion_id=state.grid_id,
                binance_order_id=tp_order_id,
                client_order_id=client_id,
                symbol=symbol, side=tp_side, tipo_orden='TAKE_PROFIT',
                price=tp_price, quantity=qty
            )
            print(f"  ✅ [EXECUTOR] {symbol} TP {tp_side} @ ${tp_price} (respuesta a {side_filled}) | Pos: {pos_id}")
        except Exception as e:
            print(f"  ❌ [EXECUTOR] Error TP neutral: {e}")

    async def _ejecutar_kill_switch_real(self, symbol: str, state: GridExecutionState,
                                          pos_id: str, pos_tipo: str, qty: float, 
                                          razon: str, binance_order_id: str = None):
        """
        CR3 FIX: Kill switch que garantiza cierre completo.
        
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
            res = await self._api_call(asyncio.to_thread(
                self.client.futures_create_order,
                symbol=symbol, side=side_cierre, type='MARKET',
                quantity=float(qty), reduceOnly=True
            ))
            order_id = str(res.get('orderId', ''))
            print(f"  ✅ [CIERRE] Kill Switch ejecutado: {side_cierre} {qty} | Order:{order_id}")

            # Esperar propagación del fill
            await asyncio.sleep(0.3)

            # Consultar trades para obtener precio real y commission
            if order_id:
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol, orderId=order_id
                ))
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
                
                pos_amt = Decimal(str(position[0].get('positionAmt', 0)))
                
                if abs(pos_amt) < Decimal('0.0001'):
                    cierre.posicion_cerrada = True
                    print(f"  ✅ [CIERRE] {cierre.symbol} Posición ya es 0")
                    cierre.avanzar('POSICION_CERRADA')
                    return
                
                # Calcular lado y cantidad
                side = 'SELL' if pos_amt > 0 else 'BUY'
                qty = float(abs(pos_amt))
                
                # Enviar orden MARKET con reduceOnly
                res_cierre = await self._api_call(asyncio.to_thread(
                    self.client.futures_create_order,
                    symbol=cierre.symbol,
                    side=side,
                    type='MARKET',
                    quantity=qty,
                    reduceOnly=True
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
            resumen = self._cerrar_grid_total(state, precio_actual)
            
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
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _recuperar_grids_activos(self):
        """CR3 FIX: Recuperar con verificación de que el grid sigue coherente."""
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
                
                pos_amt = Decimal(str(position[0].get('positionAmt', 0))) if position else Decimal('0')
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

                # Reconstruir estado
                state = GridExecutionState(
                    grid_id=grid['id'], symbol=symbol, direction=grid['direction'],
                    capital=grid['capital_asignado'], leverage=grid['apalancamiento_usado'],
                    precio_entrada=grid['precio_entrada'],
                    niveles=niveles, qty_por_orden=qty
                )
                state.pares_abiertos = []  # FIX: Inicializar atributo faltante en recuperación
                
                # FASE 2: Reconstruir grid_state para grids neutral recuperados
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
                
                # Sincronizar posición real
                state.posicion_neta = pos_amt
                print(f"  [EXECUTOR] {symbol} Posición recuperada: {float(pos_amt):.4f}")

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

            except Exception as e:
                print(f"  ❌ [EXECUTOR] Error recuperando {symbol}: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # PROCESAR MENSAJES DE LA COLA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _procesar_mensaje(self, msg: dict):
        tipo = msg.get('tipo')

        if tipo == 'CREAR_GRID':
            direction = msg.get('direction')
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
