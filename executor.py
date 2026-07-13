"""
executor.py — FASE 3.2
Motor de ejecución real de grids en Binance Futures (Testnet / Real).
Flujo: recibe mensajes por cola, coloca órdenes LIMIT, trackea fills, maneja aborto.
Integración: GridSimulator helper para grids NEUTRALES.
"""

import asyncio
import json
import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional
from binance.client import Client
from binance.exceptions import BinanceAPIException

from config import CONFIG
from database_v5 import (
    guardar_grid_ejecucion,
    guardar_orden_ejecucion,
    actualizar_orden_fill,
    cargar_grid_ejecuciones_activos,
    cargar_ordenes_por_grid,
    actualizar_grid_ejecucion_cierre,
)


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
        self.posicion_neta = Decimal('0')     # Positivo=LONG, Negativo=SHORT
        self.pnl_real = Decimal('0')
        self.fees_real = Decimal('0')
        self.activa = True
        self.cerrando = False
        self.timestamp_inicio = time.time()

        self.pares_abiertos = []  # FIX: Residuo de grid simulator, requerido por _monitoring_loop

        # ═══ NUEVO FASE 3.2: Soporte grid neutral ═══
        self.sim_state = None  # SimState del helper
        self.grid_mode = direction  # 'LONG', 'SHORT', o 'NEUTRAL'


class GridExecutor:
    """
    Executor de grids reales en Binance Futures.
    Recibe mensajes por cola asyncio y ejecuta operaciones reales.
    """

    def __init__(self, precios_vivo: dict, signal_states: dict):
        self.precios_vivo = precios_vivo
        self.signal_states = signal_states
        self.queue: asyncio.Queue = asyncio.Queue()
        self.notifier = None  # Inyectado desde fuera si existe

        # ═══ NUEVO FASE 3.2: Inyectar helper del grid simulator ═══
        try:
            from grid_simulator import GridSimulator
            self.grid_sim = GridSimulator()
        except ImportError:
            print("  ⚠️ [EXECUTOR] GridSimulator no disponible. Grid neutral desactivado.")
            self.grid_sim = None

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

        return order_ids_guardados

    # ═══════════════════════════════════════════════════════════════════════════════
    # CREAR GRID LONG (FASE 1)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_direccional(self, symbol: str, direction: str, params: dict, price: float):
        """
        Crea un grid LONG o SHORT real en Binance Futures.
        direction: 'LONG' o 'SHORT'
        """
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

            client_order_id = f"CM{grid_id}_{idx}_{timestamp}"
            ordenes.append({
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'quantity': str(qty),
                'price': str(round(nivel)),
                'timeInForce': 'GTC',
                'newClientOrderId': client_order_id
            })

        # 7. Enviar en batches
        order_ids_guardados = await self._enviar_ordenes_batch(symbol, ordenes, grid_id)

        # 8. Crear estado en RAM
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
    # CREAR GRID NEUTRAL (NUEVO FASE 3.2)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _crear_grid_neutral(self, symbol: str, params: dict, price: float):
        """Crea un grid neutral usando el helper para lógica y el executor para órdenes reales."""
        if symbol in self._grids and self._grids[symbol].activa:
            print(f"  ⚠️ [EXECUTOR] {symbol} Ya hay grid activo")
            return

        if not self.grid_sim:
            print(f"  ❌ [EXECUTOR] {symbol} GridSimulator no disponible. No se puede crear grid neutral.")
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

        # 9. INICIAR SimState del helper (síncrono)
        state.sim_state = self.grid_sim.init_sim_state(
            grid_id=grid_id,
            sim_id=grid_id,
            symbol=symbol,
            niveles=niveles,
            qty_por_orden=qty,
            fee_rate=getattr(CONFIG, 'grid_neutral_sim_fee_rate', 0.0005),
            slippage_base=getattr(CONFIG, 'grid_neutral_sim_slippage_base', 0.0001),
            precio_inicio=price,
            timestamp_inicio=int(time.time())
        )
        state.sim_state.niveles_buy = [Decimal(str(n)) for n in niveles_buy]
        state.sim_state.niveles_sell = [Decimal(str(n)) for n in niveles_sell]
        state.sim_state.precio_referencia = Decimal(str(price))
        state.grid_mode = 'NEUTRAL'

        # 10. Colocar órdenes LIMIT reales: BUY debajo, SELL encima
        ordenes = []
        timestamp = int(time.time() * 1000)
        for idx, nivel in enumerate(niveles_buy):
            ordenes.append({
                'symbol': symbol, 'side': 'BUY', 'type': 'LIMIT',
                'quantity': str(qty), 'price': str(round(nivel)),
                'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_BUY_{idx}_{timestamp}"
            })
        for idx, nivel in enumerate(niveles_sell):
            ordenes.append({
                'symbol': symbol, 'side': 'SELL', 'type': 'LIMIT',
                'quantity': str(qty), 'price': str(round(nivel)),
                'timeInForce': 'GTC', 'newClientOrderId': f"CM{grid_id}_SELL_{idx}_{timestamp}"
            })

        # 11. Enviar batches (igual que directional)
        order_ids_guardados = await self._enviar_ordenes_batch(symbol, ordenes, grid_id)

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

    async def _monitorear_grid(self, symbol: str):
        """Consulta órdenes abiertas y fills para un grid."""
        state = self._grids[symbol]
        
        # --- NUEVO: Una sola llamada para todas las órdenes ---
        try:
            all_orders_list = await self._api_call(asyncio.to_thread(
                self.client.futures_get_all_orders, symbol=symbol
            ))
            # Crear un diccionario {orderId: order_info} para búsqueda O(1)
            all_orders_map = {str(o['orderId']): o for o in (all_orders_list or [])}
        except Exception as e:
            print(f"  ⚠️ [EXECUTOR] {symbol} Error cargando órdenes: {e}")
            all_orders_map = {}
        # -------------------------------------------------------
        
        # FASE 2.6: No procesar fills si el grid está en proceso de cierre
        if state.cerrando:
            print(f"  [EXECUTOR] {symbol} Grid en proceso de cierre, skip monitoreo")
            return
        # 1. Consultar órdenes abiertas
        open_orders = await self._api_call(asyncio.to_thread(
            self.client.futures_get_open_orders, symbol=symbol
        ))
        open_ids = {str(o['orderId']) for o in open_orders}

        # 2. Cargar nuestras órdenes de SQLite
        db_ordenes = await cargar_ordenes_por_grid(state.grid_id)

        for orden in db_ordenes:
            if orden['status'] in ('FILLED', 'CANCELED'):
                continue

            # ¿Sigue abierta en Binance?
            if str(orden['binance_order_id']) in open_ids:
                continue

            # Desapareció → buscar en el mapa local (ya cargado)
            order_info = all_orders_map.get(str(orden['binance_order_id']))
            if not order_info:
                # La orden no aparece ni en abiertas ni en histórico → raro, pero skip
                continue

            new_status = order_info['status']

            if new_status == 'FILLED':
                # ─── INTEGRACIÓN HELPER: Notificar fill para grid neutral ───
                if state.grid_mode == 'NEUTRAL' and state.sim_state:
                    side_fill = order_info['side']
                    price_fill = float(order_info.get('avgPrice') or order_info.get('price') or 0)
                    qty_fill = float(order_info.get('executedQty', 0))
                    self.grid_sim.on_fill(
                        sim_state=state.sim_state,
                        side=side_fill,
                        price=price_fill,
                        qty=qty_fill,
                        timestamp=int(time.time())
                    )

                # Registrar fill en DB
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol, orderId=orden['binance_order_id']
                ))
                for trade in trades:
                    await actualizar_orden_fill(
                        orden_id=orden['id'],
                        binance_trade_id=trade['id'],
                        price=float(trade['price']),
                        qty=float(trade['qty']),
                        commission=float(trade['commission']),
                        commission_asset=trade['commissionAsset'],
                        realized_pnl=float(trade.get('realizedPnl', 0)),
                        timestamp=trade['time']
                    )

                state.fees_real += Decimal(str(trade['commission']))
                state.pnl_real += Decimal(str(trade.get('realizedPnl', 0)))

                # Colocar take-profit según modo
                if state.grid_mode == 'NEUTRAL' and state.sim_state:
                    await self._colocar_take_profit_neutral(symbol, state, orden, order_info['side'])
                else:
                    if orden['tipo_orden'] == 'ENTRY':
                        await self._colocar_take_profit(symbol, state, orden)

            elif new_status == 'CANCELED':
                print(f"  [EXECUTOR] {symbol} Orden {orden['binance_order_id']} cancelada")

        # ─── PASO 3: POLL del helper (cada ciclo) ───
        if state.grid_mode == 'NEUTRAL' and state.sim_state:
            precio_actual = self.precios_vivo.get(symbol, 0)
            if precio_actual > 0:
                acciones = self.grid_sim.poll(
                    sim_state=state.sim_state,
                    precio_actual=precio_actual,
                    timestamp=int(time.time())
                )
                for accion in acciones:
                    if accion['tipo'] == 'KILL_SWITCH':
                        await self._ejecutar_kill_switch_real(
                            symbol=symbol,
                            state=state,
                            pos_id=accion['pos_id'],
                            pos_tipo=accion['pos_tipo'],
                            qty=accion['qty'],
                            razon=accion['razon']
                        )

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
    # TAKE-PROFIT NEUTRAL Y KILL SWITCH (NUEVO FASE 3.2)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _colocar_take_profit_neutral(self, symbol: str, state: GridExecutionState,
                                            orden_entry: dict, side_filled: str):
        """Cuando se ejecuta un BUY, coloca SELL take-profit. Cuando SELL, coloca BUY take-profit."""
        info = self._get_symbol_info(symbol)
        tick_size = Decimal(str(info['tickSize']))

        sim = state.sim_state
        precio_entry = Decimal(str(orden_entry['price']))

        if side_filled == 'BUY':
            # Colocar SELL en el siguiente nivel superior (de niveles_sell)
            candidatos = [n for n in sim.niveles_sell if n > precio_entry * Decimal('1.0001')]
            if not candidatos:
                return
            siguiente = min(candidatos)
            tp_side = 'SELL'
        else:
            # Colocar BUY en el siguiente nivel inferior (de niveles_buy)
            candidatos = [n for n in sim.niveles_buy if n < precio_entry * Decimal('0.9999')]
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
            await guardar_orden_ejecucion(
                grid_ejecucion_id=state.grid_id,
                binance_order_id=str(res['orderId']),
                client_order_id=client_id,
                symbol=symbol, side=tp_side, tipo_orden='TAKE_PROFIT',
                price=tp_price, quantity=qty
            )
            print(f"  ✅ [EXECUTOR] {symbol} TP {tp_side} @ ${tp_price} (respuesta a {side_filled})")
        except Exception as e:
            print(f"  ❌ [EXECUTOR] Error TP neutral: {e}")

    async def _ejecutar_kill_switch_real(self, symbol: str, state: GridExecutionState,
                                          pos_id: str, pos_tipo: str, qty: float, razon: str):
        """Ejecuta el cierre real de una posición identificada por el helper."""
        print(f"  🛑 [EXECUTOR] Kill Switch {pos_tipo} {pos_id} | Razón: {razon}")

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
            print(f"  ✅ [EXECUTOR] Kill Switch ejecutado: {side_cierre} {qty} | Order:{order_id}")

            # Esperar propagación del fill
            await asyncio.sleep(0.3)

            # Consultar trades para obtener precio real y commission
            if order_id and state.sim_state and self.grid_sim:
                trades = await self._api_call(asyncio.to_thread(
                    self.client.futures_account_trades,
                    symbol=symbol, orderId=order_id
                ))
                if trades:
                    total_qty = sum(float(t['qty']) for t in trades)
                    avg_price = sum(float(t['price']) * float(t['qty']) for t in trades) / total_qty if total_qty > 0 else 0
                    total_commission = sum(float(t['commission']) for t in trades)
                    total_realized_pnl = sum(float(t.get('realizedPnl', 0)) for t in trades)

                    # Notificar al helper del cierre real
                    cerrado = self.grid_sim.close_position_by_id(
                        sim=state.sim_state,
                        pos_id=pos_id,
                        precio_cierre=avg_price,
                        fee_cierre=total_commission
                    )
                    if cerrado:
                        print(f"  [EXECUTOR] Helper sincronizado: Pos {pos_id} cerrada @ ${avg_price:.4f}")

                    # Actualizar estado del executor
                    state.pnl_real += Decimal(str(total_realized_pnl))
                    state.fees_real += Decimal(str(total_commission))

        except Exception as e:
            print(f"  ❌ [EXECUTOR] Kill Switch falló: {e}")
            
    # ═══════════════════════════════════════════════════════════════════════════════
    # ABORTO SEGURO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _abortar_grid(self, symbol: str, razon: str):
        """Aborta un grid: cancela órdenes y cierra posición."""
        if symbol not in self._grids:
            return

        state = self._grids[symbol]
        if state.cerrando:
            return

        state.cerrando = True
        print(f"  🛑 [EXECUTOR] {symbol} Abortando grid... Razón: {razon}")

        # 1. Cancelar TODAS las órdenes abiertas (con verificación loop)
        for intento_cancel in range(3):
            try:
                await self._api_call(asyncio.to_thread(
                    self.client.futures_cancel_all_open_orders, symbol=symbol
                ))
                await asyncio.sleep(0.5)
                # Verificar que no queden órdenes abiertas
                open_orders_check = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_open_orders, symbol=symbol
                ))
                if not open_orders_check:
                    print(f"  ✅ [EXECUTOR] {symbol} Órdenes canceladas (0 pendientes)")
                    break
                else:
                    ids_pendientes = [o['orderId'] for o in open_orders_check]
                    print(f"  ⚠️ [EXECUTOR] {symbol} Quedan {len(open_orders_check)} órdenes pendientes: {ids_pendientes}")
                    if intento_cancel == 2:
                        # Último recurso: cancelar una por una
                        for o in open_orders_check:
                            try:
                                await self._api_call(asyncio.to_thread(
                                    self.client.futures_cancel_order,
                                    symbol=symbol, orderId=o['orderId']
                                ))
                            except Exception as e_one:
                                print(f"  ❌ [EXECUTOR] No se pudo cancelar orden {o['orderId']}: {e_one}")
            except Exception as e:
                print(f"  ❌ [EXECUTOR] {symbol} Error cancelando órdenes: {e}")

        # 2. Esperar propagación
        await asyncio.sleep(0.5)

        # NUEVO FASE 3.2: Si es grid neutral, cerrar SimState del helper
        if state.grid_mode == 'NEUTRAL' and state.sim_state and self.grid_sim:
            precio_actual = self.precios_vivo.get(symbol, 0)
            resumen = self.grid_sim.close_sim_state(state.sim_state, precio_actual)
            print(f"  [EXECUTOR] Resumen grid neutral: PnL={resumen.get('pnl_neto', 0):+.4f}")

            if self.notifier:
                try:
                    await self.notifier.enviar_telegram(
                        f"🏁 <b>Grid Neutral Cerrado — {symbol}</b>\n"
                        f"Razón: {razon}\n"
                        f"PnL Neto: {resumen.get('pnl_neto', 0):+.4f} USDT\n"
                        f"Trades: {resumen.get('trades_completados', 0)} | KS: {resumen.get('trades_kill_switch', 0)}"
                    )
                except Exception as e:
                    print(f"  ⚠️ [EXECUTOR] Error notificando cierre neutral: {e}")

        # 3. Consultar y cerrar posición
        orden_cierre_id = None
        try:
            position = await self._api_call(asyncio.to_thread(
                self.client.futures_position_information, symbol=symbol
            ))

            if position and len(position) > 0:
                pos_amt = Decimal(str(position[0].get('positionAmt', 0)))
                if abs(pos_amt) > 0:
                    side = 'SELL' if pos_amt > 0 else 'BUY'
                    res_cierre = await self._api_call(asyncio.to_thread(
                        self.client.futures_create_order,
                        symbol=symbol, side=side, type='MARKET',
                        quantity=float(abs(pos_amt)), reduceOnly=True
                    ))
                    orden_cierre_id = str(res_cierre.get('orderId', ''))
                    print(f"  ✅ [EXECUTOR] {symbol} Posición cerrada: {float(pos_amt)} | Order: {orden_cierre_id}")

                    # FASE 3 FIX: Esperar propagación y capturar realizedPnl del cierre
                    await asyncio.sleep(0.5)
                    if orden_cierre_id:
                        trades_cierre = await self._api_call(asyncio.to_thread(
                            self.client.futures_account_trades,
                            symbol=symbol, orderId=orden_cierre_id
                        ))
                        for trade in trades_cierre:
                            pnl_trade = Decimal(str(trade.get('realizedPnl', 0)))
                            comm_trade = Decimal(str(trade.get('commission', 0)))
                            if pnl_trade != 0 or comm_trade != 0:
                                state.pnl_real += pnl_trade
                                state.fees_real += comm_trade
                                print(f"  [EXECUTOR] {symbol} Cierre trade | PnL: {float(pnl_trade):+.4f} | Fee: {float(comm_trade):.4f}")
        except Exception as e:
            print(f"  ❌ [EXECUTOR] {symbol} Error cerrando posición: {e}")

        # 4. Verificar que la posición sea 0 (loop con timeout)
        for _ in range(25):  # 5 segundos máximo
            await asyncio.sleep(0.2)
            try:
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information, symbol=symbol
                ))
                if not position or len(position) == 0:
                    break
                pos_amt_check = Decimal(str(position[0].get('positionAmt', 0)))
                if abs(pos_amt_check) < Decimal('0.0001'):
                    print(f"  ✅ [EXECUTOR] {symbol} Posición confirmada en 0")
                    break
            except Exception as e:
                print(f"  ⚠️ [EXECUTOR] {symbol} Error verificando posición: {e}")
                break
        # 4. Calcular PnL final (acumulado desde fills)
        pnl_final = float(state.pnl_real)
        fees_final = float(state.fees_real)

        # 5. Guardar en SQLite
        await actualizar_grid_ejecucion_cierre(
            grid_id=state.grid_id,
            estado='CERRADO',
            pnl_real=pnl_final,
            fees_real=fees_final,
            razon_cierre=razon
        )

        state.activa = False
        del self._grids[symbol]

        print(f"  ✅ [EXECUTOR] {symbol} Grid cerrado | PnL:{pnl_final:+.4f} | Fees:{fees_final:.4f}")

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
                            f"🛑 <b>LÍMITE DE PÉRDIDA DIARIO ALCANZADO</b>\n"
                            f"PnL total: <code>{pnl_total:.4f} USDT</code>\n"
                            f"(Cerrado: {pnl_cerrado:.4f} | Activo: {pnl_activo:.4f})\n"
                            f"Abortando todos los grids activos..."
                        )
                    except Exception as e:
                        print(f"  ⚠️ [EXECUTOR] Error notificando límite: {e}")

                for sym in list(self._grids.keys()):
                    await self._abortar_grid(sym, 'limite_perdida_diaria')

        except Exception as e:
            print(f"  ⚠️ [EXECUTOR] Error verificando límite de pérdida: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # RECUPERACIÓN POST-CRASH
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _recuperar_grids_activos(self):
        """Al arrancar: recupera grids que quedaron activos en DB."""
        grids_db = await cargar_grid_ejecuciones_activos()
        if not grids_db:
            print("  [EXECUTOR] No hay grids activos para recuperar")
            return

        for grid in grids_db:
            symbol = grid['symbol']
            print(f"  [EXECUTOR] Recuperando grid {symbol} (ID:{grid['id']})...")

            try:
                # Reconstruir parámetros desde JSON
                params = json.loads(grid['grid_params_json']) if grid.get('grid_params_json') else {}
                niveles = params.get('niveles', [])
                qty = params.get('qty_por_orden', 0)

                open_orders = await self._api_call(asyncio.to_thread(
                    self.client.futures_get_open_orders, symbol=symbol
                ))

                db_ordenes = await cargar_ordenes_por_grid(grid['id'])

                # Reconstruir estado
                state = GridExecutionState(
                    grid_id=grid['id'], symbol=symbol, direction=grid['direction'],
                    capital=grid['capital_asignado'], leverage=grid['apalancamiento_usado'],
                    precio_entrada=grid['precio_entrada'],
                    niveles=niveles, qty_por_orden=qty
                )
                state.pares_abiertos = []  # FIX: Inicializar atributo faltante en recuperación
                # FASE 2.3: Reconstruir sim_state para grids neutral recuperados
                if grid['direction'] == 'NEUTRAL' and self.grid_sim:
                    state.grid_mode = 'NEUTRAL'
                    try:
                        params = json.loads(grid['grid_params_json']) if grid.get('grid_params_json') else {}
                        niveles_rec = params.get('niveles', niveles)
                        state.sim_state = self.grid_sim.init_sim_state(
                            grid_id=grid['id'],
                            sim_id=grid['id'],
                            symbol=symbol,
                            niveles=niveles_rec,
                            qty_por_orden=qty,
                            fee_rate=getattr(CONFIG, 'grid_neutral_sim_fee_rate', 0.0005),
                            slippage_base=getattr(CONFIG, 'grid_neutral_sim_slippage_base', 0.0001),
                            precio_inicio=grid['precio_entrada'],
                            timestamp_inicio=int(grid.get('timestamp_inicio', time.time()))
                        )
                        # Reconstruir niveles_buy y niveles_sell
                        price_rec = grid['precio_entrada']
                        state.sim_state.niveles_buy = [Decimal(str(n)) for n in niveles_rec if n < price_rec * 0.9995]
                        state.sim_state.niveles_sell = [Decimal(str(n)) for n in niveles_rec if n > price_rec * 1.0005]
                        state.sim_state.precio_referencia = Decimal(str(price_rec))
                        print(f"  [EXECUTOR] {symbol} SimState neutral reconstruido post-crash")
                    except Exception as e:
                        print(f"  ⚠️ [EXECUTOR] {symbol} Error reconstruyendo sim_state: {e}")
                # Consultar posición actual en Binance para este símbolo
                position = await self._api_call(asyncio.to_thread(
                    self.client.futures_position_information, symbol=symbol
                ))
                if position and len(position) > 0:
                    pos_amt = Decimal(str(position[0].get('positionAmt', 0)))
                    state.posicion_neta = pos_amt
                    print(f"  [EXECUTOR] {symbol} Posición recuperada: {float(pos_amt):.4f}")

                for orden in db_ordenes:
                    if orden['status'] == 'NEW':
                        vivo = any(str(o['orderId']) == orden['binance_order_id']
                                  for o in open_orders)
                        if vivo:
                            state.ordenes[orden['client_order_id']] = orden

                # Si posición 0 y sin órdenes abiertas, el grid ya terminó
                if float(state.posicion_neta) == 0 and not open_orders:
                    print(f"  [EXECUTOR] {symbol} Grid ya estaba terminado (pos 0, sin órdenes). Marcando CERRADO.")
                    await actualizar_grid_ejecucion_cierre(
                        grid_id=grid['id'],
                        estado='CERRADO',
                        pnl_real=0,
                        fees_real=0,
                        razon_cierre='recuperacion_post_crash_ya_terminado'
                    )
                    continue

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
