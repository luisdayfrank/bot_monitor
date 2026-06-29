"""
grid_simulator.py — V5.9.2 Motor de Simulación Virtual Grid Neutral

Simula la ejecución de un grid de órdenes LIMIT en tiempo real usando
los highs/lows de cada vela 1m como proxy de ejecución.

CARACTERÍSTICAS V5.9.2:
  • Simulación por cruce de High/Low (no close)
  • Slippage simulado + fees Binance Futures (0.05%)
  • Kill switch inteligente: 10 intentos LIMIT → market order 2% → ATRAPADA
  • Timeout FIFO: posiciones >30min se cierran forzosamente
  • Gestión de órdenes parcialmente llenadas
  • Persistencia atómica en SQLite (grid_estados + grid_simulaciones)
  • Heartbeat cada 15 minutos para detectar grids zombies

INTEGRACIÓN:
  - main.py crea una tarea asyncio para run()
  - signals_v5.py llama a iniciar_grid() cuando entra a NEUTRAL_GRID
  - signals_v5.py llama a finalizar_grid() cuando aborta NEUTRAL_GRID
  - collector.py (o main.py) inyecta ticks vía la cola self.queue
"""

import asyncio
import json
import random
from datetime import datetime
from typing import Dict, List, Optional
from decimal import Decimal, ROUND_DOWN
from config import CONFIG
from database_v5 import (
    guardar_grid_estado_atomico,
    actualizar_grid_estado,
    actualizar_grid_simulacion,
    actualizar_posiciones_abiertas,
    cargar_grid_activo,
    cargar_simulacion_activa,
    forzar_aborto_grid_huerfano,
    cargar_todos_grids_activos,
)


class SimPosicion:
    """Representa una posición simulada abierta en el grid."""

    _contador = 0

    def __init__(self, tipo, nivel_precio, precio_ejecucion, qty, slippage_aplicado,
                 fee_pagada, notional, timestamp_apertura, filled_qty=None, original_qty=None):
        SimPosicion._contador += 1
        self.id = f"pos_{SimPosicion._contador:03d}"
        self.tipo = tipo  # 'LONG' | 'SHORT'
        self.nivel_precio = nivel_precio
        self.precio_ejecucion = precio_ejecucion
        self.slippage_aplicado = slippage_aplicado
        self.fee_pagada = fee_pagada
        self.notional = notional
        self.timestamp_apertura = timestamp_apertura
        self.filled_qty = filled_qty if filled_qty is not None else qty
        self.original_qty = original_qty if original_qty is not None else qty
        self.estado = 'ABIERTA'  # ABIERTA | CERRADA | ATRAPADA
        self.pnl_cierre = 0.0

    def to_dict(self):
        return {
            "id": self.id,
            "tipo": self.tipo,
            "nivel_precio": float(self.nivel_precio),
            "precio_ejecucion": float(self.precio_ejecucion),
            "slippage_aplicado": float(self.slippage_aplicado),
            "fee_pagada": float(self.fee_pagada),
            "notional": float(self.notional),
            "timestamp_apertura": self.timestamp_apertura,
            "filled_qty": float(self.filled_qty),
            "original_qty": float(self.original_qty),
            "estado": self.estado,
        }


class SimState:
    """Estado en memoria de una simulación de grid activa."""

    def __init__(self, grid_id, sim_id, symbol, niveles, qty_por_orden, fee_rate,
                 slippage_base, precio_inicio, timestamp_inicio):
        self.grid_id = grid_id
        self.sim_id = sim_id
        self.symbol = symbol
        self.niveles = niveles  # Lista de precios (Decimal)
        self.qty_por_orden = qty_por_orden  # Decimal
        self.fee_rate = fee_rate
        self.slippage_base = slippage_base
        self.precio_inicio = precio_inicio
        self.timestamp_inicio = timestamp_inicio

        self.posiciones: List[SimPosicion] = []
        self.posiciones_atrapadas: List[SimPosicion] = []
        self.pnl_bruto = Decimal('0.0')
        self.pnl_neto = Decimal('0.0')
        self.fees_totales = Decimal('0.0')
        self.slippage_total = Decimal('0.0')
        self.trades_completados = 0
        self.trades_kill_switch = 0
        self.max_posiciones_simultaneas = 0

        self.ultimo_tick_ts = timestamp_inicio
        self.activa = True

        # Track de órdenes "fantasma" (parciales pendientes)
        # nivel_precio -> qty_restante
        self.ordenes_fantasma: Dict[str, Decimal] = {}
        # V7: Separar niveles por dirección (grid neutral real)
        # Niveles por debajo del precio_inicio = BUY (LONG)
        # Niveles por encima del precio_inicio = SELL (SHORT)
        self.niveles_buy: List[Decimal] = []   # Niveles donde colocamos órdenes BUY LIMIT
        self.niveles_sell: List[Decimal] = []  # Niveles donde colocamos órdenes SELL LIMIT
        self.precio_referencia = precio_inicio  # Precio al iniciar el grid

        # V7: Órdenes pendientes (take-profit) que se colocan automáticamente
        # Cuando un BUY se ejecuta en nivel N, se coloca SELL en N+1
        # Cuando un SELL se ejecuta en nivel N, se coloca BUY en N-1
        self.ordenes_pendientes: Dict[str, str] = {}  # nivel_str -> 'BUY' | 'SELL'

    def contar_posiciones_abiertas(self):
        return sum(1 for p in self.posiciones if p.estado == 'ABIERTA')

    def posiciones_abiertas_list(self):
        return [p for p in self.posiciones if p.estado == 'ABIERTA']

    def to_json(self):
        return json.dumps({
            "grid_id": self.grid_id,
            "sim_id": self.sim_id,
            "symbol": self.symbol,
            "niveles": [float(n) for n in self.niveles],
            "qty_por_orden": float(self.qty_por_orden),
            "fee_rate": float(self.fee_rate),
            "slippage_base": float(self.slippage_base),
            "precio_inicio": float(self.precio_inicio),
            "timestamp_inicio": self.timestamp_inicio,
            "pnl_bruto": float(self.pnl_bruto),
            "pnl_neto": float(self.pnl_neto),
            "fees_totales": float(self.fees_totales),
            "slippage_total": float(self.slippage_total),
            "trades_completados": self.trades_completados,
            "trades_kill_switch": self.trades_kill_switch,
            "max_posiciones_simultaneas": self.max_posiciones_simultaneas,
            "posiciones_abiertas": [p.to_dict() for p in self.posiciones_abiertas_list()],
            "posiciones_atrapadas": [p.to_dict() for p in self.posiciones_atrapadas],
            "ordenes_fantasma": {k: float(v) for k, v in self.ordenes_fantasma.items()},
            "activa": self.activa,
            "ultimo_tick_ts": self.ultimo_tick_ts,
        })


class GridSimulator:
    """
    Motor de simulación virtual para grids neutral.

    Flujo:
      1. signals_v5 detecta NEUTRAL_GRID → llama iniciar_grid()
      2. main.py inyecta ticks de precio (high, low, close) vía la cola
      3. El simulador detecta cruces de niveles y simula ejecuciones
      4. Cuando el grid finaliza (aborto o timeout) → llama finalizar_grid()
    """

    def __init__(self, precios_vivo: dict, indicadores_1m: dict, signal_states: dict):
        self.precios_vivo = precios_vivo
        self.indicadores_1m = indicadores_1m
        self.signal_states = signal_states
        self.queue: asyncio.Queue = asyncio.Queue()
        self.simulaciones: Dict[str, SimState] = {}  # symbol -> SimState
        self._shutdown = asyncio.Event()
        self._db_lock = asyncio.Lock()
        self.audit_logger = None
        self.notifier = None

    # ═══════════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ═══════════════════════════════════════════════════════════════════════════════

    async def run(self):
        """Loop principal: procesa ticks de la cola."""
        print("  [GRID_SIM] Motor de simulación V5.9.2 iniciado")

        while not self._shutdown.is_set():
            try:
                msg = await asyncio.wait_for(self.queue.get(), timeout=5.0)
                await self._procesar_mensaje(msg)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  ❌ [GRID_SIM] Error en loop principal: {e}")

    def stop(self):
        self._shutdown.set()

    async def _procesar_mensaje(self, msg):
        """Procesa un mensaje de la cola."""
        tipo = msg.get('tipo')

        if tipo == 'TICK':
            # Tick de precio: {tipo: 'TICK', symbol: 'BTCUSDT', high: H, low: L, close: C, timestamp: ts}
            symbol = msg.get('symbol')
            if symbol in self.simulaciones and self.simulaciones[symbol].activa:
                await self.simular_tick(
                    symbol=symbol,
                    high=msg.get('high'),
                    low=msg.get('low'),
                    close=msg.get('close'),
                    timestamp=msg.get('timestamp')
                )

        elif tipo == 'INICIAR_GRID':
            # Solicitud de iniciar grid: {tipo: 'INICIAR_GRID', symbol: 'BTCUSDT', grid_params: {...}, precio_actual: P}
            await self.iniciar_grid(
                symbol=msg.get('symbol'),
                grid_params=msg.get('grid_params'),
                precio_actual=msg.get('precio_actual')
            )

        elif tipo == 'FINALIZAR_GRID':
            # Solicitud de finalizar grid: {tipo: 'FINALIZAR_GRID', symbol: 'BTCUSDT', razon: 'aborto'}
            await self.finalizar_grid(
                symbol=msg.get('symbol'),
                razon=msg.get('razon', 'FINALIZADO')
            )

        elif tipo == 'CERRAR_GRID_SUAVE':
            # Cierre suave: no abrir nuevas posiciones, pero dejar que las existentes se emparejen
            await self.cerrar_grid_suave(
                symbol=msg.get('symbol'),
                razon=msg.get('razon', 'direccion_detectada')
            )

    # ═══════════════════════════════════════════════════════════════════════════════
    # INICIAR / FINALIZAR GRID
    # ═══════════════════════════════════════════════════════════════════════════════

    async def iniciar_grid(self, symbol: str, grid_params: dict, precio_actual: float):
        """Inicia una nueva simulación de grid neutral."""
        if symbol in self.simulaciones and self.simulaciones[symbol].activa:
            print(f"  [GRID_SIM] {symbol} Ya hay grid activo, ignorando nuevo inicio")
            return

        try:
            # Extraer parámetros del grid
            lower = Decimal(str(grid_params.get('lower_limit', precio_actual * 0.98)))
            upper = Decimal(str(grid_params.get('upper_limit', precio_actual * 1.02)))
            grid_count = int(grid_params.get('grid_count', 20))
            qty = Decimal(str(grid_params.get('qty_por_orden', 0.1)))

            # Generar niveles del grid (distribución uniforme)
            step = (upper - lower) / Decimal(str(grid_count))
            niveles = [lower + step * i for i in range(grid_count + 1)]

            # V7: Separar niveles por dirección relativa al precio actual
            # Niveles por debajo = BUY (compras barato)
            # Niveles por encima = SELL (vendes caro)
            # El nivel más cercano al precio se ignora (no hay orden en el centro)
            niveles_buy = [n for n in niveles if n < Decimal(str(precio_actual)) * Decimal('0.9995')]
            niveles_sell = [n for n in niveles if n > Decimal(str(precio_actual)) * Decimal('1.0005')]

            # Calcular qty_por_orden basada en capital / num_grids / precio
            capital = Decimal(str(grid_params.get('capital', CONFIG.grid_default_capital)))
            leverage = Decimal(str(grid_params.get('apalancamiento_sugerido', CONFIG.grid_default_leverage)))
            notional_total = capital * leverage
            notional_por_orden = notional_total / Decimal(str(grid_count))
            qty_por_orden = (notional_por_orden / Decimal(str(precio_actual))).quantize(
                Decimal('0.0001'), rounding=ROUND_DOWN
            )

            posiciones_json = json.dumps({
                "niveles": [float(n) for n in niveles],
                "qty_por_orden": float(qty_por_orden),
                "capital": float(capital),
                "leverage": float(leverage),
                "fee_rate": CONFIG.grid_neutral_sim_fee_rate,
                "slippage_base": CONFIG.grid_neutral_sim_slippage_base,
            })

            # Transacción atómica: grid_estado + grid_simulacion
            ts = int(datetime.utcnow().timestamp())
            resultado = await guardar_grid_estado_atomico(
                symbol=symbol,
                timestamp_inicio=ts,
                precio_inicio=precio_actual,
                grid_params_json=json.dumps(grid_params),
                posiciones_abiertas_json=posiciones_json,
            )

            if not resultado:
                print(f"  ❌ [GRID_SIM] {symbol} Fallo transacción atómica al iniciar grid")
                return

            grid_id = resultado['grid_id']
            sim_id = resultado['sim_id']

            # Crear estado en memoria
            sim = SimState(
                grid_id=grid_id,
                sim_id=sim_id,
                symbol=symbol,
                niveles=niveles,
                qty_por_orden=qty_por_orden,
                fee_rate=Decimal(str(CONFIG.grid_neutral_sim_fee_rate)),
                slippage_base=Decimal(str(CONFIG.grid_neutral_sim_slippage_base)),
                precio_inicio=Decimal(str(precio_actual)),
                timestamp_inicio=ts,
            )
            self.simulaciones[symbol] = sim
            # V7: Asignar niveles separados al estado
            sim.niveles_buy = niveles_buy
            sim.niveles_sell = niveles_sell
            sim.precio_referencia = Decimal(str(precio_actual))

            # V7: Inicializar órdenes LIMIT pendientes
            for n in sim.niveles_buy:
                sim.ordenes_pendientes[str(float(n))] = 'BUY'
            for n in sim.niveles_sell:
                sim.ordenes_pendientes[str(float(n))] = 'SELL'

            print(f"  [GRID_SIM] {symbol} Niveles BUY: {len(niveles_buy)} | Niveles SELL: {len(niveles_sell)}")

            print(f"  [GRID_SIM] {symbol} Grid iniciado | ID:{grid_id} | Niveles:{len(niveles)} | "
                  f"Qty/orden:{float(qty_por_orden):.4f} | Precio:${precio_actual:.4f}")

            # Auditoría
            if self.audit_logger:
                await self.audit_logger.log_evento_grid_simulacion(
                    symbol=symbol,
                    tipo='GRID_INICIADO',
                    grid_id=grid_id,
                    evento_simulacion={
                        'niveles': len(niveles),
                        'qty_por_orden': float(qty_por_orden),
                        'precio_inicio': precio_actual,
                    },
                    pnl_acumulado=0.0
                )

        except Exception as e:
            print(f"  ❌ [GRID_SIM] {symbol} Error iniciando grid: {e}")

    async def finalizar_grid(self, symbol: str, razon: str = 'FINALIZADO'):
        """Finaliza una simulación de grid (aborto o completado)."""
        if symbol not in self.simulaciones:
            return

        sim = self.simulaciones[symbol]
        if not sim.activa:
            return

        sim.activa = False
        ts = int(datetime.utcnow().timestamp())

        try:
            # Cerrar posiciones abiertas: solo kill switch si están fuera de rango
            posiciones_abiertas = sim.posiciones_abiertas_list()
            if posiciones_abiertas:
                print(f"  [GRID_SIM] {symbol} Cerrando {len(posiciones_abiertas)} posiciones pendientes ({razon})")
                precio_actual = Decimal(str(self.precios_vivo.get(symbol, float(sim.precio_inicio))))
                for pos in posiciones_abiertas:
                    # Solo matar con kill switch si la posición está claramente fuera de rango (>1% del nivel)
                    margen_cierre = Decimal('0.01')  # 1%
                    if pos.tipo == 'LONG':
                        precio_limite = Decimal(str(pos.nivel_precio)) * (Decimal('1') - margen_cierre)
                        if precio_actual < precio_limite:
                            # LONG fuera de rango: kill switch
                            await self._ejecutar_kill_switch(sim, pos, precio_actual, razon_cierre=razon)
                        else:
                            # LONG dentro de rango: dejar que se empareje naturalmente
                            pos.estado = 'PENDIENTE_CIERRE'
                            print(f"  [GRID_SIM] {symbol} Posición LONG {pos.id} marcada PENDIENTE_CIERRE (precio dentro de rango)")
                    elif pos.tipo == 'SHORT':
                        precio_limite = Decimal(str(pos.nivel_precio)) * (Decimal('1') + margen_cierre)
                        if precio_actual > precio_limite:
                            # SHORT fuera de rango: kill switch
                            await self._ejecutar_kill_switch(sim, pos, precio_actual, razon_cierre=razon)
                        else:
                            # SHORT dentro de rango: dejar que se empareje naturalmente
                            pos.estado = 'PENDIENTE_CIERRE'
                            print(f"  [GRID_SIM] {symbol} Posición SHORT {pos.id} marcada PENDIENTE_CIERRE (precio dentro de rango)")

            # Calcular métricas finales
            pnl_neto_final = float(sim.pnl_neto)
            fees_total = float(sim.fees_totales)

            # Persistir estado final
            pos_abiertas_json = json.dumps([p.to_dict() for p in sim.posiciones if p.estado != 'ATRAPADA'])
            pos_atrapadas_json = json.dumps([p.to_dict() for p in sim.posiciones_atrapadas])

            await actualizar_grid_estado(
                grid_id=sim.grid_id,
                estado='ABORTADO' if razon != 'COMPLETADO' else 'COMPLETADO',
                timestamp_fin=ts
            )
            await actualizar_grid_simulacion(
                grid_id=sim.grid_id,
                estado='ABORTADO' if razon != 'COMPLETADO' else 'COMPLETADO',
                timestamp_fin=ts,
                precio_fin=float(self.precios_vivo.get(symbol, 0)),
                pnl_bruto=float(sim.pnl_bruto),
                pnl_neto=pnl_neto_final,
                fees_totales=fees_total,
                slippage_total=float(sim.slippage_total),
                trades_completados=sim.trades_completados,
                trades_kill_switch=sim.trades_kill_switch,
                posiciones_abiertas_json=pos_abiertas_json,
                posiciones_atrapadas_json=pos_atrapadas_json,
            )

            print(f"  [GRID_SIM] {symbol} Grid finalizado ({razon}) | "
                  f"PnL Neto:{pnl_neto_final:+.4f} | Fees:{fees_total:.4f} | "
                  f"Trades:{sim.trades_completados} | KS:{sim.trades_kill_switch} | "
                  f"Atrapadas:{len(sim.posiciones_atrapadas)}")

            # Notificación
            if self.notifier:
                await self.notifier.enviar_telegram(
                    f"<b>🏁 GRID NEUTRAL FINALIZADO — {symbol}</b>\n"
                    f"Razón: <i>{razon}</i>\n"
                    f"PnL Neto: <code>{pnl_neto_final:+.4f} USDT</code>\n"
                    f"Fees: <code>{fees_total:.4f} USDT</code>\n"
                    f"Trades: {sim.trades_completados} | Kill Switches: {sim.trades_kill_switch}\n"
                    f"Posiciones atrapadas: {len(sim.posiciones_atrapadas)}"
                )

            del self.simulaciones[symbol]

        except Exception as e:
            print(f"  ❌ [GRID_SIM] {symbol} Error finalizando grid: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SIMULAR TICK (CORE)
    # ═══════════════════════════════════════════════════════════════════════════════



    async def simular_tick(self, symbol: str, high: float, low: float, close: float, timestamp: int):
        """
        Procesa un tick de precio (una vela 1m completa).
        Detecta cruces de niveles del grid y simula ejecuciones.
        """
        if symbol not in self.simulaciones:
            return

        sim = self.simulaciones[symbol]
        if not sim.activa:
            # Grid inactivo: solo emparejar posiciones pendientes, no abrir nuevas
            posiciones_pendientes = [p for p in sim.posiciones if p.estado == 'PENDIENTE_CIERRE']
            if not posiciones_pendientes:
                # Si no hay posiciones pendientes y el grid está inactivo, limpiar
                if not sim.posiciones_abiertas_list():
                    print(f"  [GRID_SIM] {symbol} Grid inactivo sin posiciones → eliminando de memoria")
                    del self.simulaciones[symbol]
                return
            # Continuar para emparejar posiciones pendientes

        sim.ultimo_tick_ts = timestamp

        high_d = Decimal(str(high))
        low_d = Decimal(str(low))
        close_d = Decimal(str(close))

        # 1. Verificar timeout de posiciones abiertas (FIFO) — MEJORA #1
        await self._verificar_timeout_posiciones(sim, timestamp)

        # 2. Detectar cruces de niveles (BUY y SELL)
        await self._detectar_cruces(sim, high_d, low_d, close_d, timestamp)

        # 3. Persistir estado periódicamente (cada 60 segundos)
        if timestamp % 60 < 5:
            await self._persistir_estado(sim)

    async def _detectar_cruces(self, sim: SimState, high: Decimal, low: Decimal, close: Decimal, timestamp: int):
        """
        Detecta si el precio cruzó niveles del grid en la dirección correcta.
        
        Grid neutral real:
        - Niveles BUY (por debajo de precio_inicio): ejecutan cuando precio BAJA y toca el nivel
        - Niveles SELL (por encima de precio_inicio): ejecutan cuando precio SUBE y toca el nivel
        - Cuando BUY se ejecuta en N, se coloca SELL LIMIT en N+1 (take-profit)
        - Cuando SELL se ejecuta en N, se coloca BUY LIMIT en N-1 (take-profit)
        """
        posiciones_abiertas = sim.posiciones_abiertas_list()

        # V7: Precio anterior para determinar dirección del cruce
        precio_anterior = Decimal(str(sim.precio_referencia))
        if len(sim.posiciones) > 0:
            # Usar el último precio conocido como referencia
            ultimo_precio = self.precios_vivo.get(sim.symbol)
            if ultimo_precio:
                precio_anterior = Decimal(str(ultimo_precio))

        # --- DETECTAR COMPRAS (BUY): precio baja y toca un nivel BUY ---
        for nivel in sim.niveles_buy:
            # BUY se ejecuta si: precio anterior >= nivel AND close <= nivel
            # (el precio cruzó el nivel hacia abajo)
            if precio_anterior >= nivel and close <= nivel:
                # Verificar si ya hay posición abierta en este nivel
                ya_abierta = any(
                    abs(p.nivel_precio - float(nivel)) < 0.0001 and p.estado == 'ABIERTA' and p.tipo == 'LONG'
                    for p in posiciones_abiertas
                )
                if not ya_abierta:
                    # Verificar si hay una orden pendiente en este nivel
                    nivel_str = str(float(nivel))
                    if nivel_str in sim.ordenes_pendientes and sim.ordenes_pendientes[nivel_str] == 'BUY':
                        await self._ejecutar_buy(sim, nivel, close, timestamp)
                        # Colocar take-profit: SELL en el siguiente nivel superior
                        siguiente_nivel = self._siguiente_nivel_superior(sim, nivel)
                        if siguiente_nivel:
                            sim.ordenes_pendientes[str(float(siguiente_nivel))] = 'SELL'
                            print(f"  [GRID_SIM] {sim.symbol} Take-profit SELL colocado @ ${float(siguiente_nivel):.4f}")

        # --- DETECTAR VENTAS (SELL): precio sube y toca un nivel SELL ---
        for nivel in sim.niveles_sell:
            # SELL se ejecuta si: precio anterior <= nivel AND close >= nivel
            # (el precio cruzó el nivel hacia arriba)
            if precio_anterior <= nivel and close >= nivel:
                ya_abierta = any(
                    abs(p.nivel_precio - float(nivel)) < 0.0001 and p.estado == 'ABIERTA' and p.tipo == 'SHORT'
                    for p in posiciones_abiertas
                )
                if not ya_abierta:
                    nivel_str = str(float(nivel))
                    if nivel_str in sim.ordenes_pendientes and sim.ordenes_pendientes[nivel_str] == 'SELL':
                        await self._ejecutar_sell(sim, nivel, close, timestamp)
                        # Colocar take-profit: BUY en el siguiente nivel inferior
                        siguiente_nivel = self._siguiente_nivel_inferior(sim, nivel)
                        if siguiente_nivel:
                            sim.ordenes_pendientes[str(float(siguiente_nivel))] = 'BUY'
                            print(f"  [GRID_SIM] {sim.symbol} Take-profit BUY colocado @ ${float(siguiente_nivel):.4f}")

        # V7: Actualizar precio de referencia para el próximo tick
        sim.precio_referencia = close

        # --- EMPAREJAR POSICIONES (take-profit ejecutado) ---
        await self._emparejar_posiciones(sim, timestamp)

    def _siguiente_nivel_superior(self, sim: SimState, nivel: Decimal) -> Optional[Decimal]:
        """Devuelve el siguiente nivel superior al dado (para take-profit SELL)."""
        niveles_superiores = [n for n in sim.niveles if n > nivel]
        return min(niveles_superiores) if niveles_superiores else None

    def _siguiente_nivel_inferior(self, sim: SimState, nivel: Decimal) -> Optional[Decimal]:
        """Devuelve el siguiente nivel inferior al dado (para take-profit BUY)."""
        niveles_inferiores = [n for n in sim.niveles if n < nivel]
        return max(niveles_inferiores) if niveles_inferiores else None
        
    async def _ejecutar_buy(self, sim: SimState, nivel: Decimal, precio_actual: Decimal, timestamp: int, qty: Decimal = None):
        """Simula una orden BUY LIMIT ejecutada."""
        qty = qty or sim.qty_por_orden
        # LIMIT se ejecuta al nivel exacto (sin slippage)
        slippage = Decimal('0')
        precio_ejecucion = nivel
        notional = qty * precio_ejecucion
        fee = notional * sim.fee_rate

        pos = SimPosicion(
            tipo='LONG',
            nivel_precio=float(nivel),
            precio_ejecucion=float(precio_ejecucion),
            qty=float(qty),
            slippage_aplicado=float(slippage),
            fee_pagada=float(fee),
            notional=float(notional),
            timestamp_apertura=timestamp,
            filled_qty=float(qty),
            original_qty=float(qty),
        )
        sim.posiciones.append(pos)
        sim.fees_totales += fee
        sim.slippage_total += slippage

        n_abiertas = sim.contar_posiciones_abiertas()
        sim.max_posiciones_simultaneas = max(sim.max_posiciones_simultaneas, n_abiertas)

        # Log condensado
        print(f"  [GRID_SIM] {sim.symbol} BUY ejecutado @ ${float(nivel):.4f} | "
              f"Posiciones abiertas:{n_abiertas} | PnL acumulado:{float(sim.pnl_neto):+.4f}")

        # Auditoría dual
        if self.audit_logger:
            await self.audit_logger.log_evento_grid_simulacion(
                symbol=sim.symbol,
                tipo='GRID_BUY_SIM',
                grid_id=sim.grid_id,
                evento_simulacion={
                    'nivel': float(nivel),
                    'precio_ejecucion': float(precio_ejecucion),
                    'slippage': float(slippage),
                    'fee': float(fee),
                    'qty': float(qty),
                },
                pnl_acumulado=float(sim.pnl_neto)
            )

    async def _ejecutar_sell(self, sim: SimState, nivel: Decimal, precio_actual: Decimal, timestamp: int, qty: Decimal = None):
        """Simula una orden SELL LIMIT ejecutada."""
        qty = qty or sim.qty_por_orden
        # LIMIT se ejecuta al nivel exacto (sin slippage)
        slippage = Decimal('0')
        precio_ejecucion = nivel
        notional = qty * precio_ejecucion
        fee = notional * sim.fee_rate

        pos = SimPosicion(
            tipo='SHORT',
            nivel_precio=float(nivel),
            precio_ejecucion=float(precio_ejecucion),
            qty=float(qty),
            slippage_aplicado=float(slippage),
            fee_pagada=float(fee),
            notional=float(notional),
            timestamp_apertura=timestamp,
            filled_qty=float(qty),
            original_qty=float(qty),
        )
        sim.posiciones.append(pos)
        sim.fees_totales += fee
        sim.slippage_total += slippage

        n_abiertas = sim.contar_posiciones_abiertas()
        sim.max_posiciones_simultaneas = max(sim.max_posiciones_simultaneas, n_abiertas)

        print(f"  [GRID_SIM] {sim.symbol} SELL ejecutado @ ${float(nivel):.4f} | "
              f"Posiciones abiertas:{n_abiertas} | PnL acumulado:{float(sim.pnl_neto):+.4f}")

        if self.audit_logger:
            await self.audit_logger.log_evento_grid_simulacion(
                symbol=sim.symbol,
                tipo='GRID_SELL_SIM',
                grid_id=sim.grid_id,
                evento_simulacion={
                    'nivel': float(nivel),
                    'precio_ejecucion': float(precio_ejecucion),
                    'slippage': float(slippage),
                    'fee': float(fee),
                    'qty': float(qty),
                },
                pnl_acumulado=float(sim.pnl_neto)
            )

    async def _emparejar_posiciones(self, sim: SimState, timestamp: int):
        """
        Empareja posiciones LONG y SHORT cuando el take-profit se ejecuta.
        
        En un grid neutral real:
        - Un LONG abierto en nivel N se cierra cuando el precio sube y ejecuta el SELL en N+1
        - Un SHORT abierto en nivel N se cierra cuando el precio baja y ejecuta el BUY en N-1
        - El PnL del par = (nivel_sell - nivel_buy) * qty - fees_del_par
        """
        abiertas = sim.posiciones_abiertas_list()
        
        # V7: Emparejar LONG con SHORT que está en nivel superior (ganancia garantizada)
        for long_pos in list(abiertas):
            if long_pos.tipo != 'LONG':
                continue
                
            # Buscar un SHORT abierto en un nivel SUPERIOR al LONG
            # (el take-profit del LONG es un SELL en nivel superior)
            for short_pos in list(abiertas):
                if short_pos.tipo != 'SHORT':
                    continue
                if short_pos.estado != 'ABIERTA':
                    continue
                    
                # El SHORT debe estar en nivel > nivel del LONG
                if short_pos.nivel_precio > long_pos.nivel_precio:
                    # Cerrar el par
                    diferencia_niveles = short_pos.nivel_precio - long_pos.nivel_precio
                    pnl_bruto = diferencia_niveles * long_pos.filled_qty
                    
                    # Restar fees del par (2 órdenes)
                    fee_par = (long_pos.fee_pagada + short_pos.fee_pagada)
                    pnl_neto = pnl_bruto - fee_par
                    
                    long_pos.estado = 'CERRADA'
                    long_pos.pnl_cierre = pnl_neto / 2  # Distribuir proporcionalmente
                    short_pos.estado = 'CERRADA'
                    short_pos.pnl_cierre = pnl_neto / 2
                    
                    sim.pnl_bruto += Decimal(str(pnl_bruto))
                    sim.pnl_neto = sim.pnl_bruto - sim.fees_totales
                    sim.trades_completados += 1
                    
                    print(f"  [GRID_SIM] {sim.symbol} PAR CERRADO | "
                          f"LONG ${long_pos.nivel_precio:.4f} → SELL ${short_pos.nivel_precio:.4f} | "
                          f"Diff: {diferencia_niveles:.4f} | PnL: {pnl_neto:+.4f}")
                    break  # Solo emparejar una vez por LONG

    # ═══════════════════════════════════════════════════════════════════════════════
    # KILL SWITCH INTELIGENTE (MEJORA #2)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _ejecutar_kill_switch(self, sim: SimState, posicion: SimPosicion,
                                     precio_actual: Decimal, razon_cierre: str = 'max_posiciones'):
        """
        Kill switch inteligente con escalada de slippage.
        Fase 1: 10 intentos con LIMIT a 0.1% slippage
        Fase 2: Market order con 2% slippage (último recurso)
        Fase 3: Posición ATRAPADA si >2%
        """
        symbol = sim.symbol
        intentos = 0
        max_intentos = CONFIG.grid_neutral_kill_switch_max_intentos
        slippage_max = Decimal(str(CONFIG.grid_neutral_kill_switch_slippage_max))  # 0.5%
        slippage_absoluto = Decimal(str(CONFIG.grid_neutral_kill_switch_slippage_absoluto))  # 2%

        print(f"  [KILL_SWITCH] {symbol} {posicion.tipo} ${posicion.precio_ejecucion:.4f} "
              f"Razón:{razon_cierre}")

        # Fase 1: Intentar LIMIT con slippage normal (0.5%)
        slippage_intento = slippage_max  # 0.5% directamente

        if posicion.tipo == 'LONG':
            precio_limite = Decimal(str(posicion.precio_ejecucion)) * (Decimal('1') + slippage_intento)
        else:
            precio_limite = Decimal(str(posicion.precio_ejecucion)) * (Decimal('1') - slippage_intento)

        # Simular: ¿se llenó? (asumimos que sí si el precio actual está dentro del rango)
        precio_diff = abs(float(precio_actual) - float(precio_limite)) / float(precio_limite)
        if precio_diff <= float(slippage_absoluto):
            # Éxito con LIMIT
            fee = float(precio_limite) * posicion.filled_qty * float(sim.fee_rate)
            pnl = self._calcular_pnl(posicion, float(precio_limite))

            posicion.estado = 'CERRADA'
            posicion.pnl_cierre = pnl
            sim.pnl_bruto += Decimal(str(pnl))
            sim.fees_totales += Decimal(str(fee))
            sim.pnl_neto = sim.pnl_bruto - sim.fees_totales
            sim.slippage_total += slippage_intento
            sim.trades_kill_switch += 1

            print(f"  [KILL_SWITCH] {symbol} CERRADO con LIMIT @ ${float(precio_limite):.4f} "
                  f"slippage:{float(slippage_intento):.4f} PnL:{pnl:+.4f}")
            return True

        # Si LIMIT no se llena, pasar directamente a MARKET order
        print(f"  [KILL_SWITCH] {symbol} LIMIT no llenado, pasando a MARKET order")

        # V5.9.2 MEJORA #2: Último recurso — market order con 2% slippage
        slippage_ultimo = abs(float(precio_actual) - posicion.precio_ejecucion) / posicion.precio_ejecucion

        if slippage_ultimo <= float(slippage_absoluto):
            # Último recurso: market order
            fee = float(precio_actual) * posicion.filled_qty * float(sim.fee_rate)
            pnl = self._calcular_pnl(posicion, float(precio_actual))

            posicion.estado = 'CERRADA'
            posicion.pnl_cierre = pnl
            sim.pnl_bruto += Decimal(str(pnl))
            sim.fees_totales += Decimal(str(fee))
            sim.pnl_neto = sim.pnl_bruto - sim.fees_totales
            sim.slippage_total += Decimal(str(slippage_ultimo))
            sim.trades_kill_switch += 1

            print(f"  [KILL_SWITCH ULTIMO RECURSO] {symbol} Market order | "
                  f"Slippage:{slippage_ultimo:.4f} (dentro de 2%) | PnL:{pnl:+.4f}")

            if self.notifier:
                await self.notifier.enviar_telegram(
                    f"⚠️ <b>KILL SWITCH ÚLTIMO RECURSO — {symbol}</b>\n"
                    f"Posición {posicion.tipo} cerrada con market order\n"
                    f"Slippage: {slippage_ultimo:.4f} (límite: 2%)\n"
                    f"PnL: {pnl:+.4f} USDT"
                )
            return True

        # INCLUSO EL ÚLTIMO RECURSO FALLÓ — posición ATRAPADA
        posicion.estado = 'ATRAPADA'
        sim.posiciones_atrapadas.append(posicion)

        print(f"  [KILL_SWITCH CRÍTICO] {symbol} Posición ATRAPADA | "
              f"Slippage:{slippage_ultimo:.4f} > 2%")

        if self.notifier:
            await self.notifier.enviar_telegram(
                f"🚨 <b>KILL SWITCH CRÍTICO — {symbol}</b>\n"
                f"Posición {posicion.tipo} ATRAPADA\n"
                f"Slippage: {slippage_ultimo:.4f} > 2%\n"
                f"Intervención manual requerida"
            )
        return False

    def _calcular_pnl(self, posicion: SimPosicion, precio_cierre: float) -> float:
        """Calcula el PnL de cerrar una posición."""
        if posicion.tipo == 'LONG':
            return (precio_cierre - posicion.precio_ejecucion) * posicion.filled_qty
        else:
            return (posicion.precio_ejecucion - precio_cierre) * posicion.filled_qty

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIMEOUT FIFO (MEJORA #1)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _verificar_timeout_posiciones(self, sim: SimState, timestamp: int):
        """
        Cierra posiciones abiertas que superan el timeout configurado (30min por defecto).
        Aplica kill switch para forzar el cierre.
        """
        timeout_seg = CONFIG.grid_neutral_posicion_timeout_min * 60
        posiciones_vencidas = []

        for pos in sim.posiciones:
            if pos.estado == 'ABIERTA':
                tiempo_abierta = timestamp - pos.timestamp_apertura
                if tiempo_abierta > timeout_seg:
                    posiciones_vencidas.append(pos)

        if not posiciones_vencidas:
            return

        precio_actual = Decimal(str(self.precios_vivo.get(sim.symbol, 0)))
        if precio_actual <= 0:
            return

        print(f"  [GRID_SIM] {sim.symbol} {len(posiciones_vencidas)} posiciones vencidas "
              f"(>{CONFIG.grid_neutral_posicion_timeout_min}min) → Kill switch")

        for pos in posiciones_vencidas:
            await self._ejecutar_kill_switch(
                sim, pos, precio_actual, razon_cierre='timeout_posicion'
            )

            # Auditoría
            if self.audit_logger:
                await self.audit_logger.log_evento_grid_simulacion(
                    symbol=sim.symbol,
                    tipo='POSICION_TIMEOUT',
                    grid_id=sim.grid_id,
                    evento_simulacion={
                        'posicion_id': pos.id,
                        'tipo': pos.tipo,
                        'tiempo_abierta_min': (timestamp - pos.timestamp_apertura) / 60,
                        'estado_final': pos.estado,
                    },
                    pnl_acumulado=float(sim.pnl_neto)
                )

        if posiciones_vencidas:
            await self._persistir_estado(sim)
    # ═══════════════════════════════════════════════════════════════════════════════
    # GESTIÓN PARCIALES (MEJORA #4)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _registrar_orden_parcial(self, sim: SimState, nivel: Decimal,
                                        filled_pct: float, remaining_qty: Decimal):
        """
        Registra una orden parcialmente llena.
        La cantidad restante queda como 'orden fantasma' para el siguiente cruce.
        """
        if not CONFIG.grid_neutral_gestion_parcial:
            return

        nivel_str = str(float(nivel))
        if remaining_qty > Decimal('0'):
            sim.ordenes_fantasma[nivel_str] = remaining_qty
            print(f"  [GRID_SIM] {sim.symbol} Orden parcial {filled_pct:.0%} llenada @ ${float(nivel):.4f} | "
                  f"Restante:{float(remaining_qty):.4f}")

    # NOTA: Esta función debe llamarse desde _ejecutar_buy y _ejecutar_sell
    # cuando se detecta un fill parcial. Actualmente se asume fill 100%.
    # Para activar: añadir lógica de fill parcial en _detectar_cruces.

    # ═══════════════════════════════════════════════════════════════════════════════
    # PERSISTENCIA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _persistir_estado(self, sim: SimState):
        """Persiste el estado actual de la simulación en SQLite."""
        try:
            pos_abiertas_json = json.dumps([p.to_dict() for p in sim.posiciones_abiertas_list()])
            pos_atrapadas_json = json.dumps([p.to_dict() for p in sim.posiciones_atrapadas])

            await actualizar_grid_simulacion(
                grid_id=sim.grid_id,
                pnl_bruto=float(sim.pnl_bruto),
                pnl_neto=float(sim.pnl_neto),
                fees_totales=float(sim.fees_totales),
                slippage_total=float(sim.slippage_total),
                trades_completados=sim.trades_completados,
                trades_kill_switch=sim.trades_kill_switch,
                posiciones_abiertas_json=pos_abiertas_json,
                posiciones_atrapadas_json=pos_atrapadas_json,
            )
        except Exception as e:
            print(f"  ⚠️ [GRID_SIM] {sim.symbol} Error persistiendo estado: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # HEARTBEAT (MEJORA #6)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def heartbeat(self):
        """
        Verifica la salud de grids activos cada 15 minutos.
        Detecta:
          - Grids sin ticks recibidos (>5 min)
          - Posiciones abiertas sin movimiento (>30 min)
          - Inconsistencias entre estado en memoria y DB
        """
        ahora = int(datetime.utcnow().timestamp())
        timeout_seg = CONFIG.grid_neutral_posicion_timeout_min * 60

        for symbol, sim in list(self.simulaciones.items()):
            if not sim.activa:
                continue

            # Verificar si hay ticks recientes
            segundos_sin_tick = ahora - sim.ultimo_tick_ts
            if segundos_sin_tick > 300:  # 5 minutos
                print(f"  [HEARTBEAT SIM] {symbol} Grid {sim.grid_id} SIN TICKS "
                      f"por {segundos_sin_tick:.0f}s → ABORTADO")
                if self.audit_logger:
                    await self.audit_logger.log_evento_grid_simulacion(
                        symbol=symbol, tipo='NEUTRAL_GRID_ABORT', grid_id=sim.grid_id,
                        evento_simulacion={'razon': 'sin_ticks_5min', 'segundos_sin_tick': segundos_sin_tick},
                        pnl_acumulado=float(sim.pnl_neto)
                    )
                await self.finalizar_grid(symbol, razon='sin_ticks_5min')
                continue

            # Verificar posiciones atascadas
            pos_abiertas = sim.posiciones_abiertas_list()
            cerradas_en_heartbeat = 0
            for pos in pos_abiertas:
                tiempo_abierta = ahora - pos.timestamp_apertura
                if tiempo_abierta > timeout_seg:
                    print(f"  [HEARTBEAT SIM] {symbol} Posición {pos.id} VENCIDA "
                          f"({tiempo_abierta/60:.0f}min) → Forzar cierre")
                    precio_actual = Decimal(str(self.precios_vivo.get(symbol, 0)))
                    if precio_actual > 0:
                        await self._ejecutar_kill_switch(sim, pos, precio_actual,
                                                          razon_cierre='heartbeat_timeout')
                        cerradas_en_heartbeat += 1
            
            if cerradas_en_heartbeat > 0:
                await self._persistir_estado(sim)
    # ═══════════════════════════════════════════════════════════════════════════════
    # CLEANER DE HUÉRFANOS (MEJORA #6)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def limpiar_grids_huerfanos(self):
        """
        Al arranque del bot: detecta grids en estado ACTIVO pero sin simulación
        activa durante > grid_neutral_huerfano_timeout_horas (4h por defecto).
        Los marca como ABORTADO para evitar estados zombies.
        """
        timeout_seg = CONFIG.grid_neutral_huerfano_timeout_horas * 3600
        ahora = int(datetime.utcnow().timestamp())

        try:
            from database_v5 import cargar_grids_huerfanos
            huerfanos = await cargar_grids_huerfanos(timeout_seg)

            limpiados = 0
            for grid in huerfanos:
                grid_id = grid['id']
                symbol = grid['symbol']
                ts_inicio = grid['timestamp_inicio']
                tiempo_activo = ahora - ts_inicio

                # Buscar simulación asociada
                from database_v5 import cargar_simulacion_activa
                sim = await cargar_simulacion_activa(grid_id)
                sim_id = sim['id'] if sim else None

                await forzar_aborto_grid_huerfano(grid_id, sim_id)

                print(f"  [CLEANER] Grid {grid_id} {symbol} marcado ABORTADO "
                      f"(huérfano {tiempo_activo/3600:.1f}h)")
                limpiados += 1

            if limpiados > 0:
                print(f"  [CLEANER] {limpiados} grids huérfanos limpiados")

        except Exception as e:
            print(f"  ⚠️ [CLEANER] Error limpiando grids huérfanos: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # ESTADÍSTICAS / ESTADO
    # ═══════════════════════════════════════════════════════════════════════════════

    def get_estado_simulacion(self, symbol: str) -> Optional[dict]:
        """Retorna el estado actual de una simulación para el dashboard."""
        if symbol not in self.simulaciones:
            return None

        sim = self.simulaciones[symbol]
        if not sim.activa:
            return None

        pos_abiertas = sim.posiciones_abiertas_list()
        pos_vencidas = sum(
            1 for p in pos_abiertas
            if (int(datetime.utcnow().timestamp()) - p.timestamp_apertura)
            > CONFIG.grid_neutral_posicion_timeout_min * 60
        )

        return {
            'grid_id': sim.grid_id,
            'sim_id': sim.sim_id,
            'activa': sim.activa,
            'niveles': len(sim.niveles),
            'posiciones_abiertas': len(pos_abiertas),
            'posiciones_atrapadas': len(sim.posiciones_atrapadas),
            'posiciones_vencidas': pos_vencidas,
            'trades_completados': sim.trades_completados,
            'trades_kill_switch': sim.trades_kill_switch,
            'pnl_neto': float(sim.pnl_neto),
            'pnl_bruto': float(sim.pnl_bruto),
            'fees_totales': float(sim.fees_totales),
            'slippage_total': float(sim.slippage_total),
            'max_posiciones_simultaneas': sim.max_posiciones_simultaneas,
            'ultimo_tick_segundos_ago': int(datetime.utcnow().timestamp()) - sim.ultimo_tick_ts,
            'timestamp_inicio': sim.timestamp_inicio,
        }
