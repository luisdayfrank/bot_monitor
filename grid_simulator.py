
"""
grid_simulator.py — V7 Helper Síncrono de Lógica Pura para Grid Neutral

Convertido a clase helper (Opción C): sin async, sin colas, sin tareas, sin DB.
El executor consulta estos métodos directamente dentro de su propio loop de polling.

MÉTODOS PÚBLICOS DEL HELPER:
  • init_sim_state()  → Crea un SimState para que el executor lo guarde.
  • on_fill()         → Registra un fill real confirmado por Binance.
  • poll()            → Verifica timeouts y empareja posiciones cada ciclo.
  • close_sim_state() → Cierra todo y retorna resumen al abortar el grid.

MÉTODOS INTERNOS PRESERVADOS:
  • _emparejar_posiciones()      → FIFO LONG/SHORT.
  • _ejecutar_kill_switch()      → (legacy, mantenido intacto, sincronizado)
  • _calcular_pnl()              → Cálculo de PnL por posición.
  • get_estado_simulacion()      → (legacy, ajustado para recibir SimState)
"""

import json
from datetime import datetime
from typing import Dict, List, Optional
from decimal import Decimal
from config import CONFIG


class SimPosicion:
    """Representa una posición simulada abierta en el grid."""

    def __init__(self, tipo, nivel_precio, precio_ejecucion, qty, slippage_aplicado,
                 fee_pagada, notional, timestamp_apertura, filled_qty=None, original_qty=None,
                 sim_id: str = "sim"):
        # Usar timestamp + sim_id para ID único y trackeable por simulación
        self.id = f"{sim_id}_pos_{timestamp_apertura}_{int(nivel_precio * 10000)}"
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
    Helper de lógica pura para grids neutral.

    Flujo:
      1. Executor crea SimState vía init_sim_state() y lo guarda en su estado.
      2. Executor llama on_fill() cuando Binance confirma un fill real.
      3. Executor llama poll() cada ciclo de polling para emparejar y detectar timeouts.
      4. Executor llama close_sim_state() al abortar el grid.
    """

    def __init__(self):
        # Helper puro: sin estado propio, sin async, sin DB
        pass

    # ═══════════════════════════════════════════════════════════════════════════════
    # MÉTODOS PRESERVADOS INTACTOS
    # ═══════════════════════════════════════════════════════════════════════════════

    def _emparejar_posiciones(self, sim: SimState, timestamp: int):
        """
        FASE 2.1: Emparejamiento FIFO de posiciones LONG y SHORT.
        El LONG más antiguo se empareja primero con el SHORT más antiguo
        que esté en nivel superior, evitando ocultar pérdidas.
        """
        # Separar y ordenar por timestamp (FIFO)
        longs_abiertas = [p for p in sim.posiciones if p.estado == 'ABIERTA' and p.tipo == 'LONG']
        shorts_abiertas = [p for p in sim.posiciones if p.estado == 'ABIERTA' and p.tipo == 'SHORT']
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
                    pnl_bruto = diferencia_niveles * long_pos.filled_qty
                    fee_par = (long_pos.fee_pagada + short_pos.fee_pagada)
                    pnl_neto = pnl_bruto - fee_par

                    long_pos.estado = 'CERRADA'
                    long_pos.pnl_cierre = pnl_neto / 2
                    short_pos.estado = 'CERRADA'
                    short_pos.pnl_cierre = pnl_neto / 2

                    sim.pnl_bruto += Decimal(str(pnl_bruto))
                    sim.pnl_neto = sim.pnl_bruto - sim.fees_totales
                    sim.trades_completados += 1

                    print(f"  [GRID_SIM] {sim.symbol} PAR CERRADO FIFO | "
                          f"LONG ${long_pos.nivel_precio:.4f} → SELL ${short_pos.nivel_precio:.4f} | "
                          f"Diff: {diferencia_niveles:.4f} | PnL: {pnl_neto:+.4f}")
                    break  # Solo emparejar una vez por LONG

    def _ejecutar_kill_switch(self, sim: SimState, posicion: SimPosicion,
                               precio_actual: Decimal, razon_cierre: str = 'max_posiciones'):
        """
        FASE 2.4: El helper solo RECOMIENDA el kill switch.
        NO cierra la posición ni actualiza PnL. Eso lo hace el executor
        tras confirmar el fill real de Binance vía close_position_by_id().
        """
        print(f"  [KILL_SWITCH] {sim.symbol} {posicion.tipo} ${posicion.precio_ejecucion:.4f} "
              f"Razón:{razon_cierre}")

        # Solo marcar como PENDIENTE_CIERRE para que poll() no la vuelva a recomendar
        posicion.estado = 'PENDIENTE_CIERRE'
        return True
    def close_position_by_id(self, sim: SimState, pos_id: str, precio_cierre: float, fee_cierre: float = 0.0):
        """
        Cierra una posición específica por ID usando el precio real de ejecución.
        El executor llama esto después de confirmar un fill de MARKET order.
        """
        for pos in sim.posiciones:
            if pos.id == pos_id and pos.estado in ('ABIERTA', 'PENDIENTE_CIERRE'):
                pnl = self._calcular_pnl(pos, precio_cierre)
                pos.estado = 'CERRADA'
                pos.pnl_cierre = pnl
                sim.pnl_bruto += Decimal(str(pnl))
                sim.fees_totales += Decimal(str(fee_cierre))
                sim.pnl_neto = sim.pnl_bruto - sim.fees_totales
                sim.trades_kill_switch += 1
                print(f"  [GRID_SIM] {sim.symbol} Pos {pos_id} cerrada real @ ${precio_cierre:.4f} | PnL:{pnl:+.4f}")
                return True
        print(f"  [GRID_SIM] {sim.symbol} Pos {pos_id} no encontrada para cierre real")
        return False

    def _calcular_pnl(self, posicion: SimPosicion, precio_cierre: float) -> float:
        """Calcula el PnL de cerrar una posición."""
        if posicion.tipo == 'LONG':
            return (precio_cierre - posicion.precio_ejecucion) * posicion.filled_qty
        else:
            return (posicion.precio_ejecucion - precio_cierre) * posicion.filled_qty

    def get_estado_simulacion(self, sim_state: SimState) -> Optional[dict]:
        """Retorna el estado actual de una simulación para el dashboard."""
        if not sim_state or not sim_state.activa:
            return None

        sim = sim_state
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

    # ═══════════════════════════════════════════════════════════════════════════════
    # MÉTODOS HELPER PARA EL EXECUTOR (Opción C)
    # ═══════════════════════════════════════════════════════════════════════════════

    def init_sim_state(self, grid_id, sim_id, symbol, niveles, qty_por_orden,
                       fee_rate, slippage_base, precio_inicio, timestamp_inicio):
        """Crea un SimState para que el executor lo guarde en su GridExecutionState."""
        niveles_d = [Decimal(str(n)) for n in niveles]
        return SimState(
            grid_id=grid_id,
            sim_id=sim_id,
            symbol=symbol,
            niveles=niveles_d,
            qty_por_orden=Decimal(str(qty_por_orden)),
            fee_rate=Decimal(str(fee_rate)),
            slippage_base=Decimal(str(slippage_base)),
            precio_inicio=Decimal(str(precio_inicio)),
            timestamp_inicio=timestamp_inicio,
        )

    def on_fill(self, sim_state: SimState, side: str, price: float, qty: float, timestamp: int):
        """
        El executor llama esto cuando Binance confirma un fill real.
        Registra la posición en la lógica del simulador para emparejamiento.
        """
        if not sim_state.activa:
            return

        precio_d = Decimal(str(price))
        qty_d = Decimal(str(qty))
        notional = qty_d * precio_d
        fee = notional * sim_state.fee_rate

        pos = SimPosicion(
            tipo='LONG' if side == 'BUY' else 'SHORT',
            nivel_precio=float(price),
            precio_ejecucion=float(price),
            qty=float(qty),
            slippage_aplicado=0.0,  # En real, el fill ya tiene slippage implícito
            fee_pagada=float(fee),
            notional=float(notional),
            timestamp_apertura=timestamp,
            filled_qty=float(qty),
            original_qty=float(qty),
            sim_id=sim_state.sim_id  # <-- NUEVO: ID único por simulación
        )
        sim_state.posiciones.append(pos)
        sim_state.fees_totales += fee

        n_abiertas = sim_state.contar_posiciones_abiertas()
        sim_state.max_posiciones_simultaneas = max(sim_state.max_posiciones_simultaneas, n_abiertas)

        # Actualizar precio de referencia para emparejamiento
        sim_state.precio_referencia = precio_d

        # Auto-emparejar inmediatamente si hay par posible
        self._emparejar_posiciones(sim_state, timestamp)

        print(f"  [SIM-HELPER] {sim_state.symbol} {side} registrado @ ${price:.4f} | "
              f"Posiciones: {n_abiertas}")

    def poll(self, sim_state: SimState, precio_actual: float, timestamp: int):
        """
        El executor llama esto cada ciclo de polling (10s).
        Verifica timeouts, empareja posiciones, y retorna acciones a ejecutar.
        """
        if not sim_state.activa:
            return []

        acciones = []

        # 1. Emparejar posiciones FIFO (si hay LONG + SHORT abiertas)
        self._emparejar_posiciones(sim_state, timestamp)

        # 2. Verificar timeouts de posiciones abiertas
        posiciones_vencidas = []
        timeout_seg = CONFIG.grid_neutral_posicion_timeout_min * 60

        for pos in sim_state.posiciones:
            if pos.estado == 'ABIERTA':
                tiempo_abierta = timestamp - pos.timestamp_apertura
                if tiempo_abierta > timeout_seg:
                    posiciones_vencidas.append(pos)

        for pos in posiciones_vencidas:
            # En vez de kill switch interno, generamos una acción para el executor
            acciones.append({
                'tipo': 'KILL_SWITCH',
                'pos_id': pos.id,
                'pos_tipo': pos.tipo,
                'precio_entrada': pos.precio_ejecucion,
                'qty': pos.filled_qty,
                'razon': 'timeout_posicion'
            })
            pos.estado = 'PENDIENTE_CIERRE'  # Evitar duplicados

        # 3. Calcular PnL acumulado en RAM
        sim_state.pnl_neto = sim_state.pnl_bruto - sim_state.fees_totales

        return acciones

    def close_sim_state(self, sim_state: SimState, precio_final: float):
        """El executor llama esto al abortar el grid. Cierra todo y retorna resumen."""
        if not sim_state.activa:
            return {}

        sim_state.activa = False
        precio_d = Decimal(str(precio_final))

        # Liquidar posiciones pendientes o abiertas
        for pos in sim_state.posiciones:
            if pos.estado in ('ABIERTA', 'PENDIENTE_CIERRE'):
                pnl = self._calcular_pnl(pos, precio_final)
                pos.estado = 'CERRADA_FORZADA'
                pos.pnl_cierre = pnl
                sim_state.pnl_bruto += Decimal(str(pnl))

        sim_state.pnl_neto = sim_state.pnl_bruto - sim_state.fees_totales

        return {
            'pnl_bruto': float(sim_state.pnl_bruto),
            'pnl_neto': float(sim_state.pnl_neto),
            'fees_totales': float(sim_state.fees_totales),
            'trades_completados': sim_state.trades_completados,
            'trades_kill_switch': sim_state.trades_kill_switch,
            'posiciones_atrapadas': len(sim_state.posiciones_atrapadas),
        }
