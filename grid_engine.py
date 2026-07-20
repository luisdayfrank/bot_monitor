"""
grid_engine.py — Motor de niveles para Grid Trading V8
Lógica 100% pura, sin I/O, sin dependencias de Binance, sin DB.
Responsabilidad: decidir QUÉ hacer. El executor decide CÓMO hacerlo.

Principios:
1. Puro: sin I/O, sin estado mutable, sin dependencias externas
2. Determinístico: mismos inputs → mismos outputs
3. Verificable: 100% testeable unitariamente
4. Inmutable: cada decisión retorna nuevo estado, no modifica el anterior
"""

from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from enum import Enum, IntEnum, auto
from typing import Dict, List, Optional, Tuple, Set
import time
import json


# ═══════════════════════════════════════════════════════════════════
# ENUMS Y ESTADOS
# ═══════════════════════════════════════════════════════════════════

class LevelState(Enum):
    """Estado de un nivel en el grid."""
    PENDING = "PENDING"           # Nivel creado, orden no enviada aún
    ACTIVE = "ACTIVE"             # Orden enviada, pendiente en Binance
    FILLED = "FILLED"             # Orden ejecutada completamente
    PARTIAL = "PARTIAL"           # Orden parcialmente ejecutada (Fase 4+)
    PENDING_REPLACE = "PENDING_REPLACE"  # Orden perdida, esperando rearmado
    GAP = "GAP"                   # Nivel gap — sin orden, sin posición
    CANCELLED = "CANCELLED"       # Orden cancelada manualmente
    EXPIRED = "EXPIRED"           # Orden expirada (GTC nunca debería)


class CoverageAction(Enum):
    """Acciones que el coverage puede recomendar."""
    NONE = auto()
    REPLENISH = auto()            # Reponer orden perdida
    KILL_DUPLICATE = auto()       # Cancelar orden duplicada
    KILL_ORPHAN = auto()          # Cancelar orden huérfana
    RECALCULATE_GAP = auto()      # Mover gap (Fase 5)
    ADJUST_QTY = auto()           # Ajustar cantidad (Fase 4+)


class CoverageSeverity(IntEnum):
    """Severidad de la discrepancia detectada.
    V8 FIX: IntEnum para soportar max() en analyze_coverage (CLEAN<WARNING<CRITICAL<EMERGENCY)."""
    CLEAN = auto()                # Todo en orden
    WARNING = auto()              # Discrepancia menor, no afecta trading
    CRITICAL = auto()             # Discrepancia que afecta trading
    EMERGENCY = auto()            # Grid potencialmente roto


# ═══════════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class GridLevel:
    """
    Representa un nivel del grid.

    Inmutable por diseño. Cualquier cambio crea una nueva instancia.
    Esto elimina problemas de concurrencia y estado inconsistente.
    """
    level_index: int
    price: Decimal
    side: str                    # 'BUY' | 'SELL' — side de la ORDEN
    position_side: str           # 'LONG' | 'SHORT' — para HEDGE MODE
    quantity: Decimal
    state: LevelState = LevelState.PENDING
    order_id: Optional[str] = None
    binance_order_id: Optional[str] = None
    is_gap: bool = False
    last_placed_at_ms: Optional[int] = None
    filled_qty: Decimal = Decimal('0')
    version: int = 1

    def with_state(self, new_state: LevelState) -> 'GridLevel':
        """Retorna nuevo nivel con estado actualizado."""
        return GridLevel(
            level_index=self.level_index,
            price=self.price,
            side=self.side,
            position_side=self.position_side,
            quantity=self.quantity,
            state=new_state,
            order_id=self.order_id,
            binance_order_id=self.binance_order_id,
            is_gap=self.is_gap,
            last_placed_at_ms=self.last_placed_at_ms,
            filled_qty=self.filled_qty,
            version=self.version + 1
        )

    def with_order(self, order_id: str, binance_id: str, timestamp_ms: int) -> 'GridLevel':
        """Retorna nuevo nivel con orden asignada."""
        return GridLevel(
            level_index=self.level_index,
            price=self.price,
            side=self.side,
            position_side=self.position_side,
            quantity=self.quantity,
            state=LevelState.ACTIVE,
            order_id=order_id,
            binance_order_id=binance_id,
            is_gap=self.is_gap,
            last_placed_at_ms=timestamp_ms,
            filled_qty=self.filled_qty,
            version=self.version + 1
        )

    def with_fill(self, filled_qty: Decimal) -> 'GridLevel':
        """Retorna nuevo nivel con fill registrado."""
        new_filled = self.filled_qty + filled_qty
        new_state = LevelState.FILLED if new_filled >= self.quantity else LevelState.PARTIAL

        return GridLevel(
            level_index=self.level_index,
            price=self.price,
            side=self.side,
            position_side=self.position_side,
            quantity=self.quantity,
            state=new_state,
            order_id=self.order_id,
            binance_order_id=self.binance_order_id,
            is_gap=self.is_gap,
            last_placed_at_ms=self.last_placed_at_ms,
            filled_qty=new_filled,
            version=self.version + 1
        )

    def to_row(self, symbol: str, grid_id: int) -> dict:
        """Serializa para persistencia en DB."""
        return {
            'grid_ejecucion_id': grid_id,
            'symbol': symbol,
            'level_index': self.level_index,
            'price': float(self.price),
            'side': self.side,
            'position_side': self.position_side,
            'quantity': float(self.quantity),
            'state': self.state.value,
            'is_gap': 1 if self.is_gap else 0,
            'order_id': self.order_id,
            'binance_order_id': self.binance_order_id,
            'filled_qty': float(self.filled_qty),
            'last_placed_at_ms': self.last_placed_at_ms,
            'version': self.version,
        }

    @classmethod
    def from_row(cls, row: dict) -> 'GridLevel':
        """Deserializa desde DB."""
        return cls(
            level_index=row['level_index'],
            price=Decimal(str(row['price'])),
            side=row['side'],
            position_side=row.get('position_side', 'LONG' if row['side'] == 'BUY' else 'SHORT'),
            quantity=Decimal(str(row['quantity'])),
            state=LevelState(row['state']),
            order_id=row.get('order_id'),
            binance_order_id=row.get('binance_order_id'),
            is_gap=bool(row.get('is_gap', 0)),
            last_placed_at_ms=row.get('last_placed_at_ms'),
            filled_qty=Decimal(str(row.get('filled_qty', 0))),
            version=row.get('version', 1),
        )


@dataclass
class CoverageReport:
    """Reporte de discrepancias entre niveles esperados y órdenes reales."""
    symbol: str
    timestamp_ms: int
    severity: CoverageSeverity = CoverageSeverity.CLEAN
    matched: List[Tuple[GridLevel, dict]] = field(default_factory=list)
    uncovered_levels: List[GridLevel] = field(default_factory=list)
    orphan_orders: List[dict] = field(default_factory=list)
    duplicates: List[List[dict]] = field(default_factory=list)
    actions: List[Tuple[CoverageAction, dict]] = field(default_factory=list)
    diagnostics: List[str] = field(default_factory=list)

    def is_clean(self) -> bool:
        return self.severity == CoverageSeverity.CLEAN

    def add_diagnostic(self, msg: str):
        self.diagnostics.append(f"[{time.strftime('%H:%M:%S')}] {msg}")


@dataclass
class GridDecision:
    """Decisión del motor para una acción específica."""
    action: CoverageAction
    level: Optional[GridLevel]
    order: Optional[dict]
    reason: str
    priority: int  # 1=alta, 3=baja


# ═══════════════════════════════════════════════════════════════════
# LEVEL MAP — Estructura principal
# ═══════════════════════════════════════════════════════════════════

class LevelMap:
    """
    Mapa inmutable de niveles. Cualquier modificación retorna un nuevo LevelMap.

    Esto permite:
    - Rollback instantáneo (guardar referencia al mapa anterior)
    - Sin race conditions (el mapa nunca cambia, solo se reemplaza)
    - Historial de estados para debugging
    """

    def __init__(self, levels: Dict[int, GridLevel], gap_index: Optional[int] = None):
        self.levels = levels  # Dict[level_index, GridLevel]
        self.gap_index = gap_index
        self._by_price: Dict[Decimal, int] = {l.price: i for i, l in levels.items()}
        self._by_order_id: Dict[str, int] = {
            l.order_id: i for i, l in levels.items() if l.order_id
        }

    @classmethod
    def from_prices(
        cls,
        prices: List[float],
        qty: float,
        current_price: float,
        tick_size: Decimal,
        hedge_mode: bool = False
    ) -> 'LevelMap':
        """
        Construye LevelMap desde lista de precios.

        El gap se determina como el nivel más cercano al precio actual.
        Los índices se preservan del orden original (no se reordenan).
        """
        if not prices:
            raise ValueError("Lista de precios vacía")

        # Encontrar índice del nivel más cercano al precio actual
        gap_idx = min(range(len(prices)), key=lambda i: abs(prices[i] - current_price))

        levels = {}
        for i, price in enumerate(prices):
            is_gap = (i == gap_idx)

            # Side derivado del precio relativo al actual
            if price < current_price:
                side = 'BUY'
                position_side = 'LONG'
            elif price > current_price:
                side = 'SELL'
                position_side = 'SHORT'
            else:
                # Exactamente en el precio — ambiguo, default a BUY
                side = 'BUY'
                position_side = 'LONG'

            # Si es gap, el side es placeholder (se recalcula al rearmar)
            if is_gap:
                side = 'BUY'
                position_side = 'LONG'

            levels[i] = GridLevel(
                level_index=i,
                price=Decimal(str(price)),
                side=side,
                position_side=position_side,
                quantity=Decimal(str(qty)),
                state=LevelState.GAP if is_gap else LevelState.PENDING,
                is_gap=is_gap
            )

        return cls(levels, gap_index=gap_idx)

    def with_level(self, level: GridLevel) -> 'LevelMap':
        """Retorna nuevo LevelMap con un nivel actualizado."""
        new_levels = dict(self.levels)
        new_levels[level.level_index] = level
        return LevelMap(new_levels, self.gap_index)

    def with_gap_moved(self, new_gap_index: int, current_price: float) -> 'LevelMap':
        """
        Mueve el gap a un nuevo índice.
        Retorna nuevo LevelMap con gap actualizado y sides recalculados.
        """
        if new_gap_index == self.gap_index:
            return self

        new_levels = {}
        for i, level in self.levels.items():
            if i == self.gap_index:
                # Viejo gap: reactivar con side correcto
                new_level = GridLevel(
                    level_index=level.level_index,
                    price=level.price,
                    side='BUY' if level.price < Decimal(str(current_price)) else 'SELL',
                    position_side='LONG' if level.price < Decimal(str(current_price)) else 'SHORT',
                    quantity=level.quantity,
                    state=LevelState.PENDING,
                    order_id=None,
                    binance_order_id=None,
                    is_gap=False,
                    last_placed_at_ms=None,
                    filled_qty=level.filled_qty,
                    version=level.version
                )
                new_levels[i] = new_level
            elif i == new_gap_index:
                # Nuevo gap: desactivar
                new_levels[i] = GridLevel(
                    level_index=level.level_index,
                    price=level.price,
                    side='BUY',  # placeholder
                    position_side='LONG',
                    quantity=level.quantity,
                    state=LevelState.GAP,
                    order_id=None,
                    binance_order_id=None,
                    is_gap=True,
                    last_placed_at_ms=None,
                    filled_qty=level.filled_qty,
                    version=level.version
                )
            else:
                new_levels[i] = level

        return LevelMap(new_levels, gap_index=new_gap_index)

    def active_levels(self) -> List[GridLevel]:
        """Niveles que deberían tener orden abierta."""
        return [l for l in self.levels.values() 
                if l.state in (LevelState.ACTIVE, LevelState.PENDING, LevelState.PENDING_REPLACE)]

    def openable_levels(self) -> List[GridLevel]:
        """Niveles que pueden tener orden (excluye gap y filled)."""
        return [l for l in self.levels.values() 
                if not l.is_gap and l.state != LevelState.FILLED]

    def find_by_price(self, price: Decimal, side: str, tolerance: Decimal) -> Optional[GridLevel]:
        """Encuentra nivel por precio y side dentro de tolerancia."""
        mejor = None
        mejor_dist = None

        for level in self.levels.values():
            if level.is_gap or level.side != side:
                continue
            dist = abs(level.price - price)
            if dist <= tolerance:
                if mejor_dist is None or dist < mejor_dist:
                    mejor = level
                    mejor_dist = dist

        return mejor

    def find_by_order_id(self, order_id: str) -> Optional[GridLevel]:
        """Encuentra nivel por order_id."""
        idx = self._by_order_id.get(order_id)
        return self.levels.get(idx) if idx is not None else None

    def side_for_price(self, price: Decimal, current_price: float) -> Tuple[str, str]:
        """Determina (side, position_side) para un precio dado el precio actual."""
        if price < Decimal(str(current_price)):
            return 'BUY', 'LONG'
        else:
            return 'SELL', 'SHORT'

    def to_rows(self, symbol: str, grid_id: int) -> List[dict]:
        return [l.to_row(symbol, grid_id) for l in self.levels.values()]

    @classmethod
    def from_rows(cls, rows: List[dict]) -> 'LevelMap':
        levels = {r['level_index']: GridLevel.from_row(r) for r in rows}
        gap_idx = None
        for idx, level in levels.items():
            if level.is_gap:
                gap_idx = idx
                break
        return cls(levels, gap_index=gap_idx)


# ═══════════════════════════════════════════════════════════════════
# GRID ENGINE — Motor de decisiones
# ═══════════════════════════════════════════════════════════════════

class GridEngine:
    """
    Motor de decisiones para grid trading.

    Principios:
    1. Puro: sin I/O, sin estado mutable, sin dependencias externas
    2. Determinístico: mismos inputs → mismos outputs
    3. Verificable: 100% testeable unitariamente
    4. Inmutable: cada decisión retorna nuevo estado, no modifica el anterior
    """

    def __init__(
        self,
        symbol: str,
        tick_size: Decimal,
        step: Decimal,
        hedge_mode: bool = False,
        lag_guard_ms: int = 10000,
        max_ops_per_tick: int = 3
    ):
        self.symbol = symbol
        self.tick_size = tick_size
        self.step = step
        self.hedge_mode = hedge_mode
        self.lag_guard_ms = lag_guard_ms
        self.max_ops_per_tick = max_ops_per_tick
        self.level_map: Optional[LevelMap] = None
        self._history: List[Tuple[int, LevelMap, CoverageReport]] = []  # Para debugging

    def tolerance(self) -> Decimal:
        """
        Tolerancia para match precio↔nivel.

        Garantía: tolerance < step/2 (evita match cruzado entre niveles adyacentes)
        """
        raw = max(self.tick_size * 2, self.step / 4)
        max_allowed = (self.step / 2) - self.tick_size

        if raw >= max_allowed:
            # Grid inválido — pero el engine no rechaza, solo reporta
            # El executor debe validar antes de crear
            return max_allowed
        return raw

    def initialize(self, prices: List[float], qty: float, current_price: float) -> LevelMap:
        """Inicializa el mapa de niveles. Retorna el mapa inicial."""
        self.level_map = LevelMap.from_prices(
            prices, qty, current_price, self.tick_size, self.hedge_mode
        )
        return self.level_map

    def analyze_coverage(
        self,
        open_orders: List[dict],
        current_price: float,
        recent_fills: List[dict],
        now_ms: Optional[int] = None
    ) -> CoverageReport:
        """
        Analiza discrepancias entre niveles esperados y órdenes reales.

        MODO SHADOW: clasifica, no actúa.
        MODO ACTIVO: genera decisiones ejecutables.
        """
        report = CoverageReport(
            symbol=self.symbol,
            timestamp_ms=now_ms or int(time.time() * 1000)
        )

        if not self.level_map:
            report.severity = CoverageSeverity.EMERGENCY
            report.add_diagnostic("LevelMap no inicializado")
            return report

        tolerance = self.tolerance()
        prefix = f"CM"  # Prefijo de clientOrderId

        # ─── Indexar órdenes ───
        orders_by_key: Dict[Tuple[str, Decimal], List[dict]] = {}
        for order in open_orders:
            side = order.get('side')
            price = Decimal(str(order.get('price', 0)))
            key = (side, price)
            if key not in orders_by_key:
                orders_by_key[key] = []
            orders_by_key[key].append(order)

        # Detectar duplicados (mismo side, mismo precio, >1 orden)
        for key, orders in orders_by_key.items():
            if len(orders) > 1:
                report.duplicates.append(orders)
                report.severity = max(report.severity, CoverageSeverity.CRITICAL)
                report.add_diagnostic(f"Duplicate: {len(orders)} órdenes {key[0]} @ {key[1]}")

        # ─── Indexar fills por binance_order_id ───
        fills_by_order: Dict[str, List[dict]] = {}
        for fill in recent_fills:
            oid = str(fill.get('binance_order_id', ''))
            if oid:
                if oid not in fills_by_order:
                    fills_by_order[oid] = []
                fills_by_order[oid].append(fill)

        # ─── Match niveles ↔ órdenes ───
        matched_levels: Set[int] = set()
        matched_orders: Set[int] = set()

        for level in self.level_map.levels.values():
            if level.is_gap:
                continue

            # Buscar orden para este nivel (mismo side, precio dentro de tolerancia)
            matched_order = None
            for order in open_orders:
                if not order.get('clientOrderId', '').startswith(prefix):
                    continue

                order_price = Decimal(str(order.get('price', 0)))
                order_side = order.get('side')

                if (order_side == level.side and 
                    abs(order_price - level.price) <= tolerance):
                    matched_order = order
                    break

            if matched_order:
                report.matched.append((level, matched_order))
                matched_levels.add(level.level_index)
                matched_orders.add(id(matched_order))
            else:
                # Clasificar por qué no tiene orden
                self._classify_uncovered(level, report, recent_fills, fills_by_order, now_ms)

        # ─── Órdenes huérfanas ───
        for order in open_orders:
            if not order.get('clientOrderId', '').startswith(prefix):
                continue
            if id(order) not in matched_orders:
                report.orphan_orders.append(order)
                report.severity = max(report.severity, CoverageSeverity.WARNING)
                report.add_diagnostic(f"Orphan: {order.get('clientOrderId')}")

        # ─── Determinar severidad final ───
        if report.uncovered_levels:
            report.severity = max(report.severity, CoverageSeverity.CRITICAL)
        if report.orphan_orders:
            report.severity = max(report.severity, CoverageSeverity.WARNING)

        return report

    def _classify_uncovered(
        self,
        level: GridLevel,
        report: CoverageReport,
        recent_fills: List[dict],
        fills_by_order: Dict[str, List[dict]],
        now_ms: Optional[int]
    ):
        """Clasifica un nivel sin orden."""
        now = now_ms or int(time.time() * 1000)

        # 1. LAG: Orden recién colocada, Binance aún no la refleja
        if level.last_placed_at_ms and (now - level.last_placed_at_ms) < self.lag_guard_ms:
            report.add_diagnostic(f"LAG: nivel {level.level_index} recién colocado")
            return  # No es uncovered, solo lag

        # 2. FILL_RECIENTE: Nivel tuvo un fill en los últimos 90s
        fill_recent = False

        # Buscar por binance_order_id del nivel
        if level.binance_order_id and level.binance_order_id in fills_by_order:
            fills = fills_by_order[level.binance_order_id]
            for fill in fills:
                fill_ts = fill.get('timestamp_ms', 0)
                if (now - fill_ts) < 90000:  # 90s
                    fill_recent = True
                    break

        # Fallback: buscar por precio de orden (no precio de fill)
        if not fill_recent:
            for fill in recent_fills:
                fill_ts = fill.get('timestamp_ms', 0)
                if (now - fill_ts) < 90000:
                    order_price = fill.get('order_price') or fill.get('price')
                    if order_price:
                        op = Decimal(str(order_price))
                        if abs(op - level.price) <= self.tolerance():
                            fill_recent = True
                            break

        if fill_recent:
            report.add_diagnostic(f"FILL_RECIENTE: nivel {level.level_index} tuvo fill <90s")
            return  # No es uncovered, fue recién llenado

        # 3. REPONER: Nivel verdaderamente sin orden
        report.uncovered_levels.append(level)
        report.add_diagnostic(f"REPONER: nivel {level.level_index} {level.side} @ {level.price}")

    def generate_decisions(self, report: CoverageReport, current_price: float,
                          position_net: Decimal, hedge_mode: bool) -> List[GridDecision]:
        """
        Genera decisiones ejecutables a partir de un reporte de coverage.

        Filtros aplicados:
        - No reponer si hay posición contraria que sería aumentada
        - No reponer más de max_ops_per_tick por ciclo
        - Prioridad: KILL_DUPLICATE > KILL_ORPHAN > REPLENISH
        """
        decisions = []

        # 1. Eliminar duplicados (prioridad 1 — más crítico)
        for dup in report.duplicates:
            # Ordenar por orderId (menor = más antigua)
            dup_sorted = sorted(dup, key=lambda o: int(o.get('orderId', 0)))
            to_kill = dup_sorted[1:]  # Mantener la más antigua

            for order in to_kill:
                decisions.append(GridDecision(
                    action=CoverageAction.KILL_DUPLICATE,
                    level=None,
                    order=order,
                    reason=f"Duplicado de {dup_sorted[0].get('clientOrderId')}",
                    priority=1
                ))

        # 2. Eliminar órdenes huérfanas (prioridad 2)
        for order in report.orphan_orders:
            decisions.append(GridDecision(
                action=CoverageAction.KILL_ORPHAN,
                level=None,
                order=order,
                reason="Orden sin nivel asociado",
                priority=2
            ))

        # 3. Reponer niveles (prioridad 3, con verificaciones)
        for level in report.uncovered_levels:
            # Verificar side correcto dado el precio actual
            side, pos_side = self.level_map.side_for_price(level.price, current_price)

            # Verificar que no hay posición contraria que sería aumentada
            if self._would_increase_wrong_position(side, position_net, hedge_mode):
                report.add_diagnostic(
                    f"SKIP_REPLENISH: nivel {level.level_index} aumentaría posición contraria"
                )
                continue

            decisions.append(GridDecision(
                action=CoverageAction.REPLENISH,
                level=level,
                order=None,
                reason=f"Nivel {level.level_index} sin orden {side} @ {level.price}",
                priority=3
            ))

        # Limitar a max_ops_per_tick
        decisions.sort(key=lambda d: d.priority)
        return decisions[:self.max_ops_per_tick]

    def _would_increase_wrong_position(self, side: str, position_net: Decimal, 
                                       hedge_mode: bool) -> bool:
        """
        Verifica si una orden del lado dado aumentaría la posición en la dirección equivocada.

        ONE-WAY:
        - SELL con position_net < 0 (más SHORT) → aumenta SHORT → CONFLICTO
        - BUY con position_net > 0 (más LONG) → aumenta LONG → CONFLICTO

        HEDGE:
        - SELL con position_net SHORT > 0 → aumenta SHORT → CONFLICTO
        - BUY con position_net LONG > 0 → aumenta LONG → CONFLICTO
        """
        if hedge_mode:
            # En HEDGE necesitamos información de piernas individuales
            # El executor debe proporcionar esto; aquí usamos heurística
            # Esto se mejora en Fase 4 con tracking de piernas
            return False  # En HEDGE, asumimos que el executor maneja piernas
        else:
            if side == 'SELL' and position_net < Decimal('-0.0001'):
                return True
            if side == 'BUY' and position_net > Decimal('0.0001'):
                return True
            return False

    def update_from_fill(self, level_index: int, fill_qty: Decimal) -> LevelMap:
        """Actualiza el mapa después de un fill. Retorna nuevo mapa."""
        if not self.level_map or level_index not in self.level_map.levels:
            return self.level_map

        level = self.level_map.levels[level_index]
        new_level = level.with_fill(fill_qty)
        self.level_map = self.level_map.with_level(new_level)
        return self.level_map

    def update_from_order_placed(self, level_index: int, order_id: str,
                                  binance_id: str, timestamp_ms: int) -> LevelMap:
        """Actualiza el mapa después de colocar una orden. Retorna nuevo mapa."""
        if not self.level_map or level_index not in self.level_map.levels:
            return self.level_map

        level = self.level_map.levels[level_index]
        new_level = level.with_order(order_id, binance_id, timestamp_ms)
        self.level_map = self.level_map.with_level(new_level)
        return self.level_map

    def get_history(self) -> List[Tuple[int, LevelMap, CoverageReport]]:
        """Retorna historial de estados para debugging."""
        return self._history.copy()

    def snapshot(self, report: CoverageReport):
        """Guarda snapshot del estado actual para debugging."""
        if self.level_map:
            self._history.append((
                int(time.time() * 1000),
                self.level_map,  # Referencia al mapa inmutable
                report
            ))
            # Limitar historial a 1000 entradas
            if len(self._history) > 1000:
                self._history = self._history[-1000:]


# ═══════════════════════════════════════════════════════════════════
# GAP MANAGER — Gestión del gap dinámico (Fase 5)
# ═══════════════════════════════════════════════════════════════════

class GapManager:
    """Gestiona el movimiento del gap entre niveles."""

    @staticmethod
    def should_move_gap(
        level_map: LevelMap,
        current_price: float,
        hysteresis_ticks: int = 2
    ) -> Optional[int]:
        """
        Determina si el gap debe moverse.

        Histeresis: solo mover si el nuevo gap está al menos N ticks
        consecutivos más cerca del precio actual.

        Retorna: nuevo índice de gap, o None si no debe moverse.
        """
        if not level_map.levels:
            return None

        new_gap = min(
            level_map.levels.keys(),
            key=lambda i: abs(level_map.levels[i].price - Decimal(str(current_price)))
        )

        if new_gap == level_map.gap_index:
            return None

        # Histeresis: verificar que el nuevo gap está significativamente más cerca
        old_dist = abs(level_map.levels[level_map.gap_index].price - Decimal(str(current_price)))
        new_dist = abs(level_map.levels[new_gap].price - Decimal(str(current_price)))

        # Solo mover si la distancia se redujo al menos en 1 nivel (step)
        if old_dist - new_dist < level_map.levels[0].price * Decimal('0.001'):
            return None

        return new_gap

    @staticmethod
    def move_gap(level_map: LevelMap, new_gap_index: int, current_price: float) -> LevelMap:
        """Mueve el gap a un nuevo índice. Retorna nuevo LevelMap."""
        return level_map.with_gap_moved(new_gap_index, current_price)


# ═══════════════════════════════════════════════════════════════════
# CLIENT ORDER ID GENERATOR — Sin colisiones garantizado
# ═══════════════════════════════════════════════════════════════════

class ClientOrderIdGenerator:
    """
    Generador de clientOrderId sin colisiones.

    Esquema: CM{grid_id}_{counter:06d}
    - grid_id: identificador del grid
    - counter: contador atómico por grid (persistido en DB)

    Longitud máxima: CM12345_000001 = 15 chars (bien dentro de 36)
    """

    def __init__(self, grid_id: int):
        self.grid_id = grid_id
        self._counter = 0
        self._lock = None  # Se inicializa en el executor con asyncio.Lock

    def initialize(self, last_counter: int = 0):
        """Inicializa el contador desde DB (recuperación post-crash)."""
        self._counter = last_counter

    def generate(self, level_index: int, side: str) -> str:
        """Genera un clientOrderId único."""
        self._counter += 1
        # Formato: CM{grid_id}_{counter:06d}
        # El level_index y side se codifican en metadata separada (DB)
        return f"CM{self.grid_id}_{self._counter:06d}"

    @property
    def counter(self) -> int:
        return self._counter

    @staticmethod
    def parse(cid: str) -> Optional[Tuple[int, int]]:
        """Parsea un clientOrderId. Retorna (grid_id, counter) o None."""
        if not cid.startswith('CM'):
            return None
        try:
            parts = cid[2:].split('_')
            if len(parts) != 2:
                return None
            return int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            return None

    @staticmethod
    def belongs_to_grid(cid: str, grid_id: int) -> bool:
        """Verifica si un clientOrderId pertenece a un grid específico."""
        parsed = ClientOrderIdGenerator.parse(cid)
        return parsed is not None and parsed[0] == grid_id
