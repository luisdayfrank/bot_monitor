"""
tests/test_f12_semana3_cierre_tp.py — F12.8 / F12.7 (Plan v7.4, Semana 3)

1) B22: cierre con precio_cierre=0 -> sin PnL, precio_desconocido=True,
   trayectoria descartada (no se persiste un exit label con pnl -100%).
2) B6: cierre con precio válido -> _cancelar_tp_si_existe se invoca ANTES
   de marcar la posición como CERRADA; PnL normal.
"""
import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import executor as ex  # noqa: E402


class _LE:
    def __init__(self):
        self.salidas = []
        self.descartes = []

    def registrar_salida(self, **kw):
        self.salidas.append(kw)

    def descartar_trayectoria(self, posicion_id):
        self.descartes.append(posicion_id)


class _Exec(ex.GridExecutor):
    """Stub: solo lo que usa _cerrar_posicion_por_id."""

    def __init__(self):
        self.learning_engine = _LE()
        self.canceladas = []   # (pos_id, estado_en_el_momento, tp_id)

    async def _cancelar_tp_si_existe(self, state, pos):
        self.canceladas.append((pos.id, pos.estado, pos.orden_cierre_id))
        if state.grid_state and pos.id in state.grid_state.ordenes_tp_pendientes:
            del state.grid_state.ordenes_tp_pendientes[pos.id]


def _montar(tipo='LONG', qty=2.0, tp_id=None):
    pos = ex.PosicionReal(
        tipo=tipo, nivel_precio=100.0, precio_ejecucion=100.0, qty=qty,
        fee_pagada=0.0, timestamp_apertura=0)
    gs = SimpleNamespace(
        posiciones=[pos], ordenes_tp_pendientes={},
        pnl_bruto=Decimal('0'), fees_totales=Decimal('0'),
        pnl_neto=Decimal('0'), trades_kill_switch=0)
    if tp_id:
        pos.orden_cierre_id = tp_id
        gs.ordenes_tp_pendientes[pos.id] = tp_id
    state = SimpleNamespace(grid_state=gs, posicion_neta=Decimal(str(qty)),
                            symbol='TESTUSDT')
    return _Exec(), state, pos, gs


async def _test_b22_precio_desconocido():
    stub, state, pos, gs = _montar(tp_id='TP-777')
    ok = await stub._cerrar_posicion_por_id(state, pos.id, 0.0, razon='KILL_SWITCH')

    assert ok
    assert pos.estado == 'CERRADA'
    assert pos.precio_desconocido is True
    assert pos.pnl_cierre == Decimal('0')
    assert gs.pnl_bruto == Decimal('0'), f'pnl_bruto contaminado: {gs.pnl_bruto}'
    assert gs.pnl_neto == Decimal('0')
    assert state.posicion_neta == Decimal('0')
    assert stub.learning_engine.salidas == [], 'no debe persistir exit label con precio 0'
    assert stub.learning_engine.descartes == [pos.id]
    print('  [OK] F12.8/B22: precio 0 -> sin PnL, flag precio_desconocido, trayectoria descartada')


async def _test_b6_tp_cancel_before_close():
    stub, state, pos, gs = _montar(tp_id='TP-888')
    ok = await stub._cerrar_posicion_por_id(state, pos.id, 110.0, razon='TREND_END')

    assert ok
    assert stub.canceladas == [(pos.id, 'ABIERTA', 'TP-888')], \
        f'TP debe cancelarse ANTES de cerrar: {stub.canceladas}'
    assert pos.estado == 'CERRADA'
    assert pos.precio_desconocido is False
    assert pos.pnl_cierre == Decimal('20')      # (110-100)*2
    assert gs.pnl_bruto == Decimal('20')
    assert state.posicion_neta == Decimal('0')
    assert len(stub.learning_engine.salidas) == 1
    assert stub.learning_engine.salidas[0]['precio_salida'] == 110.0
    assert stub.learning_engine.descartes == []
    print('  [OK] F12.7/B6: TP cancelado antes del cierre; PnL normal')


async def main():
    await _test_b22_precio_desconocido()
    await _test_b6_tp_cancel_before_close()
    print('F12 semana 3 cierre/TP: PASS')


if __name__ == '__main__':
    asyncio.run(main())
