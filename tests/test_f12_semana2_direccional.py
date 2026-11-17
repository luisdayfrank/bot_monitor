"""
tests/test_f12_semana2_direccional.py — F12.11 (Plan v7.4, Semana 2)

1) fill ENTRY direccional -> registrar_entrada (dirección correcta)
2) fill TAKE_PROFIT total -> registrar_salida + posición CERRADA
3) fill TAKE_PROFIT parcial -> NO salida, qty reducida, trayectoria viva
4) fill de cierre NO crea PosicionReal huérfana
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
        self.entradas = []
        self.salidas = []

    def registrar_entrada(self, **kw):
        self.entradas.append(kw)

    def registrar_salida(self, **kw):
        self.salidas.append(kw)


class _StubSelf:
    def __init__(self):
        self.learning_engine = _LE()


def _state():
    return SimpleNamespace(
        symbol='TESTUSDT', grid_mode='SHORT', grid_id=7,
        posicion_neta=Decimal('0'), posiciones_direccionales=[],
        pnl_real=Decimal('0'), ordenes={},
    )


def _fill(side, qty, price=100.0, pnl=0.0, oid='1'):
    return {'side': side, 'price': price, 'qty': qty, 'commission': 0.0,
            'realized_pnl': pnl, 'timestamp_ms': 1700000000000,
            'binance_order_id': oid}


async def _test_flujo_direccional():
    stub, state = _StubSelf(), _state()
    le = stub.learning_engine

    # 1) ENTRY SHORT
    await ex.GridExecutor._procesar_fill_direccional(
        stub, state, _fill('SELL', 2.0, oid='10'), {'tipo_orden': 'ENTRY'})
    assert len(state.posiciones_direccionales) == 1
    pos = state.posiciones_direccionales[0]
    assert pos.tipo == 'SHORT' and pos.estado == 'ABIERTA'
    assert len(le.entradas) == 1 and le.entradas[0]['direccion'] == 'SHORT'
    print('  [OK] F12.11: ENTRY direccional -> registrar_entrada SHORT')

    # 2) TP parcial (1.0 de 2.0) -> sin salida, qty reducida
    await ex.GridExecutor._procesar_fill_direccional(
        stub, state, _fill('BUY', 1.0, oid='11'), {'tipo_orden': 'TAKE_PROFIT'})
    assert len(le.salidas) == 0, 'parcial NO debe registrar salida'
    assert pos.estado == 'ABIERTA' and pos.qty == Decimal('1.0')
    assert len(state.posiciones_direccionales) == 1, 'cierre no crea pos huérfana'
    print('  [OK] F12.11: cierre parcial -> trayectoria viva, sin pos huérfana')

    # 3) TP total del resto -> salida + CERRADA
    await ex.GridExecutor._procesar_fill_direccional(
        stub, state, _fill('BUY', 1.0, oid='12'), {'tipo_orden': 'TAKE_PROFIT'})
    assert len(le.salidas) == 1 and le.salidas[0]['posicion_id'] == pos.id
    assert le.salidas[0]['razon'] == 'TAKE_PROFIT'
    assert pos.estado == 'CERRADA' and pos.qty == Decimal('0')
    print('  [OK] F12.11: cierre total -> registrar_salida + CERRADA')


async def main():
    await _test_flujo_direccional()
    print('F12 semana 2 direccional: PASS')


if __name__ == '__main__':
    asyncio.run(main())
