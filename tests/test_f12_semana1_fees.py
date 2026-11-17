"""
tests/test_f12_semana1_fees.py — F12.5/F12.10 (Plan v7.3, Semana 1)

1) _procesar_trade_con_pnl registra el fee UNA sola vez y actualiza los 3
   acumuladores (_registrar_fee, state.fees_real, gs.fees_totales).
2) ids UNKNOWN -> ERROR_TRAZABILIDAD: sin PnL, sin fee, sin persistencia.
3) trade duplicado -> skip RAM, sin segundo fee (B23).
"""
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import executor as ex  # noqa: E402


class _StubGS:
    def __init__(self):
        self.fees_totales = Decimal('0')


class _StubState:
    def __init__(self):
        self.symbol = 'TESTUSDT'
        self.grid_id = 1
        self.fees_real = Decimal('0')
        self.pnl_real = Decimal('0')
        self.grid_state = _StubGS()


class _StubSelf:
    def __init__(self):
        self.fees = []

    def _registrar_fee(self, monto):
        self.fees.append(abs(monto))


async def _test_fee_single_owner():
    stub, state = _StubSelf(), _StubState()
    guardado = {}

    async def _fake_guardar_pnl_evento(**kw):
        guardado.update(kw)

    async def _fake_no_existe(_tid):
        return False

    orig = ex.guardar_pnl_evento
    orig_ex = ex.pnl_evento_existe
    ex.guardar_pnl_evento = _fake_guardar_pnl_evento
    ex.pnl_evento_existe = _fake_no_existe
    try:
        trade = {'price': 100.0, 'qty': 2.0, 'side': 'BUY',
                 'commission': 0.1, 'commissionAsset': 'USDT',
                 'id': 111, 'orderId': 222, 'time': 1700000000000,
                 'realizedPnl': 0.0}
        await ex.GridExecutor._procesar_trade_con_pnl(
            stub, state, trade, 'FILL_ENTRADA')
    finally:
        ex.guardar_pnl_evento = orig
        ex.pnl_evento_existe = orig_ex

    assert len(stub.fees) == 1, f'fee registrado {len(stub.fees)}x (esperado 1)'
    assert state.fees_real == Decimal('0.1')
    assert state.grid_state.fees_totales == Decimal('0.1')
    assert guardado.get('binance_trade_id') == '111'
    print('  [OK] F12.5: fee con propiedad única (1 registro, 3 acumuladores)')


async def _test_unknown_trade_id():
    stub, state = _StubSelf(), _StubState()
    orig = ex.guardar_pnl_evento

    async def _fake_guardar(**kw):
        raise AssertionError('guardar_pnl_evento NO debe llamarse con ids UNKNOWN')

    ex.guardar_pnl_evento = _fake_guardar
    try:
        trade = {'price': 100.0, 'qty': 2.0, 'side': 'SELL',
                 'commission': 0.1, 'commissionAsset': 'USDT',
                 'time': 1700000000000, 'realizedPnl': 5.0}
        res = await ex.GridExecutor._procesar_trade_con_pnl(
            stub, state, trade, 'FILL_SALIDA')
    finally:
        ex.guardar_pnl_evento = orig

    assert res.get('descartado') and res.get('error') == 'TRAZABILIDAD'
    assert len(stub.fees) == 0, 'no debe registrar fee sin trazabilidad'
    assert state.pnl_real == Decimal('0') and state.fees_real == Decimal('0')
    print('  [OK] F12.10: ids UNKNOWN -> ERROR_TRAZABILIDAD sin contabilizar')


async def _test_duplicado_no_duplica_fee():
    stub, state = _StubSelf(), _StubState()

    async def _fake_guardar(**kw):
        pass  # INSERT IGNORE en la vida real

    async def _fake_existe(_tid):
        return True  # trade ya contabilizado (B23)

    orig = ex.guardar_pnl_evento
    orig_ex = ex.pnl_evento_existe
    ex.guardar_pnl_evento = _fake_guardar
    ex.pnl_evento_existe = _fake_existe
    try:
        trade = {'price': 100.0, 'qty': 2.0, 'side': 'BUY',
                 'commission': 0.1, 'commissionAsset': 'USDT',
                 'id': 333, 'orderId': 444, 'time': 1700000000000,
                 'realizedPnl': 1.0}
        res = await ex.GridExecutor._procesar_trade_con_pnl(
            stub, state, trade, 'FILL_SALIDA')
    finally:
        ex.guardar_pnl_evento = orig
        ex.pnl_evento_existe = orig_ex

    assert res.get('duplicado'), 'trade duplicado debe marcarse como tal'
    assert len(stub.fees) == 0, 'duplicado NO debe registrar fee'
    assert state.fees_real == Decimal('0') and state.pnl_real == Decimal('0')
    print('  [OK] B23: trade duplicado -> skip RAM, sin segundo fee')


async def main():
    await _test_fee_single_owner()
    await _test_unknown_trade_id()
    await _test_duplicado_no_duplica_fee()
    print('F12 semana 1: PASS')


if __name__ == '__main__':
    asyncio.run(main())
