"""
tests/test_f12_semana3_adopcion.py — F12.6 / F12.8-bis (Plan v7.4, Semana 3)

1) _tipo_orden_desde_cid: TP/KS/CLOSE/PC/ENTRY.
2) Adopción real en _poll_fills_proactivo: una orden TP adoptada se guarda
   como tipo_orden='TAKE_PROFIT' (antes 'ENTRY' -> posición fantasma) y con
   precio real (avgPrice si price=0).
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import executor as ex  # noqa: E402


def _test_tipo_orden():
    casos = [
        ('CM12_TP_345_1700000000000', 'TAKE_PROFIT'),
        ('CM12_KS_pos_001_1700000000000', 'CIERRE'),
        ('CM12_CLOSE_1700000000000', 'CIERRE'),
        ('CM12_PC_1700000000000', 'CIERRE'),
        ('CM12_BUY_3_1700000000000', 'ENTRY'),
        ('CM12_SELL_3_1700000000000', 'ENTRY'),
        ('CM12_REPL_3_1700000000000', 'ENTRY'),
        ('CM12_000007', 'ENTRY'),
        ('SYNC_999', 'ENTRY'),
    ]
    for cid, esperado in casos:
        got = ex._tipo_orden_desde_cid(cid)
        assert got == esperado, f'{cid}: {got} != {esperado}'
    print(f'  [OK] F12.6/B14: {len(casos)} clientOrderId -> tipo_orden correcto')


class _FakeClient:
    def __init__(self, trade, order_info):
        self.trade = trade
        self.order_info = order_info

    def futures_account_trades(self, **kw):
        return [dict(self.trade)]

    def futures_get_order(self, **kw):
        return dict(self.order_info)


class _Stub(ex.GridExecutor):
    def __init__(self, client):
        self.client = client

    async def _api_call(self, coro):
        return await coro


async def _test_adopcion_tp():
    trade = {
        'id': 501, 'orderId': 900, 'side': 'SELL',
        'price': '101.5', 'qty': '1.0', 'commission': '0.01',
        'commissionAsset': 'USDT', 'realizedPnl': '0',
        'time': 1700000000000,
    }
    order_info = {
        'orderId': 900,
        'clientOrderId': 'CM5_TP_900_1700000000000',
        'side': 'SELL', 'price': '0', 'avgPrice': '101.5', 'origQty': '1.0',
    }
    capturadas = []

    async def _fake_ultimo_ts(symbol):
        return 0

    async def _fake_ordenes(grid_id):
        return []

    async def _fake_fill_tracking(**kw):
        return None

    async def _fake_guardar_orden(**kw):
        capturadas.append(kw)

    orig = (ex.obtener_ultimo_trade_timestamp, ex.cargar_ordenes_por_grid,
            ex.guardar_fill_tracking, ex.guardar_orden_ejecucion)
    ex.obtener_ultimo_trade_timestamp = _fake_ultimo_ts
    ex.cargar_ordenes_por_grid = _fake_ordenes
    ex.guardar_fill_tracking = _fake_fill_tracking
    ex.guardar_orden_ejecucion = _fake_guardar_orden
    try:
        stub = _Stub(_FakeClient(trade, order_info))
        state = SimpleNamespace(
            activa=True, grid_mode='NEUTRAL',
            grid_state=SimpleNamespace(activa=True),
            grid_id=5, timestamp_inicio=1699999000)
        res = await ex.GridExecutor._poll_fills_proactivo(stub, 'TESTUSDT', state)
    finally:
        (ex.obtener_ultimo_trade_timestamp, ex.cargar_ordenes_por_grid,
         ex.guardar_fill_tracking, ex.guardar_orden_ejecucion) = orig

    assert len(res) == 1 and res[0]['tipo'] == 'NUEVO_FILL'
    assert capturadas, 'la orden TP adoptada debe registrarse en DB'
    fila = capturadas[0]
    assert fila['tipo_orden'] == 'TAKE_PROFIT', fila
    assert fila['price'] == 101.5, f"price={fila['price']} (esperado avgPrice 101.5)"
    print('  [OK] F12.6+F12.8-bis: adopción TP -> TAKE_PROFIT y price=avgPrice')


async def main():
    _test_tipo_orden()
    await _test_adopcion_tp()
    print('F12 semana 3 adopción: PASS')


if __name__ == '__main__':
    asyncio.run(main())
