"""
tests/test_f12_semana3_paginacion.py — F12.14 (Plan v7.4, Semana 3)

1) 2500 trades -> 3 páginas (1000 startTime + 1000 fromId=1001 + 500 fromId=2001).
2) Exactamente 1000 -> segunda página vacía y corte (sin bucle infinito).
3) Sin startTime -> primera llamada sin filtro temporal.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import executor as ex  # noqa: E402


class _FakeClient:
    def __init__(self, trades):
        self.trades = trades
        self.llamadas = []

    def futures_account_trades(self, **kw):
        self.llamadas.append(dict(kw))
        desde = kw.get('fromId')
        if desde is not None:
            sel = [t for t in self.trades if t['id'] >= desde]
        else:
            st = kw.get('startTime', 0)
            sel = [t for t in self.trades if t['time'] >= st]
        return [dict(t) for t in sel[:kw.get('limit', 500)]]


class _Stub:
    _MAX_TRADES_POR_POLL = ex.GridExecutor._MAX_TRADES_POR_POLL

    def __init__(self, client):
        self.client = client

    async def _api_call(self, coro):
        return await coro


def _trades(n):
    return [{'id': i, 'time': 1_700_000_000_000 + i, 'price': '1.0',
             'qty': '1.0', 'commission': '0.0', 'side': 'BUY'}
            for i in range(1, n + 1)]


async def _test_2500():
    cliente = _FakeClient(_trades(2500))
    res = await ex.GridExecutor._traer_trades_paginado(
        _Stub(cliente), 'TESTUSDT', start_time=1)

    assert len(res) == 2500, len(res)
    assert [t['id'] for t in res] == list(range(1, 2501)), 'ids duplicados o faltantes'
    assert len(cliente.llamadas) == 3, cliente.llamadas
    assert cliente.llamadas[0]['startTime'] == 1
    assert cliente.llamadas[1]['fromId'] == 1001
    assert cliente.llamadas[2]['fromId'] == 2001
    assert all(c['limit'] == 1000 for c in cliente.llamadas)
    print('  [OK] F12.14/B8: 2500 trades -> 3 páginas sin duplicados')


async def _test_1000_exactos():
    cliente = _FakeClient(_trades(1000))
    res = await ex.GridExecutor._traer_trades_paginado(
        _Stub(cliente), 'TESTUSDT', start_time=1)

    assert len(res) == 1000
    assert len(cliente.llamadas) == 2, cliente.llamadas
    assert cliente.llamadas[1]['fromId'] == 1001
    print('  [OK] F12.14/B8: página exacta de 1000 -> corte limpio')


async def _test_sin_start_time():
    cliente = _FakeClient(_trades(5))
    res = await ex.GridExecutor._traer_trades_paginado(_Stub(cliente), 'TESTUSDT')

    assert len(res) == 5
    assert 'startTime' not in cliente.llamadas[0]
    assert 'fromId' not in cliente.llamadas[0]
    print('  [OK] F12.14/B8: sin watermark -> primera página sin filtro')


async def main():
    await _test_2500()
    await _test_1000_exactos()
    await _test_sin_start_time()
    print('F12 semana 3 paginación: PASS')


if __name__ == '__main__':
    asyncio.run(main())
