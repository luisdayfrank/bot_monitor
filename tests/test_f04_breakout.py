"""F4 — Tests standalone: gatillo breakout (I75 ruta A) + invalidacion.

Ejecutar en el host del bot:  python tests/test_f04_breakout.py
(Local requiere stubs de numpy/pytz/config/binance/database_v5/grid_engine)
"""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from signals_v5 import SignalGenerator  # noqa: E402
from executor import GridExecutor        # noqa: E402

SYM = 'TESTUSDT'
TS0 = 1794000000000


class FakeRegime:
    def __init__(self, regime):
        self.regimen_actual = {SYM: regime}


def nuevo_sg(regime):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.learning_engine = SimpleNamespace(
        regime_cls=FakeRegime(regime),
        collector=SimpleNamespace(ultima_fila=lambda s: {}),
        evaluar_bypass=lambda **k: False,
    )
    return sg


def vela(sg, k, close, high, low, vol=1000.0, vsma=1000.0):
    sg._alimentar_historial_1m(SYM, {
        'timestamp': TS0 + k * 60000, 'rsi_7': 40.0, 'ema_300': 100.0,
        'close': close, 'high': high, 'low': low,
        'wick_upper_pct': 0.1, 'wick_lower_pct': 0.1, 'body_direction': 0,
        'volume': vol, 'volume_sma20': vsma,
    })


def historial_base(sg):
    for k in range(12):
        vela(sg, k, 100.0, 100.1, 99.9)


def historial_ruptura(sg):
    vela(sg, 12, 98.0, 98.1, 97.9)
    vela(sg, 13, 97.0, 97.1, 96.9)


def preparar(sg, regime):
    sg.learning_engine.regime_cls.regimen_actual[SYM] = regime
    sg.indicadores_1m[SYM] = {
        'close': 97.0, 'high': 97.05, 'low': 96.95, 'open': 97.1,
        'rsi_7': 30.0, 'ema_300': 100.0,
        'volume': 3000.0, 'volume_sma20': 1000.0, 'atr_1m': 0.3,
    }
    sg.indicadores_15m[SYM] = {
        'close': 97.0, 'atr': 0.5, 'rsi': 35.0, 'adx': 30.0,
        'macd_hist': -0.1, 'ema200_15m': 100.0, 'ema50_15m': 100.0,
        'volume': 1600.0, 'volume_sma20': 1000.0,
        'recent_high': 100.0, 'recent_low': 97.0,
        'mfm_15m': -0.3, 'mfm_sma5': -0.30, 'timestamp': TS0 + 14 * 60000,
    }
    st = sg.states[SYM]
    st.estado = 'ARMED'
    st.grid_pausa_hasta = 0
    st.moneda_pausada = False
    st.moneda_pausada_manual = False
    return st


async def test_cap_historial():
    sg = nuevo_sg('RANGE')
    for k in range(30):
        vela(sg, k, 100.0, 100.1, 99.9)
    assert len(sg._historial_1m[SYM]) == 20, len(sg._historial_1m[SYM])
    print('  [OK] I8: _historial_1m cap 20')


async def test_gatillo_breakout():
    sg = nuevo_sg('TREND_DOWN')
    llamadas = []

    async def fake(symbol, direction, price, i1m, i15, regimen):
        llamadas.append((symbol, direction, regimen))

    sg._procesar_disparo_breakout = fake
    historial_base(sg)
    historial_ruptura(sg)
    preparar(sg, 'TREND_DOWN')
    await sg.evaluar_gatillo_breakout(SYM)
    assert llamadas == [(SYM, 'SHORT', 'TREND_DOWN')], llamadas
    print('  [OK] F4.1/I75-A: BOS bajista + 2 cierres + volumen + MFM -> SHORT')

    # Sin ruptura (precio dentro del rango previo) -> no dispara
    llamadas.clear()
    sg.indicadores_1m[SYM]['close'] = 100.0
    await sg.evaluar_gatillo_breakout(SYM)
    assert not llamadas, llamadas
    print('  [OK] F4.1: sin ruptura no dispara')

    # Historial insuficiente (<14) -> no dispara
    sg2 = nuevo_sg('TREND_DOWN')
    sg2._procesar_disparo_breakout = fake
    llamadas.clear()
    historial_base(sg2)  # solo 12
    preparar(sg2, 'TREND_DOWN')
    await sg2.evaluar_gatillo_breakout(SYM)
    assert not llamadas
    print('  [OK] F4.1: requiere >=14 velas 1m')


async def test_anti_dup():
    sg = nuevo_sg('TREND_DOWN')
    sg.executor = SimpleNamespace(_grids={SYM: object()},
                                  _grid_pending_creation={}, queue=asyncio.Queue())
    llamadas = []

    async def fake(*a, **k):
        llamadas.append(1)

    sg._procesar_disparo_breakout = fake
    historial_base(sg)
    historial_ruptura(sg)
    preparar(sg, 'TREND_DOWN')
    await sg.evaluar_gatillo_breakout(SYM)
    assert not llamadas
    print('  [OK] I74: grid activo evita re-disparo')


async def test_dispatch():
    sg = nuevo_sg('TREND_DOWN')
    hit = []

    async def br(sym):
        hit.append('br')

    async def mr(sym):
        hit.append('mr')

    sg.evaluar_gatillo_breakout = br
    sg.evaluar_gatillo_mean_rev = mr
    sg.indicadores_1m[SYM] = {'close': 1.0}
    st = sg.states[SYM]
    st.estado = 'ARMED'
    st.grid_pausa_hasta = 0
    await sg.evaluar_gatillo(SYM)
    assert hit == ['br'], hit
    sg.learning_engine.regime_cls.regimen_actual[SYM] = 'RANGE'
    await sg.evaluar_gatillo(SYM)
    assert hit == ['br', 'mr'], hit
    print('  [OK] F4.3: dispatch TREND->breakout / RANGE->mean-rev')


async def test_procesar_breakout_nivel():
    sg = nuevo_sg('TREND_DOWN')
    historial_base(sg)
    historial_ruptura(sg)
    preparar(sg, 'TREND_DOWN')
    sg.calcular_parametros_grid_blindado = lambda *a, **k: (
        {'grid_count': 5, 'auto_compressed': False}, [])
    st = sg.states[SYM]
    await sg._procesar_disparo_breakout(SYM, 'SHORT', 97.0,
                                        sg.indicadores_1m[SYM],
                                        sg.indicadores_15m[SYM], 'TREND_DOWN')
    # SHORT: min(low nivel_ventana)=99.9 + 0.5*0.5 = 100.15
    assert st.breakout_invalidation_level is not None
    assert abs(st.breakout_invalidation_level - 100.15) < 1e-9, st.breakout_invalidation_level
    assert st.estado == 'COOLDOWN'  # sin executor -> fallback cooldown real
    print(f'  [OK] F4.2/I75-A: invalidacion SHORT={st.breakout_invalidation_level:.4f} '
          f'y fallback cooldown sin executor')


async def test_invalidacion_executor():
    ex = object.__new__(GridExecutor)
    ex._hedge_mode = False
    ex.precios_vivo = {SYM: 100.6}
    ex.signal_states = {SYM: SimpleNamespace(breakout_invalidation_level=100.5)}
    aborts = []

    async def fake_abort(sym, razon):
        aborts.append((sym, razon))

    ex._abortar_grid = fake_abort
    st = SimpleNamespace(activa=True, cerrando=False, grid_mode='SHORT',
                         timestamp_inicio=0, posicion_neta=0)
    await ex._evaluar_kill_switch_direccional(SYM, st)
    assert aborts == [(SYM, 'breakout_invalidado')], aborts

    aborts.clear()
    ex.precios_vivo[SYM] = 99.0
    ex.signal_states[SYM] = SimpleNamespace(breakout_invalidation_level=99.5)
    st2 = SimpleNamespace(activa=True, cerrando=False, grid_mode='LONG',
                          timestamp_inicio=0, posicion_neta=0)
    await ex._evaluar_kill_switch_direccional(SYM, st2)
    assert aborts == [(SYM, 'breakout_invalidado')], aborts

    # Sin invalidacion -> no aborta
    aborts.clear()
    ex.precios_vivo[SYM] = 100.0
    ex.signal_states[SYM] = SimpleNamespace(breakout_invalidation_level=101.0)
    await ex._evaluar_kill_switch_direccional(SYM, st)
    assert not aborts
    print('  [OK] F4.4: invalidacion SHORT/LONG aborta; sin cruce no aborta')


async def main():
    await test_cap_historial()
    await test_gatillo_breakout()
    await test_anti_dup()
    await test_dispatch()
    await test_procesar_breakout_nivel()
    await test_invalidacion_executor()
    print('RESULTADO F4: PASS')


if __name__ == '__main__':
    asyncio.run(main())
