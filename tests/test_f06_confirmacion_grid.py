"""F0.6.8 — Tests de regresion: confirmacion de grid Executor->Signals + cooldown.

Standalone (sin pytest):  python tests/test_f06_confirmacion_grid.py
Cubre:
  1) FIRE rechazado por el executor NO vuelve a ARMED (no bucle) -> MONITOREO,
     flags limpiados, fallos incrementados y pausa 30 min tras 3 fallos.
  2) FIRE confirmado OK -> COOLDOWN con cooldown_hasta ~= now + 900_000 ms.
  3) Cooldown real 15 min: no sale de COOLDOWN por cambio de vela; sale con
     filtro OK -> ARMED o filtro perdido -> MONITOREO. cooldown_hasta=0
     (circuit breaker) conserva la condicion original (I40).
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from signals_v5 import SignalGenerator  # noqa: E402


class FakeExecutor:
    """Simula el executor (B5: usa signal_states, no signal_generator)."""

    def __init__(self, ok, motivo=''):
        self.signal_states = {}
        self.queue = asyncio.Queue()
        self._grids = {}
        self._grid_pending_creation = {}
        self._ok = ok
        self._motivo = motivo

    async def procesar_cola(self):
        msg = self.queue.get_nowait()
        st = self.signal_states.get(msg['symbol'])
        if self._ok:
            self._grids[msg['symbol']] = type('G', (), {'activa': True})()
        if st is not None:
            st.grid_creado_ok = self._ok
            st.grid_creado_motivo = self._motivo
            st.esperando_grid_ok = False


def nuevo_signals(executor=None):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    if executor is not None:
        sg.executor = executor
        executor.signal_states = sg.states
    return sg


def preparar_disparo(sg, sym='TESTUSDT'):
    sg.indicadores_15m[sym] = {
        'timestamp': 1794000000000, 'close': 100.0, 'rsi': 30.0,
        'adx': 30.0, 'atr': 0.5, 'score_macro': 85,
        'recent_high': 102.0, 'recent_low': 98.0,
    }
    sg.indicadores_1m[sym] = {'close': 100.0}
    sg.calcular_parametros_grid_blindado = lambda *a, **k: (
        {'grid_count': 5, 'auto_compressed': False,
         'upper_limit': 102.0, 'lower_limit': 98.0}, [])
    sg._calcular_umbral_dinamico = lambda symbol: 0
    sg._calcular_score_disparo = lambda i1m, direction: 80
    st = sg.states[sym]
    st.estado = 'ARMED'
    st.filtro_macro_aprobado = True
    st.direccion_filtro = 'LONG'
    st.score_macro_actual = 85
    return st


async def disparo(sg, sym='TESTUSDT'):
    await sg._procesar_disparo(
        sym, 'LONG', 100.0, sg.indicadores_1m[sym], sg.indicadores_15m[sym])


async def test_fire_sin_grid_no_bucle():
    ex = FakeExecutor(ok=False, motivo='leverage_no_viable')
    sg = nuevo_signals(ex)
    st = preparar_disparo(sg)

    # 1er FIRE -> esperando confirmacion (NO COOLDOWN directo)
    await disparo(sg)
    assert st.estado == 'FIRE', st.estado
    assert st.esperando_grid_ok is True
    await ex.procesar_cola()
    assert st.grid_creado_ok is False
    assert st.grid_creado_motivo == 'leverage_no_viable'
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'MONITOREO', st.estado
    assert st.filtro_macro_aprobado is False
    assert st.grid_fallos_consecutivos == 1
    assert st.esperando_grid_ok is False and st.grid_creado_ok is None

    # 2do FIRE fallido
    preparar_disparo(sg)
    await disparo(sg)
    await ex.procesar_cola()
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.grid_fallos_consecutivos == 2

    # 3er FIRE fallido -> pausa 30 min
    preparar_disparo(sg)
    await disparo(sg)
    await ex.procesar_cola()
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.grid_fallos_consecutivos == 3
    assert st.grid_pausa_hasta > time.time()

    # evaluar_gatillo respeta la pausa (no encola nada)
    st.estado = 'ARMED'
    st.filtro_macro_aprobado = True
    st.direccion_filtro = 'LONG'
    await sg.evaluar_gatillo('TESTUSDT')
    assert ex.queue.empty()
    print('  [OK] test_fire_sin_grid_no_bucle')


async def test_fire_con_grid_ok():
    ex = FakeExecutor(ok=True)
    sg = nuevo_signals(ex)
    st = preparar_disparo(sg)

    await disparo(sg)
    assert st.estado == 'FIRE' and st.esperando_grid_ok is True
    await ex.procesar_cola()
    assert st.grid_creado_ok is True
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'COOLDOWN', st.estado
    assert st.cooldown_hasta > int(time.time() * 1000) + 890_000
    assert st.grid_fallos_consecutivos == 0
    assert st.fire_payload is None
    print('  [OK] test_fire_con_grid_ok')


async def test_cooldown_15m_real():
    sg = nuevo_signals(None)
    st = sg.states['TESTUSDT']
    sg.indicadores_15m['TESTUSDT'] = {'timestamp': 1794000000000}

    # Cooldown vigente: cambio de vela NO debe sacar de COOLDOWN
    st.estado = 'COOLDOWN'
    st.cooldown_hasta = int(time.time() * 1000) + 900_000
    st.filtro_macro_aprobado = True
    st.ultimo_disparo_timestamp_15m = 1794000000000
    sg.indicadores_15m['TESTUSDT'] = {'timestamp': 1794000900000}
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'COOLDOWN', st.estado

    # Cooldown vencido + filtro OK -> ARMED
    st.cooldown_hasta = int(time.time() * 1000) - 1
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'ARMED', st.estado

    # Cooldown vencido + filtro perdido -> MONITOREO (I28)
    st.estado = 'COOLDOWN'
    st.cooldown_hasta = int(time.time() * 1000) - 1
    st.filtro_macro_aprobado = False
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'MONITOREO', st.estado

    # I40: cooldown_hasta=0 (CB) usa condicion original por timestamp
    st.estado = 'COOLDOWN'
    st.cooldown_hasta = 0
    st.filtro_macro_aprobado = True
    st.ultimo_disparo_timestamp_15m = 1794000900000
    sg.indicadores_15m['TESTUSDT'] = {'timestamp': 1794000900000}
    await sg._actualizar_estado_maquina('TESTUSDT')
    assert st.estado == 'COOLDOWN', st.estado
    print('  [OK] test_cooldown_15m_real')


async def main():
    await test_fire_sin_grid_no_bucle()
    await test_fire_con_grid_ok()
    await test_cooldown_15m_real()
    print('RESULTADO: PASS (3/3)')


if __name__ == '__main__':
    asyncio.run(main())
