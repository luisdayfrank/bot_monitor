"""F5 — Tests standalone: blindaje NEUTRAL (squeeze, expansion ATR, labels).

Ejecutar en el host del bot:  python tests/test_f05_neutral.py
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


class FakeRegime:
    def __init__(self):
        self.regimen_actual = {}


class FakeAudit:
    def __init__(self):
        self.seguimientos = []

    async def iniciar_seguimiento_near_miss(self, **kw):
        self.seguimientos.append(kw)


def nuevo_sg(bb_pct=None):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.learning_engine = SimpleNamespace(
        regime_cls=FakeRegime(),
        collector=SimpleNamespace(
            ultima_fila=lambda s: ({'bb_width_pct_pct': bb_pct}
                                   if bb_pct is not None else {})),
        evaluar_bypass=lambda **k: False,
    )
    return sg


def i15_ok():
    return {'adx': 15.0, 'rsi': 50.0, 'atr': 0.5, 'close': 100.0,
            'ema50_15m': 100.0, 'macd_hist': 0.0, 'timestamp': 1794000000000}


def test_squeeze():
    # bb_width_pct_pct=10 -> rechazo SQUEEZE
    sg = nuevo_sg(bb_pct=10.0)
    ok, razon = sg.evaluar_grid_neutro(SYM, i15_ok())
    assert ok is False and 'SQUEEZE' in razon, (ok, razon)
    # bb_width_pct_pct=55 -> pasa las condiciones
    sg2 = nuevo_sg(bb_pct=55.0)
    ok2, razon2 = sg2.evaluar_grid_neutro(SYM, i15_ok())
    assert ok2 is True, (ok2, razon2)
    # sin learning_engine -> no aplica veto
    sg3 = nuevo_sg(bb_pct=None)
    sg3.learning_engine = None
    ok3, _ = sg3.evaluar_grid_neutro(SYM, i15_ok())
    assert ok3 is True
    print('  [OK] F5.1: veto SQUEEZE (p10 rechaza, p55 pasa, sin engine no aplica)')


def test_aborto_signals():
    sg = nuevo_sg(bb_pct=55.0)
    st = sg.states[SYM]
    st.neutral_grid_timestamp = int(time.time())
    sg.indicadores_1m[SYM] = {'atr_1m': 1.0}
    abortar, razon = sg.evaluar_aborto_neutral_grid(
        SYM, {'atr': 0.5}, st, 310)
    assert abortar and razon.startswith('EXPANSION_ATR'), (abortar, razon)
    # I80-A: raw 0.2 -> norm 0.77 < 1.5 -> sin aborto
    sg.indicadores_1m[SYM] = {'atr_1m': 0.1}
    abortar2, _ = sg.evaluar_aborto_neutral_grid(SYM, {'atr': 0.5}, st, 310)
    assert abortar2 is False
    print('  [OK] F5.2 signals: EXPANSION_ATR (raw 2.0 aborta / raw 0.2 no)')


async def test_aborto_executor():
    ex = object.__new__(GridExecutor)
    ex.indicadores_1m = {SYM: {'atr_1m': 1.0}}
    ex.indicadores_15m = {SYM: {'atr': 0.5}}
    aborts = []

    async def fake_abort(sym, razon):
        aborts.append((sym, razon))

    ex._abortar_grid = fake_abort
    st = SimpleNamespace(
        symbol=SYM, grid_mode='NEUTRAL',
        grid_state=SimpleNamespace(activa=True, posiciones=[],
                                   pnl_bruto=0.0, fees_totales=0.0,
                                   pnl_neto=0.0))
    res = await ex._evaluar_kill_switch(st, 100.0, 0)
    assert res == [] and aborts == [(SYM, 'expansion_atr_ratio')], (res, aborts)
    # I80-A: raw 0.2 -> norm 0.77 < 1.5 -> sin aborto (stubs de calculo)
    ex.indicadores_1m[SYM] = {'atr_1m': 0.1}
    ex._calcular_adverse_price_pct = lambda s, p: 0.0
    ex._calcular_margen_usado_pct = lambda s: 0.0
    aborts.clear()
    res2 = await ex._evaluar_kill_switch(st, 100.0, 0)
    assert res2 == [] and not aborts, (res2, aborts)
    print('  [OK] F5.2 executor: raw 2.0 aborta / raw 0.2 no')


async def test_label_squeeze():
    sg = nuevo_sg(bb_pct=10.0)
    aud = FakeAudit()
    sg.audit_logger = aud
    st = sg.states[SYM]
    st.neutral_grid_last_rechazo_ts = 0
    st.circuit_breaker_activo = False
    entro = await sg._evaluar_transicion_neutral_grid(
        SYM, st, 'NEUTRAL', 70, 70, i15_ok(), 100.0,
        {'category': 'default'}, 'NINGUNA')
    assert entro is False
    tipos = [s.get('tipo_rechazo') for s in aud.seguimientos]
    assert 'COMPRESION_SQUEEZE' in tipos, tipos
    print('  [OK] F5.3: rechazo SQUEEZE registra near-miss COMPRESION_SQUEEZE')


async def main():
    test_squeeze()
    test_aborto_signals()
    await test_aborto_executor()
    await test_label_squeeze()
    print('RESULTADO F5: PASS')


if __name__ == '__main__':
    asyncio.run(main())
