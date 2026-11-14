"""F10 - Tests standalone: B1 promocion BLOCK, B2/B4 flags, B3 umbrales,
B5 CONFIG, B4 DB revisados.

Ejecutar en el host del bot:  python tests/test_f10_cierre_deudas.py
(Local requiere stubs de numpy/pytz/config/binance/database_v5/grid_engine)
"""
import asyncio
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as cfgmod               # noqa: E402
import learning_db as ldb             # noqa: E402
import learning_engine as le          # noqa: E402
from signals_v5 import SignalGenerator  # noqa: E402

SYM = 'TESTUSDT'


def _regla(rid=1, accion='BLOCK', activa=0, n=100, wilson=0.0,
           win_rate=0.0, dias=8):
    creado = (datetime.now(timezone.utc) - timedelta(days=dias))
    return {'id': rid, 'tipo_entrada': 'FIRE', 'accion': accion,
            'symbol': None, 'direction': 'SHORT', 'regime': 'RANGE',
            'condition_json': '{}', 'n_occurrences': n,
            'win_rate': win_rate, 'wilson_lower': wilson,
            'lift_vs_baseline': None, 'is_active': activa,
            'created_at': creado.strftime('%Y-%m-%d %H:%M:%S')}


async def _promover(reglas, cfg=None):
    ups = []

    async def _todas():
        return reglas

    async def _cuarentena(horas=0):
        return 0

    async def _estado(rid, activa):
        ups.append((rid, activa))

    orig = (ldb.cargar_entry_rules_todas, ldb.ultimo_guardrail_entry_rules,
            ldb.update_entry_rule_estado)
    ldb.cargar_entry_rules_todas = _todas
    ldb.ultimo_guardrail_entry_rules = _cuarentena
    ldb.update_entry_rule_estado = _estado
    try:
        eng = object.__new__(le.LearningEngine)
        eng.cfg = cfg or SimpleNamespace(
            learning_entry_block_promote_max_hi=0.35,
            learning_entry_block_demote_min_hi=0.50)
        await eng._auto_promote_demote_entry_rules()
    finally:
        (ldb.cargar_entry_rules_todas, ldb.ultimo_guardrail_entry_rules,
         ldb.update_entry_rule_estado) = orig
    return ups


def _k_frontera(n, target, mayor):
    """Primer k con hi > target (mayor=True) o hi <= target (mayor=False)."""
    for k in range(n + 1):
        _lo, hi = le._wilson(k, n)
        if mayor and hi > target:
            return k
        if not mayor and hi <= target:
            return k
    return None


async def test_b1_promote_block():
    n = 100
    k_bad = _k_frontera(n, 0.35, True)
    k_ok = k_bad - 1
    assert k_ok >= 0 and le._wilson(k_ok, n)[1] <= 0.35
    assert le._wilson(k_bad, n)[1] > 0.35
    ups_ok = await _promover([_regla(rid=11, accion='BLOCK', n=n,
                                     win_rate=k_ok / n)])
    assert ups_ok == [(11, True)], (k_ok, ups_ok)
    ups_bad = await _promover([_regla(rid=12, accion='BLOCK', n=n,
                                      win_rate=k_bad / n)])
    assert ups_bad == [], (k_bad, ups_bad)
    print(f'  [OK] F10-B1: frontera promote hi<=0.35 (k_ok={k_ok}, '
          f'k_bad={k_bad})')


async def test_b1_no_promote_block_hi():
    ups = await _promover([_regla(rid=2, accion='BLOCK', n=100,
                                  win_rate=0.45)])
    assert ups == [], ups
    print('  [OK] F10-B1: BLOCK wr=0.45 -> no promueve (hi alto)')


async def test_b1_demote_block():
    n = 100
    k_dem = None
    for k in range(n + 1):
        _lo, hi = le._wilson(k, n)
        if hi >= 0.50:
            k_dem = k
            break
    assert k_dem is not None and k_dem > 0
    k_no = k_dem - 1
    assert le._wilson(k_no, n)[1] < 0.50
    assert le._wilson(k_dem, n)[1] >= 0.50
    ups_dem = await _promover([_regla(rid=13, accion='BLOCK', activa=1,
                                      n=n, win_rate=k_dem / n)])
    assert ups_dem == [(13, False)], (k_dem, ups_dem)
    ups_no = await _promover([_regla(rid=14, accion='BLOCK', activa=1,
                                     n=n, win_rate=k_no / n)])
    assert ups_no == [], (k_no, ups_no)
    print(f'  [OK] F10-B1: frontera demote hi>=0.50 (k_dem={k_dem}, '
          f'k_no={k_no})')


async def test_b1_allow_unchanged():
    ups = await _promover([_regla(rid=4, accion='ALLOW', n=50,
                                  wilson=0.65, win_rate=0.8)])
    assert ups == [(4, True)], ups
    print('  [OK] F10-B1: ALLOW conserva criterio wilson>=0.65')


async def test_b1_allow_no_invade_block():
    """D-F10-2: ALLOW con hi bajo pero wilson<0.65 -> NO promueve."""
    ups = await _promover([_regla(rid=5, accion='ALLOW', n=100,
                                  wilson=0.13, win_rate=0.20)])
    assert ups == [], ups
    print('  [OK] F10-D2: ALLOW hi bajo + wilson<0.65 -> no promueve')


async def test_b2_b4_flags():
    eng = object.__new__(le.LearningEngine)
    d = tempfile.mkdtemp()
    f1 = os.path.join(d, 'force.flag')
    f2 = os.path.join(d, 'revisar.flag')
    eng._flag_force_run = f1
    eng._flag_revisar_guardrails = f2
    eng._last_learner_run = 0
    llamadas = {'run': 0, 'rev': 0}

    async def _run():
        llamadas['run'] += 1

    async def _rev():
        llamadas['rev'] += 1
        return 2

    eng.entrenar_todo = _run
    orig = ldb.marcar_guardrails_entry_revisados
    ldb.marcar_guardrails_entry_revisados = _rev
    try:
        await eng._procesar_flags_manuales()
        assert llamadas == {'run': 0, 'rev': 0}, llamadas
        open(f1, 'w').close()
        open(f2, 'w').close()
        await eng._procesar_flags_manuales()
        assert llamadas == {'run': 1, 'rev': 1}, llamadas
        assert not os.path.exists(f1) and not os.path.exists(f2)
    finally:
        ldb.marcar_guardrails_entry_revisados = orig
        for f in os.listdir(d):
            try:
                os.remove(os.path.join(d, f))
            except OSError:
                pass
        try:
            os.rmdir(d)
        except OSError:
            pass
    print('  [OK] F10-B2/B4: flags fuerza-run y revisar guardrails')


async def test_b4_mark_reviewed_db():
    await ldb.init_learning_db(':memory:')
    try:
        ahora = int(__import__('time').time() * 1000)
        await ldb.guardar_guardrail_event({
            'timestamp_ms': ahora, 'tipo': 'ENTRY_RULE_BLOCK_RATE',
            'severidad': 'ALTO', 'razon': 'test',
            'accion_tomada': 'DEMOTE_ALL_ENTRY_RULES',
            'modo_antes': 'ACTIVE', 'modo_despues': 'SHADOW',
            'revisado_por_humano': 0})
        assert await ldb.ultimo_guardrail_entry_rules(horas=24) == ahora
        n = await ldb.marcar_guardrails_entry_revisados()
        assert n == 1, n
        assert await ldb.ultimo_guardrail_entry_rules(horas=24) == 0
    finally:
        await ldb.close_learning_db()
    print('  [OK] F10-B4: revisado_por_humano limpia la cuarentena')


def test_b3_regime_thresholds():
    cfg = cfgmod.CONFIG
    rc = le.RegimeClassifier(min_velas=4)
    f_panic = {'symbol': SYM, 'adx_15m': 20, 'er_15m': 0.0,
               'atr_ratio_1m_15m': 0.5, 'roc_4v_pct': 0,
               'atr_pct_15m': 0.5}
    assert rc.clasificar_inst(f_panic) == 'PANIC'
    f_trend = {'symbol': SYM, 'adx_15m': 30, 'er_15m': 0.5,
               'ema7_ema25_spread_pct': 0.5, 'price_rel_ema50_pct': 1.0,
               'atr_ratio_1m_15m': 0.1, 'roc_4v_pct': 0,
               'atr_pct_15m': 0.5}
    assert rc.clasificar_inst(f_trend) == 'TREND_UP'
    a0 = cfg.learning_panic_atr_ratio_threshold
    e0 = cfg.learning_breakout_min_er
    try:
        cfg.learning_panic_atr_ratio_threshold = 5.0
        assert rc.clasificar_inst(f_panic) == 'RANGE'
        cfg.learning_breakout_min_er = 0.9
        assert rc.clasificar_inst(f_trend) == 'RANGE'
    finally:
        cfg.learning_panic_atr_ratio_threshold = a0
        cfg.learning_breakout_min_er = e0
    print('  [OK] F10-B3: PANIC/TREND respetan config (defaults intactos)')


def _nuevo_sg(bb_pct):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.learning_engine = SimpleNamespace(
        regime_cls=SimpleNamespace(regimen_actual={}),
        collector=SimpleNamespace(ultima_fila=lambda s: {
            'bb_width_pct_pct': bb_pct}),
        entry_engine=le.EntryRuleEngine(),
    )
    return sg


def test_b3_squeeze_threshold():
    cfg = cfgmod.CONFIG
    i15 = {'adx': 15.0, 'rsi': 50.0, 'atr': 0.5, 'close': 100.0,
           'ema50_15m': 100.0, 'macd_hist': 0.0,
           'timestamp': 1794000000000}
    t0 = cfg.learning_squeeze_bb_pct_threshold
    m0 = getattr(cfg, 'learning_entry_rules_mode', 'OFF')
    try:
        cfg.learning_entry_rules_mode = 'OFF'
        cfg.learning_squeeze_bb_pct_threshold = 30.0
        ok, _ = _nuevo_sg(25.0).evaluar_grid_neutro(SYM, i15)
        assert ok is False
        cfg.learning_squeeze_bb_pct_threshold = 20.0
        ok2, _ = _nuevo_sg(25.0).evaluar_grid_neutro(SYM, i15)
        assert ok2 is True
    finally:
        cfg.learning_squeeze_bb_pct_threshold = t0
        cfg.learning_entry_rules_mode = m0
    print('  [OK] F10-B3: SQUEEZE usa umbral configurable')


def test_b5_sin_config_en_tick():
    assert 'CONFIG' not in le.LearningEngine._tick.__code__.co_names
    print('  [OK] F10-B5: _tick sin CONFIG (usa self.cfg)')


async def main():
    await test_b1_promote_block()
    await test_b1_no_promote_block_hi()
    await test_b1_demote_block()
    await test_b1_allow_unchanged()
    await test_b1_allow_no_invade_block()
    await test_b2_b4_flags()
    await test_b4_mark_reviewed_db()
    test_b3_regime_thresholds()
    test_b3_squeeze_threshold()
    test_b5_sin_config_en_tick()
    print('RESULTADO F10: PASS')


if __name__ == '__main__':
    asyncio.run(main())
