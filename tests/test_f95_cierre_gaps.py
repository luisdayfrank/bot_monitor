"""F9.5 - Tests standalone: cierre de gaps P1/P2/P3 + warning ACTIVE.

Ejecutar en el host del bot:  python tests/test_f95_cierre_gaps.py
(Local requiere stubs de numpy/pytz/config/binance/database_v5/grid_engine)
"""
import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import learning_db as ldb              # noqa: E402
import learning_engine as le           # noqa: E402
import signals_v5 as sgv               # noqa: E402
from signals_v5 import SignalGenerator  # noqa: E402

SYM = 'TESTUSDT'

CASOS_MATCH = [
    ('{"adx_15m": {"_min": 25}}', {'adx_15m': 30}),
    ('{"adx_15m": {"_min": 25}}', {'adx_15m': 20}),
    ('{"adx_15m": {"_min": 25, "_max": 50}}', {'adx_15m': 60}),
    ('{"adx_15m": {"_min": 25, "_max": 50}}', {'adx_15m': 40}),
    ('{"adx_15m_min": 25}', {'adx_15m': 30}),
    ('{"adx_15m_max": 25}', {'adx_15m': 30}),
    ('{"rsi_15m": {"_min": 30}, "ema200_dist_pct": {"_max": 1.0}}',
     {'rsi_15m': 40, 'ema200_dist_pct': 0.5}),
    ('{"rsi_15m": {"_min": 30}, "ema200_dist_pct": {"_max": 1.0}}',
     {'rsi_15m': 20, 'ema200_dist_pct': 0.5}),
    ('{"adx_15m": {"_min": 25}}', {}),
    ('no es json', {'adx_15m': 30}),
    ('[1,2,3]', {'adx_15m': 30}),
]


def test_match_unificado():
    for cond, ctx in CASOS_MATCH:
        r_mod = le._match_condition_json(cond, ctx)
        r_byp = le.LearningEngine._match_bypass_condition(None, cond, ctx)
        r_ent = le.EntryRuleEngine._match_condition(cond, ctx)
        assert r_mod == r_byp == r_ent, (cond, ctx, r_mod, r_byp, r_ent)
    print(f'  [OK] F9.5-P1: matcher unificado ({len(CASOS_MATCH)} casos)')


async def test_minar_fires_pbase0():
    rows = [{'direction': 'SHORT', 'regime': 'RANGE',
             'rentable_neto_2h': 0, 'adx_15m': 30.0, 'rsi_15m': 50.0,
             'ema200_dist_pct': 1.0} for _ in range(100)]

    async def _cargar(horas=0):
        return rows

    async def _guardar(cands):
        return len(cands)

    orig = (ldb.cargar_fire_labels, ldb.guardar_entry_rules_batch)
    ldb.cargar_fire_labels = _cargar
    ldb.guardar_entry_rules_batch = _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_fires()
    finally:
        (ldb.cargar_fire_labels, ldb.guardar_entry_rules_batch) = orig
    assert len(cands) == 1, cands
    r = cands[0]
    assert r['accion'] == 'BLOCK' and r['condition_json'] == '{}', r
    assert r['n_occurrences'] == 100 and r['win_rate'] == 0.0, r
    assert r['lift_vs_baseline'] is None and r['is_active'] == 0, r
    print('  [OK] F9.5-P2: minar_fires p_base=0 -> 1 BLOCK global')


async def test_minar_neutral_pbase0():
    def _outcomes():
        return [{'n_roundtrips': 3, 'pnl_real': -1.0, 'adx_15m': 15.0,
                 'rsi_15m': 50.0, 'ema200_dist_pct': 1.0,
                 'atr_pct_15m': 0.8, 'engine_version': 2}
                for _ in range(60)]

    async def _cargar(dias=0, direction=None, engine_version_min=None):
        assert engine_version_min == 2
        return _outcomes()

    async def _guardar(cands):
        return len(cands)

    orig = (le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch)
    le.cargar_grid_outcomes = _cargar
    ldb.guardar_entry_rules_batch = _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_neutral_grids()
    finally:
        (le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch) = orig
    assert len(cands) == 1, cands
    r = cands[0]
    assert r['accion'] == 'BLOCK' and r['condition_json'] == '{}', r
    assert r['tipo_entrada'] == 'NEUTRAL' and r['direction'] == 'NEUTRAL', r
    assert r['lift_vs_baseline'] is None, r
    print('  [OK] F9.5-P2: minar_neutral_grids p_base=0 -> 1 BLOCK global')


async def test_minar_breakouts_pbase0():
    rows = [{'continuacion': 0, 'er_15m': 0.5, 'volume_ratio_15m': 2.0,
             'bb_width_pct_pct': 60.0, 'atr_ratio_1m_15m': 1.0,
             'roc_4v_pct': 1.5} for _ in range(60)]

    async def _cargar(horas=0):
        return rows

    async def _guardar(cands):
        return len(cands)

    orig = (ldb.cargar_breakout_labels, ldb.guardar_entry_rules_batch)
    ldb.cargar_breakout_labels = _cargar
    ldb.guardar_entry_rules_batch = _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_breakouts()
    finally:
        (ldb.cargar_breakout_labels, ldb.guardar_entry_rules_batch) = orig
    assert len(cands) == 1, cands
    r = cands[0]
    assert r['accion'] == 'BLOCK' and r['condition_json'] == '{}', r
    assert r['tipo_entrada'] == 'BREAKOUT' and r['lift_vs_baseline'] is None
    print('  [OK] F9.5-P2: minar_breakouts p_base=0 -> 1 BLOCK global')


def _sg_fire(eng):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.states[SYM] = SimpleNamespace()
    sg.learning_engine = SimpleNamespace(
        entry_engine=eng,
        regime_cls=SimpleNamespace(regimen_actual={SYM: 'RANGE'}),
        collector=SimpleNamespace(ultima_fila=lambda s: {}),
    )
    sg.calcular_parametros_grid_blindado = (
        lambda price, direction, atr, i15, i4h, symbol, state:
        ({'grid_count': 5}, []))
    sg._calcular_umbral_dinamico = lambda s: 0
    aud = SimpleNamespace(llamadas=[])

    async def _rechazado(**kw):
        aud.llamadas.append(kw)

    aud.log_rechazado = _rechazado
    sg.audit_logger = aud
    sg.alertas = []

    async def _alerta(*a, **k):
        sg.alertas.append(a)

    sg.emitir_alerta = _alerta
    return sg


def _i15():
    return {'close': 100.0, 'adx': 25.0, 'rsi': 50.0, 'atr': 0.5,
            'volume': 10.0, 'volume_sma20': 5.0, 'ema200_15m': 95.0,
            'macd_hist': 0.0, 'timestamp': 1794000000000}


async def test_fire_score_real():
    mode0 = getattr(sgv.CONFIG, 'learning_apply_mode', 'OFF')
    try:
        eng = le.EntryRuleEngine()
        eng.cargar([{'tipo_entrada': 'FIRE', 'accion': 'REQUIRE_SCORE',
                     'symbol': None, 'direction': 'SHORT',
                     'regime': 'RANGE', 'condition_json': '{}',
                     'score_bonus': 0, 'n_occurrences': 50,
                     'win_rate': 0.5, 'wilson_lower': 0.5,
                     'lift_vs_baseline': 1.0, 'is_active': 1}])
        sg = _sg_fire(eng)
        sgv.CONFIG.learning_apply_mode = 'ACTIVE'
        i15 = _i15()
        i1m_bajo = {'rsi_7': 50.0, 'wick_upper_pct': 0.0, 'volume': 10.0,
                    'volume_sma20': 10.0, 'atr_1m': 0.0, 'close': 100.0}
        i1m_alto = {'rsi_7': 85.0, 'wick_upper_pct': 2.0, 'volume': 20.0,
                    'volume_sma20': 10.0, 'atr_1m': 1.0, 'close': 100.0}
        s_bajo = sg._calcular_score_disparo(i1m_bajo, 'SHORT')
        s_alto = sg._calcular_score_disparo(i1m_alto, 'SHORT')
        assert s_bajo < 70 <= s_alto, (s_bajo, s_alto)
        captured = []
        real = eng.evaluar

        def _evaluar(tipo, symbol, direction, regime, context,
                     score_actual):
            captured.append(score_actual)
            return real(tipo, symbol, direction, regime, context,
                        score_actual)

        eng.evaluar = _evaluar
        await sg._procesar_disparo(SYM, 'SHORT', 100.0, i1m_bajo, i15)
        assert captured[-1] == s_bajo, captured
        assert sg.audit_logger.llamadas, 'ACTIVE bloqueo esperado'
        assert any('entry_rule_FIRE' in r
                   for kw in sg.audit_logger.llamadas
                   for r in (kw.get('rechazos') or []))
        await sg._procesar_disparo(SYM, 'SHORT', 100.0, i1m_alto, i15)
        assert captured[-1] == s_alto, captured
        assert sg.states[SYM].estado == 'COOLDOWN'
    finally:
        sgv.CONFIG.learning_apply_mode = mode0
    print('  [OK] F9.5-P3: FIRE usa _calcular_score_disparo (score real)')


async def test_fire_shadow_observa():
    mode0 = getattr(sgv.CONFIG, 'learning_apply_mode', 'OFF')
    try:
        eng = le.EntryRuleEngine()
        eng.cargar([{'tipo_entrada': 'FIRE', 'accion': 'BLOCK',
                     'symbol': None, 'direction': 'SHORT',
                     'regime': 'RANGE', 'condition_json': '{}',
                     'score_bonus': 0, 'n_occurrences': 50,
                     'win_rate': 0.0, 'wilson_lower': 0.0,
                     'lift_vs_baseline': None, 'is_active': 1}])
        sg = _sg_fire(eng)
        sgv.CONFIG.learning_apply_mode = 'SHADOW'
        await sg._procesar_disparo(SYM, 'SHORT', 100.0,
                                   {'rsi_7': 50.0, 'wick_upper_pct': 0.0,
                                    'volume': 10.0, 'volume_sma20': 10.0,
                                    'atr_1m': 0.0, 'close': 100.0}, _i15())
        assert not sg.audit_logger.llamadas
        assert not sg.alertas
        assert sg.states[SYM].estado == 'COOLDOWN'
    finally:
        sgv.CONFIG.learning_apply_mode = mode0
    print('  [OK] F9.5: SHADOW solo observa (sin bloqueo, sigue el flujo)')


async def test_f95_marker():
    await ldb.init_learning_db(':memory:')
    try:
        assert await ldb.f95_aplicado()
        row = await ldb._run(lambda: ldb._get_conn().execute(
            "SELECT 1 FROM schema_versions WHERE componente='ENTRY_RULES' "
            "AND version='v1.0_f95' AND activo=1").fetchone())
        assert row
    finally:
        await ldb.close_learning_db()
    print('  [OK] F9.5: marker ENTRY_RULES v1.0_f95 registrado')


async def test_warning_activo_sin_f95():
    eng = object.__new__(le.LearningEngine)
    eng.cfg = SimpleNamespace(learning_apply_mode='ACTIVE')
    eng.notifier = None
    orig = ldb.f95_aplicado

    async def _f95_false():
        return False

    async def _f95_true():
        return True

    try:
        ldb.f95_aplicado = _f95_false
        await eng._verificar_f95_activo()
        assert eng.cfg.learning_apply_mode == 'SHADOW'
        ldb.f95_aplicado = _f95_true
        eng.cfg.learning_apply_mode = 'ACTIVE'
        await eng._verificar_f95_activo()
        assert eng.cfg.learning_apply_mode == 'ACTIVE'
    finally:
        ldb.f95_aplicado = orig
    print('  [OK] F9.5: ACTIVE sin marker -> SHADOW; con marker -> sin cambio')


async def main():
    test_match_unificado()
    await test_minar_fires_pbase0()
    await test_minar_neutral_pbase0()
    await test_minar_breakouts_pbase0()
    await test_fire_score_real()
    await test_fire_shadow_observa()
    await test_f95_marker()
    await test_warning_activo_sin_f95()
    print('RESULTADO F9.5: PASS')


if __name__ == '__main__':
    asyncio.run(main())
