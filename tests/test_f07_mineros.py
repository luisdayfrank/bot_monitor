"""F7 — Tests standalone: mineros entry_rules + cache RAM + upsert."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import learning_db as ldb  # noqa: E402
import learning_engine as le  # noqa: E402


def _filas_fire():
    rows = []
    for _ in range(70):   # perdedores: adx=30
        rows.append({'direction': 'SHORT', 'regime': 'BULLISH',
                     'rentable_neto_2h': 0, 'adx_15m': 30.0,
                     'rsi_15m': None, 'ema200_dist_pct': None})
    for _ in range(30):   # ganadores: adx=10
        rows.append({'direction': 'SHORT', 'regime': 'BULLISH',
                     'rentable_neto_2h': 1, 'adx_15m': 10.0,
                     'rsi_15m': None, 'ema200_dist_pct': None})
    return rows


async def test_minar_fires():
    capturado = []

    async def _cargar(horas=0):
        return _filas_fire()

    async def _guardar(rows):
        capturado.extend(rows)
        return len(rows)

    orig_c, orig_g = ldb.cargar_fire_labels, ldb.guardar_entry_rules_batch
    ldb.cargar_fire_labels, ldb.guardar_entry_rules_batch = _cargar, _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_fires()
    finally:
        ldb.cargar_fire_labels, ldb.guardar_entry_rules_batch = orig_c, orig_g
    assert len(cands) == 2, cands
    assert {c['accion'] for c in cands} == {'BLOCK', 'ALLOW'}
    block = [c for c in cands if c['accion'] == 'BLOCK'][0]
    allow = [c for c in cands if c['accion'] == 'ALLOW'][0]
    assert block['n_occurrences'] == 70 and block['win_rate'] == 0.0
    assert '"_min"' in block['condition_json']
    assert allow['n_occurrences'] == 30 and allow['win_rate'] == 1.0
    assert '"_max"' in allow['condition_json']
    assert len(capturado) == 2
    print('  [OK] F7.2: minar_fires BLOCK+ALLOW (baseline=0.30)')


async def test_minar_neutral_grids():
    """I91 Ruta A: el dataset verifica discriminacion en ambos extremos.

    Desviacion documentada del revision: 80/20 dejaba el bin atr<=0.5 con
    n=20 < min_samples(30) -> sin ALLOW posible. Se usa 70 fallos / 30
    exitos para que ambos bins puros superen min_samples.
    """
    capturado = []

    def _outcomes():
        rows = []
        for _ in range(70):
            rows.append({'n_roundtrips': 0, 'pnl_real': -0.5,
                         'adx_15m': None, 'rsi_15m': None,
                         'ema200_dist_pct': None, 'atr_pct_15m': 1.5,
                         'engine_version': 2})
        for _ in range(30):
            rows.append({'n_roundtrips': 3, 'pnl_real': 0.4,
                         'adx_15m': None, 'rsi_15m': None,
                         'ema200_dist_pct': None, 'atr_pct_15m': 0.3,
                         'engine_version': 2})
        return rows

    async def _cargar(dias=0, direction=None, engine_version_min=None):
        assert engine_version_min == 2, (
            'I90: minar_neutral_grids debe filtrar engine_version>=2')
        return _outcomes()

    async def _guardar(rows):
        capturado.extend(rows)
        return len(rows)

    orig_c, orig_g = le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch
    le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch = _cargar, _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_neutral_grids()
    finally:
        le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch = orig_c, orig_g
    assert cands, cands
    assert any(c['accion'] == 'BLOCK' for c in cands), cands
    assert any(c['accion'] == 'ALLOW' for c in cands), cands
    assert all(c['tipo_entrada'] == 'NEUTRAL' for c in cands)
    block = [c for c in cands if c['accion'] == 'BLOCK'][0]
    allow = [c for c in cands if c['accion'] == 'ALLOW'][0]
    assert block['n_occurrences'] == 70
    assert allow['n_occurrences'] == 30
    assert any('atr_pct_15m' in c['condition_json'] for c in cands)
    print('  [OK] F7.3: minar_neutral_grids ALLOW+BLOCK (baseline=0.30, filtro v2)')


async def test_minar_breakouts():
    capturado = []

    def _rows():
        rows = []
        for _ in range(60):
            rows.append({'continuacion': 0, 'er_15m': 0.1,
                         'volume_ratio_15m': 0.5, 'bb_width_pct_pct': 20.0,
                         'atr_ratio_1m_15m': 0.5, 'roc_4v_pct': 0.0})
        for _ in range(40):
            rows.append({'continuacion': 1, 'er_15m': 0.6,
                         'volume_ratio_15m': 2.0, 'bb_width_pct_pct': 60.0,
                         'atr_ratio_1m_15m': 1.0, 'roc_4v_pct': 1.5})
        return rows

    async def _cargar(horas=0):
        return _rows()

    async def _guardar(rows):
        capturado.extend(rows)
        return len(rows)

    orig_c, orig_g = ldb.cargar_breakout_labels, ldb.guardar_entry_rules_batch
    ldb.cargar_breakout_labels, ldb.guardar_entry_rules_batch = _cargar, _guardar
    try:
        learner = object.__new__(le.PatternLearner)
        cands = await learner.minar_breakouts()
    finally:
        ldb.cargar_breakout_labels, ldb.guardar_entry_rules_batch = orig_c, orig_g
    assert cands, cands
    assert any(c['accion'] == 'ALLOW' for c in cands)
    assert any(c['accion'] == 'BLOCK' for c in cands)
    allow = [c for c in cands if c['accion'] == 'ALLOW'][0]
    assert allow['lift_vs_baseline'] >= 1.5
    print('  [OK] F7.4: minar_breakouts ALLOW+BLOCK (baseline=0.40)')


async def test_vacio_no_error():
    async def _vacio(*a, **k):
        return []

    orig = (ldb.cargar_fire_labels, ldb.cargar_breakout_labels,
            le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch)
    ldb.cargar_fire_labels = _vacio
    ldb.cargar_breakout_labels = _vacio
    le.cargar_grid_outcomes = _vacio
    ldb.guardar_entry_rules_batch = _vacio
    try:
        learner = object.__new__(le.PatternLearner)
        assert await learner.minar_fires() == []
        assert await learner.minar_neutral_grids() == []
        assert await learner.minar_breakouts() == []
    finally:
        (ldb.cargar_fire_labels, ldb.cargar_breakout_labels,
         le.cargar_grid_outcomes, ldb.guardar_entry_rules_batch) = orig
    print('  [OK] F7: tablas vacias -> [] sin excepciones')


async def test_cache_ram():
    reglas = [
        {'tipo_entrada': 'FIRE', 'symbol': None, 'direction': 'SHORT',
         'regime': 'BULLISH', 'accion': 'BLOCK', 'is_active': 1,
         'condition_json': '{"adx_15m": {"_min": 25}}'},
        {'tipo_entrada': 'FIRE', 'symbol': None, 'direction': 'SHORT',
         'regime': 'BULLISH', 'accion': 'ALLOW', 'is_active': 1,
         'condition_json': '{"adx_15m": {"_max": 25}}'},
        {'tipo_entrada': 'BREAKOUT', 'symbol': 'XUSDT', 'direction': 'SHORT',
         'regime': 'TREND_DOWN', 'accion': 'ALLOW', 'is_active': 1,
         'condition_json': '{}'},
    ]

    async def _activas():
        return reglas

    orig = ldb.cargar_entry_rules_activas
    ldb.cargar_entry_rules_activas = _activas
    try:
        engine = object.__new__(le.LearningEngine)
        engine._entry_rules_cache = {}
        engine.entry_engine = le.EntryRuleEngine()
        await engine._recargar_entry_rules_cache()
    finally:
        ldb.cargar_entry_rules_activas = orig
    assert len(engine._entry_rules_cache) == 2
    assert len(engine._entry_rules_cache[
        ('FIRE', None, 'SHORT', 'BULLISH')]) == 2
    assert len(engine._entry_rules_cache[
        ('BREAKOUT', 'XUSDT', 'SHORT', 'TREND_DOWN')]) == 1
    print('  [OK] F7.5: cache RAM agrupada (tipo, symbol, direction, regime)')


async def test_entry_rules_sqlite():
    await ldb.init_learning_db(':memory:')
    try:
        regla = {'tipo_entrada': 'FIRE', 'accion': 'BLOCK', 'symbol': None,
                 'direction': 'SHORT', 'regime': 'BULLISH',
                 'condition_json': '{"adx_15m": {"_min": 25}}',
                 'score_bonus': 0, 'n_occurrences': 70, 'win_rate': 0.0,
                 'wilson_lower': 0.0, 'lift_vs_baseline': 0.0, 'is_active': 0}
        await ldb.guardar_entry_rules_batch([regla])
        regla2 = dict(regla)
        regla2.update({'n_occurrences': 80, 'win_rate': 0.1})
        await ldb.guardar_entry_rules_batch([regla2])
        todas = await ldb.cargar_entry_rules_todas()
        assert len(todas) == 1, todas
        assert todas[0]['n_occurrences'] == 80, todas
        await ldb.update_entry_rule_estado(todas[0]['id'], True)
        activas = await ldb.cargar_entry_rules_activas()
        assert len(activas) == 1
        print('  [OK] F7.1: entry_rules upsert NULL-global sin duplicados')
    finally:
        await ldb.close_learning_db()


async def main():
    await test_minar_fires()
    await test_minar_neutral_grids()
    await test_minar_breakouts()
    await test_vacio_no_error()
    await test_cache_ram()
    await test_entry_rules_sqlite()
    print('RESULTADO F7: PASS')


if __name__ == '__main__':
    asyncio.run(main())
