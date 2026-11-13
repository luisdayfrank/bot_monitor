"""F8 - Tests standalone: EntryRuleEngine + contexto + serving NEUTRAL.

Ejecutar en el host del bot:  python tests/test_f08_entry_rules.py
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


def _regla(tipo='FIRE', accion='BLOCK', symbol=None, direction='SHORT',
           regime='TREND_DOWN', cond='{"adx_15m": {"_min": 25}}',
           is_active=1, bonus=0):
    return {'tipo_entrada': tipo, 'accion': accion, 'symbol': symbol,
            'direction': direction, 'regime': regime,
            'condition_json': cond, 'score_bonus': bonus,
            'n_occurrences': 50, 'win_rate': 0.1, 'wilson_lower': 0.05,
            'lift_vs_baseline': 0.3, 'is_active': is_active}


def test_cargar_filtra_inactivas():
    eng = le.EntryRuleEngine()
    eng.cargar([_regla(), _regla(is_active=0)])
    assert len(eng._cache) == 1
    assert len(next(iter(eng._cache.values()))) == 1
    print('  [OK] F8.1: cargar() filtra is_active=0')


def test_cache_vacia_permisiva():
    eng = le.EntryRuleEngine()
    out = eng.evaluar('FIRE', SYM, 'SHORT', 'TREND_DOWN',
                      {'adx_15m': 30}, 80)
    assert out == (False, '', 0), out
    print('  [OK] F8.1: cache vacia -> default permisivo (D-F8-1)')


def test_precedencia_block_sobre_allow():
    eng = le.EntryRuleEngine()
    eng.cargar([
        _regla(accion='ALLOW', cond='{"adx_15m": {"_min": 10}}'),
        _regla(accion='BLOCK', cond='{"adx_15m": {"_min": 25}}'),
    ])
    block, motivo, _ = eng.evaluar('FIRE', SYM, 'SHORT', 'TREND_DOWN',
                                   {'adx_15m': 30}, 80)
    assert block is True and 'entry_rule_FIRE' in motivo, (block, motivo)
    print('  [OK] F8.1: BLOCK gana a ALLOW (precedencia I100)')


def test_require_score():
    eng = le.EntryRuleEngine()
    eng.cargar([_regla(accion='REQUIRE_SCORE', cond='{}')])
    lo, _, _ = eng.evaluar('FIRE', SYM, 'SHORT', 'TREND_DOWN', {}, 60)
    hi, _, _ = eng.evaluar('FIRE', SYM, 'SHORT', 'TREND_DOWN', {}, 80)
    assert lo is True and hi is False, (lo, hi)
    print('  [OK] F8.1: REQUIRE_SCORE (score<70 bloquea, >=70 pasa)')


def test_fallback_dimensiones():
    eng = le.EntryRuleEngine()
    eng.cargar([
        _regla(tipo='BREAKOUT', symbol=None, direction=None, regime=None,
               cond='{"er_15m": {"_min": 0.4}}'),
        _regla(tipo='NEUTRAL', symbol=None, direction='NEUTRAL', regime=None,
               cond='{"atr_pct_15m": {"_max": 1.0}}'),
        _regla(tipo='FIRE', symbol=None, direction='LONG', regime='TREND_UP',
               cond='{"rsi_15m": {"_max": 30}}'),
    ])
    b1, _, _ = eng.evaluar('BREAKOUT', SYM, 'LONG', 'TREND_UP',
                           {'er_15m': 0.5}, 80)
    b2, _, _ = eng.evaluar('NEUTRAL', SYM, 'NEUTRAL', None,
                           {'atr_pct_15m': 0.5}, 0)
    b3, _, _ = eng.evaluar('FIRE', SYM, 'LONG', 'TREND_UP',
                           {'rsi_15m': 20}, 80)
    assert b1 and b2 and b3, (b1, b2, b3)
    print('  [OK] F8.1: fallback None en 3 dimensiones (I95)')


def test_match_formatos():
    m = le.EntryRuleEngine._match_condition
    assert m('{"adx_15m": {"_min": 25, "_max": 50}}', {'adx_15m': 30})
    assert not m('{"adx_15m": {"_min": 25}}', {})
    assert m('{"adx_15m_min": 25}', {'adx_15m': 30})
    assert not m('{"adx_15m_max": 25}', {'adx_15m': 30})
    print('  [OK] F8.1: matcher anidado + plano, feature ausente -> False')


async def test_recargar_cache_wiring():
    reglas = [_regla(accion='BLOCK'), _regla(accion='ALLOW', cond='{}')]

    async def _activas():
        return reglas

    orig = ldb.cargar_entry_rules_activas
    ldb.cargar_entry_rules_activas = _activas
    try:
        eng = object.__new__(le.LearningEngine)
        eng._entry_rules_cache = {}
        eng.entry_engine = le.EntryRuleEngine()
        await eng._recargar_entry_rules_cache()
    finally:
        ldb.cargar_entry_rules_activas = orig
    assert len(eng._entry_rules_cache) == 1
    assert len(eng.entry_engine._cache) == 1
    assert len(next(iter(eng.entry_engine._cache.values()))) == 2
    print('  [OK] F8.1: _recargar_entry_rules_cache alimenta entry_engine')


def _nuevo_sg(eng):
    sg = SignalGenerator(asyncio.Queue(), asyncio.Queue())
    sg.learning_engine = SimpleNamespace(
        entry_engine=eng,
        collector=SimpleNamespace(
            ultima_fila=lambda s: {'bb_width_pct_pct': 55.0,
                                   'er_15m': 0.5}),
    )
    return sg


def test_contexto_entry_rules():
    sg = _nuevo_sg(le.EntryRuleEngine())
    i15 = {'adx': 15.0, 'rsi': 50.0, 'atr': 0.5, 'close': 100.0,
           'ema200_15m': 95.0, 'volume': 20.0, 'volume_sma20': 10.0}
    ctx = sg._contexto_entry_rules(SYM, i15)
    assert ctx['adx_15m'] == 15.0
    assert ctx['rsi_15m'] == 50.0
    assert abs(ctx['atr_pct_15m'] - 0.5) < 1e-9
    assert abs(ctx['ema200_dist_pct'] - (5.0 / 95.0 * 100.0)) < 1e-9
    assert abs(ctx['volume_ratio_15m'] - 2.0) < 1e-9
    assert abs(ctx['volume_ratio'] - 2.0) < 1e-9
    print('  [OK] F8-I97: contexto canonico (adx/rsi/atr/ema200/vol)')


def test_neutral_serving_active_y_shadow():
    mode0 = getattr(sgv.CONFIG, 'learning_apply_mode', 'OFF')
    try:
        eng = le.EntryRuleEngine()
        eng.cargar([_regla(tipo='NEUTRAL', direction='NEUTRAL',
                           regime=None,
                           cond='{"adx_15m": {"_max": 20}}')])
        sg = _nuevo_sg(eng)
        i15 = {'adx': 15.0, 'rsi': 50.0, 'atr': 0.5, 'close': 100.0,
               'ema50_15m': 100.0, 'macd_hist': 0.0, 'ema200_15m': 95.0,
               'timestamp': 1794000000000}
        sgv.CONFIG.learning_apply_mode = 'ACTIVE'
        ok, razon = sg.evaluar_grid_neutro(SYM, i15)
        assert ok is False and 'entry_rule_bloquea' in razon, (ok, razon)
        sgv.CONFIG.learning_apply_mode = 'SHADOW'
        ok2, _ = sg.evaluar_grid_neutro(SYM, i15)
        assert ok2 is True
    finally:
        sgv.CONFIG.learning_apply_mode = mode0
    print('  [OK] F8.3: NEUTRAL ACTIVE bloquea / SHADOW solo observa')


def test_learning_apply_mode_off():
    """A11: con learning_apply_mode=OFF F8 es inerte (no evalua/bloquea)."""
    mode0 = getattr(sgv.CONFIG, 'learning_apply_mode', 'OFF')
    try:
        eng = le.EntryRuleEngine()
        eng.cargar([_regla(tipo='NEUTRAL', direction='NEUTRAL',
                           regime=None,
                           cond='{"adx_15m": {"_max": 20}}')])
        sg = _nuevo_sg(eng)
        i15 = {'adx': 15.0, 'rsi': 50.0, 'atr': 0.5, 'close': 100.0,
               'ema50_15m': 100.0, 'macd_hist': 0.0, 'ema200_15m': 95.0,
               'timestamp': 1794000000000}
        sgv.CONFIG.learning_apply_mode = 'OFF'
        ok, _ = sg.evaluar_grid_neutro(SYM, i15)
        assert ok is True
    finally:
        sgv.CONFIG.learning_apply_mode = mode0
    print('  [OK] F8-A11: learning_apply_mode=OFF -> F8 inerte')


async def main():
    test_cargar_filtra_inactivas()
    test_cache_vacia_permisiva()
    test_precedencia_block_sobre_allow()
    test_require_score()
    test_fallback_dimensiones()
    test_match_formatos()
    await test_recargar_cache_wiring()
    test_contexto_entry_rules()
    test_neutral_serving_active_y_shadow()
    test_learning_apply_mode_off()
    print('RESULTADO F8: PASS')


if __name__ == '__main__':
    asyncio.run(main())
