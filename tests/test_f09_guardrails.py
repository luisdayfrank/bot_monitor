"""F9 - Tests standalone: promocion entry_rules + guardrails.

Ejecutar en el host del bot:  python tests/test_f09_guardrails.py
(Local requiere stubs de numpy/pytz/config/binance/database_v5/grid_engine)
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import learning_db as ldb        # noqa: E402
import learning_engine as le     # noqa: E402


def _regla(rid=1, activa=0, n=50, wilson=0.65, dias=8, accion='BLOCK'):
    creado = (datetime.now(timezone.utc) - timedelta(days=dias))
    return {'id': rid, 'tipo_entrada': 'FIRE', 'accion': accion,
            'symbol': None, 'direction': 'SHORT', 'regime': 'RANGE',
            'condition_json': '{"adx_15m": {"_max": 25}}',
            'n_occurrences': n, 'win_rate': 0.1, 'wilson_lower': wilson,
            'lift_vs_baseline': 0.3, 'is_active': activa,
            'created_at': creado.strftime('%Y-%m-%d %H:%M:%S')}


async def _promover(reglas, cuarentena=0):
    updates = []

    async def _todas():
        return reglas

    async def _cuarentena(horas=0):
        return cuarentena

    async def _estado(rid, activa):
        updates.append((rid, activa))

    orig = (ldb.cargar_entry_rules_todas, ldb.ultimo_guardrail_entry_rules,
            ldb.update_entry_rule_estado)
    ldb.cargar_entry_rules_todas = _todas
    ldb.ultimo_guardrail_entry_rules = _cuarentena
    ldb.update_entry_rule_estado = _estado
    try:
        eng = object.__new__(le.LearningEngine)
        eng.cfg = SimpleNamespace(
            learning_entry_block_promote_max_hi=0.35,
            learning_entry_block_demote_min_hi=0.50)
        await eng._auto_promote_demote_entry_rules()
    finally:
        (ldb.cargar_entry_rules_todas, ldb.ultimo_guardrail_entry_rules,
         ldb.update_entry_rule_estado) = orig
    return updates


def test_created_at_ms():
    ms = le._created_at_ms('2026-11-01 03:00:00')
    assert ms == 1793502000000, ms
    assert le._created_at_ms(None) == 0
    assert le._created_at_ms('basura') == 0
    print('  [OK] F9: created_at TIMESTAMP -> ms (I-F9-5)')


async def test_promote_ok():
    ups = await _promover([_regla(rid=7, activa=0)])
    assert ups == [(7, True)], ups
    print('  [OK] F9.3: SHADOW->ACTIVE (n=50, wilson=0.65, 8d)')


async def test_no_promote_criterios():
    assert await _promover([_regla(rid=1, n=49)]) == []
    assert await _promover([_regla(rid=2, wilson=0.64, accion='ALLOW')]) == []
    assert await _promover([_regla(rid=3, dias=6)]) == []
    print('  [OK] F9.3: no promueve si n<50 / wilson<0.65 / <7d')


async def test_no_promote_cuarentena():
    ups = await _promover([_regla(rid=8, activa=0)], cuarentena=1794000000000)
    assert ups == [], ups
    print('  [OK] F9.3/I-F9-4: cuarentena 7d bloquea promocion')


async def test_demote_active():
    ups = await _promover([_regla(rid=9, activa=1, wilson=0.49, accion='ALLOW')])
    assert ups == [(9, False)], ups
    ups2 = await _promover([_regla(rid=10, activa=1, wilson=0.50, accion='ALLOW')])
    assert ups2 == [], ups2
    print('  [OK] F9.3: ACTIVE->SHADOW si wilson<0.50')


def _eng_guardrail():
    eng = object.__new__(le.LearningEngine)
    eng.cfg = SimpleNamespace(learning_entry_rule_max_block_pct=0.40)
    eng.notifier = None
    eng._last_entry_rule_guard = 0
    return eng


async def test_guardrail_trigger():
    eng = _eng_guardrail()
    calls = {'contar': 0, 'demote': 0, 'event': None, 'reload': 0}

    async def _contar(horas=24):
        calls['contar'] += 1
        return 25, 50

    async def _demote():
        calls['demote'] += 1
        return 3

    async def _event(row):
        calls['event'] = row

    async def _reload():
        calls['reload'] += 1

    orig = (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules,
            ldb.guardar_guardrail_event)
    ldb.contar_entry_rule_blocks = _contar
    ldb.demote_all_entry_rules = _demote
    ldb.guardar_guardrail_event = _event
    eng._recargar_entry_rules_cache = _reload
    try:
        await eng._guardrail_entry_rule_blocks(1000000.0)
    finally:
        (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules,
         ldb.guardar_guardrail_event) = orig
    assert calls['demote'] == 1 and calls['reload'] == 1, calls
    ev = calls['event']
    assert ev and ev['tipo'] == 'ENTRY_RULE_BLOCK_RATE', ev
    assert ev['accion_tomada'] == 'DEMOTE_ALL_ENTRY_RULES', ev
    assert ev['timestamp_ms'] == 1000000000, ev
    print('  [OK] F9.1: 25/50 > 40% -> demote all + evento + reload cache')


async def test_guardrail_no_trigger():
    eng = _eng_guardrail()
    llamados = []

    async def _contar(horas=24):
        return 10, 50

    async def _demote():
        llamados.append(1)
        return 0

    orig = (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules)
    ldb.contar_entry_rule_blocks = _contar
    ldb.demote_all_entry_rules = _demote
    try:
        await eng._guardrail_entry_rule_blocks(2000000.0)
    finally:
        (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules) = orig
    assert not llamados
    print('  [OK] F9.1: 10/50 <= 40% -> sin accion')


async def test_guardrail_min_total_y_throttle():
    eng = _eng_guardrail()
    cont = {'n': 0}

    async def _contar(horas=24):
        cont['n'] += 1
        return 20, 20

    async def _demote():
        return 0

    orig = (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules)
    ldb.contar_entry_rule_blocks = _contar
    ldb.demote_all_entry_rules = _demote
    try:
        await eng._guardrail_entry_rule_blocks(3000000.0)
        await eng._guardrail_entry_rule_blocks(3000060.0)  # <1h -> skip
    finally:
        (ldb.contar_entry_rule_blocks, ldb.demote_all_entry_rules) = orig
    assert cont['n'] == 1, cont
    print('  [OK] F9.1: n_total<=20 no dispara y throttle 1/hora')


async def test_helpers_sqlite():
    await ldb.init_learning_db(':memory:')
    try:
        a = _regla(rid=None, activa=1)
        b = _regla(rid=None, activa=0)
        b['condition_json'] = '{"rsi_15m": {"_min": 70}}'
        for r in (a, b):
            r.pop('id', None)
        await ldb.guardar_entry_rules_batch([a, b])
        todas = await ldb.cargar_entry_rules_todas()
        assert len(todas) == 2, todas
        inactiva = [r for r in todas if not r['is_active']][0]
        await ldb.update_entry_rule_estado(inactiva['id'], True)
        assert len(await ldb.cargar_entry_rules_activas()) == 2
        n = await ldb.demote_all_entry_rules()
        assert n == 2 and await ldb.cargar_entry_rules_activas() == []
    finally:
        await ldb.close_learning_db()
    print('  [OK] F9: helpers cargar_todas/update_estado/demote_all')


async def main():
    test_created_at_ms()
    await test_promote_ok()
    await test_no_promote_criterios()
    await test_no_promote_cuarentena()
    await test_demote_active()
    await test_guardrail_trigger()
    await test_guardrail_no_trigger()
    await test_guardrail_min_total_y_throttle()
    await test_helpers_sqlite()
    print('RESULTADO F9: PASS')


if __name__ == '__main__':
    asyncio.run(main())
