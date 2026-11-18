"""
tests/test_f12_semana4_threshold.py - F12.15 (Plan v7.4, Semana 4)

1) Default learning_refusal_threshold = 0.52 (placeholder hasta F14.9, C19).
2) Rango del adapter endurecido a [0.50, 0.80].
3) Override 0.35 cae al default 0.52; 0.60 se respeta.
4) validar_thresholds_registry reporta solo los overrides invalidos.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
import coin_config_adapter as cc  # noqa: E402


def _test_default():
    assert abs(config.CONFIG.learning_refusal_threshold - 0.52) < 1e-9, \
        config.CONFIG.learning_refusal_threshold
    print('  [OK] F12.15: default = 0.52')


def _test_rango():
    lo, hi = cc._RANGOS['learning_refusal_threshold']
    assert lo >= 0.50 and hi == 0.80, (lo, hi)
    assert cc.THRESHOLD_REFUSAL_MIN == 0.50
    print('  [OK] F12.15: rango adapter = [0.50, 0.80]')


def _test_validar():
    rango = cc._RANGOS['learning_refusal_threshold']
    bajo = cc._validar(0.35, float, rango, 0.52)
    ok = cc._validar(0.60, float, rango, 0.52)
    assert abs(bajo - 0.52) < 1e-9, bajo
    assert abs(ok - 0.60) < 1e-9, ok
    print('  [OK] F12.15: 0.35 -> 0.52 (fallback); 0.60 -> 0.60')


def _test_registry():
    inv = cc.validar_thresholds_registry({
        'BADUSDT': {'learning_refusal_threshold': 0.35},
        'OKUSDT': {'learning_refusal_threshold': 0.60},
        'NOOVR': {'active': 1},
        'TXT': {'learning_refusal_threshold': 'alto'},
    })
    syms = sorted(s for s, _ in inv)
    assert syms == ['BADUSDT', 'TXT'], inv
    print('  [OK] F12.15: arranque reporta BADUSDT y TXT, ignora los validos')


def _test_shadow_usa_umbral():
    src = (Path(__file__).resolve().parent.parent / 'signals_v5.py').read_text(
        encoding='utf-8')
    assert "_prob < 0.35" not in src, 'SHADOW conserva el umbral hardcodeado 0.35'
    assert "_prob < _umbral_sh" in src, 'SHADOW no usa el umbral adapter/config'
    print('  [OK] F12.15: preview SHADOW usa el mismo umbral que ACTIVE')


def main():
    _test_default()
    _test_rango()
    _test_validar()
    _test_registry()
    _test_shadow_usa_umbral()
    print('F12 semana 4 threshold: PASS')


if __name__ == '__main__':
    main()
