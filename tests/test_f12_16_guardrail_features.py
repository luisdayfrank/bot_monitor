"""
tests/test_f12_16_guardrail_features.py — F12.16 (Plan v7.4)

Guardrail: ninguna feature de diagnóstico de grid puede alimentar
clasificadores/minería. Cubre MINING_FEATURES, DriftDetector.FEATURES_CLAVE
y (cuando exista, F14) FEATURE_REGISTRY.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import learning_engine as le  # noqa: E402

FORBIDDEN = ('grid_neutral_sim', 'grid_', 'virtual_grid', 'touch_count',
             'bounce_rate', 'bounce_magnitude', 'cross_speed')


def _check(nombre, features):
    malos = [f for f in features if any(tok in str(f) for tok in FORBIDDEN)]
    assert not malos, f'{nombre}: features de grid prohibidas -> {malos}'
    print(f'  [OK] F12.16: {nombre} limpio ({len(features)} features)')


def main():
    _check('MINING_FEATURES', le.MINING_FEATURES)
    _check('DriftDetector.FEATURES_CLAVE', le.DriftDetector.FEATURES_CLAVE)
    registry = getattr(le, 'FEATURE_REGISTRY', None)
    if registry:
        for version, feats in registry.items():
            _check(f'FEATURE_REGISTRY[{version}]', feats)
    else:
        print('  [SKIP] FEATURE_REGISTRY aún no existe (llega en F14)')
    print('F12.16 guardrail: PASS')


if __name__ == '__main__':
    main()
