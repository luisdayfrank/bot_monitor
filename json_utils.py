"""
json_utils — dumps JSON seguro con tipos numpy (P3-2).

Historia: audit_logger.py aplicaba un monkey-patch GLOBAL de json.dumps al
importarse (efecto secundario accidental). 45 call sites en 6 módulos, más
módulos no auditados (main.py, notifier_v5.py), heredaban ese comportamiento.

P3-2 (fase 1 — centralización): el patch vive AHORA aquí, explícito e idempotente.
- Uso recomendado (call sites nuevos): from json_utils import dumps -> dumps(obj)
- Compatibilidad: install_global_patch() conserva el comportamiento global para
  los call sites no migrados. La llamada vive en audit_logger.py y se eliminará
  cuando main.py / notifier_v5.py se auditen (deuda de 1 línea documentada).
"""
import json

try:
    import numpy as _np
except Exception:  # numpy opcional
    _np = None


def _default(o):
    if _np is not None:
        if isinstance(o, (_np.bool_, bool)):
            return bool(o)
        if isinstance(o, _np.integer):
            return int(o)
        if isinstance(o, _np.floating):
            if _np.isnan(o) or _np.isinf(o):
                return None
            return float(o)
        if isinstance(o, _np.ndarray):
            return o.tolist()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def dumps(obj, **kwargs):
    """json.dumps con default= numpy-safe (NaN/Inf -> None)."""
    kwargs.setdefault('default', _default)
    return json.dumps(obj, **kwargs)


_PATCH_INSTALLED = False


def install_global_patch():
    """Compat: instala dumps() como json.dumps global. Idempotente."""
    global _PATCH_INSTALLED
    if _PATCH_INSTALLED or getattr(json.dumps, '_json_utils_patched', False):
        _PATCH_INSTALLED = True
        return
    _original = json.dumps

    def _dumps_with_numpy(obj, **kwargs):
        kwargs.setdefault('default', _default)
        return _original(obj, **kwargs)

    _dumps_with_numpy._json_utils_patched = True
    json.dumps = _dumps_with_numpy
    _PATCH_INSTALLED = True
