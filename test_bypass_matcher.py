"""
Tests unitarios P2-1: condiciones de bypass (matcher dual + bins + feature_keys).

Ejecutar:  python3 test_bypass_matcher.py
Cubre:
  - formato anidado {feature: {_min, _max}} (producido por _generar_bins_bypass)
  - formato plano heredado {feature_min: X}
  - feature ausente en contexto -> False (conservador)
  - JSON inválido -> False
  - roundtrip bins -> matcher (la condición minada discrimina como el filtro de filas)
  - P2-1b: 'atr_pct_15m' fuera de feature_keys (no es columna de near_miss_labels)
"""
import sys
import types
import json

# Stub de módulos con dependencias del entorno del bot (innecesarios para el matcher)
for _name in ("learning_db", "database_v5"):
    if _name not in sys.modules:
        _m = types.ModuleType(_name)
        if _name == "database_v5":
            async def _noop(*a, **k):
                return []
            _m.cargar_near_miss_seguimientos_finalizados = _noop
        sys.modules[_name] = _m

import learning_engine as le

# Localizar las clases que contienen cada método (sin ejecutar __init__).
# NB: viven en clases DISTINTAS — _generar_bins_bypass en PatternLearner y
# _match_bypass_condition en LearningEngine — justo la desconexión que causaba
# el bug de formato (P2-1c).
def _find(method_name):
    for _n in dir(le):
        _o = getattr(le, _n)
        if isinstance(_o, type) and hasattr(_o, method_name):
            return _o
    raise AssertionError(f"no se encontró {method_name}")


_eng_match = _find("_match_bypass_condition").__new__(_find("_match_bypass_condition"))
_eng_bins = _find("_generar_bins_bypass").__new__(_find("_generar_bins_bypass"))
match = _eng_match._match_bypass_condition
bins = _eng_bins._generar_bins_bypass


def test_nested_min():
    # semántica (de _generar_bins_bypass): {_min: X} = valor >= X ; {_max: Y} = valor <= Y
    cond = json.dumps({"adx_15m": {"_min": 40}})   # exige adx >= 40
    assert match(cond, {"adx_15m": 60}) is True    # cumple el mínimo
    assert match(cond, {"adx_15m": 30}) is False   # por debajo del mínimo
    assert match(cond, {"adx_15m": 40}) is True    # frontera inclusiva
    assert match(cond, {}) is False                # feature ausente -> conservador
    assert match(cond, {"adx_15m": None}) is False


def test_nested_max_and_pair():
    cond = json.dumps({"rsi_15m": {"_max": 30}})
    assert match(cond, {"rsi_15m": 25}) is True
    assert match(cond, {"rsi_15m": 75}) is False
    pair = json.dumps({"adx_15m": {"_min": 40}, "rsi_15m": {"_max": 30}})
    assert match(pair, {"adx_15m": 50, "rsi_15m": 25}) is True
    assert match(pair, {"adx_15m": 50, "rsi_15m": 75}) is False
    assert match(pair, {"adx_15m": 20, "rsi_15m": 25}) is False
    assert match(pair, {"adx_15m": 50}) is False   # falta rsi_15m


def test_flat_legacy():
    cond = json.dumps({"adx_15m_min": 40})         # formato heredado: adx >= 40
    assert match(cond, {"adx_15m": 30}) is False
    assert match(cond, {"adx_15m": 60}) is True
    cond = json.dumps({"adx_15m_max": 40})         # formato heredado: adx <= 40
    assert match(cond, {"adx_15m": 60}) is False
    assert match(cond, {"adx_15m": 30}) is True


def test_invalid_and_empty():
    assert match("no-es-json", {"adx_15m": 1}) is False
    assert match("{}", {"adx_15m": 1}) is True     # sin restricciones: matchea todo (documentado)


def _oracle(cond, f):
    """Semántica de referencia (independiente de la implementación) para formato anidado."""
    for k, th in cond.items():
        v = f.get(k)
        if v is None:
            return False
        for op, val in th.items():
            if op == "_min" and v < val:
                return False
            if op == "_max" and v > val:
                return False
    return True


def test_bins_roundtrip():
    # filas sintéticas con columnas de near_miss_labels (features con varianza)
    filas = [{"adx_15m": 20 + (i * 3) % 55, "rsi_15m": 20 + (i * 7) % 55,
              "ema200_dist_pct": -2.0 + (i % 20) * 0.2} for i in range(40)]
    resultados = bins(filas)
    assert resultados, "bins no produjo condiciones"
    for cond_json, filas_cond in resultados:
        cond = json.loads(cond_json)
        assert isinstance(list(cond.values())[0], dict), f"formato inesperado: {cond}"
        # 1) consistencia: toda fila del subconjunto debe cumplir la condición
        for f in filas_cond:
            assert match(cond_json, f) is True, f"matcher rechaza fila propia: {cond_json} vs {f}"
        # 2) oráculo: el matcher debe aplicar los umbrales EXACTAMENTE como la
        #    semántica de referencia (este test fallaba con el matcher pre-fix,
        #    que ignoraba los umbrales y devolvía True siempre)
        esperado = [f for f in filas if _oracle(cond, f)]
        obtenido = [f for f in filas if match(cond_json, f)]
        assert obtenido == esperado, f"matcher diverge del oráculo en {cond_json}"


def test_atr_fuera_de_feature_keys():
    # P2-1b: filas SOLO con atr_pct_15m no deben producir condiciones
    filas = [{"atr_pct_15m": v / 100.0} for v in range(1, 40)]
    assert bins(filas) == [], "atr_pct_15m sigue en feature_keys"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"  ✅ {t.__name__}")
    print(f"\n{len(tests)} tests OK — matcher dual + bins + feature_keys verificados")
