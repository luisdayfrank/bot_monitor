"""
coin_config_adapter.py - Plan 3.1
Capa de abstraccion entre el registry JSON y la logica de senales.
Responsabilidades:
  1. Validar tipos y rangos de parametros por moneda
  2. Proveer defaults seguros si falta un campo
  3. Cachear en RAM (O(1) por lookup)
  4. Feature flag: si use_per_coin_params=False, devuelve globals
"""

from config import CONFIG
from typing import Dict, Optional
import json

# Cache estatico: symbol -> config validada
_CACHE: Dict[str, dict] = {}

# Rangos validos por parametro (seguridad)
_RANGOS = {
    'adx_reject': (20.0, 80.0),
    'mfm_umbral': (0.05, 0.50),
    'grid_timeout': (5, 60),
    'volume_threshold': (0.1, 2.0),
}


def _validar(valor, tipo_esperado, rango: Optional[tuple] = None, default=None):
    """Valida un valor: tipo correcto + dentro de rango."""
    if valor is None:
        return default
    try:
        if tipo_esperado == float:
            v = float(valor)
        elif tipo_esperado == int:
            v = int(valor)
        else:
            return default
    except (TypeError, ValueError):
        return default

    if rango and not (rango[0] <= v <= rango[1]):
        print(f"  [ADAPTER] Valor {v} fuera de rango {rango}, usando default {default}")
        return default
    return v


def get_coin_config(symbol: str) -> dict:
    """
    Retorna configuracion validada para una moneda.
    Si use_per_coin_params=False, devuelve globals.
    Cachea resultado en RAM.
    """
    if not CONFIG.use_per_coin_params:
        return {
            'category': 'default',
            'adx_reject': CONFIG.adx_reject_global,
            'mfm_umbral': CONFIG.mfm_umbral_alineacion,
            'grid_timeout': CONFIG.grid_neutral_timeout_min,
            'volume_threshold': 0.6,
        }

    if symbol in _CACHE:
        return _CACHE[symbol]

    raw = CONFIG.coin_registry.get(symbol, {})

    config = {
        'category': str(raw.get('category', 'default'))[:20],
        'adx_reject': _validar(
            raw.get('adx_reject'), float, _RANGOS['adx_reject'], CONFIG.adx_reject_global
        ),
        'mfm_umbral': _validar(
            raw.get('mfm_umbral'), float, _RANGOS['mfm_umbral'], CONFIG.mfm_umbral_alineacion
        ),
        'grid_timeout': _validar(
            raw.get('grid_timeout'), int, _RANGOS['grid_timeout'], CONFIG.grid_neutral_timeout_min
        ),
        'volume_threshold': _validar(
            raw.get('volume_threshold'), float, _RANGOS['volume_threshold'], 0.6
        ),
        'base_threshold': _validar(
            raw.get('base_threshold'), int, (30, 95), 70
        ),
    }

    # FASE 4.2: Fusionar con perfil de categoría
    perfil = CONFIG.perfiles_moneda.get(config['category'], CONFIG.perfiles_moneda['default'])
    config['adx_min'] = perfil['adx_min']
    config['mfm_penalizacion_max'] = perfil['mfm_penalizacion_max']
    config['grid_niveles_max'] = perfil['grid_niveles_max']
    # volume_threshold del registry tiene prioridad; si no existe, usar perfil
    if raw.get('volume_threshold') is None:
        config['volume_threshold'] = perfil['volume_threshold']

    _CACHE[symbol] = config
    return config


def invalidate_cache(symbol: str = None):
    """Invalida cache. Si symbol=None, limpia todo."""
    global _CACHE
    if symbol:
        _CACHE.pop(symbol, None)
    else:
        _CACHE.clear()


def get_category_emoji(category: str) -> str:
    """Emoji representativo por categoria para el heartbeat."""
    return {
        'peso_pesado': '⚖️',
        'capa1_capa2': '🔷',
        'memecoin': '🐸',
        'clasica_privacidad': '🔒',
        'ai_defi_web3': '🤖',
    }.get(category, '❓')
