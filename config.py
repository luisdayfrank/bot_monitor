from pydantic import BaseModel, Field
from typing import List
import os
import json

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# REGISTRO DE MONEDAS â€” JSON separado del config
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
def load_coin_registry():
    """Carga el registro de monedas desde coins_registry.json."""
    registry_path = "coins_registry.json"
    default_symbols = ["TRXUSDT", "ZECUSDT", "WIFUSDT", "NEARUSDT", "MEMEUSDT",
                      "APTUSDT", "XLMUSDT", "ADAUSDT", "DOGEUSDT", "INJUSDT"]

    if not os.path.exists(registry_path):
        registry = {s: {"active": 1, "category": "default"} for s in default_symbols}
        with open(registry_path, 'w', encoding='utf-8') as f:
            json.dump(registry, f, indent=2, ensure_ascii=False)
        return default_symbols, registry

    try:
        with open(registry_path, 'r', encoding='utf-8') as f:
            registry = json.load(f)

        active_symbols = [sym for sym, data in registry.items() if data.get("active", 0) == 1]

        if not active_symbols:
            print("âš ï¸ Ninguna moneda activa en registry. Usando defaults.")
            return default_symbols, registry

        return active_symbols, registry

    except Exception as e:
        print(f"âŒ Error leyendo coins_registry.json: {e}. Usando defaults.")
        return default_symbols, {}

_ACTIVE_SYMBOLS, _COIN_REGISTRY = load_coin_registry()

class Config(BaseModel):
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # F1.1: Feature flag para I/O de precios en vivo (evita asfixia de la DB)
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    guardar_precios_vivo: bool = False   # True = persiste precios en DB (costoso I/O)

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # F1.2: Feature flag de arquitectura adaptativa (MOVIDO DENTRO de la clase)
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    use_per_coin_params: bool = True   # False = usa globals, ignora registry por moneda

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # LEARNING ENGINE â€” todos los flags arrancan OFF / SHADOW (Fase 0)
    # Ver: informe_revision_learning_engine.md (correcciones C-1..C-6, Â§3A-Â§3I)
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    learning_enabled: bool = True              # master switch (Fase 0)
    learning_db_path: str = "crypto_learning.db"
    learning_apply_mode: str = "OFF"            # OFF | SHADOW | ACTIVE (refusal-first)
    learning_exit_mode: str = "OFF"             # OFF | SHADOW | ACTIVE (Fase 8)
    learning_regime_tracking: bool = True       # Fase 6

    # ParÃ¡metros del learner
    learning_fee_rate: float = 0.0005           # fee por lado (0.05%) â€” todo neto Â§3C
    learning_label_intervalo_min: int = 10
    learning_learn_hora_local: int = 3          # corrida diaria del learner
    learning_ventana_short_dias: int = 30
    learning_ventana_medium_dias: int = 180
    learning_ventana_long_dias: int = 1825      # 5 aÃ±os
    learning_decay_half_life_dias: int = 125    # peso = 0.5 ** (edad/half_life) Â§3G
    learning_min_snapshots_perfil: int = 300
    learning_min_labels_para_ml: int = 5000
    learning_refusal_threshold: float = 0.35    # override por moneda vÃ­a registry

    # Rigor estadÃ­stico de la minerÃ­a (correcciÃ³n Â§3A)
    learning_min_lift: float = 1.5
    learning_min_lift_lower: float = 1.2        # lÃ­mite inferior Wilson 95%
    learning_min_n: int = 40
    learning_fdr_q: float = 0.10                # Benjamini-Hochberg
    learning_regime_min_velas: int = 4          # histÃ©resis del clasificador Â§3F

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # LEARNING ENGINE â€” GUARDRAILS FINANCIEROS (Anexo A Â§4: auto-rollback)
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    learning_auto_rollback_enabled: bool = True
    learning_max_reject_ratio_daily: float = 0.60    # >60% FIREs rechazados/24h â†’ SHADOW
    learning_max_reject_ratio_hourly: float = 0.80   # >80% en 1h â†’ SHADOW + alerta
    learning_min_precision_delta: float = -0.10      # precisiÃ³n de rechazos cae >10% â†’ SHADOW
    learning_drift_psi_threshold: float = 0.20       # PSI > 0.20 â†’ solo alerta
    learning_alerta_telegram: bool = True
    learning_exit_captura_pct_min: float = 30.0      # Fase 8: captura mÃ­nima de MFE

    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # CONFIGURACIÃ“N DE ZONA HORARIA
    # Formatos vÃ¡lidos: "America/Caracas", "America/Bogota", "America/Mexico_City",
    # "America/Argentina/Buenos_Aires", "Europe/Madrid", etc.
    # Lista completa: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    timezone: str = "America/Caracas"  # UTC-4, sin horario de verano
    grid_direccional_timeout_min: int = 120  # 2 horas default
    # --- PLAN 3.1: Parametros globales (fallbacks) ---
    adx_reject_global: float = 55.0          # Fallback si no hay registry

    # Monedas a monitorear (cargadas dinÃ¡micamente desde coins_registry.json)
    symbols: List[str] = Field(
        default=_ACTIVE_SYMBOLS,
        max_length=50  # Aumentado a 50 para soportar el registro completo
    )

    # Registro completo de monedas (para referencia del dashboard)
    coin_registry: dict = Field(default=_COIN_REGISTRY, exclude=True)

    # Timeframes
    tf_micro: str = "1m"       # Gatillo sniper
    tf_primary: str = "15m"    # Filtro macro / timing
    tf_macro: str = "4h"       # Contexto de sesiÃ³n
    tf_live: str = "markPrice@1s"

    # Buffers
    max_velas_1m: int = 350    # EMA_300 + margen de seguridad
    max_velas_15m: int = 300
    max_velas_4h: int = 200

    # â”€â”€â”€ Filtro Macro (15m) â”€â”€â”€
    adx_ideal: tuple = (25, 35)
    adx_reject: float = 55.0

    # FASE 1.1: ADX mÃ­nimo adaptativo por categorÃ­a de moneda
    adx_min_trend_global: float = 14.0
    adx_min_trend_memecoin: float = 12.0
    adx_min_trend_major: float = 15.0

    # FASE 1.2: RSI direccional reformulado (solo 2 umbrales extremos)
    rsi_extremo_short_min: float = 25.0   # Rechazar SHORT si RSI < 25
    rsi_extremo_long_max: float = 75.0    # Rechazar LONG si RSI > 75

    # â”€â”€â”€ Gatillo Micro (1m) â”€â”€â”€
    rsi_micro_length: int = 7
    rsi_micro_short_trigger: float = 75.0
    rsi_micro_long_trigger: float = 25.0

    # EMA equivalente: EMA_20 en 15m == EMA_300 en 1m
    ema_micro_period: int = 300

    # ConfirmaciÃ³n de mecha (rechazo)
    wick_min_pct: float = 0.05

    # ATR / MACD / Volumen (15m)
    atr_min_pct: float = 0.15
    atr_max_pct: float = 2.0
    macd_stable_threshold: float = 0.3
    macd_danger_threshold: float = 0.7
    volume_min_ratio: float = 1.0

    # HistÃ©resis y cooldown
    hysteresis_velas: int = 3
    cooldown_15m_velas: int = 1

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 5.1: PAUSA DE INACTIVIDAD (antes hardcodeado en signals_v4.py)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Horas de score bajo antes de auto-pausar una moneda
    pausa_inactividad_horas: float = 1.0    # 1 hora

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 3: PARÃMETROS DE SEGURIDAD DEL GRID
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # Grid params base
    grid_min_grids: int = 10             # CAMBIO: antes 5. Alineado con grid_niveles_min mÃ¡s bajo (memecoin=10)
    grid_max_grids_hard: int = 50        # Cap absoluto para evitar saturaciÃ³n API
    # V6.0 Punto 3c: fees maker con descuento BNB (0.02%) + slippage realista.
    # Si NO hay BNB para el descuento del 25%, usar grid_fee_rate = 0.00025.
    grid_fee_rate: float = 0.0002
    grid_slippage: float = 0.0003
    grid_default_capital: float = 10.0
    grid_default_leverage: int = 15      # Leverage sugerido en senales (rango adaptativo 5-15) â†’ ahora 10

    # Nocional mÃ­nimo por orden (Binance Futures)
    grid_notional_min: float = 10.0

    # Multiplicador de rango del grid (atr * multiplicador = rango total)
    # Rango simÃ©trico: price Â± (atr * multiplicador / 2)
    grid_rango_mult_min: float = 4.0     # CAMBIO: antes 2.0 â†’ ahora 4.0
    grid_rango_mult_max: float = 10.0     # CAMBIO: antes 6.0 â†’ ahora 10.0

    # Breakeven: step_pct debe ser al menos este mÃºltiplo para ser rentable
    grid_breakeven_mult: float = 1.05     # V6.0: antes 1.15 â†’ 1.05 (fees maker reales)

    # Auto-compresiÃ³n: si step_pct < breakeven * mult, reducir grids
    grid_auto_compress: bool = True

    # Densidad mÃ¡xima de grids: mÃ­nimo 0.5 ATR entre grids
    grid_min_dist_atr: float = 0.15       # CAMBIO: antes 0.5 â†’ ahora 0.15

    # Percentil para truncar ATR (95 = corta outliers, respeta volatilidad normal)
    grid_atr_percentil: float = 90.0     # CAMBIO: antes 95.0 â†’ ahora 90.0

    # PosiciÃ³n en rango: alerta si >80% o <20% (grid mal posicionado)
    grid_posicion_alerta_max: float = 0.80
    grid_posicion_alerta_min: float = 0.20

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 3: CIRCUIT BREAKER
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # NÃºmero de disparos consecutivos en pÃ©rdida antes de activar circuit breaker
    circuit_breaker_disparos: int = 3

    # Tiempo de pausa en segundos cuando se activa el circuit breaker
    circuit_breaker_pausa_seg: int = 1800  # 30 minutos

    # ReducciÃ³n de capital tras activar circuit breaker (0.5 = 50%)
    circuit_breaker_reduccion_capital: float = 0.5

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 3: HISTÃ‰RESIS SUAVIZADA
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # En vez de resetear a 0 en una vela contraria, decrementar progresivamente
    hysteresis_suavizada: bool = True
    hysteresis_decremento: int = 1       # Velas de confirmaciÃ³n que se pierden por vela contraria

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 5.2: MFM (Money Flow Multiplier) â€” VOLUMEN INTELIGENTE
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Umbral de MFM para considerar volumen alineado con direcciÃ³n
    # MFM > 0.2  â†’ presiÃ³n alcista (volumen comprador dominante)
    # MFM < -0.2 â†’ presiÃ³n bajista (volumen vendedor dominante)
    mfm_umbral_alineacion: float = 0.15
    # Puntos que resta el MFM cuando contradice la direcciÃ³n
    mfm_penalizacion_contrario: int = 3
    # Puntos que suma el MFM cuando alinea con direcciÃ³n (reemplaza bonus volumen genÃ©rico)
    mfm_bonus_alineado: int = 15

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 5.3: UMBRAL DINÃMICO VÃA ATR
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Ventana de velas 15m para calcular percentiles del ATR
    atr_percentil_ventana: int = 100
    # Score mÃ­nimo en consolidaciÃ³n (ATR bajo = mÃ¡s selectivo)
    score_min_consolidacion: int = 55
    # Score mÃ­nimo en expansiÃ³n (ATR alto = mÃ¡s permisivo)
    score_min_expansion: int = 60

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 5.4: VOLUMEN DINÃMICO CONDICIONAL â€” Bypass por convicciÃ³n alta
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Puntos EXTRA sobre el umbral necesarios para activar bypass por convicciÃ³n alta
    volumen_bypass_score_extra: int = 5
    # ADX mÃ­nimo para que el bypass por convicciÃ³n alta sea vÃ¡lido
    volumen_bypass_adx_min: float = 20.0
    # Percentil del ATR diario para bypass por volatilidad (75 = percentil 75)
    volumen_bypass_atr_percentil: float = 75.0

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # V5.7 FASE 1: SEGUIMIENTO VIRTUAL POST NEAR-MISS
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Horas de seguimiento virtual post near-miss
    auditoria_near_miss_horas: int = 2
    # Intervalo de muestras del seguimiento virtual (minutos)
    near_miss_tracking_intervalo_min: int = 5

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # V5.7 FASE 5: GRID NEUTRAL (desactivado por defecto, solo Fases 1-4 activas)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # Activar estrategia de grid en mercados neutrales
    grid_neutral_enabled: bool = True  # Toggle global, sin confirmaciÃ³n manual
    # Tiempo mÃ¡ximo en estado NEUTRAL_GRID antes de aborto automÃ¡tico
    grid_neutral_timeout_min: int = 310          # Minutos mÃ¡x en NEUTRAL_GRID (310 min â‰ˆ 5.2h)
    # Aborto si ADX sube +3 sobre umbral de entrada (V6.0 Punto 3b: era +5)
    grid_neutral_aborto_adx_delta: float = 3.0
    # Aborto si precio se aleja >1.0% de EMA50 (V6.0 Punto 3b: era 1.8)
    grid_neutral_aborto_precio_pct: float = 1.0
    # V6.0 Punto 3d: hard stop por precio adverso respecto al precio de entrada
    # ponderado de las posiciones abiertas (mÃ©trica distinta al aborto por EMA50).
    # Leverage-independiente: mide movimiento de PRECIO, no de PnL.
    # 0.8% < 1.0% del aborto macro => actÃºa como red de seguridad previa.
    grid_neutral_max_adverse_price_pct: float = 0.8
    # ADX mÃ¡ximo para considerar mercado neutral (sin tendencia fuerte)
    grid_neutral_adx_max: float = 25.0           # CAMBIO: antes 22.0 â†’ ahora 25.0
    # RSI rango para grid neutral (no extremos)
    grid_neutral_rsi_min: float = 30.0           # CAMBIO: antes 35.0 â†’ ahora 30.0
    grid_neutral_rsi_max: float = 70.0           # CAMBIO: antes 65.0 â†’ ahora 70.0
    # Percentil ATR para grid neutral (30-70 = volatilidad moderada)
    grid_neutral_atr_percentil_min: float = 30.0
    grid_neutral_atr_percentil_max: float = 70.0

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 4.5: MODO AUDITORÃA EXTERNA
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # Activa la recopilaciÃ³n de datos para auditorÃ­a manual externa
    # Cuando estÃ¡ activo, el bot guarda eventos y envÃ­a reporte diario
    # como archivo .json por Telegram. No afecta el funcionamiento normal.
    modo_auditoria: bool = True

    # Hora local para enviar el reporte diario (formato HH:MM en zona horaria del bot)
    # Ejemplo: "23:55" = 23:55 hora de Caracas (UTC-4)
    # Nota: Las velas de Binance cierran en UTC, pero el reporte se envÃ­a
    # en tu hora local para conveniencia
    auditoria_hora_reporte: str = "23:55"

    # CuÃ¡ntas horas post-disparo trackear para el seguimiento
    auditoria_horas_seguimiento: int = 8

    # Intervalo de muestras post-disparo (minutos)
    auditoria_muestras_intervalo_min: int = 5

    # CuÃ¡ntas horas de muestras detalladas (despuÃ©s solo velas 15m)
    auditoria_muestras_detalladas_horas: int = 2

    # Notificaciones
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # â”€â”€â”€ Base de datos MySQL / MariaDB â”€â”€â”€
    # ConexiÃ³n local por defecto (MariaDB/MySQL en tu PC). Configurable por
    # variables de entorno para no exponer credenciales en el cÃ³digo.
    db_host: str = os.getenv("DB_HOST", "database-1.cnqsq0e6cf6z.eu-west-1.rds.amazonaws.com")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_user: str = os.getenv("DB_USER", "admin")
    db_password: str = os.getenv("DB_PASSWORD", "1605584496")
    db_name: str = os.getenv("DB_NAME", "crypto_monitor")
    db_connect_timeout: int = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

    # Carpeta donde se exportan los reportes JSON de auditorÃ­a
    audit_export_dir: str = os.getenv("AUDIT_EXPORT_DIR", "auditoria")

    # Binance
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")


    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 4.5: HEARTBEAT DE VALIDACIÃ“N (debug pasivo)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # Activa un log cada N minutos con el estado interno de todas las monedas.
    # Ãštil para validar correcciones sin esperar al reporte diario.
    # No afecta el funcionamiento del bot. Solo lectura, cero interferencia.
    heartbeat_debug: bool = True
    heartbeat_intervalo_min: int = 15  # Minutos entre heartbeats

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # V5.9.2: PARÃMETROS DE SIMULACIÃ“N VIRTUAL GRID NEUTRAL
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

    # V5.9.2 MEJORA #1: Timeout por posiciÃ³n abierta
    # Cerrar posiciÃ³n si lleva N minutos abierta (evita posiciones eternas)
    # V6.0 Punto 3a: elevado a 24h (era 25 min) â€” el grid neutral necesita
    # dar tiempo al ciclo entradaâ†’TP en mercado lateral.
    grid_neutral_posicion_timeout_min: int = 1440
    grid_neutral_sin_ticks_timeout_min: int = 10  # FASE 2.4: Sin ticks (10 min)

    # V5.9.2 MEJORA #2: Slippage absoluto de Ãºltimo recurso
    # Si el kill switch falla tras 10 intentos, market order con max 2% slippage
    grid_neutral_kill_switch_slippage_absoluto: float = 0.02  # 2%
    grid_neutral_kill_switch_slippage_max: float = 0.005  # 0.5% normal
    grid_neutral_kill_switch_intervalo_seg: int = 3  # 3s entre reintentos
    grid_neutral_kill_switch_max_intentos: int = 10  # 10 intentos = 30s max

    # V5.9.2 MEJORA #3: Modo tolerancia API REST para testnet
    # Multiplicador de timeout en API REST testnet (2x = mÃ¡s tolerante)
    grid_neutral_testnet_api_timeout_mult: float = 2.0
    grid_neutral_testnet_api_retry_max: int = 5  # Reintentos en testnet
    grid_neutral_testnet_api_retry_delay_base: float = 2.0  # Segundos base

    # V5.9.2 MEJORA #4: GestiÃ³n de Ã³rdenes parcialmente llenadas
    # Ajustar orden opuesta segÃºn filled_qty de la primera
    grid_neutral_gestion_parcial: bool = True

    # V5.9.2 MEJORA #6: Cleaner de grids huÃ©rfanos
    # Marcar ABORTADO si grid ACTIVO sin actividad durante > N horas
    grid_neutral_huerfano_timeout_horas: int = 4

    # V5.9.2 MEJORA #6: Heartbeat de simulaciÃ³n (cada 15 min)
    grid_neutral_heartbeat_intervalo_min: int = 15

    # V5.9.2 MEJORA #9: Circuit Breaker afecta Grid Neutral
    # Si True, CB bloquea tambiÃ©n grids neutral (comparte capital)
    circuit_breaker_afecta_grid_neutral: bool = True

    # V5.9.2: ParÃ¡metros de simulaciÃ³n virtual
    grid_neutral_sim_max_posiciones: int = 50  # V6.0 Punto 4: era 10 (techo blando)
    # V6.0 Punto 4: lÃ­mite real por margen comprometido (reemplaza el conteo
    # arbitrario de posiciones en _evaluar_kill_switch).
    grid_neutral_max_margin_pct: float = 0.85
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 4.2: PERFILES DE MONEDA (categorÃ­as adaptativas)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # CAMBIO: grid_niveles_max aumentados drÃ¡sticamente en cada perfil.
    # Valores originales entre parÃ©ntesis: memecoin 5, altcoin_bajo_vol 6, major 8, default 6
    perfiles_moneda: dict = {
        'memecoin': {
            'adx_min': 12.0,
            'volume_threshold': 0.30,
            'mfm_penalizacion_max': 3,
            'grid_niveles_max': 25      # antes: 5
        },
        'altcoin_bajo_vol': {
            'adx_min': 13.0,
            'volume_threshold': 0.40,
            'mfm_penalizacion_max': 5,
            'grid_niveles_max': 25      # antes: 6
        },
        'major': {
            'adx_min': 15.0,
            'volume_threshold': 0.70,
            'mfm_penalizacion_max': 7,
            'grid_niveles_max': 40      # antes: 8
        },
        'default': {
            'adx_min': 14.0,
            'volume_threshold': 0.50,
            'mfm_penalizacion_max': 5,
            'grid_niveles_max': 35      # antes: 6
        }
    }
    grid_neutral_sim_fee_rate: float = 0.0005  # 0.05% fee Binance Futures
    grid_neutral_sim_slippage_base: float = 0.0005  # 0.05% slippage base simulado

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 0: CONFIGURACIÃ“N DEL EXECUTOR (Testnet / Real)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    trading_mode: str = "TESTNET"  # TESTNET | REAL

    # Capital y riesgo
    trading_capital_max_usdt: float = 100.0
    trading_apalancamiento_min: int = 5
    trading_apalancamiento_max: int = 15
    trading_notional_min_usdt: float = 5.0
    trading_notional_margen_seguridad: float = 1.2  # 20% sobre el mÃ­nimo

    # Rate limiting
    trading_batch_max_ordenes: int = 5
    trading_rate_limit_rps: int = 5  # Max 5 requests/segundo (conservador)
    # V6.0 Punto 5: 3s (era 10) â€” TP ~15s mÃ¡s rÃ¡pido en promedio.
    # OJO: triplica la frecuencia de llamadas REST (vigilar rate limit / weight).
    trading_polling_interval_seg: int = 3

    # Keys testnet (separadas de las reales para seguridad)
    binance_testnet_api_key: str = os.getenv("BINANCE_TESTNET_API_KEY", "sX7kym5t0AE11IRWWg0Ks84z2K34RH5yirclh5MoyWcPgQVbnW670vTJfDJ1W01m")
    binance_testnet_secret: str = os.getenv("BINANCE_TESTNET_SECRET", "9OflvUojY6Pyht86VezoIF6F2WnHPr4RtWp0wO1BmwZOFyHysKr8TkTZofMIf03t")

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # V8: MOTOR DE NIVELES
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    grid_engine_enabled: bool = True           # Master toggle
    # V6.0 Punto 1: ACTIVE = reposiciÃ³n de niveles huÃ©rfanos (REPLENISH)
    grid_engine_coverage_mode: str = "ACTIVE"   # SHADOW | ACTIVE | OFF
    grid_engine_roundtrip: bool = True         # Fase 4: round-trip por nivel
    grid_engine_gap_dynamic: bool = False       # Fase 5: gap dinÃ¡mico
    grid_engine_lag_guard_ms: int = 10000       # 10s real, 15s testnet
    grid_engine_max_ops_per_tick: int = 3       # MÃ¡ximo operaciones por ciclo
    grid_engine_coverage_interval_ticks: int = 3  # Cada N ciclos de monitoreo
    grid_engine_gap_hysteresis_ticks: int = 2     # Histeresis para mover gap
    grid_engine_max_snapshots: int = 100        # MÃ¡ximo snapshots en DB

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # PROTECCIÃ“N DE GRIDS (FASE 0-5: anti-drenaje de capital)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # â”€â”€ FASE 3: LÃ­mite de posiciones simultÃ¡neas por sÃ­mbolo â”€â”€
    grid_max_posiciones_simultaneas: int = 5    # MÃ¡x posiciones ABIERTAS por sÃ­mbolo (antes: ilimitado)

    # â”€â”€ FASE 2: Rango inmutable + estrategia de salida en 3 niveles â”€â”€
    grid_rango_timeout_min: int = 240           # Min fuera del rango antes de cerrar posiciones (4h)
    grid_max_perdida_pct: float = 2.0           # % mÃ¡x pÃ©rdida flotante por grid (hard stop)

    # â”€â”€ FASE 4: Filtro de fees y rentabilidad â”€â”€
    grid_ratio_rentabilidad_min: float = 2.0    # Ratio mÃ­n profit/fees por round-trip (1.0 = breakeven)
    grid_fees_diarias_max_pct: float = 0.5      # % capital mÃ¡x en fees diarias (0.5% = $25 en $5000)

    # â”€â”€ FASE 1: ReduceOnly fix â”€â”€
    grid_cierre_cooldown_seg: int = 300         # Cooldown tras CIERRE_FALLIDO (5 min)
    grid_cierre_max_intentos: int = 2           # MÃ¡x ciclos de cierre fallido antes de intervenciÃ³n manual

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # FASE 4.5: SWEEPER DE HUÃ‰RFANOS + CIERRE POR PAR + DETECTOR DE PIERNA ATASCADA
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    fase45_policy: str = "ACTIVE"               # OFF | SHADOW | ACTIVE
    fase45_sweeper_interval_seg: int = 900      # Barrido de posiciones cada 15 min (+1 al arranque)
    fase45_pair_fee_taker: float = 0.0005       # Taker fee estimado por pierna (0.05%)
    fase45_pair_fee_margin_mult: float = 2.0    # Neto del par debe ser >= fees_est Ã— este margen
    fase45_lone_leg_policy: str = "ALERT"       # ALERT | CLOSE (pierna huÃ©rfana sola)
    fase45_lone_leg_max_close_usdt: float = 30.0  # CLOSE solo si notional <= este tope
    fase45_stuck_age_hours: float = 2.0         # Detector: edad mÃ­nima del round-trip
    fase45_stuck_dist_steps: float = 3.0        # Detector: distancia en contra, en steps
    fase45_max_cierres_por_dia: int = 10        # Tope de seguridad diario de cierres F45
    fase45_live_pair_close: bool = True        # 4.5.2: cerrar pares atascados en grids VIVOS (requiere policy ACTIVE)

    # Helper para lag guard adaptativo
    def get_lag_guard_ms(self) -> int:
        if self.trading_mode == 'TESTNET':
            return 15000
        return self.grid_engine_lag_guard_ms


CONFIG = Config()

