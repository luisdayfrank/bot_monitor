from pydantic import BaseModel, Field
from typing import List
import os

class Config(BaseModel):
    # ───────────────────────────────────────────────────────────────────────────────
    # CONFIGURACIÓN DE ZONA HORARIA
    # Formatos válidos: "America/Caracas", "America/Bogota", "America/Mexico_City",
    # "America/Argentina/Buenos_Aires", "Europe/Madrid", etc.
    # Lista completa: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    # ───────────────────────────────────────────────────────────────────────────────
    timezone: str = "America/Caracas"  # UTC-4, sin horario de verano

    # Monedas a monitorear (max 10)
    symbols: List[str] = Field(
        default=["TRXUSDT", "ZECUSDT", "WIFUSDT", "NEARUSDT", "MEMEUSDT", "APTUSDT", "XLMUSDT", "ADAUSDT", "DOGEUSDT", "INJUSDT" ],
        max_length=10
    )

    # Timeframes
    tf_micro: str = "1m"       # Gatillo sniper
    tf_primary: str = "15m"    # Filtro macro / timing
    tf_macro: str = "4h"       # Contexto de sesión
    tf_live: str = "markPrice@1s"

    # Buffers
    max_velas_1m: int = 350    # EMA_300 + margen de seguridad
    max_velas_15m: int = 300
    max_velas_4h: int = 200

    # ─── Filtro Macro (15m) ───
    adx_ideal: tuple = (25, 35)
    adx_reject: float = 45.0

    # RSI Macro 15m: Oxígeno (no gatillo).
    rsi_macro_min: float = 45.0
    rsi_macro_max: float = 80.0

    # Para LONG (simetría)
    rsi_macro_long_max: float = 55.0
    rsi_macro_long_min: float = 20.0

    # ─── Gatillo Micro (1m) ───
    rsi_micro_length: int = 7
    rsi_micro_short_trigger: float = 75.0
    rsi_micro_long_trigger: float = 25.0

    # EMA equivalente: EMA_20 en 15m == EMA_300 en 1m
    ema_micro_period: int = 300

    # Confirmación de mecha (rechazo)
    wick_min_pct: float = 0.05

    # ATR / MACD / Volumen (15m)
    atr_min_pct: float = 0.15
    atr_max_pct: float = 2.0
    macd_stable_threshold: float = 0.3
    macd_danger_threshold: float = 0.5
    volume_min_ratio: float = 1.0

    # Histéresis y cooldown
    hysteresis_velas: int = 3
    cooldown_15m_velas: int = 1

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3: PARÁMETROS DE SEGURIDAD DEL GRID
    # ═══════════════════════════════════════════════════════════════════════════════

    # Grid params base
    grid_min_grids: int = 15
    grid_max_grids_hard: int = 50      # Cap absoluto para evitar saturación API
    grid_fee_rate: float = 0.0005
    grid_slippage: float = 0.0005
    grid_default_capital: float = 100.0
    grid_default_leverage: int = 3

    # Nocional mínimo por orden (Binance Futures)
    grid_notional_min: float = 10.0

    # Multiplicador de rango del grid (atr * multiplicador = rango total)
    # Rango simétrico: price ± (atr * multiplicador / 2)
    grid_rango_mult_min: float = 2.0   # ATR bajo → rango mínimo
    grid_rango_mult_max: float = 6.0   # ATR alto → rango máximo

    # Breakeven: step_pct debe ser al menos este múltiplo para ser rentable
    grid_breakeven_mult: float = 1.2   # 1.2x breakeven (no 2x como antes, más realista)

    # Auto-compresión: si step_pct < breakeven * mult, reducir grids
    grid_auto_compress: bool = True

    # Densidad máxima de grids: mínimo 0.5 ATR entre grids
    grid_min_dist_atr: float = 0.5     # step_usdt >= atr * 0.5

    # Percentil para truncar ATR (95 = corta outliers, respeta volatilidad normal)
    grid_atr_percentil: float = 95.0

    # Posición en rango: alerta si >80% o <20% (grid mal posicionado)
    grid_posicion_alerta_max: float = 0.80
    grid_posicion_alerta_min: float = 0.20

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3: CIRCUIT BREAKER
    # ═══════════════════════════════════════════════════════════════════════════════

    # Número de disparos consecutivos en pérdida antes de activar circuit breaker
    circuit_breaker_disparos: int = 3

    # Tiempo de pausa en segundos cuando se activa el circuit breaker
    circuit_breaker_pausa_seg: int = 1800  # 30 minutos

    # Reducción de capital tras activar circuit breaker (0.5 = 50%)
    circuit_breaker_reduccion_capital: float = 0.5

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3: HISTÉRESIS SUAVIZADA
    # ═══════════════════════════════════════════════════════════════════════════════

    # En vez de resetear a 0 en una vela contraria, decrementar progresivamente
    hysteresis_suavizada: bool = True
    hysteresis_decremento: int = 1       # Velas de confirmación que se pierden por vela contraria

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 4.5: MODO AUDITORÍA EXTERNA
    # ═══════════════════════════════════════════════════════════════════════════════

    # Activa la recopilación de datos para auditoría manual externa
    # Cuando está activo, el bot guarda eventos y envía reporte diario
    # como archivo .json por Telegram. No afecta el funcionamiento normal.
    modo_auditoria: bool = True

    # Hora local para enviar el reporte diario (formato HH:MM en zona horaria del bot)
    # Ejemplo: "23:55" = 23:55 hora de Caracas (UTC-4)
    # Nota: Las velas de Binance cierran en UTC, pero el reporte se envía
    # en tu hora local para conveniencia
    auditoria_hora_reporte: str = "23:55"

    # Cuántas horas post-disparo trackear para el seguimiento
    auditoria_horas_seguimiento: int = 8

    # Intervalo de muestras post-disparo (minutos)
    auditoria_muestras_intervalo_min: int = 5

    # Cuántas horas de muestras detalladas (después solo velas 15m)
    auditoria_muestras_detalladas_horas: int = 2

    # Notificaciones
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # DB
    db_path: str = "crypto_monitor.db"

    # Binance
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")


    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 4.5: HEARTBEAT DE VALIDACIÓN (debug pasivo)
    # ═══════════════════════════════════════════════════════════════════════════════

    # Activa un log cada N minutos con el estado interno de todas las monedas.
    # Útil para validar correcciones sin esperar al reporte diario.
    # No afecta el funcionamiento del bot. Solo lectura, cero interferencia.
    heartbeat_debug: bool = True
    heartbeat_intervalo_min: int = 15  # Minutos entre heartbeats


CONFIG = Config()
