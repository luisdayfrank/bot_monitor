import asyncio
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Set
from config import CONFIG
from coin_config_adapter import get_coin_config, get_category_emoji
import pytz


class SignalState:
    """Estado de la maquina de estados por simbolo — V5.7 Fases 1-5."""
    def __init__(self):
        self.estado = 'MONITOREO'
        self.velas_confirmacion = 0
        self.ultima_direccion = None
        self.ultimo_score = 0
        self.ultimos_params = None
        self.ultimo_disparo_timestamp_15m = 0
        self.ultimo_disparo_precio = 0.0
        self.ultimo_disparo_direccion = None
        self.ultimo_disparo_evaluado = False
        self.filtro_macro_aprobado = False
        self.direccion_filtro = None
        self.ultimo_filtro_timestamp = 0

        # FASE 3: Circuit breaker
        self.disparos_consecutivos = 0
        self.ultimo_disparo_fue_rentable = True
        self.circuit_breaker_activo = False
        self.circuit_breaker_hasta = 0
        self.capital_actual = CONFIG.grid_default_capital

        # FASE 4.5: Tracking de auditoria
        self._prev_filtro_aprobado = False
        self._prev_estado = 'MONITOREO'

        # Correcciones post-analisis cruzado
        self.score_macro_actual = 0
        self.armed_timestamp = 0
        self.direccion_ultima_valida = None
        self.score_bajo_desde = None

        # V4.2: PAUSA MANUAL
        self.moneda_pausada = False
        self.moneda_pausada_manual = False
        self.moneda_pausada_razon = None
        self.moneda_pausada_timestamp = 0

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5: GRID NEUTRAL — Estado persistente
        # ═══════════════════════════════════════════════════════════════════════════════
        self.neutral_grid_timestamp = 0  # Timestamp de entrada a NEUTRAL_GRID
        self.grid_params_neutral = None  # Parámetros del grid neutral activo
        # FASE 2.3: Contador de velas consecutivas para aborto de NEUTRAL_GRID
        self.neutral_grid_dir_consec = 0
        self.neutral_grid_dir_previa = 'NEUTRAL'        


        # FASE 5.5: Metricas diarias acumuladas
        self.metricas_dia: dict = {
            'score_max': 0,
            'score_min': 100,
            'score_sum': 0,
            'score_count': 0,
            'veces_cerca_umbral': 0,
            'veces_muy_cerca': 0,
            'veces_paso_umbral': 0,
            'score_max_timestamp': None,
            'score_min_timestamp': None,
            'estados_visitados': set(),
            'direcciones_detectadas': set(),
            'rechazos_frecuentes': {},
            'ultimo_log_continuo_ts': 0,
        }


class SignalGenerator:
    """
    Generador de senales V5.7 — Fases 1-5 implementadas.

    FASE 1: Post near-miss seguimiento persistente (SQLite)
    FASE 2: Volumen adaptativo por clase de moneda (volume_threshold en registry)
    FASE 3: RSI contextualizado (evaluar segun tendencia)
    FASE 4: Umbrales dinamicos por volatilidad historica (ATR percentil)
    FASE 5: Grid Neutral reformulado — 5 condiciones de entrada + 5 de aborto + timeout 30min
    FASE 6: Commitment score ELIMINADO completamente
    """

    def __init__(self, queue_in: asyncio.Queue, queue_out: asyncio.Queue):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.states: Dict[str, SignalState] = {s: SignalState() for s in CONFIG.symbols}

        self.indicadores_1m: Dict[str, dict] = {}
        self.indicadores_15m: Dict[str, dict] = {}
        self.indicadores_4h: Dict[str, dict] = {}

        self._atr_historico: Dict[str, list] = {s: [] for s in CONFIG.symbols}
        self._atr15m_historico: Dict[str, list] = {s: [] for s in CONFIG.symbols}
        self._rango_reciente: Dict[str, dict] = {s: {'highs': [], 'lows': []} for s in CONFIG.symbols}
        self._historial_1m: Dict[str, list] = {s: [] for s in CONFIG.symbols}

        self.audit_logger = None
        self.grid_simulator = None  # V5.9.2: Referencia al simulador de grid neutral

    ARMED_TIMEOUT_MIN = 30
    SCORE_DISPARO_MIN = 70
    SCORE_MANTENIMIENTO_ARMED = 50
    ADX_RECHAZO_MIN = 20

    UMBRAL_BLOQUEADO_ADX_EXTREMO = -1
    UMBRAL_BLOQUEADO_ADX_BAJO = -2

    # FASE 5.5: Umbrales para near-misses
    NEAR_MISS_UMBRAL_PCT = 0.70
    NEAR_MISS_MUY_CERCA_PCT = 0.85

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 2: VOLUMEN ADAPTATIVO POR CLASE DE MONEDA
    # ═══════════════════════════════════════════════════════════════════════════════
    def _get_volume_threshold(self, symbol: str) -> float:
        """Obtiene el umbral de volumen adaptativo desde el registry."""
        coin_config = CONFIG.coin_registry.get(symbol, {})
        return coin_config.get('volume_threshold', 0.7)

    def _calcular_adx_min(self, symbol: str) -> float:
        """FASE 4.2: Usa adx_min del perfil ya fusionado en get_coin_config."""
        coin_cfg = get_coin_config(symbol)
        return coin_cfg.get('adx_min', CONFIG.adx_min_trend_global)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 3: RSI CONTEXTUALIZADO
    # ═══════════════════════════════════════════════════════════════════════════════
    def evaluar_rsi_oxigeno(self, rsi: float, direction: str, adx: float, trend_strength: str) -> Tuple[str, str]:
        """
        FASE 1.2: RSI direccional reformulado.
        ÚNICA autoridad. Elimina doble validación con CONFIG.rsi_macro_*.
        """
        if direction == "SHORT":
            if rsi < CONFIG.rsi_extremo_short_min:
                return "RECHAZAR", f"RSI sobreventa extrema ({rsi:.1f}) — posible rebote"
            elif rsi < 40:
                return "PERMITIR", f"RSI {rsi:.1f} en SHORT — momentum bajista confirmado"
            elif rsi < 50:
                return "PERMITIR", f"RSI {rsi:.1f} en SHORT — aceptable"
            else:
                return "RECHAZAR", f"RSI alto {rsi:.1f} para SHORT — momentum alcista interno"
        elif direction == "LONG":
            if rsi > CONFIG.rsi_extremo_long_max:
                return "RECHAZAR", f"RSI sobrecompra extrema ({rsi:.1f}) — posible correccion"
            elif rsi > 55:
                return "PERMITIR", f"RSI {rsi:.1f} en LONG — momentum alcista confirmado"
            elif rsi > 45:
                return "PERMITIR", f"RSI {rsi:.1f} en LONG — aceptable"
            else:
                return "RECHAZAR", f"RSI bajo {rsi:.1f} para LONG — momentum bajista interno"
        else:
            return "PERMITIR", f"RSI {rsi:.1f} en direccion NEUTRAL"

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 4: UMBRALES DINAMICOS POR VOLATILIDAD HISTORICA
    # ═══════════════════════════════════════════════════════════════════════════════
    def calcular_umbral_base(self, symbol: str, atr_percentil: float, coin_config: dict) -> int:
        """
        Calcula umbral base dinamico segun percentil del ATR historico.
        ATR bajo = mas selectivo, ATR alto = mas permisivo.
        """
        base_threshold = coin_config.get('base_threshold', 70)
        if atr_percentil < 20:
            return max(50, base_threshold - 15)
        elif atr_percentil < 40:
            return max(55, base_threshold - 10)
        elif atr_percentil > 80:
            return min(85, base_threshold + 10)
        else:
            return base_threshold

    def _get_atr_percentil(self, symbol: str) -> float:
        """Calcula el percentil del ATR actual respecto al historico."""
        atr_historico = self._atr15m_historico.get(symbol, [])
        if len(atr_historico) < 20:
            return 50.0
        atr_actual = atr_historico[-1] if atr_historico else 0
        if atr_actual <= 0:
            return 50.0
        menores = sum(1 for a in atr_historico if a < atr_actual)
        return (menores / len(atr_historico)) * 100

    # ═══════════════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5: GRID NEUTRAL — EVALUACION DE ENTRADA
    # ═══════════════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════════════
    def evaluar_grid_neutro(self, symbol: str, i15: dict) -> Tuple[bool, str]:
        """
        Evalua las 5 condiciones de entrada al estado NEUTRAL_GRID.
        Retorna: (entrar, razon)

        Condiciones de entrada:
        1. ADX < 20 (sin tendencia fuerte)
        2. RSI entre 40-60 (no extremos)
        3. ATR percentil 30-70 (volatilidad moderada)
        4. Precio cerca de EMA50 (< 1% distancia)
        5. MACD histograma cerca de 0 (|hist| < 0.2 * ATR en terminos de precio)
        """
        rechazos = []

        # 1. ADX bajo
        adx = i15.get('adx', 0)
        if adx is None or np.isnan(adx):
            return False, "ADX no disponible"
        if adx >= CONFIG.grid_neutral_adx_max:
            rechazos.append(f"ADX {adx:.1f} >= {CONFIG.grid_neutral_adx_max} (tendencia detectada)")

        # 2. RSI neutral
        rsi = i15.get('rsi', 50)
        if rsi < CONFIG.grid_neutral_rsi_min:
            rechazos.append(f"RSI {rsi:.1f} < {CONFIG.grid_neutral_rsi_min} (sobreventa)")
        elif rsi > CONFIG.grid_neutral_rsi_max:
            rechazos.append(f"RSI {rsi:.1f} > {CONFIG.grid_neutral_rsi_max} (sobrecompra)")

        # 3. ATR percentil moderado (usa config, no hardcode)
        atr = i15.get('atr', 0)
        atr_hist = self._atr15m_historico.get(symbol, [])
        atr_pct_min = CONFIG.grid_neutral_atr_percentil_min
        atr_pct_max = CONFIG.grid_neutral_atr_percentil_max
        if len(atr_hist) >= 20 and atr > 0:
            atr_p = np.percentile(atr_hist, [atr_pct_min, atr_pct_max])
            if atr < atr_p[0]:
                rechazos.append(f"ATR {atr:.6f} < p{atr_pct_min:.0f} {atr_p[0]:.6f} (volatilidad muy baja)")
            elif atr > atr_p[1]:
                rechazos.append(f"ATR {atr:.6f} > p{atr_pct_max:.0f} {atr_p[1]:.6f} (volatilidad muy alta)")

        # 4. Precio cerca de EMA50
        price = i15.get('close', 0)
        ema50 = i15.get('ema50_15m') or price
        if price > 0 and ema50 > 0:
            distancia_pct = abs(price - ema50) / ema50 * 100
            if distancia_pct > 1.0:
                rechazos.append(f"Precio {distancia_pct:.2f}% lejos de EMA50")

        # 5. MACD cerca de cero
        macd_hist = i15.get('macd_hist', 0)
        if macd_hist is not None and price > 0 and atr > 0:
            macd_en_atr = abs(macd_hist) / atr if atr > 0 else float('inf')
            if macd_en_atr > 0.5:
                rechazos.append(f"MACD {macd_hist:.4f} lejos de 0 ({macd_en_atr:.1f}x ATR)")

        if rechazos:
            return False, f"Grid neutral rechazado: {'; '.join(rechazos[:2])}"
        return True, "Condiciones de grid neutral cumplidas"

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5: GRID NEUTRAL — EVALUACION DE ABORTO (5 condiciones)
    # ═══════════════════════════════════════════════════════════════════════════════
    def evaluar_aborto_neutral_grid(self, symbol: str, i15: dict, state: SignalState, timeout_moneda: int) -> Tuple[bool, str]:
        """
        Evalua las 5 condiciones de aborto del estado NEUTRAL_GRID.
        Se ejecuta en cada vela 15m mientras esta en NEUTRAL_GRID.
        Retorna: (abortar, razon)

        Condiciones de aborto:
        1. Timeout > 30 minutos
        2. ADX explosivo: ADX > 25 (o +5 sobre entrada)
        3. RSI extremo: RSI < 35 o RSI > 65
        4. Ruptura EMAs: Precio se aleja > 2% de EMA50
        5. ATR explosivo: ATR > p80 * 1.5
        """
        ahora = int(datetime.now(pytz.UTC).timestamp())

        # 1. Timeout adaptativo por moneda
        if state.neutral_grid_timestamp > 0:
            tiempo_min = (ahora - state.neutral_grid_timestamp) / 60
            if tiempo_min > timeout_moneda:
                return True, f"Timeout: {tiempo_min:.0f}min > {timeout_moneda}min"

        # 2. ADX explosivo (usa delta configurable sobre el umbral de entrada)
        adx = i15.get('adx', 0)
        adx_umbral_entrada = CONFIG.grid_neutral_adx_max
        adx_umbral_aborto = adx_umbral_entrada + CONFIG.grid_neutral_aborto_adx_delta
        if adx > adx_umbral_aborto:
            return True, f"ADX {adx:.1f} > {adx_umbral_aborto:.1f} (umbral entrada {adx_umbral_entrada} + delta {CONFIG.grid_neutral_aborto_adx_delta})"

        # 3. RSI extremo
        rsi = i15.get('rsi', 50)
        if rsi < CONFIG.grid_neutral_rsi_min:
            return True, f"RSI {rsi:.1f} < {CONFIG.grid_neutral_rsi_min} (sobreventa extrema)"
        if rsi > CONFIG.grid_neutral_rsi_max:
            return True, f"RSI {rsi:.1f} > {CONFIG.grid_neutral_rsi_max} (sobrecompra extrema)"

        # 4. Ruptura EMAs (> 2% de distancia)
        price = i15.get('close', 0)
        ema50 = i15.get('ema50_15m') or price
        if price > 0 and ema50 > 0:
            distancia_pct = abs(price - ema50) / ema50 * 100
            if distancia_pct > CONFIG.grid_neutral_aborto_precio_pct:
                return True, f"Precio rompe EMA50: {distancia_pct:.2f}% > {CONFIG.grid_neutral_aborto_precio_pct}%"

        # 5. ATR explosivo
        atr = i15.get('atr', 0)
        atr_hist = self._atr15m_historico.get(symbol, [])
        if len(atr_hist) >= 20 and atr > 0:
            atr_p80 = np.percentile(atr_hist, 80)
            if atr_p80 > 0 and atr > atr_p80 * 1.5:
                return True, f"ATR explosivo: {atr:.6f} > p80*1.5 ({atr_p80*1.5:.6f})"

        return False, ""

    # ═══════════════════════════════════════════════════════════════════════════════
    # METODOS DE PAUSA / REANUDACION
    # ═══════════════════════════════════════════════════════════════════════════════
    def pausar_moneda_manual(self, symbol: str, razon: str = "Comando usuario") -> bool:
        if symbol not in self.states:
            return False
        state = self.states[symbol]
        if state.moneda_pausada_manual:
            return False

        estado_anterior = state.estado
        state.moneda_pausada_manual = True
        state.moneda_pausada = True
        state.moneda_pausada_razon = razon
        state.moneda_pausada_timestamp = int(datetime.now(pytz.UTC).timestamp())

        state.estado = 'MONITOREO'
        state.velas_confirmacion = 0
        state.filtro_macro_aprobado = False
        state.direccion_filtro = None
        state.armed_timestamp = 0
        state.score_bajo_desde = None
        # FASE 5: Reset de neutral_grid al pausar
        state.neutral_grid_timestamp = 0

        print(f"  [PAUSA] {symbol} PAUSADA MANUALMENTE | Razon: {razon} | Estado previo: {estado_anterior}")
        return True

    def reanudar_moneda_manual(self, symbol: str) -> bool:
        if symbol not in self.states:
            return False
        state = self.states[symbol]
        if not state.moneda_pausada_manual:
            return False

        state.moneda_pausada_manual = False
        state.moneda_pausada = False
        state.moneda_pausada_razon = None
        state.moneda_pausada_timestamp = 0
        state.score_bajo_desde = None

        print(f"  [RESUME] {symbol} REANUDADA MANUALMENTE | Monitoreo reiniciado")
        return True

    def pausar_todas_manual(self, razon: str = "Comando usuario") -> list:
        pausadas = []
        for symbol in CONFIG.symbols:
            if self.pausar_moneda_manual(symbol, razon):
                pausadas.append(symbol)
        return pausadas

    def reanudar_todas_manual(self) -> list:
        reanudadas = []
        for symbol in CONFIG.symbols:
            if self.reanudar_moneda_manual(symbol):
                reanudadas.append(symbol)
        return reanudadas

    def get_monedas_pausadas(self) -> Dict[str, dict]:
        resultado = {}
        for symbol, state in self.states.items():
            if state.moneda_pausada or state.moneda_pausada_manual:
                resultado[symbol] = {
                    'pausa_manual': state.moneda_pausada_manual,
                    'pausa_auto': state.moneda_pausada,
                    'razon': state.moneda_pausada_razon,
                    'timestamp': state.moneda_pausada_timestamp,
                    'estado_maquina': state.estado
                }
        return resultado

    # ═══════════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ═══════════════════════════════════════════════════════════════════════════════
    async def run(self):
        while True:
            tf, symbol, data = await self.queue_in.get()

            if tf == '1m':
                self.indicadores_1m[symbol] = data
                self._alimentar_historial_1m(symbol, data)
                await self._actualizar_estado_maquina(symbol)
                if self.states[symbol].estado == 'ARMED':
                    await self.evaluar_gatillo(symbol)

            elif tf == '15m':
                self.indicadores_15m[symbol] = data
                atr_val = data.get('atr')
                if atr_val and atr_val > 0:
                    self._atr_historico[symbol].append(atr_val)
                    if len(self._atr_historico[symbol]) > 500:
                        self._atr_historico[symbol] = self._atr_historico[symbol][-500:]

                    self._atr15m_historico[symbol].append(atr_val)
                    if len(self._atr15m_historico[symbol]) > CONFIG.atr_percentil_ventana:
                        self._atr15m_historico[symbol] = self._atr15m_historico[symbol][-CONFIG.atr_percentil_ventana:]

                if data.get('high') and data.get('low'):
                    self._rango_reciente[symbol]['highs'].append(data['high'])
                    self._rango_reciente[symbol]['lows'].append(data['low'])
                    if len(self._rango_reciente[symbol]['highs']) > 20:
                        self._rango_reciente[symbol]['highs'] = self._rango_reciente[symbol]['highs'][-20:]
                        self._rango_reciente[symbol]['lows'] = self._rango_reciente[symbol]['lows'][-20:]

                # ═══════════════════════════════════════════════════════════════════
                # FASE 5: Evaluar aborto de NEUTRAL_GRID en cada vela 15m
                # ═══════════════════════════════════════════════════════════════════
                state = self.states[symbol]
                if abortar:
                    state.estado = 'MONITOREO'
                    state.neutral_grid_timestamp = 0
                    state.filtro_macro_aprobado = False
                    state.direccion_filtro = None
                    print(f"  [NEUTRAL_GRID] {symbol} -> MONITOREO (aborto: {razon})")
                    
                    # FIX: Notificar al simulador para finalizar grid (evita "sin_ticks_5min" fantasma)
                    if self.grid_simulator:
                        await self.grid_simulator.queue.put({
                            'tipo': 'FINALIZAR_GRID',
                            'symbol': symbol,
                            'razon': razon
                        })
                    
                    # F3.6: Auditoría de aborto de grid neutral
                    if self.audit_logger:
                        await self.audit_logger.log_evento_grid_simulacion(
                            symbol=symbol, tipo='NEUTRAL_GRID_ABORT', grid_id=0,
                            evento_simulacion={'razon': razon}, pnl_acumulado=0.0
                        )
                        await self.audit_logger.log_cambio_estado(
                            symbol=symbol,
                            de='NEUTRAL_GRID',
                            a='MONITOREO',
                            direccion='NEUTRAL',
                            score_macro=state.score_macro_actual
                        )
                        state._prev_estado = 'MONITOREO'
                    continue  # No evaluar filtro macro este ciclo

                await self.evaluar_filtro_macro(symbol)

            elif tf == '4h':
                self.indicadores_4h[symbol] = data

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.5: LOG CONTINUO Y NEAR-MISSES
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _reiniciar_metricas_dia(self, symbol: str):
        """FIX #3: Reinicia métricas diarias si cambió el día."""
        state = self.states[symbol]
        ahora = datetime.now(pytz.UTC).strftime('%Y-%m-%d')
        if state.metricas_dia.get('fecha') != ahora:
            state.metricas_dia = {
                'fecha': ahora,
                'score_max': 0,
                'score_min': 100,
                'score_sum': 0,
                'score_count': 0,
                'veces_cerca_umbral': 0,
                'veces_muy_cerca': 0,
                'veces_paso_umbral': 0,
                'score_max_timestamp': None,
                'score_min_timestamp': None,
                'estados_visitados': set(),
                'direcciones_detectadas': set(),
                'rechazos_frecuentes': {},
                'ultimo_log_continuo_ts': 0,
            }

    async def _log_continuo(self, symbol: str, data: dict):
        if not self.audit_logger:
            return

        # FIX #3: Reiniciar métricas si cambió el día
        await self._reiniciar_metricas_dia(symbol)

        state = self.states[symbol]
        i15 = self.indicadores_15m.get(symbol, {})
        if not i15:
            return

        timestamp = i15.get('timestamp', 0)
        if timestamp == state.metricas_dia['ultimo_log_continuo_ts']:
            return
        state.metricas_dia['ultimo_log_continuo_ts'] = timestamp

        score = state.score_macro_actual
        state.metricas_dia['score_sum'] += score
        state.metricas_dia['score_count'] += 1
        if score > state.metricas_dia['score_max']:
            state.metricas_dia['score_max'] = score
            state.metricas_dia['score_max_timestamp'] = timestamp
        if score < state.metricas_dia['score_min']:
            state.metricas_dia['score_min'] = score
            state.metricas_dia['score_min_timestamp'] = timestamp

        state.metricas_dia['estados_visitados'].add(state.estado)
        if state.direccion_filtro:
            state.metricas_dia['direcciones_detectadas'].add(state.direccion_filtro)
        elif state.direccion_ultima_valida:
            state.metricas_dia['direcciones_detectadas'].add(state.direccion_ultima_valida)

        await self.audit_logger.log_continuo(
            symbol=symbol,
            timestamp=timestamp,
            estado_maquina=state.estado,
            score_macro=score,
            direccion=state.direccion_filtro or state.direccion_ultima_valida,
            contexto={
                'precio': i15.get('close'),
                'rsi': i15.get('rsi'),
                'adx': i15.get('adx'),
                'atr': i15.get('atr'),
                'macd_hist': i15.get('macd_hist'),
                'volumen_ratio': i15.get('volume') / i15.get('volume_sma20', 1) if i15.get('volume_sma20') else 0,
                'mfm_sma5': i15.get('mfm_sma5'),
                'ema200_15m': i15.get('ema200_15m'),
                'ema50_15m': i15.get('ema50_15m'),
            }
        )

    async def _evaluar_near_misses(self, symbol: str, score_macro: int, umbral: int,
                                    rechazos: list, direction: str, filtro_aprobado: bool):
        if not self.audit_logger:
            return

        state = self.states[symbol]
        i15 = self.indicadores_15m.get(symbol, {})

        umbral_efectivo = umbral if umbral > 0 else self._calcular_umbral_filtro(symbol)
        if umbral_efectivo <= 0:
            umbral_efectivo = 70

        near_miss = False
        tipo_near_miss = None
        detalle = {}

        if score_macro >= umbral_efectivo and not filtro_aprobado:
            near_miss = True
            tipo_near_miss = "SCORE_PASA_OTRO_FILTRO_NO"
            detalle = {
                'score': score_macro,
                'umbral': umbral_efectivo,
                'razon': 'Score pasa umbral pero otro filtro rechaza',
                'rechazos': rechazos,
                'direccion': direction
            }
        # FIX 0.1: Reordenar tiers de mayor a menor para evitar solapamiento
        elif score_macro >= umbral_efectivo * self.NEAR_MISS_MUY_CERCA_PCT:  # 0.85
            near_miss = True
            tipo_near_miss = "MUY_CERCA_DEL_UMBRAL"
            detalle = {
                'score': score_macro,
                'umbral': umbral_efectivo,
                'porcentaje_umbral': round(score_macro / umbral_efectivo * 100, 1),
                'razon': f'Score al {round(score_macro / umbral_efectivo * 100, 0)}% del umbral',
                'rechazos': rechazos,
                'direccion': direction
            }
            state.metricas_dia['veces_muy_cerca'] += 1
        elif score_macro >= umbral_efectivo * 0.75:  # FIX: Umbral 75% (antes era imposible)
            near_miss = True
            tipo_near_miss = "EN_ZONA_DE_INTERES"
            detalle = {
                'score': score_macro,
                'umbral': umbral_efectivo,
                'porcentaje_umbral': round(score_macro / umbral_efectivo * 100, 1),
                'razon': f'Score al {round(score_macro / umbral_efectivo * 100, 0)}% del umbral — seguimiento preventivo',
                'rechazos': rechazos,
                'direccion': direction
            }
            state.metricas_dia['veces_cerca_umbral'] += 1
        elif score_macro >= umbral_efectivo * self.NEAR_MISS_UMBRAL_PCT:  # 0.70
            near_miss = True
            tipo_near_miss = "CERCA_DEL_UMBRAL"
            detalle = {
                'score': score_macro,
                'umbral': umbral_efectivo,
                'porcentaje_umbral': round(score_macro / umbral_efectivo * 100, 1),
                'razon': f'Score al {round(score_macro / umbral_efectivo * 100, 0)}% del umbral',
                'rechazos': rechazos,
                'direccion': direction
            }
            state.metricas_dia['veces_cerca_umbral'] += 1
        if near_miss and tipo_near_miss:
            print(f"  [NEAR-MISS] {symbol} NEAR-MISS: {tipo_near_miss} | Score:{score_macro} Umbral:{umbral_efectivo} Dir:{direction}")
            await self.audit_logger.log_near_miss(
                symbol=symbol,
                tipo=tipo_near_miss,
                score_macro=score_macro,
                umbral=umbral_efectivo,
                direccion=direction,
                contexto={
                    'precio': i15.get('close'),
                    'rsi': i15.get('rsi'),
                    'adx': i15.get('adx'),
                    'atr': i15.get('atr'),
                    'macd_hist': i15.get('macd_hist'),
                    'volumen_ratio': i15.get('volume') / i15.get('volume_sma20', 1) if i15.get('volume_sma20') else 0,
                    'mfm_sma5': i15.get('mfm_sma5'),
                    'estado_maquina': state.estado,
                    'rechazos': rechazos,
                },
                detalle=detalle
            )

            if tipo_near_miss == "SCORE_PASA_OTRO_FILTRO_NO" and self.audit_logger:
                await self.audit_logger.iniciar_seguimiento_near_miss(
                    symbol=symbol,
                    score=score_macro,
                    umbral=umbral_efectivo,
                    direccion=direction,
                    precio=i15.get('close', 0),
                    contexto={
                        'tipo_near_miss': tipo_near_miss,
                        'rechazos': rechazos,
                        'adx': i15.get('adx'),
                        'atr': i15.get('atr'),
                        'macd_hist': i15.get('macd_hist'),
                        'rsi': i15.get('rsi'),
                        'volumen_ratio': i15.get('volume') / i15.get('volume_sma20', 1) if i15.get('volume_sma20') else 0,
                        'mfm_sma5': i15.get('mfm_sma5'),
                    }
                )

    def _evaluar_condiciones_macro(self, symbol: str, coin_cfg: dict, direction: str,
                                    trend_strength: str, price: float, adx: float,
                                    rsi: float, atr: float, macd_hist: float,
                                    macd_hist_prev: float, vol_ratio: float,
                                    mfm: float, mfm_sma5: float) -> tuple:
        """
        FASE 4.1: Cálculo de score macro, rechazos y umbrales.
        Extraído de evaluar_filtro_macro para reducir complejidad.
        """
        rechazos = []
        score_macro = 0
        umbral_entrada = 70
        umbral_mantenimiento = self.SCORE_MANTENIMIENTO_ARMED
        umbral_bloqueado = False

        adx_techo = coin_cfg['adx_reject']
        mfm_umb = coin_cfg['mfm_umbral']
        adx_min = self._calcular_adx_min(symbol)

        # ADX scoring (FASE 1.1 adaptativo)
        if adx is None or np.isnan(adx) or adx > adx_techo:
            rechazos.append(f"ADX extremo (>{adx_techo}): {adx:.1f}")
            umbral_entrada = self.UMBRAL_BLOQUEADO_ADX_EXTREMO
            umbral_mantenimiento = self.UMBRAL_BLOQUEADO_ADX_EXTREMO
            umbral_bloqueado = True
        elif adx < adx_min - 2:
            rechazos.append(f"ADX sin tendencia (<{adx_min - 2:.0f}): {adx:.1f}")
            umbral_entrada = self.UMBRAL_BLOQUEADO_ADX_BAJO
            umbral_mantenimiento = self.UMBRAL_BLOQUEADO_ADX_BAJO
            umbral_bloqueado = True
        elif adx < adx_min + 3:
            score_macro += 10
            umbral_entrada = 75
        elif 25 <= adx <= 35:
            score_macro += 25
            umbral_entrada = 70
        else:
            score_macro += 15
            umbral_entrada = 65

        # RSI direccional (FASE 1.2)
        if direction == 'SHORT':
            decision, razon = self.evaluar_rsi_oxigeno(rsi, direction, adx, trend_strength)
            if decision == "PERMITIR":
                score_macro += 20
            else:
                rechazos.append(razon)
        elif direction == 'LONG':
            decision, razon = self.evaluar_rsi_oxigeno(rsi, direction, adx, trend_strength)
            if decision == "PERMITIR":
                score_macro += 20
            else:
                rechazos.append(razon)
        else:
            rechazos.append("Direccion NEUTRAL")

        # ATR scoring
        atr_pct = (atr / price) * 100 if price > 0 else 0
        if atr_pct < CONFIG.atr_min_pct:
            rechazos.append(f"ATR bajo: {atr_pct:.3f}%")
        elif atr_pct > CONFIG.atr_max_pct:
            rechazos.append(f"ATR alto: {atr_pct:.3f}%")
        else:
            score_macro += 15

        # MACD scoring
        hist_magnitude_pct = abs(macd_hist) / price * 100 if price > 0 else 0
        atr_pct_safe = atr_pct if atr_pct > 0 else 0.01
        if hist_magnitude_pct > CONFIG.macd_danger_threshold * atr_pct_safe:
            rechazos.append(f"MACD explosivo: {hist_magnitude_pct:.3f}%")
        elif hist_magnitude_pct > CONFIG.macd_stable_threshold * atr_pct_safe:
            score_macro += 10
        else:
            score_macro += 15

        # Volumen dinámico condicional (FASE 5.6)
        atr_historico_hoy = self._atr15m_historico.get(symbol, [])
        atr_percentil_umbral = np.percentile(atr_historico_hoy, CONFIG.volumen_bypass_atr_percentil) if len(atr_historico_hoy) >= 10 else float('inf')
        atr_en_percentil_75 = atr >= atr_percentil_umbral and len(atr_historico_hoy) >= 10

        macd_confirma_direccion = False
        if direction == 'SHORT' and macd_hist < 0 and macd_hist < macd_hist_prev:
            macd_confirma_direccion = True
        elif direction == 'LONG' and macd_hist > 0 and macd_hist > macd_hist_prev:
            macd_confirma_direccion = True

        conviccion_alta = (
            score_macro >= umbral_entrada + CONFIG.volumen_bypass_score_extra and
            adx > CONFIG.volumen_bypass_adx_min and
            macd_confirma_direccion and
            direction != 'NEUTRAL'
        )

        bypass_volumen = conviccion_alta or atr_en_percentil_75
        bypass_razon = None

        if bypass_volumen:
            if conviccion_alta and atr_en_percentil_75:
                bypass_razon = f"Conviccion Alta + ATR p75 | Score:{score_macro}>={umbral_entrada + CONFIG.volumen_bypass_score_extra} ADX:{adx:.1f}>{CONFIG.volumen_bypass_adx_min}"
            elif conviccion_alta:
                bypass_razon = f"Conviccion Alta | Score:{score_macro}>={umbral_entrada + CONFIG.volumen_bypass_score_extra} ADX:{adx:.1f}>{CONFIG.volumen_bypass_adx_min}"
            else:
                bypass_razon = f"ATR percentil {CONFIG.volumen_bypass_atr_percentil:.0f} | ATR:{atr:.6f} >= p{CONFIG.volumen_bypass_atr_percentil:.0f}:{atr_percentil_umbral:.6f}"
            print(f"  [BYPASS] {symbol} BYPASS VOLUMEN — {bypass_razon}")

        volume_threshold = self._get_volume_threshold(symbol)

        if not bypass_volumen:
            if vol_ratio >= volume_threshold:
                mfm_alineado = False
                if direction == 'SHORT' and mfm_sma5 < -mfm_umb:
                    mfm_alineado = True
                    score_macro += CONFIG.mfm_bonus_alineado
                    print(f"  [VOLUMEN] {symbol} Volumen+MFM alineado SHORT | mfm={mfm_sma5:.3f} (umbral: {mfm_umb}) | +{CONFIG.mfm_bonus_alineado}pts")
                elif direction == 'LONG' and mfm_sma5 > mfm_umb:
                    mfm_alineado = True
                    score_macro += CONFIG.mfm_bonus_alineado
                    print(f"  [VOLUMEN] {symbol} Volumen+MFM alineado LONG | mfm={mfm_sma5:.3f} (umbral: {mfm_umb}) | +{CONFIG.mfm_bonus_alineado}pts")
                else:
                    mfm_fuerza = abs(mfm_sma5)
                    if mfm_fuerza >= 0.5:
                        penalizacion = 7
                    elif mfm_fuerza >= 0.3:
                        penalizacion = 5
                    elif mfm_fuerza >= 0.1:
                        penalizacion = 3
                    else:
                        penalizacion = 0

                    # FASE 4.2: Tope de penalización por perfil
                    perfil_max = coin_cfg.get('mfm_penalizacion_max', 5)
                    if penalizacion > perfil_max:
                        penalizacion = perfil_max

                    if adx > 40:
                        penalizacion = penalizacion // 2

                    if direction == "NEUTRAL":
                        penalizacion = 0

                    score_macro -= penalizacion
                    rechazos.append(f"MFM contradictorio: mfm={mfm_sma5:.3f} vs {direction} (-{penalizacion}pts)")
                    print(f"  [MFM] {symbol} Volumen+MFM CONTRADICTORIO | mfm={mfm_sma5:.3f} vs {direction} | -{penalizacion}pts")
            else:
                rechazos.append(f"Volumen bajo: {vol_ratio:.1%} (umbral: {volume_threshold})")
        else:
            score_macro += CONFIG.mfm_bonus_alineado
            print(f"  [VOLUMEN] {symbol} Volumen BYPASS (+{CONFIG.mfm_bonus_alineado}pts) | {bypass_razon}")

        score_macro = max(0, min(100, score_macro))
        return score_macro, rechazos, umbral_entrada, umbral_mantenimiento, umbral_bloqueado


    async def _evaluar_transicion_neutral_grid(self, symbol: str, state: SignalState,
                                                direction: str, score_macro: int,
                                                umbral_entrada: int, i15: dict,
                                                price: float, coin_cfg: dict) -> bool:
        """
        FASE 4.1: Evalúa entrada y salida del estado NEUTRAL_GRID.
        Retorna True si entró a NEUTRAL_GRID (el flujo principal debe detenerse).
        """
        entro_neutral_grid = False

        if direction == 'NEUTRAL' and CONFIG.grid_neutral_enabled:
            cb_bloquea_grid = (
                CONFIG.circuit_breaker_afecta_grid_neutral and
                state.circuit_breaker_activo
            )
            if cb_bloquea_grid:
                print(f"  [NEUTRAL_GRID] {symbol} Grid neutral BLOQUEADO por Circuit Breaker activo")
            else:
                entrar_grid, razon_grid = self.evaluar_grid_neutro(symbol, i15)
                if entrar_grid and state.estado != 'NEUTRAL_GRID':
                    state.filtro_macro_aprobado = False
                    state.direccion_filtro = None
                    state.score_bajo_desde = None
                    state._prev_filtro_aprobado = False
                    state.neutral_grid_dir_consec = 0
                    state.neutral_grid_dir_previa = 'NEUTRAL'
                    state.estado = 'NEUTRAL_GRID'
                    state.neutral_grid_timestamp = int(datetime.now(pytz.UTC).timestamp())
                    print(f"  [NEUTRAL_GRID] {symbol} -> NEUTRAL_GRID | {razon_grid}")

                    i15_gp = self.indicadores_15m.get(symbol, {})
                    i4h_gp = self.indicadores_4h.get(symbol, {})
                    atr_gp = i15_gp.get('atr', price * 0.01)
                    grid_params, gp_rechazos = self.calcular_parametros_grid_blindado(
                        price=price, direction='NEUTRAL', atr=atr_gp, i15=i15_gp, i4h=i4h_gp,
                        symbol=symbol, state=state
                    )
                    state.grid_params_neutral = grid_params
                    if grid_params and self.grid_simulator:
                        await self.grid_simulator.queue.put({
                            'tipo': 'INICIAR_GRID',
                            'symbol': symbol,
                            'grid_params': grid_params,
                            'precio_actual': price,
                        })
                        i1m_actual = self.indicadores_1m.get(symbol, {})
                        if i1m_actual:
                            await self.grid_simulator.queue.put({
                                'tipo': 'TICK',
                                'symbol': symbol,
                                'high': i1m_actual.get('high', price),
                                'low': i1m_actual.get('low', price),
                                'close': i1m_actual.get('close', price),
                                'timestamp': i1m_actual.get('timestamp', int(datetime.now(pytz.UTC).timestamp()))
                            })
                    elif not grid_params:
                        print(f"  [NEUTRAL_GRID] {symbol} No se pudieron calcular params del grid: {gp_rechazos}")

                    if grid_params is None:
                        grid_params = {}
                    if self.audit_logger:
                        await self.audit_logger.log_cambio_estado(
                            symbol=symbol,
                            de=state._prev_estado if state._prev_estado != 'NEUTRAL_GRID' else 'MONITOREO',
                            a='NEUTRAL_GRID',
                            direccion='NEUTRAL',
                            score_macro=score_macro
                        )
                        state._prev_estado = 'NEUTRAL_GRID'
                    await self.emitir_alerta(symbol, 'NEUTRAL_GRID', 'NEUTRAL', score_macro, [], grid_params, price)
                    entro_neutral_grid = True

        if state.estado == 'NEUTRAL_GRID' and direction != 'NEUTRAL':
            if state.neutral_grid_dir_previa == direction:
                state.neutral_grid_dir_consec += 1
            else:
                state.neutral_grid_dir_consec = 1
                state.neutral_grid_dir_previa = direction

            if state.neutral_grid_dir_consec >= 2:
                state.estado = 'MONITOREO'
                state.neutral_grid_timestamp = 0
                state.grid_params_neutral = None
                state.neutral_grid_dir_consec = 0
                if self.audit_logger:
                    await self.audit_logger.log_evento_grid_simulacion(
                        symbol=symbol, tipo='NEUTRAL_GRID_ABORT', grid_id=0,
                        evento_simulacion={'razon': 'direccion_detectada_2velas', 'nueva_direccion': direction},
                        pnl_acumulado=0.0
                    )
                if self.grid_simulator:
                    await self.grid_simulator.queue.put({
                        'tipo': 'CERRAR_GRID_SUAVE',
                        'symbol': symbol,
                        'razon': 'direccion_detectada_2velas'
                    })
                print(f"  [NEUTRAL_GRID] {symbol} -> MONITOREO (direccion {direction} confirmada x2)")
            else:
                print(f"  [NEUTRAL_GRID] {symbol} Dir {direction} ({state.neutral_grid_dir_consec}/2 velas)")

        return entro_neutral_grid


    async def _loguear_y_auditar_macro(self, symbol: str, state: SignalState,
                                        score_macro: int, score_minimo: int,
                                        rechazos: list, direction: str,
                                        filtro_aprobado: bool, umbral_entrada: int,
                                        umbral_bloqueado: bool, adx: float,
                                        i15: dict, entro_neutral_grid: bool,
                                        trend_strength: str, coin_cfg: dict):
        """
        FASE 4.1: Logs, near-misses, auditoría, métricas diarias y trackeo de precio.
        """
        if entro_neutral_grid:
            return

        for r in rechazos:
            tipo = r.split(':')[0] if ':' in r else r
            state.metricas_dia['rechazos_frecuentes'][tipo] = state.metricas_dia['rechazos_frecuentes'].get(tipo, 0) + 1

        await self._evaluar_despausa_automatica(symbol, state, score_macro)

        if state.moneda_pausada and not state.moneda_pausada_manual:
            state.filtro_macro_aprobado = False
            await self._log_continuo(symbol, i15)
            await self._evaluar_near_misses(symbol, score_macro, score_minimo, rechazos, direction, filtro_aprobado)
            return

        if state.estado == 'NEUTRAL_GRID':
            state.score_bajo_desde = None
        elif score_macro < 50:
            if state.score_bajo_desde is None:
                state.score_bajo_desde = int(datetime.now(pytz.UTC).timestamp())
            else:
                segundos_bajo = int(datetime.now(pytz.UTC).timestamp()) - state.score_bajo_desde
                if segundos_bajo > CONFIG.pausa_inactividad_horas * 3600 and not state.moneda_pausada:
                    state.moneda_pausada = True
                    state.moneda_pausada_razon = f"Score < 50 durante {segundos_bajo/60:.0f}min"
                    state.moneda_pausada_timestamp = int(datetime.now(pytz.UTC).timestamp())
                    print(f"  [PAUSA] {symbol} AUTO-PAUSADA por inactividad | {state.moneda_pausada_razon}")
        else:
            state.score_bajo_desde = None

        await self._log_continuo(symbol, i15)
        await self._evaluar_near_misses(symbol, score_macro, score_minimo, rechazos, direction, filtro_aprobado)

        if self.audit_logger:
            estado_previo = state._prev_filtro_aprobado
            if estado_previo != state.filtro_macro_aprobado:
                umbral_legible = self._umbral_a_string(score_minimo)
                umbral_real = umbral_entrada if not umbral_bloqueado else (
                    75 if adx < 25 else 70 if adx <= 35 else 65
                )

                contexto_macro = {
                    'precio': i15.get('close'),
                    'rsi': i15.get('rsi'),
                    'adx': adx,
                    'atr': i15.get('atr'),
                    'ema200_15m': i15.get('ema200_15m'),
                    'ema50_15m': i15.get('ema50_15m'),
                    'macd_hist': i15.get('macd_hist'),
                    'volumen_ratio': i15.get('volume') / i15.get('volume_sma20', 1) if i15.get('volume_sma20') else 0,
                    'mfm_sma5': i15.get('mfm_sma5'),
                    'direccion': direction,
                    'score_macro': score_macro,
                    'umbral_aplicado': umbral_legible,
                    'umbral_real': umbral_real,
                    'umbral_bloqueado': umbral_bloqueado,
                    'estado_maquina': state.estado,
                    'pausa_auto': state.moneda_pausada,
                    'pausa_manual': state.moneda_pausada_manual,
                    'trend_strength': trend_strength,
                    'atr_percentil': self._get_atr_percentil(symbol),
                    'categoria': coin_cfg.get('category', 'default'),
                    'adx_reject_aplicado': coin_cfg.get('adx_reject'),
                    'mfm_umbral_aplicado': coin_cfg.get('mfm_umbral'),
                    'grid_timeout_aplicado': coin_cfg.get('grid_timeout'),
                    'volume_threshold_aplicado': coin_cfg.get('volume_threshold'),
                }
                await self.audit_logger.log_cambio_estado(
                    symbol=symbol,
                    de='FILTRO_RECHAZADO' if not estado_previo else 'FILTRO_APROBADO',
                    a='FILTRO_APROBADO' if state.filtro_macro_aprobado else 'FILTRO_RECHAZADO',
                    direccion=state.direccion_filtro or state.direccion_ultima_valida,
                    contexto_macro=contexto_macro,
                    score_macro=score_macro
                )
                state._prev_filtro_aprobado = state.filtro_macro_aprobado

        if filtro_aprobado:
            mfm_str = f" | MFM={i15.get('mfm_sma5', 0):.3f}" if i15.get('mfm_sma5', 0) != 0 else ""
            umbral_str = self._umbral_a_string(score_minimo)
            print(f"  [FILTRO] {symbol} Filtro Macro OK ({direction}) | Score: {score_macro} | Umbral: {umbral_str} | RSI: {i15.get('rsi', 0):.1f} | ADX: {adx:.1f}{mfm_str}")
        else:
            if state.estado == 'ARMED':
                print(f"  [FILTRO] {symbol} Filtro Macro ROTO | Rechazos: {rechazos[:2]} | Score: {score_macro} | Umbral: {self._umbral_a_string(score_minimo)}")
            elif state.estado in ('MONITOREO',):
                if score_macro > 40:
                    print(f"  [FILTRO] {symbol} Filtro Macro NO apto | Score: {score_macro} | {rechazos[:1]}")

        if self.audit_logger:
            i1m_nm = self.indicadores_1m.get(symbol, {})
            ts_nm = i1m_nm.get('timestamp')
            if ts_nm:
                ts = datetime.fromtimestamp(ts_nm / 1000, tz=pytz.UTC) if ts_nm > 1e12 else datetime.now(pytz.UTC)
            else:
                ts = datetime.now(pytz.UTC)
            await self.audit_logger.trackear_precio_near_miss(symbol, i1m_nm.get('close', i15.get('close', 0)), ts)


    async def evaluar_filtro_macro(self, symbol: str):
        """FASE 4.1: Director de orquesta del filtro macro. Delega en 3 métodos especializados."""
        i15 = self.indicadores_15m.get(symbol)
        i4h = self.indicadores_4h.get(symbol)
        state = self.states[symbol]

        coin_cfg = get_coin_config(symbol)

        if state.moneda_pausada_manual:
            state.filtro_macro_aprobado = False
            await self._log_continuo(symbol, i15 or {})
            await self._evaluar_near_misses(symbol, 0, 0, [], None, False)
            return

        if not i15 or not i4h:
            state.filtro_macro_aprobado = False
            await self._log_continuo(symbol, i15 or {})
            await self._evaluar_near_misses(symbol, 0, 0, [], None, False)
            return

        required_15m = ['rsi', 'adx', 'atr', 'ema200_15m', 'macd_hist',
                        'macd_hist_prev', 'volume', 'volume_sma20']
        if any(i15.get(k) is None for k in required_15m):
            state.filtro_macro_aprobado = False
            await self._log_continuo(symbol, i15 or {})
            await self._evaluar_near_misses(symbol, 0, 0, [], None, False)
            return
        if i4h.get('ema200_4h') is None:
            state.filtro_macro_aprobado = False
            await self._log_continuo(symbol, i15 or {})
            await self._evaluar_near_misses(symbol, 0, 0, [], None, False)
            return

        price = i15['close']
        ema15 = i15['ema200_15m']
        ema4h = i4h['ema200_4h']
        atr = i15['atr']
        adx = i15['adx']
        rsi = i15['rsi']
        macd_hist = i15['macd_hist']
        macd_hist_prev = i15['macd_hist_prev']
        vol_ratio = i15['volume'] / i15['volume_sma20'] if i15['volume_sma20'] > 0 else 0
        mfm = i15.get('mfm_15m', 0.0)
        mfm_sma5 = i15.get('mfm_sma5', 0.0)
        ema50 = i15.get('ema50_15m') or i15.get('ema25_15m', ema15)

        trend_threshold_15m = ema15 * 0.002
        trend_threshold_4h = ema4h * 0.02
        trend_threshold_50 = ema50 * 0.002 if ema50 is not None else trend_threshold_15m

        if ema50 is not None and price > ema4h + trend_threshold_4h and price > ema50 + trend_threshold_50:
            direction = 'LONG'
        elif ema50 is not None and price < ema4h - trend_threshold_4h and price < ema50 - trend_threshold_50:
            direction = 'SHORT'
        elif ema50 is None:
            if price > ema4h + trend_threshold_4h and price > ema15 + trend_threshold_15m:
                direction = 'LONG'
            elif price < ema4h - trend_threshold_4h and price < ema15 - trend_threshold_15m:
                direction = 'SHORT'
            else:
                direction = 'NEUTRAL'
        else:
            direction = 'NEUTRAL'

        trend_strength = "NEUTRAL"
        if direction == 'SHORT':
            if adx > 30:
                trend_strength = "BAJISTA_FUERTE"
            elif adx > 20:
                trend_strength = "BAJISTA_MODERADA"
        elif direction == 'LONG':
            if adx > 30:
                trend_strength = "ALCISTA_FUERTE"
            elif adx > 20:
                trend_strength = "ALCISTA_MODERADA"

        # BLOQUE 1: CÁLCULO DE SCORE Y RECHAZOS
        score_macro, rechazos, umbral_entrada, umbral_mantenimiento, umbral_bloqueado = self._evaluar_condiciones_macro(
            symbol, coin_cfg, direction, trend_strength, price, adx, rsi, atr,
            macd_hist, macd_hist_prev, vol_ratio, mfm, mfm_sma5
        )

        # FASE 3.2: Alerta pre-disparo
        if (direction != 'NEUTRAL' and umbral_entrada > 0 and
            score_macro >= umbral_entrada * 0.90 and score_macro < umbral_entrada):
            notifier = getattr(self, 'notifier', None)
            if notifier:
                await notifier.enviar_telegram(
                    f"🟡 PRE-DISPARO — {symbol} | Score: {score_macro}/{umbral_entrada} "
                    f"({score_macro/umbral_entrada*100:.0f}%) | Dir: {direction} | "
                    f"ADX: {adx:.1f} | RSI: {rsi:.1f}"
                )

        # FASE 1.5: Seguimiento virtual NEUTRAL con convicción alta
        if direction == 'NEUTRAL' and score_macro >= 55 and adx >= 20 and adx <= 35 and self.audit_logger:
            await self.audit_logger.iniciar_seguimiento_near_miss(
                symbol=symbol,
                score=score_macro,
                umbral=umbral_entrada if umbral_entrada > 0 else 70,
                direccion='NEUTRAL',
                precio=price,
                contexto={
                    'tipo': 'NEUTRAL_CONVICCION',
                    'adx': adx,
                    'rsi': rsi,
                    'score_macro': score_macro,
                }
            )

        state.score_macro_actual = score_macro

        # BLOQUE 2: TRANSICIÓN A NEUTRAL_GRID
        entro_neutral_grid = await self._evaluar_transicion_neutral_grid(
            symbol, state, direction, score_macro, umbral_entrada, i15, price, coin_cfg
        )

        # BLOQUE 3: APROBACIÓN DEL FILTRO (FASE 4.3: Filtros ponderados opcional)
        if state.estado == 'ARMED':
            umbral_dinamico = self._calcular_umbral_dinamico(symbol)
            score_minimo = umbral_dinamico if umbral_dinamico > 0 else umbral_mantenimiento
        else:
            atr_percentil = self._get_atr_percentil(symbol)
            umbral_base_dinamico = self.calcular_umbral_base(symbol, atr_percentil, coin_cfg)
            score_minimo = umbral_base_dinamico

        # FASE 4.3: Score penalizado por tipo de rechazo
        score_penalizado = score_macro
        for r in rechazos:
            if 'ADX' in r: score_penalizado -= 15
            elif 'RSI' in r: score_penalizado -= 10
            elif 'Volumen' in r: score_penalizado -= 8
            elif 'MFM' in r: score_penalizado -= 5
            else: score_penalizado -= 5

        filtro_aprobado = False
        if len(rechazos) == 0 and direction != 'NEUTRAL' and score_macro >= score_minimo:
            filtro_aprobado = True
            state.direccion_filtro = direction
            state.direccion_ultima_valida = direction
            state.metricas_dia['veces_paso_umbral'] += 1
        elif (score_penalizado >= score_minimo and len(rechazos) <= 2 and
              direction != 'NEUTRAL' and not umbral_bloqueado):
            filtro_aprobado = True
            state.direccion_filtro = direction
            state.direccion_ultima_valida = direction
            state.metricas_dia['veces_paso_umbral'] += 1
            print(f"  [FILTRO] {symbol} DISPARO CON ADVERTENCIA | {score_macro}->{score_penalizado}")
        else:
            state.direccion_filtro = None

        # BLOQUE 4: LOGS, AUDITORÍA Y NEAR-MISSES
        await self._loguear_y_auditar_macro(
            symbol, state, score_macro, score_minimo, rechazos, direction,
            filtro_aprobado, umbral_entrada, umbral_bloqueado, adx, i15,
            entro_neutral_grid, trend_strength, coin_cfg
        )

    # ═══════════════════════════════════════════════════════════════════════════════
    # METODOS AUXILIARES
    # ═══════════════════════════════════════════════════════════════════════════════
    def _calcular_umbral_filtro(self, symbol: str) -> int:
        """Calcula el umbral base del filtro macro."""
        i15 = self.indicadores_15m.get(symbol, {})
        adx = i15.get('adx', 30)

        if adx is None or np.isnan(adx) or adx > 45:
            return self.UMBRAL_BLOQUEADO_ADX_EXTREMO
        elif adx < self.ADX_RECHAZO_MIN:
            return self.UMBRAL_BLOQUEADO_ADX_BAJO
        elif self.ADX_RECHAZO_MIN <= adx < 25:
            return 75
        elif 25 <= adx <= 35:
            return 70
        else:
            return 65

    def _umbral_a_string(self, umbral: int) -> str:
        """Convierte umbral numerico a string descriptivo."""
        if umbral == self.UMBRAL_BLOQUEADO_ADX_EXTREMO:
            return "BLOQUEADO_ADX_EXTREMO"
        elif umbral == self.UMBRAL_BLOQUEADO_ADX_BAJO:
            return "BLOQUEADO_ADX_BAJO"
        elif umbral < 0:
            return f"BLOQUEADO({umbral})"
        else:
            return str(umbral)

    async def _evaluar_despausa_automatica(self, symbol: str, state: SignalState, score_macro: int):
        if state.moneda_pausada_manual:
            return
        if not state.moneda_pausada:
            return
        if score_macro >= 50:
            tiempo_pausada = int(datetime.now(pytz.UTC).timestamp()) - state.moneda_pausada_timestamp
            state.moneda_pausada = False
            state.moneda_pausada_razon = None
            state.moneda_pausada_timestamp = 0
            state.score_bajo_desde = None
            print(f"  [RESUME] {symbol} AUTO-REANUDADA | Score recuperado a {score_macro} | Pausada durante {tiempo_pausada/60:.0f}min")
        else:
            if state.moneda_pausada_timestamp > 0:
                tiempo_pausada = int(datetime.now(pytz.UTC).timestamp()) - state.moneda_pausada_timestamp
                if tiempo_pausada % 300 == 0:
                    print(f"  [PAUSA] {symbol} sigue AUTO-PAUSADA | Score: {score_macro} | Tiempo pausada: {tiempo_pausada/60:.0f}min")

    # ═══════════════════════════════════════════════════════════════════════════════
    # MAQUINA DE ESTADOS (SIN COMMITMENT SCORE — transicion directa)
    # ═══════════════════════════════════════════════════════════════════════════════
    def _alimentar_historial_1m(self, symbol: str, data: dict):
        if not data:
            return

        registro = {
            'timestamp': data.get('timestamp', 0),
            'rsi_7': data.get('rsi_7'),
            'ema_300': data.get('ema_300'),
            'close': data.get('close'),
            'high': data.get('high'),
            'low': data.get('low'),
            'wick_upper_pct': data.get('wick_upper_pct', 0),
            'wick_lower_pct': data.get('wick_lower_pct', 0),
            'body_direction': data.get('body_direction', 0),
            'volume': data.get('volume', 0),
            'volume_sma20': data.get('volume_sma20', 1),
        }

        self._historial_1m[symbol].append(registro)
        if len(self._historial_1m[symbol]) > 5:
            self._historial_1m[symbol] = self._historial_1m[symbol][-5:]

    async def _actualizar_estado_maquina(self, symbol: str):
        state = self.states[symbol]
        i15 = self.indicadores_15m.get(symbol, {})
        timestamp_15m_actual = i15.get('timestamp', 0)

        if state.moneda_pausada or state.moneda_pausada_manual:
            if state.estado != 'MONITOREO':
                estado_anterior_log = state.estado
                state.estado = 'MONITOREO'
                state.velas_confirmacion = 0
                state.filtro_macro_aprobado = False
                state.direccion_filtro = None
                state.armed_timestamp = 0
                state.neutral_grid_timestamp = 0  # FASE 5: Reset
                tipo_pausa = "MANUAL" if state.moneda_pausada_manual else "AUTO"
                if self.audit_logger and estado_anterior_log != 'MONITOREO':
                    await self.audit_logger.log_cambio_estado(
                        symbol=symbol, de=estado_anterior_log, a='MONITOREO',
                        direccion=state.direccion_ultima_valida,
                        score_macro=state.score_macro_actual
                    )
                    state._prev_estado = 'MONITOREO'
                print(f"  [PAUSA] {symbol} -> MONITOREO (pausa {tipo_pausa} activa)")
            return

        # FASE 2.2: CIRCUIT BREAKER con PnL real del grid (Bug B)
        if state.ultimo_disparo_precio > 0 and not state.ultimo_disparo_evaluado:
            price_actual = self.indicadores_1m.get(symbol, {}).get('close', 0)

            # Prioridad 1: PnL real del grid simulator si existe
            pnl_grid = 0.0
            if self.grid_simulator and symbol in self.grid_simulator.simulaciones:
                pnl_grid = float(self.grid_simulator.simulaciones[symbol].pnl_neto)

            if pnl_grid != 0:
                fue_rentable = pnl_grid > 0
                razon_pnl = f"PnL grid: {pnl_grid:+.4f}"
            elif price_actual > 0:
                # Fallback: estimación por precio spot si no hay grid activo
                if state.ultimo_disparo_direccion == 'SHORT':
                    fue_rentable = price_actual <= state.ultimo_disparo_precio * 1.003
                else:
                    fue_rentable = price_actual >= state.ultimo_disparo_precio * 0.997
                razon_pnl = f"Precio spot: ${price_actual:.4f}"
            else:
                fue_rentable = True
                razon_pnl = "Sin precio disponible"

            if not fue_rentable:
                state.disparos_consecutivos += 1
                state.ultimo_disparo_fue_rentable = False
                print(f"  [CB] {symbol} Disparo en pérdida | #{state.disparos_consecutivos} | {razon_pnl}")
            else:
                state.disparos_consecutivos = 0
                state.ultimo_disparo_fue_rentable = True
                print(f"  [CB] {symbol} Disparo rentable | Reset contador | {razon_pnl}")

            state.ultimo_disparo_evaluado = True

            if state.disparos_consecutivos >= CONFIG.circuit_breaker_disparos:
                await self._activar_circuit_breaker(symbol)
                return
                
        # FASE 5: Si esta en NEUTRAL_GRID, evaluar aborto de emergencia por precio 1m
        # El aborto macro (ADX/RSI/EMA) se evalua en el loop principal (run) con evaluar_aborto_neutral_grid
        # F2.4: Aborto de emergencia por ruptura de límites del grid + 0.5% margen
        if state.estado == 'NEUTRAL_GRID':
            grid_params = getattr(state, 'grid_params_neutral', None)
            if grid_params:
                price_1m = self.indicadores_1m.get(symbol, {}).get('close', 0)
                lower = grid_params.get('lower_limit', 0)
                upper = grid_params.get('upper_limit', 0)
                # Validación defensiva: lower debe ser < upper
                if price_1m > 0 and lower > 0 and upper > 0 and upper > lower:
                    margen = 0.005
                    if price_1m < lower * (1 - margen) or price_1m > upper * (1 + margen):
                        state.estado = 'MONITOREO'
                        state.neutral_grid_timestamp = 0
                        state.grid_params_neutral = None
                        print(f"  [NEUTRAL_GRID] {symbol} -> MONITOREO (aborto emergencia: precio {price_1m:.4f} fuera de grid [{lower:.4f}, {upper:.4f}])")
                        if self.audit_logger:
                            await self.audit_logger.log_cambio_estado(
                                symbol=symbol, de='NEUTRAL_GRID', a='MONITOREO',
                                direccion='NEUTRAL', score_macro=state.score_macro_actual,
                                contexto_macro={'razon': 'Aborto emergencia 1m: precio fuera de grid + 0.5%'}
                            )
                            state._prev_estado = 'MONITOREO'
                            await self.audit_logger.log_evento_grid_simulacion(
                                symbol=symbol, tipo='NEUTRAL_GRID_ABORT', grid_id=0,
                                evento_simulacion={'razon': 'aborto_emergencia_1m', 'precio': price_1m, 'lower': lower, 'upper': upper},
                                pnl_acumulado=0.0
                            )
                        # Notificar al simulador solo si existe
                        if self.grid_simulator:
                            await self.grid_simulator.queue.put({
                                'tipo': 'FINALIZAR_GRID',
                                'symbol': symbol,
                                'razon': 'aborto_emergencia_1m'
                            })
                        else:
                            print(f"  ⚠️ [NEUTRAL_GRID] {symbol} grid_simulator no disponible, aborto solo en estado")
                        return
                elif lower > 0 and upper > 0 and upper <= lower:
                    print(f"  ⚠️ [NEUTRAL_GRID] {symbol} Grid params inválidos: upper({upper}) <= lower({lower}), abortando")
                    state.estado = 'MONITOREO'
                    state.neutral_grid_timestamp = 0
                    state.grid_params_neutral = None
                    return
            # Si no hay aborto de emergencia, mantener NEUTRAL_GRID sin seguir máquina tradicional
            return

        estado_anterior_log = state.estado

        if state.estado == 'COOLDOWN':
            if timestamp_15m_actual != state.ultimo_disparo_timestamp_15m:
                if state.filtro_macro_aprobado:
                    state.estado = 'ARMED'
                    state.velas_confirmacion = 0
                    state.armed_timestamp = int(datetime.now(pytz.UTC).timestamp() * 1000)
                    print(f"  [ESTADO] {symbol} -> ARMED (nueva vela 15m, filtro activo)")
                else:
                    state.estado = 'MONITOREO'
                    state.velas_confirmacion = 0
                    state.direccion_filtro = None
                    state.armed_timestamp = 0
                    print(f"  [ESTADO] {symbol} -> MONITOREO (nueva vela 15m, filtro perdido)")
            return

        if state.estado == 'MONITOREO':
            if state.filtro_macro_aprobado and state.direccion_filtro:
                # FASE 6: Transicion DIRECTA a ARMED (sin commitment score)
                state.estado = 'ARMED'
                state.velas_confirmacion = 0
                state.armed_timestamp = int(datetime.now(pytz.UTC).timestamp() * 1000)
                print(f"  [ESTADO] {symbol} -> ARMED ({state.direccion_filtro})")
            else:
                if CONFIG.hysteresis_suavizada and state.velas_confirmacion > 0:
                    state.velas_confirmacion = max(0, state.velas_confirmacion - CONFIG.hysteresis_decremento)
                else:
                    state.velas_confirmacion = 0

        elif state.estado == 'ARMED':
            ahora_ms = int(datetime.now(pytz.UTC).timestamp() * 1000)
            if state.armed_timestamp > 0:
                tiempo_en_armed_ms = ahora_ms - state.armed_timestamp
                armed_timeout_ms = self.ARMED_TIMEOUT_MIN * 60 * 1000
                if tiempo_en_armed_ms > armed_timeout_ms:
                    state.estado = 'MONITOREO'
                    state.velas_confirmacion = 0
                    state.filtro_macro_aprobado = False
                    state.direccion_filtro = None
                    state.armed_timestamp = 0
                    print(f"  [ESTADO] {symbol} -> MONITOREO (timeout ARMED: {tiempo_en_armed_ms/60000:.0f}min)")
                    if self.audit_logger:
                        await self.audit_logger.log_cambio_estado(
                            symbol=symbol, de='ARMED', a='MONITOREO',
                            direccion=state.direccion_ultima_valida
                        )
                        state._prev_estado = 'MONITOREO'
                    return

            if not state.filtro_macro_aprobado or not state.direccion_filtro:
                if CONFIG.hysteresis_suavizada:
                    state.velas_confirmacion += 1
                    if state.velas_confirmacion >= CONFIG.hysteresis_velas:
                        state.estado = 'MONITOREO'
                        state.velas_confirmacion = 0
                        state.filtro_macro_aprobado = False
                        state.direccion_filtro = None
                        state.armed_timestamp = 0
                        print(f"  [ESTADO] {symbol} -> MONITOREO (filtro roto, {CONFIG.hysteresis_velas} velas contrarias)")
                else:
                    state.velas_confirmacion += 1
                    if state.velas_confirmacion >= CONFIG.hysteresis_velas:
                        state.estado = 'MONITOREO'
                        state.velas_confirmacion = 0
                        state.filtro_macro_aprobado = False
                        state.direccion_filtro = None
                        state.armed_timestamp = 0
                        print(f"  [ESTADO] {symbol} -> MONITOREO (filtro roto, {CONFIG.hysteresis_velas} velas 1m)")

        if estado_anterior_log != state.estado:
            if self.audit_logger:
                await self.audit_logger.log_cambio_estado(
                    symbol=symbol,
                    de=estado_anterior_log,
                    a=state.estado,
                    direccion=state.direccion_filtro or state.direccion_ultima_valida
                )
            state._prev_estado = state.estado

    # ═══════════════════════════════════════════════════════════════════════════════
    # UMBRAL DINAMICO PARA MANTENER ARMED
    # ═══════════════════════════════════════════════════════════════════════════════
    def _calcular_umbral_dinamico(self, symbol: str) -> int:
        """Calcula umbral dinamico para mantener ARMED segun ATR historico."""
        atr_historico = self._atr15m_historico.get(symbol, [])
        if len(atr_historico) < 20:
            return self.SCORE_DISPARO_MIN

        atr_actual = atr_historico[-1]
        atr_p20 = np.percentile(atr_historico, 20)
        atr_p80 = np.percentile(atr_historico, 80)

        if atr_p80 > atr_p20:
            atr_normalizado = (atr_actual - atr_p20) / (atr_p80 - atr_p20)
        else:
            atr_normalizado = 0.5

        atr_normalizado = max(0.0, min(1.0, atr_normalizado))

        score_min = CONFIG.score_min_consolidacion - atr_normalizado * (CONFIG.score_min_consolidacion - CONFIG.score_min_expansion)
        score_min = int(round(score_min))

        rango = self._rango_reciente.get(symbol, {})
        highs = rango.get('highs', [])
        lows = rango.get('lows', [])
        i15 = self.indicadores_15m.get(symbol, {})
        price = i15.get('close', 0)

        if len(highs) >= 10 and len(lows) >= 10 and price > 0:
            recent_high = max(highs)
            recent_low = min(lows)
            rango_size = recent_high - recent_low

            if price > recent_high + rango_size * 0.01 or price < recent_low - rango_size * 0.01:
                score_min_pre_exp = max(CONFIG.score_min_expansion, score_min - 10)
                print(f"  [EXPANSION] {symbol} PRE-EXPANSION detectada | Umbral: {score_min} -> {score_min_pre_exp}")
                return score_min_pre_exp

        return score_min

    # ═══════════════════════════════════════════════════════════════════════════════
    # GATILLO Y DISPARO
    # ═══════════════════════════════════════════════════════════════════════════════
    async def evaluar_gatillo(self, symbol: str):
        i1m = self.indicadores_1m.get(symbol)
        state = self.states[symbol]

        if not i1m:
            return
        if state.estado != 'ARMED':
            return
        if not state.filtro_macro_aprobado or not state.direccion_filtro:
            return

        score_min_dinamico = self._calcular_umbral_dinamico(symbol)
        if state.score_macro_actual < score_min_dinamico:
            return
        if state.moneda_pausada or state.moneda_pausada_manual:
            return

        required_1m = ['rsi_7', 'ema_300', 'close', 'high', 'low', 'open']
        if any(i1m.get(k) is None for k in required_1m):
            return

        direction = state.direccion_filtro
        price = i1m['close']
        rsi_7 = i1m['rsi_7']
        ema_300 = i1m['ema_300']
        high = i1m['high']
        low = i1m['low']
        wick_upper = i1m.get('wick_upper_pct', 0)
        wick_lower = i1m.get('wick_lower_pct', 0)
        body_dir = i1m.get('body_direction', 0)

        i15 = self.indicadores_15m.get(symbol, {})
        timestamp_15m_actual = i15.get('timestamp', 0)
        if timestamp_15m_actual == state.ultimo_disparo_timestamp_15m:
            return

        mecha_valida_short = i1m.get('mecha_valida_short')
        mecha_valida_long = i1m.get('mecha_valida_long')

        if mecha_valida_short is None:
            mecha_valida_short = (
                high >= ema_300 * 0.9995 and
                price < high * 0.999 and
                body_dir <= 0 and
                wick_upper >= CONFIG.wick_min_pct
            )
        if mecha_valida_long is None:
            mecha_valida_long = (
                low <= ema_300 * 1.0005 and
                price > low * 1.001 and
                body_dir >= 0 and
                wick_lower >= CONFIG.wick_min_pct
            )

        if direction == 'SHORT':
            rsi_trigger = rsi_7 >= CONFIG.rsi_micro_short_trigger
            if rsi_trigger and mecha_valida_short:
                await self._procesar_disparo(symbol, direction, price, i1m, i15)

        elif direction == 'LONG':
            rsi_trigger = rsi_7 <= CONFIG.rsi_micro_long_trigger
            if rsi_trigger and mecha_valida_long:
                await self._procesar_disparo(symbol, direction, price, i1m, i15)

    async def _procesar_disparo(self, symbol: str, direction: str, price: float, i1m: dict, i15: dict):
        state = self.states[symbol]
        i4h = self.indicadores_4h.get(symbol, {})
        atr_15m = i15.get('atr', price * 0.001)

        params, rechazos = self.calcular_parametros_grid_blindado(
            price, direction, atr_15m, i15, i4h, symbol, state
        )

        if rechazos:
            print(f"  [DISPARO] {symbol} DISPARO RECHAZADO | {rechazos[0]}")

            if self.audit_logger:
                await self.audit_logger.log_rechazado(
                    symbol=symbol,
                    direccion=direction,
                    precio=price,
                    contexto_1m=i1m,
                    contexto_macro={
                        'rsi_15m': i15.get('rsi'),
                        'adx': i15.get('adx'),
                        'score_macro': self._calcular_score_macro(i15)
                    },
                    rechazos=rechazos,
                    score_macro=i15.get('score_macro')
                )

            await self.emitir_alerta(symbol, 'RECHAZADO', direction, 0, rechazos, None, price)
            return

        score = self._calcular_score_disparo(i1m, direction)

        state.estado = 'FIRE'
        state.ultimo_disparo_timestamp_15m = i15.get('timestamp', 0)
        state.ultima_direccion = direction
        state.ultimo_score = score
        state.ultimos_params = params
        # Guardar datos para evaluación de Circuit Breaker (Bug Crítico #3)
        state.ultimo_disparo_precio = price
        state.ultimo_disparo_direccion = direction
        state.ultimo_disparo_evaluado = False

        score_min_usado = self._calcular_umbral_dinamico(symbol)
        umbral_info = f" | Umbral: {score_min_usado}" if score_min_usado != self.SCORE_DISPARO_MIN else ""

        if params.get('auto_compressed'):
            print(f"  [FIRE] {symbol} -> FIRE ({direction}) | Score: {score}{umbral_info} | GRID AUTO-COMPRIMIDO")
        else:
            print(f"  [FIRE] {symbol} -> FIRE ({direction}) | Score: {score}{umbral_info} | Grid: {params['grid_count']} lineas")

        if self.audit_logger:
            contexto_1m = {
                'rsi_7': i1m.get('rsi_7'),
                'ema_300': i1m.get('ema_300'),
                'close': i1m.get('close'),
                'high': i1m.get('high'),
                'low': i1m.get('low'),
                'wick_upper_pct': i1m.get('wick_upper_pct'),
                'wick_lower_pct': i1m.get('wick_lower_pct'),
                'body_direction': i1m.get('body_direction'),
                'volume': i1m.get('volume'),
                'volume_sma20': i1m.get('volume_sma20'),
                'atr_1m': i1m.get('atr_1m'),
                'mecha_valida_short': i1m.get('mecha_valida_short'),
                'mecha_valida_long': i1m.get('mecha_valida_long'),
                'ema300_distancia_pct': i1m.get('ema300_distancia_pct')
            }

            contexto_15m = {
                'rsi': i15.get('rsi'),
                'adx': i15.get('adx'),
                'macd_hist': i15.get('macd_hist'),
                'atr': i15.get('atr'),
                'ema200_15m': i15.get('ema200_15m'),
                'ema50_15m': i15.get('ema50_15m'),
                'volume': i15.get('volume'),
                'volume_sma20': i15.get('volume_sma20'),
                'recent_high': i15.get('recent_high'),
                'recent_low': i15.get('recent_low'),
                'mfm_15m': i15.get('mfm_15m'),
                'mfm_sma5': i15.get('mfm_sma5')
            }

            contexto_4h = {
                'ema200_4h': i4h.get('ema200_4h'),
                'close': i4h.get('close')
            }

            await self.audit_logger.log_fire(
                symbol=symbol,
                direccion=direction,
                precio=price,
                contexto_1m=contexto_1m,
                contexto_15m=contexto_15m,
                contexto_4h=contexto_4h,
                grid_params=params,
                score_disparo=score
            )

        # FASE 6 FIX: Doble candado para FIRE (nunca emitir params=None)
        if params is None:
            params = {}
        await self.emitir_alerta(symbol, 'FIRE', direction, score, [], params, price)

        await asyncio.sleep(0.1)
        state.estado = 'COOLDOWN'
        print(f"  [ESTADO] {symbol} -> COOLDOWN")

    # ═══════════════════════════════════════════════════════════════════════════════
    # CALCULO DE PARAMETROS DEL GRID
    # ═══════════════════════════════════════════════════════════════════════════════
    def calcular_parametros_grid_blindado(self, price, direction, atr, i15, i4h, symbol, state) -> tuple:
        rechazos = []
        recent_high = i15.get('recent_high', price * 1.02) if i15 else price * 1.02
        recent_low = i15.get('recent_low', price * 0.98) if i15 else price * 0.98

        atr_historico = self._atr_historico.get(symbol, [])
        if len(atr_historico) >= 20:
            atr_percentil_95 = np.percentile(atr_historico, CONFIG.grid_atr_percentil)
            atr_seguro = min(atr, atr_percentil_95)
        else:
            atr_seguro = min(atr, price * 0.05) if atr else price * 0.01

        atr_pct = (atr_seguro / price) * 100 if price > 0 else 0
        if atr_pct < 0.5:
            mult = CONFIG.grid_rango_mult_min
        elif atr_pct > 2.0:
            mult = CONFIG.grid_rango_mult_max
        else:
            mult = CONFIG.grid_rango_mult_min + (atr_pct - 0.5) * (CONFIG.grid_rango_mult_max - CONFIG.grid_rango_mult_min) / 1.5

        rango_total = atr_seguro * mult

        upper = price + (rango_total / 2)
        lower = price - (rango_total / 2)

        capital = state.capital_actual if state else CONFIG.grid_default_capital
        leverage = CONFIG.grid_default_leverage
        poder_total = capital * leverage
        notional_min = CONFIG.grid_notional_min

        max_grids_posibles = int(poder_total / notional_min)

        if max_grids_posibles < CONFIG.grid_min_grids:
            rechazos.append(f"Capital insuficiente: {max_grids_posibles} grids posibles, minimo {CONFIG.grid_min_grids}")
            return None, rechazos

        grid_count = min(max_grids_posibles, CONFIG.grid_max_grids_hard)

        max_grids_por_densidad = int(rango_total / (atr_seguro * CONFIG.grid_min_dist_atr))
        grid_count = min(grid_count, max(2, max_grids_por_densidad))
        grid_count = max(grid_count, CONFIG.grid_min_grids)
        
        # FASE 4.2: Limitar grids por perfil de moneda
        coin_cfg = get_coin_config(symbol)
        perfil_max = coin_cfg.get('grid_niveles_max', CONFIG.grid_max_grids_hard)
        grid_count = min(grid_count, perfil_max)

        step_usdt = rango_total / grid_count if grid_count > 0 else 0
        step_pct = (step_usdt / price) * 100 if price > 0 else 0

        # FASE 2.5: Limitar grids para monedas caras (evita qty mínima inválida)
        if price > 100 and grid_count > 6:
            grid_count = 6
            step_usdt = rango_total / grid_count if grid_count > 0 else 0
            step_pct = (step_usdt / price) * 100 if price > 0 else 0
            print(f"  [GRID] {symbol} Grid limitado a 6 por precio > $100")

        # FASE 2.5: Validar distancia mínima entre grids (breakeven real)
        min_dist_pct = 2 * CONFIG.grid_fee_rate * 100 + CONFIG.grid_slippage * 100 + 0.1
        if step_pct < min_dist_pct:
            rechazos.append(f"step_pct {step_pct:.3f}% < min {min_dist_pct:.3f}%")
            return None, rechazos

        comisiones_ciclo = 2 * CONFIG.grid_fee_rate          # 0.10% (2 órdenes × 0.05%)
        slippage_total = 2 * CONFIG.grid_slippage              # 0.10% (2 órdenes × 0.05%)
        breakeven = (comisiones_ciclo + slippage_total) * 100  # 0.20% costo real por par
        margen_seguridad = breakeven * CONFIG.grid_breakeven_mult

        auto_compressed = False
        if CONFIG.grid_auto_compress and step_pct < margen_seguridad and grid_count > 2:
            step_pct_objetivo = margen_seguridad
            step_usdt_objetivo = (step_pct_objetivo / 100) * price
            grid_count_nuevo = max(2, int(rango_total / step_usdt_objetivo))

            if grid_count_nuevo < grid_count:
                grid_count = grid_count_nuevo
                step_usdt = rango_total / grid_count
                step_pct = (step_usdt / price) * 100
                auto_compressed = True
                print(f"  [GRID] {symbol} Auto-compresion: {grid_count} grids -> step_pct {step_pct:.3f}%")

        if step_pct < breakeven:
            rechazos.append(f"step_pct {step_pct:.3f}% < breakeven {breakeven:.3f}%. Grid imposible de rentabilizar")
            return None, rechazos

        posicion = (price - lower) / rango_total if rango_total > 0 else 0.5
        posicion_alerta = False
        if posicion > CONFIG.grid_posicion_alerta_max or posicion < CONFIG.grid_posicion_alerta_min:
            posicion_alerta = True
            print(f"  [GRID] {symbol} Posicion en rango extrema: {posicion:.1%}")

        rentable = step_pct >= breakeven

        qty_por_orden = round(poder_total / grid_count / price, 4) if grid_count > 0 and price > 0 else 0

        # FASE 2.5: Validar qty mínima para monedas > $100
        if qty_por_orden < 1.0 and price > 100:
            rechazos.append(f"Qty {qty_por_orden:.3f} < 1.0 para moneda > $100")
            return None, rechazos

        return {
            'direction': direction,
            'upper_limit': round(float(upper), 4),
            'lower_limit': round(float(lower), 4),
            'grid_count': grid_count,
            'step_usdt': round(float(step_usdt), 4),
            'step_pct': round(float(step_pct), 3),
            'breakeven_pct': round(breakeven, 3),
            'capital_sugerido': capital,
            'apalancamiento_sugerido': leverage,
            'notional_por_orden': round(poder_total / grid_count, 2) if grid_count > 0 else 0,
            'qty_por_orden': qty_por_orden,
            'margen_sobre_breakeven': round(step_pct - breakeven, 3),
            'rentable': rentable,
            'posicion_en_rango': round(float(posicion), 2),
            'recent_high': round(float(recent_high), 4),
            'recent_low': round(float(recent_low), 4),
            'auto_compressed': auto_compressed,
            'posicion_extrema': posicion_alerta,
            'atr_seguro': round(float(atr_seguro), 6),
            'rango_mult': round(float(mult), 2),
        }, rechazos

    # ═══════════════════════════════════════════════════════════════════════════════
    # CIRCUIT BREAKER Y METODOS AUXILIARES
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _activar_circuit_breaker(self, symbol: str):
        state = self.states[symbol]
        state.circuit_breaker_activo = True
        pausa_ms = CONFIG.circuit_breaker_pausa_seg * 1000
        state.circuit_breaker_hasta = int(datetime.now(pytz.UTC).timestamp() * 1000) + pausa_ms

        state.capital_actual = max(
            CONFIG.grid_default_capital * CONFIG.circuit_breaker_reduccion_capital,
            CONFIG.grid_notional_min * CONFIG.grid_min_grids / CONFIG.grid_default_leverage
        )

        print(f"  [CB] {symbol} CIRCUIT BREAKER ACTIVADO | Pausa: {CONFIG.circuit_breaker_pausa_seg}s | "
              f"Capital reducido: ${state.capital_actual:.2f}")

        # Forzar estado de la máquina a COOLDOWN (bloquea nuevos disparos)
        estado_previo = state.estado
        state.estado = 'COOLDOWN'
        state.velas_confirmacion = 0
        state.filtro_macro_aprobado = False
        state.direccion_filtro = None
        state.armed_timestamp = 0
        print(f"  [CB] {symbol} Estado forzado a COOLDOWN (era: {estado_previo})")

        if self.audit_logger:
            await self.audit_logger.log_cambio_estado(
                symbol=symbol,
                de=estado_previo,
                a='COOLDOWN',
                direccion=state.direccion_ultima_valida,
                score_macro=0,
                contexto_macro={'razon': 'Circuit Breaker activado'}
            )
            state._prev_estado = 'COOLDOWN'

        # V5.9.2 MEJORA #9: Si CB afecta grid neutral, abortar grid activo
        if CONFIG.circuit_breaker_afecta_grid_neutral and estado_previo == 'NEUTRAL_GRID':
            state.neutral_grid_timestamp = 0
            state.grid_params_neutral = None
            print(f"  [CB] {symbol} Grid neutral ABORTADO por Circuit Breaker")
            if self.audit_logger:
                await self.audit_logger.log_evento_grid_simulacion(
                    symbol=symbol, tipo='NEUTRAL_GRID_ABORT', grid_id=0,
                    evento_simulacion={'razon': 'circuit_breaker'}, pnl_acumulado=0.0
                )
            # Notificar al simulador para finalizar grid
            if self.grid_simulator:
                await self.grid_simulator.queue.put({
                    'tipo': 'FINALIZAR_GRID',
                    'symbol': symbol,
                    'razon': 'circuit_breaker'
                })

        if self.audit_logger:
            await self.audit_logger.log_circuit_breaker(
                symbol=symbol,
                direccion=state.direccion_filtro or state.direccion_ultima_valida,
                rechazos=[f"{state.disparos_consecutivos} disparos en perdida"]
            )

        await self.emitir_alerta(symbol, 'CIRCUIT_BREAKER', state.direccion_filtro or 'NEUTRAL', 0,
                                 [f"{state.disparos_consecutivos} disparos en perdida"], None, 0)

    def _get_current_15m_timestamp(self) -> int:
        now = datetime.now(pytz.UTC)
        minute_15m = (now.minute // 15) * 15
        ts = now.replace(minute=minute_15m, second=0, microsecond=0)
        return int(ts.timestamp() * 1000)

    def _calcular_score_macro(self, i15: dict) -> int:
        score = 0
        if i15.get('adx', 0) >= 25:
            score += 25
        rsi = i15.get('rsi', 50)
        if 45 <= rsi <= 80:
            score += 20
        atr_pct = (i15.get('atr', 0) / i15.get('close', 1)) * 100
        if 0.15 <= atr_pct <= 2.0:
            score += 15
        vol_ratio = i15.get('volume', 0) / max(i15.get('volume_sma20', 1), 1)
        if vol_ratio >= 1.0:
            score += 10
        return min(100, score)

    def _calcular_score_disparo(self, i1m: dict, direction: str) -> int:
        score = 50

        if direction == 'SHORT':
            score += min(i1m.get('wick_upper_pct', 0) * 2, 20)
            if i1m['rsi_7'] > 80:
                score += 15
            elif i1m['rsi_7'] > 75:
                score += 10
            vol_sma = i1m.get('volume_sma20') or i1m['volume'] or 1
            vol_ratio = i1m['volume'] / vol_sma
            if vol_ratio > 1.5:
                score += 5
            atr1m = i1m.get('atr_1m', 0)
            if atr1m and i1m['close'] > 0:
                atr_pct = (atr1m / i1m['close']) * 100
                wick_pct = i1m.get('wick_upper_pct', 0)
                if wick_pct > atr_pct * 0.8:
                    score += 5

        elif direction == 'LONG':
            score += min(i1m.get('wick_lower_pct', 0) * 2, 20)
            if i1m['rsi_7'] < 20:
                score += 15
            elif i1m['rsi_7'] < 25:
                score += 10
            vol_ratio = i1m['volume'] / i1m.get('volume_sma20', i1m['volume'])
            if vol_ratio > 1.5:
                score += 5
            atr1m = i1m.get('atr_1m', 0)
            if atr1m and i1m['close'] > 0:
                atr_pct = (atr1m / i1m['close']) * 100
                wick_pct = i1m.get('wick_lower_pct', 0)
                if wick_pct > atr_pct * 0.8:
                    score += 5

        return min(100, score)

    async def emitir_alerta(self, symbol, tipo, direction, score, rechazos, params, price):
        evento = {
            'symbol': symbol,
            'tipo': tipo,
            'direction': direction,
            'score': score,
            'rechazos': rechazos,
            'params': params,
            'price': price,
            'timestamp': datetime.now(pytz.UTC).isoformat(),
            'estado_maquina': self.states[symbol].estado,
            'indicadores_15m': self.indicadores_15m.get(symbol, {})  # V6.0: Para notificaciones enriquecidas
        }
        await self.queue_out.put(evento)
