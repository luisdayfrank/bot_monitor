import asyncio
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Set
from config import CONFIG
import pytz


class SignalState:
    """Estado de la máquina de estados por símbolo — V5.2 MFM + Correcciones."""
    def __init__(self):
        self.estado = 'MONITOREO'
        self.velas_confirmacion = 0
        self.ultima_direccion = None
        self.ultimo_score = 0
        self.ultimos_params = None
        self.ultimo_disparo_timestamp_15m = 0
        self.filtro_macro_aprobado = False
        self.direccion_filtro = None
        self.ultimo_filtro_timestamp = 0

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 3: Circuit breaker
        # ═══════════════════════════════════════════════════════════════════════════════
        self.disparos_consecutivos = 0
        self.ultimo_disparo_fue_rentable = True
        self.circuit_breaker_activo = False
        self.circuit_breaker_hasta = 0
        self.capital_actual = CONFIG.grid_default_capital

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 4.5: Tracking de auditoría
        # ═══════════════════════════════════════════════════════════════════════════════
        self._prev_filtro_aprobado = None
        self._prev_estado = 'MONITOREO'

        # ═══════════════════════════════════════════════════════════════════════════════
        # CORRECCIONES POST-ANÁLISIS CRUZADO (Jun 2026)
        # ═══════════════════════════════════════════════════════════════════════════════
        self.score_macro_actual = 0
        self.armed_timestamp = 0
        self.direccion_ultima_valida = None
        self.score_bajo_desde = None

        # ═══════════════════════════════════════════════════════════════════════════════
        # V4.2: PAUSA MANUAL (nueva)
        # ═══════════════════════════════════════════════════════════════════════════════
        # moneda_pausada = True → pausa automática por inactividad (score bajo)
        # moneda_pausada_manual = True → pausa manual por comando del usuario
        # La pausa manual tiene PRIORIDAD sobre la automática
        self.moneda_pausada = False           # Pausa automática por inactividad
        self.moneda_pausada_manual = False    # Pausa manual por comando usuario
        self.moneda_pausada_razon = None      # Razón de la última pausa automática
        self.moneda_pausada_timestamp = 0     # Cuándo se pausó automáticamente

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5.4: COMMITMENT SCORE (historial de 1m para confirmación real)
        # ═══════════════════════════════════════════════════════════════════════════════
        # Buffer de últimas 2 velas 1m para evaluar commitment
        self.commitment_historial_1m: list = []  # [{rsi_7, ema300_dist, close, timestamp}, ...]
        self.commitment_score_actual: int = 0    # 0-100, score de commitment
        self.commitment_velas_requeridas: int = 2  # Velas consecutivas alineadas requeridas


class SignalGenerator:
    """
    Generador de señales V5.2 — MFM + Correcciones Pausa Manual/Automática.

    CORRECCIONES APLICADAS:
    ────────────────────────
    1. FIX: Despausa automática ahora funciona correctamente (bug #4)
    2. NUEVO: Pausa manual por comando Telegram (/pause, /resume)
    3. NUEVO: Persistencia de pausa manual en SignalState
    4. FIX: PAUSA_INACTIVIDAD_HORAS movido a config.py
    5. NUEVO: Diferenciación clara entre pausa automática y manual en logs
    6. NUEVO: Comandos /pause, /resume, /pause_all, /resume_all, /list_paused

    FASE 5.2 NUEVO:
    ────────────────
    7. MFM (Money Flow Multiplier) integrado en filtro macro.
       El bonus de volumen ahora depende de que el MFM esté alineado
       con la dirección operativa. Volumen contradictorio = penalización.
    8. Nuevo rechazo: "MFM contradictorio" cuando volumen presiona
       en dirección opuesta al setup.
    """

    def __init__(self, queue_in: asyncio.Queue, queue_out: asyncio.Queue):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.states: Dict[str, SignalState] = {s: SignalState() for s in CONFIG.symbols}

        self.indicadores_1m: Dict[str, dict] = {}
        self.indicadores_15m: Dict[str, dict] = {}
        self.indicadores_4h: Dict[str, dict] = {}

        # FASE 3: ATR histórico por símbolo para percentil 95
        self._atr_historico: Dict[str, list] = {s: [] for s in CONFIG.symbols}

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5.3: BUFFER HISTÓRICO DE ATR(15m) PARA UMBRAL DINÁMICO
        # ═══════════════════════════════════════════════════════════════════════════════
        # Ventana de velas 15m para calcular percentiles del ATR
        self._atr15m_historico: Dict[str, list] = {s: [] for s in CONFIG.symbols}
        # Rango reciente (high/low de últimas N velas) para detectar rupturas
        self._rango_reciente: Dict[str, dict] = {s: {'highs': [], 'lows': []} for s in CONFIG.symbols}

        # FASE 5.4: BUFFER DE HISTORIAL 1M PARA COMMITMENT SCORE
        self._historial_1m: Dict[str, list] = {s: [] for s in CONFIG.symbols}

        # FASE 4.5: AuditLogger (inyectado desde main.py)
        self.audit_logger = None

    # ================================================================
    # CONSTANTES DE CORRECCIÓN
    # ================================================================
    ARMED_TIMEOUT_MIN = 30
    SCORE_DISPARO_MIN = 70
    SCORE_MANTENIMIENTO_ARMED = 50
    # FASE 5.1: PAUSA_INACTIVIDAD_HORAS ahora viene de config.py
    # self.PAUSA_INACTIVIDAD_HORAS → CONFIG.pausa_inactividad_horas
    ADX_RECHAZO_MIN = 20

    # ================================================================
    # V4.2: COMANDOS DE PAUSA MANUAL (nuevos métodos públicos)
    # ================================================================
    def pausar_moneda_manual(self, symbol: str, razon: str = "Comando usuario") -> bool:
        """Pausa una moneda manualmente. Retorna True si se pausó."""
        if symbol not in self.states:
            return False
        state = self.states[symbol]
        if state.moneda_pausada_manual:
            return False  # Ya está pausada manualmente

        estado_anterior = state.estado
        state.moneda_pausada_manual = True
        state.moneda_pausada = True  # También marca como pausada automática para consistencia
        state.moneda_pausada_razon = razon
        state.moneda_pausada_timestamp = int(datetime.now(pytz.UTC).timestamp())

        # Resetear estado de la máquina
        state.estado = 'MONITOREO'
        state.velas_confirmacion = 0
        state.filtro_macro_aprobado = False
        state.direccion_filtro = None
        state.armed_timestamp = 0
        state.score_bajo_desde = None

        print(f"  ⏸️ {symbol} PAUSADA MANUALMENTE | Razón: {razon} | Estado previo: {estado_anterior}")
        return True

    def reanudar_moneda_manual(self, symbol: str) -> bool:
        """Reanuda una moneda pausada manualmente. Retorna True si se reanudó."""
        if symbol not in self.states:
            return False
        state = self.states[symbol]
        if not state.moneda_pausada_manual:
            return False  # No estaba pausada manualmente

        state.moneda_pausada_manual = False
        # La pausa automática se limpia también, pero el sistema de inactividad
        # puede volver a pausarla si el score sigue bajo
        state.moneda_pausada = False
        state.moneda_pausada_razon = None
        state.moneda_pausada_timestamp = 0
        state.score_bajo_desde = None

        print(f"  ▶️ {symbol} REANUDADA MANUALMENTE | Monitoreo reiniciado")
        return True

    def pausar_todas_manual(self, razon: str = "Comando usuario") -> list:
        """Pausa todas las monedas. Retorna lista de símbolos pausados."""
        pausadas = []
        for symbol in CONFIG.symbols:
            if self.pausar_moneda_manual(symbol, razon):
                pausadas.append(symbol)
        return pausadas

    def reanudar_todas_manual(self) -> list:
        """Reanuda todas las monedas pausadas manualmente. Retorna lista de símbolos reanudadas."""
        reanudadas = []
        for symbol in CONFIG.symbols:
            if self.reanudar_moneda_manual(symbol):
                reanudadas.append(symbol)
        return reanudadas

    def get_monedas_pausadas(self) -> Dict[str, dict]:
        """Retorna dict con todas las monedas pausadas y su tipo de pausa."""
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

    # ================================================================
    # LOOP PRINCIPAL
    # ================================================================
    async def run(self):
        while True:
            tf, symbol, data = await self.queue_in.get()

            if tf == '1m':
                self.indicadores_1m[symbol] = data

                # FASE 5.4: Alimentar historial de 1m para commitment score
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

                    # FASE 5.3: Alimentar buffer de ATR(15m) para umbral dinámico
                    self._atr15m_historico[symbol].append(atr_val)
                    if len(self._atr15m_historico[symbol]) > CONFIG.atr_percentil_ventana:
                        self._atr15m_historico[symbol] = self._atr15m_historico[symbol][-CONFIG.atr_percentil_ventana:]

                # FASE 5.3: Actualizar rango reciente (highs/lows de últimas 20 velas)
                if data.get('high') and data.get('low'):
                    self._rango_reciente[symbol]['highs'].append(data['high'])
                    self._rango_reciente[symbol]['lows'].append(data['low'])
                    if len(self._rango_reciente[symbol]['highs']) > 20:
                        self._rango_reciente[symbol]['highs'] = self._rango_reciente[symbol]['highs'][-20:]
                        self._rango_reciente[symbol]['lows'] = self._rango_reciente[symbol]['lows'][-20:]

                await self.evaluar_filtro_macro(symbol)

            elif tf == '4h':
                self.indicadores_4h[symbol] = data

    # ================================================================
    # CAPA 1: FILTRO MACRO (evalúa cada 15m) — FASE 5.2 MFM
    # ================================================================
    async def evaluar_filtro_macro(self, symbol: str):
        """Evalúa si el mercado macro permite operar. Con MFM integrado."""
        i15 = self.indicadores_15m.get(symbol)
        i4h = self.indicadores_4h.get(symbol)
        state = self.states[symbol]

        # ═══════════════════════════════════════════════════════════════════════════════
        # V4.2 FIX #4: Despausa automática AHORA FUNCIONA
        # ═══════════════════════════════════════════════════════════════════════════════
        # ANTES: Si moneda_pausada == True, hacía return early y NUNCA evaluaba despausa
        # AHORA: Evaluamos SIEMPRE el score primero, y luego decidimos si pausar/despausar

        # Si está pausada MANUALMENTE, no evaluamos nada (el usuario tiene control total)
        if state.moneda_pausada_manual:
            state.filtro_macro_aprobado = False
            return

        if not i15 or not i4h:
            state.filtro_macro_aprobado = False
            # V4.2: Aún así evaluamos si debemos despausar una pausa automática
            await self._evaluar_despausa_automatica(symbol, state, score_macro=0)
            return

        required_15m = ['rsi', 'adx', 'atr', 'ema200_15m', 'macd_hist',
                        'macd_hist_prev', 'volume', 'volume_sma20']
        if any(i15.get(k) is None for k in required_15m):
            state.filtro_macro_aprobado = False
            await self._evaluar_despausa_automatica(symbol, state, score_macro=0)
            return
        if i4h.get('ema200_4h') is None:
            state.filtro_macro_aprobado = False
            await self._evaluar_despausa_automatica(symbol, state, score_macro=0)
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

        # FASE 5.2: Obtener MFM del indicador
        mfm = i15.get('mfm_15m', 0.0)
        mfm_sma5 = i15.get('mfm_sma5', 0.0)

        ema50 = i15.get('ema50_15m')
        if ema50 is None:
            ema50 = i15.get('ema25_15m', ema15)

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

        rechazos = []
        score_macro = 0

        if adx is None or np.isnan(adx) or adx > 45:
            rechazos.append(f"ADX extremo: {adx:.1f}")
            umbral_entrada = 999
            umbral_mantenimiento = 999
        elif adx < self.ADX_RECHAZO_MIN:
            rechazos.append(f"ADX sin tendencia: {adx:.1f}")
            umbral_entrada = 999
            umbral_mantenimiento = 999
        elif self.ADX_RECHAZO_MIN <= adx < 25:
            score_macro += 10
            umbral_entrada = 75
            umbral_mantenimiento = self.SCORE_MANTENIMIENTO_ARMED
        elif 25 <= adx <= 35:
            score_macro += 25
            umbral_entrada = 70
            umbral_mantenimiento = self.SCORE_MANTENIMIENTO_ARMED
        else:
            score_macro += 10
            umbral_entrada = 65
            umbral_mantenimiento = self.SCORE_MANTENIMIENTO_ARMED

        rsi_ok = False
        if direction == 'SHORT':
            if CONFIG.rsi_macro_min <= rsi <= CONFIG.rsi_macro_max:
                score_macro += 20
                rsi_ok = True
            else:
                rechazos.append(f"RSI macro sin oxígeno SHORT: {rsi:.1f}")
        elif direction == 'LONG':
            if CONFIG.rsi_macro_long_min <= rsi <= CONFIG.rsi_macro_long_max:
                score_macro += 20
                rsi_ok = True
            else:
                rechazos.append(f"RSI macro sin oxígeno LONG: {rsi:.1f}")
        else:
            rechazos.append("Dirección NEUTRAL")

        atr_pct = (atr / price) * 100 if price > 0 else 0
        if atr_pct < CONFIG.atr_min_pct:
            rechazos.append(f"ATR bajo: {atr_pct:.3f}%")
        elif atr_pct > CONFIG.atr_max_pct:
            rechazos.append(f"ATR alto: {atr_pct:.3f}%")
        else:
            score_macro += 15

        hist_change = abs(macd_hist - macd_hist_prev) if macd_hist_prev else 0
        hist_magnitude_pct = abs(macd_hist) / price * 100 if price > 0 else 0
        atr_pct_safe = atr_pct if atr_pct > 0 else 0.01

        if hist_magnitude_pct > CONFIG.macd_danger_threshold * atr_pct_safe:
            rechazos.append(f"MACD explosivo: {hist_magnitude_pct:.3f}%")
        elif hist_magnitude_pct > CONFIG.macd_stable_threshold * atr_pct_safe:
            score_macro += 10
        else:
            score_macro += 15

        # ═══════════════════════════════════════════════════════════════════════════════
        # FASE 5.2: VOLUMEN INTELIGENTE CON MFM
        # ═══════════════════════════════════════════════════════════════════════════════
        # ANTES: Volumen alto = +10 puntos siempre (independiente de dirección)
        # AHORA: Volumen alto solo suma si MFM está alineado con dirección
        #        Volumen alto + MFM contrario = penalización (absorción/trampa)
        # ═══════════════════════════════════════════════════════════════════════════════
        if vol_ratio >= CONFIG.volume_min_ratio:
            mfm_alineado = False
            if direction == 'SHORT' and mfm_sma5 < -CONFIG.mfm_umbral_alineacion:
                # Volumen presiona a la baja → alineado con SHORT
                mfm_alineado = True
                score_macro += CONFIG.mfm_bonus_alineado
                print(f"  📊 {symbol} Volumen+MFM alineado SHORT | mfm={mfm_sma5:.3f} | +{CONFIG.mfm_bonus_alineado}pts")
            elif direction == 'LONG' and mfm_sma5 > CONFIG.mfm_umbral_alineacion:
                # Volumen presiona al alza → alineado con LONG
                mfm_alineado = True
                score_macro += CONFIG.mfm_bonus_alineado
                print(f"  📊 {symbol} Volumen+MFM alineado LONG | mfm={mfm_sma5:.3f} | +{CONFIG.mfm_bonus_alineado}pts")
            else:
                # Volumen contradice dirección → posible trampa o absorción
                score_macro -= CONFIG.mfm_penalizacion_contrario
                rechazos.append(f"MFM contradictorio: mfm={mfm_sma5:.3f} vs dirección {direction}")
                print(f"  ⚠️ {symbol} Volumen+MFM CONTRADICTORIO | mfm={mfm_sma5:.3f} vs {direction} | -{CONFIG.mfm_penalizacion_contrario}pts")
        else:
            rechazos.append(f"Volumen bajo: {vol_ratio:.1%}")

        score_macro = max(0, min(100, score_macro))
        state.score_macro_actual = score_macro

        # ═══════════════════════════════════════════════════════════════════════════════
        # V4.2 FIX #4: Evaluar despausa automática ANTES de decidir si pausar
        # ═══════════════════════════════════════════════════════════════════════════════
        # Si está pausada automáticamente, verificamos si el score mejoró
        await self._evaluar_despausa_automatica(symbol, state, score_macro)

        # Si sigue pausada automáticamente después de evaluar despausa, no continuamos
        if state.moneda_pausada and not state.moneda_pausada_manual:
            state.filtro_macro_aprobado = False
            return

        if state.estado == 'ARMED':
            score_minimo = umbral_mantenimiento
        else:
            score_minimo = umbral_entrada

        base_aprobado = len(rechazos) == 0 and direction != 'NEUTRAL'
        filtro_aprobado = base_aprobado and score_macro >= score_minimo

        if filtro_aprobado:
            state.direccion_filtro = direction
            state.direccion_ultima_valida = direction
        else:
            state.direccion_filtro = None

        state.ultimo_filtro_timestamp = i15.get('timestamp', 0)

        # V4.2: Tracking de inactividad (pausa automática) - AHORA DESPUÉS del despausa
        # FASE 5.1: Usar CONFIG.pausa_inactividad_horas en vez de hardcodeo
        if score_macro < 50:
            if state.score_bajo_desde is None:
                state.score_bajo_desde = int(datetime.now(pytz.UTC).timestamp())
            else:
                segundos_bajo = int(datetime.now(pytz.UTC).timestamp()) - state.score_bajo_desde
                if segundos_bajo > CONFIG.pausa_inactividad_horas * 3600 and not state.moneda_pausada:
                    state.moneda_pausada = True
                    state.moneda_pausada_razon = f"Score < 50 durante {segundos_bajo/60:.0f}min"
                    state.moneda_pausada_timestamp = int(datetime.now(pytz.UTC).timestamp())
                    print(f"  ⏸️ {symbol} AUTO-PAUSADA por inactividad | {state.moneda_pausada_razon}")
        else:
            state.score_bajo_desde = None
            # NOTA: No limpiamos moneda_pausada aquí, eso lo hace _evaluar_despausa_automatica

        # FASE 4.5: Loggear cambio de filtro macro
        if self.audit_logger:
            estado_previo = state._prev_filtro_aprobado
            if estado_previo != state.filtro_macro_aprobado:
                contexto_macro = {
                    'precio': price,
                    'rsi': rsi,
                    'adx': adx,
                    'atr': atr,
                    'ema200_15m': ema15,
                    'ema50_15m': ema50,
                    'macd_hist': macd_hist,
                    'volumen_ratio': vol_ratio,
                    # FASE 5.2: Incluir MFM en contexto de auditoría
                    'mfm_15m': mfm,
                    'mfm_sma5': mfm_sma5,
                    'direccion': direction,
                    'score_macro': score_macro,
                    'umbral_aplicado': score_minimo,
                    'estado_maquina': state.estado,
                    'pausa_auto': state.moneda_pausada,
                    'pausa_manual': state.moneda_pausada_manual
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
            mfm_str = f" | MFM={mfm_sma5:.3f}" if mfm_sma5 != 0 else ""
            print(f"  🟢 {symbol} Filtro Macro OK ({direction}) | Score: {score_macro} | Umbral: {score_minimo} | RSI: {rsi:.1f} | ADX: {adx:.1f}{mfm_str}")
        else:
            if state.estado == 'ARMED':
                print(f"  🔴 {symbol} Filtro Macro ROTO | Rechazos: {rechazos[:2]} | Score: {score_macro} | Umbral: {score_minimo}")
            elif state.estado in ('MONITOREO',):
                if score_macro > 40:
                    print(f"  🟡 {symbol} Filtro Macro NO apto | Score: {score_macro} | {rechazos[:1]}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # V4.2: NUEVO MÉTODO - Evaluar despausa automática
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _evaluar_despausa_automatica(self, symbol: str, state: SignalState, score_macro: int):
        """
        Evalúa si una moneda pausada automáticamente debe reactivarse.
        Se llama SIEMPRE al inicio de evaluar_filtro_macro, antes de cualquier early return.
        """
        # Si está pausada MANUALMENTE, NUNCA despausar automáticamente
        if state.moneda_pausada_manual:
            return

        # Si NO está pausada automáticamente, nada que hacer
        if not state.moneda_pausada:
            return

        # Si el score mejoró a >= 50, despausar automáticamente
        if score_macro >= 50:
            tiempo_pausada = int(datetime.now(pytz.UTC).timestamp()) - state.moneda_pausada_timestamp
            state.moneda_pausada = False
            state.moneda_pausada_razon = None
            state.moneda_pausada_timestamp = 0
            state.score_bajo_desde = None
            print(f"  ▶️ {symbol} AUTO-REANUDADA | Score recuperado a {score_macro} | Pausada durante {tiempo_pausada/60:.0f}min")
        else:
            # Aún pausada, mostrar cuánto tiempo lleva pausada
            if state.moneda_pausada_timestamp > 0:
                tiempo_pausada = int(datetime.now(pytz.UTC).timestamp()) - state.moneda_pausada_timestamp
                if tiempo_pausada % 300 == 0:  # Log cada 5 minutos
                    print(f"  ⏸️ {symbol} sigue AUTO-PAUSADA | Score: {score_macro} | Tiempo pausada: {tiempo_pausada/60:.0f}min")

    # ================================================================
    # CAPA 1.5: MÁQUINA DE ESTADOS (hysteresis suavizada + timeout)
    # ================================================================

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.4: COMMITMENT SCORE REAL
    # ═══════════════════════════════════════════════════════════════════════════════
    def _alimentar_historial_1m(self, symbol: str, data: dict):
        """Alimenta el buffer de historial 1m para evaluar commitment."""
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
        # Mantener solo últimas 5 velas para análisis
        if len(self._historial_1m[symbol]) > 5:
            self._historial_1m[symbol] = self._historial_1m[symbol][-5:]

    def _evaluar_commitment_score(self, symbol: str, direction: str) -> int:
        """
        Evalúa el commitment score basado en alineación de indicadores de 1m.

        Requisitos para commitment positivo:
        1. RSI(7) alineado con dirección durante N velas consecutivas
        2. Precio alineado con dirección (vs EMA300) durante N velas
        3. Volumen soportando el movimiento

        Retorna: score 0-100 (commitment)
        """
        historial = self._historial_1m.get(symbol, [])
        if len(historial) < 2:
            return 0

        state = self.states[symbol]
        velas_requeridas = state.commitment_velas_requeridas

        # Tomar últimas N velas
        velas = historial[-velas_requeridas:]
        if len(velas) < velas_requeridas:
            return 0

        score = 0

        # 1. RSI(7) alineado con dirección
        rsi_alineado = True
        for v in velas:
            rsi = v.get('rsi_7')
            if rsi is None:
                rsi_alineado = False
                break
            if direction == 'SHORT' and rsi < 60:  # RSI debe estar alto para SHORT
                rsi_alineado = False
                break
            if direction == 'LONG' and rsi > 40:   # RSI debe estar bajo para LONG
                rsi_alineado = False
                break

        if rsi_alineado:
            score += 40

        # 2. Precio alineado con dirección vs EMA300
        precio_alineado = True
        for v in velas:
            close = v.get('close')
            ema300 = v.get('ema_300')
            if close is None or ema300 is None or ema300 == 0:
                precio_alineado = False
                break
            distancia_pct = ((close - ema300) / ema300) * 100

            if direction == 'SHORT' and distancia_pct > -0.05:  # Debe estar por encima o cerca de EMA300
                precio_alineado = False
                break
            if direction == 'LONG' and distancia_pct < 0.05:    # Debe estar por debajo o cerca de EMA300
                precio_alineado = False
                break

        if precio_alineado:
            score += 40

        # 3. Volumen soportando (al menos 1 velas con volumen > SMA20)
        volumen_soportando = 0
        for v in velas:
            vol = v.get('volume', 0)
            vol_sma = v.get('volume_sma20', 1)
            if vol_sma > 0 and vol >= vol_sma * 0.8:  # 80% del promedio es suficiente
                volumen_soportando += 1

        if volumen_soportando >= 1:
            score += 20

        return min(100, score)

    def _commitment_aprobado(self, symbol: str) -> bool:
        """Verifica si el commitment score es suficiente para pasar a ARMED."""
        state = self.states[symbol]
        direction = state.direccion_filtro

        if not direction:
            return False

        commitment = self._evaluar_commitment_score(symbol, direction)
        state.commitment_score_actual = commitment

        # Umbral de commitment: 60/100 (2 de 3 requisitos cumplidos)
        aprobado = commitment >= 60

        if aprobado:
            print(f"  ✅ {symbol} Commitment APROBADO: {commitment}/100 ({direction})")
        else:
            if state.estado == 'MONITOREO':
                print(f"  ⏳ {symbol} Commitment insuficiente: {commitment}/100 ({direction})")

        return aprobado

    async def _actualizar_estado_maquina(self, symbol: str):
        """
        Transiciona estados con hysteresis suavizada y timeout ARMED.
        V4.2: Respeta pausa manual y automática.
        """
        state = self.states[symbol]
        i15 = self.indicadores_15m.get(symbol, {})
        timestamp_15m_actual = i15.get('timestamp', 0)

        # V4.2: Si moneda pausada (manual o automática), forzar MONITOREO
        if state.moneda_pausada or state.moneda_pausada_manual:
            if state.estado != 'MONITOREO':
                estado_anterior_log = state.estado
                state.estado = 'MONITOREO'
                state.velas_confirmacion = 0
                state.filtro_macro_aprobado = False
                state.direccion_filtro = None
                state.armed_timestamp = 0
                tipo_pausa = "MANUAL" if state.moneda_pausada_manual else "AUTO"
                if self.audit_logger and estado_anterior_log != 'MONITOREO':
                    await self.audit_logger.log_cambio_estado(
                        symbol=symbol, de=estado_anterior_log, a='MONITOREO',
                        direccion=state.direccion_ultima_valida,
                        score_macro=state.score_macro_actual
                    )
                    state._prev_estado = 'MONITOREO'
                print(f"  ⏸️ {symbol} → MONITOREO (pausa {tipo_pausa} activa)")
            return

        # FASE 4.5: Loggear cambio de estado
        estado_anterior_log = state.estado

        if state.estado == 'COOLDOWN':
            if timestamp_15m_actual != state.ultimo_disparo_timestamp_15m:
                if state.filtro_macro_aprobado:
                    state.estado = 'ARMED'
                    state.velas_confirmacion = 0
                    state.armed_timestamp = int(datetime.now(pytz.UTC).timestamp() * 1000)
                    print(f"  🎯 {symbol} → ARMED (nueva vela 15m, filtro activo)")
                else:
                    state.estado = 'MONITOREO'
                    state.velas_confirmacion = 0
                    state.direccion_filtro = None
                    state.armed_timestamp = 0
                    print(f"  🔄 {symbol} → MONITOREO (nueva vela 15m, filtro perdido)")
            return

        if state.estado == 'MONITOREO':
            if state.filtro_macro_aprobado and state.direccion_filtro:
                # FASE 5.4: Commitment Score Real en vez de hysteresis simple
                # ANTES: contar 3 velas de confirmación (hysteresis_velas)
                # AHORA: verificar alineación real de indicadores de 1m
                commitment_ok = self._commitment_aprobado(symbol)

                if commitment_ok:
                    state.estado = 'ARMED'
                    state.velas_confirmacion = 0
                    state.armed_timestamp = int(datetime.now(pytz.UTC).timestamp() * 1000)
                    print(f"  🎯 {symbol} → ARMED ({state.direccion_filtro}) | Commitment: {state.commitment_score_actual}/100")
                else:
                    # Aún no hay commitment suficiente, seguir en MONITOREO
                    pass
            else:
                # Resetear commitment si filtro se pierde
                state.commitment_score_actual = 0
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
                    state.commitment_score_actual = 0
                    print(f"  🔄 {symbol} → MONITOREO (timeout ARMED: {tiempo_en_armed_ms/60000:.0f}min)")
                    if self.audit_logger:
                        await self.audit_logger.log_cambio_estado(
                            symbol=symbol, de='ARMED', a='MONITOREO',
                            direccion=state.direccion_ultima_valida
                        )
                        state._prev_estado = 'MONITOREO'
                    return

            # FASE 5.4: En ARMED, verificar que commitment sigue siendo válido
            if state.filtro_macro_aprobado and state.direccion_filtro:
                commitment_actual = self._evaluar_commitment_score(symbol, state.direccion_filtro)
                state.commitment_score_actual = commitment_actual

                if commitment_actual < 30:  # Commitment perdido drásticamente
                    state.estado = 'MONITOREO'
                    state.velas_confirmacion = 0
                    state.filtro_macro_aprobado = False
                    state.direccion_filtro = None
                    state.armed_timestamp = 0
                    state.commitment_score_actual = 0
                    print(f"  🔄 {symbol} → MONITOREO (commitment perdido: {commitment_actual}/100)")
                # Si commitment sigue OK, permanece en ARMED
            else:
                # Filtro macro roto, salir de ARMED
                if CONFIG.hysteresis_suavizada:
                    state.velas_confirmacion += 1
                    if state.velas_confirmacion >= CONFIG.hysteresis_velas:
                        state.estado = 'MONITOREO'
                        state.velas_confirmacion = 0
                        state.filtro_macro_aprobado = False
                        state.direccion_filtro = None
                        state.armed_timestamp = 0
                        state.commitment_score_actual = 0
                        print(f"  🔄 {symbol} → MONITOREO (filtro roto, {CONFIG.hysteresis_velas} velas contrarias)")
                else:
                    state.velas_confirmacion += 1
                    if state.velas_confirmacion >= CONFIG.hysteresis_velas:
                        state.estado = 'MONITOREO'
                        state.velas_confirmacion = 0
                        state.filtro_macro_aprobado = False
                        state.direccion_filtro = None
                        state.armed_timestamp = 0
                        state.commitment_score_actual = 0
                        print(f"  🔄 {symbol} → MONITOREO (filtro roto, {CONFIG.hysteresis_velas} velas 1m)")

        # FASE 4.5: Loggear transición de estado
        if estado_anterior_log != state.estado:
            if self.audit_logger:
                await self.audit_logger.log_cambio_estado(
                    symbol=symbol,
                    de=estado_anterior_log,
                    a=state.estado,
                    direccion=state.direccion_filtro or state.direccion_ultima_valida
                )
            state._prev_estado = state.estado

    # ================================================================
    # CAPA 2: GATILLO MICRO (evalúa cada 1m, solo si ARMED)
    # ================================================================

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.3: UMBRAL DINÁMICO VÍA ATR(15m)
    # ═══════════════════════════════════════════════════════════════════════════════
    def _calcular_umbral_dinamico(self, symbol: str) -> int:
        """
        Calcula el score mínimo de disparo adaptativo basado en ATR(15m).

        Lógica:
        - ATR en percentil 20 (bajo, consolidación) → score_min alto (85) = más selectivo
        - ATR en percentil 80 (alto, expansión) → score_min bajo (65) = más permisivo
        - Interpolación lineal entre ambos

        Pre-expansión: Si el precio rompe el rango reciente (high/low últimas 20 velas),
        bajamos temporalmente el umbral para capturar el inicio del movimiento.
        """
        atr_historico = self._atr15m_historico.get(symbol, [])
        if len(atr_historico) < 20:
            # Datos insuficientes, usar umbral estático
            return self.SCORE_DISPARO_MIN

        atr_actual = atr_historico[-1]
        atr_p20 = np.percentile(atr_historico, 20)
        atr_p80 = np.percentile(atr_historico, 80)

        # Normalizar ATR actual entre 0 y 1 (0 = consolidación, 1 = expansión)
        if atr_p80 > atr_p20:
            atr_normalizado = (atr_actual - atr_p20) / (atr_p80 - atr_p20)
        else:
            atr_normalizado = 0.5

        atr_normalizado = max(0.0, min(1.0, atr_normalizado))

        # Interpolación lineal: consolidación = 85, expansión = 65
        score_min = CONFIG.score_min_consolidacion - atr_normalizado * (CONFIG.score_min_consolidacion - CONFIG.score_min_expansion)
        score_min = int(round(score_min))

        # FASE 5.3: Detección de pre-expansión (ruptura de rango reciente)
        rango = self._rango_reciente.get(symbol, {})
        highs = rango.get('highs', [])
        lows = rango.get('lows', [])
        i15 = self.indicadores_15m.get(symbol, {})
        price = i15.get('close', 0)

        if len(highs) >= 10 and len(lows) >= 10 and price > 0:
            recent_high = max(highs)
            recent_low = min(lows)
            rango_size = recent_high - recent_low

            # Si el precio rompe el rango reciente → pre-expansión detectada
            if price > recent_high + rango_size * 0.01 or price < recent_low - rango_size * 0.01:
                # Bajamos temporalmente el umbral para capturar el movimiento
                score_min_pre_exp = max(CONFIG.score_min_expansion, score_min - 10)
                print(f"  📊 {symbol} PRE-EXPANSIÓN detectada | Umbral: {score_min} → {score_min_pre_exp}")
                return score_min_pre_exp

        return score_min

    async def evaluar_gatillo(self, symbol: str):
        """Evalúa condiciones de disparo en 1m. Solo si estado == ARMED."""
        i1m = self.indicadores_1m.get(symbol)
        state = self.states[symbol]

        if not i1m:
            return
        if state.estado != 'ARMED':
            return
        if not state.filtro_macro_aprobado or not state.direccion_filtro:
            return
        # FASE 5.3: Umbral dinámico vía ATR(15m)
        score_min_dinamico = self._calcular_umbral_dinamico(symbol)
        if state.score_macro_actual < score_min_dinamico:
            return
        # V4.2: No disparar si pausada
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

    # ================================================================
    # FASE 3: PROCESAR DISPARO CON GRID BLINDADO
    # ================================================================
    async def _procesar_disparo(self, symbol: str, direction: str, price: float, i1m: dict, i15: dict):
        """Procesa un disparo validado: calcula grid blindado, verifica rentabilidad, emite alerta."""
        state = self.states[symbol]
        i4h = self.indicadores_4h.get(symbol, {})
        atr_15m = i15.get('atr', price * 0.001)

        params, rechazos = self.calcular_parametros_grid_blindado(
            price, direction, atr_15m, i15, i4h, symbol, state
        )

        if rechazos:
            print(f"  ❌ {symbol} DISPARO RECHAZADO | {rechazos[0]}")

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

        # FASE 5.3: Log del umbral dinámico usado
        score_min_usado = self._calcular_umbral_dinamico(symbol)
        umbral_info = f" | Umbral: {score_min_usado}" if score_min_usado != self.SCORE_DISPARO_MIN else ""

        if params.get('auto_compressed'):
            print(f"  🔥 {symbol} → FIRE ({direction}) | Score: {score}{umbral_info} | GRID AUTO-COMPRIMIDO")
        else:
            print(f"  🔥 {symbol} → FIRE ({direction}) | Score: {score}{umbral_info} | Grid: {params['grid_count']} líneas")

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
                # FASE 5.2: Incluir MFM en contexto de auditoría
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

        await self.emitir_alerta(symbol, 'FIRE', direction, score, [], params, price)

        await asyncio.sleep(0.1)
        state.estado = 'COOLDOWN'
        print(f"  ⏳ {symbol} → COOLDOWN")

    # ================================================================
    # FASE 3: CALCULAR PARÁMETROS DE GRID BLINDADO
    # ================================================================
    def calcular_parametros_grid_blindado(self, price, direction, atr, i15, i4h, symbol, state) -> tuple:
        """Calcula parámetros de grid con protecciones múltiples."""
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
            rechazos.append(f"Capital insuficiente: {max_grids_posibles} grids posibles, mínimo {CONFIG.grid_min_grids}")
            return None, rechazos

        grid_count = min(max_grids_posibles, CONFIG.grid_max_grids_hard)

        max_grids_por_densidad = int(rango_total / (atr_seguro * CONFIG.grid_min_dist_atr))
        grid_count = min(grid_count, max(2, max_grids_por_densidad))
        grid_count = max(grid_count, CONFIG.grid_min_grids)

        step_usdt = rango_total / grid_count if grid_count > 0 else 0
        step_pct = (step_usdt / price) * 100 if price > 0 else 0

        comisiones_ciclo = 2 * CONFIG.grid_fee_rate
        breakeven = (comisiones_ciclo + CONFIG.grid_slippage) * 100
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
                print(f"  📐 {symbol} Auto-compresión: {grid_count} grids → step_pct {step_pct:.3f}%")

        if step_pct < breakeven:
            rechazos.append(f"step_pct {step_pct:.3f}% < breakeven {breakeven:.3f}%. Grid imposible de rentabilizar")
            return None, rechazos

        posicion = (price - lower) / rango_total if rango_total > 0 else 0.5
        posicion_alerta = False
        if posicion > CONFIG.grid_posicion_alerta_max or posicion < CONFIG.grid_posicion_alerta_min:
            posicion_alerta = True
            print(f"  ⚠️ {symbol} Posición en rango extrema: {posicion:.1%}")

        rentable = step_pct >= breakeven

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

    # ================================================================
    # FASE 3: CIRCUIT BREAKER
    # ================================================================
    async def _activar_circuit_breaker(self, symbol: str):
        """Activa el circuit breaker tras disparos consecutivos en pérdida."""
        state = self.states[symbol]
        state.circuit_breaker_activo = True
        pausa_ms = CONFIG.circuit_breaker_pausa_seg * 1000
        state.circuit_breaker_hasta = int(datetime.now(pytz.UTC).timestamp() * 1000) + pausa_ms

        state.capital_actual = max(
            CONFIG.grid_default_capital * CONFIG.circuit_breaker_reduccion_capital,
            CONFIG.grid_notional_min * CONFIG.grid_min_grids / CONFIG.grid_default_leverage
        )

        print(f"  🔒 {symbol} CIRCUIT BREAKER ACTIVADO | Pausa: {CONFIG.circuit_breaker_pausa_seg}s | "
              f"Capital reducido: ${state.capital_actual:.2f}")

        if self.audit_logger:
            await self.audit_logger.log_circuit_breaker(
                symbol=symbol,
                direccion=state.direccion_filtro or state.direccion_ultima_valida,
                rechazos=[f"{state.disparos_consecutivos} disparos en pérdida"]
            )

        await self.emitir_alerta(symbol, 'CIRCUIT_BREAKER', state.direccion_filtro or 'NEUTRAL', 0,
                                 [f"{state.disparos_consecutivos} disparos en pérdida"], None, 0)

    # ================================================================
    # UTILIDADES
    # ================================================================
    def _get_current_15m_timestamp(self) -> int:
        """DEPRECADO en Fase 2. Mantenido por compatibilidad."""
        now = datetime.now(pytz.UTC)
        minute_15m = (now.minute // 15) * 15
        ts = now.replace(minute=minute_15m, second=0, microsecond=0)
        return int(ts.timestamp() * 1000)

    def _calcular_score_macro(self, i15: dict) -> int:
        """Calcula score macro rápido para auditoría."""
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
        """Score del disparo basado en calidad del setup."""
        score = 50

        if direction == 'SHORT':
            score += min(i1m.get('wick_upper_pct', 0) * 2, 20)
            if i1m['rsi_7'] > 80:
                score += 15
            elif i1m['rsi_7'] > 75:
                score += 10
            vol_ratio = i1m['volume'] / i1m.get('volume_sma20', i1m['volume'])
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
            'estado_maquina': self.states[symbol].estado
        }
        await self.queue_out.put(evento)
