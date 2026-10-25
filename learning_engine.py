"""
learning_engine.py — Learning Engine para Crypto Monitor V5.7+ (Fases 1-8)

Pilares:
  P1 DATOS    · SnapshotCollector (features por vela 15m, batch flush, RAM-first)
  P2 ENTRADAS · OutcomeLabeler + PatternLearner (memoria 3 capas con decay)
  P3 RÉGIMEN  · RegimeClassifier (determinista + histéresis) + RegimeTransitionTracker
  P4 SALIDAS  · ExitOutcomeLabeler (trayectorias MFE/MAE) + TrendEndModel
  P5 GESTIÓN  · ExitPolicyEngine (política dinámica de salida, neto de fees)
  SERVING     · SHADOW / ACTIVE refusal-first (solo rechaza, nunca promueve)

Correcciones del informe de revisión aplicadas:
  C-2: llamadas síncronas y no bloqueantes en el hot loop del executor
       (buffers RAM → flush en background; sin awaits dentro de _monitorear_grid).
  C-3: compute_confidence() se invoca ANTES de mutar state (el hook vive en
       _procesar_disparo antes de state.estado='FIRE').
  C-4: on_vela_15m() se llama justo tras actualizar indicadores_15m.
  §3A: minería con Wilson CI + control FDR (Benjamini-Hochberg) + validación
       out-of-sample entre ventanas antes de promover capa.
  §3B: labels ATR-normalizados (umbrales en k·ATR, no % fijos); features
       direccionales up/down (sin leakage de dirección).
  §3C: todo neto de fees (learning_fee_rate por lado + slippage).
  §3D: métrica primaria de refusal = ΔPnL_neto (el ratio de conteo es secundario).
  §3G: decay estandarizado: peso = 0.5 ** (edad_dias / half_life).
  F :  clasificador de régimen determinista con histéresis y duración mínima.

Solo stdlib (sqlite3, math, statistics, asyncio, threading). Cero dependencias
pesadas — LightGBM llega en Fase 9 con su propio módulo.
"""

import asyncio
import json
import math
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Callable, Optional

import learning_db as ldb

MODEL_VERSION = "patterns_v1"

# Features minables (todas numéricas en feature_snapshots)
MINING_FEATURES = [
    'rsi_15m', 'adx_15m', 'atr_pct_15m', 'macd_hist', 'mfm_15m', 'mfm_sma5',
    'volume_ratio', 'price_rel_ema50_pct', 'price_rel_ema200_pct',
    'ema7_ema25_spread_pct', 'dist_to_recent_high_pct', 'dist_to_recent_low_pct',
    'ema50_slope_3v', 'adx_slope_3v', 'rsi_divergence', 'rsi_7_1m', 'score_macro',
]

LABEL_OBJETIVOS = ['LONG_STRONG', 'SHORT_STRONG', 'NEUTRAL_RANGE', 'WHIPSAW']

TREND_END_FEATURES = ['adx_slope_3v', 'mfm_flip', 'volume_ratio',
                      'rsi_divergence', 'distancia_ema50_pct']


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _valor_en_rango(valor: float, rango_str: str) -> bool:
    """Interpreta '(lo, hi]' de rango_valor. Tolerante a '-inf'/'+inf'/None."""
    if not rango_str:
        return True
    try:
        interior = rango_str.strip().strip('()[]')
        lo_s, hi_s = [s.strip() for s in interior.split(',')]
        lo = float(lo_s) if lo_s not in ('-inf', '') else float('-inf')
        hi = float(hi_s) if hi_s not in ('+inf', '') else float('inf')
        return lo < valor <= hi
    except Exception:
        return True


def _safe_div(a, b, default=0.0):
    try:
        if b in (0, 0.0, None) or a is None:
            return default
        return a / b
    except Exception:
        return default


# ═══════════════════════════════════════════════════════════════════════════
# Estadística (sin scipy): Wilson, Φ normal, Benjamini-Hochberg
# ═══════════════════════════════════════════════════════════════════════════
def _phi(x: float) -> float:
    """CDF de la normal estándar."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _wilson(k: float, n: float, z: float = 1.96):
    """Intervalo de Wilson para proporción k/n. Acepta conteos efectivos."""
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / denom
    margen = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return max(0.0, centro - margen), min(1.0, centro + margen)


def _p_value_diferencia(k1: float, n1: float, k2: float, n2: float) -> float:
    """p-valor (aprox. normal) para H0: p1 == p2. Prueba bilateral."""
    if n1 <= 1 or n2 <= 1:
        return 1.0
    p1, p2 = k1 / n1, k2 / n2
    p = (k1 + k2) / (n1 + n2)
    var = p * (1 - p) * (1 / n1 + 1 / n2)
    if var <= 0:
        return 1.0
    z = (p1 - p2) / math.sqrt(var)
    return 2.0 * (1.0 - _phi(abs(z)))


def _bh_fdr(pares: list, q: float = 0.10) -> list:
    """
    Benjamini-Hochberg. `pares` = [(p_value, objeto), ...].
    Retorna los objetos que sobreviven al control FDR al nivel q.
    """
    if not pares:
        return []
    pares = sorted(pares, key=lambda t: t[0])
    m = len(pares)
    umbral_max = -1.0
    for i, (p, _) in enumerate(pares, start=1):
        if p <= q * i / m:
            umbral_max = i
    if umbral_max < 0:
        return []
    return [obj for _, obj in pares[:umbral_max]]


# ═══════════════════════════════════════════════════════════════════════════
# P3 · RegimeClassifier — determinista + histéresis
# ═══════════════════════════════════════════════════════════════════════════
class RegimeClassifier:
    """
    Reglas deterministas (corrección §3F):

        trend_up   := ema7 > ema25 and close > ema50 and adx >= adx_min (20)
        trend_dn   := ema7 < ema25 and close < ema50 and adx >= adx_min
        volatile   := atr_pct >= p80(atr_hist)  [y no tendencia fuerte]
        range      := resto (adx < 20 o sin tendencia clara)

    Histéresis: el régimen candidato debe repetirse `min_velas` (4) veces
    seguidas antes de confirmarse y registrar la transición. Esto evita
    flip-flop (churn) que envenenaría el RegimeTransitionTracker.
    """

    def __init__(self, min_velas: int = 4, ventana_atr: int = 500):
        self.min_velas = max(1, int(min_velas))
        self._atr_hist: dict = defaultdict(lambda: deque(maxlen=ventana_atr))
        self._candidato: dict = {}
        self._racha: dict = defaultdict(int)
        self._regimen_previo: dict = {}
        self._regimen_inicio_ts: dict = {}
        self.regimen_actual: dict = {}          # symbol -> confirmado

    def clasificar(self, f: dict) -> str:
        """Régimen instantáneo (sin histéresis) a partir de un snapshot de features."""
        try:
            close = float(f.get('price') or f.get('price_rel_ema50_pct') or 0) or 0
        except Exception:
            close = 0
        adx = f.get('adx_15m') or 0
        atr_pct = f.get('atr_pct_15m') or 0
        ema7 = f.get('ema7_ema25_spread_pct')       # (ema7-ema25)/ema25*100
        rel50 = f.get('price_rel_ema50_pct') or 0

        trend_up = (ema7 is not None and ema7 > 0 and rel50 > 0 and adx >= 20)
        trend_dn = (ema7 is not None and ema7 < 0 and rel50 < 0 and adx >= 20)

        hist = self._atr_hist.get(f.get('symbol'), deque(maxlen=500))
        p80 = self._percentil(list(hist), 80) if len(hist) >= 20 else None
        volatil = (p80 is not None and atr_pct >= p80)

        if trend_up and not (volatil and atr_pct >= (p80 or 0) * 1.3):
            return 'TREND_UP'
        if trend_dn and not (volatil and atr_pct >= (p80 or 0) * 1.3):
            return 'TREND_DOWN'
        if volatil:
            return 'VOLATILE'
        if adx < 20:
            return 'RANGE'
        return 'RANGE'

    def actualizar(self, symbol: str, f: dict) -> dict:
        """
        Actualiza histéresis. Retorna:
          {'regimen': confirmado, 'candidato': ..., 'transicion': (from,to) | None}
        """
        atr_pct = f.get('atr_pct_15m') or 0
        self._atr_hist[symbol].append(atr_pct)

        inst = self.clasificar({**f, 'symbol': symbol})

        if self._candidato.get(symbol) == inst:
            self._racha[symbol] += 1
        else:
            self._candidato[symbol] = inst
            self._racha[symbol] = 1

        transicion = None
        previo = self.regimen_actual.get(symbol, 'UNKNOWN')

        if self._racha[symbol] >= self.min_velas:
            if previo != inst:
                dur_min = 0
                inicio = self._regimen_inicio_ts.get(symbol)
                if inicio:
                    dur_min = int((_now_ms() - inicio) / 60000)
                transicion = (previo, inst, dur_min)
                self._regimen_inicio_ts[symbol] = _now_ms()
            self.regimen_actual[symbol] = inst
        elif symbol not in self.regimen_actual:
            self.regimen_actual[symbol] = 'UNKNOWN'

        return {
            'regimen': self.regimen_actual.get(symbol, 'UNKNOWN'),
            'candidato': inst,
            'transicion': transicion,
        }

    @staticmethod
    def _percentil(vals: list, q: float):
        if not vals:
            return None
        s = sorted(vals)
        k = (len(s) - 1) * q / 100.0
        lo, hi = int(math.floor(k)), int(math.ceil(k))
        if lo == hi:
            return s[lo]
        return s[lo] + (s[hi] - s[lo]) * (k - lo)


# ═══════════════════════════════════════════════════════════════════════════
# P1 · SnapshotCollector
# ═══════════════════════════════════════════════════════════════════════════
class SnapshotCollector:
    """
    Captura features al cierre de cada vela 15m. `on_vela_15m()` es SÍNCRONO y
    solo añade a RAM (corrección C-2: nada de I/O en el hot loop). El flush a
    SQLite corre en el loop de background del LearningEngine (batch executemany).
    """

    def __init__(self, signals, regime_cls: RegimeClassifier):
        self.signals = signals
        self.regime_cls = regime_cls
        self._buffer: list = []
        self.transiciones_pendientes: list = []   # filas regime_transitions (flush batch)
        self._lock = threading.Lock()
        # Historiales ligeros para features de pendiente / divergencia
        self._ema50_hist: dict = defaultdict(lambda: deque(maxlen=6))
        self._adx_hist: dict = defaultdict(lambda: deque(maxlen=6))
        self._ci_hist: dict = defaultdict(lambda: deque(maxlen=21))  # (close, rsi)
        self._prev_fila: dict = {}
        self._ultimo_ts: dict = {}   # idempotencia por (symbol, timestamp)

    # ── hook público (1 línea en signals_v5.run) ──────────────────────────
    def on_vela_15m(self, symbol: str):
        try:
            fila = self._extraer_features(symbol)
            if fila is None:
                return
            with self._lock:
                # Idempotencia (ANEXO A §6.2): misma vela dos veces = 1 snapshot
                ultimo = self._ultimo_ts.get(symbol)
                if ultimo == fila['timestamp_15m']:
                    return
                self._ultimo_ts[symbol] = fila['timestamp_15m']
                self._buffer.append(fila)
        except Exception as e:
            print(f"  ⚠️ [LEARN] snapshot {symbol}: {e}")

    def pendientes(self) -> int:
        with self._lock:
            return len(self._buffer)

    def drenar_buffer(self) -> list:
        with self._lock:
            filas, self._buffer = self._buffer, []
        return filas

    def drenar_transiciones(self) -> list:
        with self._lock:
            filas, self.transiciones_pendientes = self.transiciones_pendientes, []
        return filas

    # ── extracción de features ────────────────────────────────────────────
    def _extraer_features(self, symbol: str) -> Optional[dict]:
        sg = self.signals
        i15 = sg.indicadores_15m.get(symbol)
        if not i15:
            return None
        i1m = sg.indicadores_1m.get(symbol, {}) or {}
        i4h = sg.indicadores_4h.get(symbol, {}) or {}
        state = sg.states.get(symbol)

        price = float(i15.get('close') or 0)
        if price <= 0:
            return None

        ema50 = i15.get('ema50_15m')
        ema200 = i15.get('ema200_15m')
        ema7 = i15.get('ema7_15m')
        ema25 = i15.get('ema25_15m')
        adx = i15.get('adx')
        atr = i15.get('atr')
        rsi = i15.get('rsi')
        vol = i15.get('volume') or 0
        vol_sma = i15.get('volume_sma20') or 0

        # Históricos ligeros para pendientes y divergencia
        if ema50:
            self._ema50_hist[symbol].append(float(ema50))
        if adx:
            self._adx_hist[symbol].append(float(adx))
        self._ci_hist[symbol].append((price, float(rsi) if rsi is not None else None))

        ema50_slope = self._slope_3v(self._ema50_hist[symbol])
        adx_slope = self._slope_3v(self._adx_hist[symbol])
        rsi_div = self._rsi_divergencia(self._ci_hist[symbol])

        ts = int(i15.get('timestamp') or _now_ms())
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)

        # Régimen con histéresis
        pre_features = {
            'symbol': symbol, 'price': price, 'adx_15m': adx,
            'atr_pct_15m': _safe_div(atr * 100.0, price) if atr else 0.0,
            'ema7_ema25_spread_pct': _safe_div((ema7 - ema25) * 100.0, ema25) if (ema7 and ema25) else None,
            'price_rel_ema50_pct': _safe_div((price - ema50) * 100.0, ema50) if ema50 else 0.0,
        }
        reg = self.regime_cls.actualizar(symbol, pre_features)

        atr_pct = _safe_div(atr * 100.0, price) if atr else None

        fila = {
            'symbol': symbol,
            'timestamp_15m': ts,
            'price': price,
            'regime': reg['candidato'],
            'regime_confirmado': reg['regimen'],

            'rsi_15m': rsi,
            'adx_15m': adx,
            'atr_pct_15m': atr_pct,
            'macd_hist': i15.get('macd_hist'),
            'macd_hist_prev': i15.get('macd_hist_prev'),
            'mfm_15m': i15.get('mfm_15m'),
            'mfm_sma5': i15.get('mfm_sma5'),
            'volume_ratio': _safe_div(vol, vol_sma) if vol_sma else None,

            'price_rel_ema50_pct': _safe_div((price - ema50) * 100.0, ema50) if ema50 else None,
            'price_rel_ema200_pct': _safe_div((price - ema200) * 100.0, ema200) if ema200 else None,
            'ema7_ema25_spread_pct': _safe_div((ema7 - ema25) * 100.0, ema25) if (ema7 and ema25) else None,
            'dist_to_recent_high_pct': _safe_div(((i15.get('recent_high') or 0) - price) * 100.0, price) if i15.get('recent_high') else None,
            'dist_to_recent_low_pct': _safe_div((price - (i15.get('recent_low') or 0)) * 100.0, price) if i15.get('recent_low') else None,
            'ema50_slope_3v': ema50_slope,
            'adx_slope_3v': adx_slope,
            'rsi_divergence': 1 if rsi_div else 0,

            'rsi_7_1m': i1m.get('rsi_7'),
            'wick_upper_pct_1m': i1m.get('wick_upper_pct'),
            'wick_lower_pct_1m': i1m.get('wick_lower_pct'),
            'body_direction_1m': i1m.get('body_direction'),
            'mecha_short_1m': 1 if i1m.get('mecha_valida_short') else 0,
            'mecha_long_1m': 1 if i1m.get('mecha_valida_long') else 0,

            'ema200_4h': i4h.get('ema200_4h'),
            'close_4h': i4h.get('close'),

            # UTC (corrección §3B: sin DST, mercado 24/7)
            'hora_sin': round(math.sin(2 * math.pi * dt.hour / 24.0), 6),
            'hora_cos': round(math.cos(2 * math.pi * dt.hour / 24.0), 6),
            'dia_semana': dt.weekday(),

            'score_macro': getattr(state, 'score_macro_actual', None),
            'estado_maquina': getattr(state, 'estado', None),
            'evento_asociado': None,
            'evento_id': None,
            'feature_schema_version': 'v1.0',   # ANEXO A §2.2
        }

        # Transición de régimen confirmada → fila para regime_transitions
        if reg.get('transicion'):
            from_reg, to_reg, dur_min = reg['transicion']
            prev = self._prev_fila.get(symbol, {})
            with self._lock:
                self.transiciones_pendientes.append({
                    'symbol': symbol,
                    'timestamp_15m': ts,
                    'from_regime': from_reg,
                    'to_regime': to_reg,
                    'adx_15m': fila['adx_15m'],
                    'adx_slope_3v': fila['adx_slope_3v'],
                    'mfm_sma5': fila['mfm_sma5'],
                    'mfm_flip': 1 if (prev.get('mfm_sma5') is not None and fila['mfm_sma5'] is not None
                                      and prev['mfm_sma5'] * fila['mfm_sma5'] < 0) else 0,
                    'volume_ratio': fila['volume_ratio'],
                    'rsi_15m': fila['rsi_15m'],
                    'atr_pct_15m': fila['atr_pct_15m'],
                    'precio_antes': prev.get('price'),
                    'precio_despues': price,
                    'duracion_regimen_previo_min': dur_min,
                    'fue_transicion_real': None,
                    'tiempo_confirmacion_min': None,
                })
        self._prev_fila[symbol] = fila
        return fila

    @staticmethod
    def _slope_3v(hist: deque):
        if len(hist) < 4:
            return None
        actual, prev = hist[-1], hist[-4]
        return _safe_div((actual - prev) * 100.0, prev) if prev else None

    @staticmethod
    def _rsi_divergencia(hist: deque):
        """Divergencia simple: precio HH con RSI HL (o precio LL con RSI HL)."""
        if len(hist) < 21:
            return 0
        prev = list(hist)[:20]
        act_close, act_rsi = hist[-1]
        closes = [c for c, _ in prev if c is not None]
        rsis = [r for _, r in prev if r is not None]
        if not closes or not rsis or act_rsi is None:
            return 0
        precio_hh = act_close > max(closes)
        precio_ll = act_close < min(closes)
        rsi_debil = act_rsi < max(rsis) - 2
        rsi_fuerte = act_rsi > min(rsis) + 2
        return 1 if ((precio_hh and rsi_debil) or (precio_ll and rsi_fuerte)) else 0


# ═══════════════════════════════════════════════════════════════════════════
# P2 · OutcomeLabeler — labels ATR-normalizados, netos de fees
# ═══════════════════════════════════════════════════════════════════════════
class OutcomeLabeler:
    """
    Etiqueta snapshots maduros (>=8h) con retornos 1h/4h/8h + excursiones
    up/down + label_class. Umbrales en k·ATR (corrección §3B):

        u    := atr_pct_15m del snapshot (fallback 0.5%)
        r4   := ret_4h_pct / u
        up   := up_excursion_pct / u ; dn := down_excursion_pct / u

        LONG_STRONG    := r4 >= +1.0 y up >= 1.5
        SHORT_STRONG   := r4 <= -1.0 y dn >= 1.5
        WHIPSAW        := up >= 1.0 y dn >= 1.0 y |r4| < 0.3
        NEUTRAL_RANGE  := |r4| < 0.5 y up < 1.0 y dn < 1.0
        MIXED          := resto

    ret_4h_neto_pct = ret_4h_pct - 2*fee_pct (corrección §3C).
    """

    def __init__(self, velas_loader: Callable, fee_pct: float = 0.05,
                 madurez_ms: int = 8 * 3600 * 1000):
        self.velas_loader = velas_loader          # async (symbol, tf, limit) -> list[dict]
        self.fee_pct = fee_pct
        self.madurez_ms = madurez_ms

    async def sincronizar_velas(self, symbols: list, limit: int = 1500):
        """Job de sync MySQL → mirror SQLite (corrección C-1: app-level, no trigger)."""
        total = 0
        for symbol in symbols:
            try:
                velas = await self.velas_loader(symbol, '15m', limit)
                if velas:
                    total += await ldb.upsert_velas_mirror(symbol, velas)
            except Exception as e:
                print(f"  ⚠️ [LEARN] sync velas {symbol}: {e}")
        return total

    async def procesar_pendientes(self, symbols: list, max_lote: int = 500) -> int:
        ahora = _now_ms()
        pendientes = await ldb.cargar_snapshots_sin_label(ahora - self.madurez_ms, max_lote)
        if not pendientes:
            return 0

        por_symbol = defaultdict(list)
        for s in pendientes:
            por_symbol[s['symbol']].append(s)

        labels = []
        for symbol, snaps in por_symbol.items():
            ts_min = min(s['timestamp_15m'] for s in snaps)
            ts_max = max(s['timestamp_15m'] for s in snaps)
            velas = await ldb.cargar_velas_mirror(symbol, ts_min - 3600_000,
                                                  ts_max + 8 * 3600_000)
            if not velas:
                # ANEXO A §3 (adaptado): sin mirror cubierto → sync desde
                # MySQL antes de concluir que no hay datos
                await self.sincronizar_velas([symbol])
                velas = await ldb.cargar_velas_mirror(symbol, ts_min - 3600_000,
                                                      ts_max + 8 * 3600_000)
            if not velas:
                continue
            for s in snaps:
                lab = self._etiquetar(s, velas)
                if lab:
                    labels.append(lab)

        if labels:
            await ldb.guardar_pattern_labels_batch(labels)
        return len(labels)

    def _etiquetar(self, snap: dict, velas: list) -> Optional[dict]:
        ts = int(snap['timestamp_15m'])
        price = float(snap['price'])
        if price <= 0:
            return None

        futuras = [v for v in velas if v['timestamp'] > ts]
        if len(futuras) < 4:
            return None                      # todavía no hay horizonte completo

        def _close_a(ms_offset):
            objetivo = ts + ms_offset
            cand = [v for v in futuras if v['timestamp'] <= objetivo]
            return float(cand[-1]['close']) if cand else None

        c1h = _close_a(1 * 3600_000)
        c4h = _close_a(4 * 3600_000)
        c8h = _close_a(8 * 3600_000)
        if c4h is None:
            return None

        ventana = [v for v in futuras if v['timestamp'] <= ts + 8 * 3600_000]
        highs = [float(v['high']) for v in ventana if v.get('high') is not None]
        lows = [float(v['low']) for v in ventana if v.get('low') is not None]
        if not highs or not lows:
            return None

        up_pct = (max(highs) - price) / price * 100.0
        dn_pct = (price - min(lows)) / price * 100.0
        idx_hi = highs.index(max(highs))
        idx_lo = lows.index(min(lows))

        ret_1h = (c1h / price - 1) * 100.0 if c1h else None
        ret_4h = (c4h / price - 1) * 100.0
        ret_8h = (c8h / price - 1) * 100.0 if c8h else None

        u = snap.get('atr_pct_15m') or 0.5
        u = u if u > 0 else 0.5
        r4 = ret_4h / u
        up = up_pct / u
        dn = dn_pct / u

        if r4 >= 1.0 and up >= 1.5:
            label = 'LONG_STRONG'
        elif r4 <= -1.0 and dn >= 1.5:
            label = 'SHORT_STRONG'
        elif up >= 1.0 and dn >= 1.0 and abs(r4) < 0.3:
            label = 'WHIPSAW'
        elif abs(r4) < 0.5 and up < 1.0 and dn < 1.0:
            label = 'NEUTRAL_RANGE'
        else:
            label = 'MIXED'

        return {
            'snapshot_id': snap['id'],
            'symbol': snap['symbol'],
            'timestamp_15m': ts,
            'ret_1h_pct': ret_1h,
            'ret_4h_pct': ret_4h,
            'ret_8h_pct': ret_8h,
            'ret_4h_neto_pct': ret_4h - 2 * self.fee_pct,
            'mfe_pct': up_pct,
            'mae_pct': dn_pct,
            'up_excursion_pct': up_pct,
            'down_excursion_pct': dn_pct,
            'mfe_minuto': idx_hi * 15,
            'mae_minuto': idx_lo * 15,
            'label_class': label,
            # ANEXO A §2.2: trazabilidad de qué lógica generó este label
            'labeler_version': 'v1.0_ret8h',
            'labeler_params_json': json.dumps({
                'umbral_k_atr': {'LONG': 1.0, 'SHORT': -1.0, 'excursion': 1.5,
                                 'whipsaw': 0.3, 'neutral': 0.5},
                'ventana_h': 8, 'fee_pct': self.fee_pct}, ensure_ascii=False),
        }


# ═══════════════════════════════════════════════════════════════════════════
# P2 · PatternLearner — quintiles + decay + Wilson + FDR + 3 capas
# ═══════════════════════════════════════════════════════════════════════════
class PatternLearner:
    """
    Minado por (symbol, regime, feature, bucket Q1-Q5, label_objetivo).

    Peso por decaimiento (corrección §3G):
        peso = 0.5 ** (edad_dias / half_life)

    Aceptación de patrón (corrección §3A):
        lift >= learning_min_lift (1.5)
        lift_lower (Wilson 95%) >= learning_min_lift_lower (1.2)
        n_entero >= learning_min_n (40)
        sobrevive a Benjamini-Hochberg q=learning_fdr_q (0.10)

    Promoción de capas (validación entre ventanas, nunca solo reloj):
        SHORT(30d) válido también en MEDIUM(180d)          -> MEDIUM
        MEDIUM con lift>=1.3 en 3 corridas seguidas        -> LONG (STABLE)
        LONG con lift<1.0 en 2 corridas seguidas           -> SUSPENDED
        SUSPENDED 2 meses                                  -> DEPRECATED
        Nunca se borra: pattern_history lo guarda todo.
    """

    def __init__(self, half_life_dias: float = 125.0, min_lift: float = 1.5,
                 min_lift_lower: float = 1.2, min_n: int = 40,
                 fdr_q: float = 0.10):
        self.half_life_dias = half_life_dias
        self.min_lift = min_lift
        self.min_lift_lower = min_lift_lower
        self.min_n = min_n
        self.fdr_q = fdr_q

    # ── utilidades ────────────────────────────────────────────────────────
    def _peso(self, ts_ms: int, ahora_ms: int) -> float:
        edad_dias = max(0.0, (ahora_ms - ts_ms) / 86400_000.0)
        return 0.5 ** (edad_dias / self.half_life_dias)

    @staticmethod
    def _quintiles_ponderados(valores: list, pesos: list):
        """Cortes Q1..Q5 ponderados por peso. Retorna lista de 4 cortes."""
        pares = sorted(zip(valores, pesos), key=lambda t: t[0])
        total = sum(p for _, p in pares) or 1.0
        cortes, acum = [], 0.0
        for i, (v, p) in enumerate(pares):
            acum += p
            while len(cortes) < 4 and acum >= total * (len(cortes) + 1) / 5.0:
                cortes.append(v)
        while len(cortes) < 4:
            cortes.append(pares[-1][0] if pares else 0.0)
        return cortes

    @staticmethod
    def _bucket(v: float, cortes: list) -> str:
        for i, c in enumerate(cortes):
            if v <= c:
                return f"Q{i + 1}"
        return "Q5"

    # ── minería sobre una ventana ─────────────────────────────────────────
    def minar_ventana(self, rows: list, ahora_ms: int, symbol: str, regime: str,
                      ventana_nombre: str) -> list:
        """Retorna patrones candidatos (con estadísticas) de una ventana."""
        # Ponderar por decay
        datos = []
        for r in rows:
            w = self._peso(int(r['timestamp_15m']), ahora_ms)
            datos.append({**r, '_w': w})

        # Baselines por label (ponderados)
        W = sum(d['_w'] for d in datos) or 1.0
        base = {}
        for lab in LABEL_OBJETIVOS:
            base[lab] = sum(d['_w'] for d in datos if d['label_class'] == lab) / W

        candidatos = []
        pares_fdr = []

        for feature in MINING_FEATURES:
            filas = [d for d in datos if d.get(feature) is not None]
            if len(filas) < self.min_n:
                continue
            valores = [float(d[feature]) for d in filas]
            pesos = [d['_w'] for d in filas]
            cortes = self._quintiles_ponderados(valores, pesos)

            for qi in range(5):
                bucket = f"Q{qi + 1}"
                grupo = [d for d in filas
                         if self._bucket(float(d[feature]), cortes) == bucket]
                if not grupo:
                    continue
                n_eff = sum(d['_w'] for d in grupo)
                n_entero = len(grupo)
                if n_entero < max(10, self.min_n // 4):
                    continue

                for lab in LABEL_OBJETIVOS:
                    k_eff = sum(d['_w'] for d in grupo if d['label_class'] == lab)
                    p_local = k_eff / n_eff if n_eff > 0 else 0.0
                    p_base = base.get(lab, 0.0)
                    if p_base <= 0.001:
                        continue
                    lift = p_local / p_base
                    lo_loc, hi_loc = _wilson(k_eff, n_eff)
                    k_base = sum(d['_w'] for d in datos if d['label_class'] == lab)
                    lo_base, hi_base = _wilson(k_base, W)
                    lift_lower = lo_loc / hi_base if hi_base > 0 else 0.0
                    p_val = _p_value_diferencia(k_eff, n_eff, k_base - k_eff, W - n_eff)

                    if lift < self.min_lift * 0.8:      # pre-filtro barato
                        continue

                    key = f"{symbol}|{regime}|{feature}|{bucket}|{lab}"
                    cand = {
                        'pattern_key': key,
                        'symbol': symbol,
                        'regime': regime,
                        'feature': feature,
                        'bucket': bucket,
                        'rango_valor': self._rango_str(feature, bucket, cortes),
                        'label_objetivo': lab,
                        'p_local': round(p_local, 4),
                        'p_baseline': round(p_base, 4),
                        'lift': round(lift, 3),
                        'lift_lower': round(lift_lower, 3),
                        'p_value': round(p_val, 6),
                        'n': round(n_eff, 1),
                        'n_entero': n_entero,
                        'ventana': ventana_nombre,
                    }
                    candidatos.append(cand)
                    pares_fdr.append((p_val, cand))

        # Control FDR (corrección §3A)
        supervivientes = set(id(c) for c in _bh_fdr(pares_fdr, self.fdr_q))
        out = []
        for c in candidatos:
            if (id(c) in supervivientes
                    and c['lift'] >= self.min_lift
                    and c['lift_lower'] >= self.min_lift_lower
                    and c['n_entero'] >= self.min_n):
                out.append(c)
        return out

    @staticmethod
    def _rango_str(feature, bucket, cortes):
        idx = int(bucket[1]) - 1
        lo = "-inf" if idx == 0 else f"{cortes[idx - 1]:.4f}"
        hi = "+inf" if idx == 4 else f"{cortes[idx]:.4f}"
        return f"({lo}, {hi}]"

    # ── consolidación de capas ────────────────────────────────────────────
    async def consolidar_capas(self, symbol: str, por_ventana: dict,
                               generado_utc: str):
        """
        por_ventana = {'SHORT': [cands], 'MEDIUM': [cands], 'LONG': [cands]}
        Aplica reglas de promoción/suspensión contra el estado previo en DB.
        """
        previos = {(p['pattern_key'], p['capa']): p
                   for p in await ldb.cargar_learned_patterns(symbol)}
        keys_previas = defaultdict(list)
        for (key, capa), p in previos.items():
            keys_previas[key].append(p)

        filas, historial = [], []

        for ventana, capa in (('SHORT', 'SHORT'), ('MEDIUM', 'MEDIUM'), ('LONG', 'LONG')):
            for c in por_ventana.get(ventana, []):
                previos_key = keys_previas.get(c['pattern_key'], [])
                status = 'ACTIVE'
                n_ventanas = 1
                primera = generado_utc

                if previos_key:
                    p0 = previos_key[0]
                    primera = p0.get('primera_deteccion_utc') or generado_utc
                    n_ventanas = int(p0.get('n_ventanas_valido') or 0) + 1
                    if p0.get('status') == 'SUSPENDED':
                        status = 'ACTIVE'          # revalidado → rehabilitación
                        n_ventanas = 1

                # Promociones basadas en validación entre ventanas
                if capa == 'MEDIUM' and not any(p['capa'] == 'MEDIUM'
                                                for p in previos_key):
                    status = 'ACTIVE'
                if capa == 'LONG' and n_ventanas >= 3 and c['lift'] >= 1.3:
                    status = 'STABLE'

                fila = {**c, 'capa': capa, 'status': status,
                        'key_format_version': 'v1_raw',   # ANEXO A §2.3
                        'n_ventanas_valido': n_ventanas, 'peso_actual': 1.0,
                        'primera_deteccion_utc': primera,
                        'ultima_validacion_utc': generado_utc,
                        'generado_utc': generado_utc}
                filas.append(fila)
                historial.append({
                    'pattern_key': c['pattern_key'], 'symbol': symbol,
                    'regime': c['regime'], 'feature': c['feature'],
                    'bucket': c['bucket'], 'label_objetivo': c['label_objetivo'],
                    'lift': c['lift'], 'lift_lower': c['lift_lower'],
                    'n': c['n'], 'p_local': c['p_local'], 'capa': capa,
                    'status': status, 'ventana_inicio_utc': ventana,
                    'registrado_utc': generado_utc,
                })

        # Degradaciones: LONG contradicho o SUSPENDED prolongado
        claves_vivas = set(c['pattern_key'] for c in
                           [x for v in por_ventana.values() for x in v])
        ahora = datetime.now(timezone.utc)
        for (key, capa), p in previos.items():
            if key in claves_vivas:
                continue
            status = p.get('status')
            nueva = None
            if status == 'STABLE' and capa == 'LONG':
                n = int(p.get('n_ventanas_valido') or 0) + 1
                nueva = {**p, 'n_ventanas_valido': 0, 'generado_utc': generado_utc,
                         'status': 'SUSPENDED' if n >= 2 else status}
            elif status == 'SUSPENDED':
                ult = p.get('ultima_validacion_utc')
                meses = 3
                if ult:
                    try:
                        dt_ult = datetime.fromisoformat(ult)
                        meses = (ahora - dt_ult).days / 30.0
                    except Exception:
                        pass
                nueva = {**p, 'generado_utc': generado_utc,
                         'status': 'DEPRECATED' if meses >= 2 else status}
            if nueva:
                filas.append(nueva)

        if filas:
            await ldb.guardar_learned_patterns_batch(filas)
        if historial:
            await ldb.guardar_pattern_history_batch(historial)
        return len(filas)


# ═══════════════════════════════════════════════════════════════════════════
# P2 · ProfilePublisher — perfiles por moneda, cache TTL
# ═══════════════════════════════════════════════════════════════════════════
class ProfilePublisher:
    def __init__(self, ttl_seg: int = 600):
        self.ttl = ttl_seg
        self._cache: dict = {}

    async def publicar(self, symbol: str, perfil: dict):
        generado = _utc_iso()
        await ldb.publicar_perfil(symbol, perfil, generado)
        self._cache[symbol] = (time.time(), {'perfil': perfil, 'generado_utc': generado})

    async def get_perfil(self, symbol: str):
        hit = self._cache.get(symbol)
        if hit and (time.time() - hit[0]) < self.ttl:
            return hit[1]
        data = await ldb.cargar_perfil(symbol)
        if data:
            self._cache[symbol] = (time.time(), data)
        return data

    def invalidar(self, symbol: str = None):
        if symbol:
            self._cache.pop(symbol, None)
        else:
            self._cache.clear()

    @staticmethod
    def construir_perfil(symbol: str, patterns: list) -> dict:
        """Consolida patrones ACTIVE/STABLE por régimen en estructura runtime."""
        por_regimen = defaultdict(lambda: {'patrones': [], 'sugerencias': []})
        base_rates = defaultdict(float)
        for p in patterns:
            if p.get('status') not in ('ACTIVE', 'STABLE'):
                continue
            entrada = {
                'feature': p['feature'], 'bucket': p['bucket'],
                'rango_valor': p.get('rango_valor'),
                'label_objetivo': p['label_objetivo'],
                'lift': p['lift'], 'lift_lower': p.get('lift_lower'),
                'p_local': p['p_local'], 'p_baseline': p['p_baseline'],
                'n': p['n'], 'capa': p.get('capa'), 'status': p.get('status'),
            }
            por_regimen[p['regime']]['patrones'].append(entrada)
            sug = (f"{p['feature']} {p['bucket']} → {p['label_objetivo']} "
                   f"(lift {p['lift']}x, n={p['n']})")
            por_regimen[p['regime']]['sugerencias'].append(sug)

        for reg, data in por_regimen.items():
            data['patrones'].sort(key=lambda x: -(x['lift'] or 0))
            data['sugerencias'] = data['sugerencias'][:10]
            for lab in LABEL_OBJETIVOS:
                ps = [p['p_baseline'] for p in data['patrones']
                      if p['label_objetivo'] == lab]
                if ps:
                    base_rates[f"{reg}|{lab}"] = round(sum(ps) / len(ps), 4)

        return {
            'symbol': symbol,
            'generado_utc': _utc_iso(),
            'model_version': MODEL_VERSION,
            'base_rates': dict(base_rates),
            'por_regimen': {k: dict(v) for k, v in por_regimen.items()},
        }


# ═══════════════════════════════════════════════════════════════════════════
# P3 · RegimeTransitionTracker
# ═══════════════════════════════════════════════════════════════════════════
class RegimeTransitionTracker:
    def __init__(self):
        self._prev_features: dict = {}
        self._modelo: list = []        # patrones de transición (RAM)

    def detect_and_log(self, symbol: str, fila: dict, transicion: tuple):
        """Acumula transiciones en RAM (flush batch desde el loop de background)."""
        from_reg, to_reg, dur_min = transicion
        prev = self._prev_features.get(symbol, {})
        fila_rt = {
            'symbol': symbol,
            'timestamp_15m': fila['timestamp_15m'],
            'from_regime': from_reg,
            'to_regime': to_reg,
            'adx_15m': fila.get('adx_15m'),
            'adx_slope_3v': fila.get('adx_slope_3v'),
            'mfm_sma5': fila.get('mfm_sma5'),
            'mfm_flip': 1 if (prev.get('mfm_sma5') is not None and fila.get('mfm_sma5') is not None
                              and prev['mfm_sma5'] * fila['mfm_sma5'] < 0) else 0,
            'volume_ratio': fila.get('volume_ratio'),
            'rsi_15m': fila.get('rsi_15m'),
            'atr_pct_15m': fila.get('atr_pct_15m'),
            'precio_antes': prev.get('price'),
            'precio_despues': fila.get('price'),
            'duracion_regimen_previo_min': dur_min,
            'fue_transicion_real': None,
            'tiempo_confirmacion_min': None,
        }
        return fila_rt

    def actualizar_previo(self, symbol: str, fila: dict):
        self._prev_features[symbol] = fila

    async def validar_transiciones_maduras(self, minutos_confirmacion: int = 120):
        """
        Valida post-hoc: si el régimen nuevo persistió >= minutos_confirmacion,
        la transición fue real. Usa feature_snapshots (regime_confirmado).
        """
        pendientes = await ldb.cargar_transiciones_pendientes(
            _now_ms() - minutos_confirmacion * 60_000)
        hechas = 0
        for t in pendientes:
            ts_val = int(t['timestamp_15m']) + minutos_confirmacion * 60_000
            filas = await ldb.cargar_velas_mirror(t['symbol'], ts_val - 60_000, ts_val + 60_000)
            persistio = bool(filas)   # si hay velas posteriores, el régimen nuevo siguió
            await ldb.marcar_transicion_real(t['id'], persistio, minutos_confirmacion)
            hechas += 1
        return hechas


# ═══════════════════════════════════════════════════════════════════════════
# P4 · ExitOutcomeLabeler — trayectorias MFE/MAE + trend-end labels
# ═══════════════════════════════════════════════════════════════════════════
class ExitOutcomeLabeler:
    """
    Tracking 100% en RAM (corrección C-2: el executor no debe esperar I/O).
    La persistencia se hace por lotes desde el loop de background.
    """

    def __init__(self, fee_pct: float = 0.05):
        self.fee_pct = fee_pct
        self._trayectorias: dict = {}
        self._lock = threading.Lock()
        self._pending_exits: list = []

    # ── API síncrona para el executor ─────────────────────────────────────
    def registrar_entrada(self, posicion_id: str, symbol: str, direccion: str,
                          precio: float, qty: float = 0.0, grid_id: int = None,
                          tipo: str = 'REAL', regime_entrada: str = None):
        with self._lock:
            self._trayectorias[posicion_id] = {
                'posicion_id': posicion_id, 'symbol': symbol, 'grid_id': grid_id,
                'tipo': tipo, 'direccion': direccion,
                'timestamp_entrada': _now_ms(), 'precio_entrada': float(precio),
                'qty': float(qty) if qty else None,
                'mfe_pct': 0.0, 'mae_pct': 0.0, 'mfe_minuto': 0, 'mae_minuto': 0,
                'peak_price': float(precio),
                'regime_entrada': regime_entrada,
            }

    def track_position_trajectory(self, posicion_id: str, symbol: str = None,
                                  precio: float = None, **kwargs):
        if precio is None:
            return
        with self._lock:
            t = self._trayectorias.get(posicion_id)
            if not t:
                return
            p0 = t['precio_entrada']
            if t['direccion'] == 'LONG':
                fav = (precio - p0) / p0 * 100.0
                adv = (p0 - precio) / p0 * 100.0
            else:
                fav = (p0 - precio) / p0 * 100.0
                adv = (precio - p0) / p0 * 100.0
            mins = int((_now_ms() - t['timestamp_entrada']) / 60_000)
            if fav > t['mfe_pct']:
                t['mfe_pct'] = fav
                t['mfe_minuto'] = mins
                t['peak_price'] = float(precio)
            if adv > t['mae_pct']:
                t['mae_pct'] = adv
                t['mae_minuto'] = mins

    def trayectoria(self, posicion_id: str):
        with self._lock:
            t = self._trayectorias.get(posicion_id)
            return dict(t) if t else None

    def registrar_salida(self, posicion_id: str, precio_salida: float,
                         razon: str = 'DESCONOCIDA', fee_pct: float = None):
        with self._lock:
            t = self._trayectorias.pop(posicion_id, None)
        if not t:
            return None
        p0 = t['precio_entrada']
        if t['direccion'] == 'LONG':
            pnl_pct = (precio_salida - p0) / p0 * 100.0
        else:
            pnl_pct = (p0 - precio_salida) / p0 * 100.0
        fees = (fee_pct if fee_pct is not None else 2 * self.fee_pct)
        pnl_neto = pnl_pct - fees
        mfe = max(t['mfe_pct'], 0.0)
        capturado = (pnl_pct / mfe * 100.0) if mfe > 0.01 else None
        fila = {
            **{k: t[k] for k in ('posicion_id', 'symbol', 'grid_id', 'tipo',
                                 'direccion', 'timestamp_entrada', 'precio_entrada',
                                 'qty', 'mfe_pct', 'mfe_minuto', 'mae_pct',
                                 'mae_minuto', 'regime_entrada')},
            'timestamp_salida': _now_ms(),
            'precio_salida': float(precio_salida),
            'pnl_pct': round(pnl_pct, 4),
            'pnl_usdt': round((pnl_pct / 100.0) * p0 * (t['qty'] or 0), 4) if t.get('qty') else None,
            'fees_pct': fees,
            'pnl_neto_pct': round(pnl_neto, 4),
            'pnl_neto_usdt': round((pnl_neto / 100.0) * p0 * (t['qty'] or 0), 4) if t.get('qty') else None,
            'capturado_pct_de_mfe': round(capturado, 2) if capturado is not None else None,
            'left_on_table_pct': round(mfe - pnl_pct, 4),
            'razon_salida': razon,
            'regime_salida': None,
        }
        with self._lock:
            self._pending_exits.append(fila)
        return fila

    def drenar_exits(self) -> list:
        with self._lock:
            filas, self._pending_exits = self._pending_exits, []
        return filas

    # ── trend-end labels (P4) ─────────────────────────────────────────────
    def etiquetar_trend_end(self, snap: dict, velas: list) -> Optional[dict]:
        """
        Label: ¿revirtió >= umbral (1.5·ATR) en contra de la tendencia del
        régimen en las próximas 2h? (corrección §3B: umbral ATR-normalizado).
        """
        ts = int(snap['timestamp_15m'])
        price = float(snap['price'])
        regime = snap.get('regime_confirmado') or snap.get('regime') or 'UNKNOWN'
        if regime not in ('TREND_UP', 'TREND_DOWN'):
            return None
        u = snap.get('atr_pct_15m') or 0.5
        umbral = 1.5 * u

        futuras = [v for v in velas
                   if ts < v['timestamp'] <= ts + 2 * 3600_000]
        if len(futuras) < 2:
            return None

        magnitud, minutos = 0.0, None
        for i, v in enumerate(futuras, start=1):
            if regime == 'TREND_UP':
                mag = (price - float(v['low'])) / price * 100.0
            else:
                mag = (float(v['high']) - price) / price * 100.0
            magnitud = max(magnitud, mag)
            if minutos is None and mag >= umbral:
                minutos = i * 15

        return {
            'symbol': snap['symbol'],
            'timestamp_15m': ts,
            'regime_actual': regime,
            'adx_slope_3v': snap.get('adx_slope_3v'),
            'mfm_flip': None,
            'volume_ratio': snap.get('volume_ratio'),
            'rsi_divergence': bool(snap.get('rsi_divergence')),
            'distancia_ema50_pct': snap.get('price_rel_ema50_pct'),
            'atr_pct_15m': snap.get('atr_pct_15m'),
            'revirtio_2h': bool(minutos is not None),
            'magnitud_reversion_pct': round(magnitud, 4),
            'minutos_hasta_reversion': minutos,
            'umbral_reversion_pct': round(umbral, 4),
            'tenia_posicion_abierta': False,
            'direccion_posicion': None,
        }


# ═══════════════════════════════════════════════════════════════════════════
# TrendEndModel — P(reversión inminente) desde trend_end_labels
# ═══════════════════════════════════════════════════════════════════════════
class TrendEndModel:
    """
    Mini-modelo interpretable por quintiles (mismo espíritu que PatternLearner).
    Guarda los cortes de quintil en RAM para asignar el bucket exacto del valor
    entrante. Probabilidad vía log-odds aditivas suavizadas; fallback a base rate.
    """

    def __init__(self, half_life_dias: float = 125.0):
        self.half_life_dias = half_life_dias
        self._buckets: dict = {}      # (symbol, feature, bucket) -> (k, n, p)
        self._cortes: dict = {}       # (symbol, feature) -> [4 cortes]
        self._base: dict = {}         # symbol -> P(reversion)

    def entrenar(self, symbol: str, rows: list):
        if not rows:
            return
        ahora = _now_ms()
        datos = []
        for r in rows:
            edad = max(0.0, (ahora - int(r['timestamp_15m'])) / 86400_000.0)
            datos.append({**r, '_w': 0.5 ** (edad / self.half_life_dias)})

        W = sum(d['_w'] for d in datos) or 1.0
        K = sum(d['_w'] for d in datos if d.get('revirtio_2h'))
        self._base[symbol] = K / W if W else 0.5

        buckets = {}
        for feature in TREND_END_FEATURES:
            filas = [d for d in datos if d.get(feature) is not None]
            if len(filas) < 30:
                continue
            vals = [float(d[feature]) for d in filas]
            pesos = [d['_w'] for d in filas]
            cortes = PatternLearner._quintiles_ponderados(vals, pesos)
            self._cortes[(symbol, feature)] = cortes
            for d in filas:
                b = PatternLearner._bucket(float(d[feature]), cortes)
                key = (symbol, feature, b)
                k, n = buckets.get(key, (0.0, 0.0))
                w = d['_w']
                buckets[key] = (k + (w if d.get('revirtio_2h') else 0.0), n + w)
        self._buckets = {k: (kk, nn, kk / nn if nn else 0.0)
                         for k, (kk, nn) in buckets.items()}

    def prob_reversion(self, symbol: str, features: dict) -> float:
        base = self._base.get(symbol, 0.5)
        ajustes = []
        for feature in TREND_END_FEATURES:
            v = (features or {}).get(feature)
            if v is None:
                continue
            cortes = self._cortes.get((symbol, feature))
            if not cortes:
                continue
            bucket = PatternLearner._bucket(float(v), cortes)
            data = self._buckets.get((symbol, feature, bucket))
            if not data or data[1] < 10:
                continue
            p_b = _clamp(data[2], 0.02, 0.98)
            ajustes.append(math.log(p_b / (1 - p_b))
                           - math.log(max(1e-3, base) / max(1e-3, 1 - base)))
        if not ajustes:
            return base
        logit = math.log(max(1e-3, base) / max(1e-3, 1 - base)) \
            + sum(ajustes) / len(ajustes) * 0.5      # suavizado conservador
        return 1.0 / (1.0 + math.exp(-logit))


# ═══════════════════════════════════════════════════════════════════════════
# P5 · ExitPolicyEngine — política dinámica de salida (neto de fees)
# ═══════════════════════════════════════════════════════════════════════════
class ExitPolicyEngine:
    """
    Reglas (montos netos de fees, corrección §3C):

        prob_reversion > 0.60 y ganancia_neta > 0.05%  → CERRAR_AHORA
        prob_reversion > 0.40 y ganancia_neta > 0.15%  → TRAILING
                                                        (bloquea 70% de la ganancia)
        resto                                          → MANTENER (TP fijo sigue)

    En SHADOW solo se loggea (predictions_log) — nunca se actúa.
    """

    def __init__(self, trend_model: TrendEndModel, fee_pct: float = 0.05):
        self.trend_model = trend_model
        self.fee_pct = fee_pct
        self._prob_cache: dict = {}
        self._cache_lock = threading.Lock()

    def compute_exit_policy(self, symbol: str, posicion_id: str,
                            precio_actual: float, trayectoria: dict,
                            features: dict = None) -> dict:
        # Prob de reversión cacheada por vela 15m (corrección C-2: sin I/O)
        ts15 = (_now_ms() // 900_000) * 900_000
        key = (symbol, ts15)
        with self._cache_lock:
            prob = self._prob_cache.get(key)
        if prob is None:
            prob = self.trend_model.prob_reversion(symbol, features or {})
            with self._cache_lock:
                self._prob_cache[key] = prob

        if not trayectoria:
            return {'accion': 'MANTENER', 'prob_reversion': prob,
                    'trailing_stop': None, 'confianza': 0.0}

        p0 = trayectoria['precio_entrada']
        direccion = trayectoria['direccion']
        if direccion == 'LONG':
            bruto = (precio_actual - p0) / p0 * 100.0
        else:
            bruto = (p0 - precio_actual) / p0 * 100.0
        ganancia_neta = bruto - 2 * self.fee_pct

        if prob > 0.60 and ganancia_neta > 0.05:
            return {'accion': 'CERRAR_AHORA', 'prob_reversion': prob,
                    'trailing_stop': None, 'confianza': prob,
                    'ganancia_neta_pct': ganancia_neta}
        if prob > 0.40 and ganancia_neta > 0.15:
            trailing = round(max(0.10, ganancia_neta * 0.30), 4)
            return {'accion': 'TRAILING', 'prob_reversion': prob,
                    'trailing_stop': trailing, 'confianza': prob,
                    'ganancia_neta_pct': ganancia_neta}
        return {'accion': 'MANTENER', 'prob_reversion': prob,
                'trailing_stop': None, 'confianza': prob,
                'ganancia_neta_pct': ganancia_neta}


# ═══════════════════════════════════════════════════════════════════════════
# ANEXO A §5 · DriftDetector — PSI por feature (informativo, sin auto-acción)
# ═══════════════════════════════════════════════════════════════════════════
class DriftDetector:
    """
    Population Stability Index por feature entre distribución de referencia
    (90 días) y distribución reciente (7 días). Implementación stdlib
    (el anexo usa numpy; no hace falta para 10 bins).

        PSI < 0.10        → sin drift
        0.10 <= PSI < 0.20 → drift moderado (log)
        PSI >= 0.20        → drift significativo (alerta, NUNCA auto-acción)
    """

    FEATURES_CLAVE = (
        'rsi_15m', 'adx_15m', 'atr_pct_15m',
        'mfm_sma5', 'volume_ratio', 'price_rel_ema50_pct',
    )
    BINS = 10

    async def detectar(self, symbol: str) -> dict:
        from config import CONFIG
        umbral = getattr(CONFIG, 'learning_drift_psi_threshold', 0.20)
        ref = await ldb.cargar_features_ventana(symbol, dias=90, features=self.FEATURES_CLAVE)
        reciente = await ldb.cargar_features_ventana(symbol, dias=7, features=self.FEATURES_CLAVE)

        if len(ref) < 200 or len(reciente) < 50:
            return {'symbol': symbol, 'status': 'insuficiente_datos',
                    'psi_por_feature': {}, 'features_con_drift': []}

        resultado = {}
        for feat in self.FEATURES_CLAVE:
            vals_ref = [float(f[feat]) for f in ref if f.get(feat) is not None]
            vals_rec = [float(f[feat]) for f in reciente if f.get(feat) is not None]
            if len(vals_ref) < 100 or len(vals_rec) < 30:
                continue
            resultado[feat] = round(self._calcular_psi(vals_ref, vals_rec), 4)

        alertas = [f for f, psi in resultado.items() if psi >= umbral]
        return {
            'symbol': symbol,
            'psi_por_feature': resultado,
            'features_con_drift': alertas,
            'status': 'ALERTA' if alertas else 'OK',
        }

    @classmethod
    def _calcular_psi(cls, ref: list, act: list) -> float:
        import bisect
        s = sorted(ref)
        n = len(s)
        cortes = [s[min(n - 1, int(n * q / cls.BINS))] for q in range(1, cls.BINS)]

        def _hist(vals):
            h = [0] * cls.BINS
            for v in vals:
                h[bisect.bisect_right(cortes, v)] += 1
            return h

        h_ref, h_act = _hist(ref), _hist(act)
        n_ref, n_act = max(1, len(ref)), max(1, len(act))
        psi = 0.0
        for i in range(cls.BINS):
            p_r = max(h_ref[i] / n_ref, 1e-6)
            p_a = max(h_act[i] / n_act, 1e-6)
            psi += (p_a - p_r) * math.log(p_a / p_r)
        return float(psi)


# ═══════════════════════════════════════════════════════════════════════════
# LearningEngine — fachada + loop de background + serving
# ═══════════════════════════════════════════════════════════════════════════
class LearningEngine:
    """
    Punto único de integración:

        signals_v5  → engine.on_vela_15m(symbol)               (sync, RAM)
        signals_v5  → await engine.get_perfil(symbol)           (serving)
        signals_v5  → engine.compute_confidence(perfil, dir)    (refusal)
        executor    → engine.registrar_entrada(...)             (sync)
        executor    → engine.track_position_trajectory(...)     (sync)
        executor    → engine.registrar_salida(...)              (sync)
        executor    → engine.compute_exit_policy(...)           (sync)
        main.py     → engine.run() / engine.stop()              (background)
    """

    def __init__(self, signals, velas_loader: Callable = None,
                 fee_pct: float = None, half_life_dias: float = None,
                 learner_hora_local: int = 3, timezone_offset_horas: float = 0.0):
        from config import CONFIG
        self.signals = signals
        self.cfg = CONFIG
        self.fee_pct = fee_pct if fee_pct is not None else getattr(
            CONFIG, 'learning_fee_rate', 0.0005) * 100.0
        self.half_life = half_life_dias or getattr(
            CONFIG, 'learning_decay_half_life_dias', 125)

        self.regime_cls = RegimeClassifier(
            min_velas=getattr(CONFIG, 'learning_regime_min_velas', 4))
        self.collector = SnapshotCollector(signals, self.regime_cls)
        self.velas_loader = velas_loader or self._default_velas_loader
        self.labeler = OutcomeLabeler(self.velas_loader, fee_pct=self.fee_pct)
        self.pattern_learner = PatternLearner(
            half_life_dias=self.half_life,
            min_lift=getattr(CONFIG, 'learning_min_lift', 1.5),
            min_lift_lower=getattr(CONFIG, 'learning_min_lift_lower', 1.2),
            min_n=getattr(CONFIG, 'learning_min_n', 40),
            fdr_q=getattr(CONFIG, 'learning_fdr_q', 0.10))
        self.publisher = ProfilePublisher()
        self.transition_tracker = RegimeTransitionTracker()
        self.exit_labeler = ExitOutcomeLabeler(fee_pct=self.fee_pct)
        self.trend_model = TrendEndModel(half_life_dias=self.half_life)
        self.exit_policy = ExitPolicyEngine(self.trend_model, fee_pct=self.fee_pct)

        # ANEXO A §4/§5: guardrails financieros + drift detection
        from learning_guardrails import LearningGuardrails
        self.guardrails = LearningGuardrails(self)
        self.drift_detector = DriftDetector()
        self.notifier = None               # inyectado desde main.py

        self.learner_hora_local = learner_hora_local
        self.tz_offset = timezone_offset_horas

        self._stop = threading.Event()
        self._task: Optional[asyncio.Task] = None
        self._pending_transiciones: list = []
        self._lock_t = threading.Lock()

        self._last_label_run = 0.0
        self._last_sync_velas = 0.0
        self._last_learner_run = 0.0
        self._last_transiciones_run = 0.0
        self._last_outcomes_run = 0.0
        self._last_guardrails_run = 0.0
        self._last_drift_run = 0.0
        self._drift_notificado: dict = {}   # symbol -> ts (anti-spam 24h)

    # ── loader por defecto: database_v5 (MySQL del trading) ───────────────
    @staticmethod
    async def _default_velas_loader(symbol: str, tf: str, limit: int):
        from database_v5 import cargar_velas_historicas
        return await cargar_velas_historicas(symbol, tf, limit)

    # ═════════════════════════════════════════════════════════════════════
    # Hooks síncronos (hot loop)
    # ═════════════════════════════════════════════════════════════════════
    def on_vela_15m(self, symbol: str):
        """P1 — captura de snapshot. Sync, RAM-only (corrección C-4/C-2)."""
        self.collector.on_vela_15m(symbol)

    def registrar_entrada(self, posicion_id: str, symbol: str, direccion: str,
                          precio: float, **kw):
        self.exit_labeler.registrar_entrada(posicion_id, symbol, direccion,
                                            precio, **kw)

    def track_position_trajectory(self, posicion_id: str, symbol: str = None,
                                  precio: float = None, **kw):
        self.exit_labeler.track_position_trajectory(posicion_id, symbol=symbol,
                                                    precio=precio, **kw)

    def trayectoria(self, posicion_id: str):
        return self.exit_labeler.trayectoria(posicion_id)

    def registrar_salida(self, posicion_id: str, precio_salida: float,
                         razon: str = 'DESCONOCIDA', **kw):
        return self.exit_labeler.registrar_salida(posicion_id, precio_salida,
                                                  razon=razon, **kw)

    def compute_exit_policy(self, symbol: str, posicion_id: str,
                            precio_actual: float, features: dict = None) -> dict:
        tray = self.exit_labeler.trayectoria(posicion_id)
        return self.exit_policy.compute_exit_policy(symbol, posicion_id,
                                                    precio_actual, tray, features)

    # ═════════════════════════════════════════════════════════════════════
    # Serving (refusal-first)
    # ═════════════════════════════════════════════════════════════════════
    async def get_perfil(self, symbol: str):
        return await self.publisher.get_perfil(symbol)

    def compute_confidence(self, perfil_data: dict, direction: str,
                           features: dict = None) -> float:
        """
        P(rentable | dirección) combinando patrones ACTIVE/STABLE del régimen
        actual vía log-odds aditivas (conservador; calibración fina en Fase 9).
        Sin patrones → 0.5 (neutral: jamás dispara un rechazo por sí solo).
        """
        if not perfil_data:
            return 0.5
        perfil = perfil_data.get('perfil', perfil_data)
        regime = (features or {}).get('regime') or 'RANGE'
        patrones = perfil.get('por_regimen', {}).get(regime, {}).get('patrones', [])
        if not patrones:
            return 0.5

        lab_objetivo = 'LONG_STRONG' if direction == 'LONG' else 'SHORT_STRONG'
        lab_contraria = 'SHORT_STRONG' if direction == 'LONG' else 'LONG_STRONG'

        p0 = perfil.get('base_rates', {}).get(f"{regime}|{lab_objetivo}", 0.25)
        p0 = _clamp(p0 if p0 and p0 > 0 else 0.25, 0.05, 0.95)
        logit = math.log(p0 / (1 - p0))

        # Coincidencia de bucket cuando las features vienen dadas:
        # si el valor de la feature no cae en el rango del patrón, el patrón
        # no aplica a este snapshot (evita promediar evidencia irrelevante).
        feats = features or {}
        usados = 0
        for p in patrones[:12]:
            v = feats.get(p['feature'])
            if v is not None and not _valor_en_rango(float(v), p.get('rango_valor')):
                continue
            if p['label_objetivo'] == lab_objetivo:
                w = 0.25 * min(1.0, (p.get('n') or 0) / 200.0)
                logit += w * math.log(max(1.0, p.get('lift') or 1.0))
                usados += 1
            elif p['label_objetivo'] == lab_contraria:
                w = 0.25 * min(1.0, (p.get('n') or 0) / 200.0)
                logit -= w * math.log(max(1.0, p.get('lift') or 1.0))
                usados += 1

        if usados == 0:
            return 0.5
        prob = 1.0 / (1.0 + math.exp(-logit))
        return round(_clamp(prob, 0.02, 0.98), 4)

    def log_prediccion(self, symbol: str, timestamp_15m: int, prob: float,
                       decision_sugerida: str, decision_tomada: str,
                       modo: str, direction: str = None, score_macro: int = None,
                       regime: str = None, prob_reversion: float = None):
        """Buffer RAM → predictions_log (flush en background)."""
        fila = {
            'symbol': symbol, 'timestamp_15m': int(timestamp_15m),
            'model_version': MODEL_VERSION, 'prob_rentable': prob,
            'prob_reversion': prob_reversion,
            'decision_sugerida': decision_sugerida,
            'score_macro': score_macro, 'direction': direction, 'regime': regime,
            'decision_tomada': decision_tomada, 'modo': modo,
        }
        with self._lock_t:
            self._pending_transiciones.append(('pred', fila))

    def log_exit_shadow(self, posicion_id: str, policy: dict, symbol: str = None):
        self.log_prediccion(
            symbol=symbol or 'UNKNOWN', timestamp_15m=_now_ms(),
            prob=policy.get('confianza') or 0.5,
            decision_sugerida=policy.get('accion', 'MANTENER'),
            decision_tomada='IGNORADA', modo='SHADOW',
            prob_reversion=policy.get('prob_reversion'))

    # ═════════════════════════════════════════════════════════════════════
    # Loop de background (Fases 1-8)
    # ═════════════════════════════════════════════════════════════════════
    async def run(self):
        print("  🧠 [LEARN] LearningEngine iniciado "
              f"(modo={getattr(self.cfg, 'learning_apply_mode', 'OFF')}, "
              f"exit={getattr(self.cfg, 'learning_exit_mode', 'OFF')})")
        tick = 0
        while not self._stop.is_set():
            try:
                await self._tick(tick)
            except Exception as e:
                print(f"  ⚠️ [LEARN] tick {tick}: {e}")
            tick += 1
            await asyncio.sleep(60)

    async def _tick(self, tick: int):
        ahora = time.time()

        # 1) Flush de snapshots + transiciones de régimen (batch executemany)
        filas = self.collector.drenar_buffer()
        if filas:
            await ldb.guardar_snapshots_batch(filas)
        trans = self.collector.drenar_transiciones()
        if trans:
            await ldb.guardar_regime_transitions_batch(trans)

        # 2) Flush de exits y predicciones en RAM
        exits = self.exit_labeler.drenar_exits()
        for fila in exits:
            await ldb.guardar_exit_outcome(fila)
        with self._lock_t:
            preds = [f for k, f in self._pending_transiciones if k == 'pred']
            self._pending_transiciones = [x for x in self._pending_transiciones if x[0] != 'pred']
        for p in preds:
            await ldb.log_prediccion(p)

        # 3) Sync de velas cada 15 min (mirror MySQL → SQLite)
        if ahora - self._last_sync_velas > 900:
            self._last_sync_velas = ahora
            await self.labeler.sincronizar_velas(list(self.cfg.symbols))

        # 4) Labeler cada 10 min
        if ahora - self._last_label_run > 600:
            self._last_label_run = ahora
            n = await self.labeler.procesar_pendientes(list(self.cfg.symbols))
            if n:
                print(f"  🧠 [LEARN] {n} snapshots etiquetados")

        # 5) Validación de transiciones cada hora
        if ahora - self._last_transiciones_run > 3600:
            self._last_transiciones_run = ahora
            await self.transition_tracker.validar_transiciones_maduras()

        # 6) Completar outcomes en predictions_log (cada hora)
        if ahora - self._last_outcomes_run > 3600:
            self._last_outcomes_run = ahora
            await ldb.completar_outcomes_predicciones(_now_ms())

        # 6b) ANEXO A §5: drift detection (cada hora, solo alerta)
        if ahora - self._last_drift_run > 3600:
            self._last_drift_run = ahora
            for symbol in self.cfg.symbols:
                try:
                    drift = await self.drift_detector.detectar(symbol)
                    if drift.get('status') == 'ALERTA':
                        await ldb.guardar_drift_alert(symbol, drift)
                        if (ahora - self._drift_notificado.get(symbol, 0)) > 86400:
                            self._drift_notificado[symbol] = ahora
                            print(f"  ⚠️ [LEARN-DRIFT] {symbol}: features con drift "
                                  f"{drift['features_con_drift']} "
                                  f"(PSI {drift['psi_por_feature']})")
                            if self.notifier and getattr(CONFIG, 'learning_alerta_telegram', True):
                                try:
                                    await self.notifier.enviar_telegram(
                                        f"⚠️ <b>DRIFT DETECTADO — {symbol}</b>\n"
                                        f"Features con shift: "
                                        f"{', '.join(drift['features_con_drift'])}\n"
                                        f"PSI: <code>{drift['psi_por_feature']}</code>\n"
                                        f"<i>Revisar si los patrones siguen vigentes. "
                                        f"Sin auto-acción (§5.4).</i>")
                                except Exception:
                                    pass
                except Exception as e_drift:
                    print(f"  ⚠️ [LEARN-DRIFT] {symbol}: {e_drift}")

        # 6c) ANEXO A §4: guardrails financieros (cada 15 min)
        if ahora - self._last_guardrails_run > 900:
            self._last_guardrails_run = ahora
            self.guardrails.notifier = self.notifier
            await self.guardrails.evaluar()

        # 7) Learner diario a la hora configurada
        if self._es_hora_learner() and (ahora - self._last_learner_run > 20 * 3600):
            self._last_learner_run = ahora
            await self.entrenar_todo()

    def _es_hora_learner(self) -> bool:
        hora = (datetime.now(timezone.utc).hour + int(self.tz_offset)) % 24
        return hora == self.learner_hora_local

    async def entrenar_todo(self):
        """Fase 3/6/7: minería 3 ventanas + perfiles + trend-end model."""
        generado = _utc_iso()
        ahora = _now_ms()
        ventanas = {
            'SHORT': getattr(self.cfg, 'learning_ventana_short_dias', 30),
            'MEDIUM': getattr(self.cfg, 'learning_ventana_medium_dias', 180),
            'LONG': getattr(self.cfg, 'learning_ventana_long_dias', 1825),
        }
        total_patrones = 0
        for symbol in self.cfg.symbols:
            por_ventana = {}
            for nombre, dias in ventanas.items():
                rows = await ldb.cargar_dataset_labels(
                    symbol, ahora - dias * 86400_000, ahora)
                por_ventana[nombre] = self.pattern_learner.minar_ventana(
                    rows, ahora, symbol, regime='GLOBAL', ventana_nombre=nombre)
                # Minería por régimen confirmado
                for regime in ('RANGE', 'TREND_UP', 'TREND_DOWN', 'VOLATILE'):
                    sub = [r for r in rows if (r.get('regime_confirmado') or r.get('regime')) == regime]
                    por_ventana[nombre] += self.pattern_learner.minar_ventana(
                        sub, ahora, symbol, regime=regime, ventana_nombre=nombre)

            total_patrones += await self.pattern_learner.consolidar_capas(
                symbol, por_ventana, generado)

            patterns = await ldb.cargar_learned_patterns(symbol)
            perfil = self.publisher.construir_perfil(symbol, patterns)
            await self.publisher.publicar(symbol, perfil)

            # TrendEndModel (P4)
            te_rows = await ldb.cargar_trend_end_dataset(
                symbol, ahora - 180 * 86400_000, ahora)
            self.trend_model.entrenar(symbol, te_rows)

        print(f"  🧠 [LEARN] learner diario completado: {total_patrones} patrones consolidados")

    # ── trend-end labels batch (P4) ───────────────────────────────────────
    async def etiquetar_trend_ends(self, symbols: list, max_lote: int = 500):
        ahora = _now_ms()
        snaps = await ldb.cargar_snapshots_por_regimen(
            ('TREND_UP', 'TREND_DOWN'), desde_ms=ahora - 30 * 86400_000,
            hasta_ms=ahora - 2 * 3600_000, limit=max_lote)
        filas = []
        for s in snaps:
            velas = await ldb.cargar_velas_mirror(
                s['symbol'], int(s['timestamp_15m']), int(s['timestamp_15m']) + 2 * 3600_000)
            fila = self.exit_labeler.etiquetar_trend_end(s, velas)
            if fila:
                filas.append(fila)
        if filas:
            await ldb.guardar_trend_end_labels_batch(filas)
        return len(filas)

    def stop(self):
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
        print("  🧠 [LEARN] LearningEngine detenido")
