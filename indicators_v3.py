import asyncio
import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import Dict, Optional
from collections import deque
from config import CONFIG


class IndicatorEngineV3:
    """
    Motor de indicadores V3.1 — Multi-Timeframe Sniper Optimizado.

    MEJORAS CLAVE vs V3:
    ─────────────────────
    1. EMA50 de 15m añadido para dirección intradía (fix análisis cruzado).
    2. EMA_300 incremental: ~0.1ms por símbolo (vs ~50ms recalculando pandas_ta).
    3. RSI(7) incremental: ~0.05ms por símbolo (vs ~20ms recalculando pandas_ta).
    4. ATR(14) en 1m: contexto de significancia para mechas de gatillo.
    5. Validación de mecha mejorada: flags de rechazo confirmado vs EMA_300.

    FASE 1 — ESTABILIDAD:
    ─────────────────────
    • Lock por símbolo en procesamiento 1m: evita race conditions en estado
      incremental si llegan velas desordenadas por reconexión WS.

    Mantiene 100% compatibilidad de interfaz con V3.
    """

    def __init__(self, queue_in: asyncio.Queue, queue_out: asyncio.Queue,
                 buffers_1m: Optional[Dict[str, pd.DataFrame]] = None,
                 buffers_15m: Optional[Dict[str, pd.DataFrame]] = None,
                 buffers_4h: Optional[Dict[str, pd.DataFrame]] = None):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.buffers_1m: Dict[str, pd.DataFrame] = buffers_1m or {}
        self.buffers_15m: Dict[str, pd.DataFrame] = buffers_15m or {}
        self.buffers_4h: Dict[str, pd.DataFrame] = buffers_4h or {}

        # ─── Estado incremental 1M (Motor Micro) ───
        self._ema300_state: Dict[str, dict] = {}
        self._rsi7_state: Dict[str, dict] = {}
        self._atr1m_state: Dict[str, dict] = {}

        # ─── FASE 1: Locks por símbolo ───
        self._symbol_locks: Dict[str, asyncio.Lock] = {}

        self._k_ema300 = 2.0 / (CONFIG.ema_micro_period + 1)

    def _get_lock(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._symbol_locks:
            self._symbol_locks[symbol] = asyncio.Lock()
        return self._symbol_locks[symbol]

    # ================================================================
    # PRECÁLCULO (Cold Start)
    # ================================================================
    async def precalcular(self):
        print("📊 Precalculando indicadores V3.1 (con EMA50 15m)...")

        for symbol, df in self.buffers_1m.items():
            n = len(df)
            if n >= CONFIG.ema_micro_period:
                self._init_estado_1m(symbol, df)
                result = self.calcular_1m(symbol, incremental=True)
                if result:
                    await self.queue_out.put(('1m', symbol, result))
                    print(f"  ✅ {symbol} 1M precalculado | "
                          f"RSI7={result.get('rsi_7', 'N/A'):.1f} "
                          f"EMA300={result.get('ema_300', 'N/A'):.4f} "
                          f"ATR1m={result.get('atr_1m', 'N/A'):.4f}")
            else:
                print(f"  ⚠️ {symbol} 1M: solo {n} velas, necesita {CONFIG.ema_micro_period}")

        for symbol, df in self.buffers_15m.items():
            if len(df) >= 50:
                result = self.calcular_15m(symbol)
                if result:
                    await self.queue_out.put(('15m', symbol, result))
                    print(f"  ✅ {symbol} 15M precalculado | "
                          f"RSI={result.get('rsi', 'N/A'):.1f} "
                          f"ADX={result.get('adx', 'N/A'):.1f} "
                          f"EMA50={result.get('ema50_15m', 'N/A'):.4f}")
            else:
                print(f"  ⚠️ {symbol} 15M: solo {len(df)} velas, esperando más...")

        for symbol, df in self.buffers_4h.items():
            if len(df) >= 200:
                result = self.calcular_4h(symbol)
                if result:
                    await self.queue_out.put(('4h', symbol, result))
                    print(f"  ✅ {symbol} 4H precalculado | EMA200={result.get('ema200_4h', 'N/A'):.4f}")
            else:
                print(f"  ⚠️ {symbol} 4H: solo {len(df)} velas, esperando más...")

        print("📊 Precálculo V3.1 completado. Estado incremental listo.")

    def _init_estado_1m(self, symbol: str, df: pd.DataFrame):
        df = df.copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values

        seed_sma = np.mean(closes[:CONFIG.ema_micro_period])
        ema_values = [seed_sma]
        k = self._k_ema300
        for i in range(CONFIG.ema_micro_period, len(closes)):
            ema_values.append(closes[i] * k + ema_values[-1] * (1 - k))
        self._ema300_state[symbol] = {
            'last_ema': ema_values[-1],
            'k': k,
            'initialized': True
        }

        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        if len(gains) >= 7:
            avg_gain = np.mean(gains[:7])
            avg_loss = np.mean(losses[:7])
            for i in range(7, len(gains)):
                avg_gain = (avg_gain * 6 + gains[i]) / 7
                avg_loss = (avg_loss * 6 + losses[i]) / 7
            self._rsi7_state[symbol] = {
                'avg_gain': avg_gain,
                'avg_loss': avg_loss,
                'last_close': closes[-1],
                'initialized': True
            }
        else:
            rsi_series = ta.rsi(pd.Series(closes), length=7)
            last_rsi = rsi_series.iloc[-1]
            if not pd.isna(last_rsi) and last_rsi < 100:
                rs = last_rsi / (100 - last_rsi)
                avg_loss_est = 1.0
                avg_gain_est = rs * avg_loss_est
            else:
                avg_gain_est = 0.0
                avg_loss_est = 1.0
            self._rsi7_state[symbol] = {
                'avg_gain': avg_gain_est,
                'avg_loss': avg_loss_est,
                'last_close': closes[-1],
                'initialized': True
            }

        tr_list = deque(maxlen=14)
        for i in range(1, len(df)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            tr_list.append(tr)
        self._atr1m_state[symbol] = {
            'tr_deque': tr_list,
            'prev_close': closes[-1],
            'atr': np.mean(list(tr_list)) if tr_list else 0.0,
            'initialized': True
        }

    # ================================================================
    # LOOP PRINCIPAL
    # ================================================================
    async def run(self):
        while True:
            tf, symbol, vela = await self.queue_in.get()

            if tf == '1m':
                if symbol not in self.buffers_1m:
                    self.buffers_1m[symbol] = pd.DataFrame()
                df = pd.concat([self.buffers_1m[symbol], pd.DataFrame([vela])], ignore_index=True)
                self.buffers_1m[symbol] = df.tail(CONFIG.max_velas_1m)

                if len(self.buffers_1m[symbol]) >= CONFIG.ema_micro_period:
                    async with self._get_lock(symbol):
                        if symbol not in self._ema300_state:
                            self._init_estado_1m(symbol, self.buffers_1m[symbol])
                        result = self.calcular_1m(symbol, incremental=True)
                        if result:
                            await self.queue_out.put(('1m', symbol, result))

            elif tf == '15m':
                if symbol not in self.buffers_15m:
                    self.buffers_15m[symbol] = pd.DataFrame()
                df = pd.concat([self.buffers_15m[symbol], pd.DataFrame([vela])], ignore_index=True)
                self.buffers_15m[symbol] = df.tail(CONFIG.max_velas_15m)

                if len(self.buffers_15m[symbol]) >= 50:
                    result = self.calcular_15m(symbol)
                    if result:
                        await self.queue_out.put(('15m', symbol, result))

            elif tf == '4h':
                if symbol not in self.buffers_4h:
                    self.buffers_4h[symbol] = pd.DataFrame()
                df = pd.concat([self.buffers_4h[symbol], pd.DataFrame([vela])], ignore_index=True)
                self.buffers_4h[symbol] = df.tail(CONFIG.max_velas_4h)

                if len(self.buffers_4h[symbol]) >= 200:
                    result = self.calcular_4h(symbol)
                    if result:
                        await self.queue_out.put(('4h', symbol, result))

    # ================================================================
    # CAPA MICRO: calcular_1m (INCREMENTAL)
    # ================================================================
    def calcular_1m(self, symbol: str, incremental: bool = False) -> dict:
        df = self.buffers_1m[symbol].copy()
        if len(df) < CONFIG.ema_micro_period:
            return None

        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        close = float(last['close'])
        high = float(last['high'])
        low = float(last['low'])
        open_p = float(last['open'])
        volume = float(last['volume'])
        prev_close = float(prev['close']) if len(df) > 1 else close

        if incremental and symbol in self._ema300_state and self._ema300_state[symbol]['initialized']:
            ema_state = self._ema300_state[symbol]
            ema_300 = close * ema_state['k'] + ema_state['last_ema'] * (1 - ema_state['k'])
            ema_state['last_ema'] = ema_300
        else:
            df.ta.ema(length=CONFIG.ema_micro_period, append=True)
            ema_300 = float(last[f'EMA_{CONFIG.ema_micro_period}']) if not pd.isna(last.get(f'EMA_{CONFIG.ema_micro_period}')) else None

        if incremental and symbol in self._rsi7_state and self._rsi7_state[symbol]['initialized']:
            rsi_state = self._rsi7_state[symbol]
            gain = max(0.0, close - rsi_state['last_close'])
            loss = max(0.0, rsi_state['last_close'] - close)
            avg_gain = (rsi_state['avg_gain'] * 6 + gain) / 7
            avg_loss = (rsi_state['avg_loss'] * 6 + loss) / 7
            rsi_state['avg_gain'] = avg_gain
            rsi_state['avg_loss'] = avg_loss
            rsi_state['last_close'] = close
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi_7 = 100.0 - (100.0 / (1.0 + rs))
            else:
                rsi_7 = 100.0
        else:
            df.ta.rsi(length=CONFIG.rsi_micro_length, append=True)
            rsi_7 = float(last[f'RSI_{CONFIG.rsi_micro_length}']) if not pd.isna(last.get(f'RSI_{CONFIG.rsi_micro_length}')) else None

        if incremental and symbol in self._atr1m_state and self._atr1m_state[symbol]['initialized']:
            atr_state = self._atr1m_state[symbol]
            tr = max(
                high - low,
                abs(high - atr_state['prev_close']),
                abs(low - atr_state['prev_close'])
            )
            atr_state['tr_deque'].append(tr)
            atr_state['prev_close'] = close
            atr_1m = np.mean(list(atr_state['tr_deque'])) if atr_state['tr_deque'] else 0.0
            atr_state['atr'] = atr_1m
        else:
            df.ta.atr(length=14, append=True)
            atr_1m = float(last['ATRr_14']) if not pd.isna(last.get('ATRr_14')) else None

        wick_upper_pct = ((high - close) / close) * 100 if close > 0 else 0
        wick_lower_pct = ((close - low) / close) * 100 if close > 0 else 0
        body = close - open_p
        body_direction = 1 if body > 0 else (-1 if body < 0 else 0)

        ema_val = ema_300 if ema_300 is not None else 0
        mecha_valida_short = (
            high >= ema_val * 0.9995 and
            close < high * 0.999 and
            body_direction <= 0 and
            wick_upper_pct >= CONFIG.wick_min_pct
        )
        mecha_valida_long = (
            low <= ema_val * 1.0005 and
            close > low * 1.001 and
            body_direction >= 0 and
            wick_lower_pct >= CONFIG.wick_min_pct
        )

        volume_sma20 = float(df['volume'].tail(20).mean())

        return {
            'timestamp': int(last['timestamp']),
            'close': close,
            'open': open_p,
            'high': high,
            'low': low,
            'rsi_7': rsi_7,
            'ema_300': ema_300,
            'wick_upper_pct': round(wick_upper_pct, 3),
            'wick_lower_pct': round(wick_lower_pct, 3),
            'body_direction': body_direction,
            'volume': volume,
            'volume_sma20': volume_sma20,
            'atr_1m': round(atr_1m, 6) if atr_1m is not None else None,
            'mecha_valida_short': mecha_valida_short,
            'mecha_valida_long': mecha_valida_long,
            'ema300_distancia_pct': round(((close - ema_val) / ema_val) * 100, 3) if ema_val > 0 else 0,
        }

    # ================================================================
    # CAPA MACRO: calcular_15m (EMA50 añadido)
    # ================================================================
    def calcular_15m(self, symbol: str) -> dict:
        """Filtro Macro: RSI(14), ADX(14), MACD, ATR, EMAs, volumen, recent_high/low"""
        df = self.buffers_15m[symbol].copy()
        if len(df) < 50:
            return None

        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.ta.rsi(length=14, append=True)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        df.ta.adx(length=14, append=True)
        df.ta.atr(length=14, append=True)
        df.ta.ema(length=200, append=True)
        df.ta.ema(length=50, append=True)   # ← NUEVO: EMA50 para dirección intradía
        df.ta.ema(length=7, append=True)
        df.ta.ema(length=25, append=True)

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        recent_high = float(df['high'].tail(50).max())
        recent_low = float(df['low'].tail(50).min())

        return {
            'timestamp': int(last['timestamp']),
            'close': float(last['close']),
            'rsi': float(last['RSI_14']) if not pd.isna(last['RSI_14']) else None,
            'macd_hist': float(last['MACDh_12_26_9']) if not pd.isna(last['MACDh_12_26_9']) else None,
            'macd_hist_prev': float(prev['MACDh_12_26_9']) if not pd.isna(prev['MACDh_12_26_9']) else None,
            'adx': float(last['ADX_14']) if not pd.isna(last['ADX_14']) else None,
            'atr': float(last['ATRr_14']) if not pd.isna(last['ATRr_14']) else None,
            'ema200_15m': float(last['EMA_200']) if not pd.isna(last['EMA_200']) else None,
            'ema50_15m': float(last['EMA_50']) if not pd.isna(last.get('EMA_50')) else None,  # ← NUEVO
            'volume': float(last['volume']),
            'volume_sma20': float(df['volume'].tail(20).mean()),
            'recent_high': recent_high,
            'recent_low': recent_low,
            'prev_close': float(prev['close']) if len(df) > 1 else float(last['close']),
            'ema7_15m': float(last['EMA_7']) if not pd.isna(last.get('EMA_7')) else None,
            'ema25_15m': float(last['EMA_25']) if not pd.isna(last.get('EMA_25')) else None,
            'ema7_15m_prev': float(prev['EMA_7']) if not pd.isna(prev.get('EMA_7')) else None,
            'ema25_15m_prev': float(prev['EMA_25']) if not pd.isna(prev.get('EMA_25')) else None,
        }

    # ================================================================
    # CAPA SESIÓN: calcular_4h (sin cambios)
    # ================================================================
    def calcular_4h(self, symbol: str) -> dict:
        df = self.buffers_4h[symbol].copy()
        if len(df) < 200:
            return None

        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.ta.ema(length=200, append=True)
        last = df.iloc[-1]

        return {
            'timestamp': int(last['timestamp']),
            'close': float(last['close']),
            'ema200_4h': float(last['EMA_200']) if not pd.isna(last['EMA_200']) else None,
        }
