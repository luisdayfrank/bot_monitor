"""
learning_guardrails.py — ANEXO A §4: Kill-switches financieros del Learning Engine

Auto-rollback a SHADOW cuando el learner se comporta peligrosamente. Se evalúa
cada 15 min desde `LearningEngine.run()`.

Reglas (§4.4):
  | Condición                                          | Acción                     |
  |----------------------------------------------------|----------------------------|
  | reject_ratio_1h  > 80% con >= 5 evaluaciones       | Rollback a SHADOW + alerta |
  | reject_ratio_24h > 60% con >= 10 evaluaciones      | Rollback a SHADOW + alerta |
  | precision de rechazos cae > 10% vs baseline        | Rollback a SHADOW + alerta |
  | capturado_pct_de_mfe < min (7 días, n>=20)         | Exit mode → OFF + alerta   |
  | PSI > umbral en features clave                     | Solo alerta (sin rollback) |

Principios:
  - El rollback es automático; la REACTIVACIÓN siempre es manual
    (evita loops auto-reactivar/auto-degradar).
  - Todo evento queda auditado en `guardrail_events`.
  - Nunca toca el executor ni las reglas base: solo los flags del learner.

Solo stdlib.
"""

import time

from config import CONFIG
import learning_db as ldb


class LearningGuardrails:
    def __init__(self, engine=None, notifier=None):
        self.engine = engine
        self.notifier = notifier
        self._ultimo_evento: dict = {}      # anti-spam: tipo -> ts

    # ── punto de entrada (cada 15 min) ───────────────────────────────────
    async def evaluar(self):
        if not getattr(CONFIG, 'learning_auto_rollback_enabled', True):
            return

        modo = getattr(CONFIG, 'learning_apply_mode', 'OFF')
        exit_modo = getattr(CONFIG, 'learning_exit_mode', 'OFF')

        if modo == 'ACTIVE':
            await self._check_ratio('REJECT', 1.0,
                                    getattr(CONFIG, 'learning_max_reject_ratio_hourly', 0.80),
                                    min_evals=5, severidad='CRITICO')
            await self._check_ratio('REJECT', 24.0,
                                    getattr(CONFIG, 'learning_max_reject_ratio_daily', 0.60),
                                    min_evals=10, severidad='CRITICO')
            await self._check_precision()

        if exit_modo == 'ACTIVE':
            await self._check_exit_captura()

    # ── reglas ───────────────────────────────────────────────────────────
    async def _check_ratio(self, decision: str, horas: float, umbral: float,
                           min_evals: int, severidad: str):
        n_dec, total = await ldb.contar_predicciones_periodo(decision=decision, horas=horas)
        if total < min_evals:
            return
        ratio = n_dec / total
        if ratio > umbral:
            await self._rollback(
                tipo='REJECT_RATIO', severidad=severidad,
                razon=(f"reject_ratio_{int(horas)}h={ratio:.1%} > {umbral:.0%} "
                       f"({n_dec}/{total} evaluaciones)"),
                accion='ROLLBACK_A_SHADOW')

    async def _check_precision(self):
        """
        §4.4 regla 3: si la precisión de los rechazos (fracción de FIREs
        rechazados que efectivamente eran malas entradas) cae > delta vs la
        baseline de 7 días, el learner está rechazando entradas buenas.
        """
        prec_actual, n_act = await ldb.calcular_precision_rechazos(horas=24.0)
        prec_base, n_base = await ldb.calcular_precision_rechazos(horas=24.0 * 7)
        if prec_actual is None or prec_base is None or n_act < 10 or n_base < 20:
            return                                  # sin evidencia suficiente
        if prec_base <= 0:
            return
        delta = (prec_actual - prec_base) / prec_base
        if delta < getattr(CONFIG, 'learning_min_precision_delta', -0.10):
            await self._rollback(
                tipo='PRECISION_DROP', severidad='ALTO',
                razon=(f"precision_rechazos cayó {delta:.1%} "
                       f"(24h={prec_actual:.2f}, base7d={prec_base:.2f}, n={n_act})"),
                accion='ROLLBACK_A_SHADOW')

    async def _check_exit_captura(self):
        """§9 Fase 8: si la política dinámica captura poco MFE, se desactiva."""
        avg, n = await ldb.avg_capturado_mfe(dias=7.0)
        umbral = getattr(CONFIG, 'learning_exit_captura_pct_min', 30.0)
        if avg is None or n < 20:
            return
        if avg < umbral:
            await self._rollback(
                tipo='EXIT_CAPTURA', severidad='ALTO',
                razon=(f"capturado_pct_de_mfe medio={avg:.1f}% < {umbral}% "
                       f"en 7 días (n={n})"),
                accion='ROLLBACK_EXIT_OFF')

    # ── rollback ─────────────────────────────────────────────────────────
    async def _rollback(self, tipo: str, severidad: str, razon: str, accion: str):
        # Anti-spam: máximo 1 evento del mismo tipo por hora
        ahora = time.time()
        if ahora - self._ultimo_evento.get(tipo, 0) < 3600:
            return
        self._ultimo_evento[tipo] = ahora

        modo_antes = getattr(CONFIG, 'learning_apply_mode', 'OFF')
        exit_antes = getattr(CONFIG, 'learning_exit_mode', 'OFF')

        if accion == 'ROLLBACK_EXIT_OFF':
            CONFIG.learning_exit_mode = 'OFF'
            modo_despues = f"apply={modo_antes}, exit=OFF"
        else:
            CONFIG.learning_apply_mode = 'SHADOW'
            modo_despues = f"apply=SHADOW, exit={exit_antes}"

        print(f"  🛑 [GUARDRAIL-{severidad}] Rollback automático ({tipo}): {razon}")
        print(f"     modo: {modo_antes} → {modo_despues} | REACTIVACIÓN MANUAL REQUERIDA")

        await ldb.guardar_guardrail_event({
            'timestamp_ms': int(ahora * 1000),
            'tipo': tipo, 'severidad': severidad, 'razon': razon,
            'accion_tomada': accion,
            'modo_antes': modo_antes, 'modo_despues': modo_despues,
            'revisado_por_humano': 0,
        })

        await self._notificar(
            f"🛑 <b>LEARNING ENGINE — ROLLBACK AUTOMÁTICO</b>\n"
            f"Severidad: <b>{severidad}</b>\n"
            f"Regla: <code>{tipo}</code>\n"
            f"Razón: <code>{razon}</code>\n"
            f"Modo ahora: <b>{modo_despues}</b>\n"
            f"<i>Revisar guardrail_events y predictions_log antes de reactivar (manual).</i>")

    async def _notificar(self, texto: str):
        if not getattr(CONFIG, 'learning_alerta_telegram', True) or not self.notifier:
            return
        try:
            fn = getattr(self.notifier, 'enviar_telegram', None)
            if fn:
                await fn(texto)
        except Exception as e:
            print(f"  ⚠️ [GUARDRAIL] error notificando: {e}")
