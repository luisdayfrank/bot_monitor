import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List
from config import CONFIG
from database_v5 import (
    cargar_eventos_dia, cargar_muestras_post_evento,
    cargar_eventos_post_evento, cargar_velas_post_evento,
    utc_to_local, now_local, now_utc, local_to_utc
)


class AuditReporter:
    """
    Generador de reportes de auditoría diarios.

    Se ejecuta una vez al día (hora configurada en config.py),
    genera un JSON por moneda con todos los eventos y seguimiento,
    y lo envía como archivo adjunto por Telegram.

    FASE 5.2: Incluye análisis de MFM en reportes.
    """

    def __init__(self, notifier):
        self.notifier = notifier
        self._shutdown = asyncio.Event()

    async def run(self):
        """
        Loop principal: espera hasta la hora configurada del día siguiente,
        genera el reporte y lo envía.
        """
        while not self._shutdown.is_set():
            # Calcular cuánto falta para la próxima ejecución
            sleep_seconds = self._segundos_hasta_proximo_reporte()

            hora_local = CONFIG.auditoria_hora_reporte
            print(f"📋 [AUDITORÍA] Próximo reporte en {sleep_seconds/3600:.1f}h ({hora_local} {CONFIG.timezone})")

            await asyncio.sleep(sleep_seconds)

            if self._shutdown.is_set():
                break

            await self._generar_y_enviar_reporte()

    def _segundos_hasta_proximo_reporte(self) -> float:
        """Calcula los segundos hasta la próxima hora de reporte configurada."""
        ahora = now_local()

        # Parsear hora configurada
        try:
            hora_str, minuto_str = CONFIG.auditoria_hora_reporte.split(':')
            hora_target = ahora.replace(hour=int(hora_str), minute=int(minuto_str), second=0, microsecond=0)
        except Exception:
            # Fallback a 23:55
            hora_target = ahora.replace(hour=23, minute=55, second=0, microsecond=0)

        # Si ya pasó la hora hoy, programar para mañana
        if hora_target <= ahora:
            hora_target += timedelta(days=1)

        return (hora_target - ahora).total_seconds()

    async def _generar_y_enviar_reporte(self):
        """Genera el reporte diario y lo envía por Telegram."""
        fecha_ayer = (now_local() - timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"📋 [AUDITORÍA] Generando reporte para {fecha_ayer}...")

        try:
            # Cargar todos los eventos del día
            eventos_raw = await cargar_eventos_dia(fecha_ayer)

            if not eventos_raw:
                print(f"  ⚠️ Sin eventos para {fecha_ayer}")
                await self.notifier.enviar_telegram(
                    f"📋 <b>Auditoría {fecha_ayer}</b>\n"
                    f"Sin eventos registrados para este día."
                )
                return

            # Agrupar por moneda
            eventos_por_moneda: Dict[str, List[dict]] = {}
            for ev in eventos_raw:
                sym = ev['symbol']
                if sym not in eventos_por_moneda:
                    eventos_por_moneda[sym] = []
                eventos_por_moneda[sym].append(ev)

            # Generar JSON por moneda
            archivos_generados = []
            for symbol, eventos in eventos_por_moneda.items():
                json_data = await self._generar_json_moneda(symbol, fecha_ayer, eventos)

                # Guardar archivo temporal en misma carpeta que la DB
                db_dir = os.path.dirname(os.path.abspath(CONFIG.db_path))
                os.makedirs(db_dir, exist_ok=True)
                filename = f"auditoria_{symbol}_{fecha_ayer}.json"
                filepath = os.path.join(db_dir, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                archivos_generados.append((filepath, filename))

            # Enviar resumen por Telegram
            resumen = self._generar_resumen_texto(fecha_ayer, eventos_por_moneda)
            await self.notifier.enviar_telegram(resumen)

            # Enviar archivos JSON como adjuntos usando el método del notifier
            for filepath, filename in archivos_generados:
                await self.notifier.enviar_archivo_telegram(
                    filepath=filepath,
                    caption=f"📎 {filename}"
                )
                print(f"  ✅ Enviado: {filename}")

            print(f"  ✅ Reporte enviado: {len(archivos_generados)} monedas")

        except Exception as e:
            print(f"  ❌ Error generando reporte: {e}")
            await self.notifier.enviar_telegram(
                f"⚠️ <b>Error en auditoría {fecha_ayer}</b>\n{e}"
            )

    async def _generar_json_moneda(self, symbol: str, fecha: str, eventos: List[dict]) -> dict:
        """Genera el JSON completo para una moneda."""

        # Separar eventos por tipo
        eventos_fire = [e for e in eventos if e['tipo'] == 'FIRE']
        eventos_rechazados = [e for e in eventos if e['tipo'] == 'RECHAZADO']
        eventos_cambio_estado = [e for e in eventos if e['tipo'] == 'CAMBIO_ESTADO']
        eventos_cb = [e for e in eventos if e['tipo'] == 'CIRCUIT_BREAKER']

        # Construir lista de eventos procesados
        eventos_procesados = []
        for ev in eventos:
            evento_proc = {
                "timestamp_utc": ev['timestamp_utc'],
                "timestamp_local": ev['timestamp_local'],
                "tipo": ev['tipo'],
                "direccion": ev['direccion'],
                "precio": ev['precio'],
                "score": ev['score'],
                "estado_maquina": ev['estado_maquina']
            }

            # Parsear contexto si existe
            if ev.get('contexto_json'):
                try:
                    evento_proc["contexto"] = json.loads(ev['contexto_json'])
                except:
                    pass

            # Parsear grid_params si existe
            if ev.get('grid_params_json'):
                try:
                    evento_proc["grid_params"] = json.loads(ev['grid_params_json'])
                except:
                    pass

            # Parsear rechazos si existe
            if ev.get('rechazos_json'):
                try:
                    evento_proc["rechazos"] = json.loads(ev['rechazos_json'])
                except:
                    pass

            eventos_procesados.append(evento_proc)

        # Seguimiento post-disparo para cada FIRE
        seguimiento_post = {}
        for ev in eventos_fire:
            evento_id = ev['id']
            timestamp_disparo = ev['timestamp_utc']
            precio_entrada = ev['precio']
            direccion = ev['direccion']

            # Cargar datos de seguimiento
            muestras = await cargar_muestras_post_evento(evento_id)
            eventos_post = await cargar_eventos_post_evento(evento_id)
            velas_post = await cargar_velas_post_evento(evento_id)

            # Calcular estadísticas del recorrido
            estadisticas = self._calcular_estadisticas_recorrido(
                muestras, eventos_post, precio_entrada, direccion
            )

            seguimiento_post[timestamp_disparo] = {
                "direccion": direccion,
                "precio_entrada": precio_entrada,
                "estadisticas_recorrido": estadisticas,
                "eventos_significativos": [
                    {
                        "timestamp_utc": ep['timestamp_utc'],
                        "timestamp_local": ep['timestamp_local'],
                        "tipo_evento": ep['tipo_evento'],
                        "precio": ep['precio'],
                        "distancia_desde_entrada_pct": ep['distancia_desde_entrada_pct'],
                        "grid_rentable_aqui": bool(ep['grid_rentable_aqui']),
                        "nota": ep['nota']
                    }
                    for ep in eventos_post
                ],
                "muestras_5min": [
                    {
                        "timestamp_utc": m['timestamp_utc'],
                        "timestamp_local": m['timestamp_local'],
                        "precio": m['precio'],
                        "minutos_desde_disparo": m['minutos_desde_disparo']
                    }
                    for m in muestras
                ],
                "velas_15m_posterior": [
                    {
                        "timestamp_utc": v['timestamp_utc'],
                        "timestamp_local": v['timestamp_local'],
                        "open": v['open'],
                        "high": v['high'],
                        "low": v['low'],
                        "close": v['close'],
                        "volume": v['volume'],
                        "minutos_desde_disparo": v['minutos_desde_disparo']
                    }
                    for v in velas_post
                ]
            }

        # Generar preguntas de evaluación
        preguntas = self._generar_preguntas_evaluacion(eventos_fire, eventos_rechazados, seguimiento_post)

        # Generar recomendaciones basadas en patrones
        recomendaciones = self._generar_recomendaciones(eventos, seguimiento_post)

        return {
            "meta": {
                "symbol": symbol,
                "fecha": fecha,
                "modo": "auditoria_externa",
                "version_bot": "5.2",
                "timezone": CONFIG.timezone,
                "hora_generacion": now_local().isoformat(),
                "total_eventos": len(eventos)
            },
            "resumen_dia": {
                "disparos": len(eventos_fire),
                "rechazados": len(eventos_rechazados),
                "cambios_estado": len(eventos_cambio_estado),
                "circuit_breakers": len(eventos_cb),
                "estados_visitados": list(set(e['estado_maquina'] for e in eventos if e['estado_maquina'])),
                "direcciones": list(set(e['direccion'] for e in eventos if e['direccion']))
            },
            "eventos": eventos_procesados,
            "seguimiento_post_disparo": seguimiento_post,
            "evaluacion_manual": {
                "preguntas": preguntas,
                "recomendaciones_parametros": recomendaciones
            }
        }

    def _calcular_estadisticas_recorrido(self, muestras, eventos_post, precio_entrada, direccion):
        """Calcula estadísticas del recorrido post-disparo."""
        if not muestras:
            return {}

        precios = [m['precio'] for m in muestras if m['precio'] is not None]
        if not precios:
            return {}

        maximo = max(precios)
        minimo = min(precios)

        # Calcular distancias desde entrada
        if direccion == 'SHORT':
            max_drawdown = ((maximo - precio_entrada) / precio_entrada) * 100  # Peor caso para SHORT
            max_runup = ((precio_entrada - minimo) / precio_entrada) * 100      # Mejor caso para SHORT
        else:  # LONG
            max_drawdown = ((precio_entrada - minimo) / precio_entrada) * 100   # Peor caso para LONG
            max_runup = ((maximo - precio_entrada) / precio_entrada) * 100       # Mejor caso para LONG

        # Tiempo en rango (si hay eventos de entrada/salida)
        tiempo_en_rango = None
        evento_entrada = next((e for e in eventos_post if e['tipo_evento'] == 'PRIMERA_VEZ_EN_GRID'), None)
        evento_salida = next((e for e in eventos_post if e['tipo_evento'] == 'PRIMERA_VEZ_FUERA_RANGO'), None)

        if evento_entrada and evento_salida:
            tiempo_en_rango = evento_salida['minutos_desde_disparo'] - evento_entrada['minutos_desde_disparo']
        elif evento_entrada:
            tiempo_en_rango = muestras[-1]['minutos_desde_disparo'] - evento_entrada['minutos_desde_disparo']

        return {
            "maximo_alcanzado": round(maximo, 6),
            "minimo_alcanzado": round(minimo, 6),
            "maximo_drawdown_pct": round(max_drawdown, 3),
            "maximo_runup_pct": round(max_runup, 3),
            "precio_final": round(precios[-1], 6),
            "variacion_total_pct": round(((precios[-1] - precio_entrada) / precio_entrada) * 100, 3),
            "tiempo_en_rango_minutos": tiempo_en_rango,
            "total_muestras": len(muestras)
        }

    def _generar_preguntas_evaluacion(self, eventos_fire, eventos_rechazados, seguimiento_post):
        """Genera preguntas guía para la evaluación manual."""
        preguntas = []

        # Preguntas sobre FIREs
        for ev in eventos_fire:
            ts = ev['timestamp_utc']
            direccion = ev['direccion']
            precio = ev['precio']
            score = ev['score']

            seg = seguimiento_post.get(ts, {})
            stats = seg.get('estadisticas_recorrido', {})

            variacion = stats.get('variacion_total_pct', 0)
            max_dd = stats.get('maximo_drawdown_pct', 0)
            max_run = stats.get('maximo_runup_pct', 0)

            # Determinar si fue correcto según dirección
            if direccion == 'SHORT':
                correcto = variacion < 0
                direccion_str = "bajó"
            else:
                correcto = variacion > 0
                direccion_str = "subió"

            # FASE 5.2: Extraer MFM del contexto si existe
            mfm_info = ""
            try:
                ctx = json.loads(ev.get('contexto_json', '{}'))
                ctx_15m = ctx.get('contexto_15m', {})
                mfm = ctx_15m.get('mfm_sma5')
                if mfm is not None:
                    mfm_info = f" | MFM={mfm:.3f}"
            except:
                pass

            pregunta = {
                "disparo": f"{ts} {direccion} @ ${precio} (score: {score}){mfm_info}",
                "pregunta": f"¿El FIRE {direccion} fue correcto?",
                "datos": f"Precio {direccion_str} {variacion:.2f}% en {CONFIG.auditoria_horas_seguimiento}h. "
                        f"Máximo drawdown: {max_dd:.2f}%. Máximo runup: {max_run:.2f}%.",
                "analisis_sugerido": "correcto" if correcto else "incorrecto",
                "notas": "Revisar si el drawdown hubiera liquidado el grid"
            }
            preguntas.append(pregunta)

        # Preguntas sobre rechazos
        for ev in eventos_rechazados:
            rechazos = []
            if ev.get('rechazos_json'):
                try:
                    rechazos = json.loads(ev['rechazos_json'])
                except:
                    pass

            # FASE 5.2: Destacar rechazos por MFM contradictorio
            mfm_rechazo = any('MFM contradictorio' in r for r in rechazos)
            nota_extra = " | ⚠️ Rechazado por MFM contradictorio" if mfm_rechazo else ""

            pregunta = {
                "disparo": f"{ev['timestamp_utc']} {ev['direccion']} @ ${ev['precio']}{nota_extra}",
                "pregunta": "¿Este rechazo fue correcto?",
                "datos": f"Motivo: {'; '.join(rechazos) if rechazos else 'Sin motivo registrado'}",
                "analisis_sugerido": "Verificar si el precio se movió favorablemente después",
                "notas": "Considerar si el umbral fue demasiado estricto"
            }
            preguntas.append(pregunta)

        return preguntas

    def _generar_recomendaciones(self, eventos, seguimiento_post):
        """Genera recomendaciones de parámetros basadas en patrones detectados."""
        recomendaciones = []

        # Analizar rechazos frecuentes
        rechazos_frecuentes = {}
        mfm_contradictorios = 0
        for ev in eventos:
            if ev['tipo'] == 'RECHAZADO' and ev.get('rechazos_json'):
                try:
                    rechazos = json.loads(ev['rechazos_json'])
                    for r in rechazos:
                        # Extraer tipo de rechazo (ej: "ADX extremo: 46.2" -> "ADX extremo")
                        tipo_rechazo = r.split(':')[0] if ':' in r else r
                        rechazos_frecuentes[tipo_rechazo] = rechazos_frecuentes.get(tipo_rechazo, 0) + 1
                        if 'MFM contradictorio' in r:
                            mfm_contradictorios += 1
                except:
                    pass

        # Si hay muchos rechazos por ADX, sugerir ajuste
        adx_reject_val = getattr(CONFIG, 'adx_reject', 45.0)
        if rechazos_frecuentes.get('ADX extremo', 0) >= 3:
            recomendaciones.append({
                "parametro": "adx_reject",
                "actual": adx_reject_val,
                "sugerido": min(adx_reject_val + 5, 70.0),  # Cap en 70
                "motivo": f"{rechazos_frecuentes['ADX extremo']} rechazos por ADX extremo. "
                          "Considerar aumentar el umbral."
            })

        # Si hay muchos rechazos por ATR bajo
        if rechazos_frecuentes.get('ATR bajo', 0) >= 3:
            recomendaciones.append({
                "parametro": "atr_min_pct",
                "actual": CONFIG.atr_min_pct,
                "sugerido": max(0.05, CONFIG.atr_min_pct - 0.05),
                "motivo": f"{rechazos_frecuentes['ATR bajo']} rechazos por ATR bajo. "
                          "Considerar reducir el umbral mínimo."
            })

        # FASE 5.2: Si hay muchos rechazos por MFM contradictorio
        if mfm_contradictorios >= 3:
            recomendaciones.append({
                "parametro": "mfm_umbral_alineacion",
                "actual": CONFIG.mfm_umbral_alineacion,
                "sugerido": max(0.1, CONFIG.mfm_umbral_alineacion - 0.05),
                "motivo": f"{mfm_contradictorios} rechazos por MFM contradictorio. "
                          "Considerar relajar el umbral de alineación (más permisivo)."
            })

        # Analizar si los FIREs tuvieron drawdowns excesivos
        drawdowns_altos = 0
        for ts, seg in seguimiento_post.items():
            stats = seg.get('estadisticas_recorrido', {})
            dd = stats.get('maximo_drawdown_pct', 0)
            if dd > 3.0:  # Más de 3% de drawdown
                drawdowns_altos += 1

        if drawdowns_altos >= 2:
            recomendaciones.append({
                "parametro": "grid_rango_mult_max",
                "actual": CONFIG.grid_rango_mult_max,
                "sugerido": CONFIG.grid_rango_mult_max + 1.0,
                "motivo": f"{drawdowns_altos} disparos con drawdown >3%. "
                          "Considerar ampliar el rango del grid."
            })

        return recomendaciones

    def _generar_resumen_texto(self, fecha: str, eventos_por_moneda: Dict) -> str:
        """Genera el resumen en texto plano para Telegram."""
        total_eventos = sum(len(e) for e in eventos_por_moneda.values())
        total_fire = sum(len([e for e in evs if e['tipo'] == 'FIRE']) for evs in eventos_por_moneda.values())
        total_rechazos = sum(len([e for e in evs if e['tipo'] == 'RECHAZADO']) for evs in eventos_por_moneda.values())

        # FASE 5.2: Contar rechazos por MFM
        mfm_rechazos = 0
        for evs in eventos_por_moneda.values():
            for ev in evs:
                if ev['tipo'] == 'RECHAZADO' and ev.get('rechazos_json'):
                    try:
                        rechazos = json.loads(ev['rechazos_json'])
                        if any('MFM contradictorio' in r for r in rechazos):
                            mfm_rechazos += 1
                    except:
                        pass

        lineas = [
            f"📋 <b>AUDITORÍA DIARIA — {fecha}</b>",
            f"🕐 Generado: {now_local().strftime('%H:%M:%S')} ({CONFIG.timezone})",
            "",
            f"📊 Total eventos: {total_eventos}",
            f"🔥 Disparos: {total_fire}",
            f"❌ Rechazados: {total_rechazos}",
        ]

        if mfm_rechazos > 0:
            lineas.append(f"📉 Rechazos MFM: {mfm_rechazos}")

        lineas.extend([
            f"📁 Archivos adjuntos: {len(eventos_por_moneda)} monedas",
            "",
            "<b>Monedas con actividad:</b>"
        ])

        for symbol, eventos in sorted(eventos_por_moneda.items()):
            fires = len([e for e in eventos if e['tipo'] == 'FIRE'])
            rechazos = len([e for e in eventos if e['tipo'] == 'RECHAZADO'])
            lineas.append(f"• {symbol}: {fires} FIRE, {rechazos} RECHAZOS")

        lineas.append("")
        lineas.append("📎 Revisa los archivos JSON adjuntos para el análisis completo.")

        return "\n".join(lineas)

    def stop(self):
        self._shutdown.set()
