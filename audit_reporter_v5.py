import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List
from config import CONFIG
from database_v5 import (
    cargar_eventos_dia, cargar_muestras_post_evento,
    cargar_eventos_post_evento, cargar_velas_post_evento,
    # V5.7: Funciones para near-miss seguimientos
    cargar_near_miss_seguimientos_dia,
    utc_to_local, now_local, now_utc, local_to_utc
)


class AuditReporter:
    """
    Generador de reportes de auditoria diarios — V5.7 Fases 1-5.

    FASE 5.7 CAMBIOS:
    ————————————————
    • Manejo de eventos CONTINUO (cada vela 15m)
    • Manejo de eventos NEAR_MISS (casi-disparos) con seguimiento virtual
    • Manejo de eventos METRICAS_DIA (agregados)
    • FASE 5: Manejo de eventos NEUTRAL_GRID (grid neutral)
    • FASE 6: Commitment score eliminado completamente — NO se reporta
    • Analisis de evolucion del score durante el dia
    • Deteccion de "horas muertas" vs "horas activas"
    • Near-miss seguimientos persistentes (SQLite)
    """

    def __init__(self, notifier):
        self.notifier = notifier
        self._shutdown = asyncio.Event()

    async def run(self):
        while not self._shutdown.is_set():
            sleep_seconds = self._segundos_hasta_proximo_reporte()
            hora_local = CONFIG.auditoria_hora_reporte
            print(f"[AUDITORIA] Proximo reporte en {sleep_seconds/3600:.1f}h ({hora_local} {CONFIG.timezone})")
            await asyncio.sleep(sleep_seconds)
            if self._shutdown.is_set():
                break
            await self._generar_y_enviar_reporte()

    def _segundos_hasta_proximo_reporte(self) -> float:
        ahora = now_local()
        try:
            hora_str, minuto_str = CONFIG.auditoria_hora_reporte.split(':')
            hora_target = ahora.replace(hour=int(hora_str), minute=int(minuto_str), second=0, microsecond=0)
        except Exception:
            hora_target = ahora.replace(hour=23, minute=55, second=0, microsecond=0)
        if hora_target <= ahora:
            hora_target += timedelta(days=1)
        return (hora_target - ahora).total_seconds()

    async def _generar_y_enviar_reporte(self):
        fecha_ayer = (now_local() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"[AUDITORIA] Generando reporte para {fecha_ayer}...")

        try:
            eventos_raw = await cargar_eventos_dia(fecha_ayer)
            # V5.7: Cargar near-miss seguimientos tambien
            near_miss_seguimientos = await cargar_near_miss_seguimientos_dia(fecha_ayer)

            if not eventos_raw and not near_miss_seguimientos:
                print(f"  [AVISO] Sin eventos para {fecha_ayer}")
                await self.notifier.enviar_telegram(
                    f"[AUDITORIA] <b>Auditoria {fecha_ayer}</b>\n"
                    f"Sin eventos registrados para este dia."
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
                # V5.7: Filtrar near-miss seguimientos por moneda
                nm_seguimientos_moneda = [nm for nm in near_miss_seguimientos if nm.get('symbol') == symbol]
                json_data = await self._generar_json_moneda(symbol, fecha_ayer, eventos, nm_seguimientos_moneda)

                db_dir = os.path.dirname(os.path.abspath(CONFIG.db_path))
                os.makedirs(db_dir, exist_ok=True)
                filename = f"auditoria_{symbol}_{fecha_ayer}.json"
                filepath = os.path.join(db_dir, filename)

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)

                archivos_generados.append((filepath, filename))

            # Enviar resumen por Telegram
            resumen = self._generar_resumen_texto(fecha_ayer, eventos_por_moneda, near_miss_seguimientos)
            await self.notifier.enviar_telegram(resumen)

            # Enviar archivos JSON + limpieza post-envio
            archivos_borrados = 0
            archivos_conservados = 0

            for filepath, filename in archivos_generados:
                try:
                    enviado = await self.notifier.enviar_archivo_telegram(
                        filepath=filepath,
                        caption=f"[ADJUNTO] {filename}"
                    )

                    if enviado:
                        # Envio confirmado -> borrar archivo
                        try:
                            os.remove(filepath)
                            archivos_borrados += 1
                            print(f"  [OK] Enviado + [BORRADO] Borrado: {filename}")
                        except Exception as e_borrar:
                            print(f"  [OK] Enviado pero [AVISO] NO borrado: {filename} | Error: {e_borrar}")
                            archivos_conservados += 1
                    else:
                        # Envio fallo -> conservar archivo para reintento
                        archivos_conservados += 1
                        print(f"  [FALLO] NO enviado (conservado): {filename}")

                except Exception as e_envio:
                    archivos_conservados += 1
                    print(f"  [ERROR] Error enviando {filename}: {e_envio} | Archivo conservado en {filepath}")

            # Resumen de limpieza
            if archivos_borrados > 0 or archivos_conservados > 0:
                print(f"  [LIMPIEZA] {archivos_borrados} borrados, {archivos_conservados} conservados")

            # Alerta si quedan archivos sin enviar (posible problema con Telegram)
            if archivos_conservados > 0:
                await self.notifier.enviar_telegram(
                    f"[AVISO] <b>Auditoria {fecha_ayer}</b>\n"
                    f"{archivos_conservados} archivo(s) JSON no pudieron enviarse y permanecen en el VPS.\n"
                    f"Ubicacion: <code>{db_dir}</code>\n"
                    f"Revisa conectividad con Telegram o envia manualmente."
                )

            print(f"  [OK] Reporte enviado: {len(archivos_generados)} monedas")

        except Exception as e:
            print(f"  [ERROR] Error generando reporte: {e}")
            await self.notifier.enviar_telegram(
                f"[ERROR] <b>Error en auditoria {fecha_ayer}</b>\n{e}"
            )

    async def _generar_json_moneda(self, symbol: str, fecha: str, eventos: List[dict],
                                    near_miss_seguimientos: List[dict] = None) -> dict:
        """Genera el JSON completo para una moneda con auditoria granular."""

        near_miss_seguimientos = near_miss_seguimientos or []

        # Separar eventos por tipo
        eventos_fire = [e for e in eventos if e['tipo'] == 'FIRE']
        eventos_rechazados = [e for e in eventos if e['tipo'] == 'RECHAZADO']
        eventos_cambio_estado = [e for e in eventos if e['tipo'] == 'CAMBIO_ESTADO']
        eventos_cb = [e for e in eventos if e['tipo'] == 'CIRCUIT_BREAKER']
        eventos_continuo = [e for e in eventos if e['tipo'] == 'CONTINUO']
        eventos_near_miss = [e for e in eventos if e['tipo'] == 'NEAR_MISS']
        eventos_metricas = [e for e in eventos if e['tipo'] == 'METRICAS_DIA']
        # FASE 5: Eventos de grid neutral
        eventos_neutral_grid = [e for e in eventos if e['tipo'] in ('NEUTRAL_GRID', 'NEUTRAL_GRID_ABORT')]
        eventos_nm_seguimiento = [e for e in eventos if e['tipo'] in ('NEAR_MISS_SEGUIMIENTO_INICIO', 'NEAR_MISS_SEGUIMIENTO_FIN')]

        # Procesar eventos principales (sin CONTINUO)
        eventos_procesados = []
        for ev in eventos:
            if ev['tipo'] == 'CONTINUO':
                continue  # Los continuos van en seccion separada

            evento_proc = {
                "timestamp_utc": ev['timestamp_utc'],
                "timestamp_local": ev['timestamp_local'],
                "tipo": ev['tipo'],
                "direccion": ev['direccion'],
                "precio": ev['precio'],
                "score": ev['score'],
                "estado_maquina": ev['estado_maquina']
            }

            if ev.get('contexto_json'):
                try:
                    ctx = json.loads(ev['contexto_json'])
                    evento_proc["contexto"] = ctx
                    if isinstance(ctx, dict):
                        umbral_raw = ctx.get('umbral_aplicado')
                        if isinstance(umbral_raw, str) and umbral_raw.startswith('BLOQUEADO'):
                            evento_proc["umbral_estado"] = umbral_raw
                        umbral_real = ctx.get('umbral_real')
                        if umbral_real is not None:
                            evento_proc["umbral_numerico"] = umbral_real
                except:
                    pass

            if ev.get('grid_params_json'):
                try:
                    evento_proc["grid_params"] = json.loads(ev['grid_params_json'])
                except:
                    pass

            if ev.get('rechazos_json'):
                try:
                    evento_proc["rechazos"] = json.loads(ev['rechazos_json'])
                except:
                    pass

            eventos_procesados.append(evento_proc)

        # FASE 5.5: Procesar logs continuos para analisis de evolucion
        evolucion_score = self._analizar_evolucion_score(eventos_continuo)

        # FASE 5.5: Procesar near-misses
        near_misses_procesados = []
        for ev in eventos_near_miss:
            nm_proc = {
                "timestamp_utc": ev['timestamp_utc'],
                "timestamp_local": ev['timestamp_local'],
                "tipo": ev['tipo'],
                "score": ev['score'],
                "direccion": ev['direccion']
            }
            if ev.get('contexto_json'):
                try:
                    ctx = json.loads(ev['contexto_json'])
                    nm_proc["contexto"] = ctx
                    if 'near_miss_detalle' in ctx:
                        nm_proc["detalle"] = ctx['near_miss_detalle']
                except:
                    pass
            near_misses_procesados.append(nm_proc)

        # V5.7: Procesar near-miss seguimientos persistentes
        nm_seguimientos_procesados = []
        for nm in near_miss_seguimientos:
            nm_proc = {
                "id": nm.get('id'),
                "timestamp_inicio": nm.get('timestamp_inicio'),
                "timestamp_fin": nm.get('timestamp_fin'),
                "precio_inicio": nm.get('precio_inicio'),
                "precio_fin": nm.get('precio_fin'),
                "direccion_nm": nm.get('direccion_nm'),
                "score": nm.get('score'),
                "umbral": nm.get('umbral'),
                "precio_max": nm.get('precio_max'),
                "precio_min": nm.get('precio_min'),
                "movimiento_pct": nm.get('movimiento_pct'),
                "direccion_real": nm.get('direccion_real'),
                "acerto_bot": bool(nm.get('acerto_bot')),
                "hubiera_sido_rentable": bool(nm.get('hubiera_sido_rentable')),
                "notas": nm.get('notas')
            }
            # Parsear filtros de rechazo si existen
            if nm.get('filtros_rechazo'):
                try:
                    nm_proc["filtros_rechazo"] = json.loads(nm['filtros_rechazo'])
                except:
                    nm_proc["filtros_rechazo"] = nm['filtros_rechazo']
            nm_seguimientos_procesados.append(nm_proc)

        # FASE 5: Procesar eventos de grid neutral
        neutral_grid_procesados = []
        for ev in eventos_neutral_grid:
            ng_proc = {
                "timestamp_utc": ev['timestamp_utc'],
                "timestamp_local": ev['timestamp_local'],
                "tipo": ev['tipo'],
                "direccion": ev['direccion'],
                "precio": ev['precio'],
                "score": ev['score'],
                "estado_maquina": ev['estado_maquina']
            }
            if ev.get('contexto_json'):
                try:
                    ng_proc["contexto"] = json.loads(ev['contexto_json'])
                except:
                    pass
            neutral_grid_procesados.append(ng_proc)

        # Seguimiento post-disparo para cada FIRE
        seguimiento_post = {}
        for ev in eventos_fire:
            evento_id = ev['id']
            timestamp_disparo = ev['timestamp_utc']
            precio_entrada = ev['precio']
            direccion = ev['direccion']

            muestras = await cargar_muestras_post_evento(evento_id)
            eventos_post = await cargar_eventos_post_evento(evento_id)
            velas_post = await cargar_velas_post_evento(evento_id)

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

        # Generar preguntas y recomendaciones
        preguntas = self._generar_preguntas_evaluacion(eventos_fire, eventos_rechazados, seguimiento_post)
        recomendaciones = self._generar_recomendaciones(eventos, seguimiento_post, eventos_near_miss, eventos_neutral_grid)

        resultado = {
            "meta": {
                "symbol": symbol,
                "fecha": fecha,
                "modo": "auditoria_externa",
                "version_bot": "5.7",
                "fases_implementadas": ["1", "2", "3", "4", "5", "6"],
                "timezone": CONFIG.timezone,
                "hora_generacion": now_local().isoformat(),
                "total_eventos": len(eventos),
                "total_continuos": len(eventos_continuo),
                "total_near_misses": len(eventos_near_miss),
                "total_nm_seguimientos": len(nm_seguimientos_procesados),
                "total_neutral_grid": len(neutral_grid_procesados),
                # FASE 6: commitment_score ELIMINADO - no existe en el reporte
                "commitment_score": "ELIMINADO (Fase 6)"
            },
            "resumen_dia": {
                "disparos": len(eventos_fire),
                "rechazados": len(eventos_rechazados),
                "cambios_estado": len(eventos_cambio_estado),
                "circuit_breakers": len(eventos_cb),
                "logs_continuos": len(eventos_continuo),
                "near_misses": len(eventos_near_miss),
                "near_miss_seguimientos": len(nm_seguimientos_procesados),
                "neutral_grid_eventos": len(neutral_grid_procesados),
                "estados_visitados": list(set(e['estado_maquina'] for e in eventos if e['estado_maquina'])),
                "direcciones": list(set(e['direccion'] for e in eventos if e['direccion']))
            },
            "evolucion_score": evolucion_score,
            "eventos": eventos_procesados,
            "near_misses": near_misses_procesados,
            "near_miss_seguimientos": nm_seguimientos_procesados,
            "neutral_grid_eventos": neutral_grid_procesados,
            "seguimiento_post_disparo": seguimiento_post,
            "evaluacion_manual": {
                "preguntas": preguntas,
                "recomendaciones_parametros": recomendaciones
            }
        }

        return resultado

    def _analizar_evolucion_score(self, eventos_continuo: List[dict]) -> dict:
        """
        Analiza la evolucion del score durante el dia a partir de los logs continuos.
        """
        if not eventos_continuo:
            return {
                "mensaje": "No hay logs continuos para este dia",
                "score_max": None,
                "score_min": None,
                "score_promedio": None,
                "velas_monitoreadas": 0
            }

        scores = []
        scores_por_hora = {}

        for ev in eventos_continuo:
            score = ev.get('score')
            if score is not None:
                scores.append(score)
                # Extraer hora del timestamp
                try:
                    ts = ev['timestamp_utc']
                    if isinstance(ts, str):
                        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    else:
                        dt = ts
                    hora = dt.hour
                    if hora not in scores_por_hora:
                        scores_por_hora[hora] = []
                    scores_por_hora[hora].append(score)
                except:
                    pass

        if not scores:
            return {
                "mensaje": "Logs continuos sin scores validos",
                "score_max": None,
                "score_min": None,
                "score_promedio": None,
                "velas_monitoreadas": len(eventos_continuo)
            }

        # Calcular promedio por hora
        promedio_por_hora = {}
        for hora, vals in sorted(scores_por_hora.items()):
            promedio_por_hora[f"{hora:02d}:00"] = round(sum(vals) / len(vals), 1)

        return {
            "score_max": max(scores),
            "score_min": min(scores),
            "score_promedio": round(sum(scores) / len(scores), 1),
            "velas_monitoreadas": len(eventos_continuo),
            "promedio_por_hora": promedio_por_hora,
            "horas_mas_activas": self._detectar_horas_activas(scores_por_hora),
            "horas_menos_activas": self._detectar_horas_menos_activas(scores_por_hora)
        }

    def _detectar_horas_activas(self, scores_por_hora: dict) -> list:
        """Detecta horas con scores mas altos (mas cerca de disparar)."""
        if not scores_por_hora:
            return []
        promedios = [(hora, sum(vals)/len(vals)) for hora, vals in scores_por_hora.items()]
        promedios.sort(key=lambda x: x[1], reverse=True)
        return [f"{h:02d}:00 (score avg: {s:.1f})" for h, s in promedios[:3]]

    def _detectar_horas_menos_activas(self, scores_por_hora: dict) -> list:
        """Detecta horas con scores mas bajos (mercado plano)."""
        if not scores_por_hora:
            return []
        promedios = [(hora, sum(vals)/len(vals)) for hora, vals in scores_por_hora.items()]
        promedios.sort(key=lambda x: x[1])
        return [f"{h:02d}:00 (score avg: {s:.1f})" for h, s in promedios[:3]]

    def _calcular_estadisticas_recorrido(self, muestras, eventos_post, precio_entrada, direccion):
        if not muestras:
            return {}
        precios = [m['precio'] for m in muestras if m['precio'] is not None]
        if not precios:
            return {}
        maximo = max(precios)
        minimo = min(precios)
        if direccion == 'SHORT':
            max_drawdown = ((maximo - precio_entrada) / precio_entrada) * 100
            max_runup = ((precio_entrada - minimo) / precio_entrada) * 100
        else:
            max_drawdown = ((precio_entrada - minimo) / precio_entrada) * 100
            max_runup = ((maximo - precio_entrada) / precio_entrada) * 100
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
        preguntas = []
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
            if direccion == 'SHORT':
                correcto = variacion < 0
                direccion_str = "bajo"
            else:
                correcto = variacion > 0
                direccion_str = "subio"
            umbral_info = ""
            try:
                ctx = json.loads(ev.get('contexto_json', '{}'))
                ctx_15m = ctx.get('contexto_15m', {})
                umbral = ctx_15m.get('umbral_aplicado')
                if umbral is not None:
                    umbral_info = f" | Umbral: {umbral}"
                mfm = ctx_15m.get('mfm_sma5')
                if mfm is not None:
                    umbral_info += f" | MFM={mfm:.3f}"
            except:
                pass
            pregunta = {
                "disparo": f"{ts} {direccion} @ ${precio} (score: {score}){umbral_info}",
                "pregunta": f"El FIRE {direccion} fue correcto?",
                "datos": f"Precio {direccion_str} {variacion:.2f}% en {CONFIG.auditoria_horas_seguimiento}h. "
                        f"Maximo drawdown: {max_dd:.2f}%. Maximo runup: {max_run:.2f}%.",
                "analisis_sugerido": "correcto" if correcto else "incorrecto",
                "notas": "Revisar si el drawdown hubiera liquidado el grid"
            }
            preguntas.append(pregunta)

        for ev in eventos_rechazados:
            rechazos = []
            if ev.get('rechazos_json'):
                try:
                    rechazos = json.loads(ev['rechazos_json'])
                except:
                    pass
            umbral_bloqueado = any('ADX extremo' in r or 'ADX sin tendencia' in r for r in rechazos)
            mfm_rechazo = any('MFM contradictorio' in r for r in rechazos)
            nota_extra = ""
            if umbral_bloqueado:
                nota_extra = " | [BLOQUEO] Umbral bloqueado por ADX"
            if mfm_rechazo:
                nota_extra += " | [MFM] Rechazado por MFM contradictorio"
            pregunta = {
                "disparo": f"{ev['timestamp_utc']} {ev['direccion']} @ ${ev['precio']}{nota_extra}",
                "pregunta": "Este rechazo fue correcto?",
                "datos": f"Motivo: {'; '.join(rechazos) if rechazos else 'Sin motivo registrado'}",
                "analisis_sugerido": "Verificar si el precio se movio favorablemente despues",
                "notas": "Considerar si el umbral fue demasiado estricto"
            }
            preguntas.append(pregunta)

        return preguntas

    # FASE 6: commitment_score eliminado — no se reporta en recomendaciones
    def _generar_recomendaciones(self, eventos, seguimiento_post, eventos_near_miss,
                                  eventos_neutral_grid=None):
        recomendaciones = []
        rechazos_frecuentes = {}
        mfm_contradictorios = 0
        umbrales_bloqueados = 0
        near_miss_count = len(eventos_near_miss)
        neutral_grid_count = len(eventos_neutral_grid or [])

        for ev in eventos:
            if ev['tipo'] == 'RECHAZADO' and ev.get('rechazos_json'):
                try:
                    rechazos = json.loads(ev['rechazos_json'])
                    for r in rechazos:
                        tipo_rechazo = r.split(':')[0] if ':' in r else r
                        rechazos_frecuentes[tipo_rechazo] = rechazos_frecuentes.get(tipo_rechazo, 0) + 1
                        if 'MFM contradictorio' in r:
                            mfm_contradictorios += 1
                        if 'ADX extremo' in r or 'ADX sin tendencia' in r:
                            umbrales_bloqueados += 1
                except:
                    pass

        adx_reject_val = getattr(CONFIG, 'adx_reject', 45.0)
        if rechazos_frecuentes.get('ADX extremo', 0) >= 3:
            recomendaciones.append({
                "parametro": "adx_reject",
                "actual": adx_reject_val,
                "sugerido": min(adx_reject_val + 5, 70.0),
                "motivo": f"{rechazos_frecuentes['ADX extremo']} rechazos por ADX extremo."
            })

        if umbrales_bloqueados >= 5:
            recomendaciones.append({
                "parametro": "umbral_adx",
                "actual": "45 (extremo) / 20 (bajo)",
                "sugerido": "Revisar si el mercado esta en rango lateral prolongado",
                "motivo": f"{umbrales_bloqueados} eventos con umbral bloqueado por ADX."
            })

        if rechazos_frecuentes.get('ATR bajo', 0) >= 3:
            recomendaciones.append({
                "parametro": "atr_min_pct",
                "actual": CONFIG.atr_min_pct,
                "sugerido": max(0.05, CONFIG.atr_min_pct - 0.05),
                "motivo": f"{rechazos_frecuentes['ATR bajo']} rechazos por ATR bajo."
            })

        if mfm_contradictorios >= 3:
            recomendaciones.append({
                "parametro": "mfm_umbral_alineacion",
                "actual": CONFIG.mfm_umbral_alineacion,
                "sugerido": max(0.1, CONFIG.mfm_umbral_alineacion - 0.05),
                "motivo": f"{mfm_contradictorios} rechazos por MFM contradictorio."
            })

        # FASE 5.5: Recomendacion basada en near-misses
        if near_miss_count >= 5:
            recomendaciones.append({
                "parametro": "umbrales_generales",
                "actual": "65-75",
                "sugerido": "60-70 (mas permisivo)",
                "motivo": f"{near_miss_count} near-misses detectados. El bot estuvo cerca de disparar multiples veces. "
                          "Considerar bajar umbrales para capturar mas oportunidades."
            })

        # FASE 5: Recomendacion basada en grid neutral
        if neutral_grid_count >= 3:
            recomendaciones.append({
                "parametro": "grid_neutral_timeout_min",
                "actual": CONFIG.grid_neutral_timeout_min,
                "sugerido": max(15, CONFIG.grid_neutral_timeout_min - 5),
                "motivo": f"{neutral_grid_count} eventos de grid neutral. El bot paso mucho tiempo en grid neutral. "
                          "Considerar reducir timeout para ser mas reactivo."
            })

        drawdowns_altos = 0
        for ts, seg in seguimiento_post.items():
            stats = seg.get('estadisticas_recorrido', {})
            dd = stats.get('maximo_drawdown_pct', 0)
            if dd > 3.0:
                drawdowns_altos += 1

        if drawdowns_altos >= 2:
            recomendaciones.append({
                "parametro": "grid_rango_mult_max",
                "actual": CONFIG.grid_rango_mult_max,
                "sugerido": CONFIG.grid_rango_mult_max + 1.0,
                "motivo": f"{drawdowns_altos} disparos con drawdown >3%."
            })

        # FASE 6: Nota sobre eliminacion de commitment score
        recomendaciones.append({
            "parametro": "commitment_score",
            "actual": "ELIMINADO",
            "sugerido": "N/A",
            "motivo": "Fase 6: commitment_score eliminado. Transicion ARMED es directa sin commitment."
        })

        return recomendaciones

    def _generar_resumen_texto(self, fecha: str, eventos_por_moneda: Dict,
                                near_miss_seguimientos: List[dict] = None) -> str:
        near_miss_seguimientos = near_miss_seguimientos or []
        total_eventos = sum(len(e) for e in eventos_por_moneda.values())
        total_fire = sum(len([e for e in evs if e['tipo'] == 'FIRE']) for evs in eventos_por_moneda.values())
        total_rechazos = sum(len([e for e in evs if e['tipo'] == 'RECHAZADO']) for evs in eventos_por_moneda.values())
        total_continuos = sum(len([e for e in evs if e['tipo'] == 'CONTINUO']) for evs in eventos_por_moneda.values())
        total_near_miss = sum(len([e for e in evs if e['tipo'] == 'NEAR_MISS']) for evs in eventos_por_moneda.values())
        # FASE 5: Contar eventos de grid neutral
        total_neutral_grid = sum(len([e for e in evs if e['tipo'] in ('NEUTRAL_GRID', 'NEUTRAL_GRID_ABORT')]) for evs in eventos_por_moneda.values())
        # V5.7: Near-miss seguimientos finalizados
        total_nm_seguimientos_finalizados = len([nm for nm in near_miss_seguimientos if nm.get('timestamp_fin') is not None])
        total_nm_acertados = len([nm for nm in near_miss_seguimientos if nm.get('acerto_bot') == 1])

        mfm_rechazos = 0
        umbrales_bloqueados = 0
        for evs in eventos_por_moneda.values():
            for ev in evs:
                if ev['tipo'] == 'RECHAZADO' and ev.get('rechazos_json'):
                    try:
                        rechazos = json.loads(ev['rechazos_json'])
                        if any('MFM contradictorio' in r for r in rechazos):
                            mfm_rechazos += 1
                        if any('ADX extremo' in r or 'ADX sin tendencia' in r for r in rechazos):
                            umbrales_bloqueados += 1
                    except:
                        pass

        lineas = [
            f"[AUDITORIA] <b>AUDITORIA DIARIA V5.7 — {fecha}</b>",
            f"[RELOJ] Generado: {now_local().strftime('%H:%M:%S')} ({CONFIG.timezone})",
            "",
            f"[DATOS] Total eventos: {total_eventos}",
            f"[FIRE] Disparos: {total_fire}",
            f"[RECHAZO] Rechazados: {total_rechazos}",
            f"[LOGS] Logs continuos: {total_continuos} (cada vela 15m)",
        ]

        if total_near_miss > 0:
            lineas.append(f"[NEAR-MISS] Near-misses: {total_near_miss} (casi disparos)")

        # V5.7: Near-miss seguimientos
        if near_miss_seguimientos:
            lineas.append(f"[TRACKING] Near-miss seguimientos finalizados: {total_nm_seguimientos_finalizados}/{len(near_miss_seguimientos)}")
            if total_nm_seguimientos_finalizados > 0:
                pct_acierto = (total_nm_acertados / total_nm_seguimientos_finalizados) * 100
                lineas.append(f"[TRACKING] Bot acerto: {total_nm_acertados}/{total_nm_seguimientos_finalizados} ({pct_acierto:.0f}%)")

        # FASE 5: Grid neutral info
        if total_neutral_grid > 0:
            lineas.append(f"[N-GRID] Grid Neutral: {total_neutral_grid} eventos")

        if mfm_rechazos > 0:
            lineas.append(f"[MFM] Rechazos MFM: {mfm_rechazos}")

        if umbrales_bloqueados > 0:
            lineas.append(f"[BLOQUEO] Umbrales bloqueados (ADX): {umbrales_bloqueados}")

        # FASE 6: Nota sobre commitment score
        lineas.append(f"[FASE6] Commitment Score: ELIMINADO (transicion ARMED directa)")

        lineas.extend([
            f"[ARCHIVOS] Archivos adjuntos: {len(eventos_por_moneda)} monedas",
            "",
            "<b>Monedas con actividad:</b>"
        ])

        for symbol, eventos in sorted(eventos_por_moneda.items()):
            fires = len([e for e in eventos if e['tipo'] == 'FIRE'])
            rechazos = len([e for e in eventos if e['tipo'] == 'RECHAZADO'])
            continuos = len([e for e in eventos if e['tipo'] == 'CONTINUO'])
            near = len([e for e in eventos if e['tipo'] == 'NEAR_MISS'])
            ngrid = len([e for e in eventos if e['tipo'] in ('NEUTRAL_GRID', 'NEUTRAL_GRID_ABORT')])
            extras = []
            if continuos > 0:
                extras.append(f"[LOGS] {continuos} logs")
            if near > 0:
                extras.append(f"[NM] {near} near-miss")
            if ngrid > 0:
                extras.append(f"[NG] {ngrid} neutral-grid")
            extra_str = f" | {' | '.join(extras)}" if extras else ""
            lineas.append(f"• {symbol}: {fires} FIRE, {rechazos} RECHAZOS{extra_str}")

        lineas.append("")
        lineas.append("[NOTA] Revisa los archivos JSON adjuntos para el analisis completo.")
        lineas.append("[NOTA] Los logs continuos muestran la evolucion del score cada vela 15m.")
        lineas.append("[NOTA] Los near-misses indican oportunidades que el bot casi capturo.")
        if near_miss_seguimientos:
            lineas.append("[NOTA] Near-miss seguimientos: tracking virtual persistente con evaluacion 'bot acerto?'.")
        lineas.append("[NOTA] FASE 5: Grid Neutral reformulado con 5 condiciones de aborto automatico.")
        lineas.append("[NOTA] FASE 6: Commitment score eliminado — transicion ARMED es directa.")

        return "\n".join(lineas)

    def stop(self):
        self._shutdown.set()
