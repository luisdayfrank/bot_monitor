import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz
from config import CONFIG
from database_v5 import (
    guardar_evento_auditoria, guardar_muestra_post,
    guardar_evento_post, guardar_vela_post,
    guardar_disparo_activo, actualizar_disparo_activo, eliminar_disparo_activo,
    cargar_disparos_activos,
    # V5.7: Nuevas funciones para near-miss seguimiento
    guardar_near_miss_seguimiento, actualizar_near_miss_muestras,
    finalizar_near_miss_seguimiento, cargar_near_miss_seguimientos_activos,
    utc_to_local, now_utc, now_local
)


class AuditLogger:
    """
    Logger de auditoria V5.7 — Seguimiento virtual post near-miss con persistencia SQLite.

    FASE V5.7 CAMBIOS:
    —————————————————
    • NUEVO: Seguimientos virtuales persistentes en SQLite (tabla near_miss_seguimientos)
    • NUEVO: Muestras cada 5 minutos con JSON en columna muestras_json
    • NUEVO: Evento NEAR_MISS_SEGUIMIENTO_FIN con evaluacion completa
    • log_continuo() eliminado (reemplazado por granularidad nativa de near-miss)
    • Hardcoding eliminado: usa CONFIG.auditoria_near_miss_horas
    """

    def __init__(self):
        self._buffer_eventos: List[dict] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_interval = 60
        self._shutdown = asyncio.Event()
        self._disparos_lock = asyncio.Lock()
        self._disparos_activos: Dict[str, dict] = {}

        # V5.7: Seguimiento virtual de near-misses con persistencia
        self._near_miss_lock = asyncio.Lock()
        self._near_miss_activos: Dict[str, dict] = {}

    async def run(self):
        await self._recuperar_disparos_activos()
        # V5.7: Recuperar seguimientos de near-miss pendientes
        await self._recuperar_near_miss_activos()
        while not self._shutdown.is_set():
            await asyncio.sleep(self._flush_interval)
            await self._flush_buffer()

    async def stop(self):
        self._shutdown.set()
        await self._flush_buffer()
        await self._persistir_disparos_activos()
        # V5.7: Persistir seguimientos de near-miss activos antes de salir
        await self._persistir_near_miss_activos()

    async def _flush_buffer(self):
        async with self._buffer_lock:
            if not self._buffer_eventos:
                return
            eventos = self._buffer_eventos.copy()
            self._buffer_eventos = []

        for evento in eventos:
            try:
                evento_id = await guardar_evento_auditoria(**evento)
                if evento.get("tipo") == "FIRE" and evento_id:
                    symbol = evento["symbol"]
                    ts_str = evento["timestamp_utc"]
                    if isinstance(ts_str, str):
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_str

                    async with self._disparos_lock:
                        self._disparos_activos[symbol] = {
                            "evento_id": evento_id,
                            "timestamp_utc": ts,
                            "precio_entrada": evento["precio"],
                            "grid_params": json.loads(evento["grid_params_json"]) if evento.get("grid_params_json") else None,
                            "direccion": evento.get("direccion"),
                            "muestras_guardadas": 0,
                            "maximo_visto": evento["precio"],
                            "minimo_visto": evento["precio"],
                            "primera_vez_en_grid": None,
                            "primera_vez_fuera_rango": None
                        }
                    await self._persistir_disparo_individual(symbol)
            except Exception as e:
                print(f"  ⚠️ Error guardando evento auditoria: {e}")

    async def _persistir_disparos_activos(self):
        async with self._disparos_lock:
            for symbol, disparo in self._disparos_activos.items():
                await self._persistir_disparo_individual(symbol, disparo)

    async def _persistir_disparo_individual(self, symbol: str, disparo: dict = None):
        if disparo is None:
            disparo = self._disparos_activos.get(symbol)
        if not disparo:
            return
        try:
            await guardar_disparo_activo(
                evento_id=disparo["evento_id"],
                symbol=symbol,
                timestamp_utc=disparo["timestamp_utc"],
                precio_entrada=disparo["precio_entrada"],
                direccion=disparo["direccion"],
                grid_params_json=json.dumps(disparo["grid_params"]) if disparo.get("grid_params") else None,
                maximo_visto=disparo["maximo_visto"],
                minimo_visto=disparo["minimo_visto"],
                primera_vez_en_grid_json=json.dumps(disparo["primera_vez_en_grid"]) if disparo.get("primera_vez_en_grid") else None,
                primera_vez_fuera_rango_json=json.dumps(disparo["primera_vez_fuera_rango"]) if disparo.get("primera_vez_fuera_rango") else None,
                muestras_guardadas=disparo["muestras_guardadas"],
                horas_seguimiento=CONFIG.auditoria_horas_seguimiento
            )
        except Exception as e:
            print(f"  ⚠️ Error persistiendo disparo activo {symbol}: {e}")

    async def _recuperar_disparos_activos(self):
        try:
            rows = await cargar_disparos_activos()
            if not rows:
                print("  📋 No hay disparos activos persistentes para recuperar")
                return
            async with self._disparos_lock:
                for row in rows:
                    symbol = row["symbol"]
                    ts_str = row["timestamp_utc"]
                    if isinstance(ts_str, str):
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_str
                    grid_params = json.loads(row["grid_params_json"]) if row.get("grid_params_json") else None
                    primera_grid = json.loads(row["primera_vez_en_grid_json"]) if row.get("primera_vez_en_grid_json") else None
                    primera_fuera = json.loads(row["primera_vez_fuera_rango_json"]) if row.get("primera_vez_fuera_rango_json") else None
                    self._disparos_activos[symbol] = {
                        "evento_id": row["evento_id"],
                        "timestamp_utc": ts,
                        "precio_entrada": row["precio_entrada"],
                        "grid_params": grid_params,
                        "direccion": row["direccion"],
                        "muestras_guardadas": row["muestras_guardadas"] or 0,
                        "maximo_visto": row["maximo_visto"] or row["precio_entrada"],
                        "minimo_visto": row["minimo_visto"] or row["precio_entrada"],
                        "primera_vez_en_grid": primera_grid,
                        "primera_vez_fuera_rango": primera_fuera
                    }
            print(f"  ✅ {len(rows)} disparos activos recuperados de DB: {list(self._disparos_activos.keys())}")
        except Exception as e:
            print(f"  ⚠️ Error recuperando disparos activos: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # METODOS PUBLICOS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def log_cambio_estado(self, symbol: str, de: str, a: str, direccion: str = None,
                                 contexto_macro: dict = None, score_macro: int = None):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        contexto_json = json.dumps(contexto_macro) if contexto_macro else None
        evento = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc,
            "tipo": "CAMBIO_ESTADO",
            "direccion": direccion,
            "precio": contexto_macro.get("precio") if contexto_macro else None,
            "contexto_json": contexto_json,
            "grid_params_json": None,
            "score": score_macro,
            "rechazos_json": None,
            "estado_maquina": a
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # V5.7: LOG CONTINUO SIMPLIFICADO — Solo near-misses significativos
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_continuo(self, symbol: str, timestamp: int, estado_maquina: str,
                          score_macro: int, direccion: str = None,
                          contexto: dict = None):
        """
        Loguea estado continuo solo cuando hay actividad significativa.
        En V5.7 esto se usa principalmente para trigger de near-misses.
        """
        if not CONFIG.modo_auditoria:
            return

        contexto_json = json.dumps(contexto) if contexto else None

        evento = {
            "symbol": symbol,
            "timestamp_utc": datetime.fromtimestamp(timestamp / 1000, tz=pytz.UTC) if timestamp > 1e12 else now_utc(),
            "tipo": "CONTINUO",
            "direccion": direccion,
            "precio": contexto.get("precio") if contexto else None,
            "contexto_json": contexto_json,
            "grid_params_json": None,
            "score": score_macro,
            "rechazos_json": None,
            "estado_maquina": estado_maquina
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # V5.7: NEAR-MISS — Con seguimiento virtual persistente
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_near_miss(self, symbol: str, tipo: str, score_macro: int, umbral: int,
                            direccion: str = None, contexto: dict = None, detalle: dict = None):
        """
        Detecta, guarda e inicia seguimiento virtual de near-misses.
        El seguimiento se persiste en SQLite para sobrevivir reinicios.
        """
        if not CONFIG.modo_auditoria:
            return

        contexto_completo = contexto.copy() if contexto else {}
        if detalle:
            contexto_completo["near_miss_detalle"] = detalle

        evento = {
            "symbol": symbol,
            "timestamp_utc": now_utc(),
            "tipo": "NEAR_MISS",
            "direccion": direccion,
            "precio": contexto.get("precio") if contexto else None,
            "contexto_json": json.dumps(contexto_completo),
            "grid_params_json": None,
            "score": score_macro,
            "rechazos_json": json.dumps([tipo]) if tipo else None,
            "estado_maquina": contexto.get("estado_maquina") if contexto else None
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

        # V5.7: Iniciar seguimiento virtual persistente para near-misses de alto score
        if tipo == "SCORE_PASA_OTRO_FILTRO_NO" and score_macro >= umbral:
            await self.iniciar_seguimiento_near_miss(
                symbol=symbol,
                score=score_macro,
                umbral=umbral,
                direccion=direccion,
                precio=contexto.get("precio", 0) if contexto else 0,
                filtros_rechazo=detalle.get("rechazos", []) if detalle else [],
                contexto=contexto
            )

    # ═══════════════════════════════════════════════════════════════════════════════
    # V5.7: METRICAS DIARIAS — Agregadas al cierre del dia
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_metricas_diarias(self, symbol: str, fecha: str, metricas: dict):
        """
        Guarda metricas agregadas del dia para analisis post-dia.
        Llamado por audit_reporter al generar el reporte.
        """
        if not CONFIG.modo_auditoria:
            return

        evento = {
            "symbol": symbol,
            "timestamp_utc": now_utc(),
            "tipo": "METRICAS_DIA",
            "direccion": None,
            "precio": None,
            "contexto_json": json.dumps({
                "fecha": fecha,
                "metricas": metricas
            }),
            "grid_params_json": None,
            "score": None,
            "rechazos_json": None,
            "estado_maquina": "RESUMEN"
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    async def log_fire(self, symbol: str, direccion: str, precio: float,
                       contexto_1m: dict, contexto_15m: dict, contexto_4h: dict,
                       grid_params: dict, score_disparo: int):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        contexto_completo = {
            "timestamp_utc": timestamp_utc.isoformat(),
            "timestamp_local": utc_to_local(timestamp_utc).isoformat(),
            "contexto_1m": contexto_1m,
            "contexto_15m": contexto_15m,
            "contexto_4h": contexto_4h
        }
        evento = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc,
            "tipo": "FIRE",
            "direccion": direccion,
            "precio": precio,
            "contexto_json": json.dumps(contexto_completo),
            "grid_params_json": json.dumps(grid_params),
            "score": score_disparo,
            "rechazos_json": None,
            "estado_maquina": "FIRE"
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    async def log_rechazado(self, symbol: str, direccion: str, precio: float,
                            contexto_1m: dict = None, contexto_macro: dict = None,
                            rechazos: list = None, score_macro: int = None):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        contexto_completo = {
            "timestamp_utc": timestamp_utc.isoformat(),
            "timestamp_local": utc_to_local(timestamp_utc).isoformat(),
            "contexto_1m": contexto_1m,
            "contexto_macro": contexto_macro
        }
        evento = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc,
            "tipo": "RECHAZADO",
            "direccion": direccion,
            "precio": precio,
            "contexto_json": json.dumps(contexto_completo) if contexto_1m or contexto_macro else None,
            "grid_params_json": None,
            "score": score_macro,
            "rechazos_json": json.dumps(rechazos) if rechazos else None,
            "estado_maquina": "RECHAZADO"
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    async def log_circuit_breaker(self, symbol: str, direccion: str = None,
                                  rechazos: list = None):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        evento = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc,
            "tipo": "CIRCUIT_BREAKER",
            "direccion": direccion,
            "precio": None,
            "contexto_json": None,
            "grid_params_json": None,
            "score": None,
            "rechazos_json": json.dumps(rechazos) if rechazos else None,
            "estado_maquina": "CIRCUIT_BREAKER"
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # SEGUIMIENTO POST-DISPARO (sin cambios)
    # ═══════════════════════════════════════════════════════════════════════════════

    async def trackear_precio_post_disparo(self, symbol: str, precio: float, timestamp_utc: datetime):
        if not CONFIG.modo_auditoria:
            return
        async with self._disparos_lock:
            if symbol not in self._disparos_activos:
                return
            if timestamp_utc.tzinfo is None:
                timestamp_utc = timestamp_utc.replace(tzinfo=pytz.UTC)
            disparo = self._disparos_activos[symbol]
            evento_id = disparo["evento_id"]
            precio_entrada = disparo["precio_entrada"]
            direccion = disparo["direccion"]
            grid_params = disparo["grid_params"]
            minutos_desde = int((timestamp_utc - disparo["timestamp_utc"]).total_seconds() / 60)
            if precio > disparo["maximo_visto"]:
                disparo["maximo_visto"] = precio
            if precio < disparo["minimo_visto"]:
                disparo["minimo_visto"] = precio
            if grid_params:
                upper = grid_params.get("upper_limit")
                lower = grid_params.get("lower_limit")
                if disparo["primera_vez_en_grid"] is None and lower and upper:
                    if lower <= precio <= upper:
                        disparo["primera_vez_en_grid"] = {
                            "timestamp_utc": timestamp_utc.isoformat(),
                            "precio": precio,
                            "minutos_desde": minutos_desde
                        }
                        if direccion == "SHORT":
                            grid_rentable = precio <= precio_entrada
                        elif direccion == "LONG":
                            grid_rentable = precio >= precio_entrada
                        else:
                            grid_rentable = True
                        await guardar_evento_post(
                            evento_id, symbol, timestamp_utc, "PRIMERA_VEZ_EN_GRID",
                            precio,
                            distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                            grid_rentable_aqui=grid_rentable,
                            nota=f"Precio entra en rango del grid [{lower}, {upper}] | Rentable para {direccion}: {grid_rentable}"
                        )
                        await actualizar_disparo_activo(
                            evento_id,
                            primera_vez_en_grid_json=json.dumps(disparo["primera_vez_en_grid"])
                        )
                if disparo["primera_vez_fuera_rango"] is None and lower and upper:
                    if precio > upper or precio < lower:
                        direccion_fuera = "UPPER" if precio > upper else "LOWER"
                        disparo["primera_vez_fuera_rango"] = {
                            "timestamp_utc": timestamp_utc.isoformat(),
                            "precio": precio,
                            "minutos_desde": minutos_desde,
                            "direccion": direccion_fuera
                        }
                        if direccion == "SHORT" and precio < lower:
                            nota_extra = " | RUPTURA FAVORABLE SHORT"
                        elif direccion == "LONG" and precio > upper:
                            nota_extra = " | RUPTURA FAVORABLE LONG"
                        else:
                            nota_extra = " | RUPTURA DESFAVORABLE"
                        await guardar_evento_post(
                            evento_id, symbol, timestamp_utc, "PRIMERA_VEZ_FUERA_RANGO",
                            precio,
                            distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                            grid_rentable_aqui=False,
                            nota=f"Precio rompe {direccion_fuera} del grid [{lower}, {upper}]" + nota_extra
                        )
                        await actualizar_disparo_activo(
                            evento_id,
                            primera_vez_fuera_rango_json=json.dumps(disparo["primera_vez_fuera_rango"])
                        )
            horas_seguimiento = CONFIG.auditoria_horas_seguimiento
            intervalo = CONFIG.auditoria_muestras_intervalo_min
            if minutos_desde <= horas_seguimiento * 60:
                intervalo_actual = minutos_desde // intervalo
                if intervalo_actual > disparo["muestras_guardadas"]:
                    await guardar_muestra_post(evento_id, symbol, timestamp_utc, precio, minutos_desde)
                    disparo["muestras_guardadas"] = intervalo_actual
                    await actualizar_disparo_activo(
                        evento_id,
                        maximo_visto=disparo["maximo_visto"],
                        minimo_visto=disparo["minimo_visto"],
                        muestras_guardadas=disparo["muestras_guardadas"]
                    )
            if minutos_desde > horas_seguimiento * 60:
                await self._guardar_eventos_finales(symbol, disparo, timestamp_utc)
                await eliminar_disparo_activo(evento_id)
                del self._disparos_activos[symbol]

    async def _guardar_eventos_finales(self, symbol: str, disparo: dict, timestamp_utc: datetime):
        evento_id = disparo["evento_id"]
        precio_entrada = disparo["precio_entrada"]
        direccion = disparo["direccion"]
        if disparo["maximo_visto"] > precio_entrada:
            if direccion == "SHORT":
                grid_rentable = False
                nota = "Maximo precio alcanzado durante seguimiento | DRAWDOWN para SHORT"
            else:
                grid_rentable = True
                nota = "Maximo precio alcanzado durante seguimiento | RUNUP para LONG"
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, "MAXIMO_ABSOLUTO",
                disparo["maximo_visto"],
                distancia_desde_entrada_pct=round((disparo["maximo_visto"] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=grid_rentable,
                nota=nota
            )
        if disparo["minimo_visto"] < precio_entrada:
            if direccion == "SHORT":
                grid_rentable = True
                nota = "Minimo precio alcanzado durante seguimiento | RUNUP para SHORT"
            else:
                grid_rentable = False
                nota = "Minimo precio alcanzado durante seguimiento | DRAWDOWN para LONG"
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, "MINIMO_ABSOLUTO",
                disparo["minimo_visto"],
                distancia_desde_entrada_pct=round((disparo["minimo_visto"] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=grid_rentable,
                nota=nota
            )
        await eliminar_disparo_activo(evento_id)

    async def cerrar_seguimiento_todos(self):
        async with self._disparos_lock:
            disparos_copia = list(self._disparos_activos.items())
        for symbol, disparo in disparos_copia:
            await self._guardar_eventos_finales(symbol, disparo, now_utc())
            await eliminar_disparo_activo(disparo["evento_id"])
        async with self._disparos_lock:
            self._disparos_activos.clear()


    # ═══════════════════════════════════════════════════════════════════════════════
    # V5.7: SEGUIMIENTO VIRTUAL DE NEAR-MISSES CON PERSISTENCIA SQLITE
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _recuperar_near_miss_activos(self):
        """Recupera seguimientos de near-miss pendientes de la base de datos."""
        try:
            rows = await cargar_near_miss_seguimientos_activos()
            if not rows:
                print("  📋 No hay seguimientos near-miss activos para recuperar")
                return

            async with self._near_miss_lock:
                for row in rows:
                    symbol = row["symbol"]
                    ts_inicio = row["timestamp_inicio"]
                    # Convertir timestamp a datetime si es necesario
                    if isinstance(ts_inicio, (int, float)):
                        ts = datetime.fromtimestamp(ts_inicio, tz=pytz.UTC)
                    elif isinstance(ts_inicio, str):
                        ts = datetime.fromisoformat(ts_inicio)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_inicio

                    # Recuperar muestras existentes
                    muestras = []
                    if row.get("muestras_json"):
                        try:
                            muestras = json.loads(row["muestras_json"])
                        except:
                            muestras = []

                    # Recuperar filtros de rechazo
                    filtros = []
                    if row.get("filtros_rechazo"):
                        try:
                            filtros = json.loads(row["filtros_rechazo"])
                        except:
                            filtros = []

                    self._near_miss_activos[symbol] = {
                        "seguimiento_id": row["id"],
                        "timestamp_utc": ts,
                        "symbol": symbol,
                        "score": row["score"],
                        "umbral": row["umbral"],
                        "direccion": row["direccion_nm"],
                        "precio_inicial": row["precio_inicio"],
                        "precio_maximo": row.get("precio_max") or row["precio_inicio"],
                        "precio_minimo": row.get("precio_min") or row["precio_inicio"],
                        "muestras": muestras,
                        "muestras_guardadas": len(muestras),
                        "filtros_rechazo": filtros,
                        "contexto_json": row.get("contexto_json"),
                    }
            print(f"  ✅ {len(rows)} seguimientos near-miss recuperados de DB: {list(self._near_miss_activos.keys())}")
        except Exception as e:
            print(f"  ⚠️ Error recuperando near-miss activos: {e}")

    async def _persistir_near_miss_activos(self):
        """Persiste todos los seguimientos near-miss activos en la base de datos."""
        async with self._near_miss_lock:
            for symbol, near_miss in self._near_miss_activos.items():
                try:
                    await actualizar_near_miss_muestras(
                        seguimiento_id=near_miss["seguimiento_id"],
                        muestras_json=near_miss["muestras"],
                        precio_max=near_miss["precio_maximo"],
                        precio_min=near_miss["precio_minimo"],
                        precio_fin=near_miss.get("precio_fin")
                    )
                except Exception as e:
                    print(f"  ⚠️ Error persistiendo near-miss {symbol}: {e}")

    async def iniciar_seguimiento_near_miss(self, symbol: str, score: int, umbral: int,
                                             direccion: str, precio: float,
                                             filtros_rechazo: list = None,
                                             contexto: dict = None):
        """
        Inicia seguimiento virtual persistente de un near-miss de alto score.
        Se guarda en SQLite para sobrevivir reinicios del bot.
        """
        if not CONFIG.modo_auditoria:
            return

        async with self._near_miss_lock:
            # Evitar duplicados: si ya hay seguimiento para este simbolo, reemplazar solo si es mas reciente
            ts_actual = now_utc()
            if symbol in self._near_miss_activos:
                existente = self._near_miss_activos[symbol]
                minutos_desde_inicio = (ts_actual - existente["timestamp_utc"]).total_seconds() / 60
                horas_seguimiento = CONFIG.auditoria_near_miss_horas
                if minutos_desde_inicio < horas_seguimiento * 60:
                    # Ya hay seguimiento activo, no duplicar
                    return

            # Guardar en SQLite primero
            seguimiento_id = await guardar_near_miss_seguimiento(
                symbol=symbol,
                timestamp_inicio=ts_actual.timestamp(),
                precio_inicio=precio,
                direccion_nm=direccion,
                score=score,
                umbral=umbral,
                filtros_rechazo=filtros_rechazo
            )

            if not seguimiento_id:
                print(f"  ⚠️ {symbol} No se pudo persistir near-miss seguimiento en DB")
                return

            self._near_miss_activos[symbol] = {
                "seguimiento_id": seguimiento_id,
                "timestamp_utc": ts_actual,
                "symbol": symbol,
                "score": score,
                "umbral": umbral,
                "direccion": direccion,
                "precio_inicial": precio,
                "precio_maximo": precio,
                "precio_minimo": precio,
                "muestras": [],  # Lista de {timestamp, precio, minutos_desde}
                "muestras_guardadas": 0,
                "filtros_rechazo": filtros_rechazo or [],
                "contexto_json": json.dumps(contexto) if contexto else None,
            }

            # Guardar evento de inicio de seguimiento virtual
            contexto_seguimiento = {
                "seguimiento_id": seguimiento_id,
                "score": score,
                "umbral": umbral,
                "direccion": direccion,
                "precio_inicial": precio,
                "timestamp_local": utc_to_local(ts_actual).isoformat(),
                "contexto_original": contexto,
            }

            evento = {
                "symbol": symbol,
                "timestamp_utc": ts_actual,
                "tipo": "NEAR_MISS_SEGUIMIENTO_INICIO",
                "direccion": direccion,
                "precio": precio,
                "contexto_json": json.dumps(contexto_seguimiento),
                "grid_params_json": None,
                "score": score,
                "rechazos_json": json.dumps([f"Umbral: {umbral}"]),
                "estado_maquina": "NEAR_MISS_TRACKING"
            }

            async with self._buffer_lock:
                self._buffer_eventos.append(evento)

            print(f"  🔍 {symbol} Seguimiento virtual NEAR-MISS iniciado | Score: {score} | Umbral: {umbral} | Dir: {direccion} | Precio: {precio}")

    async def trackear_precio_near_miss(self, symbol: str, precio: float, timestamp_utc: datetime):
        """
        Trackea precio post-near-miss igual que se hace post-FIRE.
        Se llama periodicamente (cada vela 15m o cada muestra de precio).
        """
        if not CONFIG.modo_auditoria:
            return

        async with self._near_miss_lock:
            if symbol not in self._near_miss_activos:
                return

            if timestamp_utc.tzinfo is None:
                timestamp_utc = timestamp_utc.replace(tzinfo=pytz.UTC)

            near_miss = self._near_miss_activos[symbol]
            minutos_desde = int((timestamp_utc - near_miss["timestamp_utc"]).total_seconds() / 60)
            horas_seguimiento = CONFIG.auditoria_near_miss_horas
            intervalo = CONFIG.near_miss_tracking_intervalo_min

            # Actualizar maximos/minimos
            if precio > near_miss["precio_maximo"]:
                near_miss["precio_maximo"] = precio
            if precio < near_miss["precio_minimo"]:
                near_miss["precio_minimo"] = precio

            # Guardar muestra periodica
            if minutos_desde <= horas_seguimiento * 60:
                intervalo_actual = minutos_desde // intervalo
                if intervalo_actual > near_miss["muestras_guardadas"]:
                    near_miss["muestras"].append({
                        "timestamp_utc": timestamp_utc.isoformat(),
                        "precio": precio,
                        "minutos_desde": minutos_desde,
                        "distancia_pct": round((precio - near_miss["precio_inicial"]) / near_miss["precio_inicial"] * 100, 4)
                    })
                    near_miss["muestras_guardadas"] = intervalo_actual

                    # Persistir muestras en DB
                    try:
                        await actualizar_near_miss_muestras(
                            seguimiento_id=near_miss["seguimiento_id"],
                            muestras_json=near_miss["muestras"],
                            precio_max=near_miss["precio_maximo"],
                            precio_min=near_miss["precio_minimo"]
                        )
                    except Exception as e:
                        print(f"  ⚠️ Error persistiendo muestras near-miss {symbol}: {e}")

                    # Guardar evento de muestra
                    muestra_evento = {
                        "symbol": symbol,
                        "timestamp_utc": timestamp_utc,
                        "tipo": "NEAR_MISS_MUESTRA",
                        "direccion": near_miss["direccion"],
                        "precio": precio,
                        "contexto_json": json.dumps({
                            "seguimiento_id": near_miss["seguimiento_id"],
                            "minutos_desde": minutos_desde,
                            "precio_inicial": near_miss["precio_inicial"],
                            "distancia_pct": round((precio - near_miss["precio_inicial"]) / near_miss["precio_inicial"] * 100, 4),
                            "precio_maximo": near_miss["precio_maximo"],
                            "precio_minimo": near_miss["precio_minimo"],
                        }),
                        "grid_params_json": None,
                        "score": near_miss["score"],
                        "rechazos_json": None,
                        "estado_maquina": "NEAR_MISS_TRACKING"
                    }
                    async with self._buffer_lock:
                        self._buffer_eventos.append(muestra_evento)

            # Si se cumplio el tiempo de seguimiento, guardar resultados finales
            if minutos_desde > horas_seguimiento * 60:
                await self._guardar_resultados_near_miss(symbol, near_miss, timestamp_utc)
                del self._near_miss_activos[symbol]

    async def _guardar_resultados_near_miss(self, symbol: str, near_miss: dict, timestamp_utc: datetime):
        """Guarda resultados finales del seguimiento virtual de un near-miss."""
        precio_inicial = near_miss["precio_inicial"]
        precio_max = near_miss["precio_maximo"]
        precio_min = near_miss["precio_minimo"]
        direccion = near_miss["direccion"]

        # Calcular metricas de rendimiento virtual
        if direccion == "SHORT":
            # Para SHORT: el precio bajando es favorable
            mejor_movimiento_pct = round((precio_inicial - precio_min) / precio_inicial * 100, 4)
            peor_movimiento_pct = round((precio_max - precio_inicial) / precio_inicial * 100, 4)
            precio_final_virtual = precio_min  # Asumimos entrada optima
            rentable = precio_min < precio_inicial  # Bajo = hubiera sido rentable
        elif direccion == "LONG":
            # Para LONG: el precio subiendo es favorable
            mejor_movimiento_pct = round((precio_max - precio_inicial) / precio_inicial * 100, 4)
            peor_movimiento_pct = round((precio_inicial - precio_min) / precio_inicial * 100, 4)
            precio_final_virtual = precio_max  # Asumimos entrada optima
            rentable = precio_max > precio_inicial  # Subio = hubiera sido rentable
        else:
            mejor_movimiento_pct = round(abs(precio_max - precio_inicial) / precio_inicial * 100, 4)
            peor_movimiento_pct = round(abs(precio_min - precio_inicial) / precio_inicial * 100, 4)
            precio_final_virtual = precio_max if precio_max > precio_inicial else precio_min
            rentable = None

        # Determinar direccion real del movimiento
        precio_fin = precio_max if abs(precio_max - precio_inicial) >= abs(precio_min - precio_inicial) else precio_min
        if precio_fin > precio_inicial * 1.005:
            direccion_real = "ALCISTA"
        elif precio_fin < precio_inicial * 0.995:
            direccion_real = "BAJISTA"
        else:
            direccion_real = "LATERAL"

        # ¿El bot acerto al rechazar?
        if direccion == "NEUTRAL":
            acerto = (abs(mejor_movimiento_pct) < 2.0)  # Si se quedo lateral, acerto
        else:
            acerto = (direccion == direccion_real)

        resultados = {
            "seguimiento_id": near_miss["seguimiento_id"],
            "symbol": symbol,
            "direccion": direccion,
            "score": near_miss["score"],
            "umbral": near_miss["umbral"],
            "precio_inicial": precio_inicial,
            "precio_maximo": precio_max,
            "precio_minimo": precio_min,
            "mejor_movimiento_pct": mejor_movimiento_pct,
            "peor_movimiento_pct": peor_movimiento_pct,
            "rentable": rentable,
            "direccion_real": direccion_real,
            "acerto_bot": acerto,
            "duracion_minutos": CONFIG.auditoria_near_miss_horas * 60,
            "total_muestras": len(near_miss["muestras"]),
            "muestras": near_miss["muestras"],
            "conclusion": "HUBIERA SIDO RENTABLE" if rentable else "NO HUBIERA SIDO RENTABLE" if rentable is not None else "INDETERMINADO",
            "timestamp_final_utc": timestamp_utc.isoformat(),
            "timestamp_final_local": utc_to_local(timestamp_utc).isoformat(),
        }

        # Finalizar en base de datos
        try:
            await finalizar_near_miss_seguimiento(
                seguimiento_id=near_miss["seguimiento_id"],
                timestamp_fin=timestamp_utc.timestamp(),
                precio_fin=precio_fin,
                movimiento_pct=mejor_movimiento_pct if not acerto else -peor_movimiento_pct,
                direccion_real=direccion_real,
                acerto_bot=acerto,
                hubiera_sido_rentable=rentable if rentable is not None else False,
                notas=resultados["conclusion"]
            )
        except Exception as e:
            print(f"  ⚠️ Error finalizando near-miss en DB {symbol}: {e}")

        # Guardar evento final de seguimiento virtual
        evento_final = {
            "symbol": symbol,
            "timestamp_utc": timestamp_utc,
            "tipo": "NEAR_MISS_SEGUIMIENTO_FIN",
            "direccion": direccion,
            "precio": precio_final_virtual,
            "contexto_json": json.dumps(resultados),
            "grid_params_json": None,
            "score": near_miss["score"],
            "rechazos_json": json.dumps([resultados["conclusion"]]),
            "estado_maquina": "NEAR_MISS_TRACKING_FIN"
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento_final)

        rentable_icon = "✅" if rentable else "❌" if rentable is not None else "⚪"
        acerto_icon = "✅" if acerto else "❌"
        print(f"  📊 {symbol} Seguimiento virtual NEAR-MISS finalizado | {rentable_icon} {resultados['conclusion']} | Mejor mov: {mejor_movimiento_pct:+.4f}% | Peor mov: {peor_movimiento_pct:+.4f}% | Bot acerto: {acerto_icon}")

    async def cerrar_seguimiento_near_miss_todos(self):
        """Cierra todos los seguimientos virtuales de near-misses pendientes."""
        async with self._near_miss_lock:
            near_miss_copia = list(self._near_miss_activos.items())
        for symbol, near_miss in near_miss_copia:
            await self._guardar_resultados_near_miss(symbol, near_miss, now_utc())
        async with self._near_miss_lock:
            self._near_miss_activos.clear()
