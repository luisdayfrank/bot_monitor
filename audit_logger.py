import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional
import pytz
from config import CONFIG
from database_v5 import (
    guardar_evento_auditoria, guardar_muestra_post,
    guardar_evento_post, guardar_vela_post,
    guardar_disparo_activo, actualizar_disparo_activo, eliminar_disparo_activo,
    cargar_disparos_activos,
    utc_to_local, now_utc, now_local
)


class AuditLogger:
    """
    Logger de auditoría V5.5 Granular — Auditoría completa del comportamiento.

    FASE 5.5 CAMBIOS:
    ─────────────────
    • NUEVO: log_continuo() — guarda estado CADA vela 15m
    • NUEVO: log_near_miss() — detecta "casi-disparos"
    • NUEVO: log_metricas_diarias() — métricas agregadas al cierre
    • log_snapshot() eliminado (reemplazado por log_continuo más granular)
    """

    def __init__(self):
        self._buffer_eventos: List[dict] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_interval = 60
        self._shutdown = asyncio.Event()
        self._disparos_lock = asyncio.Lock()
        self._disparos_activos: Dict[str, dict] = {}

    async def run(self):
        await self._recuperar_disparos_activos()
        while not self._shutdown.is_set():
            await asyncio.sleep(self._flush_interval)
            await self._flush_buffer()

    async def stop(self):
        self._shutdown.set()
        await self._flush_buffer()
        await self._persistir_disparos_activos()

    async def _flush_buffer(self):
        async with self._buffer_lock:
            if not self._buffer_eventos:
                return
            eventos = self._buffer_eventos.copy()
            self._buffer_eventos = []

        for evento in eventos:
            try:
                evento_id = await guardar_evento_auditoria(**evento)
                if evento.get('tipo') == 'FIRE' and evento_id:
                    symbol = evento['symbol']
                    ts_str = evento['timestamp_utc']
                    if isinstance(ts_str, str):
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_str

                    async with self._disparos_lock:
                        self._disparos_activos[symbol] = {
                            'evento_id': evento_id,
                            'timestamp_utc': ts,
                            'precio_entrada': evento['precio'],
                            'grid_params': json.loads(evento['grid_params_json']) if evento.get('grid_params_json') else None,
                            'direccion': evento.get('direccion'),
                            'muestras_guardadas': 0,
                            'maximo_visto': evento['precio'],
                            'minimo_visto': evento['precio'],
                            'primera_vez_en_grid': None,
                            'primera_vez_fuera_rango': None
                        }
                    await self._persistir_disparo_individual(symbol)
            except Exception as e:
                print(f"  ⚠️ Error guardando evento auditoría: {e}")

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
                evento_id=disparo['evento_id'],
                symbol=symbol,
                timestamp_utc=disparo['timestamp_utc'],
                precio_entrada=disparo['precio_entrada'],
                direccion=disparo['direccion'],
                grid_params_json=json.dumps(disparo['grid_params']) if disparo.get('grid_params') else None,
                maximo_visto=disparo['maximo_visto'],
                minimo_visto=disparo['minimo_visto'],
                primera_vez_en_grid_json=json.dumps(disparo['primera_vez_en_grid']) if disparo.get('primera_vez_en_grid') else None,
                primera_vez_fuera_rango_json=json.dumps(disparo['primera_vez_fuera_rango']) if disparo.get('primera_vez_fuera_rango') else None,
                muestras_guardadas=disparo['muestras_guardadas'],
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
                    symbol = row['symbol']
                    ts_str = row['timestamp_utc']
                    if isinstance(ts_str, str):
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_str
                    grid_params = json.loads(row['grid_params_json']) if row.get('grid_params_json') else None
                    primera_grid = json.loads(row['primera_vez_en_grid_json']) if row.get('primera_vez_en_grid_json') else None
                    primera_fuera = json.loads(row['primera_vez_fuera_rango_json']) if row.get('primera_vez_fuera_rango_json') else None
                    self._disparos_activos[symbol] = {
                        'evento_id': row['evento_id'],
                        'timestamp_utc': ts,
                        'precio_entrada': row['precio_entrada'],
                        'grid_params': grid_params,
                        'direccion': row['direccion'],
                        'muestras_guardadas': row['muestras_guardadas'] or 0,
                        'maximo_visto': row['maximo_visto'] or row['precio_entrada'],
                        'minimo_visto': row['minimo_visto'] or row['precio_entrada'],
                        'primera_vez_en_grid': primera_grid,
                        'primera_vez_fuera_rango': primera_fuera
                    }
            print(f"  ✅ {len(rows)} disparos activos recuperados de DB: {list(self._disparos_activos.keys())}")
        except Exception as e:
            print(f"  ⚠️ Error recuperando disparos activos: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # MÉTODOS PÚBLICOS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def log_cambio_estado(self, symbol: str, de: str, a: str, direccion: str = None,
                                 contexto_macro: dict = None, score_macro: int = None):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        contexto_json = json.dumps(contexto_macro) if contexto_macro else None
        evento = {
            'symbol': symbol,
            'timestamp_utc': timestamp_utc,
            'tipo': 'CAMBIO_ESTADO',
            'direccion': direccion,
            'precio': contexto_macro.get('precio') if contexto_macro else None,
            'contexto_json': contexto_json,
            'grid_params_json': None,
            'score': score_macro,
            'rechazos_json': None,
            'estado_maquina': a
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.5: LOG CONTINUO — CADA vela 15m, sin importar si cambió
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_continuo(self, symbol: str, timestamp: int, estado_maquina: str,
                          score_macro: int, commitment_score: int, direccion: str = None,
                          contexto: dict = None):
        """
        Guarda el estado actual CADA vela 15m.
        Esto permite ver la evolución completa del score durante el día.
        """
        if not CONFIG.modo_auditoria:
            return

        contexto_json = json.dumps(contexto) if contexto else None

        evento = {
            'symbol': symbol,
            'timestamp_utc': datetime.fromtimestamp(timestamp / 1000, tz=pytz.UTC) if timestamp > 1e12 else now_utc(),
            'tipo': 'CONTINUO',
            'direccion': direccion,
            'precio': contexto.get('precio') if contexto else None,
            'contexto_json': contexto_json,
            'grid_params_json': None,
            'score': score_macro,
            'rechazos_json': None,
            'estado_maquina': estado_maquina
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.5: NEAR-MISS — Cuando el bot "casi" dispara
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_near_miss(self, symbol: str, tipo: str, score_macro: int, umbral: int,
                            direccion: str = None, contexto: dict = None, detalle: dict = None):
        """
        Detecta y guarda momentos donde el bot estuvo cerca de disparar.
        Tipos: SCORE_PASA_OTRO_FILTRO_NO, CERCA_DEL_UMBRAL, MUY_CERCA_DEL_UMBRAL, COMMITMENT_CASI
        """
        if not CONFIG.modo_auditoria:
            return

        contexto_completo = contexto.copy() if contexto else {}
        if detalle:
            contexto_completo['near_miss_detalle'] = detalle

        evento = {
            'symbol': symbol,
            'timestamp_utc': now_utc(),
            'tipo': 'NEAR_MISS',
            'direccion': direccion,
            'precio': contexto.get('precio') if contexto else None,
            'contexto_json': json.dumps(contexto_completo),
            'grid_params_json': None,
            'score': score_macro,
            'rechazos_json': json.dumps([tipo]) if tipo else None,
            'estado_maquina': contexto.get('estado_maquina') if contexto else None
        }

        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5.5: MÉTRICAS DIARIAS — Agregadas al cierre del día
    # ═══════════════════════════════════════════════════════════════════════════════
    async def log_metricas_diarias(self, symbol: str, fecha: str, metricas: dict):
        """
        Guarda métricas agregadas del día para análisis post-día.
        Llamado por audit_reporter al generar el reporte.
        """
        if not CONFIG.modo_auditoria:
            return

        evento = {
            'symbol': symbol,
            'timestamp_utc': now_utc(),
            'tipo': 'METRICAS_DIA',
            'direccion': None,
            'precio': None,
            'contexto_json': json.dumps({
                'fecha': fecha,
                'metricas': metricas
            }),
            'grid_params_json': None,
            'score': None,
            'rechazos_json': None,
            'estado_maquina': 'RESUMEN'
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
            'timestamp_utc': timestamp_utc.isoformat(),
            'timestamp_local': utc_to_local(timestamp_utc).isoformat(),
            'contexto_1m': contexto_1m,
            'contexto_15m': contexto_15m,
            'contexto_4h': contexto_4h
        }
        evento = {
            'symbol': symbol,
            'timestamp_utc': timestamp_utc,
            'tipo': 'FIRE',
            'direccion': direccion,
            'precio': precio,
            'contexto_json': json.dumps(contexto_completo),
            'grid_params_json': json.dumps(grid_params),
            'score': score_disparo,
            'rechazos_json': None,
            'estado_maquina': 'FIRE'
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
            'timestamp_utc': timestamp_utc.isoformat(),
            'timestamp_local': utc_to_local(timestamp_utc).isoformat(),
            'contexto_1m': contexto_1m,
            'contexto_macro': contexto_macro
        }
        evento = {
            'symbol': symbol,
            'timestamp_utc': timestamp_utc,
            'tipo': 'RECHAZADO',
            'direccion': direccion,
            'precio': precio,
            'contexto_json': json.dumps(contexto_completo) if contexto_1m or contexto_macro else None,
            'grid_params_json': None,
            'score': score_macro,
            'rechazos_json': json.dumps(rechazos) if rechazos else None,
            'estado_maquina': 'RECHAZADO'
        }
        async with self._buffer_lock:
            self._buffer_eventos.append(evento)

    async def log_circuit_breaker(self, symbol: str, direccion: str = None,
                                  rechazos: list = None):
        if not CONFIG.modo_auditoria:
            return
        timestamp_utc = now_utc()
        evento = {
            'symbol': symbol,
            'timestamp_utc': timestamp_utc,
            'tipo': 'CIRCUIT_BREAKER',
            'direccion': direccion,
            'precio': None,
            'contexto_json': None,
            'grid_params_json': None,
            'score': None,
            'rechazos_json': json.dumps(rechazos) if rechazos else None,
            'estado_maquina': 'CIRCUIT_BREAKER'
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
            evento_id = disparo['evento_id']
            precio_entrada = disparo['precio_entrada']
            direccion = disparo['direccion']
            grid_params = disparo['grid_params']
            minutos_desde = int((timestamp_utc - disparo['timestamp_utc']).total_seconds() / 60)
            if precio > disparo['maximo_visto']:
                disparo['maximo_visto'] = precio
            if precio < disparo['minimo_visto']:
                disparo['minimo_visto'] = precio
            if grid_params:
                upper = grid_params.get('upper_limit')
                lower = grid_params.get('lower_limit')
                if disparo['primera_vez_en_grid'] is None and lower and upper:
                    if lower <= precio <= upper:
                        disparo['primera_vez_en_grid'] = {
                            'timestamp_utc': timestamp_utc.isoformat(),
                            'precio': precio,
                            'minutos_desde': minutos_desde
                        }
                        if direccion == 'SHORT':
                            grid_rentable = precio <= precio_entrada
                        elif direccion == 'LONG':
                            grid_rentable = precio >= precio_entrada
                        else:
                            grid_rentable = True
                        await guardar_evento_post(
                            evento_id, symbol, timestamp_utc, 'PRIMERA_VEZ_EN_GRID',
                            precio,
                            distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                            grid_rentable_aqui=grid_rentable,
                            nota=f"Precio entra en rango del grid [{lower}, {upper}] | Rentable para {direccion}: {grid_rentable}"
                        )
                        await actualizar_disparo_activo(
                            evento_id,
                            primera_vez_en_grid_json=json.dumps(disparo['primera_vez_en_grid'])
                        )
                if disparo['primera_vez_fuera_rango'] is None and lower and upper:
                    if precio > upper or precio < lower:
                        direccion_fuera = 'UPPER' if precio > upper else 'LOWER'
                        disparo['primera_vez_fuera_rango'] = {
                            'timestamp_utc': timestamp_utc.isoformat(),
                            'precio': precio,
                            'minutos_desde': minutos_desde,
                            'direccion': direccion_fuera
                        }
                        if direccion == 'SHORT' and precio < lower:
                            nota_extra = " | RUPTURA FAVORABLE SHORT"
                        elif direccion == 'LONG' and precio > upper:
                            nota_extra = " | RUPTURA FAVORABLE LONG"
                        else:
                            nota_extra = " | RUPTURA DESFAVORABLE"
                        await guardar_evento_post(
                            evento_id, symbol, timestamp_utc, 'PRIMERA_VEZ_FUERA_RANGO',
                            precio,
                            distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                            grid_rentable_aqui=False,
                            nota=f"Precio rompe {direccion_fuera} del grid [{lower}, {upper}]" + nota_extra
                        )
                        await actualizar_disparo_activo(
                            evento_id,
                            primera_vez_fuera_rango_json=json.dumps(disparo['primera_vez_fuera_rango'])
                        )
            horas_seguimiento = CONFIG.auditoria_horas_seguimiento
            intervalo = CONFIG.auditoria_muestras_intervalo_min
            if minutos_desde <= horas_seguimiento * 60:
                intervalo_actual = minutos_desde // intervalo
                if intervalo_actual > disparo['muestras_guardadas']:
                    await guardar_muestra_post(evento_id, symbol, timestamp_utc, precio, minutos_desde)
                    disparo['muestras_guardadas'] = intervalo_actual
                    await actualizar_disparo_activo(
                        evento_id,
                        maximo_visto=disparo['maximo_visto'],
                        minimo_visto=disparo['minimo_visto'],
                        muestras_guardadas=disparo['muestras_guardadas']
                    )
            if minutos_desde > horas_seguimiento * 60:
                await self._guardar_eventos_finales(symbol, disparo, timestamp_utc)
                await eliminar_disparo_activo(evento_id)
                del self._disparos_activos[symbol]

    async def _guardar_eventos_finales(self, symbol: str, disparo: dict, timestamp_utc: datetime):
        evento_id = disparo['evento_id']
        precio_entrada = disparo['precio_entrada']
        direccion = disparo['direccion']
        if disparo['maximo_visto'] > precio_entrada:
            if direccion == 'SHORT':
                grid_rentable = False
                nota = "Máximo precio alcanzado durante seguimiento | DRAWDOWN para SHORT"
            else:
                grid_rentable = True
                nota = "Máximo precio alcanzado durante seguimiento | RUNUP para LONG"
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, 'MAXIMO_ABSOLUTO',
                disparo['maximo_visto'],
                distancia_desde_entrada_pct=round((disparo['maximo_visto'] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=grid_rentable,
                nota=nota
            )
        if disparo['minimo_visto'] < precio_entrada:
            if direccion == 'SHORT':
                grid_rentable = True
                nota = "Mínimo precio alcanzado durante seguimiento | RUNUP para SHORT"
            else:
                grid_rentable = False
                nota = "Mínimo precio alcanzado durante seguimiento | DRAWDOWN para LONG"
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, 'MINIMO_ABSOLUTO',
                disparo['minimo_visto'],
                distancia_desde_entrada_pct=round((disparo['minimo_visto'] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=grid_rentable,
                nota=nota
            )
        await eliminar_disparo_activo(evento_id)

    async def cerrar_seguimiento_todos(self):
        async with self._disparos_lock:
            disparos_copia = list(self._disparos_activos.items())
        for symbol, disparo in disparos_copia:
            await self._guardar_eventos_finales(symbol, disparo, now_utc())
            await eliminar_disparo_activo(disparo['evento_id'])
        async with self._disparos_lock:
            self._disparos_activos.clear()
