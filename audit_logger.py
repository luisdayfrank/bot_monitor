import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional
import pytz
from config import CONFIG
from database_v5 import (
    guardar_evento_auditoria, guardar_muestra_post,
    guardar_evento_post, guardar_vela_post,
    utc_to_local, now_utc, now_local
)


class AuditLogger:
    """
    Logger de auditoría para modo auditoría externa.

    Responsabilidad: SOLO guardar eventos en SQLite.
    NO evalúa, NO simula, NO toma decisiones.
    """

    def __init__(self):
        self._buffer_eventos: List[dict] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_interval = 60  # Flush cada 60 segundos
        self._shutdown = asyncio.Event()

        # Tracking de disparos activos para seguimiento post-FIRE
        self._disparos_activos: Dict[str, dict] = {}  # symbol -> {evento_id, timestamp, precio_entrada, grid_params}

    async def run(self):
        """Loop de flush periódico del buffer."""
        while not self._shutdown.is_set():
            await asyncio.sleep(self._flush_interval)
            await self._flush_buffer()

    async def stop(self):
        self._shutdown.set()
        await self._flush_buffer()

    async def _flush_buffer(self):
        """Flush del buffer a SQLite."""
        async with self._buffer_lock:
            if not self._buffer_eventos:
                return

            eventos = self._buffer_eventos.copy()
            self._buffer_eventos = []

        for evento in eventos:
            try:
                evento_id = await guardar_evento_auditoria(**evento)
                # Si es un FIRE, registrar para seguimiento post-disparo
                if evento.get('tipo') == 'FIRE' and evento_id:
                    symbol = evento['symbol']
                    ts_str = evento['timestamp_utc']
                    # Asegurar que el timestamp sea timezone-aware
                    if isinstance(ts_str, str):
                        ts = datetime.fromisoformat(ts_str)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=pytz.UTC)
                    else:
                        ts = ts_str

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
            except Exception as e:
                print(f"  ⚠️ Error guardando evento auditoría: {e}")

    # ───────────────────────────────────────────────────────────────────────────────
    # MÉTODOS PÚBLICOS: Hooks para signals_v4.py
    # ───────────────────────────────────────────────────────────────────────────────

    async def log_cambio_estado(self, symbol: str, de: str, a: str, direccion: str = None,
                                 contexto_macro: dict = None, score_macro: int = None):
        """Registra un cambio de estado de la máquina."""
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

    async def log_fire(self, symbol: str, direccion: str, precio: float,
                       contexto_1m: dict, contexto_15m: dict, contexto_4h: dict,
                       grid_params: dict, score_disparo: int):
        """Registra un disparo FIRE con todo el contexto."""
        if not CONFIG.modo_auditoria:
            return

        timestamp_utc = now_utc()

        # Combinar contextos
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
        """Registra un disparo rechazado."""
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
        """Registra activación de circuit breaker."""
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

    # ───────────────────────────────────────────────────────────────────────────────
    # SEGUIMIENTO POST-DISPARO
    # ───────────────────────────────────────────────────────────────────────────────

    async def trackear_precio_post_disparo(self, symbol: str, precio: float, timestamp_utc: datetime):
        """
        Llamado en cada tick de precio para monitorear disparos activos.
        Guarda muestras cada N minutos y detecta eventos significativos.
        """
        if not CONFIG.modo_auditoria:
            return

        if symbol not in self._disparos_activos:
            return

        # Asegurar que el timestamp sea timezone-aware
        if timestamp_utc.tzinfo is None:
            timestamp_utc = timestamp_utc.replace(tzinfo=pytz.UTC)

        disparo = self._disparos_activos[symbol]
        evento_id = disparo['evento_id']
        precio_entrada = disparo['precio_entrada']
        direccion = disparo['direccion']
        grid_params = disparo['grid_params']

        # Calcular minutos desde el disparo
        minutos_desde = int((timestamp_utc - disparo['timestamp_utc']).total_seconds() / 60)

        # Actualizar máximo y mínimo vistos
        if precio > disparo['maximo_visto']:
            disparo['maximo_visto'] = precio
        if precio < disparo['minimo_visto']:
            disparo['minimo_visto'] = precio

        # Detectar eventos significativos
        if grid_params:
            upper = grid_params.get('upper_limit')
            lower = grid_params.get('lower_limit')

            # Primera vez dentro del grid
            if disparo['primera_vez_en_grid'] is None and lower and upper:
                if lower <= precio <= upper:
                    disparo['primera_vez_en_grid'] = {
                        'timestamp_utc': timestamp_utc,
                        'precio': precio,
                        'minutos_desde': minutos_desde
                    }
                    await guardar_evento_post(
                        evento_id, symbol, timestamp_utc, 'PRIMERA_VEZ_EN_GRID',
                        precio,
                        distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                        grid_rentable_aqui=True,
                        nota=f"Precio entra en rango del grid [{lower}, {upper}]"
                    )

            # Primera vez fuera del grid
            if disparo['primera_vez_fuera_rango'] is None and lower and upper:
                if precio > upper or precio < lower:
                    direccion_fuera = 'UPPER' if precio > upper else 'LOWER'
                    disparo['primera_vez_fuera_rango'] = {
                        'timestamp_utc': timestamp_utc,
                        'precio': precio,
                        'minutos_desde': minutos_desde,
                        'direccion': direccion_fuera
                    }
                    await guardar_evento_post(
                        evento_id, symbol, timestamp_utc, 'PRIMERA_VEZ_FUERA_RANGO',
                        precio,
                        distancia_desde_entrada_pct=round((precio - precio_entrada) / precio_entrada * 100, 3),
                        grid_rentable_aqui=False,
                        nota=f"Precio rompe {direccion_fuera} del grid [{lower}, {upper}]"
                    )

        # Guardar muestra cada N minutos (solo primeras X horas)
        horas_seguimiento = CONFIG.auditoria_horas_seguimiento
        intervalo = CONFIG.auditoria_muestras_intervalo_min

        if minutos_desde <= horas_seguimiento * 60:
            # Verificar si ya guardamos una muestra en este intervalo
            intervalo_actual = minutos_desde // intervalo
            if intervalo_actual > disparo['muestras_guardadas']:
                await guardar_muestra_post(evento_id, symbol, timestamp_utc, precio, minutos_desde)
                disparo['muestras_guardadas'] = intervalo_actual

        # Verificar si debemos dejar de trackear (después de X horas)
        if minutos_desde > horas_seguimiento * 60:
            # Guardar eventos finales: máximo y mínimo absolutos
            await self._guardar_eventos_finales(symbol, disparo, timestamp_utc)
            del self._disparos_activos[symbol]

    async def _guardar_eventos_finales(self, symbol: str, disparo: dict, timestamp_utc: datetime):
        """Guarda los eventos finales de un disparo (máximo y mínimo absolutos)."""
        evento_id = disparo['evento_id']
        precio_entrada = disparo['precio_entrada']

        # Máximo absoluto
        if disparo['maximo_visto'] > precio_entrada:
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, 'MAXIMO_ABSOLUTO',
                disparo['maximo_visto'],
                distancia_desde_entrada_pct=round((disparo['maximo_visto'] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=False,
                nota="Máximo precio alcanzado durante el seguimiento"
            )

        # Mínimo absoluto
        if disparo['minimo_visto'] < precio_entrada:
            await guardar_evento_post(
                evento_id, symbol, timestamp_utc, 'MINIMO_ABSOLUTO',
                disparo['minimo_visto'],
                distancia_desde_entrada_pct=round((disparo['minimo_visto'] - precio_entrada) / precio_entrada * 100, 3),
                grid_rentable_aqui=True,
                nota="Mínimo precio alcanzado durante el seguimiento"
            )

    async def cerrar_seguimiento_todos(self):
        """Cierra todos los seguimientos activos (útil al final del día)."""
        for symbol, disparo in list(self._disparos_activos.items()):
            await self._guardar_eventos_finales(symbol, disparo, now_utc())
        self._disparos_activos.clear()
