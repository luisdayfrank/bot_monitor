import asyncio
import uvicorn
from contextlib import asynccontextmanager
from datetime import datetime
import pytz

from config import CONFIG
from database_v5 import init_db, close_db
from collector import DataCollector
from indicators_v3 import IndicatorEngineV3 as IndicatorEngine
from signals_v5 import SignalGenerator
from web.server_v5 import app, orquestador_eventos
from notifier_v5 import Notifier

from audit_logger import AuditLogger
from audit_reporter_v5 import AuditReporter
from grid_simulator import GridSimulator  # V5.9.2: Motor de simulación grid neutral

background_tasks = set()


@asynccontextmanager
async def lifespan(fastapi_app):
    await init_db()
    print("🗄️ Base de datos SQLite inicializada.")

    queue_velas = asyncio.Queue()
    queue_indicadores = asyncio.Queue()
    queue_eventos = asyncio.Queue()
    precios_vivo = {}

    collector = DataCollector(queue_velas, precios_vivo)
    engine = IndicatorEngine(queue_velas, queue_indicadores)
    signals = SignalGenerator(queue_indicadores, queue_eventos)
    notifier = Notifier()

    # V4.2: Guardar referencia para comandos Telegram
    notifier.signal_generator = signals

    # F2.8: Crear todas las instancias PRIMERO (antes de inyectar dependencias)
    audit_logger = None
    audit_reporter = None
    if CONFIG.modo_auditoria:
        audit_logger = AuditLogger()
        audit_reporter = AuditReporter(notifier)
        print(f"📋 Modo auditoría ACTIVADO ({CONFIG.timezone})")
        print(f"   Reporte diario: {CONFIG.auditoria_hora_reporte} {CONFIG.timezone}")
        # FASE 5.2: Notificar que MFM está activo
        print(f"   📊 MFM (Money Flow Multiplier) ACTIVADO en filtro macro")
        print(f"   Umbral alineación: ±{CONFIG.mfm_umbral_alineacion}")
    else:
        print("📋 Modo auditoría DESACTIVADO")

    # V5.9.2: Crear simulador de grid neutral
    grid_simulator = GridSimulator(
        precios_vivo=precios_vivo,
        indicadores_1m=signals.indicadores_1m,
        signal_states=signals.states,
    )

    # F2.8: BLOQUE SECUENCIAL de inyección de dependencias (TODAS juntas, antes de lanzar tareas)
    signals.audit_logger = audit_logger
    signals.grid_simulator = grid_simulator
    grid_simulator.audit_logger = audit_logger
    grid_simulator.notifier = notifier

    print("⏳ Cold start en progreso...")
    await collector.cold_start()
    print("✅ Cold start OK.")

    engine.buffers_1m = collector.buffers_1m
    engine.buffers_15m = collector.buffers_15m
    engine.buffers_4h = collector.buffers_4h
    await engine.precalcular()

    app.state.buffers_1m = engine.buffers_1m
    app.state.buffers_15m = engine.buffers_15m
    app.state.buffers_4h = engine.buffers_4h
    app.state.signal_states = signals.states
    app.state.precios_vivo = precios_vivo
    app.state.indicadores_1m = signals.indicadores_1m
    app.state.indicadores_15m = signals.indicadores_15m
    app.state.indicadores_4h = signals.indicadores_4h
    # V4.2: Exponer signal_generator para endpoints REST
    app.state.signal_generator = signals
    # V5.9.2: Exponer grid_simulator para endpoint /api/grid-neutral/{symbol}
    app.state.grid_simulator = grid_simulator

    # V5.9.2 MEJORA #6: Limpiar grids huérfanos al arranque
    print("  🧹 V5.9.2: Limpiando grids huérfanos...")
    await grid_simulator.limpiar_grids_huerfanos()

    print("✅ Precálculo completado. Arrancando pipeline V5.9.2...")

    t1 = asyncio.create_task(collector.run())
    t2 = asyncio.create_task(engine.run())
    t3 = asyncio.create_task(signals.run())
    t4 = asyncio.create_task(orquestador_eventos(
        queue_eventos=queue_eventos,
        precios_vivo=precios_vivo,
        indicadores_1m=signals.indicadores_1m,
        indicadores_15m=signals.indicadores_15m,
        indicadores_4h=signals.indicadores_4h,
        signal_states=signals.states
    ))

    # V5.9.2: Tarea del simulador de grid neutral
    t_grid_sim = asyncio.create_task(grid_simulator.run())

    # V5.9.2: Ticker que inyecta ticks de precio al simulador cada 1 minuto
    async def grid_ticker():
        """Inyecta ticks de precio al simulador usando velas 1m."""
        while True:
            await asyncio.sleep(60)  # Cada minuto
            try:
                for symbol in CONFIG.symbols:
                    st = signals.states.get(symbol)
                    if st and st.estado == 'NEUTRAL_GRID' and symbol in precios_vivo:
                        i1m = signals.indicadores_1m.get(symbol, {})
                        precio = precios_vivo[symbol]
                        # Usar high/low del indicador 1m o estimar desde precio
                        # F3.2: Usar datos reales de vela 1m, nunca estimados
                        high = i1m.get('high', precio)
                        low = i1m.get('low', precio)
                        close = i1m.get('close', precio)
                        # F3.2: Usar timestamp real de la vela 1m
                        ts_raw = i1m.get('timestamp')
                        if ts_raw and ts_raw > 1e12:
                            ts = int(ts_raw / 1000)
                        elif ts_raw:
                            ts = int(ts_raw)
                        else:
                            ts = int(datetime.now(pytz.UTC).timestamp())
                        await grid_simulator.queue.put({
                            'tipo': 'TICK',
                            'symbol': symbol,
                            'high': high,
                            'low': low,
                            'close': close,
                            'timestamp': ts
                        })
            except Exception as e:
                print(f"  ⚠️ [GRID_TICKER] Error: {e}")

    t_grid_ticker = asyncio.create_task(grid_ticker())

    # V5.9.2 MEJORA #6: Heartbeat del simulador cada 15 minutos
    async def grid_heartbeat():
        """Verifica salud de grids activos cada 15 minutos."""
        while True:
            await asyncio.sleep(CONFIG.grid_neutral_heartbeat_intervalo_min * 60)
            try:
                await grid_simulator.heartbeat()
            except Exception as e:
                print(f"  ⚠️ [GRID_HEARTBEAT] Error: {e}")

    t_grid_heartbeat = asyncio.create_task(grid_heartbeat())

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.3 FIX: Definir callback ANTES de usarlo en las tareas del grid
    # ═══════════════════════════════════════════════════════════════════════════════
    def task_done_callback(t):
        try:
            t.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"❌ ERROR CRÍTICO EN TAREA: {e}")

    # Aplicar callback a tareas del grid (F1.3 del Plan 6.1)
    for t in [t_grid_sim, t_grid_ticker, t_grid_heartbeat]:
        t.add_done_callback(task_done_callback)

    background_tasks.update([t1, t2, t3, t4, t_grid_sim, t_grid_ticker, t_grid_heartbeat])

    # Aplicar callback a tareas principales
    for t in [t1, t2, t3, t4]:
        t.add_done_callback(task_done_callback)

    if CONFIG.modo_auditoria and audit_logger and audit_reporter:
        t5 = asyncio.create_task(audit_logger.run())
        background_tasks.add(t5)
        t5.add_done_callback(task_done_callback)

        t6 = asyncio.create_task(audit_reporter.run())
        background_tasks.add(t6)
        t6.add_done_callback(task_done_callback)

        async def trackear_precios_post():
            while True:
                await asyncio.sleep(5)
                if not CONFIG.modo_auditoria:
                    break
                for symbol, precio in list(precios_vivo.items()):
                    try:
                        ts = datetime.now(pytz.UTC)
                        await audit_logger.trackear_precio_post_disparo(symbol, precio, ts)
                        await audit_logger.trackear_precio_near_miss(symbol, precio, ts)
                    except Exception as e:
                        print(f"  ⚠️ Error trackeando {symbol}: {e}")
                    await asyncio.sleep(0)

        t7 = asyncio.create_task(trackear_precios_post())
        background_tasks.add(t7)
        t7.add_done_callback(task_done_callback)

    await asyncio.sleep(2)
    await notifier.notificar_online(
        symbols=CONFIG.symbols,
        uptime_start=datetime.now(pytz.UTC).timestamp()
    )
    await notifier.notificar_resumen(
        estados=signals.states,
        precios=precios_vivo,
        indicadores_15m=signals.indicadores_15m
    )

    # V4.2: Pasar signal_generator al polling
    t8 = asyncio.create_task(notifier.polling_comandos(
        estados=signals.states,
        precios=precios_vivo,
        indicadores_15m=signals.indicadores_15m,
        indicadores_1m=signals.indicadores_1m,
        indicadores_4h=signals.indicadores_4h,
        signal_generator=signals
    ))
    background_tasks.add(t8)
    t8.add_done_callback(task_done_callback)

    # ═══════════════════════════════════════════════════════════════════════════════
    # V4.2: HEARTBEAT MEJORADO CON DOCUMENTACIÓN DE ESTADOS
    # ═══════════════════════════════════════════════════════════════════════════════
    if CONFIG.heartbeat_debug:
        async def heartbeat_validacion():
            """Log periódico del estado interno de todas las monedas. Solo lectura."""
            from datetime import datetime
            while True:
                await asyncio.sleep(CONFIG.heartbeat_intervalo_min * 60)
                lineas = []
                ahora = datetime.now(pytz.UTC).strftime('%H:%M:%S')

                # Contadores para resumen
                total_monedas = len(CONFIG.symbols)
                monedas_pausadas_manual = 0
                monedas_pausadas_auto = 0
                monedas_armed = 0
                monedas_fire = 0
                monedas_neutral_grid = 0  # FIX #7: Contador NEUTRAL_GRID

                for symbol in CONFIG.symbols:
                    st = signals.states[symbol]
                    i15 = signals.indicadores_15m.get(symbol, {})
                    i1m = signals.indicadores_1m.get(symbol, {})
                    adx = i15.get('adx', 'N/A')
                    rsi = i15.get('rsi', 'N/A')
                    score = st.score_macro_actual
                    dir_valida = st.direccion_ultima_valida or 'None'

                    # V4.2: Información de pausa mejorada
                    pausa_info = ''
                    if st.moneda_pausada_manual:
                        pausa_info = ' [PAUSADA-MANUAL]'
                        monedas_pausadas_manual += 1
                    elif st.moneda_pausada:
                        pausa_info = ' [PAUSADA-AUTO]'
                        monedas_pausadas_auto += 1
                    elif st.score_bajo_desde:
                        mins_bajo = (int(datetime.now(pytz.UTC).timestamp()) - st.score_bajo_desde) // 60
                        pausa_info = f' [bajo:{mins_bajo}min→pausa en {max(0, int(CONFIG.pausa_inactividad_horas*60)-mins_bajo)}min]'

                    armed_age = ''
                    if st.estado == 'ARMED' and st.armed_timestamp > 0:
                        mins = (int(datetime.now(pytz.UTC).timestamp() * 1000) - st.armed_timestamp) // 60000
                        armed_age = f' armed:{mins}min'
                        monedas_armed += 1

                    if st.estado == 'FIRE':
                        monedas_fire += 1

                    if st.estado == 'NEUTRAL_GRID':
                        monedas_neutral_grid += 1

                    # V4.2: Score con indicador visual
                    score_visual = score
                    if score >= 70:
                        score_str = f"{score}✅"
                    elif score >= 50:
                        score_str = f"{score}🟡"
                    elif score > 0:
                        score_str = f"{score}🔴"
                    else:
                        score_str = f"{score}⚪"

                    lineas.append(
                        f"{symbol}:{st.estado} score={score_str} adx={adx} rsi={rsi} "
                        f"dir={dir_valida}{armed_age}{pausa_info}"
                    )

                # V4.2: Resumen al inicio del heartbeat
                resumen = (
                    f"💓 [{ahora}] HEARTBEAT | "
                    f"Total:{total_monedas} | "
                    f"🔥FIRE:{monedas_fire} | "
                    f"🎯ARMED:{monedas_armed} | "
                    f"💠N-GRID:{monedas_neutral_grid} | "  # FIX #7: Mostrar NEUTRAL_GRID
                    f"⏸️Manual:{monedas_pausadas_manual} | "
                    f"⏸️Auto:{monedas_pausadas_auto}"
                )
                print(resumen)
                print(f"  💓 [{ahora}] DETALLE | {' | '.join(lineas)}")

                # V4.2: Leyenda cada 4 ciclos (1 hora)
                ciclo_actual = int(datetime.now(pytz.UTC).timestamp()) // (CONFIG.heartbeat_intervalo_min * 60)
                if ciclo_actual % 4 == 0:
                    print("  📖 LEYENDA: [PAUSADA-MANUAL]=solo /resume la reactiva | "
                          "[PAUSADA-AUTO]=se reactiva sola cuando score>=50 | "
                          "[bajo:Xmin→pausa en Ymin]=cuenta regresiva a pausa auto | "
                          "armed:Xmin=tiempo en ARMED | score✅>=70 🟡>=50 🔴>0 ⚪=0")

        t9 = asyncio.create_task(heartbeat_validacion())
        background_tasks.add(t9)
        t9.add_done_callback(task_done_callback)
        print(f"  💓 Heartbeat de validación activado (cada {CONFIG.heartbeat_intervalo_min}min)")
        print(f"  📖 Comandos Telegram: /pause, /resume, /pause_all, /resume_all, /list_paused")

    yield

    print("⏹️ Apagando procesos de forma segura...")

    # V5.9.2: Detener simulador de grid neutral
    grid_simulator.stop()

    if CONFIG.modo_auditoria and audit_logger:
        await audit_logger.cerrar_seguimiento_todos()
        await audit_logger.stop()

    collector.stop()


app.router.lifespan_context = lifespan

if __name__ == "__main__":
    print("🚀 Iniciando Crypto Monitor V6.0 — Multi-Timeframe Sniper Dashboard + MFM...")
    if CONFIG.modo_auditoria:
        print(f"📋 MODO AUDITORÍA: Reportes a las {CONFIG.auditoria_hora_reporte} {CONFIG.timezone}")
        print(f"📊 MODO MFM: Volumen inteligente con Money Flow Multiplier")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
