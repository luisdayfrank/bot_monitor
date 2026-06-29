import asyncio
import logging
import os
import shutil
import signal
import sqlite3
from datetime import datetime, timedelta
from plyer import notification
from telegram import Bot, Update, InputFile
from telegram.constants import ParseMode
from config import CONFIG
import pytz

class Notifier:
    def __init__(self):
        self.bot = Bot(token=CONFIG.telegram_token) if CONFIG.telegram_token else None
        self._last_update_id = 0
        self.signal_generator = None
        self._ws_last_msg_time = datetime.now(pytz.UTC)
        self._ws_connected = False
        self._process_start_time = datetime.now(pytz.UTC)

    # ═══════════════════════════════════════════════════════════════════════════════
    # FIX CRITICO: Limpiar mensajes pendientes de Telegram al iniciar
    # ═══════════════════════════════════════════════════════════════════════════════
    async def limpiar_updates_pendientes(self):
        """\n        Al iniciar el bot, limpia todos los mensajes pendientes de Telegram\n        para evitar que comandos antiguos se reprocesen.\n        """
        if not self.bot:
            return

        print("  [LIMPIEZA] Limpiando mensajes pendientes de Telegram...")
        try:
            updates = await self.bot.get_updates(limit=100, timeout=1)
            if updates:
                self._last_update_id = max(u.update_id for u in updates)
                print(f"  ✅ {len(updates)} mensajes pendientes limpiados. Offset: {self._last_update_id}")
            else:
                print("  ✅ Sin mensajes pendientes.")
        except Exception as e:
            print(f"  [ERROR] Error limpiando updates: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.5: Envío con reintentos exponenciales (1s → 2s → 4s → 8s)
    # ═══════════════════════════════════════════════════════════════════════════════
    async def _enviar_con_reintentos(self, mensaje: str, max_retries: int = 4) -> bool:
        """Envía mensaje a Telegram con backoff exponencial."""
        if not self.bot or not CONFIG.telegram_chat_id:
            return False

        for intento in range(max_retries):
            try:
                await self.bot.send_message(
                    chat_id=CONFIG.telegram_chat_id,
                    text=mensaje,
                    parse_mode=ParseMode.HTML
                )
                return True
            except Exception as e:
                delay = 2 ** intento  # 1s, 2s, 4s, 8s
                if intento < max_retries - 1:
                    logging.warning(f"Telegram error (intento {intento+1}/{max_retries}): {e}. Reintentando en {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Error enviando Telegram después de {max_retries} intentos: {e}")
                    print(f"  [ERROR] Telegram falló tras {max_retries} reintentos: {e}")
        return False

    async def enviar_telegram(self, mensaje: str):
        # FASE 3 FIX: Rastreo de flujo para diagnóstico
        print(f"  [TELEGRAM] Enviando mensaje ({len(mensaje)} chars): {mensaje[:60]}...")
        
        # FASE 4 FIX: Truncar mensajes que excedan el límite de Telegram (4096 chars)
        # Dejamos margen de 96 chars para el tag de truncamiento
        if len(mensaje) > 4000:
            print(f"  [TELEGRAM] Mensaje truncado de {len(mensaje)} a 4000 chars")
            mensaje = mensaje[:4000] + "\n\n<i>[Mensaje truncado por límite de Telegram]</i>"
        
        enviado = await self._enviar_con_reintentos(mensaje)
        if enviado:
            print(f"  📤 Telegram: {mensaje[:45]}...")
        else:
            print(f"  ❌ [TELEGRAM] Falló envío de mensaje ({len(mensaje)} chars)")

    async def enviar_archivo_telegram(self, filepath: str, caption: str = ""):
        if not self.bot or not CONFIG.telegram_chat_id:
            print(f"  ⚠️ Telegram no configurado, no se envio archivo")
            return False
        try:
            with open(filepath, 'rb') as f:
                await self.bot.send_document(
                    chat_id=CONFIG.telegram_chat_id,
                    document=InputFile(f, filename=filepath.split('/')[-1]),
                    caption=caption[:1024] if caption else None,
                    parse_mode=ParseMode.HTML
                )
            print(f"  📎 Archivo enviado: {filepath.split('/')[-1]}")
            return True
        except Exception as e:
            logging.error(f"Error enviando archivo Telegram: {e}")
            print(f"  [ERROR] Error enviando archivo: {e}")
            return False

    def enviar_local(self, titulo: str, mensaje: str):
        try:
            notification.notify(title=titulo, message=mensaje, app_name="Crypto Monitor V5.7", timeout=10)
        except Exception as e:
            logging.error(f"Error notificacion local: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # F1.5: Verificación de conexión al arrancar (getMe)
    # ═══════════════════════════════════════════════════════════════════════════════
    async def verificar_conexion_telegram(self):
        """Verifica token y chat_id al arrancar. No bloquea el bot si falla."""
        if not self.bot:
            print("  ⚠️ Telegram no configurado (sin token)")
            return False
        try:
            me = await self.bot.get_me()
            print(f"  ✅ Telegram conectado: @{me.username}")
            return True
        except Exception as e:
            print(f"  ❌ ERROR CRÍTICO: Telegram no conectado. Verifica token. Error: {e}")
            return False

    # ═══════════════════════════════════════════════════════════════════════════════
    # FASE 5: PROCESAR ALERTA
    # ═══════════════════════════════════════════════════════════════════════════════
    async def procesar_alerta(self, evento: dict):
        # FASE 3 FIX: Rastreo de flujo para diagnóstico
        symbol = evento.get('symbol', 'UNKNOWN')
        tipo = evento.get('tipo', 'UNKNOWN')
        print(f"  [NOTIFIER] Procesando alerta: {tipo} | {symbol} | precio={evento.get('price')}")
        
        # Validación defensiva de entrada
        if not isinstance(evento, dict):
            print(f"  ❌ [NOTIFIER] evento no es dict: {type(evento)}")
            return
        if not symbol or not tipo:
            print(f"  ❌ [NOTIFIER] evento incompleto: symbol={symbol} tipo={tipo}")
            return
        dir_ = evento['direction']
        score = evento['score']
        price = evento['price']
        estado = evento.get('estado_maquina', 'UNKNOWN')

        if tipo == 'FIRE':
            titulo = f"SNIPER {symbol} | {dir_}"
            p = evento.get('params') or {}
            auto_comp = "📐 <i>Auto-comprimido</i>\n" if p.get('auto_compressed') else ""
            pos_ext = "⚠️ <i>Posición extrema en rango</i>\n" if p.get('posicion_extrema') else ""
            dir_icon = "🟢" if dir_ == "LONG" else "🔴"

            # V6.0: Parámetros formateados para copiar-pegar en Binance
            params_binance = (
                f"<code>"
                f"Tipo: {dir_}\n"
                f"Rango: {p['lower_limit']} - {p['upper_limit']}\n"
                f"Grids: {p['grid_count']}\n"
                f"Step: {p['step_pct']}%\n"
                f"Capital: ${p['capital_sugerido']}\n"
                f"Apalancamiento: {p['apalancamiento_sugerido']}x\n"
                f"Notional/orden: ${p['notional_por_orden']}\n"
                f"Breakeven: {p['breakeven_pct']}%\n"
                f"Margen seguro: +{p['margen_sobre_breakeven']:.3f}%"
                f"</code>"
            )

            msg = (
                f"<b>🔥 DISPARO {dir_icon} {dir_} - {symbol}</b>\n"
                f"Estado: <b>{estado}</b>\n"
                f"Precio: 💲{price:.4f} | Score: 🎯 {score}/100\n\n"
                f"{auto_comp}"
                f"{pos_ext}"
                f"<b>⚙️ PARÁMETROS PARA REPLICAR EN BINANCE:</b>\n"
                f"{params_binance}\n\n"
                f"<b>📊 Stats del grid:</b>\n"
                f"Posición en rango: {p['posicion_en_rango']:.0%}\n"
                f"ATR seguro: {p.get('atr_seguro', 'N/A')}\n"
                f"Rango mult: {p.get('rango_mult', 'N/A')}x"
            )

        elif tipo == 'NEUTRAL_GRID':
            titulo = f"GRID NEUTRAL {symbol}"
            i15 = evento.get('indicadores_15m') or {}
            p = evento.get('params') or {}  # FASE 2 FIX: Proteger contra params=None
            adx = i15.get('adx')
            rsi = i15.get('rsi')

            adx_str = f"{adx:.1f}" if isinstance(adx, (int, float)) else "N/A"
            rsi_str = f"{rsi:.1f}" if isinstance(rsi, (int, float)) else "N/A"

            # V6.0: Parámetros formateados para copiar-pegar en Binance
            params_binance = ""
            if p:
                params_binance = (
                    f"\n\n<b>⚙️ PARÁMETROS PARA REPLICAR EN BINANCE:</b>\n"
                    f"<code>"
                    f"Tipo: NEUTRAL\n"
                    f"Rango: {p.get('lower_limit', 'N/A')} - {p.get('upper_limit', 'N/A')}\n"
                    f"Grids: {p.get('grid_count', 'N/A')}\n"
                    f"Step: {p.get('step_pct', 'N/A')}%\n"
                    f"Capital: ${p.get('capital_sugerido', 'N/A')}\n"
                    f"Apalancamiento: {p.get('apalancamiento_sugerido', 'N/A')}x\n"
                    f"Notional/orden: ${p.get('notional_por_orden', 'N/A')}\n"
                    f"Breakeven: {p.get('breakeven_pct', 'N/A')}%\n"
                    f"Margen seguro: +{p.get('margen_sobre_breakeven', 'N/A')}%"
                    f"</code>"
                )
                if p.get('auto_compressed'):
                    params_binance += "\n<i>⚠️ Grid auto-comprimido por bajo ATR</i>"
                if p.get('posicion_extrema'):
                    params_binance += "\n<i>⚠️ Posición extrema en rango</i>"
            else:
                params_binance = "\n\n<i>⚠️ Parámetros del grid no disponibles</i>"

            msg = (
                f"<b>💠 GRID NEUTRAL ACTIVADO - {symbol}</b>\n"
                f"Estado: <b>{estado}</b>\n"
                f"Precio: 💲{price:.4f}\n\n"
                f"<b>📊 Condiciones de entrada:</b>\n"
                f"• ADX: {adx_str} (< 25, sin tendencia fuerte)\n"
                f"• RSI: {rsi_str} (40-60, neutral)\n"
                f"• Precio cerca de EMA50\n"
                f"• Volatilidad moderada"
                f"{params_binance}\n\n"
                f"<b>🛑 Aborto automático en:</b>\n"
                f"• Timeout: ⏳ {CONFIG.grid_neutral_timeout_min} min\n"
                f"• ADX > {CONFIG.grid_neutral_adx_max}\n"
                f"• RSI < {CONFIG.grid_neutral_rsi_min} o RSI > {CONFIG.grid_neutral_rsi_max}\n"
                f"• Precio se aleja > {CONFIG.grid_neutral_aborto_precio_pct}% de EMA50\n"
                f"• ATR explosivo (> p80 x 1.5)\n\n"
                f"<i>🤖 Estado autónomo. Sin confirmación manual.</i>"
            )

        elif tipo == 'RECHAZADO':
            titulo = f"RECHAZADO {symbol}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Condiciones no cumplidas"
            msg = (
                f"<b>❌ DISPARO RECHAZADO - {symbol}</b>\n"
                f"Dirección: {dir_}\n"
                f"Precio: 💲{price:.4f}\n"
                f"Motivo: <i>{motivos}</i>"
            )

        elif tipo == 'CIRCUIT_BREAKER':
            titulo = f"CIRCUIT BREAKER {symbol}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Pérdidas consecutivas"
            msg = (
                f"<b>🔒 CIRCUIT BREAKER - {symbol}</b>\n"
                f"Motivo: <i>{motivos}</i>\n"
                f"Pausa: ⏳ {CONFIG.circuit_breaker_pausa_seg // 60} minutos\n"
                f"⚠️ Capital reducido al 50% para próximos disparos"
            )

        elif tipo == 'ARMED':
            titulo = f"ARMED {symbol} | {dir_}"
            dir_icon = "🟢" if dir_ == "LONG" else "🔴"
            msg = f"<b>🟡 ARMED - {symbol}</b>\nFiltro macro activado. Esperando gatillo {dir_icon} {dir_}..."

        else:
            titulo = f"ALERTA {symbol} | {tipo}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Cambio de condiciones."
            msg = (
                f"<b>⚠️ {symbol} - ALERTA</b>\n"
                f"Tipo: {tipo}\n"
                f"Motivos: <i>{motivos}</i>"
            )
        self.enviar_local(titulo, "Revisa Telegram para detalles.")
        await self.enviar_telegram(msg)

    async def notificar_online(self, symbols: list, uptime_start: float = None):
        # F1.5: Verificar conexión antes de enviar mensaje de bienvenida
        await self.verificar_conexion_telegram()

        uptime_str = ""
        if uptime_start:
            elapsed = datetime.now(pytz.UTC).timestamp() - uptime_start
            uptime_str = f"\n⏱️ Tiempo de arranque: {elapsed:.1f}s"

        grid_neutral_info = ""
        if CONFIG.grid_neutral_enabled:
            grid_neutral_info = f"\n\n<b>💠 GRID NEUTRAL:</b> ✅ ACTIVADO\n"
            grid_neutral_info += f"• Timeout: ⏳ {CONFIG.grid_neutral_timeout_min}min | "
            grid_neutral_info += f"ADX max: {CONFIG.grid_neutral_adx_max} | "
            grid_neutral_info += f"RSI rango: {CONFIG.grid_neutral_rsi_min}-{CONFIG.grid_neutral_rsi_max}"
        else:
            grid_neutral_info = f"\n\n<b>💠 GRID NEUTRAL:</b> ❌ DESACTIVADO (toggle: grid_neutral_enabled=false)"

        msg = (
            f"<b>🚀 Crypto Monitor V5.7 ONLINE</b>\n"
            f"📡 Monitoreando {len(symbols)} activos{uptime_str}\n\n"
            f"<b>Activos:</b> {', '.join(symbols)}"
            f"{grid_neutral_info}"
        )
        await self.enviar_telegram(msg)

    async def notificar_resumen(self, estados: dict, precios: dict, indicadores_15m: dict):
        lineas = ["<b>📊 RESUMEN DE MONEDAS</b>\n"]
        for symbol in sorted(estados.keys()):
            st = estados[symbol]
            precio = precios.get(symbol, 0)
            i15 = indicadores_15m.get(symbol, {})

            estado_icon = {
                'FIRE': '🔥',
                'ARMED': '🟡',
                'COOLDOWN': '🟠',
                'MONITOREO': '⚪',
                'NEUTRAL_GRID': '💠'
            }.get(st.estado, '⚫')

            dir_icon = '🟢 LONG' if st.direccion_filtro == 'LONG' else ('🔴 SHORT' if st.direccion_filtro == 'SHORT' else '⚖️ NEUTRAL')

            # FIX: Evitar TypeError formateando un valor que podría ser None
            rsi_val = i15.get('rsi')
            adx_val = i15.get('adx')
            rsi_str = f"{rsi_val:.1f}" if isinstance(rsi_val, (int, float)) else "--"
            adx_str = f"{adx_val:.1f}" if isinstance(adx_val, (int, float)) else "--"

            pausa_info = ""
            if getattr(st, 'moneda_pausada_manual', False):
                pausa_info = " ⏸️[M]"
            elif getattr(st, 'moneda_pausada', False):
                pausa_info = " ⏸️[A]"

            grid_info = ""
            if st.estado == 'NEUTRAL_GRID' and hasattr(st, 'neutral_grid_timestamp') and st.neutral_grid_timestamp > 0:
                mins_en_grid = (int(datetime.now(pytz.UTC).timestamp()) - st.neutral_grid_timestamp) // 60
                grid_info = f" ⏳{mins_en_grid}m"

            lineas.append(
                f"{estado_icon} <b>{symbol}</b>{pausa_info}{grid_info} | {st.estado} | {dir_icon}\n"
                f"   💲 {precio:.4f} | RSI: {rsi_str} | ADX: {adx_str}"
            )
        msg = "\n".join(lineas)
        await self.enviar_telegram(msg)

    async def notificar_status(self, estados: dict, precios: dict, indicadores_15m: dict):
        ahora = datetime.now(pytz.UTC).strftime("%H:%M:%S UTC")
        lineas = [f"<b>📋 ESTADO ACTUAL</b>\n🕐 {ahora}\n"]

        for symbol in sorted(estados.keys()):
            st = estados[symbol]
            precio = precios.get(symbol, 0)
            i15 = indicadores_15m.get(symbol, {})

            estado_icon = {
                'FIRE': '🔥',
                'ARMED': '🟡',
                'COOLDOWN': '🟠',
                'MONITOREO': '⚪',
                'NEUTRAL_GRID': '💠'
            }.get(st.estado, '⚫')

            dir_str = st.direccion_filtro or 'NEUTRAL'
            dir_icon = "🟢" if dir_str == "LONG" else "🔴" if dir_str == "SHORT" else "⚖️"

            # FIX: Evitar TypeError
            rsi_val = i15.get('rsi')
            adx_val = i15.get('adx')
            rsi_str = f"{rsi_val:.1f}" if isinstance(rsi_val, (int, float)) else "--"
            adx_str = f"{adx_val:.1f}" if isinstance(adx_val, (int, float)) else "--"

            cb_info = ""
            if hasattr(st, 'circuit_breaker_activo') and st.circuit_breaker_activo:
                remaining = (st.circuit_breaker_hasta - int(datetime.now(pytz.UTC).timestamp()*1000)) // 1000
                cb_info = f" 🔒[CB:{remaining}s]"

            pausa_info = ""
            if getattr(st, 'moneda_pausada_manual', False):
                pausa_info = " ⏸️[M]"
            elif getattr(st, 'moneda_pausada', False):
                pausa_info = " ⏸️[A]"

            grid_info = ""
            if st.estado == 'NEUTRAL_GRID':
                grid_info = " 💠[GRID]"

            lineas.append(
                f"{estado_icon} <b>{symbol}</b>{cb_info}{pausa_info}{grid_info} | {st.estado} | {dir_icon} {dir_str}\n"
                f"   💲 {precio:.4f} | RSI(15m): {rsi_str} | ADX: {adx_str}"
            )
        msg = "\n".join(lineas)
        await self.enviar_telegram(msg)

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 1: COMANDOS OBLIGATORIOS PARA VPS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_restart(self):
        """/restart — Reinicio controlado del bot."""
        await self.enviar_telegram(
            "🔄 <b>REINICIANDO BOT...</b>\n"
            "Guardando memoria RAM en la base de datos de forma segura..."
        )
        await asyncio.sleep(1)
        # FIX: os._exit() no permite apagar elegantemente (pierdes los near-misses en RAM)
        # Usamos SIGINT para activar el bloque de apagado seguro en main.py
        print("🔄 Comando /restart recibido. Solicitando shutdown ordenado...")
        os.kill(os.getpid(), signal.SIGINT)

    async def _cmd_stop(self):
        """/stop — Apagado seguro."""
        await self.enviar_telegram(
            "🛑 <b>DETENIENDO BOT...</b>\n"
            "Cerrando conexiones WS y guardando métricas..."
        )
        await asyncio.sleep(1)
        print("🛑 Comando /stop recibido. Solicitando shutdown ordenado...")
        os.kill(os.getpid(), signal.SIGINT)

    async def _cmd_config(self):
        """/config — Ver configuracion actual."""
        grid_neutral_status = "✅ ACTIVADO" if CONFIG.grid_neutral_enabled else "❌ DESACTIVADO"
        msg = (
            f"<b>⚙️ CONFIGURACIÓN ACTUAL V5.7</b>\n\n"
            f"<b>Monedas:</b> {', '.join(CONFIG.symbols)} ({len(CONFIG.symbols)})\n"
            f"<b>Timeframes:</b> {CONFIG.tf_micro} / {CONFIG.tf_primary} / {CONFIG.tf_macro}\n"
            f"<b>Zona horaria:</b> {CONFIG.timezone}\n"
            f"<b>Modo auditoría:</b> {'✅ ON' if CONFIG.modo_auditoria else '❌ OFF'}\n"
            f"<b>Heartbeat:</b> {'✅ ON' if getattr(CONFIG, 'heartbeat_debug', False) else '❌ OFF'}\n\n"
            f"<b>💠 Grid Neutral:</b> {grid_neutral_status}\n"
            f"  • Timeout: ⏳ {CONFIG.grid_neutral_timeout_min}min\n"
            f"  • ADX max: {CONFIG.grid_neutral_adx_max}\n"
            f"  • RSI rango: {CONFIG.grid_neutral_rsi_min}-{CONFIG.grid_neutral_rsi_max}\n"
            f"  • Aborto precio: {CONFIG.grid_neutral_aborto_precio_pct}%\n"
            f"  • Aborto ADX delta: +{getattr(CONFIG, 'grid_neutral_aborto_adx_delta', 'N/A')}\n\n"
            f"<b>🎚️ Umbrales:</b>\n"
            f"  • ADX ideal: {CONFIG.adx_ideal}\n"
            f"  • ADX reject: {CONFIG.adx_reject}\n"
            f"  • RSI micro short: {CONFIG.rsi_micro_short_trigger}\n"
            f"  • RSI micro long: {CONFIG.rsi_micro_long_trigger}\n"
            f"  • ATR min: {CONFIG.atr_min_pct}% | max: {CONFIG.atr_max_pct}%\n\n"
            f"<b>🤖 Grid:</b>\n"
            f"  • Min grids: {CONFIG.grid_min_grids}\n"
            f"  • Max grids: {CONFIG.grid_max_grids_hard}\n"
            f"  • Capital default: 💲{CONFIG.grid_default_capital}\n"
            f"  • Leverage default: {CONFIG.grid_default_leverage}x\n\n"
            f"<b>🔒 Circuit Breaker:</b>\n"
            f"  • Disparos: {CONFIG.circuit_breaker_disparos}\n"
            f"  • Pausa: ⏳ {CONFIG.circuit_breaker_pausa_seg//60}min\n"
            f"  • Reducción capital: {CONFIG.circuit_breaker_reduccion_capital*100:.0f}%"
        )
        await self.enviar_telegram(msg)

    async def _cmd_logs(self, args: str):
        """/logs [N] — Ultimas N lineas del log."""
        try:
            n = int(args.strip()) if args.strip() else 20
            n = min(max(n, 1), 100)
        except ValueError:
            n = 20

        log_path = "crypto_monitor.log"
        if not os.path.exists(log_path):
            await self.enviar_telegram("❌ No se encontró crypto_monitor.log")
            return

        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            last_lines = lines[-n:]
            content = "".join(last_lines)
            if len(content) > 3500:
                content = content[-3500:]
            await self.enviar_telegram(
                f"<b>📝 ÚLTIMAS {len(last_lines)} LÍNEAS DE LOG</b>\n"
                f"<pre>{content}</pre>"
            )
        except Exception as e:
            await self.enviar_telegram(f"❌ Error leyendo logs: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 2: COMANDOS DE MONITOREO AVANZADO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_health(self, precios_vivo: dict, signal_states: dict):
        """/health — Diagnostico completo."""
        uptime = datetime.now(pytz.UTC) - self._process_start_time
        uptime_str = f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m"

        estados_count = {}
        pausadas_manual = 0
        pausadas_auto = 0
        neutral_grid_count = 0

        for s in signal_states.values():
            estados_count[s.estado] = estados_count.get(s.estado, 0) + 1
            if getattr(s, 'moneda_pausada_manual', False):
                pausadas_manual += 1
            elif getattr(s, 'moneda_pausada', False):
                pausadas_auto += 1
            if s.estado == 'NEUTRAL_GRID':
                neutral_grid_count += 1

        db_size = 0
        if os.path.exists(CONFIG.db_path):
            db_size = os.path.getsize(CONFIG.db_path) / (1024*1024)

        msg = (
            f"<b>🏥 HEALTH CHECK V5.7</b>\n\n"
            f"<b>⚙️ Proceso:</b>\n"
            f"  • Uptime: ⏱️ {uptime_str}\n"
            f"  • Precios activos: 📡 {len(precios_vivo)}/{len(CONFIG.symbols)}\n\n"
            f"<b>🚦 Estados máquina:</b>\n"
            f"  🔥 FIRE: {estados_count.get('FIRE', 0)}\n"
            f"  🟡 ARMED: {estados_count.get('ARMED', 0)}\n"
            f"  🟠 COOLDOWN: {estados_count.get('COOLDOWN', 0)}\n"
            f"  ⚪ MONITOREO: {estados_count.get('MONITOREO', 0)}\n"
            f"  💠 NEUTRAL_GRID: {neutral_grid_count}\n"
            f"  ⏸️ Pausadas manual: {pausadas_manual}\n"
            f"  ⏸️ Pausadas auto: {pausadas_auto}\n\n"
            f"<b>💠 Fase 5 - Grid Neutral:</b> {'✅ ACTIVADO' if CONFIG.grid_neutral_enabled else '❌ DESACTIVADO'}\n\n"
            f"<b>🗄️ Base de datos:</b>\n"
            f"  • Tamaño: {db_size:.1f} MB\n"
            f"  • Path: <code>{CONFIG.db_path}</code>\n\n"
            f"<b>🔌 WebSocket:</b>\n"
            f"  • Último mensaje: hace {(datetime.now(pytz.UTC)-self._ws_last_msg_time).seconds}s\n"
            f"  • Estado: {'✅ Conectado' if self._ws_connected else '❌ Desconectado'}"
        )
        await self.enviar_telegram(msg)

    async def _cmd_symbol(self, symbol: str, precios: dict, indicadores_1m: dict,
                          indicadores_15m: dict, indicadores_4h: dict, signal_states: dict):
        """/symbol <SYMBOL> — Estado detallado de una moneda."""
        symbol = symbol.upper().strip()
        if symbol not in CONFIG.symbols:
            await self.enviar_telegram(f"❌ <b>{symbol}</b> no está en la lista de monedas.")
            return

        st = signal_states.get(symbol)
        i1m = indicadores_1m.get(symbol, {})
        i15 = indicadores_15m.get(symbol, {})
        i4h = indicadores_4h.get(symbol, {})
        precio = precios.get(symbol, 'N/A')

        pausa_info = ""
        if st:
            if getattr(st, 'moneda_pausada_manual', False):
                pausa_info = "\n⏸️ <b>PAUSADA MANUALMENTE</b>"
            elif getattr(st, 'moneda_pausada', False):
                pausa_info = f"\n⏸️ <b>AUTO-PAUSADA</b> | Razón: {getattr(st, 'moneda_pausada_razon', 'N/A')}"

        cb_info = ""
        if st and getattr(st, 'circuit_breaker_activo', False):
            remaining = (st.circuit_breaker_hasta - int(datetime.now(pytz.UTC).timestamp()*1000)) // 1000
            cb_info = f"\n🔒 <b>Circuit Breaker</b> | ⏳ {remaining}s restantes"

        grid_info = ""
        if st and st.estado == 'NEUTRAL_GRID':
            mins_en_grid = 0
            if hasattr(st, 'neutral_grid_timestamp') and st.neutral_grid_timestamp > 0:
                mins_en_grid = (int(datetime.now(pytz.UTC).timestamp()) - st.neutral_grid_timestamp) // 60
            grid_info = f"\n💠 <b>GRID NEUTRAL</b> | Tiempo: ⏳ {mins_en_grid}min / {CONFIG.grid_neutral_timeout_min}min"

        dir_icon = "🟢" if st and st.direccion_filtro == "LONG" else "🔴" if st and st.direccion_filtro == "SHORT" else "⚖️"

        msg = (
            f"<b>🔍 {symbol} — DETALLE COMPLETO</b>{pausa_info}{cb_info}{grid_info}\n\n"
            f"<b>Precio:</b> 💲{precio if isinstance(precio, str) else f'{precio:.4f}'}\n"
            f"<b>Estado máquina:</b> {st.estado if st else 'N/A'}\n"
            f"<b>Dirección filtro:</b> {dir_icon} {st.direccion_filtro if st else 'N/A'}\n"
            f"<b>Score macro:</b> {st.score_macro_actual if st else 'N/A'}\n"
            f"<b>Score último disparo:</b> {st.ultimo_score if st else 'N/A'}\n\n"
            f"<b>⏱️ Indicadores 1m:</b>\n"
            f"  • RSI(7): {i1m.get('rsi_7', 'N/A')}\n"
            f"  • EMA300: {i1m.get('ema_300', 'N/A')}\n"
            f"  • ATR(1m): {i1m.get('atr_1m', 'N/A')}\n"
            f"  • Mecha SHORT: {'✅' if i1m.get('mecha_valida_short') else '❌'}\n"
            f"  • Mecha LONG: {'✅' if i1m.get('mecha_valida_long') else '❌'}\n\n"
            f"<b>🧭 Indicadores 15m:</b>\n"
            f"  • RSI(14): {i15.get('rsi', 'N/A')}\n"
            f"  • ADX(14): {i15.get('adx', 'N/A')}\n"
            f"  • ATR(15m): {i15.get('atr', 'N/A')}\n"
            f"  • MACD hist: {i15.get('macd_hist', 'N/A')}\n"
            f"  • EMA200: {i15.get('ema200_15m', 'N/A')}\n\n"
            f"<b>🔭 Indicadores 4h:</b>\n"
            f"  • EMA200: {i4h.get('ema200_4h', 'N/A')}"
        )
        await self.enviar_telegram(msg)

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 3: CONFIGURACION EN CALIENTE
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_set_threshold(self, args: str):
        """/set_threshold <param> <valor> — Cambiar umbrales sin reiniciar."""
        parts = args.strip().split()
        if len(parts) != 2:
            await self.enviar_telegram(
                "❌ Uso: <code>/set_threshold parametro valor</code>\n"
                "Ejemplos:\n"
                "<code>/set_threshold adx_reject 50</code>\n"
                "<code>/set_threshold grid_neutral_enabled true</code>\n" 
                "<code>/set_threshold rsi_micro_short_trigger 80</code>\n"
                "<code>/set_threshold atr_min_pct 0.10</code>"
            )
            return

        param, valor_str = parts
        if not hasattr(CONFIG, param):
            await self.enviar_telegram(f"❌ Parámetro <b>{param}</b> no existe en config.")
            return

        try:
            current_val = getattr(CONFIG, param)
            if isinstance(current_val, bool):
                new_val = valor_str.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(current_val, int):
                new_val = int(valor_str)
            elif isinstance(current_val, float):
                new_val = float(valor_str)
            else:
                new_val = valor_str

            setattr(CONFIG, param, new_val)
            await self.enviar_telegram(
                f"✅ <b>{param}</b> cambiado a <b>{new_val}</b>\n"
                f"⚠️ <i>Cambio temporal. Se perderá al reiniciar el bot.</i>"
            )
        except Exception as e:
            await self.enviar_telegram(f"❌ Error: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 4: EMERGENCIA
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_reset_circuit(self, symbol: str, signal_states: dict):
        """/reset_circuit <SYMBOL> — Resetear circuit breaker."""
        symbol = symbol.upper().strip()
        if symbol not in signal_states:
            await self.enviar_telegram(f"❌ <b>{symbol}</b> no encontrado.")
            return

        st = signal_states[symbol]
        st.circuit_breaker_activo = False
        st.circuit_breaker_hasta = 0
        st.disparos_consecutivos = 0
        st.capital_actual = CONFIG.grid_default_capital

        await self.enviar_telegram(
            f"🔓 <b>{symbol}</b> Circuit Breaker RESETEADO\n"
            f"Capital restaurado a 💲{CONFIG.grid_default_capital}\n"
            f"Moneda lista para operar."
        )

    async def _cmd_grid_neutral(self, args: str):
        """/grid_neutral <on|off> — Activar/desactivar grid neutral en caliente."""
        arg = args.strip().lower()
        if arg in ('on', 'true', '1', 'yes'):
            CONFIG.grid_neutral_enabled = True
            await self.enviar_telegram(
                f"✅ <b>GRID NEUTRAL ACTIVADO</b>\n"
                f"El bot ahora entrará en estado NEUTRAL_GRID cuando\n"
                f"detecte mercado lateral (ADX < 20, RSI 40-60, etc).\n\n"
                f"⚙️ <b>Parámetros actuales:</b>\n"
                f"• Timeout: ⏳ {CONFIG.grid_neutral_timeout_min}min\n"
                f"• ADX max: {CONFIG.grid_neutral_adx_max}\n"
                f"• Aborto precio: {CONFIG.grid_neutral_aborto_precio_pct}%"
            )
        elif arg in ('off', 'false', '0', 'no'):
            CONFIG.grid_neutral_enabled = False
            await self.enviar_telegram(
                f"❌ <b>GRID NEUTRAL DESACTIVADO</b>\n"
                f"El bot ya no entrará en estado NEUTRAL_GRID.\n"
                f"Los estados existentes se mantienen hasta su aborto natural."
            )
        else:
            status = "✅ ACTIVADO" if CONFIG.grid_neutral_enabled else "❌ DESACTIVADO"
            await self.enviar_telegram(
                f"ℹ️ Grid Neutral está <b>{status}</b>\n\n"
                f"Uso: <code>/grid_neutral on</code> o <code>/grid_neutral off</code>"
            )

    async def _cmd_backup(self):
        """/backup — Forzar backup seguro de la base de datos."""
        if not os.path.exists(CONFIG.db_path):
            await self.enviar_telegram("❌ Base de datos no encontrada.")
            return

        timestamp = datetime.now(pytz.UTC).strftime("%Y%m%d_%H%M%S")
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = f"{backup_dir}/crypto_monitor_{timestamp}.db"

        try:
            # FIX: Copiar SQLite usando el motor nativo garantiza que los
            # datos temporales en el archivo WAL pasen completos al backup.
            def hacer_backup():
                with sqlite3.connect(CONFIG.db_path) as src:
                    with sqlite3.connect(backup_path) as dst:
                        src.backup(dst)

            await asyncio.to_thread(hacer_backup)
            size_mb = os.path.getsize(backup_path) / (1024*1024)
            await self.enviar_telegram(
                f"💾 <b>BACKUP CREADO EXITOSAMENTE</b>\n"
                f"Archivo: <code>{backup_path}</code>\n"
                f"Tamaño: {size_mb:.1f} MB"
            )
        except Exception as e:
            await self.enviar_telegram(f"❌ Error creando backup: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # POLLING PRINCIPAL DE COMANDOS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def polling_comandos(self, estados: dict, precios: dict, indicadores_15m: dict,
                                signal_generator=None, indicadores_1m: dict = None,
                                indicadores_4h: dict = None):
        if not self.bot or not CONFIG.telegram_chat_id:
            print("  ⚠️ Telegram no configurado. Polling de comandos desactivado.")
            return

        self.signal_generator = signal_generator
        indicadores_1m = indicadores_1m or {}
        indicadores_4h = indicadores_4h or {}

        await self.limpiar_updates_pendientes()

        print("  ✅ Polling de comandos Telegram iniciado (cada 3s)")
        print("  📋 Comandos: /status, /pause, /resume, /list_paused, /restart, /stop, /config, /logs, /health, /symbol, /set_threshold, /grid_neutral, /reset_circuit, /backup, /help")

        while True:
            try:
                updates = await self.bot.get_updates(
                    offset=self._last_update_id + 1,
                    limit=10,
                    timeout=5
                )

                for update in updates:
                    self._last_update_id = update.update_id

                    if not update.message or not update.message.text:
                        continue

                    chat_id = update.message.chat_id
                    text = update.message.text.strip()
                    text_lower = text.lower()

                    if str(chat_id) != str(CONFIG.telegram_chat_id):
                        continue

                    cmd_parts = text_lower.split(' ', 1)
                    cmd = cmd_parts[0]
                    args = cmd_parts[1] if len(cmd_parts) > 1 else ""

                    # Comandos existentes
                    if cmd == '/status':
                        await self.notificar_status(estados, precios, indicadores_15m)

                    elif cmd == '/pause' and args:
                        symbol = args.strip().upper()
                        if self.signal_generator and symbol in self.signal_generator.states:
                            if self.signal_generator.pausar_moneda_manual(symbol, "Comando Telegram"):
                                await self.enviar_telegram(f"⏸️ <b>{symbol}</b> pausada MANUALMENTE.")
                            else:
                                await self.enviar_telegram(f"⚠️ <b>{symbol}</b> ya estaba pausada manualmente.")
                        else:
                            await self.enviar_telegram(f"❌ Símbolo <b>{symbol}</b> no encontrado.")

                    elif cmd == '/pause_all':
                        if self.signal_generator:
                            pausadas = self.signal_generator.pausar_todas_manual("Comando /pause_all")
                            await self.enviar_telegram(f"⏸️ <b>Todas pausadas:</b> {', '.join(pausadas) if pausadas else 'Ninguna'}")

                    elif cmd == '/resume' and args:
                        symbol = args.strip().upper()
                        if self.signal_generator and symbol in self.signal_generator.states:
                            if self.signal_generator.reanudar_moneda_manual(symbol):
                                await self.enviar_telegram(f"▶️ <b>{symbol}</b> reanudada MANUALMENTE.")
                            else:
                                st = self.signal_generator.states[symbol]
                                await self.enviar_telegram(f"⚠️ <b>{symbol}</b> no estaba pausada manualmente. Auto-pausada={st.moneda_pausada}")

                    elif cmd == '/resume_all':
                        if self.signal_generator:
                            reanudadas = self.signal_generator.reanudar_todas_manual()
                            await self.enviar_telegram(f"▶️ <b>Reanudadas:</b> {', '.join(reanudadas) if reanudadas else 'Ninguna estaba pausada manualmente'}")

                    elif cmd == '/list_paused':
                        if self.signal_generator:
                            pausadas = self.signal_generator.get_monedas_pausadas()
                            if not pausadas:
                                await self.enviar_telegram("✅ <b>Ninguna moneda pausada.</b>")
                            else:
                                lineas = ["<b>📋 Monedas Pausadas:</b>\n"]
                                for sym, info in pausadas.items():
                                    tipo = "MANUAL" if info['pausa_manual'] else "AUTO"
                                    razon = info.get('razon', 'N/A')
                                    tiempo = ""
                                    if info['timestamp'] > 0:
                                        mins = (datetime.now(pytz.UTC).timestamp() - info['timestamp']) / 60
                                        tiempo = f" (⏳ {mins:.0f}min)"
                                    lineas.append(f"⏸️ <b>{sym}</b> [{tipo}]{tiempo}\n   Razón: {razon}")
                                await self.enviar_telegram("\n".join(lineas))

                    # Tier 1
                    elif cmd == '/restart':
                        await self._cmd_restart()

                    elif cmd == '/stop':
                        await self._cmd_stop()

                    elif cmd == '/config':
                        await self._cmd_config()

                    elif cmd == '/logs':
                        await self._cmd_logs(args)

                    # Tier 2
                    elif cmd == '/health':
                        await self._cmd_health(precios, estados)

                    elif cmd == '/symbol' and args:
                        await self._cmd_symbol(args, precios, indicadores_1m, indicadores_15m, indicadores_4h, estados)

                    # Tier 3
                    elif cmd == '/set_threshold':
                        await self._cmd_set_threshold(args)

                    # FASE 5: Toggle grid neutral
                    elif cmd == '/grid_neutral':
                        await self._cmd_grid_neutral(args)

                    # Tier 4
                    elif cmd == '/reset_circuit' and args:
                        await self._cmd_reset_circuit(args, estados)

                    elif cmd == '/backup':
                        await self._cmd_backup()

                    # Ayuda
                    elif cmd == '/help':
                        await self.enviar_telegram(
                            "<b>🤖 COMANDOS DISPONIBLES V5.7</b>\n\n"
                            "<b>[MONIT] Monitoreo:</b>\n"
                            "<code>/status</code> — Estado actual\n"
                            "<code>/health</code> — Diagnóstico completo\n"
                            "<code>/symbol SYMBOL</code> — Detalle de moneda\n\n"
                            "<b>[FASE5] Grid Neutral:</b>\n"
                            "<code>/grid_neutral on</code> — Activar grid neutral\n"
                            "<code>/grid_neutral off</code> — Desactivar grid neutral\n"
                            "<code>/grid_neutral</code> — Ver estado\n\n"
                            "<b>[CONTROL] Control:</b>\n"
                            "<code>/pause SYMBOL</code> — Pausar moneda\n"
                            "<code>/pause_all</code> — Pausar todas\n"
                            "<code>/resume SYMBOL</code> — Reanudar moneda\n"
                            "<code>/resume_all</code> — Reanudar todas\n"
                            "<code>/list_paused</code> — Listar pausadas\n\n"
                            "<b>[SISTEMA] Sistema:</b>\n"
                            "<code>/restart</code> — Reiniciar bot (Seguro)\n"
                            "<code>/stop</code> — Detener bot (Seguro)\n"
                            "<code>/config</code> — Ver configuración\n"
                            "<code>/logs [N]</code> — Últimas N líneas de log\n"
                            "<code>/set_threshold param valor</code> — Cambiar umbral\n\n"
                            "<b>[SOS] Emergencia:</b>\n"
                            "<code>/reset_circuit SYMBOL</code> — Resetear circuit breaker\n"
                            "<code>/backup</code> — Backup nativo de SQLite\n\n"
                            "<b>📖 Leyenda de Estados:</b>\n"
                            "⚪ = Monitoreo\n"
                            "🟡 = Armed\n"
                            "🔥 = Fire\n"
                            "🟠 = Cooldown\n"
                            "💠 = Grid Neutral activo\n"
                            "⏸️[M] = Pausa Manual (solo /resume)\n"
                            "⏸️[A] = Pausa Auto (se reactiva sola)"
                        )

                    elif cmd.startswith('/'):
                        await self.enviar_telegram(f"❓ Comando <code>{cmd}</code> no reconocido. Usa <code>/help</code>")

            except Exception as e:
                print(f"  [ERROR] Error en polling Telegram: {e}")

            await asyncio.sleep(3)
