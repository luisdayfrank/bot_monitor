import asyncio
import logging
import os
import shutil
from datetime import datetime, timedelta
from plyer import notification
from telegram import Bot, Update, InputFile
from telegram.constants import ParseMode
from config import CONFIG

class Notifier:
    def __init__(self):
        self.bot = Bot(token=CONFIG.telegram_token) if CONFIG.telegram_token else None
        self._last_update_id = 0
        self.signal_generator = None
        self._ws_last_msg_time = datetime.utcnow()
        self._ws_connected = False
        self._process_start_time = datetime.utcnow()

    # ═══════════════════════════════════════════════════════════════════════════════
    # FIX CRÍTICO: Limpiar mensajes pendientes de Telegram al iniciar
    # ═══════════════════════════════════════════════════════════════════════════════
    async def limpiar_updates_pendientes(self):
        """
        Al iniciar el bot, limpia todos los mensajes pendientes de Telegram
        para evitar que comandos antiguos (como /restart) se reprocesen.

        Esto es CRÍTICO porque si el bot murió con os._exit(42) sin hacer
        acknowledge del /restart, Telegram seguirá enviando ese mensaje.
        """
        if not self.bot:
            return

        print("  🧹 Limpiando mensajes pendientes de Telegram...")
        try:
            # Obtener todos los updates pendientes (sin offset = desde el principio)
            updates = await self.bot.get_updates(limit=100, timeout=1)
            if updates:
                # Actualizar offset al último update recibido
                self._last_update_id = max(u.update_id for u in updates)
                print(f"  ✅ {len(updates)} mensajes pendientes limpiados. Offset: {self._last_update_id}")
            else:
                print("  ✅ Sin mensajes pendientes.")
        except Exception as e:
            print(f"  ⚠️ Error limpiando updates: {e}")

    async def enviar_telegram(self, mensaje: str):
        if self.bot and CONFIG.telegram_chat_id:
            try:
                await self.bot.send_message(
                    chat_id=CONFIG.telegram_chat_id,
                    text=mensaje,
                    parse_mode=ParseMode.HTML
                )
                print(f"  Telegram enviado: {mensaje[:50]}...")
            except Exception as e:
                logging.error(f"Error enviando Telegram: {e}")
                print(f"  Error Telegram: {e}")

    async def enviar_archivo_telegram(self, filepath: str, caption: str = ""):
        if not self.bot or not CONFIG.telegram_chat_id:
            print(f"  ⚠️ Telegram no configurado, no se envió archivo")
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
            print(f"  ❌ Error enviando archivo: {e}")
            return False

    def enviar_local(self, titulo: str, mensaje: str):
        try:
            notification.notify(title=titulo, message=mensaje, app_name="Crypto Monitor V5", timeout=10)
        except Exception as e:
            logging.error(f"Error notificacion local: {e}")

    async def procesar_alerta(self, evento: dict):
        symbol = evento['symbol']
        tipo = evento['tipo']
        dir_ = evento['direction']
        score = evento['score']
        price = evento['price']
        estado = evento.get('estado_maquina', 'UNKNOWN')

        if tipo == 'FIRE':
            titulo = f"SNIPER {symbol} | {dir_}"
            p = evento['params']
            auto_comp = "📐 AUTO-COMPRIMIDO\n" if p.get('auto_compressed') else ""
            pos_ext = "⚠️ Posición extrema en rango\n" if p.get('posicion_extrema') else ""
            msg = (
                f"<b>🔥 DISPARO {dir_} - {symbol}</b>\n"
                f"Estado: <b>{estado}</b>\n"
                f"Precio: ${price:.4f} | Score: {score}/100\n\n"
                f"{auto_comp}"
                f"{pos_ext}"
                f"<b>Grid Config:</b>\n"
                f"Rango: ${p['lower_limit']} - ${p['upper_limit']}\n"
                f"Grids: {p['grid_count']}"
            )
            if p.get('auto_compressed'):
                msg += f" (comprimido de más)"
            msg += (
                f"\nPaso: {p['step_pct']}%\n"
                f"Apalancamiento: {p['apalancamiento_sugerido']}x\n"
                f"Posicion: {p['posicion_en_rango']:.0%}\n"
                f"Breakeven: {p['breakeven_pct']}%\n"
                f"Margen: +{p['margen_sobre_breakeven']:.3f}%"
            )
        elif tipo == 'RECHAZADO':
            titulo = f"RECHAZADO {symbol}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Condiciones no cumplidas"
            msg = (
                f"<b>❌ DISPARO RECHAZADO - {symbol}</b>\n"
                f"Dirección: {dir_}\n"
                f"Precio: ${price:.4f}\n"
                f"Motivo: <i>{motivos}</i>"
            )
        elif tipo == 'CIRCUIT_BREAKER':
            titulo = f"CIRCUIT BREAKER {symbol}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Pérdidas consecutivas"
            msg = (
                f"<b>🔒 CIRCUIT BREAKER - {symbol}</b>\n"
                f"Motivo: <i>{motivos}</i>\n"
                f"Pausa: {CONFIG.circuit_breaker_pausa_seg // 60} minutos\n"
                f"Capital reducido al 50% para próximos disparos"
            )
        elif tipo == 'ARMED':
            titulo = f"ARMED {symbol} | {dir_}"
            msg = f"<b>🟡 {symbol}</b> filtro macro activado. Esperando gatillo {dir_}..."
        else:
            titulo = f"ALERTA {symbol} | {tipo}"
            motivos = ", ".join(evento['rechazos']) if evento.get('rechazos') else "Cambio de condiciones."
            msg = (
                f"<b>{symbol} - ALERTA</b>\n"
                f"Tipo: {tipo}\n"
                f"Motivos: <i>{motivos}</i>"
            )
        self.enviar_local(titulo, "Revisa Telegram para detalles.")
        await self.enviar_telegram(msg)

    async def notificar_online(self, symbols: list, uptime_start: float = None):
        uptime_str = ""
        if uptime_start:
            elapsed = datetime.utcnow().timestamp() - uptime_start
            uptime_str = f"\n⏱️ Tiempo de arranque: {elapsed:.1f}s"
        msg = (
            f"<b>🚀 Crypto Monitor V5.1 ONLINE</b>\n"
            f"📡 Monitoreando {len(symbols)} activos{uptime_str}\n\n"
            f"<b>Activos:</b> {', '.join(symbols)}"
        )
        await self.enviar_telegram(msg)

    async def notificar_resumen(self, estados: dict, precios: dict, indicadores_15m: dict):
        lineas = ["<b>📊 RESUMEN DE MONEDAS</b>\n"]
        for symbol in sorted(estados.keys()):
            st = estados[symbol]
            precio = precios.get(symbol, 0)
            i15 = indicadores_15m.get(symbol, {})
            estado_icon = {'FIRE': '🔥', 'ARMED': '🟡', 'COOLDOWN': '🟠', 'MONITOREO': '⚪'}.get(st.estado, '⚫')
            dir_icon = '🟢 LONG' if st.direccion_filtro == 'LONG' else ('🔴 SHORT' if st.direccion_filtro == 'SHORT' else '⚪ NEUTRAL')
            rsi = i15.get('rsi', '--')
            adx = i15.get('adx', '--')
            pausa_info = ""
            if getattr(st, 'moneda_pausada_manual', False):
                pausa_info = " ⏸️[M]"
            elif getattr(st, 'moneda_pausada', False):
                pausa_info = " ⏸️[A]"
            lineas.append(
                f"{estado_icon} <b>{symbol}</b>{pausa_info} | {st.estado} | {dir_icon}\n"
                f"   💰 ${precio:.4f} | RSI: {rsi if isinstance(rsi, str) else f'{rsi:.1f}'} | ADX: {adx if isinstance(adx, str) else f'{adx:.1f}'}"
            )
        msg = "\n".join(lineas)
        await self.enviar_telegram(msg)

    async def notificar_status(self, estados: dict, precios: dict, indicadores_15m: dict):
        lineas = ["<b>📊 ESTADO ACTUAL</b>\n"]
        ahora = datetime.utcnow().strftime("%H:%M:%S UTC")
        lineas.append(f"🕐 {ahora}\n")
        for symbol in sorted(estados.keys()):
            st = estados[symbol]
            precio = precios.get(symbol, 0)
            i15 = indicadores_15m.get(symbol, {})
            estado_icon = {'FIRE': '🔥', 'ARMED': '🟡', 'COOLDOWN': '🟠', 'MONITOREO': '⚪'}.get(st.estado, '⚫')
            dir_str = st.direccion_filtro or 'NEUTRAL'
            rsi = i15.get('rsi', '--')
            adx = i15.get('adx', '--')
            cb_info = ""
            if hasattr(st, 'circuit_breaker_activo') and st.circuit_breaker_activo:
                cb_info = " 🔒CB"
            pausa_info = ""
            if getattr(st, 'moneda_pausada_manual', False):
                pausa_info = " ⏸️MANUAL"
            elif getattr(st, 'moneda_pausada', False):
                pausa_info = " ⏸️AUTO"
            lineas.append(
                f"{estado_icon} <b>{symbol}</b>{cb_info}{pausa_info} | {st.estado} | {dir_str}\n"
                f"   💰 ${precio:.4f} | RSI15m: {rsi if isinstance(rsi, str) else f'{rsi:.1f}'} | ADX: {adx if isinstance(adx, str) else f'{adx:.1f}'}"
            )
        msg = "\n".join(lineas)
        await self.enviar_telegram(msg)

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 1: COMANDOS OBLIGATORIOS PARA VPS
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_restart(self):
        """/restart — Reinicio controlado del bot."""
        # FIX: Hacer acknowledge del update ANTES de salir
        await self.enviar_telegram(
            "🔄 <b>REINICIANDO BOT...</b>\n"
            "Guardando estado, cerrando conexiones..."
        )
        # Pequeña espera para que Telegram procese el acknowledge
        await asyncio.sleep(1)
        print("🔄 Comando /restart recibido. Saliendo con código 42...")
        os._exit(42)

    async def _cmd_stop(self):
        """/stop — Apagado seguro."""
        await self.enviar_telegram(
            "🛑 <b>DETENIENDO BOT...</b>\n"
            "Cerrando conexiones WS, guardando auditoría..."
        )
        await asyncio.sleep(1)
        print("🛑 Comando /stop recibido. Saliendo...")
        os._exit(0)

    async def _cmd_config(self):
        """/config — Ver configuración actual."""
        msg = (
            f"<b>⚙️ CONFIGURACIÓN ACTUAL</b>\n\n"
            f"<b>Monedas:</b> {', '.join(CONFIG.symbols)} ({len(CONFIG.symbols)}/10)\n"
            f"<b>Timeframes:</b> {CONFIG.tf_micro} / {CONFIG.tf_primary} / {CONFIG.tf_macro}\n"
            f"<b>Zona horaria:</b> {CONFIG.timezone}\n"
            f"<b>Modo auditoría:</b> {'✅ ON' if CONFIG.modo_auditoria else '❌ OFF'}\n"
            f"<b>Heartbeat:</b> {'✅ ON' if CONFIG.heartbeat_debug else '❌ OFF'} ({CONFIG.heartbeat_intervalo_min}min)\n\n"
            f"<b>Umbrales:</b>\n"
            f"  ADX ideal: {CONFIG.adx_ideal}\n"
            f"  ADX reject: {CONFIG.adx_reject}\n"
            f"  RSI micro short: {CONFIG.rsi_micro_short_trigger}\n"
            f"  RSI micro long: {CONFIG.rsi_micro_long_trigger}\n"
            f"  ATR min: {CONFIG.atr_min_pct}% | max: {CONFIG.atr_max_pct}%\n\n"
            f"<b>Grid:</b>\n"
            f"  Min grids: {CONFIG.grid_min_grids}\n"
            f"  Max grids: {CONFIG.grid_max_grids_hard}\n"
            f"  Capital default: ${CONFIG.grid_default_capital}\n"
            f"  Leverage default: {CONFIG.grid_default_leverage}x\n\n"
            f"<b>Circuit Breaker:</b>\n"
            f"  Disparos: {CONFIG.circuit_breaker_disparos}\n"
            f"  Pausa: {CONFIG.circuit_breaker_pausa_seg//60}min\n"
            f"  Reducción capital: {CONFIG.circuit_breaker_reduccion_capital*100:.0f}%"
        )
        await self.enviar_telegram(msg)

    async def _cmd_logs(self, args: str):
        """/logs [N] — Últimas N líneas del log."""
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
                f"<b>📋 ÚLTIMAS {len(last_lines)} LÍNEAS DE LOG</b>\n"
                f"<pre>{content}</pre>"
            )
        except Exception as e:
            await self.enviar_telegram(f"❌ Error leyendo logs: {e}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 2: COMANDOS DE MONITOREO AVANZADO
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_health(self, precios_vivo: dict, signal_states: dict):
        """/health — Diagnóstico completo."""
        uptime = datetime.utcnow() - self._process_start_time
        uptime_str = f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m"

        estados_count = {}
        pausadas_manual = 0
        pausadas_auto = 0
        for s in signal_states.values():
            estados_count[s.estado] = estados_count.get(s.estado, 0) + 1
            if getattr(s, 'moneda_pausada_manual', False):
                pausadas_manual += 1
            elif getattr(s, 'moneda_pausada', False):
                pausadas_auto += 1

        db_size = 0
        if os.path.exists(CONFIG.db_path):
            db_size = os.path.getsize(CONFIG.db_path) / (1024*1024)

        msg = (
            f"<b>🏥 HEALTH CHECK</b>\n\n"
            f"<b>Proceso:</b>\n"
            f"  Uptime: {uptime_str}\n"
            f"  Precios activos: {len(precios_vivo)}/{len(CONFIG.symbols)}\n\n"
            f"<b>Estados máquina:</b>\n"
            f"  🔥 FIRE: {estados_count.get('FIRE', 0)}\n"
            f"  🎯 ARMED: {estados_count.get('ARMED', 0)}\n"
            f"  🟠 COOLDOWN: {estados_count.get('COOLDOWN', 0)}\n"
            f"  ⚪ MONITOREO: {estados_count.get('MONITOREO', 0)}\n"
            f"  ⏸️ Pausadas manual: {pausadas_manual}\n"
            f"  ⏸️ Pausadas auto: {pausadas_auto}\n\n"
            f"<b>Base de datos:</b>\n"
            f"  Tamaño: {db_size:.1f} MB\n"
            f"  Path: {CONFIG.db_path}\n\n"
            f"<b>WebSocket:</b>\n"
            f"  Último mensaje: hace {(datetime.utcnow()-self._ws_last_msg_time).seconds}s\n"
            f"  Estado: {'✅ Conectado' if self._ws_connected else '❌ Desconectado'}"
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
            remaining = (st.circuit_breaker_hasta - int(datetime.utcnow().timestamp()*1000)) // 1000
            cb_info = f"\n🔒 <b>Circuit Breaker</b> | {remaining}s restantes"

        msg = (
            f"<b>📊 {symbol} — DETALLE COMPLETO</b>{pausa_info}{cb_info}\n\n"
            f"<b>Precio:</b> ${precio if isinstance(precio, str) else f'{precio:.4f}'}\n"
            f"<b>Estado máquina:</b> {st.estado if st else 'N/A'}\n"
            f"<b>Dirección filtro:</b> {st.direccion_filtro if st else 'N/A'}\n"
            f"<b>Score macro:</b> {st.score_macro_actual if st else 'N/A'}\n"
            f"<b>Score último disparo:</b> {st.ultimo_score if st else 'N/A'}\n\n"
            f"<b>Indicadores 1m:</b>\n"
            f"  RSI(7): {i1m.get('rsi_7', 'N/A')}\n"
            f"  EMA300: {i1m.get('ema_300', 'N/A')}\n"
            f"  ATR(1m): {i1m.get('atr_1m', 'N/A')}\n"
            f"  Mecha SHORT: {'✅' if i1m.get('mecha_valida_short') else '❌'}\n"
            f"  Mecha LONG: {'✅' if i1m.get('mecha_valida_long') else '❌'}\n\n"
            f"<b>Indicadores 15m:</b>\n"
            f"  RSI(14): {i15.get('rsi', 'N/A')}\n"
            f"  ADX(14): {i15.get('adx', 'N/A')}\n"
            f"  ATR(15m): {i15.get('atr', 'N/A')}\n"
            f"  MACD hist: {i15.get('macd_hist', 'N/A')}\n"
            f"  EMA200: {i15.get('ema200_15m', 'N/A')}\n\n"
            f"<b>Indicadores 4h:</b>\n"
            f"  EMA200: {i4h.get('ema200_4h', 'N/A')}"
        )
        await self.enviar_telegram(msg)

    # ═══════════════════════════════════════════════════════════════════════════════
    # TIER 3: CONFIGURACIÓN EN CALIENTE
    # ═══════════════════════════════════════════════════════════════════════════════

    async def _cmd_set_threshold(self, args: str):
        """/set_threshold <param> <valor> — Cambiar umbrales sin reiniciar."""
        parts = args.strip().split()
        if len(parts) != 2:
            await self.enviar_telegram(
                "❌ Uso: <code>/set_threshold parametro valor</code>\n"
                "Ejemplos:\n"
                "<code>/set_threshold adx_reject 50</code>\n"
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
            f"Capital restaurado a ${CONFIG.grid_default_capital}\n"
            f"Moneda lista para operar."
        )

    async def _cmd_backup(self):
        """/backup — Forzar backup de la base de datos."""
        if not os.path.exists(CONFIG.db_path):
            await self.enviar_telegram("❌ Base de datos no encontrada.")
            return

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = f"{backup_dir}/crypto_monitor_{timestamp}.db"

        try:
            shutil.copy2(CONFIG.db_path, backup_path)
            size_mb = os.path.getsize(backup_path) / (1024*1024)
            await self.enviar_telegram(
                f"💾 <b>BACKUP CREADO</b>\n"
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

        # ═══════════════════════════════════════════════════════════════════════════════
        # FIX CRÍTICO #1: Limpiar mensajes pendientes ANTES de empezar el polling
        # ═══════════════════════════════════════════════════════════════════════════════
        await self.limpiar_updates_pendientes()

        print("  🤖 Polling de comandos Telegram iniciado (cada 3s)")
        print("  📋 Comandos: /status, /pause, /resume, /list_paused, /restart, /stop, /config, /logs, /health, /symbol, /set_threshold, /reset_circuit, /backup, /help")

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

                    # ─── Comandos existentes ───
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
                                        mins = (datetime.utcnow().timestamp() - info['timestamp']) / 60
                                        tiempo = f" ({mins:.0f}min)"
                                    lineas.append(f"⏸️ <b>{sym}</b> [{tipo}]{tiempo}\n   Razón: {razon}")
                                await self.enviar_telegram("\n".join(lineas))

                    # ─── Tier 1 ───
                    elif cmd == '/restart':
                        await self._cmd_restart()

                    elif cmd == '/stop':
                        await self._cmd_stop()

                    elif cmd == '/config':
                        await self._cmd_config()

                    elif cmd == '/logs':
                        await self._cmd_logs(args)

                    # ─── Tier 2 ───
                    elif cmd == '/health':
                        await self._cmd_health(precios, estados)

                    elif cmd == '/symbol' and args:
                        await self._cmd_symbol(args, precios, indicadores_1m, indicadores_15m, indicadores_4h, estados)

                    # ─── Tier 3 ───
                    elif cmd == '/set_threshold':
                        await self._cmd_set_threshold(args)

                    # ─── Tier 4 ───
                    elif cmd == '/reset_circuit' and args:
                        await self._cmd_reset_circuit(args, estados)

                    elif cmd == '/backup':
                        await self._cmd_backup()

                    # ─── Ayuda ───
                    elif cmd == '/help':
                        await self.enviar_telegram(
                            "<b>🤖 COMANDOS DISPONIBLES</b>\n\n"
                            "<b>🎛️ Control:</b>\n"
                            "<code>/status</code> — Estado actual\n"
                            "<code>/restart</code> — Reiniciar bot\n"
                            "<code>/stop</code> — Detener bot\n"
                            "<code>/config</code> — Ver configuración\n"
                            "<code>/logs [N]</code> — Últimas N líneas de log\n\n"
                            "<b>⏸️ Pausa:</b>\n"
                            "<code>/pause SYMBOL</code> — Pausar moneda\n"
                            "<code>/pause_all</code> — Pausar todas\n"
                            "<code>/resume SYMBOL</code> — Reanudar moneda\n"
                            "<code>/resume_all</code> — Reanudar todas\n"
                            "<code>/list_paused</code> — Listar pausadas\n\n"
                            "<b>📊 Monitoreo:</b>\n"
                            "<code>/health</code> — Diagnóstico completo\n"
                            "<code>/symbol SYMBOL</code> — Detalle de moneda\n\n"
                            "<b>⚙️ Config:</b>\n"
                            "<code>/set_threshold param valor</code> — Cambiar umbral\n\n"
                            "<b>🚨 Emergencia:</b>\n"
                            "<code>/reset_circuit SYMBOL</code> — Resetear circuit breaker\n"
                            "<code>/backup</code> — Backup de base de datos\n\n"
                            "<b>Leyenda pausas:</b>\n"
                            "⏸️[M] = Manual (solo /resume)\n"
                            "⏸️[A] = Auto (se reactiva sola si score>=50)"
                        )

                    elif cmd.startswith('/'):
                        await self.enviar_telegram(f"❓ Comando <code>{cmd}</code> no reconocido. Usa <code>/help</code>")

            except Exception as e:
                print(f"  ⚠️ Error en polling Telegram: {e}")

            await asyncio.sleep(3)
