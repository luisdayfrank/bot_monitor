"""
P0-3 (alcance MÍNIMO) — Backfill one-shot de `direction` en near_miss_labels.

Motivo: la columna `direction` no existía en SQLite antes del fix P0-2. Las filas
importadas quedaron con direction=NULL aunque MySQL conserva `direccion_nm`.
Este script SOLO rellena `direction` (WHERE direction IS NULL — nunca pisa valores).
Sin ETL desde auditoria_eventos y sin tocar regime/tipo_rechazo históricos
(ver plan_reparacion_pipeline_nmi.md, sección P0-3).

Uso (en el entorno del bot, con MySQL accesible):
    python backfill_nmi_direction.py                       # aplica
    python backfill_nmi_direction.py --dry-run             # solo muestra
    python backfill_nmi_direction.py --db otra.db          # ruta SQLite distinta
"""
import argparse
import asyncio
import sqlite3

from database_v5 import cargar_near_miss_seguimientos_finalizados


async def main():
    ap = argparse.ArgumentParser(description="Backfill de direction en near_miss_labels")
    ap.add_argument("--db", default="crypto_learning.db")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Misma fuente que usa NearMissOutcomeImporter (MySQL near_miss_seguimientos
    # finalizados; la exclusión A8 de filas planas ya se aplica en la consulta).
    rows = await cargar_near_miss_seguimientos_finalizados(limite=10000)
    conn = sqlite3.connect(args.db, timeout=60.0)

    actualizados = 0
    sin_fila_sqlite = 0
    sin_direccion = 0
    for r in rows:
        sid = r["id"]
        dir_nm = r.get("direccion_nm")
        if not dir_nm:
            sin_direccion += 1
            continue
        existe = conn.execute(
            "SELECT 1 FROM near_miss_labels WHERE seguimiento_id = ?", (sid,)
        ).fetchone()
        if not existe:
            sin_fila_sqlite += 1
            continue
        if args.dry_run:
            print(f"  [DRY] seguimiento_id={sid} direction -> {dir_nm}")
        else:
            conn.execute(
                "UPDATE near_miss_labels SET direction = ? "
                "WHERE seguimiento_id = ? AND direction IS NULL",
                (dir_nm, sid),
            )
        actualizados += 1

    if not args.dry_run:
        conn.commit()
    pendientes = conn.execute(
        "SELECT COUNT(*) FROM near_miss_labels WHERE direction IS NULL"
    ).fetchone()[0]
    conn.close()

    prefijo = "[DRY-RUN] " if args.dry_run else ""
    print(f"✅ {prefijo}Backfill direction: {actualizados} filas procesadas | "
          f"{sin_fila_sqlite} seguimientos MySQL sin fila en SQLite | "
          f"{sin_direccion} sin direccion_nm | "
          f"pendientes direction NULL: {pendientes}")


if __name__ == "__main__":
    asyncio.run(main())
