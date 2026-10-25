# Bot Monitor — Base de datos MySQL / MariaDB

Este bot fue migrado de **SQLite** a **MySQL / MariaDB**. Ya no usa `aiosqlite`
ni `sqlite3`; toda la persistencia pasa por **`aiomysql`** (driver async). El
código de negocio no cambió: se conservaron las mismas funciones y estructura.

---

## 1. Requisitos

- **MariaDB** (recomendado) o **MySQL** corriendo en tu PC.
- Python 3.10+ con las dependencias: `pip install -r requirements.txt`
  (lo esencial nuevo: `aiomysql` y `pymysql`).

---

## 2. Preparar la base de datos (una sola vez)

Conéctate a tu MariaDB/MySQL como administrador y crea la base de datos y un
usuario dedicado (no uses root en producción):

```sql
CREATE DATABASE IF NOT EXISTS crypto_monitor
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'bot'@'localhost' IDENTIFIED BY 'TU_PASSWORD';
GRANT ALL PRIVILEGES ON crypto_monitor.* TO 'bot'@'localhost';
FLUSH PRIVILEGES;
```

> El esquema (tablas e índices) lo crea el bot automáticamente en el arranque
> (`init_db()`), incluida la migración de columnas de versiones anteriores.

---

## 3. Configurar la conexión

La conexión se define por **variables de entorno** (para no exponer credenciales
en el código). Valores por defecto en `config.py`:

| Variable             | Descripción              | Por defecto     |
|----------------------|--------------------------|-----------------|
| `DB_HOST`            | Host                     | `127.0.0.1`     |
| `DB_PORT`            | Puerto                   | `3306`          |
| `DB_USER`            | Usuario                  | `root`          |
| `DB_PASSWORD`        | Contraseña               | *(vacía)*       |
| `DB_NAME`            | Base de datos            | `crypto_monitor`|
| `DB_CONNECT_TIMEOUT` | Timeout de conexión (s)  | `10`            |
| `AUDIT_EXPORT_DIR`   | Carpeta de reportes JSON | `auditoria`     |

Ejemplo antes de arrancar el bot:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=bot
export DB_PASSWORD=TU_PASSWORD
export DB_NAME=crypto_monitor

python main.py
```

Si prefieres no usar variables de entorno, edita los valores por defecto en
`config.py` (bloque `# ─── Base de datos MySQL / MariaDB ───`).

---

## 4. Backup

El comando `/backup` (Telegram) ahora genera un volcado con **`mysqldump`**
(archivo `.sql` en `backups/`). Requiere tener las herramientas de cliente
instaladas (en Debian/Ubuntu: `sudo apt install mariadb-client`).

---

## 5. Migración desde una base SQLite antigua

- Las tablas/columnas se crean solas. El bot ejecuta una **migración de
  columnas** idempotente al arrancar, por lo que una base con esquema antiguo se
  actualiza sin intervención manual.
- **Los datos antiguos de SQLite no se convierten automáticamente.** Si
  necesitas conservarlos, habría que volcarlos (p. ej. con un script de copia
  tabla a tabla). Dime si quieres que genere ese script de importación.

---

## 6. Notas técnicas (para quien revise el código)

- `database_v5.py` incluye una **capa de compatibilidad** (`_CompatConnection`,
  `Row`) con la misma interfaz que tenía `aiosqlite`, de modo que las ~50
  funciones existentes quedan intactas.
- Traducción centralizada y segura (conserva **parametrización real**, sin
  inyección SQL):
  - placeholders `?` → `%s`
  - `%` literales (`LIKE`, `DATE_FORMAT`) escapados para el formateo del driver
  - `INSERT OR REPLACE` → `REPLACE`, `INSERT OR IGNORE` → `INSERT IGNORE`
  - `last_insert_rowid()` → `LAST_INSERT_ID()`
  - `CREATE INDEX IF NOT EXISTS` → portable entre MySQL y MariaDB
- Funciones de fecha SQLite → MySQL: `datetime(x,'unixepoch')` →
  `FROM_UNIXTIME(x)`, `date(...)` → `DATE(...)`, `strftime` → `DATE_FORMAT`.
  La sesión se fija en **UTC** para preservar la semántica original.
- Las `FOREIGN KEY` no se aplican (`FOREIGN_KEY_CHECKS=0`), igual que antes.
- Las pruebas automatizadas están en `tests/test_db.py` (crea tablas, inserta y
  consulta contra una base real).
