from core.calculations import calcula_operacoes_descarga_tanques
import sqlite3
import os
import threading
from datetime import datetime, timedelta
from config import CLEANUP_DAYS, DB_FILE  # DB_FILE still used for initial path
import csv

# ============================================================
# GLOBALS (minimal additions)
# ============================================================

GLOBAL_CONN = None
DB_LOCK = threading.Lock()


def get_global_conn():
    global GLOBAL_CONN

    if GLOBAL_CONN is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_dir, "..", DB_FILE) if not os.path.isabs(DB_FILE) else DB_FILE
        db_path = os.path.abspath(db_path)

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        GLOBAL_CONN = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        GLOBAL_CONN.execute("PRAGMA journal_mode=WAL;")
        GLOBAL_CONN.execute("PRAGMA synchronous=NORMAL;")

    return GLOBAL_CONN


# ============================================================
# INIT DB
# ============================================================

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            device TEXT,
            tag TEXT,
            value REAL
        )
    """)

  # --- Operations table (detected discharge events) ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS dosagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_start TEXT,
            device TEXT,
            tipo TEXT,
            destino TEXT,
            qnt_solicitada REAL,
            peso_inicio REAL,
            peso_fim REAL,
            peso_real REAL,
            created_at TEXT,
            UNIQUE(device, timestamp_start, tipo, destino)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS masseira_daily (
            date TEXT NOT NULL,
            device TEXT NOT NULL,
            energia_kWh REAL,
            horas_operacao REAL,
            corrente_max REAL,
            first_ts TEXT,
            last_ts TEXT,
            samples INTEGER,
            updated_at TEXT,
            PRIMARY KEY (date, device)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# READ-ONLY CONNECTION (unchanged)
# ============================================================

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=20)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# ============================================================
# CLEANUP — switched to safe write
# ============================================================

def cleanup_db():
    conn = get_global_conn()
    c = conn.cursor()

    cut_off = (datetime.now() - timedelta(days=CLEANUP_DAYS)).isoformat()
    print(f"[CLEANUP] Deletando registros de 'readings' anteriores a {cut_off}...")

    try:
        with DB_LOCK:
            c.execute("DELETE FROM readings WHERE timestamp < ?", (cut_off,))
            deleted_count = c.rowcount
            conn.commit()
        print(f"[CLEANUP] {deleted_count} registros antigos deletados com sucesso.")
    except Exception as e:
        print(f"[CLEANUP ERROR] Falha ao deletar registros antigos: {e}")


# ============================================================
# SAVE TO SQLITE — NOW THREAD-SAFE
# ============================================================

def save_to_sqlite(device_name, result):
    conn = get_global_conn()
    c = conn.cursor()

    timestamp = datetime.now().replace(microsecond=0).isoformat()

    data_to_insert = [
        (timestamp, device_name, tag, value)
        for tag, value in result.items()
    ]

    with DB_LOCK:
        c.executemany(
            "INSERT INTO readings (timestamp, device, tag, value) VALUES (?, ?, ?, ?)",
            data_to_insert
        )
        conn.commit()


# ============================================================
# INDEXES — safe but kept local (unchanged logic)
# ============================================================

def ensure_indexes():
    conn = get_global_conn()
    cur = conn.cursor()

    with DB_LOCK:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_masseiras_cover_dev
            ON readings(device, timestamp, tag, value)
            WHERE tag IN ('OutputPower','OutputFrequency','CurrentMagnitude')
            AND device IN ('Masseira_1', 'Masseira_2')
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_readings_tanque_ops_cover_dev
            ON readings(device, timestamp, tag, value)
            WHERE tag IN ('Descarga selecionada','Operacao em andamento','Botão Liga',
                        'Valv. Desc. Mass. 1','Valv. Desc. Mass. 2',
                        'Qnt. Solicitada (descarga)','Peso')
            AND device IN ('Tanque_1_Agua', 'Tanque_2_Agua', 'Tanque_1_Resina', 'Tanque_2_Resina')
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_readings_device_tag_ts ON readings(device, tag, timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_readings_device_tag_ts_value ON readings(device, tag, timestamp, value)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dosagens_device_ts ON dosagens(device, timestamp_start)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_masseira_daily_device_date ON masseira_daily(device, date)")
        conn.commit()


# ============================================================
# CSV SAVE (unchanged)
# ============================================================

def save_to_csv(device_name, result, folder="data"):
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{device_name}.csv")
    timestamp = datetime.now().isoformat()
    fieldnames = ["timestamp"] + list(result.keys())
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        row = {"timestamp": timestamp}
        row.update(result)
        writer.writerow(row)


# ============================================================
# META TABLE — using safe writes
# ============================================================

def _get_meta_value(key: str, conn=None):
    close_after = False
    if conn is None:
        conn = get_db_connection()  # read-only allowed
        close_after = True

    c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = c.fetchone()
    if close_after:
        conn.close()
    return row[0] if row else None


def _set_meta_value(key: str, value: str, conn=None):
    if conn:
        c = conn.cursor()
        with DB_LOCK:
            c.execute("""
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, value))
            conn.commit()
    else:
        conn2 = get_global_conn()
        c2 = conn2.cursor()
        with DB_LOCK:
            c2.execute("""
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, value))
            conn2.commit()


# ============================================================
# DOSAGENS — only INSERT modified to safe write
# ============================================================

def _insert_dosagens(ops_dict, tipo):
    conn = get_global_conn()
    c = conn.cursor()

    inserted = 0
    for dev, destinos in (ops_dict or {}).items():
        for destino, lst in destinos.items():
            for op in lst:
                ts_start = op.get("horario")
                if not ts_start:
                    continue

                # skip if already exists (idempotency)
                c.execute("SELECT COUNT(*) FROM dosagens WHERE device=? AND timestamp_start=?", (dev, ts_start))
                if c.fetchone()[0] > 0:
                    continue

                peso_inicio = op.get("peso_inicio")
                peso_fim = op.get("peso_fim")
                peso_real = (float(peso_inicio or 0) - float(peso_fim or 0))

                with DB_LOCK:
                    c.execute("""
                        INSERT INTO dosagens (
                            timestamp_start, device, tipo, destino,
                            qnt_solicitada, peso_inicio, peso_fim, peso_real, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ts_start,
                        dev,
                        tipo,
                        destino,
                        op.get("qnt_solicitada"),
                        peso_inicio,
                        peso_fim,
                        peso_real,
                        datetime.now().isoformat(),
                    ))
                    conn.commit()
                inserted += 1

    return inserted


# ============================================================
# DOSAGENS UPDATE (unchanged except safe write usage)
# ============================================================

def update_dosagens_table():
    print("[dosagens_updater] Starting batch update...")

    last_ts = _get_meta_value("last_ops_ts")
    start_ts = last_ts if last_ts else "1970-01-01T00:00:00"
    end_ts = datetime.now().isoformat()

    ops_resina = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Resina")
    ops_agua   = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Agua")

    count_res = _insert_dosagens(ops_resina, "Resina")
    count_agua = _insert_dosagens(ops_agua, "Agua")

    _set_meta_value("last_ops_ts", end_ts)

    total = count_res + count_agua
    print(f"[dosagens_updater] Inserted {total} new dosagens (Resina: {count_res}, Agua: {count_agua}).")
    return total


# ============================================================
# MASSEIRA DAILY — only upserts use safe writes
# ============================================================

def _clip_5min_expr():
    return """
        CASE
            WHEN prev_ts IS NULL THEN 0.0
            ELSE
                CASE
                    WHEN ((strftime('%s', timestamp) - strftime('%s', prev_ts)) / 3600.0) <= (5.0/60.0)
                    THEN ((strftime('%s', timestamp) - strftime('%s', prev_ts)) / 3600.0)
                    ELSE 0
                END
        END
    """


def _upsert_masseira_daily_row(day, device, energia, horas, corrmax, first_ts, last_ts, samples):
    conn = get_global_conn()

    with DB_LOCK:
        c = conn.cursor()
        c.execute("""
            INSERT INTO masseira_daily
                (date, device, energia_kWh, horas_operacao, corrente_max, first_ts, last_ts, samples, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, device) DO UPDATE SET
                energia_kWh   = COALESCE(masseira_daily.energia_kWh, 0) + COALESCE(excluded.energia_kWh, 0),
                horas_operacao= COALESCE(masseira_daily.horas_operacao, 0) + COALESCE(excluded.horas_operacao, 0),
                corrente_max  = CASE
                    WHEN masseira_daily.corrente_max IS NULL THEN excluded.corrente_max
                    WHEN excluded.corrente_max IS NULL THEN masseira_daily.corrente_max
                    WHEN excluded.corrente_max > masseira_daily.corrente_max THEN excluded.corrente_max
                    ELSE masseira_daily.corrente_max END,
                first_ts      = COALESCE(masseira_daily.first_ts, excluded.first_ts),
                last_ts       = CASE
                    WHEN masseira_daily.last_ts IS NULL THEN excluded.last_ts
                    WHEN excluded.last_ts IS NULL THEN masseira_daily.last_ts
                    WHEN excluded.last_ts > masseira_daily.last_ts THEN excluded.last_ts
                    ELSE masseira_daily.last_ts END,
                samples       = COALESCE(masseira_daily.samples, 0) + COALESCE(excluded.samples, 0),
                updated_at    = excluded.updated_at
        """, (day, device, energia, horas, corrmax, first_ts, last_ts, samples, datetime.now().isoformat()))
        conn.commit()


def update_masseira_daily():
    print("[masseira_daily] Starting batch update...")

    DEVICES = ("Masseira_1", "Masseira_2")
    TAGS = ("OutputPower","OutputFrequency","CurrentMagnitude")

    conn_ro = get_db_connection()  # read-only safe
    c = conn_ro.cursor()

    end_ts = datetime.now().isoformat()
    clip_expr = _clip_5min_expr()

    total_rows = 0

    for dev in DEVICES:
        meta_key = f"last_energy_ts_{dev}"
        start_ts = _get_meta_value(meta_key, conn=conn_ro)
        if not start_ts:
            start_ts = "1970-01-01T00:00:00"

        query = f"""
        WITH pivot AS (
            SELECT
                device,
                timestamp,
                strftime('%Y-%m-%d', timestamp) AS day,
                MAX(CASE WHEN tag='OutputPower'      THEN value END) AS power,
                MAX(CASE WHEN tag='OutputFrequency'  THEN value END) AS freq,
                MAX(CASE WHEN tag='CurrentMagnitude' THEN value END) AS curr_mag
            FROM readings
            WHERE device = ?
              AND tag IN ('OutputPower','OutputFrequency','CurrentMagnitude')
              AND timestamp BETWEEN ? AND ?
            GROUP BY timestamp, device
        ),
        seq AS (
            SELECT
                device, day, timestamp, power, freq, curr_mag,
                LAG(power)     OVER (PARTITION BY device ORDER BY timestamp) AS prev_power,
                LAG(freq)      OVER (PARTITION BY device ORDER BY timestamp) AS prev_freq,
                LAG(timestamp) OVER (PARTITION BY device ORDER BY timestamp) AS prev_ts
            FROM pivot
        ),
        deltas AS (
            SELECT
                day, device, timestamp,
                {clip_expr} AS dt_h,
                prev_power, prev_freq, curr_mag
            FROM seq
        )
        SELECT
            day,
            device,
            ROUND(SUM(CASE WHEN prev_power IS NOT NULL THEN prev_power * dt_h ELSE 0 END), 6) AS energia_kWh,
            ROUND(SUM(CASE WHEN prev_freq  > 0     THEN dt_h              ELSE 0 END), 6)     AS horas_operacao,
            MAX(curr_mag) AS corrente_max,
            MIN(timestamp) AS first_ts,
            MAX(timestamp) AS last_ts,
            COUNT(*) AS samples
        FROM deltas
        GROUP BY day, device
        ORDER BY day;
        """

        c.execute(query, (dev, start_ts, end_ts))
        rows = c.fetchall()

        for (day, device, energia, horas, corrmax, first_ts, last_ts, samples) in rows:
            _upsert_masseira_daily_row(day, device, energia, horas, corrmax, first_ts, last_ts, samples)
            total_rows += 1

        if rows:
            _set_meta_value(meta_key, end_ts)

    conn_ro.close()

    print(f"[masseira_daily] Upserted {total_rows} day/device rows. Cursor moved to {end_ts}.")
    return total_rows
