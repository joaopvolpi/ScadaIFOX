from core.calculations import calcula_operacoes_descarga_tanques
import sqlite3
import os
from datetime import datetime, timedelta
from config import DB_FILE
import csv

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

    # --- Daily cache for masseiras (energy integration, hours, current) ---
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


    # --- Meta table to store last processed timestamp for incremental updates ---
    c.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=3)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def save_to_sqlite(device_name, result):
    conn = get_db_connection()
    c = conn.cursor()

    timestamp = datetime.now().replace(microsecond=0).isoformat() # Formato: '2025-08-18T08:44:41'

    for tag, value in result.items():
        c.execute(
            "INSERT INTO readings (timestamp, device, tag, value) VALUES (?, ?, ?, ?)",
            (timestamp, device_name, tag, value)
        )

    conn.commit()
    conn.close()

def ensure_indexes():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

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

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_device_tag_ts
        ON readings(device, tag, timestamp);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_readings_device_tag_ts_value
        ON readings(device, tag, timestamp, value);
    """)

    # index for dosagens lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dosagens_device_ts
        ON dosagens(device, timestamp_start);
    """)

    # fast lookups for daily cache
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_masseira_daily_device_date
        ON masseira_daily(device, date);
    """)


    conn.commit()
    conn.close()

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

def _get_meta_value(key: str, conn=None):
    """Reads a value from the meta table (e.g., last processed timestamp)."""
    close_after = False
    if conn is None:
        conn = get_db_connection()
        close_after = True
    c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = c.fetchone()
    if close_after:
        conn.close()
    return row[0] if row else None

def _set_meta_value(key: str, value: str, conn=None):
    """Sets or updates a key-value pair in the meta table."""
    close_after = False
    if conn is None:
        conn = get_db_connection()
        close_after = True
    c = conn.cursor()
    c.execute("""
        INSERT INTO meta(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    if close_after:
        conn.commit()
        conn.close()

def _insert_dosagens(ops_dict, tipo):
    """Insert detected dosagens into the 'dosagens' table (idempotent)."""
    conn = get_db_connection()
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
                inserted += 1

    conn.commit()
    conn.close()
    return inserted

def update_dosagens_table():
    """
    Batch process to detect and insert new discharge dosagens into the dosagens table.
    - Reads last processed timestamp from meta table
    - Uses calcula_operacoes_descarga_tanques() to find new ones
    - Inserts new dosagens (if not already stored)
    - Updates the last processed timestamp
    """
    print("[dosagens_updater] Starting batch update...")

    last_ts = _get_meta_value("last_ops_ts")
    start_ts = last_ts if last_ts else "1970-01-01T00:00:00"
    end_ts = datetime.now().isoformat()

    # --- Run detection for each type ---
    ops_resina = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Resina")
    ops_agua   = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Agua")

    # --- Insert new ones ---
    count_res = _insert_dosagens(ops_resina, "Resina")
    count_agua = _insert_dosagens(ops_agua, "Agua")

    _set_meta_value("last_ops_ts", end_ts)

    total = count_res + count_agua
    print(f"[dosagens_updater] Inserted {total} new dosagens (Resina: {count_res}, Agua: {count_agua}).")
    print(f"[dosagens_updater] Updated last processed timestamp to {end_ts}.")

    return total

def _clip_5min_expr():
    # Returns the SQL expression for dt_h capped at 5 minutes
    # dt_h = min((t - prev_t)/3600, 5min)
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

def _upsert_masseira_daily_row(c, day, device, energia, horas, corrmax, first_ts, last_ts, samples):
    # UPSERT with additive fields and max semantics where needed
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
                                ELSE masseira_daily.corrente_max
                            END,
            first_ts      = COALESCE(masseira_daily.first_ts, excluded.first_ts),
            last_ts       = CASE
                                WHEN masseira_daily.last_ts IS NULL THEN excluded.last_ts
                                WHEN excluded.last_ts IS NULL THEN masseira_daily.last_ts
                                WHEN excluded.last_ts > masseira_daily.last_ts THEN excluded.last_ts
                                ELSE masseira_daily.last_ts
                            END,
            samples       = COALESCE(masseira_daily.samples, 0) + COALESCE(excluded.samples, 0),
            updated_at    = excluded.updated_at
    """, (day, device, energia, horas, corrmax, first_ts, last_ts, samples, datetime.now().isoformat()))

def update_masseira_daily():
    """
    Incrementally aggregate energy/hours/current for masseiras into masseira_daily,
    processing ONLY new readings since the last processed timestamp per device.
    """
    print("[masseira_daily] Starting batch update...")

    DEVICES = ("Masseira_1", "Masseira_2")
    TAGS = ("OutputPower","OutputFrequency","CurrentMagnitude")

    conn = get_db_connection()
    c = conn.cursor()

    end_ts = datetime.now().isoformat()
    clip_expr = _clip_5min_expr()

    total_rows = 0
    for dev in DEVICES:
        meta_key = f"last_energy_ts_{dev}"
        start_ts = _get_meta_value(meta_key, conn=conn)
        if not start_ts:
            start_ts = "1970-01-01T00:00:00"

        # Build the incremental aggregation query for this device
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

        # Upsert per (day, device)
        for (day, device, energia, horas, corrmax, first_ts, last_ts, samples) in rows:
            _upsert_masseira_daily_row(c, day, device, energia, horas, corrmax, first_ts, last_ts, samples)
            total_rows += 1

        # Advance the meta cursor only if we processed anything
        if rows:
            _set_meta_value(meta_key, end_ts, conn=conn)

    conn.commit()
    conn.close()

    print(f"[masseira_daily] Upserted {total_rows} day/device rows. Cursor moved to {end_ts}.")
    return total_rows