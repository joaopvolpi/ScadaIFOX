import sqlite3
import os
from datetime import datetime
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
        CREATE TABLE IF NOT EXISTS operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_start TEXT,
            device TEXT,
            tipo TEXT,
            destino TEXT,
            qnt_solicitada REAL,
            peso_inicio REAL,
            peso_fim REAL,
            peso_real REAL,
            created_at TEXT
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

def save_to_sqlite(device_name, result):
    conn = sqlite3.connect(DB_FILE)
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

    # index for operations lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_operations_device_ts
        ON operations(device, timestamp_start);
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

from core.calculations import calcula_operacoes_descarga_tanques

def _get_meta_value(key: str):
    """Reads a value from the meta table (e.g., last processed timestamp)."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def _set_meta_value(key: str, value: str):
    """Sets or updates a key-value pair in the meta table."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO meta(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()
    conn.close()


def _insert_operations(ops_dict, tipo):
    """Insert detected operations into the 'operations' table (idempotent)."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    inserted = 0
    for dev, destinos in (ops_dict or {}).items():
        for destino, lst in destinos.items():
            for op in lst:
                ts_start = op.get("horario")
                if not ts_start:
                    continue

                # skip if already exists (idempotency)
                c.execute("SELECT COUNT(*) FROM operations WHERE device=? AND timestamp_start=?", (dev, ts_start))
                if c.fetchone()[0] > 0:
                    continue

                peso_inicio = op.get("peso_inicio")
                peso_fim = op.get("peso_fim")
                peso_real = (float(peso_inicio or 0) - float(peso_fim or 0))

                c.execute("""
                    INSERT INTO operations (
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


def update_operations_table():
    """
    Batch process to detect and insert new discharge operations into the operations table.
    - Reads last processed timestamp from meta table
    - Uses calcula_operacoes_descarga_tanques() to find new ones
    - Inserts new operations (if not already stored)
    - Updates the last processed timestamp
    """
    print("[operations_updater] Starting batch update...")

    last_ts = _get_meta_value("last_ops_ts")
    start_ts = last_ts if last_ts else "1970-01-01T00:00:00"
    end_ts = datetime.now().isoformat()

    # --- Run detection for each type ---
    ops_resina = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Resina")
    ops_agua   = calcula_operacoes_descarga_tanques(start_ts, end_ts, "Agua")

    # --- Insert new ones ---
    count_res = _insert_operations(ops_resina, "Resina")
    count_agua = _insert_operations(ops_agua, "Agua")

    _set_meta_value("last_ops_ts", end_ts)

    total = count_res + count_agua
    print(f"[operations_updater] Inserted {total} new operations (Resina: {count_res}, Agua: {count_agua}).")
    print(f"[operations_updater] Updated last processed timestamp to {end_ts}.")

    return total