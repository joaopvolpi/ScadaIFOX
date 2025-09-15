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