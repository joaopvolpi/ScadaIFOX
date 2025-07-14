import sqlite3
import os
from datetime import datetime

DB_FILE = "./data/scada.db"

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

    # ✅ Strip microseconds for ISO format to match datetime-local input
    timestamp = datetime.now().replace(microsecond=0).isoformat()

    for tag, value in result.items():
        c.execute(
            "INSERT INTO readings (timestamp, device, tag, value) VALUES (?, ?, ?, ?)",
            (timestamp, device_name, tag, value)
        )

    conn.commit()
    conn.close()
