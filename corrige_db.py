import sqlite3
import os
from config import DB_FILE

REPAIRED = DB_FILE.replace(".db", "_repaired.db")
SKIP_TABLE = "readings"   # table to ignore

def recover_except_readings(db, repaired):
    print("Opening corrupted DB...")

    # Open corrupted DB in read-only mode
    old = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    old.row_factory = sqlite3.Row

    # Create new clean DB
    if os.path.exists(repaired):
        os.remove(repaired)
    new = sqlite3.connect(repaired)

    # Get table names (except sqlite internal)
    tables = old.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    ).fetchall()

    print("\nTables found:")
    for t in tables:
        print(" -", t["name"])

    for t in tables:
        name = t["name"]

        # Skip the corrupted table
        if name == SKIP_TABLE:
            print(f"\nSkipping table '{name}' (requested).")
            continue

        print(f"\nRecovering table: {name}")

        # Get CREATE TABLE SQL
        create_sql = old.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{name}';"
        ).fetchone()[0]

        new.execute(create_sql)

        try:
            rows = old.execute(f"SELECT * FROM {name}")
            cols = [d[0] for d in rows.description]

            for r in rows:
                values = [r[c] for c in cols]
                placeholders = ",".join(["?"] * len(values))

                new.execute(
                    f"INSERT INTO {name} ({','.join(cols)}) VALUES ({placeholders})",
                    values,
                )

        except Exception as e:
            print(f"  ! Error reading rows from {name}: {e}")

        new.commit()

    old.close()
    new.close()

    print("\n✔ Recovery complete!")
    print("Repaired DB saved as:", repaired)

recover_except_readings(DB_FILE, REPAIRED)
