"""Rename map_crypto_is_spot -> map_crypto_type in SQLite DB tables.

Run once after deploying the code changes:
    python scripts/migrate_crypto_type_rename.py
"""
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "market.db"


def migrate(db_path: Path) -> None:
    if not db_path.exists():
        print(f"DB not found at {db_path} -- nothing to migrate")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    for table in ("mkt_etp_master", "mkt_master_data"):
        cur.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall()]
        if "map_crypto_is_spot" in cols and "map_crypto_type" not in cols:
            print(f"  Renaming map_crypto_is_spot -> map_crypto_type in {table}")
            cur.execute(f"ALTER TABLE {table} RENAME COLUMN map_crypto_is_spot TO map_crypto_type")
        elif "map_crypto_type" in cols:
            print(f"  {table}: already has map_crypto_type -- skipping")
        else:
            print(f"  {table}: map_crypto_is_spot not found -- skipping")

    conn.commit()
    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    migrate(path)
