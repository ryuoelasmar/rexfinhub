"""Migration: drop the stale `live_feed` table from etp_tracker.db.

LiveFeedItem was originally added to Base (main DB) but that caused every
daily DB upload to wipe the live feed on Render. The model moved to
LiveFeedBase, which binds to a separate data/live_feed.db file.

This migration drops the now-orphaned live_feed table from etp_tracker.db
on both VPS and Render. Safe to run multiple times.

    python -m scripts.migrations.003_drop_live_feed_from_main_db
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _drop(db_path: Path) -> bool:
    if not db_path.exists():
        print(f"{db_path}: not present, skipping")
        return False
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='live_feed'"
        ).fetchone()
        if not row:
            print(f"{db_path}: no live_feed table, nothing to drop")
            return False
        conn.execute("DROP TABLE live_feed")
        print(f"{db_path}: dropped live_feed")
        return True
    finally:
        conn.close()


def main() -> int:
    main_db = PROJECT_ROOT / "data" / "etp_tracker.db"
    _drop(main_db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
