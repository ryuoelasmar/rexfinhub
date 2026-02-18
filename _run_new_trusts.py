"""Run pipeline for all 50 new trusts, sync DB, upload to Render."""
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

# Identify new trusts (not in original 20)
ORIGINAL_20 = {
    "2043954","1424958","1040587","1174610","1689873","1884021","1976517",
    "1924868","1540305","1976322","1771146","1452937","1587982","1547950",
    "1579881","826732","1782952","1722388","1683471","1396092",
}

from etp_tracker.trusts import TRUST_CIKS
new_ciks = {c: n for c, n in TRUST_CIKS.items() if c not in ORIGINAL_20}
print(f"Starting pipeline for {len(new_ciks)} new trusts...", flush=True)

from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(
    ciks=list(new_ciks.keys()),
    overrides=new_ciks,
    user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com",
)
print(f"Pipeline complete: {count} trusts processed.", flush=True)

# Sync to DB
print("Syncing to DB...", flush=True)
from webapp.database import SessionLocal, engine, Base
Base.metadata.create_all(bind=engine)
from webapp.services.db_sync import sync_outputs_to_db
db = SessionLocal()
try:
    sync_outputs_to_db(db)
    db.commit()
    print("DB sync complete.", flush=True)
finally:
    db.close()

# Upload DB to Render
print("Uploading DB to Render...", flush=True)
import requests
db_path = "data/etp_tracker.db"
with open(db_path, "rb") as f:
    resp = requests.post(
        "https://rex-etp-tracker.onrender.com/api/v1/db/upload",
        files={"file": ("etp_tracker.db", f)},
        timeout=120,
    )
print(f"Upload: {resp.status_code} - {resp.text[:200]}", flush=True)
print("ALL DONE - 50 new trusts processed, synced, and uploaded to Render!", flush=True)
