"""Sync all outputs to DB and upload to Render."""
import os
from dotenv import load_dotenv
load_dotenv()

print("Seeding new trusts into DB...", flush=True)
from webapp.database import SessionLocal, engine, Base
Base.metadata.create_all(bind=engine)
from webapp.services.sync_service import seed_trusts, sync_all
db = SessionLocal()
try:
    seeded = seed_trusts(db)
    print(f"  {seeded} new trusts seeded.", flush=True)

    print("Syncing all outputs to DB...", flush=True)
    results = sync_all(db)
    db.commit()
    total_filings = sum(r.get("filings", 0) for r in results)
    total_funds = sum(r.get("funds", 0) for r in results)
    print(f"DB sync complete. {len(results)} trusts synced, {total_filings} filings, {total_funds} funds.", flush=True)
finally:
    db.close()

print("Uploading DB to Render...", flush=True)
import requests
api_key = os.environ.get("API_KEY", "")
with open("data/etp_tracker.db", "rb") as f:
    resp = requests.post(
        "https://rex-etp-tracker.onrender.com/api/v1/db/upload",
        files={"file": ("etp_tracker.db", f)},
        headers={"X-API-Key": api_key},
        timeout=120,
    )
print(f"Upload: {resp.status_code} - {resp.text[:200]}", flush=True)
print("ALL DONE!", flush=True)
