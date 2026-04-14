"""Force a minimal scrape of Monday's filings.

- Loads curated 290 trusts only
- Forces submissions JSON re-fetch (ignores cache age)
- Scopes to filings since 2026-04-11 (weekend + Monday)
- Runs SEC pipeline step 2-5
- Then runs DB sync only for trusts with new filings
- Prints final status

Run directly, not via run_daily.py or systemd:
    /home/jarvis/venv/bin/python scripts/force_scrape_recent.py
"""
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)
os.environ.setdefault("SEC_CACHE_DIR", "/home/jarvis/rexfinhub/cache/sec")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("force_scrape")


def main():
    from etp_tracker.run_pipeline import run_pipeline, load_ciks_from_db

    log.info("Loading curated CIKs...")
    ciks, overrides = load_ciks_from_db()
    log.info("Loaded %d curated CIKs", len(ciks))

    log.info("Starting SEC pipeline (since=2026-04-11, refresh_force_now=True)")
    start = time.time()
    result = run_pipeline(
        ciks=ciks,
        overrides=overrides,
        since="2026-04-11",
        refresh_submissions=True,
        refresh_force_now=True,  # force re-fetch all submissions JSONs
        etf_only=True,
        user_agent="REX-ETP-Tracker/2.0 (relasmar@rexfin.com)",
        triggered_by="manual-force-recent",
    )
    elapsed = time.time() - start

    trusts_total, changed_trusts = result if isinstance(result, tuple) else (result, set())
    log.info("Pipeline done in %.1fs. Trusts processed: %s, changed: %d",
             elapsed, trusts_total, len(changed_trusts))

    if changed_trusts:
        log.info("Changed trusts: %s", sorted(changed_trusts)[:20])
    else:
        log.info("No trusts had new filings")

    # Run DB sync - only for changed trusts
    if changed_trusts:
        log.info("Running DB sync for %d changed trusts", len(changed_trusts))
        from scripts.run_daily import run_db_sync
        try:
            run_db_sync(list(changed_trusts))
            log.info("DB sync complete")
        except Exception as e:
            log.error("DB sync failed: %s", e, exc_info=True)
    else:
        log.info("Skipping DB sync (no changed trusts)")

    # Final status: how many recent filings does the DB have now?
    from webapp.database import init_db, SessionLocal
    from sqlalchemy import text
    init_db()
    db = SessionLocal()
    try:
        r = db.execute(text("""
            SELECT filing_date, COUNT(*)
            FROM filings
            WHERE form LIKE '485%'
            AND filing_date >= date('now', '-7 days')
            GROUP BY filing_date
            ORDER BY filing_date DESC
        """)).fetchall()
        log.info("Filings by date (last 7 days):")
        for row in r:
            log.info("  %s: %d", row[0], row[1])
    finally:
        db.close()


if __name__ == "__main__":
    main()
