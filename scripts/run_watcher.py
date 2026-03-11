"""
Watcher Runner - 30-Minute New Trust Detection

Polls EDGAR EFTS for recent 40-Act and 33-Act filings, creates
alerts for known trusts and candidates for unknown ones, then
auto-approves high-scoring candidates.

Scheduling (Windows Task Scheduler):
    schtasks /create /tn "ETP_Watcher" /tr "python C:\\Projects\\rexfinhub\\scripts\\run_watcher.py" /sc minute /mo 30 /f

Manual run:
    python scripts/run_watcher.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"watcher_{today}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_file), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    log = logging.getLogger("watcher")

    from webapp.database import SessionLocal, init_db
    from etp_tracker.watcher import poll_recent_filings, auto_approve_candidates

    init_db()
    db = SessionLocal()

    try:
        log.info("=== Watcher run started ===")

        # Poll both 40-Act (485 series) and 33-Act (S-1/S-3) forms
        result = poll_recent_filings(db, lookback_days=1, poll_33act=True)

        log.info(
            "Poll results: alerts=%d (skipped=%d), candidates_new=%d (updated=%d), errors=%d",
            result.alerts_created,
            result.alerts_skipped,
            result.candidates_new,
            result.candidates_updated,
            len(result.errors),
        )

        if result.errors:
            for err in result.errors:
                log.warning("  Error: %s", err)

        # Auto-approve high-scoring candidates
        if result.candidates_new > 0:
            approved = auto_approve_candidates(db)
            log.info("Auto-approved %d new trust(s)", approved)

        log.info("=== Watcher run complete ===")

    except Exception as e:
        log.error("Watcher failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
