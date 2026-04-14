"""Migration: purge S-1/S-3 operating-company trusts auto-created by the
first deployment of the atom watcher.

The initial atom_watcher FORM_QUERIES included S-1 and S-3, which are used
by every non-fund operating company (Devon Energy, Lennar Homes, Alto
Neuroscience, etc.). Tier 2 enricher blindly created Trust rows for every
unknown CIK. This cleans that up.

Idempotent — identifies bad trusts by:
  - source = 'watcher_atom'
  - Any attached filing_alerts row with form_type in the S-1/S-3 family

Deletes (cascade order): FundExtractions -> Filings -> FundStatus ->
FilingAlerts -> Trust.

Run:
    python -m scripts.migrations.002_purge_s1_s3_watcher_trusts
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

S1_S3_FORMS = {
    "S-1", "S-1/A", "S-11", "S-11/A",
    "S-3", "S-3/A", "S-3ASR", "S-3D", "S-3DPOS",
}


def main() -> int:
    from webapp.database import SessionLocal, init_db
    from webapp.models import (
        Trust, Filing, FundExtraction, FundStatus, FilingAlert,
    )
    from sqlalchemy import select, func, delete

    init_db()
    db = SessionLocal()
    try:
        # Find watcher_atom trusts whose ONLY filing forms were S-1/S-3.
        # A trust is bad if every alert for its CIK is in S1_S3_FORMS.
        watcher_trusts = db.execute(
            select(Trust).where(Trust.source == "watcher_atom")
        ).scalars().all()
        if not watcher_trusts:
            print("No watcher_atom trusts — nothing to do")
            return 0

        bad_trust_ids = []
        kept_ids = []
        for t in watcher_trusts:
            # Pull all alert form types for this CIK
            forms = {
                row[0] for row in db.execute(
                    select(FilingAlert.form_type).where(FilingAlert.cik == t.cik)
                ).all()
            }
            if not forms:
                # No alerts at all — orphan trust, definitely bad
                bad_trust_ids.append(t.id)
                continue
            # If EVERY form for this CIK is S-1/S-3 family, purge it
            if forms.issubset(S1_S3_FORMS):
                bad_trust_ids.append(t.id)
            else:
                kept_ids.append(t.id)

        print(f"watcher_atom trusts: {len(watcher_trusts)} total")
        print(f"  purging: {len(bad_trust_ids)} (S-1/S-3 only)")
        print(f"  keeping: {len(kept_ids)} (has fund-form alerts)")

        if not bad_trust_ids:
            return 0

        # Cascade delete
        # 1. FundExtractions attached to filings of bad trusts
        filing_ids = [
            row[0] for row in db.execute(
                select(Filing.id).where(Filing.trust_id.in_(bad_trust_ids))
            ).all()
        ]
        if filing_ids:
            n = db.execute(
                delete(FundExtraction).where(FundExtraction.filing_id.in_(filing_ids))
            ).rowcount
            print(f"  deleted fund_extractions: {n}")

        # 2. Filings
        n = db.execute(
            delete(Filing).where(Filing.trust_id.in_(bad_trust_ids))
        ).rowcount
        print(f"  deleted filings: {n}")

        # 3. FundStatus
        n = db.execute(
            delete(FundStatus).where(FundStatus.trust_id.in_(bad_trust_ids))
        ).rowcount
        print(f"  deleted fund_status: {n}")

        # 4. FilingAlerts for those trusts — null out trust_id so the alerts
        # remain as historical records (so atom watcher doesn't re-fetch them)
        # but aren't linked to the deleted trust
        bad_ciks = [t.cik for t in watcher_trusts if t.id in bad_trust_ids]
        if bad_ciks:
            n = db.execute(
                delete(FilingAlert).where(FilingAlert.cik.in_(bad_ciks))
            ).rowcount
            print(f"  deleted filing_alerts (by cik): {n}")

        # 5. Trusts
        n = db.execute(
            delete(Trust).where(Trust.id.in_(bad_trust_ids))
        ).rowcount
        print(f"  deleted trusts: {n}")

        db.commit()
        print("OK")
        return 0
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
