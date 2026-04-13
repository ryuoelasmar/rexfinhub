"""Sync REX ETNs from Bloomberg into rex_products table.

ETNs are '33 Act notes, not '40 Act funds, so they don't flow through the
SEC 485 pipeline. They're sourced from Bloomberg master data.

Typically called from admin panel or as part of the daily sync.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

log = logging.getLogger(__name__)


def sync_rex_etns() -> dict:
    """Upsert all REX ETNs from Bloomberg into rex_products."""
    from webapp.database import init_db, SessionLocal
    from webapp.models import RexProduct
    from webapp.services.market_data import get_master_data

    init_db()
    db = SessionLocal()

    try:
        master = get_master_data(db, etn_overrides=True)
        if master is None or len(master) == 0:
            return {"error": "No Bloomberg data available"}

        ft_col = "fund_type" if "fund_type" in master.columns else "t_w1.fund_type"
        tc_col = "ticker_clean" if "ticker_clean" in master.columns else "ticker"

        # REX ETNs only
        rex_mask = master["is_rex"] == True if "is_rex" in master.columns else master.get("issuer_display", "").astype(str).str.contains("REX", case=False, na=False)
        etn_mask = master[ft_col] == "ETN"
        etns = master[rex_mask & etn_mask]

        added = 0
        updated = 0
        for _, r in etns.iterrows():
            ticker = str(r.get(tc_col, "")).strip()
            if not ticker or ticker.lower() == "nan":
                continue

            existing = db.query(RexProduct).filter(RexProduct.ticker == ticker).first()

            # Parse inception date
            inc_date = None
            inc_raw = r.get("inception_date")
            if inc_raw is not None:
                try:
                    import pandas as pd
                    if not pd.isna(inc_raw):
                        inc_date = pd.Timestamp(inc_raw).date()
                except Exception:
                    pass

            name = str(r.get("fund_name", ticker))[:200]
            exchange = str(r.get("exchange_name", "") or r.get("exchange", ""))[:20] or None
            aum = float(r.get("t_w4.aum", 0) or 0)

            if existing:
                existing.name = name
                existing.status = "Listed"
                existing.trust = existing.trust or "MicroSectors (REX)"
                existing.exchange = exchange or existing.exchange
                existing.official_listed_date = existing.official_listed_date or inc_date
                existing.initial_filing_date = existing.initial_filing_date or inc_date
                existing.updated_at = datetime.utcnow()
                updated += 1
            else:
                db.add(RexProduct(
                    name=name,
                    trust="MicroSectors (REX)",
                    product_suite="Crypto" if "BITCOIN" in name.upper() else "MicroSectors ETN",
                    status="Listed",
                    ticker=ticker,
                    initial_filing_date=inc_date,
                    official_listed_date=inc_date,
                    latest_form="ETN",
                    exchange=exchange,
                ))
                added += 1

        db.commit()
        return {"added": added, "updated": updated, "total_etns": len(etns)}
    finally:
        db.close()


if __name__ == "__main__":
    result = sync_rex_etns()
    print(f"ETN sync complete: {result}")
