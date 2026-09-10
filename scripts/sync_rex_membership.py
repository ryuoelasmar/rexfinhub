#!/usr/bin/env python
"""Auto-derive REX membership (is_rex) from the DEFINITIONS rule.

Why this exists
---------------
is_rex was a HAND-MAINTAINED list (config/rules/rex_funds.csv). Every time REX or
BMO launched a product, someone had to remember to add it. Nobody ever did, so the
fund got is_rex=0 and counted NOWHERE in any REX-wide number - while the contract
gate still PASSED, because the expected count was set before the fund existed.

Five recurrences: HYNX (2026-07-22), RAM (06-29), RAMZ (07-28), AKAL/OCNL/PENU
(08-05), HYGU/HYGD/LQDU/LQDD (08-11).

docs/DEFINITIONS.md already states the rule deterministically:
    MicroSectors = REX's index/basket L&I suite (BMO-issued, REX-branded),
    matched by fund name starting MICROSECTORS.
So the membership never needed to be typed by hand. This step reconciles the CSV
against that rule every run, and stamps is_rex on the live table so the fix lands
in the SAME run rather than the next one.

Scope is deliberately narrow: only the MicroSectors rule is machine-derivable from
DEFINITIONS today. T-REX/other suites still rely on the CSV; they are reported as
advisory drift, never auto-added.
"""
from __future__ import annotations
import logging, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV = ROOT / "config" / "rules" / "rex_funds.csv"
DB = ROOT / "data" / "etp_tracker.db"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("sync_rex_membership")


def main() -> int:
    if not CSV.exists() or not DB.exists():
        log.error("missing %s or %s", CSV, DB)
        return 1

    existing = {l.strip() for l in CSV.read_text().splitlines() if l.strip() and l.strip() != "ticker"}

    con = sqlite3.connect(str(DB))
    cur = con.cursor()
    # DEFINITIONS.md: MicroSectors is a REX suite. rex_suite is itself derived from
    # the name rule, so keying off it keeps one source of truth.
    cur.execute(
        """SELECT ticker, fund_name FROM mkt_master_data
           WHERE market_status='ACTV' AND rex_suite='MicroSectors' ORDER BY ticker"""
    )
    live = [(t, n) for t, n in cur.fetchall() if t]

    missing = [(t, n) for t, n in live if t not in existing]
    if missing:
        s = CSV.read_text()
        if not s.endswith("\n"):
            s += "\n"
        s += "".join(f"{t}\n" for t, _ in missing)
        CSV.write_text(s)
        log.warning(
            "ADDED %d MicroSectors ETN(s) to rex_funds.csv that the manual list missed: %s",
            len(missing), ", ".join(t for t, _ in missing),
        )
    else:
        log.info("rex_funds.csv already covers all %d live MicroSectors ETNs", len(live))

    # Stamp is_rex in the SAME run (ingest already read the CSV before this step).
    stamped = 0
    for t, _ in live:
        cur.execute(
            "UPDATE mkt_master_data SET is_rex=1 WHERE ticker=? AND market_status='ACTV' AND COALESCE(is_rex,0)!=1",
            (t,),
        )
        stamped += cur.rowcount
    if stamped:
        con.commit()
        log.warning("stamped is_rex=1 on %d live MicroSectors row(s) this run", stamped)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
