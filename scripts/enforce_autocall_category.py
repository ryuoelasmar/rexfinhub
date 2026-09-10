#!/usr/bin/env python
"""Make the deterministic AUTOCALL rule authoritative over the AI classifier.

market/auto_classify.py Rule 1.5 already says: a fund with AUTOCALL in its name is
cc_category='Autocallable' -- the structure outranks the underlier theme. But the AI
middleman (ai_classify_unmapped) writes attributes_CC.csv rows FIRST, and the CSV wins,
so every new autocallable launch lands with an EXPOSURE value (Broad Beta / Tech /
Small Caps) in the structure field and silently drops out of the autocall report.

That is not hypothetical: 8 funds on 2026-08-17 (incl. REX's own DACL), then IACL on
2026-08-18, its launch day. The report was showing 22 of 30 products to RBC and CAIS.

This step re-asserts the rule after the AI has run. Name-based and unambiguous -- if
'AUTOCALL' is in the fund name, the structure IS autocallable.
"""
from __future__ import annotations
import csv, io, logging, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "config" / "rules" / "attributes_CC.csv"
DB = ROOT / "data" / "etp_tracker.db"
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("enforce_autocall_category")


def main() -> int:
    con = sqlite3.connect(str(DB)); cur = con.cursor()
    cur.execute(
        """SELECT ticker, fund_name FROM mkt_master_data
           WHERE market_status='ACTV' AND etp_category='CC'
             AND UPPER(fund_name) LIKE '%AUTOCALL%'"""
    )
    live = {t: n for t, n in cur.fetchall()}
    if not live:
        log.info("no live autocallable CC funds found"); con.close(); return 0

    rows = list(csv.reader(io.StringIO(CSV_PATH.read_text())))
    hdr, body = rows[0], [r for r in rows[1:] if r]
    seen, fixed, added = set(), [], []
    for r in body:
        while len(r) < 5:
            r.append("")
        t = r[0].strip(); seen.add(t)
        if t in live and r[4].strip() != "Autocallable":
            fixed.append((t, r[4].strip() or "(blank)"))
            r[4] = "Autocallable"
            if not r[3].strip():
                r[3] = "Synthetic"
    for t in live:
        if t not in seen:
            body.append([t, "", "", "Synthetic", "Autocallable"]); added.append(t)

    if fixed or added:
        buf = io.StringIO(); w = csv.writer(buf, lineterminator="\n")
        w.writerow(hdr); w.writerows(body)
        CSV_PATH.write_text(buf.getvalue())
        for t, was in fixed:
            log.warning("cc_category %s: %s -> Autocallable (%s)", t, was, live[t][:44])
        for t in added:
            log.warning("added missing attributes_CC row for %s", t)
        # apply to the live table too, so the fix lands THIS run
        for t in list(dict.fromkeys([x[0] for x in fixed] + added)):
            cur.execute(
                "UPDATE mkt_master_data SET cc_category='Autocallable' "
                "WHERE ticker=? AND market_status='ACTV'", (t,))
        con.commit()
        log.warning("enforced the AUTOCALL rule on %d fund(s)", len(fixed) + len(added))
    else:
        log.info("all %d live autocallable fund(s) already carry cc_category=Autocallable", len(live))
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
