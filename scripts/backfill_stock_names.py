#!/usr/bin/env python
"""Populate a stock_names cache so briefs can show a Company column.

The T-REX brief renders a Company column sourced from mkt_stock_data's "Name" field,
but the Bloomberg stock pull supplies 29 numeric fields and no name -- 0 of 6,594 rows
have one, so the column has always rendered blank for every ticker. Rather than change
Ryu's workbook, cache names locally and let the brief fall back to this table.

Scope: only tickers the engine actually scores, newest run, highest scores first.
Idempotent -- already-cached tickers are skipped, so re-runs are cheap.
"""
from __future__ import annotations
import argparse, logging, sqlite3, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "etp_tracker.db"
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("backfill_stock_names")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=250, help="max tickers to fetch this run")
    ap.add_argument("--tickers", default="", help="comma-separated override list")
    a = ap.parse_args()

    con = sqlite3.connect(str(DB)); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS stock_names (
        ticker TEXT PRIMARY KEY, company_name TEXT, source TEXT, updated_at TEXT)""")
    con.commit()

    have = {r[0] for r in cur.execute("SELECT ticker FROM stock_names WHERE company_name IS NOT NULL AND company_name != ''")}
    if a.tickers:
        want = [t.strip().upper() for t in a.tickers.split(",") if t.strip()]
    else:
        cur.execute("SELECT MAX(run_date) FROM li_engine_daily")
        rd = cur.fetchone()[0]
        cur.execute("""SELECT ticker FROM li_engine_daily WHERE run_date=?
                       ORDER BY final_score DESC""", (rd,))
        want = [r[0].upper() for r in cur.fetchall()]
    todo = [t for t in want if t not in have][: a.limit]
    if not todo:
        log.info("nothing to fetch — %d names already cached", len(have)); con.close(); return 0

    try:
        import yfinance as yf
    except Exception as e:
        log.error("yfinance unavailable: %s", e); con.close(); return 1

    log.info("fetching %d name(s) (%d already cached)", len(todo), len(have))
    ok = 0
    for i, tk in enumerate(todo, 1):
        name = ""
        try:
            info = yf.Ticker(tk).info
            name = str(info.get("longName") or info.get("shortName") or "").strip()
        except Exception:
            pass
        cur.execute("""INSERT INTO stock_names (ticker, company_name, source, updated_at)
                       VALUES (?,?,?,datetime('now'))
                       ON CONFLICT(ticker) DO UPDATE SET company_name=excluded.company_name,
                       source=excluded.source, updated_at=excluded.updated_at""",
                    (tk, name, "yfinance" if name else "unresolved"))
        if name: ok += 1
        if i % 25 == 0:
            con.commit(); log.info("  %d/%d ...", i, len(todo)); time.sleep(0.4)
    con.commit()
    log.info("resolved %d of %d", ok, len(todo))
    con.close(); return 0


if __name__ == "__main__":
    sys.exit(main())
