"""
Non-ETF Trust Cleanup

Deactivates pure mutual fund trusts so the pipeline stops wasting time
processing entities that have zero ETF products.

Detection uses multiple signals -- not just "ETF" in the fund name:
  - Fund name keywords: ETF, Shares, Bull, Bear, Ultra, Inverse, Leveraged,
    2X/3X/4X, Bitcoin, Ether, Crypto, Exchange-Traded, ProShares, Direxion, etc.
  - Trust name keywords: same set
  - Cross-reference with mkt_master_data tickers (our Bloomberg universe)

Usage:
    python scripts/cleanup_non_etf.py --dry-run   # Preview what would change
    python scripts/cleanup_non_etf.py --apply      # Commit deactivations
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "etp_tracker.db"

# Patterns that indicate an exchange-traded product (not a mutual fund)
ETP_KEYWORDS = [
    "ETF", "Exchange-Traded", "Exchange Traded",
    "Leveraged", "Inverse", "Ultra", "UltraPro", "QuadPro",
    "Bull", "Bear",
    "1X", "1.5X", "2X", "3X", "4X", "-1X", "-2X", "-3X",
    "Daily Target",
    "Bitcoin", "Ether", "Crypto",
    "ProShares", "Direxion", "GraniteShares",
    "Shares",
]
_ETP_RE = re.compile("|".join(re.escape(kw) for kw in ETP_KEYWORDS), re.IGNORECASE)


def _is_etp_name(name: str) -> bool:
    return bool(_ETP_RE.search(name or ""))


def classify_trusts_fast(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Classify all active trusts using bulk queries (fast)."""
    c = conn.cursor()

    # Load all active trusts
    trusts = c.execute(
        "SELECT id, cik, name, source, is_rex FROM trusts WHERE is_active = 1"
    ).fetchall()
    print(f"  Loaded {len(trusts)} active trusts")

    # Bulk: fund names + tickers per trust
    fund_data: dict[int, list[tuple[str, str]]] = {}
    for tid, fname, ticker in c.execute(
        "SELECT trust_id, fund_name, ticker FROM fund_status"
    ):
        fund_data.setdefault(tid, []).append((fname or "", ticker or ""))
    print(f"  Loaded fund_status ({sum(len(v) for v in fund_data.values())} rows)")

    # Bulk: filing counts per trust
    filing_counts: dict[int, int] = {}
    for tid, cnt in c.execute(
        "SELECT trust_id, COUNT(*) FROM filings GROUP BY trust_id"
    ):
        filing_counts[tid] = cnt

    # Bulk: mkt_master_data tickers (Bloomberg universe)
    mkt_tickers = set(
        row[0] for row in c.execute("SELECT DISTINCT ticker FROM mkt_master_data")
        if row[0]
    )
    print(f"  Loaded {len(mkt_tickers)} Bloomberg tickers")

    results = {"keep": [], "deactivate": []}

    for tid, cik, name, source, is_rex in trusts:
        info = {"id": tid, "cik": cik, "name": name, "source": source}

        # Rule 1: never deactivate REX trusts
        if is_rex:
            info["reason"] = "protected (is_rex)"
            results["keep"].append(info)
            continue

        funds = fund_data.get(tid, [])
        total_funds = len(funds)
        total_filings = filing_counts.get(tid, 0)
        info["total_funds"] = total_funds
        info["total_filings"] = total_filings

        # Rule 2: Any fund name matches ETP keywords?
        etp_funds = sum(1 for fname, _ in funds if _is_etp_name(fname))
        info["etp_funds"] = etp_funds

        if etp_funds >= 1:
            info["reason"] = f"has {etp_funds} ETP fund(s)"
            results["keep"].append(info)
            continue

        # Rule 3: Trust name itself contains ETP keywords?
        if _is_etp_name(name):
            info["reason"] = "trust name matches ETP keyword"
            results["keep"].append(info)
            continue

        # Rule 4: Any fund ticker in our Bloomberg universe?
        fund_tickers = {ticker for _, ticker in funds if ticker}
        matched = fund_tickers & mkt_tickers
        if matched:
            sample = ", ".join(list(matched)[:3])
            info["reason"] = f"ticker in Bloomberg ({sample})"
            results["keep"].append(info)
            continue

        # Rules 5-7: Classification by fund/filing counts
        if total_funds > 0:
            info["reason"] = f"pure mutual fund ({total_funds} funds, 0 ETP signals)"
            results["deactivate"].append(info)
        elif total_filings == 0:
            info["reason"] = "empty (no funds, no filings)"
            results["deactivate"].append(info)
        else:
            info["reason"] = f"unprocessed ({total_filings} filings, 0 funds extracted)"
            results["keep"].append(info)

    return results


def main():
    parser = argparse.ArgumentParser(description="Cleanup non-ETF trusts from pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Preview deactivations without applying")
    group.add_argument("--apply", action="store_true", help="Apply deactivations to database")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        print("Classifying trusts...")
        results = classify_trusts_fast(conn)

        keep = results["keep"]
        deactivate = results["deactivate"]

        print(f"\n=== Trust Classification ===")
        print(f"  Active trusts scanned: {len(keep) + len(deactivate)}")
        print(f"  Keep active:           {len(keep)}")
        print(f"  Deactivate:            {len(deactivate)}")

        # Show keep breakdown
        keep_reasons: dict[str, int] = {}
        for t in keep:
            r = t["reason"]
            key = r.split("(")[0].strip() if "(" in r else r
            keep_reasons[key] = keep_reasons.get(key, 0) + 1
        print(f"\n--- Keep reasons ---")
        for reason, cnt in sorted(keep_reasons.items(), key=lambda x: -x[1]):
            print(f"  {cnt:>5d}  {reason}")

        if deactivate:
            print(f"\n--- Trusts to deactivate ({len(deactivate)}) ---")
            by_reason: dict[str, list] = {}
            for t in deactivate:
                by_reason.setdefault(t["reason"], []).append(t)

            for reason, trusts_list in sorted(by_reason.items()):
                print(f"\n  [{reason}] ({len(trusts_list)} trusts)")
                for t in trusts_list[:15]:
                    print(f"    CIK {t['cik']:>10s}  {t['name'][:60]}")
                if len(trusts_list) > 15:
                    print(f"    ... and {len(trusts_list) - 15} more")

        if args.apply and deactivate:
            print(f"\nApplying {len(deactivate)} deactivations...")
            ids = [t["id"] for t in deactivate]
            # Batch update
            conn.executemany(
                "UPDATE trusts SET is_active = 0 WHERE id = ?",
                [(i,) for i in ids],
            )
            conn.commit()
            print(f"Done. {len(ids)} trusts deactivated.")
        elif args.dry_run:
            print(f"\n[DRY RUN] No changes applied. Use --apply to commit.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
