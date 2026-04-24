"""CBOE symbol-availability scanner — systemd entrypoint.

Runs an async scan of the chosen tier, upserts results into cboe_symbols,
appends to cboe_state_changes, records the run in cboe_scan_runs.

Usage:
    python scripts/run_cboe_scan.py                # default: --tier daily
    python scripts/run_cboe_scan.py --tier 1-letter
    python scripts/run_cboe_scan.py --tier 3-letter
    python scripts/run_cboe_scan.py --tier 4-letter
    python scripts/run_cboe_scan.py --tier full
    python scripts/run_cboe_scan.py --concurrency 25   # override env

Reads CBOE_SESSION_COOKIE and CBOE_CONCURRENCY from config/.env or env.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from webapp.database import SessionLocal, init_db  # noqa: E402
from webapp.services.cboe.scanner import AuthError, CboeScanner  # noqa: E402
from webapp.services.cboe.universe import tier_by_name  # noqa: E402

DEFAULT_CONCURRENCY = 10
VALID_TIERS = ("daily", "1-letter", "2-letter", "3-letter", "4-letter", "full")


def _load_env(key: str) -> str:
    val = os.environ.get(key, "")
    if val:
        return val
    env_file = PROJECT_ROOT / "config" / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a CBOE symbol-availability scan.")
    parser.add_argument(
        "--tier",
        default="daily",
        choices=VALID_TIERS,
        help="Which slice of the universe to scan (default: daily)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Override CBOE_CONCURRENCY from env",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("cboe-scan")

    cookie = _load_env("CBOE_SESSION_COOKIE")
    if not cookie:
        log.error("CBOE_SESSION_COOKIE not set — refresh from Chrome DevTools and update config/.env")
        return 2

    if args.concurrency is not None:
        concurrency = args.concurrency
    else:
        env_conc = _load_env("CBOE_CONCURRENCY")
        try:
            concurrency = int(env_conc) if env_conc else DEFAULT_CONCURRENCY
        except ValueError:
            log.warning("CBOE_CONCURRENCY=%r is not an int; using default %d", env_conc, DEFAULT_CONCURRENCY)
            concurrency = DEFAULT_CONCURRENCY

    init_db()

    with SessionLocal() as db:
        tickers = tier_by_name(db, args.tier)
    log.info(
        "Scanning tier=%s with concurrency=%d (%d tickers)",
        args.tier, concurrency, len(tickers),
    )

    scanner = CboeScanner(cookie=cookie, concurrency=concurrency)
    try:
        summary = asyncio.run(scanner.scan(tickers, tier=args.tier))
    except AuthError as e:
        log.error("Auth failure: %s", e)
        return 3

    log.info("Scan complete: %s", json.dumps(summary, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
