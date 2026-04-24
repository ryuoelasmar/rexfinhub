"""Refresh the cboe_known_active table from NASDAQ + SEC EDGAR sources.

Run this before re-rendering /filings/symbols if the ticker universe is stale,
and on the same nightly cadence as the CBOE scanner so the active/reserved
split stays accurate.

Usage:
    python scripts/refresh_cboe_known_active.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from webapp.database import init_db  # noqa: E402
from webapp.services.cboe.known_active import refresh_known_active  # noqa: E402


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()
    summary = refresh_known_active()
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
