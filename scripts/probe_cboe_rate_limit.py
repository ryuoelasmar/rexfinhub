"""One-shot CBOE rate-limit probe.

Ramps concurrency through 5 -> 100 in stages, watching for 429s, auth
failures, and timeouts. Prints a suggested CBOE_CONCURRENCY at the end
(85% of the last clean rung).

Usage:
    python scripts/probe_cboe_rate_limit.py

Reads CBOE_SESSION_COOKIE from config/.env or the environment.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from webapp.services.cboe.rate_probe import probe_rate_limit  # noqa: E402


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cookie = _load_env("CBOE_SESSION_COOKIE")
    if not cookie:
        print("ERROR: CBOE_SESSION_COOKIE not found in env or config/.env", file=sys.stderr)
        return 2

    result = asyncio.run(probe_rate_limit(cookie))

    print()
    print("=== Probe summary ===")
    print(json.dumps(result, indent=2, default=str))
    print()
    print(f"Suggested CBOE_CONCURRENCY={result['suggested_concurrency']}")
    print("Add this to config/.env, then enable rexfinhub-cboe.timer.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
