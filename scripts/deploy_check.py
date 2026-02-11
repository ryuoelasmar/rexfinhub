"""
Post-deploy health check for Render.

Usage:
    python scripts/deploy_check.py

Polls the /health endpoint until the new version responds,
or times out after 5 minutes.
"""
from __future__ import annotations

import sys
import time

import requests

RENDER_URL = "https://rex-etp-tracker.onrender.com"
HEALTH_ENDPOINT = f"{RENDER_URL}/health"
TIMEOUT_SECONDS = 300  # 5 minutes
POLL_INTERVAL = 10  # seconds


def main():
    print(f"Checking deployment at {RENDER_URL}")
    print(f"Polling {HEALTH_ENDPOINT} every {POLL_INTERVAL}s (timeout: {TIMEOUT_SECONDS}s)")
    print()

    start = time.time()
    attempts = 0

    while time.time() - start < TIMEOUT_SECONDS:
        attempts += 1
        try:
            r = requests.get(HEALTH_ENDPOINT, timeout=10)
            if r.status_code == 200:
                data = r.json()
                elapsed = time.time() - start
                print(f"[OK] Health check passed after {elapsed:.0f}s ({attempts} attempts)")
                print(f"     Status: {data.get('status')}")
                print(f"     Version: {data.get('version')}")
                if "commit" in data:
                    print(f"     Commit: {data['commit']}")
                return 0
            else:
                print(f"  [{attempts}] HTTP {r.status_code} - waiting...")
        except requests.ConnectionError:
            print(f"  [{attempts}] Connection refused - service restarting...")
        except requests.Timeout:
            print(f"  [{attempts}] Timeout - service starting up...")
        except Exception as e:
            print(f"  [{attempts}] Error: {e}")

        time.sleep(POLL_INTERVAL)

    elapsed = time.time() - start
    print(f"\n[FAIL] Health check timed out after {elapsed:.0f}s ({attempts} attempts)")
    print("Check Render dashboard for deployment logs.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
