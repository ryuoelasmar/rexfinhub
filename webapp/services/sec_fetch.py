"""
On-demand SEC filing text fetcher.

Used when http_cache is not available (e.g., on Render deployment).
Fetches filing text directly from SEC EDGAR on demand.
"""
from __future__ import annotations

import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

USER_AGENT = "REX-ETP-Tracker/2.0 (relasmar@rexfin.com)"
PAUSE = 0.25  # SEC rate limit safe pause


def fetch_filing_text(url: str, timeout: int = 30) -> str:
    """Fetch filing text directly from SEC. Returns empty string on failure."""
    if not url:
        return ""

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503))
    session.mount("https://", HTTPAdapter(max_retries=retry))

    time.sleep(PAUSE)
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        log.error("Failed to fetch filing text from %s: %s", url, e)
        return ""
