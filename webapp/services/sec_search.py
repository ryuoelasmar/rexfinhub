"""
SEC EDGAR Full-Text Search Service

Uses the EDGAR Full-Text Search (EFTS) API to find trusts/registrants,
and the submissions API to verify CIKs and retrieve entity details.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

USER_AGENT = "REX-ETP-Tracker/2.0 (relasmar@rexfin.com)"
EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik_padded}.json"
PAUSE = 0.25  # SEC rate limit safe pause


def _get_session() -> requests.Session:
    """Create a requests session with retry + user agent."""
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def search_trusts(
    query: str,
    forms: str = "485BPOS,485APOS,485BXT,N-1A",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search EDGAR full-text search for trusts matching the query.

    Returns list of dicts: {cik, name, forms_found, filing_count}
    Deduplicates by CIK.
    """
    if not query or not query.strip():
        return []

    session = _get_session()
    params = {
        "q": f'"{query}"',
        "forms": forms,
        "dateRange": "custom",
        "startdt": "2020-01-01",
    }

    time.sleep(PAUSE)
    try:
        resp = session.get(EFTS_URL, params=params, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("EFTS search failed: %s", e)
        return []

    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])

    # Deduplicate by CIK
    seen: dict[str, dict] = {}
    for hit in hits:
        source = hit.get("_source", {})
        cik = str(source.get("file_num_cik", source.get("ciks", [""])[0] if source.get("ciks") else ""))
        if not cik or cik == "0":
            # Try extracting from entity_name or other fields
            ciks = source.get("ciks", [])
            if ciks:
                cik = str(ciks[0])
            else:
                continue

        name = source.get("entity_name", source.get("display_names", ["Unknown"])[0] if source.get("display_names") else "Unknown")
        form = source.get("form_type", "")

        if cik not in seen:
            seen[cik] = {
                "cik": cik,
                "name": name,
                "forms_found": set(),
                "filing_count": 0,
            }
        seen[cik]["forms_found"].add(form)
        seen[cik]["filing_count"] += 1

    # Convert sets to sorted lists
    results = []
    for item in seen.values():
        item["forms_found"] = sorted(item["forms_found"])
        results.append(item)

    results.sort(key=lambda x: x["filing_count"], reverse=True)
    return results[:limit]


def verify_cik(cik: str) -> dict[str, Any] | None:
    """Verify a CIK against the SEC submissions API.

    Returns dict with entity details, or None if CIK not found.
    """
    cik_padded = cik.zfill(10)
    url = SUBMISSIONS_URL.format(cik_padded=cik_padded)

    session = _get_session()
    time.sleep(PAUSE)
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("CIK verification failed for %s: %s", cik, e)
        return None

    data = resp.json()
    recent_filings = data.get("filings", {}).get("recent", {})
    filing_count = len(recent_filings.get("accessionNumber", []))

    # Get form types
    forms = set(recent_filings.get("form", []))
    prospectus_forms = {f for f in forms if f.startswith(("485", "497", "N-1A"))}

    return {
        "cik": str(data.get("cik", cik)),
        "name": data.get("name", "Unknown"),
        "entity_type": data.get("entityType", ""),
        "sic": data.get("sic", ""),
        "sic_description": data.get("sicDescription", ""),
        "state": data.get("stateOfIncorporation", ""),
        "filing_count": filing_count,
        "prospectus_forms": sorted(prospectus_forms),
        "has_prospectus_filings": bool(prospectus_forms),
    }


def search_and_verify(query: str) -> list[dict[str, Any]]:
    """Search EDGAR and verify each result's CIK.

    This is a convenience function that combines search + verify.
    Use sparingly as it makes multiple API calls.
    """
    results = search_trusts(query)
    verified = []
    for r in results[:10]:  # Limit verification to top 10
        details = verify_cik(r["cik"])
        if details:
            r.update(details)
            verified.append(r)
    return verified
