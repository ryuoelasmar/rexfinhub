"""IPO calendar scraper — stockanalysis.com/ipos/.

Scrapes the upcoming and recent IPO tables. No API — uses a stable HTML
structure. Falls back gracefully if the site changes; the report should
still render with an empty IPO section rather than crash.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

_UPCOMING_URL = "https://stockanalysis.com/ipos/"
_RECENT_URL = "https://stockanalysis.com/ipos/"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


@dataclass
class IPORow:
    ticker: str
    company: str
    exchange: str | None
    price_range: str | None
    expected_date: str | None
    shares_offered: str | None
    source_section: str  # "upcoming" | "recent"


def _clean(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _parse_table(table, section_label: str) -> list[IPORow]:
    rows: list[IPORow] = []
    if table is None:
        return rows
    headers = [_clean(th.get_text()) for th in table.find_all("th")]
    header_map = {h.lower(): i for i, h in enumerate(headers)}

    def idx(names: Iterable[str]) -> int | None:
        for n in names:
            if n.lower() in header_map:
                return header_map[n.lower()]
        return None

    i_ticker = idx(["symbol", "ticker"])
    i_company = idx(["company name", "company"])
    i_date = idx(["expected ipo date", "ipo date", "listing date"])
    i_price = idx(["price range", "ipo price", "price"])
    i_shares = idx(["shares offered", "shares"])
    i_exch = idx(["exchange"])

    for tr in table.find("tbody").find_all("tr") if table.find("tbody") else []:
        cells = [_clean(td.get_text()) for td in tr.find_all("td")]
        if not cells:
            continue
        def get(i):
            return cells[i] if (i is not None and i < len(cells)) else ""
        ticker = get(i_ticker)
        if not ticker:
            continue
        rows.append(IPORow(
            ticker=ticker,
            company=get(i_company),
            exchange=get(i_exch) or None,
            price_range=get(i_price) or None,
            expected_date=get(i_date) or None,
            shares_offered=get(i_shares) or None,
            source_section=section_label,
        ))
    return rows


def fetch_ipos(timeout: float = 15.0) -> list[IPORow]:
    """Return the union of upcoming + recent IPOs from stockanalysis.com.

    On any network or parse failure, returns an empty list and logs the issue.
    The weekly report treats an empty list as 'no data this week' rather than
    a hard failure.
    """
    try:
        resp = requests.get(_UPCOMING_URL, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        log.warning("IPO scrape: fetch failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[IPORow] = []
    for table in soup.find_all("table"):
        caption = table.find("caption")
        caption_text = _clean(caption.get_text()) if caption else ""
        label = "unknown"
        prev_heading = table.find_previous(["h1", "h2", "h3"])
        heading_text = _clean(prev_heading.get_text()) if prev_heading else ""
        if "upcoming" in heading_text.lower() or "upcoming" in caption_text.lower():
            label = "upcoming"
        elif "recent" in heading_text.lower() or "recent" in caption_text.lower() or "priced" in heading_text.lower():
            label = "recent"
        try:
            parsed = _parse_table(table, label)
            if parsed:
                rows.extend(parsed)
        except Exception as e:
            log.warning("IPO scrape: parse error in %s table: %s", label, e)

    # Deduplicate by ticker, preferring the upcoming entry over recent
    seen: dict[str, IPORow] = {}
    for r in rows:
        if r.ticker not in seen or (r.source_section == "upcoming" and seen[r.ticker].source_section == "recent"):
            seen[r.ticker] = r
    unique = list(seen.values())
    log.info("IPO scrape: fetched %d unique rows (%d upcoming, %d recent)",
             len(unique),
             sum(1 for r in unique if r.source_section == "upcoming"),
             sum(1 for r in unique if r.source_section == "recent"))
    return unique


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ipos = fetch_ipos()
    for r in ipos[:20]:
        print(f"  [{r.source_section}] {r.ticker:8s} {r.company[:40]:40s} {r.expected_date or ''}")
