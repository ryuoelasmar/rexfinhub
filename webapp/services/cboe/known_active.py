"""Fetch the known-active US-listed security universe from public sources.

Sources (in priority order; later sources fill gaps left by earlier ones):
  1. NASDAQ stock screener API     — ~7k stocks with metadata
  2. NASDAQ ETF screener API       — ~4k ETFs
  3. NASDAQ Trader nasdaqlisted    — authoritative NASDAQ symbol file
  4. NASDAQ Trader otherlisted     — NYSE / AMEX / BATS authoritative
  5. SEC EDGAR company_tickers     — issuer-side cross-check

A row in cboe_known_active means "this ticker is currently a live listing
on a major US exchange." Subtracting cboe_known_active.base_ticker from
cboe_symbols (where available=False) yields the reservations-without-
listings set — the competitor pipeline intel.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Iterable

import requests

from webapp.database import SessionLocal
from webapp.models import CboeKnownActive

log = logging.getLogger(__name__)

USER_AGENT = "rexfinhub-ticker-radar/0.1 (relasmar@rexfin.com)"
HTTP_TIMEOUT = 30

NASDAQ_STOCK_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&offset=0&download=true"
NASDAQ_ETF_URL = "https://api.nasdaq.com/api/screener/etf?tableonly=true&limit=10000&offset=0&download=true"
NASDAQ_TRADER_NASDAQ_URL = "http://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_TRADER_OTHER_URL = "http://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
SEC_EDGAR_URL = "https://www.sec.gov/files/company_tickers.json"


def normalize_base_ticker(raw: str | None) -> str | None:
    """'BRK.A' -> 'BRK', 'AAPL' -> 'AAPL', 'GOOGL' -> None (over 4 chars)."""
    if not raw:
        return None
    m = re.match(r"^[A-Z]+", raw.strip().upper())
    if not m:
        return None
    base = m.group(0)
    return base if 1 <= len(base) <= 4 else None


def _parse_market_cap(raw) -> float | None:
    if raw in (None, "", "N/A"):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).replace("$", "").replace(",", "").strip()
    if not s or s in ("N/A", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_nasdaq_stocks() -> list[dict]:
    log.info("Fetching NASDAQ stock screener...")
    r = requests.get(
        NASDAQ_STOCK_URL,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    rows = data.get("data", {}).get("rows") or data.get("data", {}).get("table", {}).get("rows", [])
    out = []
    for row in rows or []:
        full = (row.get("symbol") or "").strip().upper()
        base = normalize_base_ticker(full)
        if not base:
            continue
        out.append({
            "full_ticker": full,
            "base_ticker": base,
            "name": row.get("name"),
            "sec_type": "stock",
            "exchange": None,  # screener doesn't include exchange directly
            "sector": row.get("sector") or None,
            "industry": row.get("industry") or None,
            "market_cap": _parse_market_cap(row.get("marketCap")),
            "source": "nasdaq_screener_stocks",
        })
    log.info("  -> %d stock rows", len(out))
    return out


def fetch_nasdaq_etfs() -> list[dict]:
    log.info("Fetching NASDAQ ETF screener...")
    r = requests.get(
        NASDAQ_ETF_URL,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    rows = data.get("data", {}).get("data", {}).get("rows") or data.get("data", {}).get("rows", [])
    out = []
    for row in rows or []:
        full = (row.get("symbol") or "").strip().upper()
        base = normalize_base_ticker(full)
        if not base:
            continue
        out.append({
            "full_ticker": full,
            "base_ticker": base,
            "name": row.get("companyName") or row.get("name"),
            "sec_type": "etf",
            "exchange": None,
            "sector": None,
            "industry": None,
            "market_cap": None,
            "source": "nasdaq_screener_etfs",
        })
    log.info("  -> %d ETF rows", len(out))
    return out


def fetch_nasdaq_trader_nasdaq() -> list[dict]:
    log.info("Fetching NASDAQ Trader nasdaqlisted.txt...")
    r = requests.get(
        NASDAQ_TRADER_NASDAQ_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    out = []
    for line in r.text.splitlines()[1:]:  # skip header
        if not line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        full = parts[0].strip().upper()
        if not full:
            continue
        base = normalize_base_ticker(full)
        if not base:
            continue
        is_etf = parts[6].strip().upper() == "Y" if len(parts) > 6 else False
        out.append({
            "full_ticker": full,
            "base_ticker": base,
            "name": parts[1].strip() or None,
            "sec_type": "etf" if is_etf else "stock",
            "exchange": "NASDAQ",
            "sector": None,
            "industry": None,
            "market_cap": None,
            "source": "nasdaq_trader_nasdaq",
        })
    log.info("  -> %d NASDAQ Trader (NASDAQ) rows", len(out))
    return out


def fetch_nasdaq_trader_other() -> list[dict]:
    log.info("Fetching NASDAQ Trader otherlisted.txt (NYSE/AMEX/BATS)...")
    r = requests.get(
        NASDAQ_TRADER_OTHER_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    out = []
    for line in r.text.splitlines()[1:]:
        if not line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        # ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
        full = parts[0].strip().upper()
        if not full:
            continue
        base = normalize_base_ticker(full)
        if not base:
            continue
        exchange_code = parts[2].strip().upper() if len(parts) > 2 else ""
        exchange_map = {"N": "NYSE", "A": "AMEX", "P": "ARCA", "Z": "BATS", "V": "IEX"}
        is_etf = parts[4].strip().upper() == "Y" if len(parts) > 4 else False
        out.append({
            "full_ticker": full,
            "base_ticker": base,
            "name": parts[1].strip() or None,
            "sec_type": "etf" if is_etf else "stock",
            "exchange": exchange_map.get(exchange_code, exchange_code or None),
            "sector": None,
            "industry": None,
            "market_cap": None,
            "source": "nasdaq_trader_other",
        })
    log.info("  -> %d NASDAQ Trader (other) rows", len(out))
    return out


def fetch_sec_edgar_tickers() -> list[dict]:
    log.info("Fetching SEC EDGAR company_tickers.json...")
    r = requests.get(
        SEC_EDGAR_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    out = []
    iterable = data.values() if isinstance(data, dict) else data
    for row in iterable:
        full = (row.get("ticker") or "").strip().upper()
        if not full:
            continue
        base = normalize_base_ticker(full)
        if not base:
            continue
        out.append({
            "full_ticker": full,
            "base_ticker": base,
            "name": row.get("title"),
            "sec_type": "other",
            "exchange": None,
            "sector": None,
            "industry": None,
            "market_cap": None,
            "source": "sec_edgar",
        })
    log.info("  -> %d SEC EDGAR rows", len(out))
    return out


_FETCHERS = (
    ("nasdaq_screener_stocks", fetch_nasdaq_stocks),
    ("nasdaq_screener_etfs", fetch_nasdaq_etfs),
    ("nasdaq_trader_nasdaq", fetch_nasdaq_trader_nasdaq),
    ("nasdaq_trader_other", fetch_nasdaq_trader_other),
    ("sec_edgar", fetch_sec_edgar_tickers),
)


def refresh_known_active(*, db_factory=SessionLocal) -> dict:
    """Fetch all sources, full-replace the cboe_known_active table.

    Returns per-source counts and the total deduplicated row count.
    """
    per_source: dict[str, int] = {}
    rows: dict[str, dict] = {}  # full_ticker -> latest row (later sources overwrite)
    failed: list[str] = []

    for name, fetcher in _FETCHERS:
        try:
            fetched = fetcher()
            per_source[name] = len(fetched)
            for r in fetched:
                rows[r["full_ticker"]] = r
        except Exception as e:
            log.error("Source %s failed: %s", name, e)
            per_source[name] = 0
            failed.append(name)

    now = datetime.utcnow()
    with db_factory() as db:
        db.query(CboeKnownActive).delete()
        for r in rows.values():
            db.add(CboeKnownActive(
                full_ticker=r["full_ticker"],
                base_ticker=r["base_ticker"],
                name=r["name"],
                sec_type=r["sec_type"],
                exchange=r["exchange"],
                sector=r["sector"],
                industry=r["industry"],
                market_cap=r["market_cap"],
                source=r["source"],
                refreshed_at=now,
            ))
        db.commit()

    distinct_bases = len({r["base_ticker"] for r in rows.values()})
    return {
        "per_source": per_source,
        "failed_sources": failed,
        "total_full_tickers": len(rows),
        "distinct_base_tickers": distinct_bases,
        "refreshed_at": now,
    }
