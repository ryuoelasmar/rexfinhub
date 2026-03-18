"""Market Monitor — live underlier and index performance."""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

log = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="webapp/templates")

# All tickers — fetched in a single batch call
WATCHLIST = [
    # Indices
    {"yf": "^GSPC", "display": "SPX", "name": "S&P 500", "category": "Index"},
    {"yf": "^NDX", "display": "NDX", "name": "Nasdaq 100", "category": "Index"},
    {"yf": "^RUT", "display": "RUT", "name": "Russell 2000", "category": "Index"},
    {"yf": "^DJI", "display": "DJIA", "name": "Dow Jones", "category": "Index"},
    # Volatility
    {"yf": "^VIX", "display": "VIX", "name": "VIX", "category": "Volatility"},
    # Commodities
    {"yf": "GC=F", "display": "GOLD", "name": "Gold", "category": "Commodity"},
    {"yf": "SI=F", "display": "SILVER", "name": "Silver", "category": "Commodity"},
    {"yf": "CL=F", "display": "OIL", "name": "Crude Oil (WTI)", "category": "Commodity"},
    # Crypto
    {"yf": "BTC-USD", "display": "BTC", "name": "Bitcoin", "category": "Crypto"},
    {"yf": "ETH-USD", "display": "ETH", "name": "Ethereum", "category": "Crypto"},
    # Single Stocks
    {"yf": "NVDA", "display": "NVDA", "name": "NVIDIA", "category": "Single Stock"},
    {"yf": "TSLA", "display": "TSLA", "name": "Tesla", "category": "Single Stock"},
    {"yf": "AAPL", "display": "AAPL", "name": "Apple", "category": "Single Stock"},
    {"yf": "MSFT", "display": "MSFT", "name": "Microsoft", "category": "Single Stock"},
    {"yf": "GOOGL", "display": "GOOGL", "name": "Alphabet", "category": "Single Stock"},
    {"yf": "AMZN", "display": "AMZN", "name": "Amazon", "category": "Single Stock"},
    {"yf": "META", "display": "META", "name": "Meta", "category": "Single Stock"},
    {"yf": "AVGO", "display": "AVGO", "name": "Broadcom", "category": "Single Stock"},
    # Sectors
    {"yf": "XLF", "display": "XLF", "name": "Financials", "category": "Sector"},
    {"yf": "XLE", "display": "XLE", "name": "Energy", "category": "Sector"},
    {"yf": "XLK", "display": "XLK", "name": "Technology", "category": "Sector"},
    {"yf": "GDX", "display": "GDX", "name": "Gold Miners", "category": "Sector"},
]

# Simple in-memory cache: {data, timestamp}
_cache = {"data": None, "ts": 0}
CACHE_TTL = 60  # seconds


def _fetch_all() -> dict:
    """Batch fetch all tickers in one yfinance call. Returns dict of results."""
    import yfinance as yf

    tickers = [w["yf"] for w in WATCHLIST]
    results = {}

    try:
        df = yf.download(tickers, period="5d", progress=False, threads=True)
        close = df["Close"]

        for item in WATCHLIST:
            sym = item["yf"]
            try:
                col = close[sym] if sym in close.columns else close.get(sym)
                if col is None or col.dropna().empty:
                    results[sym] = None
                    continue
                prices = col.dropna()
                last_price = float(prices.iloc[-1])
                prev_price = float(prices.iloc[-2]) if len(prices) >= 2 else None

                change = None
                change_pct = None
                if prev_price and prev_price != 0:
                    change = last_price - prev_price
                    change_pct = (change / prev_price) * 100

                results[sym] = {
                    "price": last_price,
                    "change": change,
                    "change_pct": change_pct,
                }
            except Exception:
                results[sym] = None
    except Exception as e:
        log.warning("yfinance batch download failed: %s", e)

    return results


def _build_categories(raw: dict) -> dict:
    """Build ordered category dict from raw yfinance data."""
    categories = {}
    for item in WATCHLIST:
        cat = item["category"]
        if cat not in categories:
            categories[cat] = []

        data = raw.get(item["yf"])
        if data and data.get("price"):
            price = data["price"]
            change_pct = data.get("change_pct")
            change = data.get("change")
            categories[cat].append({
                "name": item["name"],
                "ticker": item["display"],
                "price": f"{price:,.2f}",
                "change": f"{change:+.2f}" if change is not None else "--",
                "change_pct": f"{change_pct:+.2f}%" if change_pct is not None else "--",
                "is_positive": change_pct > 0 if change_pct is not None else None,
                "is_negative": change_pct < 0 if change_pct is not None else None,
            })
        else:
            categories[cat].append({
                "name": item["name"],
                "ticker": item["display"],
                "price": "--",
                "change": "--",
                "change_pct": "--",
                "is_positive": None,
                "is_negative": None,
            })
    return categories


@router.get("/market/monitor")
def market_monitor(request: Request):
    """Market monitor page — live data, auto-refreshes."""
    now = time.time()
    if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
        raw = _fetch_all()
        _cache["data"] = raw
        _cache["ts"] = now
    else:
        raw = _cache["data"]

    categories = _build_categories(raw)

    return templates.TemplateResponse("monitor.html", {
        "request": request,
        "categories": categories,
        "last_updated": datetime.now().strftime("%H:%M:%S"),
    })


@router.get("/api/v1/monitor")
def api_monitor():
    """JSON endpoint for AJAX refresh."""
    now = time.time()
    if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
        raw = _fetch_all()
        _cache["data"] = raw
        _cache["ts"] = now
    else:
        raw = _cache["data"]

    categories = _build_categories(raw)
    return JSONResponse({
        "categories": categories,
        "last_updated": datetime.now().strftime("%H:%M:%S"),
    })
