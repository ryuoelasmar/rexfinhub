"""
Scrape total return data from TotalRealReturns.com.

Fetches normalized total return prices and drawdown data for any set of
tickers. Data is embedded in the HTML as JavaScript arrays — no API key needed.

Usage:
    python scripts/scrape_total_returns.py NVII,NVDY,NVYY
    python scripts/scrape_total_returns.py SPY,QQQ --start 2020-01-01
    python scripts/scrape_total_returns.py FEPI,JEPI,JEPQ --json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
BASE_URL = "https://totalrealreturns.com/n/"


def fetch_page(symbols: list[str], start: str = "", end: str = "") -> str:
    """Fetch the TotalRealReturns page HTML for given symbols."""
    url = BASE_URL + ",".join(symbols)
    params = []
    if start:
        params.append(f"start={start}")
    if end:
        params.append(f"end={end}")
    if params:
        url += "?" + "&".join(params)

    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def parse_dates(html: str) -> list[date]:
    """Extract and decode the delta-encoded date array from HTML."""
    m = re.search(r"let sharedDatesColumnInput = \[([\d,]+)\]", html)
    if not m:
        return []

    deltas = [int(x) for x in m.group(1).split(",")]
    if not deltas:
        return []

    # First value = days since Unix epoch. Rest = day deltas.
    epoch = date(1970, 1, 1)
    day_num = deltas[0]
    dates = [epoch + timedelta(days=day_num)]
    for d in deltas[1:]:
        day_num += d
        dates.append(epoch + timedelta(days=day_num))

    return dates


def parse_series(html: str, num_dates: int) -> list[list[float]]:
    """Extract all data series that match the date array length."""
    # Find all numeric arrays in the HTML
    all_arrays = re.findall(r"\[([\d\.\-e,]+)\]", html)

    series = []
    for arr_str in all_arrays:
        vals_str = arr_str.split(",")
        if len(vals_str) == num_dates:
            try:
                vals = [float(v) for v in vals_str]
                # Skip the dates array itself (first value would be huge)
                if vals[0] < 10000:
                    series.append(vals)
            except ValueError:
                continue

    return series


def parse_symbol_names(html: str, expected: list[str]) -> list[str]:
    """Extract symbol names. Falls back to expected list."""
    m = re.search(r'<title>([^<]+)</title>', html)
    if m:
        title = m.group(1)
        symbols_part = title.split(" - ")[0].strip()
        parsed = [s.strip() for s in symbols_part.split(",")]
        # Only use parsed if they look like tickers (short, uppercase)
        if all(len(s) <= 10 and s == s.upper() for s in parsed):
            return parsed
    return expected


def _clean_text(s: str) -> str:
    """Strip HTML tags, normalize whitespace, fix entities."""
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&times;", "").replace("&mdash;", "-").replace("&minus;", "-")
    s = s.replace("\u2212", "-").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_pct(s: str) -> float | None:
    """Parse a percentage string like '+40.30%' or '-4.66%' to float."""
    s = _clean_text(s).replace("%", "").replace("+", "").replace(",", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_structured_stats(html: str, symbols: list[str]) -> dict:
    """Extract all stats into structured format per symbol."""
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)

    result = {sym: {} for sym in symbols}

    for table_html in tables:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
        if len(rows) < 2:
            continue

        headers_raw = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rows[0], re.DOTALL)
        headers = [_clean_text(h) for h in headers_raw]

        for row_html in rows[1:]:
            cells_raw = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.DOTALL)
            cells = [_clean_text(c) for c in cells_raw]
            if len(cells) != len(headers):
                continue

            # Detect table type from headers
            if "Overall Return" in " ".join(headers):
                # Return stats table: Symbol | Overall Return | Trendline
                sym = cells[0].split()[0] if cells[0] else ""
                if sym in result:
                    parts = cells[1].split() if len(cells) > 1 else []
                    result[sym]["overall_return"] = _parse_pct(parts[0]) if parts else None
                    result[sym]["annualized_return"] = _parse_pct(parts[1].replace("/yr", "")) if len(parts) > 1 else None
                    if len(cells) > 2:
                        trend_parts = cells[2].split()
                        result[sym]["trendline_rate"] = _parse_pct(trend_parts[0].replace("/yr", "")) if trend_parts else None
                        r2_match = re.search(r"R2=([\d.]+)", cells[2])
                        result[sym]["r_squared"] = float(r2_match.group(1)) if r2_match else None

            elif "Start Value" in " ".join(headers) or "End Value" in " ".join(headers):
                # Growth of 10K table
                sym = cells[0].split()[0] if cells[0] else ""
                if sym in result:
                    for i, h in enumerate(headers):
                        if "End" in h and i < len(cells):
                            val = cells[i].replace("$", "").replace(",", "").split()[0]
                            try:
                                result[sym]["growth_of_10k"] = float(val)
                            except ValueError:
                                pass

            elif "Current Drawdown" in " ".join(headers):
                # Drawdown table
                sym = cells[0].split()[0] if cells[0] else ""
                if sym in result:
                    result[sym]["current_drawdown"] = _parse_pct(cells[1]) if len(cells) > 1 else None
                    if len(cells) > 2:
                        worst_parts = cells[2].split()
                        result[sym]["worst_drawdown"] = _parse_pct(worst_parts[0]) if worst_parts else None
                        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", cells[2])
                        result[sym]["worst_drawdown_date"] = date_match.group(1) if date_match else None

            elif "YTD Return" in " ".join(headers):
                # Symbol summary table
                sym = cells[0].split()[0] if cells[0] else ""
                if sym in result:
                    result[sym]["ytd_return"] = _parse_pct(cells[1]) if len(cells) > 1 else None
                    if len(cells) > 2:
                        tr_match = re.search(r"([\d.]+)\s*TR", cells[2])
                        result[sym]["total_return_price"] = float(tr_match.group(1)) if tr_match else None

            elif "Year" in headers[0]:
                # Annual returns table
                for i, sym in enumerate(symbols):
                    col_idx = i + 1
                    if col_idx < len(cells):
                        year = cells[0].split()[0]
                        val = _parse_pct(cells[col_idx])
                        if "annual_returns" not in result[sym]:
                            result[sym]["annual_returns"] = {}
                        result[sym]["annual_returns"][year] = val

    return result


def _scrape_batch(symbols: list[str], start: str = "", end: str = "") -> dict:
    """Scrape a single batch (up to ~4 symbols) from TotalRealReturns."""
    html = fetch_page(symbols, start, end)

    dates = parse_dates(html)
    if not dates:
        return {"dates": [], "growth_series": {}, "stats": {}}

    all_series = parse_series(html, len(dates))
    parsed_symbols = parse_symbol_names(html, symbols)

    growth_series = {}
    for i, sym in enumerate(parsed_symbols):
        if i < len(all_series):
            growth_series[sym] = all_series[i]

    stats = parse_structured_stats(html, parsed_symbols)
    date_strs = [d.isoformat() for d in dates]

    return {"dates": date_strs, "growth_series": growth_series, "stats": stats}


def scrape(symbols: list[str], start: str = "", end: str = "") -> dict:
    """
    Scrape TotalRealReturns.com for given symbols.
    Handles batching in pairs to get full stats for all symbols.
    Supports up to 10 symbols.

    Returns:
        {
            "symbols": ["NVII", "NVDY", ...],
            "dates": ["2025-05-28", ...],
            "growth_series": {"NVII": [1.0, 1.037, ...], ...},
            "stats": {"NVII": {"overall_return": 40.3, ...}, ...},
            "data_points": 213,
            "date_range": ["2025-05-28", "2026-04-01"],
        }
    """
    import time

    if len(symbols) > 10:
        return {"error": "Max 10 symbols supported", "symbols": symbols}

    # Batch in pairs for full stats coverage
    # First request: all symbols together (gets chart data for all)
    main_result = _scrape_batch(symbols, start, end)
    if not main_result["dates"]:
        return {"error": "No date data found", "symbols": symbols}

    all_growth = dict(main_result["growth_series"])
    all_stats = dict(main_result["stats"])

    # For symbols missing stats, fetch in pairs
    missing = [s for s in symbols if s not in all_stats or not all_stats[s].get("overall_return")]
    if missing:
        # Batch missing symbols in pairs
        for i in range(0, len(missing), 2):
            batch = missing[i:i+2]
            if len(batch) == 1:
                batch = batch + [symbols[0]]  # pair with first symbol for comparison
            time.sleep(0.5)  # polite delay
            try:
                batch_result = _scrape_batch(batch, start, end)
                for sym in batch:
                    if sym in batch_result["stats"] and batch_result["stats"][sym].get("overall_return"):
                        all_stats[sym] = batch_result["stats"][sym]
                    if sym in batch_result["growth_series"] and sym not in all_growth:
                        all_growth[sym] = batch_result["growth_series"][sym]
            except Exception:
                pass  # best effort for supplementary batches

    return {
        "symbols": symbols,
        "dates": main_result["dates"],
        "growth_series": all_growth,
        "stats": all_stats,
        "data_points": len(main_result["dates"]),
        "date_range": [main_result["dates"][0], main_result["dates"][-1]] if main_result["dates"] else [],
    }


def save_to_disk(result: dict, output_dir: Path | None = None) -> Path:
    """Save scraped data to D: drive (or local fallback) as JSON + CSV."""
    import csv
    from datetime import date as dt_date

    if output_dir is None:
        d_path = Path("D:/sec-data/archives/total_returns")
        if d_path.parent.exists():
            output_dir = d_path
        else:
            output_dir = PROJECT_ROOT / "data" / "total_returns"

    output_dir.mkdir(parents=True, exist_ok=True)

    today = dt_date.today().isoformat()
    symbols_key = "_".join(result["symbols"][:5])

    # Save full JSON
    json_path = output_dir / f"{symbols_key}_{today}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # Save growth series as CSV (dates x symbols)
    csv_path = output_dir / f"{symbols_key}_{today}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date"] + result["symbols"])
        for i, d in enumerate(result["dates"]):
            row = [d]
            for sym in result["symbols"]:
                series = result["growth_series"].get(sym, [])
                row.append(series[i] if i < len(series) else "")
            writer.writerow(row)

    print(f"  Saved: {json_path}")
    print(f"  Saved: {csv_path}")
    return json_path


def main():
    parser = argparse.ArgumentParser(description="Scrape TotalRealReturns.com")
    parser.add_argument("symbols", help="Comma-separated tickers (e.g. NVII,NVDY)")
    parser.add_argument("--start", default="", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="", help="End date YYYY-MM-DD")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--save", action="store_true", help="Save to D: drive")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    print(f"Scraping TotalRealReturns for: {', '.join(symbols)}")

    result = scrape(symbols, args.start, args.end)

    if args.save:
        save_to_disk(result)

    if args.json:
        sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', errors='replace', closefd=False)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"  Date range: {result['date_range'][0]} to {result['date_range'][1]}")
        print(f"  Data points: {result['data_points']}")
        print()

        for sym in result["symbols"]:
            growth = result["growth_series"].get(sym, [])
            st = result["stats"].get(sym, {})
            print(f"  {sym}:")
            if growth:
                total_return = (growth[-1] / growth[0] - 1) * 100 if growth[0] != 0 else 0
                print(f"    Total Return: {total_return:+.2f}%")
            if st.get("annualized_return") is not None:
                print(f"    Annualized: {st['annualized_return']:+.2f}%")
            if st.get("growth_of_10k") is not None:
                print(f"    Growth of $10K: ${st['growth_of_10k']:,.2f}")
            if st.get("current_drawdown") is not None:
                print(f"    Current Drawdown: {st['current_drawdown']:.2f}%")
            if st.get("worst_drawdown") is not None:
                print(f"    Worst Drawdown: {st['worst_drawdown']:.2f}% ({st.get('worst_drawdown_date', '')})")
            if st.get("annual_returns"):
                print(f"    Annual Returns:")
                for year, ret in sorted(st["annual_returns"].items(), reverse=True):
                    print(f"      {year}: {ret:+.2f}%" if ret is not None else f"      {year}: --")
            print()


if __name__ == "__main__":
    main()
