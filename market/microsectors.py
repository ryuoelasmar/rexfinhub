"""MicroSectors ETN true data overrides.

Bloomberg reports total issuances (not actual AUM) and zero flows for ETNs.
This module reads proprietary data from the 'microsector' and 'data_msector'
sheets in bloomberg_daily_file.xlsm to compute true AUM and fund flows
for 21 reliable MicroSectors tickers.

Integration: called after w1-w4 join in ingest.py to override AUM and flow
columns before any downstream transformations.

Sheet layouts:
  microsector   - Row 3: short tickers.  Rows 4+: Date | AUM per ticker (raw $)
  data_msector  - Shares (cols 0-32): Row 1 BBG IDs (NaN=unreliable), Row 3 short tickers
                  Prices (cols 34-55): Row 1 BBG IDs, data aligned by date
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

log = logging.getLogger(__name__)

# 21 reliable tickers (have BBG IDs in data_msector)
_RELIABLE_TICKERS = {
    "NRGU", "NRGD", "BNKU", "BNKD", "FNGA", "FNGD", "FNGO", "FNGS", "FNGU",
    "BULZ", "BERZ", "OILU", "OILD", "FLYU", "FLYD", "WTIU", "WTID",
    "SHNY", "DULL", "GDXU", "GDXD",
}

# Period lookback in trading days
_PERIOD_DAYS = {
    "fund_flow_1day": 1,
    "fund_flow_1week": 5,
    "fund_flow_1month": 21,
    "fund_flow_3month": 63,
    "fund_flow_6month": 126,
    "fund_flow_1year": 252,
    "fund_flow_3year": 756,
}


def read_overrides(xl: pd.ExcelFile) -> dict[str, dict]:
    """Read microsector + data_msector sheets, return per-ticker overrides.

    Returns::

        {
            "NRGU US": {
                "aum": 33.04,           # in millions
                "fund_flow_1day": 0.5,   # in millions
                "fund_flow_1week": ...,
                ...
                "aum_1": 31.2,           # 1 month ago, millions
                ...
            },
        }
    """
    if "microsector" not in xl.sheet_names:
        log.info("microsector sheet not found, skipping ETN overrides")
        return {}
    if "data_msector" not in xl.sheet_names:
        log.info("data_msector sheet not found, skipping ETN overrides")
        return {}

    try:
        aum_daily = _read_microsector_aum(xl)
        shares_daily, prices_daily = _read_data_msector(xl)
    except Exception as e:
        log.warning("Failed to read MicroSectors sheets: %s", e)
        return {}

    overrides = {}
    for ticker in _RELIABLE_TICKERS:
        ticker_us = f"{ticker} US"
        ov = {}

        # AUM from microsector sheet (raw dollars -> millions)
        if ticker in aum_daily.columns:
            aum_series = aum_daily[ticker].dropna()
            if not aum_series.empty:
                ov["aum"] = aum_series.iloc[-1] / 1e6
                ov.update(_monthly_aum_history(aum_series))

        # Flows from data_msector shares + prices
        if ticker in shares_daily.columns and ticker in prices_daily.columns:
            ov.update(_compute_flows(shares_daily[ticker], prices_daily[ticker]))

        if ov:
            overrides[ticker_us] = ov

    if overrides:
        # Log sample for validation
        sample = next(iter(overrides))
        sample_aum = overrides[sample].get("aum", "?")
        log.info("MicroSectors: %d tickers overridden (e.g. %s AUM=%.2fM)",
                 len(overrides), sample, sample_aum if isinstance(sample_aum, (int, float)) else 0)

    return overrides


def apply_overrides(df: pd.DataFrame, overrides: dict[str, dict]) -> pd.DataFrame:
    """Override AUM and flow columns for MicroSectors tickers.

    Works with both prefixed (t_w4.aum) and non-prefixed (aum) column names.
    """
    if not overrides:
        return df

    # Detect column prefix
    prefix = "t_w4." if "t_w4.aum" in df.columns else ""

    count = 0
    for ticker_us, vals in overrides.items():
        mask = df["ticker"] == ticker_us
        if not mask.any():
            continue
        for key, value in vals.items():
            col = f"{prefix}{key}"
            if col in df.columns:
                df.loc[mask, col] = value
        count += 1

    log.info("MicroSectors: applied overrides to %d tickers", count)
    return df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_microsector_aum(xl: pd.ExcelFile) -> pd.DataFrame:
    """Read microsector sheet: dates x tickers with daily AUM in raw dollars."""
    raw = xl.parse("microsector", header=None)

    # Row 3 has short tickers; filter to reliable set
    ticker_cols = {}
    for i in range(1, raw.shape[1]):
        val = raw.iloc[3, i]
        if pd.notna(val) and str(val).strip() in _RELIABLE_TICKERS:
            ticker_cols[i] = str(val).strip()

    if not ticker_cols:
        log.warning("microsector sheet: no reliable ticker columns found")
        return pd.DataFrame()

    cols = [0] + list(ticker_cols.keys())
    data = raw.iloc[4:, cols].copy()
    data.columns = ["Date"] + [ticker_cols[i] for i in ticker_cols]
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data = data.dropna(subset=["Date"])
    data = data.set_index("Date").sort_index()

    for col in data.columns:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    log.info("microsector AUM: %d dates x %d tickers, range %s to %s",
             len(data), len(data.columns),
             data.index[0].strftime("%Y-%m-%d") if len(data) else "?",
             data.index[-1].strftime("%Y-%m-%d") if len(data) else "?")
    return data


def _read_data_msector(xl: pd.ExcelFile) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read data_msector into separate shares and prices DataFrames.

    Only includes the 21 reliable tickers (those with BBG IDs in row 1).
    """
    raw = xl.parse("data_msector", header=None)

    # --- Shares section (cols 0-32) ---
    # Row 1 = BBG IDs (NaN for unreliable), Row 3 = short tickers
    shares_map = {}
    for i in range(1, min(33, raw.shape[1])):
        bbg = raw.iloc[1, i]
        short = raw.iloc[3, i]
        if pd.notna(bbg) and pd.notna(short) and str(short).strip() in _RELIABLE_TICKERS:
            shares_map[i] = str(short).strip()

    shares_cols = [0] + list(shares_map.keys())
    shares = raw.iloc[4:, shares_cols].copy()
    shares.columns = ["Date"] + [shares_map[i] for i in shares_map]
    shares["Date"] = pd.to_datetime(shares["Date"], errors="coerce")
    shares = shares.dropna(subset=["Date"])
    shares = shares.set_index("Date").sort_index()
    for col in shares.columns:
        shares[col] = pd.to_numeric(shares[col], errors="coerce")

    # --- Prices section (cols 34+) ---
    # Col 34 = date, cols 35+ = prices with BBG IDs in row 1
    prices_map = {}
    for i in range(35, min(56, raw.shape[1])):
        bbg = raw.iloc[1, i]
        if pd.notna(bbg) and "Equity" in str(bbg):
            short = str(bbg).split()[0].strip()
            if short in _RELIABLE_TICKERS:
                prices_map[i] = short

    prices_cols = [34] + list(prices_map.keys())
    prices = raw.iloc[4:, prices_cols].copy()
    prices.columns = ["Date"] + [prices_map[i] for i in prices_map]
    prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
    prices = prices.dropna(subset=["Date"])
    prices = prices.set_index("Date").sort_index()
    for col in prices.columns:
        prices[col] = pd.to_numeric(prices[col], errors="coerce")

    log.info("data_msector: shares %d dates x %d tickers, prices %d dates x %d tickers",
             len(shares), len(shares.columns), len(prices), len(prices.columns))
    return shares, prices


def _compute_flows(shares: pd.Series, prices: pd.Series) -> dict[str, float]:
    """Compute fund flows from daily shares and prices.

    Flow_t = (shares_t - shares_{t-1}) * price_t
    Period flow = sum of daily flows over the lookback window.
    Returns values in millions.
    """
    combined = pd.DataFrame({"shares": shares, "prices": prices}).dropna()
    if len(combined) < 2:
        return {}

    delta_shares = combined["shares"].diff()
    daily_flow = delta_shares * combined["prices"]
    daily_flow = daily_flow.iloc[1:]  # drop first NaN row

    if daily_flow.empty:
        return {}

    today = combined.index[-1]
    flows = {}

    # Fixed-count lookbacks
    for key, n_days in _PERIOD_DAYS.items():
        n = min(n_days, len(daily_flow))
        flows[key] = daily_flow.iloc[-n:].sum() / 1e6

    # YTD: from Jan 1 of current year
    year_start = pd.Timestamp(datetime(today.year, 1, 1))
    flows["fund_flow_ytd"] = daily_flow[daily_flow.index >= year_start].sum() / 1e6

    return flows


def _monthly_aum_history(aum_series: pd.Series) -> dict[str, float]:
    """Compute monthly AUM snapshots (aum_1 through aum_36) in millions.

    aum_1 = AUM ~1 month ago, aum_2 = ~2 months ago, etc.
    Uses the closest available date at or before the target.
    """
    result = {}
    today = aum_series.index[-1]

    for i in range(1, 37):
        target = today - pd.DateOffset(months=i)
        valid = aum_series[aum_series.index <= target]
        if not valid.empty:
            result[f"aum_{i}"] = valid.iloc[-1] / 1e6

    return result
