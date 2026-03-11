"""Excel ingest: read bloomberg_daily_file.xlsm sheets (w1-w4, s1) into canonical format."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from market.config import (
    DATA_FILE,
    W2_PREFIX, W3_PREFIX, W4_PREFIX,
    SHEET_W1, SHEET_W2, SHEET_W3, SHEET_W4, SHEET_S1, SHEET_MKT_STATUS,
    W1_COL_MAP, W2_COL_MAP, W3_COL_MAP, W4_FLOW_COL_MAP,
)

log = logging.getLogger(__name__)


def read_input(data_file: Path | str | None = None) -> dict:
    """Read bloomberg_daily_file.xlsm (w1/w2/w3/w4/s1 sheets).

    Returns dict with keys:
    - etp_combined: all ETP sheets joined on ticker with prefixed columns
    - stock_data: raw s1 sheet
    - mkt_status: market status reference (if present)
    - source_path: str path to input file
    """
    path = Path(data_file) if data_file else DATA_FILE
    if not path.exists():
        raise FileNotFoundError(f"Input data file not found: {path}")

    log.info("Reading input: %s", path)
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheets = xl.sheet_names

    if SHEET_W1 not in sheets:
        raise ValueError(
            f"bloomberg_daily_file missing required w1 sheet. Found: {sheets}"
        )

    etp, stock, mkt_status = _read_bbg_format(xl)

    # NOTE: MicroSectors ETN proprietary overrides are NOT applied here.
    # Website gets Bloomberg-reported ETN data as-is.
    # Overrides are applied only in report_data.py for internal email reports.

    return {
        "etp_combined": etp,
        "stock_data": stock,
        "mkt_status": mkt_status,
        "source_path": str(path),
    }


# ---------------------------------------------------------------------------
# New BBG format (w1/w2/w3/w4/s1/mkt_status)
# ---------------------------------------------------------------------------

def _read_bbg_format(xl: pd.ExcelFile) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read new BBG format with abbreviated column names.

    Returns (etp_combined, stock_data, mkt_status).
    """
    # --- w1: base data ---
    w1 = _read_sheet(xl, SHEET_W1)
    w1 = w1.rename(columns=W1_COL_MAP)
    w1 = w1.dropna(subset=["ticker"])
    log.info("w1 (base): %d rows x %d cols", *w1.shape)

    # --- w2: metrics ---
    w2 = _read_sheet(xl, SHEET_W2)
    w2 = w2.rename(columns=W2_COL_MAP)
    # Drop Fund Name column if present (duplicate of w1)
    w2 = w2.drop(columns=["Fund Name", "fund_name"], errors="ignore")
    w2 = w2.dropna(subset=["ticker"])
    log.info("w2 (metrics): %d rows x %d cols", *w2.shape)

    # --- w3: returns ---
    w3 = _read_sheet(xl, SHEET_W3)
    w3 = w3.rename(columns=W3_COL_MAP)
    w3 = w3.drop(columns=["Fund Name", "fund_name"], errors="ignore")
    w3 = w3.dropna(subset=["ticker"])
    log.info("w3 (returns): %d rows x %d cols", *w3.shape)

    # --- w4: flows + AUM history ---
    w4 = _read_sheet(xl, SHEET_W4)
    w4 = _process_w4(w4)
    log.info("w4 (flows+AUM): %d rows x %d cols", *w4.shape)

    # Apply t_w2./t_w3./t_w4. prefixes on non-ticker columns
    w2_rename = {c: f"{W2_PREFIX}{c}" for c in w2.columns if c != "ticker"}
    w3_rename = {c: f"{W3_PREFIX}{c}" for c in w3.columns if c != "ticker"}
    w4_rename = {c: f"{W4_PREFIX}{c}" for c in w4.columns if c != "ticker"}

    w2 = w2.rename(columns=w2_rename)
    w3 = w3.rename(columns=w3_rename)
    w4 = w4.rename(columns=w4_rename)

    # Join all 4 sheets on ticker (left from w1)
    combined = w1
    combined = combined.merge(w2, on="ticker", how="left")
    combined = combined.merge(w3, on="ticker", how="left")
    combined = combined.merge(w4, on="ticker", how="left")

    log.info("ETP combined (BBG): %d rows x %d cols", *combined.shape)

    # --- s1: stock data ---
    stock = pd.DataFrame()
    if SHEET_S1 in xl.sheet_names:
        stock = _read_sheet(xl, SHEET_S1)
        log.info("s1 (stock): %d rows x %d cols", *stock.shape)

    # --- mkt_status: reference ---
    mkt_status = pd.DataFrame()
    if SHEET_MKT_STATUS in xl.sheet_names:
        mkt_status = _read_sheet(xl, SHEET_MKT_STATUS)
        log.info("mkt_status: %d rows", len(mkt_status))

    return combined, stock, mkt_status


def _process_w4(w4: pd.DataFrame) -> pd.DataFrame:
    """Process w4 sheet: rename flow columns and positionally rename AUM columns.

    W4 layout:
    - Col 0: Ticker
    - Col 1: Fund Name (drop)
    - Cols 2-9: 8 flow columns
    - Col 10: AUM current (Formula Col. 1 or similar)
    - Cols 11-46: AUM history (aum_1 through aum_36)
    """
    # First rename known flow columns
    w4 = w4.rename(columns=W4_FLOW_COL_MAP)

    # Drop Fund Name if present
    w4 = w4.drop(columns=["Fund Name", "fund_name"], errors="ignore")

    # Drop null tickers
    w4 = w4.dropna(subset=["ticker"])

    # Positionally rename AUM columns (indices 9+ after ticker + 8 flows)
    # Find the position after the known columns
    known_cols = {"ticker"} | {v for v in W4_FLOW_COL_MAP.values() if v != "ticker"}
    remaining_cols = [c for c in w4.columns if c not in known_cols]

    # These remaining columns are the AUM columns (positional: current, then 1-36 months back)
    aum_names = ["aum"] + [f"aum_{i}" for i in range(1, 37)]

    if remaining_cols:
        rename_map = {}
        for i, col in enumerate(remaining_cols):
            if i < len(aum_names):
                rename_map[col] = aum_names[i]
        w4 = w4.rename(columns=rename_map)
        log.info("  w4 AUM columns renamed: %d cols (of %d remaining)",
                 len(rename_map), len(remaining_cols))

    return w4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_sheet(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """Read a sheet, stripping whitespace from column names."""
    df = xl.parse(sheet)
    df.columns = [str(c).strip() for c in df.columns]
    return df
