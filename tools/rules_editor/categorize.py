"""Auto-categorization engine for the rules editor.

Loads Bloomberg data, runs market/auto_classify.py, and manages
fund_mapping.csv with expanded category support (14 strategies).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st

from market.auto_classify import classify_to_dataframe
from market.config import ETP_CATEGORY_TO_STRATEGY, RULES_DIR

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Category vocabulary
# ---------------------------------------------------------------------------

# Strategy name (auto_classify output) -> etp_category code (fund_mapping)
STRATEGY_TO_CODE: dict[str, str] = {
    v: k for k, v in ETP_CATEGORY_TO_STRATEGY.items()
}
# New categories use their display name as the code
for _name in [
    "Broad Beta", "Fixed Income", "Sector", "Commodity",
    "International", "Alternative", "Multi-Asset", "Currency",
]:
    STRATEGY_TO_CODE[_name] = _name
STRATEGY_TO_CODE["Unclassified"] = "Unclassified"

# All valid etp_category values (legacy short codes + new names)
VALID_CATEGORIES = [
    "LI", "CC", "Crypto", "Defined", "Thematic",
    "Broad Beta", "Fixed Income", "Sector", "Commodity",
    "International", "Alternative", "Multi-Asset", "Currency",
]

# Human-readable labels
CATEGORY_LABELS: dict[str, str] = {
    "LI": "Leveraged & Inverse",
    "CC": "Income / Covered Call",
    "Crypto": "Crypto",
    "Defined": "Defined Outcome",
    "Thematic": "Thematic",
    "Broad Beta": "Broad Beta",
    "Fixed Income": "Fixed Income",
    "Sector": "Sector",
    "Commodity": "Commodity",
    "International": "International",
    "Alternative": "Alternative",
    "Multi-Asset": "Multi-Asset",
    "Currency": "Currency",
}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner="Loading Bloomberg data...")
def load_bloomberg() -> pd.DataFrame:
    """Load and join Bloomberg ETP data (w1-w4), filtered to active ETPs."""
    from market.ingest import read_input

    data = read_input()
    etp = data.get("etp_combined", pd.DataFrame())
    if etp.empty:
        return etp

    # Filter to ACTV ETPs (ETF + ETN)
    mkt_col = next((c for c in etp.columns if c.lower() == "market_status"), None)
    if mkt_col:
        etp = etp[etp[mkt_col] == "ACTV"]
    ft_col = next((c for c in etp.columns if c.lower() == "fund_type"), None)
    if ft_col:
        etp = etp[etp[ft_col].isin(["ETF", "ETN"])]

    return etp.reset_index(drop=True)


def run_classification(etp_combined: pd.DataFrame) -> pd.DataFrame:
    """Auto-classify all tickers. Returns DataFrame with strategy + context.

    Columns: ticker, strategy, etp_category, confidence, reason,
             underlier_type, fund_name, issuer, asset_class_focus, aum
    """
    classified = classify_to_dataframe(etp_combined)

    # Map strategy -> etp_category code
    classified["etp_category"] = (
        classified["strategy"].map(STRATEGY_TO_CODE).fillna("Unclassified")
    )

    # Add fund context columns from Bloomberg
    deduped = etp_combined.drop_duplicates(subset=["ticker"], keep="first")
    for col in ["fund_name", "issuer", "asset_class_focus"]:
        if col in deduped.columns:
            vals = deduped.set_index("ticker")[col]
            classified[col] = classified["ticker"].map(vals)

    # AUM (may be prefixed t_w4.aum or just aum)
    aum_col = next(
        (c for c in etp_combined.columns if c in ("aum", "t_w4.aum")), None
    )
    if aum_col:
        vals = deduped.set_index("ticker")[aum_col]
        classified["aum"] = classified["ticker"].map(vals)
    else:
        classified["aum"] = 0

    return classified


# ---------------------------------------------------------------------------
# Fund mapping I/O
# ---------------------------------------------------------------------------

def load_fund_mapping() -> pd.DataFrame:
    """Load fund_mapping.csv with backward-compatible column handling."""
    path = RULES_DIR / "fund_mapping.csv"
    if not path.exists():
        return pd.DataFrame(columns=["ticker", "etp_category", "is_primary", "source"])

    df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    if df.empty:
        return pd.DataFrame(columns=["ticker", "etp_category", "is_primary", "source"])

    # Add missing columns
    if "is_primary" not in df.columns:
        df["is_primary"] = (~df.duplicated(subset=["ticker"], keep="first")).astype(int)
    if "source" not in df.columns:
        df["source"] = "manual"

    return df


def save_fund_mapping(df: pd.DataFrame) -> Path:
    """Save fund_mapping.csv. Returns file path."""
    path = RULES_DIR / "fund_mapping.csv"
    cols = ["ticker", "etp_category"]
    for extra in ("is_primary", "source"):
        if extra in df.columns:
            cols.append(extra)
    df[cols].to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Coverage statistics
# ---------------------------------------------------------------------------

def get_coverage_stats(
    classified: pd.DataFrame, fund_mapping: pd.DataFrame
) -> dict:
    """Compute coverage stats for the dashboard."""
    total = classified["ticker"].nunique()
    mapped_set = set(fund_mapping["ticker"].astype(str).str.strip()) if not fund_mapping.empty else set()
    classified_set = set(classified["ticker"].astype(str).str.strip())
    mapped = len(mapped_set & classified_set)

    # Confidence breakdown for unmapped
    unmapped_df = classified[~classified["ticker"].isin(mapped_set)]
    conf = unmapped_df["confidence"].value_counts().to_dict() if not unmapped_df.empty else {}

    # Category distributions
    manual_cats = fund_mapping["etp_category"].value_counts().to_dict() if not fund_mapping.empty else {}
    auto_cats = unmapped_df["etp_category"].value_counts().to_dict() if not unmapped_df.empty else {}

    return {
        "total": total,
        "mapped": mapped,
        "unmapped": total - mapped,
        "pct_mapped": round(100 * mapped / total, 1) if total else 0,
        "confidence": conf,
        "manual_categories": manual_cats,
        "auto_categories": auto_cats,
    }
