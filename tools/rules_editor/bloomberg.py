"""Bloomberg data loader for validation cross-reference.

Loads ETP tickers + issuers from the Bloomberg daily file via market/ingest,
without any FastAPI dependency.
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@st.cache_data(ttl=300)
def load_bloomberg_tickers() -> pd.DataFrame | None:
    """Load ETP tickers from Bloomberg daily file.

    Returns DataFrame with at least columns: ticker, issuer, t_w1.aum
    Returns None if Bloomberg file not found.
    """
    try:
        from market.ingest import read_input
        result = read_input()
        etp = result["etp_combined"]
        # Normalize column names -- ingest prefixes vary
        cols = {}
        for c in etp.columns:
            if c == "ticker":
                cols[c] = "ticker"
            elif "issuer" in c.lower():
                cols[c] = "issuer"
            elif "aum" in c.lower() and "aum" not in cols.values():
                cols[c] = "aum"
        df = etp.rename(columns=cols)
        keep = [c for c in ["ticker", "issuer", "aum"] if c in df.columns]
        if "ticker" not in keep:
            return None
        return df[keep].copy()
    except Exception as e:
        st.warning(f"Could not load Bloomberg data: {e}")
        return None
