"""
ETP Trust CIK Registry

Each trust we monitor for filings. CIKs sourced from SEC EDGAR.
"""
from __future__ import annotations

# CIK -> Trust Name (override SEC's name if needed)
# Verified against SEC EDGAR on 2026-02-06
# To add a new trust:
#   1. Search https://efts.sec.gov/LATEST/search-index?q="Trust+Name"&forms=485BPOS
#   2. Get the CIK from the result
#   3. Verify at https://data.sec.gov/submissions/CIK{padded_10_digits}.json
#   4. Add entry below and re-run pipeline
TRUST_CIKS = {
    "2043954": "REX ETF Trust",
    "1424958": "Direxion Shares ETF Trust",
    "1040587": "Direxion Funds",
    "1174610": "ProShares Trust",
    "1689873": "GraniteShares ETF Trust",
    "1884021": "Volatility Shares Trust",
    "1976517": "Roundhill ETF Trust",
    "1924868": "Tidal Trust II",
    "1540305": "ETF Series Solutions",
    "1976322": "Themes ETF Trust",
    "1771146": "ETF Opportunities Trust",  # Tuttle/T-REX products
    "1452937": "Exchange Traded Concepts Trust",
    "1587982": "Investment Managers Series Trust II",
    "1547950": "Exchange Listed Funds Trust",
}

def get_all_ciks() -> list[str]:
    """Return list of all CIKs to track."""
    return list(TRUST_CIKS.keys())

def get_overrides() -> dict[str, str]:
    """Return CIK -> Trust Name overrides."""
    return TRUST_CIKS.copy()
