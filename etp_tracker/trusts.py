"""
ETP Trust CIK Registry

Each trust we monitor for filings. CIKs sourced from SEC EDGAR.
"""
from __future__ import annotations

# CIK -> Trust Name (override SEC's name if needed)
TRUST_CIKS = {
    "2043954": "REX ETF Trust",
    "1424958": "Direxion Shares ETF Trust",
    "1064642": "ProShares Trust",
    "1592900": "GraniteShares ETF Trust",
    "1683471": "Volatility Shares Trust",
    "1976517": "Roundhill ETF Trust",
    "1714899": "Tidal Trust II",
    "1547950": "ETF Series Solutions",
    "1924868": "Themes ETF Trust",
    "1771146": "ETF Opportunities Trust",  # Tuttle/T-REX products
    "1355064": "Exchange Traded Concepts Trust",
    "1587982": "Investment Managers Series Trust II",
    "1040587": "Direxion Funds",
    "1479026": "Exchange Listed Funds Trust",
}

def get_all_ciks() -> list[str]:
    """Return list of all CIKs to track."""
    return list(TRUST_CIKS.keys())

def get_overrides() -> dict[str, str]:
    """Return CIK -> Trust Name overrides."""
    return TRUST_CIKS.copy()
