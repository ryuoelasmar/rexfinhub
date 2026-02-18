"""Mega Batch C: Bloomberg screener trusts (group 3 of 3)."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    # Heavy 485 filers
    "1039803": "ProFunds",
    "1537140": "Northern Lights Fund Trust III",
    "945908": "Fidelity Covington Trust",
    "1650149": "Series Portfolios Trust",
    "1496608": "AB Active ETFs, Inc.",
    "1516212": "SSGA Active Trust",
    "1795351": "T. Rowe Price Exchange-Traded Funds, Inc.",
    "1761055": "BlackRock ETF Trust",
    "1970751": "Advisor Managed Portfolios",
    "1040612": "Madison Funds",
    "1506001": "Neuberger Berman ETF Trust",
    "1506213": "Strategy Shares",
    # Light / crypto S-1
    "2039505": "Canary XRP ETF",
    "2039458": "Canary HBAR ETF",
    "2039461": "Canary Litecoin ETF",
    "2041869": "Canary Marinade Solana ETF",
    "1345125": "Cyber Hornet Trust",
}

print(f"[MEGA C] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(ciks=list(BATCH.keys()), overrides=BATCH, user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com")
print(f"[MEGA C] Complete: {count} trusts processed.", flush=True)
