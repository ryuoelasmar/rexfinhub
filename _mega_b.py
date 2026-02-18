"""Mega Batch B: Bloomberg screener trusts (group 2 of 3)."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    # Heavy 485 filers
    "1408970": "AdvisorShares Trust",
    "1064641": "Select Sector SPDR Trust",
    "1719812": "Collaborative Investment Series Trust",
    "1797318": "AIM ETF Products Trust",
    "1415311": "ProShares Trust II",
    "1501825": "Hartford Funds Exchange-Traded Trust",
    "1676326": "Morgan Stanley ETF Trust",
    "768847": "VanEck Funds",
    "1898391": "Fidelity Greenwood Street Trust",
    "1580843": "WEBs ETF Trust",
    "1529505": "United States Commodity Funds Trust I",
    # Light / crypto S-1
    "2045872": "Bitwise Solana Staking ETF",
    "2039525": "Bitwise XRP ETF",
    "2063380": "Fidelity Solana Fund",
    "2033807": "Franklin Crypto Trust",
    "2074409": "Invesco Galaxy Solana ETF",
    "1767057": "Osprey Bitcoin Trust",
    "1985840": "Tidal Commodities Trust I",
}

print(f"[MEGA B] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(ciks=list(BATCH.keys()), overrides=BATCH, user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com")
print(f"[MEGA B] Complete: {count} trusts processed.", flush=True)
