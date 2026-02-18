"""Mega Batch A: Bloomberg screener trusts (group 1 of 3)."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    # Heavy 485 filers
    "1644419": "Northern Lights Fund Trust IV",
    "1633061": "Amplify ETF Trust",
    "1454889": "Schwab Strategic Trust",
    "1547576": "Krane Shares Trust",
    "1804196": "BlackRock ETF Trust II",
    "1527428": "Arrow Investments Trust",
    "1581539": "Horizons ETF Trust",
    "1727074": "PGIM ETF Trust",
    "1616668": "Pacer Funds Trust",
    "1936157": "Elevation Series Trust",
    "836267": "SCM Trust",
    "1371571": "Invesco DB US Dollar Index Trust",
    # Light / crypto S-1
    "2064314": "21Shares Dogecoin ETF",
    "2028834": "21Shares Solana ETF",
    "2082889": "Bitwise Chainlink ETF",
    "2053791": "Bitwise Dogecoin ETF",
    "2078265": "Corgi ETF Trust I",
}

print(f"[MEGA A] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(ciks=list(BATCH.keys()), overrides=BATCH, user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com")
print(f"[MEGA A] Complete: {count} trusts processed.", flush=True)
