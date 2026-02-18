"""Batch C: Multi-strategy + remaining crypto S-1 group 3."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    "1579982": "ARK ETF Trust",
    "1655589": "Franklin Templeton ETF Trust",
    "1657201": "Invesco Exchange-Traded Self-Indexed Fund Trust",
    "1419139": "Invesco India Exchange-Traded Fund Trust",
    "1595386": "Invesco Actively Managed Exchange-Traded Commodity Fund Trust",
    "1877493": "Valkyrie ETF Trust II",
    # Crypto S-1 (fast - 0 pipeline filings)
    "1860788": "VanEck Ethereum ETF",
    "2011535": "Franklin Ethereum Trust",
    "1732409": "Grayscale Bitcoin Cash Trust",
    "1705181": "Grayscale Ethereum Classic Trust",
    "1732406": "Grayscale Litecoin Trust",
    "1896677": "Grayscale Solana Staking ETF",
    "2037427": "Grayscale XRP Trust ETF",
    "1723788": "Bitwise 10 Crypto Index ETF",
}

print(f"[BATCH C] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(
    ciks=list(BATCH.keys()),
    overrides=BATCH,
    user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com",
)
print(f"[BATCH C] Complete: {count} trusts processed.", flush=True)
