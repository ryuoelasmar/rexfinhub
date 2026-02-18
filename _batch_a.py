"""Batch A: Pipeline-compatible trusts + crypto S-1 group 1."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    "1667919": "First Trust Exchange-Traded Fund VIII",  # Crashed in prior run
    "1742912": "Tidal Trust I",
    "1592900": "EA Series Trust",
    "1378872": "Invesco Exchange-Traded Fund Trust II",
    "1418144": "Invesco Actively Managed Exchange-Traded Fund Trust",
    "1067839": "Invesco QQQ Trust Series 1",
    "1432353": "Global X Funds",
    "1976672": "Grayscale Funds Trust",
    # Crypto S-1 (fast - 0 pipeline filings)
    "1588489": "Grayscale Bitcoin Trust ETF",
    "2015034": "Grayscale Bitcoin Mini Trust ETF",
    "1980994": "iShares Bitcoin Trust ETF",
    "1852317": "Fidelity Wise Origin Bitcoin Fund",
    "1838028": "VanEck Bitcoin ETF",
    "1763415": "Bitwise Bitcoin ETF",
    "1992870": "Franklin Templeton Digital Holdings Trust",
}

print(f"[BATCH A] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(
    ciks=list(BATCH.keys()),
    overrides=BATCH,
    user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com",
)
print(f"[BATCH A] Complete: {count} trusts processed.", flush=True)
