"""Batch B: Income/covered call trusts + crypto S-1 group 2."""
import sys, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

BATCH = {
    "1485894": "J.P. Morgan Exchange-Traded Fund Trust",
    "1479026": "Goldman Sachs ETF Trust",
    "1882879": "Goldman Sachs ETF Trust II",
    "1848758": "NEOS ETF Trust",
    "1810747": "Simplify Exchange Traded Funds",
    "1137360": "VanEck ETF Trust",
    "1350487": "WisdomTree Trust",
    # Crypto S-1 (fast - 0 pipeline filings)
    "1869699": "Ark 21Shares Bitcoin ETF",
    "1841175": "CoinShares Bitcoin ETF",
    "1850391": "WisdomTree Bitcoin Fund",
    "1725210": "Grayscale Ethereum Staking ETF",
    "2020455": "Grayscale Ethereum Staking Mini ETF",
    "2000638": "iShares Ethereum Trust ETF",
    "2000046": "Fidelity Ethereum Fund",
}

print(f"[BATCH B] Starting {len(BATCH)} trusts...", flush=True)
from etp_tracker.run_pipeline import run_pipeline
count = run_pipeline(
    ciks=list(BATCH.keys()),
    overrides=BATCH,
    user_agent="REX-ETP-Tracker/2.0 relasmar@rexfin.com",
)
print(f"[BATCH B] Complete: {count} trusts processed.", flush=True)
