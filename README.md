# ETP Filing Tracker

SEC EDGAR filing tracker for Exchange-Traded Products (ETPs). Monitors prospectus filings, extracts fund details, and tracks effective dates and name changes.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline for all trusts
python -c "
from etp_tracker.run_pipeline import run_pipeline
from etp_tracker.trusts import get_all_ciks, get_overrides

run_pipeline(
    ciks=get_all_ciks(),
    overrides=get_overrides(),
    user_agent='YourName/1.0 (your-email@example.com)'
)
"
```

## Pipeline Output

For each trust, generates:
- `_1_All_Trust_Filings.csv` - All SEC filings
- `_2_All_Prospectus_Related_Filings.csv` - 485A/485B/497 filings only
- `_3_Prospectus_Fund_Extraction.csv` - Extracted fund details
- `_4_Fund_Status.csv` - Current status (PENDING/EFFECTIVE/DELAYED)
- `_5_Name_History.csv` - Name change tracking

## Key Columns in _4_Fund_Status.csv

| Column | Description |
|--------|-------------|
| Series ID | Permanent SEC identifier |
| Fund Name | Current fund name |
| Ticker | Trading symbol |
| Trust | Registrant trust |
| Status | PENDING, EFFECTIVE, or DELAYED |
| Effective Date | When fund became/becomes effective |
| Latest Form | Most recent filing type |
| Prospectus Link | URL to latest filing |

## Trusts Tracked

See `etp_tracker/trusts.py` for the full list (14 trusts including REX ETF Trust, ProShares Trust, Direxion, etc.)

## Project Structure

```
etp_tracker/
├── config.py       # Constants and SEC endpoints
├── trusts.py       # CIK registry for monitored trusts
├── sec_client.py   # HTTP client with caching
├── sgml.py         # SGML header parser
├── step2.py        # Fetch submissions
├── step3.py        # Extract fund details
├── step4.py        # Roll up to current status
├── step5.py        # Name history tracking
└── run_pipeline.py # Main entry point
```

## Documentation

See `docs/` folder for detailed architecture and design docs.
