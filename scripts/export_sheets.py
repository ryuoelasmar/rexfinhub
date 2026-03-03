"""Export needed sheets from bloomberg_daily_file.xlsm to CSV.

Used during Render build to avoid loading 25MB xlsm at runtime.
CSVs use ~10x less memory than openpyxl Excel parsing.

Usage: python scripts/export_sheets.py <xlsm_path> <output_dir>
"""
import sys
from pathlib import Path

import pandas as pd

SHEETS = [
    "w1", "w2", "w3", "w4", "s1",
    "data_aum", "data_flow", "data_notional",
    "assets", "cost", "performance", "flows",
    "liquidity", "gics", "geographic", "structure",
]


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/export_sheets.py <xlsm_path> <output_dir>")
        sys.exit(1)

    xlsm = Path(sys.argv[1])
    out = Path(sys.argv[2])

    if not xlsm.exists():
        print(f"ERROR: Source file not found: {xlsm}")
        sys.exit(1)

    out.mkdir(parents=True, exist_ok=True)

    try:
        xl = pd.ExcelFile(xlsm, engine="openpyxl")
    except Exception as e:
        print(f"ERROR: Cannot open {xlsm}: {e}")
        sys.exit(1)

    available = set(xl.sheet_names)
    exported = 0

    for sheet in SHEETS:
        if sheet not in available:
            print(f"  SKIP {sheet} (not in workbook)")
            continue
        try:
            df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
            csv_path = out / f"{sheet}.csv"
            df.to_csv(csv_path, index=False)
            print(f"  {sheet}: {len(df)} rows -> {csv_path.name}")
            exported += 1
        except Exception as e:
            print(f"  ERROR exporting {sheet}: {e}")
            xl.close()
            sys.exit(1)

    xl.close()

    if exported == 0:
        print("ERROR: No sheets exported")
        sys.exit(1)

    print(f"Done. {exported} sheets exported.")


if __name__ == "__main__":
    main()
