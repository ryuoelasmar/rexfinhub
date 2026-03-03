"""Export needed sheets from bloomberg_daily_file.xlsm to CSV.

Used during Render build to avoid loading 25MB xlsm at runtime.
CSVs use ~10x less memory than openpyxl Excel parsing.

Usage: python scripts/export_sheets.py <xlsm_path> <output_dir>
"""
import sys
from pathlib import Path

import pandas as pd

SHEETS = ["w1", "w2", "w3", "w4", "s1", "data_aum", "data_flow", "data_notional"]


def main():
    xlsm = Path(sys.argv[1])
    out = Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)

    xl = pd.ExcelFile(xlsm, engine="openpyxl")
    available = set(xl.sheet_names)

    for sheet in SHEETS:
        if sheet not in available:
            print(f"  SKIP {sheet} (not in workbook)")
            continue
        df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
        csv_path = out / f"{sheet}.csv"
        df.to_csv(csv_path, index=True)
        print(f"  {sheet}: {len(df)} rows -> {csv_path.name}")

    xl.close()
    print("Done.")


if __name__ == "__main__":
    main()
