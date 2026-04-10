"""
Verify that data_engine.py output matches Excel's pre-computed q_master_data
and q_aum_time_series_labeled.

Run: python scripts/verify_data_engine.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from webapp.services.data_engine import DATA_FILE, _load_excel, build_master_data, build_time_series


def _compare_shapes(label, excel_df, py_df):
    """Compare DataFrame shapes and report."""
    match = excel_df.shape == py_df.shape
    status = "PASS" if match else "WARN"
    print(f"  [{status}] Shape: Excel={excel_df.shape}, Python={py_df.shape}")
    return match


def _compare_columns(label, excel_df, py_df):
    """Compare column sets and report."""
    ex_cols = set(excel_df.columns)
    py_cols = set(py_df.columns)
    extra_py = sorted(py_cols - ex_cols)
    extra_ex = sorted(ex_cols - py_cols)
    match = len(extra_py) == 0 and len(extra_ex) == 0
    status = "PASS" if match else "WARN"
    print(f"  [{status}] Columns: Excel={len(ex_cols)}, Python={len(py_cols)}")
    if extra_py:
        print(f"         Extra in Python: {extra_py}")
    if extra_ex:
        print(f"         Extra in Excel: {extra_ex}")
    return match


def _compare_tickers(label, excel_df, py_df, ticker_col="ticker"):
    """Compare ticker overlap and report."""
    if ticker_col not in excel_df.columns or ticker_col not in py_df.columns:
        print(f"  [SKIP] No '{ticker_col}' column found")
        return True

    et = set(excel_df[ticker_col].dropna().astype(str))
    pt = set(py_df[ticker_col].dropna().astype(str))
    shared = et & pt
    only_excel = et - pt
    only_python = pt - et
    pct = len(shared) / len(et) * 100 if et else 100

    status = "PASS" if pct >= 90 else "FAIL"
    print(f"  [{status}] Ticker match: {len(shared)}/{len(et)} ({pct:.1f}%)")

    if only_excel:
        print(f"         Only in Excel ({len(only_excel)}): {sorted(only_excel)[:10]}")
    if only_python:
        print(f"         Only in Python ({len(only_python)}): {sorted(only_python)[:10]}")

    return pct >= 90


def verify_master(xl):
    """Verify q_master_data match."""
    print("\n=== q_master_data Verification ===")

    # Load Excel reference
    excel_master = xl.parse("q_master_data")
    excel_master.columns = [str(c).strip() for c in excel_master.columns]
    print(f"  Excel q_master_data loaded: {excel_master.shape}")

    # Build Python master
    py_master = build_master_data(xl)
    print(f"  Python build_master_data: {py_master.shape}")

    results = []
    results.append(_compare_shapes("master", excel_master, py_master))
    results.append(_compare_columns("master", excel_master, py_master))
    results.append(_compare_tickers("master", excel_master, py_master))

    # Compare etp_category distribution
    if "etp_category" in excel_master.columns and "etp_category" in py_master.columns:
        ex_dist = excel_master["etp_category"].value_counts().sort_index()
        py_dist = py_master["etp_category"].value_counts().sort_index()
        match = ex_dist.equals(py_dist)
        status = "PASS" if match else "WARN"
        print(f"  [{status}] etp_category distribution match")
        if not match:
            print(f"         Excel: {ex_dist.to_dict()}")
            print(f"         Python: {py_dist.to_dict()}")
        results.append(match)

    # Compare category_display distribution
    if "category_display" in excel_master.columns and "category_display" in py_master.columns:
        ex_dist = excel_master["category_display"].value_counts().sort_index()
        py_dist = py_master["category_display"].value_counts().sort_index()
        match = ex_dist.equals(py_dist)
        status = "PASS" if match else "WARN"
        print(f"  [{status}] category_display distribution match")
        results.append(match)

    # Compare is_rex counts
    if "is_rex" in excel_master.columns and "is_rex" in py_master.columns:
        ex_rex = excel_master["is_rex"].sum()
        py_rex = py_master["is_rex"].sum()
        match = ex_rex == py_rex
        status = "PASS" if match else "WARN"
        print(f"  [{status}] is_rex count: Excel={ex_rex}, Python={py_rex}")
        results.append(match)

    return all(results), py_master


def verify_time_series(xl, py_master):
    """Verify q_aum_time_series_labeled match."""
    print("\n=== q_aum_time_series_labeled Verification ===")

    # Load Excel reference
    excel_ts = xl.parse("q_aum_time_series_labeled")
    excel_ts.columns = [str(c).strip() for c in excel_ts.columns]
    print(f"  Excel q_aum_time_series_labeled loaded: {excel_ts.shape}")

    # Build Python time series
    py_ts = build_time_series(py_master, xl)
    print(f"  Python build_time_series: {py_ts.shape}")

    results = []
    results.append(_compare_shapes("ts", excel_ts, py_ts))
    results.append(_compare_columns("ts", excel_ts, py_ts))
    results.append(_compare_tickers("ts", excel_ts, py_ts))

    # Compare fund_category_key coverage
    if "fund_category_key" in excel_ts.columns and "fund_category_key" in py_ts.columns:
        ex_fck = set(excel_ts["fund_category_key"].dropna().astype(str))
        py_fck = set(py_ts["fund_category_key"].dropna().astype(str))
        shared = ex_fck & py_fck
        pct = len(shared) / len(ex_fck) * 100 if ex_fck else 100
        status = "PASS" if pct >= 90 else "FAIL"
        print(f"  [{status}] fund_category_key match: {len(shared)}/{len(ex_fck)} ({pct:.1f}%)")
        results.append(pct >= 90)

    # Compare issuer_group distribution
    if "issuer_group" in excel_ts.columns and "issuer_group" in py_ts.columns:
        ex_dist = excel_ts["issuer_group"].value_counts().sort_index()
        py_dist = py_ts["issuer_group"].value_counts().sort_index()
        match = ex_dist.equals(py_dist)
        status = "PASS" if match else "WARN"
        print(f"  [{status}] issuer_group distribution match")
        results.append(match)

    # Spot-check AUM values for a few tickers
    if "aum_value" in excel_ts.columns and "aum_value" in py_ts.columns:
        sample_tickers = list(excel_ts["ticker"].unique()[:5])
        aum_mismatches = 0
        for ticker in sample_tickers:
            ex_vals = excel_ts[excel_ts["ticker"] == ticker].sort_values("months_ago")["aum_value"].tolist()
            py_vals = py_ts[py_ts["ticker"] == ticker].sort_values("months_ago")["aum_value"].tolist()
            # Compare lengths first, then values
            if len(ex_vals) != len(py_vals):
                aum_mismatches += 1
            elif any(abs(e - p) > 0.01 for e, p in zip(ex_vals, py_vals) if pd.notna(e) and pd.notna(p)):
                aum_mismatches += 1

        status = "PASS" if aum_mismatches == 0 else "WARN"
        print(f"  [{status}] AUM value spot-check: {len(sample_tickers) - aum_mismatches}/{len(sample_tickers)} tickers match")
        results.append(aum_mismatches == 0)

    return all(results)


def verify():
    """Run full verification."""
    print(f"Data file: {DATA_FILE}")
    if not DATA_FILE.exists():
        print(f"ERROR: Data file not found: {DATA_FILE}")
        return

    xl = _load_excel()

    master_ok, py_master = verify_master(xl)
    ts_ok = verify_time_series(xl, py_master)

    print("\n" + "=" * 50)
    if master_ok and ts_ok:
        print("RESULT: ALL CHECKS PASSED")
    else:
        print("RESULT: SOME CHECKS FAILED (see above)")
    print("=" * 50)


if __name__ == "__main__":
    verify()
