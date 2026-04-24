"""Build weekly top-20 target + sticky-flow baseline.

Step 2 of the iterative analysis. Goal:
    1. Filter underliers to an 'active universe' (non-zero flow in >=50% of weeks)
    2. Define binary target: was underlier in top-20 by |weekly flow| this week?
    3. Baseline: predicting next week's top-20 = this week's top-20.
       Measure hit rate. Every signal-based model must beat this.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
TS = _ROOT / "data" / "analysis" / "bbg_timeseries_panel.parquet"
DB = _ROOT / "data" / "etp_tracker.db"


def build_weekly_flow_panel() -> pd.DataFrame:
    """Weekly flow aggregated per underlier."""
    p = pd.read_parquet(TS)
    flow = p[p["metric"] == "daily_flow"].copy()
    flow["date"] = pd.to_datetime(flow["date"])

    conn = sqlite3.connect(str(DB))
    try:
        m = pd.read_sql_query(
            "SELECT ticker, map_li_underlier FROM mkt_master_data "
            "WHERE primary_category='LI' AND map_li_underlier IS NOT NULL",
            conn,
        )
    finally:
        conn.close()
    m["prod"] = m["ticker"].str.split().str[0]
    m["underlier"] = m["map_li_underlier"].str.split().str[0]
    prod_to_und = dict(zip(m["prod"], m["underlier"]))

    flow["prod"] = flow["ticker"].str.split().str[0]
    flow["underlier"] = flow["prod"].map(prod_to_und)
    flow = flow.dropna(subset=["underlier"])

    flow["week"] = flow["date"].dt.to_period("W").dt.start_time
    weekly = flow.groupby(["underlier", "week"])["value"].sum().reset_index()
    weekly.columns = ["underlier", "week", "weekly_flow"]
    weekly["abs_flow"] = weekly["weekly_flow"].abs()
    return weekly


def filter_active_universe(weekly: pd.DataFrame, min_active_rate: float = 0.50,
                           lookback_weeks: int = 52) -> list[str]:
    """Return list of underliers that had non-zero flow in >=min_active_rate
    of the last lookback_weeks weeks."""
    latest = weekly["week"].max()
    cutoff = latest - pd.Timedelta(weeks=lookback_weeks)
    recent = weekly[weekly["week"] >= cutoff]

    stats = recent.groupby("underlier").agg(
        total_weeks=("week", "count"),
        active_weeks=("weekly_flow", lambda x: (x.abs() > 0.01).sum()),
    )
    stats["active_rate"] = stats["active_weeks"] / stats["total_weeks"]
    active = stats[(stats["active_rate"] >= min_active_rate) & (stats["total_weeks"] >= 20)]
    return sorted(active.index.tolist())


def mark_top_n(weekly: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """For each week, mark top-N by |weekly flow| with in_top = 1."""
    weekly = weekly.copy()
    weekly["rank_in_week"] = weekly.groupby("week")["abs_flow"].rank(ascending=False, method="first")
    weekly["in_top"] = (weekly["rank_in_week"] <= n).astype(int)
    return weekly


def sticky_baseline_hit_rate(weekly_marked: pd.DataFrame, n: int = 20) -> dict:
    """For every week, predict top-N = last week's top-N. Measure overlap."""
    weeks = sorted(weekly_marked["week"].unique())
    hit_rates = []
    for i in range(1, len(weeks)):
        this_week = weeks[i]
        last_week = weeks[i - 1]
        this = set(weekly_marked[(weekly_marked["week"] == this_week) &
                                  (weekly_marked["in_top"] == 1)]["underlier"])
        last = set(weekly_marked[(weekly_marked["week"] == last_week) &
                                  (weekly_marked["in_top"] == 1)]["underlier"])
        if len(this) == 0 or len(last) == 0:
            continue
        overlap = len(this & last)
        hit_rates.append({
            "week": this_week,
            "overlap": overlap,
            "rate": overlap / n,
            "n_actual": len(this),
            "n_predicted": len(last),
        })
    hit_rates = pd.DataFrame(hit_rates)
    return {
        "mean_hit_rate": float(hit_rates["rate"].mean()),
        "median_hit_rate": float(hit_rates["rate"].median()),
        "std_hit_rate": float(hit_rates["rate"].std()),
        "min_hit_rate": float(hit_rates["rate"].min()),
        "max_hit_rate": float(hit_rates["rate"].max()),
        "n_weeks": len(hit_rates),
        "hit_rates": hit_rates,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("=" * 60)
    print("STEP 2A — Active universe + top-20 weekly target")
    print("=" * 60)

    weekly = build_weekly_flow_panel()
    print(f"Raw weekly panel: {len(weekly):,} rows, "
          f"{weekly['underlier'].nunique()} underliers, "
          f"{weekly['week'].min().date()} to {weekly['week'].max().date()}")

    active = filter_active_universe(weekly)
    print(f"\nActive universe (flow in >=50% of last 52 weeks): "
          f"{len(active)} underliers")
    print(f"Examples: {', '.join(active[:15])} ...")

    weekly_active = weekly[weekly["underlier"].isin(active)].copy()
    print(f"Weekly obs in active universe: {len(weekly_active):,}")

    # Top-20 target
    marked = mark_top_n(weekly_active, n=20)
    in_top_rate = marked["in_top"].mean()
    print(f"\nFraction of weeks each underlier spends in top-20: "
          f"mean={in_top_rate:.1%} (sanity: 20/{len(active)}={20/len(active):.1%})")
    print(f"Per-underlier: top-20 appearance count distribution:")
    appear = marked.groupby("underlier")["in_top"].sum().describe()
    print(appear.round(1).to_string())

    print(f"\n{'='*60}")
    print("STEP 2B — Baseline: sticky-flow hit rate")
    print("=" * 60)
    baseline = sticky_baseline_hit_rate(marked, n=20)
    print(f"Weeks tested: {baseline['n_weeks']}")
    print(f"Mean hit rate (sticky baseline): {baseline['mean_hit_rate']:.1%}")
    print(f"Median: {baseline['median_hit_rate']:.1%}")
    print(f"Std: {baseline['std_hit_rate']:.1%}")
    print(f"Min: {baseline['min_hit_rate']:.1%}, Max: {baseline['max_hit_rate']:.1%}")
    print()
    print("This is the number any signal-based model must BEAT.")

    # Save artifacts
    out_dir = _ROOT / "data" / "analysis"
    marked.to_parquet(out_dir / "weekly_top20_panel.parquet", index=False)
    baseline["hit_rates"].to_parquet(out_dir / "sticky_baseline_hit_rates.parquet", index=False)
    print(f"\nSaved: weekly_top20_panel.parquet, sticky_baseline_hit_rates.parquet")

    return weekly_active, marked, baseline, active


if __name__ == "__main__":
    main()
