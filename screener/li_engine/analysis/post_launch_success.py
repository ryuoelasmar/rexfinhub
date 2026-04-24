"""Post-launch success analysis.

The real question behind 'should we file?' is: for historical leveraged
single-stock products, which underlier features predict that the product
actually became a commercial success?

Success = AUM >= $50M (same threshold as temp/backtest_2x.py).
Sample = all L&I single-stock products aged 18+ months.

Caveat (same as the existing backtest_2x.py): we snapshot underlier metrics
TODAY, not at launch date. Ideally we'd time-travel to the launch snapshot.
For now, today's metrics are a proxy — stable attributes (market cap class,
sector) are reasonable; momentum signals are contaminated.

This module runs the NEW signal set (insider_pct, mentions_24h, ret_1y,
pct_of_52w_high, etc.) against binary success AND continuous log(AUM) AND
survival timing.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DB = _ROOT / "data" / "etp_tracker.db"
OUT = _ROOT / "data" / "analysis" / "post_launch_success_panel.parquet"


def _clean(t):
    if not isinstance(t, str):
        return ""
    return t.split()[0].upper().strip()


def _safe(v):
    if v in (None, "", "#ERROR", "#N/A"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_panel() -> pd.DataFrame:
    """Load products + their underlier metrics + success labels."""
    conn = sqlite3.connect(str(DB))
    try:
        prods = pd.read_sql_query(
            """
            SELECT ticker, fund_name, issuer_display AS issuer, aum, inception_date,
                   market_status, map_li_direction AS direction,
                   map_li_leverage_amount AS leverage,
                   map_li_underlier AS underlier
            FROM mkt_master_data
            WHERE primary_category = 'LI' AND map_li_underlier IS NOT NULL
            """,
            conn,
        )
        run_id = conn.execute(
            "SELECT id FROM mkt_pipeline_runs WHERE status='completed' "
            "AND stock_rows_written > 0 ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()[0]
        stock_rows = conn.execute(
            "SELECT ticker, data_json FROM mkt_stock_data WHERE pipeline_run_id=?", (run_id,),
        ).fetchall()
    finally:
        conn.close()

    prods["aum"] = pd.to_numeric(prods["aum"], errors="coerce").fillna(0)
    prods["inception_date"] = pd.to_datetime(prods["inception_date"], errors="coerce")
    prods["age_days"] = (pd.Timestamp.today() - prods["inception_date"]).dt.days
    prods["age_months"] = prods["age_days"] / 30.44
    prods["underlier_clean"] = prods["underlier"].astype(str).map(_clean)

    # Parse stock JSON
    stock = {}
    for ticker, blob in stock_rows:
        if not blob:
            continue
        try:
            d = json.loads(blob)
            d = d[0] if isinstance(d, list) else d
        except json.JSONDecodeError:
            continue
        stock[_clean(ticker)] = d

    fields = {
        "market_cap": "Mkt Cap",
        "adv_30d": "Avg Volume 30D",
        "turnover": "Turnover / Traded Value",
        "total_oi": "Total OI",
        "call_oi": "Total Call OI",
        "put_oi": "Total Put OI",
        "rvol_30d": "Volatility 30D",
        "rvol_90d": "Volatility 90D",
        "si_ratio": "Short Interest Ratio",
        "ret_1m": "1M Total Return",
        "ret_3m": "3M Total Return",
        "ret_6m": "6M Total Return",
        "ret_1y": "1Y Total Return",
        "inst_own_pct": "Institutional Owner % Shares Outstanding",
        "insider_pct": "% Insider Shares Outstanding",
        "news_sentiment_bbg": "News Sentiment Daily Avg",
    }

    for col, bbg_key in fields.items():
        prods[col] = prods["underlier_clean"].map(
            lambda u: _safe(stock.get(u, {}).get(bbg_key))
        )

    # Put/call skew
    prods["put_call_skew"] = prods.apply(
        lambda r: ((r["call_oi"] or 0) - (r["put_oi"] or 0)) / ((r["call_oi"] or 0) + (r["put_oi"] or 0))
        if ((r["call_oi"] or 0) + (r["put_oi"] or 0)) > 0 else None, axis=1
    )

    # GICS sector
    prods["gics_sector"] = prods["underlier_clean"].map(lambda u: stock.get(u, {}).get("GICS Sector"))

    # Clean outlier: insider_pct > 100% is nonsense
    prods.loc[prods["insider_pct"] > 100, "insider_pct"] = np.nan

    # Binary success labels
    prods["mature_18m"] = prods["age_months"] >= 18
    prods["success_18m"] = (prods["aum"] >= 50) & prods["mature_18m"]
    prods["failure_18m"] = (prods["aum"] < 10) & prods["mature_18m"]
    prods["moderate_18m"] = (prods["aum"] >= 10) & (prods["aum"] < 50) & prods["mature_18m"]
    prods["liquidated"] = (prods["market_status"] != "ACTV")
    prods["log_aum"] = np.log1p(prods["aum"])
    prods["cohort"] = prods["inception_date"].dt.year

    return prods


def run_analysis(panel: pd.DataFrame) -> dict:
    """Multi-angle on success + log_aum targets."""
    signals = [
        "market_cap", "adv_30d", "turnover",
        "total_oi", "call_oi", "put_oi", "put_call_skew",
        "rvol_30d", "rvol_90d", "si_ratio",
        "ret_1m", "ret_3m", "ret_6m", "ret_1y",
        "inst_own_pct", "insider_pct", "news_sentiment_bbg",
    ]

    mature = panel[panel["mature_18m"]].copy()
    log.info("Mature (>=18m) products: %d", len(mature))
    log.info("Success rate: %.1f%%", 100 * mature["success_18m"].mean())
    log.info("Moderate rate: %.1f%%", 100 * mature["moderate_18m"].mean())
    log.info("Failure rate: %.1f%%", 100 * mature["failure_18m"].mean())

    results = {"n_mature": len(mature), "success_rate": mature["success_18m"].mean()}
    results["outcome_counts"] = mature[["success_18m", "moderate_18m", "failure_18m", "liquidated"]].sum().to_dict()

    targets = {
        "log_aum": mature["log_aum"],
        "success_18m_binary": mature["success_18m"].astype(float),
        "aum_above_10m": (mature["aum"] >= 10).astype(float),
        "aum_above_100m": (mature["aum"] >= 100).astype(float),
    }

    signal_results = {}
    for sig in signals:
        if sig not in mature.columns:
            continue
        row = {}
        for tgt_name, tgt in targets.items():
            sub = mature[[sig]].join(tgt.rename("tgt")).dropna()
            if len(sub) < 20:
                row[tgt_name] = None
                continue
            rho, pval = spearmanr(sub[sig], sub["tgt"])
            row[tgt_name] = {"ic": float(rho), "p": float(pval), "n": len(sub)}
        signal_results[sig] = row

    results["signal_ic_by_target"] = signal_results

    # Cohort stability: does signal IC hold across launch years?
    cohort_results = {}
    for cohort in sorted(mature["cohort"].dropna().unique()):
        cohort_sub = mature[mature["cohort"] == cohort]
        if len(cohort_sub) < 15:
            continue
        cohort_ics = {}
        for sig in signals:
            if sig not in cohort_sub.columns:
                continue
            dat = cohort_sub[[sig, "log_aum"]].dropna()
            if len(dat) < 10:
                continue
            rho, _ = spearmanr(dat[sig], dat["log_aum"])
            if not np.isnan(rho):
                cohort_ics[sig] = float(rho)
        cohort_results[int(cohort)] = {"n": len(cohort_sub), "ics": cohort_ics}
    results["cohort_stability"] = cohort_results

    # Cross-sector IC on log_aum
    sector_results = {}
    for sector, grp in mature.groupby("gics_sector"):
        if len(grp) < 10:
            continue
        sec_ics = {}
        for sig in signals:
            if sig not in grp.columns:
                continue
            d = grp[[sig, "log_aum"]].dropna()
            if len(d) < 10:
                continue
            rho, _ = spearmanr(d[sig], d["log_aum"])
            if not np.isnan(rho):
                sec_ics[sig] = float(rho)
        sector_results[sector] = {"n": len(grp), "ics": sec_ics}
    results["sector_ic"] = sector_results

    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    panel = build_panel()
    panel.to_parquet(OUT, compression="snappy")
    log.info("Saved panel to %s (%d rows)", OUT, len(panel))

    print(f"\nTotal products: {len(panel)}")
    print(f"Mature (18+ mo): {panel['mature_18m'].sum()}")

    res = run_analysis(panel)
    print(f"\nSuccess rate (mature products, AUM >= $50M): {res['success_rate']:.1%}")
    print(f"Outcome counts: {res['outcome_counts']}")

    print("\n=== IC vs. log(AUM) for mature products ===")
    print(f"{'Signal':<22} {'log_aum':>10} {'succ_50m':>10} {'ge_10m':>10} {'ge_100m':>10}")
    for sig, tgts in res["signal_ic_by_target"].items():
        line = [sig]
        for tn in ("log_aum", "success_18m_binary", "aum_above_10m", "aum_above_100m"):
            v = tgts.get(tn)
            line.append(f"{v['ic']:+.3f}" if v else "—")
        print(f"  {line[0]:<22} {line[1]:>10} {line[2]:>10} {line[3]:>10} {line[4]:>10}")

    print("\n=== Cohort stability (IC of each signal vs. log_aum by launch year) ===")
    for yr, cohort in sorted(res["cohort_stability"].items()):
        ics = cohort["ics"]
        print(f"\n  Cohort {yr} (n={cohort['n']}):")
        for sig in sorted(ics.keys(), key=lambda s: -abs(ics[s]))[:8]:
            print(f"    {sig:<22} {ics[sig]:+.3f}")


if __name__ == "__main__":
    main()
