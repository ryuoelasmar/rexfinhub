"""Expanded signal panel v2 — every field in bbg mkt_stock_data JSON,
consolidated collinear pairs, fixed forward-flow target, cross-sector tag.

Signals (18) — organized by factor family:

    size (pick 1):         market_cap
    liquidity:             adv_30d, turnover
    options:               total_oi, call_oi, put_oi, call_put_ratio, put_call_skew
    volatility:            rvol_90d  (rvol_30d dropped, 0.93 corr with 90d)
    momentum (NEW):        ret_1m, ret_3m, ret_6m, ret_1y
    position_in_range:     pct_of_52w_high (NEW)
    ownership (NEW):       inst_own_pct, insider_pct
    sentiment:             news_sentiment_bbg (NEW), mentions_24h (ApeWisdom)
    tag:                   gics_sector (for cross-sector stratification)

Target:
    forward_flow_30d — fixed construction (as-of date 60 BDays ago, window 30 BDays)

Removed:
    underlier_aum_12m_growth — target leakage; dropped
    rvol_30d — 0.93 corr with rvol_90d; redundant
    turnover_30d — 0.88 corr with market_cap; kept a separate turnover field instead
    short_interest (raw) — 0.86 corr with adv_30d; kept si_ratio instead
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DB = _ROOT / "data" / "etp_tracker.db"
OUT = _ROOT / "data" / "analysis" / "expanded_signal_panel_v2.parquet"

from screener.li_engine.analysis.forward_flow_fixed import (
    build_lookback_target, build_signal_snapshot_at,
)


def _clean(t: str) -> str:
    if not isinstance(t, str):
        return ""
    return t.split()[0].upper().strip()


def _coerce(v):
    if v in (None, "#ERROR", "#N/A", "", "N/A"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_full_bbg_stock(db: Path = DB) -> pd.DataFrame:
    """Pull EVERY field from mkt_stock_data.data_json, not just 8."""
    conn = sqlite3.connect(str(db))
    try:
        run_id = conn.execute(
            "SELECT id FROM mkt_pipeline_runs WHERE status='completed' "
            "AND stock_rows_written > 0 ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT ticker, data_json FROM mkt_stock_data WHERE pipeline_run_id=?",
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    recs = []
    for ticker, blob in rows:
        if not blob:
            continue
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            continue
        d = parsed[0] if isinstance(parsed, list) else parsed

        mkt_cap = _coerce(d.get("Mkt Cap"))
        last = _coerce(d.get("Last Price"))
        high52 = _coerce(d.get("52W High"))
        low52 = _coerce(d.get("52W Low"))
        adv30 = _coerce(d.get("Avg Volume 30D"))
        call_oi = _coerce(d.get("Total Call OI"))
        put_oi = _coerce(d.get("Total Put OI"))
        total_oi = _coerce(d.get("Total OI"))
        if total_oi is None and (call_oi is not None or put_oi is not None):
            total_oi = (call_oi or 0) + (put_oi or 0)

        call_put_ratio = (call_oi / put_oi) if (call_oi and put_oi and put_oi > 0) else None
        skew = ((call_oi or 0) - (put_oi or 0)) / ((call_oi or 0) + (put_oi or 0)) \
            if ((call_oi or 0) + (put_oi or 0)) > 0 else None

        pct_52w = (last / high52) if (last and high52 and high52 > 0) else None
        range_position = ((last - low52) / (high52 - low52)) if (last and high52 and low52 and high52 > low52) else None

        turnover = _coerce(d.get("Turnover / Traded Value"))

        recs.append({
            "ticker": _clean(ticker),
            "market_cap": mkt_cap,
            "adv_30d": adv30,
            "turnover": turnover,
            "total_oi": total_oi,
            "call_oi": call_oi,
            "put_oi": put_oi,
            "call_put_ratio": call_put_ratio,
            "put_call_skew": skew,
            "rvol_30d": _coerce(d.get("Volatility 30D")),
            "rvol_90d": _coerce(d.get("Volatility 90D")),
            "si_ratio": _coerce(d.get("Short Interest Ratio")),
            "ret_1m": _coerce(d.get("1M Total Return")),
            "ret_3m": _coerce(d.get("3M Total Return")),
            "ret_6m": _coerce(d.get("6M Total Return")),
            "ret_1y": _coerce(d.get("1Y Total Return")),
            "pct_of_52w_high": pct_52w,
            "range_position": range_position,
            "inst_own_pct": _coerce(d.get("Institutional Owner % Shares Outstanding")),
            "insider_pct": _coerce(d.get("% Insider Shares Outstanding")),
            "news_sentiment_bbg": _coerce(d.get("News Sentiment Daily Avg")),
            "gics_sector": d.get("GICS Sector") or None,
        })
    df = pd.DataFrame.from_records(recs)
    df = df[df["ticker"] != ""].drop_duplicates("ticker").set_index("ticker")
    log.info("Loaded bbg stock fields: %d tickers, %d columns", len(df), df.shape[1])
    return df


def load_apewisdom(max_pages: int = 5, timeout: float = 10.0) -> pd.DataFrame:
    url = "https://apewisdom.io/api/v1.0/filter/{f}/page/{p}"
    recs: dict[str, dict] = {}
    for filt in ("all-stocks", "wallstreetbets"):
        for page in range(1, max_pages + 1):
            try:
                r = requests.get(url.format(f=filt, p=page), timeout=timeout)
                if r.status_code != 200:
                    break
                items = r.json().get("results", [])
                if not items:
                    break
                for it in items:
                    tk = _clean(it.get("ticker", ""))
                    if not tk:
                        continue
                    m = int(it.get("mentions", 0) or 0)
                    if tk not in recs or m > recs[tk]["mentions_24h"]:
                        recs[tk] = {"mentions_24h": m}
                time.sleep(0.2)
            except Exception as e:
                log.warning("apewisdom %s p%d: %s", filt, page, e)
                break
    if not recs:
        return pd.DataFrame(columns=["mentions_24h"])
    df = pd.DataFrame.from_dict(recs, orient="index")
    df.index.name = "ticker"
    return df


def build_panel() -> pd.DataFrame:
    log.info("Loading bbg stock (full)...")
    stock = load_full_bbg_stock()

    log.info("Building lookback forward-flow target...")
    target = build_lookback_target(as_of_days_ago=60, window=30).rename("forward_flow_30d")

    log.info("Building as-of snapshot (aum + trailing flow at as-of)...")
    snap = build_signal_snapshot_at(as_of_days_ago=60)

    log.info("Loading ApeWisdom...")
    sentiment = load_apewisdom()

    panel = stock.join(target, how="left").join(snap, how="left")
    if not sentiment.empty:
        panel = panel.join(sentiment, how="left")

    panel.index.name = "ticker"
    return panel


def save(panel: pd.DataFrame, path: Path = OUT) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(path, compression="snappy")
    log.info("saved %d rows x %d cols to %s", len(panel), panel.shape[1], path)
    return path


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    panel = build_panel()
    save(panel)

    print(f"\nPanel: {panel.shape}")
    print("\nColumn coverage:")
    for c in panel.columns:
        n = panel[c].notna().sum()
        print(f"  {c:25s} {n}/{len(panel)} ({n/len(panel):.0%})")

    # Analytical universe: underliers with signals AND target
    uni = panel[panel["forward_flow_30d"].notna() & panel["market_cap"].notna()]
    print(f"\nAnalytical universe (has signals + target): {len(uni)}")


if __name__ == "__main__":
    main()
