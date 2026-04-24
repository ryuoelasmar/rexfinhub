"""Persistent storage for L&I engine daily runs.

Every run writes: one row per (run_date, ticker) with all signal values,
pillar scores, final score, and metadata. Enables:
    - Quarterly multi-angle recalibration with proper forward windows
    - Per-ticker score trajectory (is TSLA's score rising for 3 weeks?)
    - Signal drift monitoring
    - Out-of-sample validation

Table lives in data/etp_tracker.db alongside existing mkt_* tables.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent
DB = _ROOT / "data" / "etp_tracker.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS li_engine_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    run_timestamp TEXT NOT NULL,
    ticker TEXT NOT NULL,
    final_score REAL,
    pillar_scores_json TEXT,
    signal_values_json TEXT,
    n_signals INTEGER,
    has_rex_filing INTEGER,
    has_rex_launch INTEGER,
    weights_version TEXT,
    pipeline_run_id INTEGER,
    UNIQUE(run_date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_li_daily_run_date ON li_engine_daily(run_date);
CREATE INDEX IF NOT EXISTS idx_li_daily_ticker ON li_engine_daily(ticker);
CREATE INDEX IF NOT EXISTS idx_li_daily_score ON li_engine_daily(run_date, final_score DESC);

CREATE TABLE IF NOT EXISTS li_engine_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    run_timestamp TEXT NOT NULL,
    weights_version TEXT,
    pipeline_run_id INTEGER,
    n_tickers INTEGER,
    n_with_sentiment INTEGER,
    n_with_oc_equity INTEGER,
    skip_sentiment INTEGER DEFAULT 0,
    notes TEXT,
    UNIQUE(run_date, weights_version)
);
"""


def ensure_schema(db_path: Path = DB) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def write_run(
    scored: pd.DataFrame,
    weights_version: str,
    pipeline_run_id: int | None = None,
    skip_sentiment: bool = False,
    notes: str | None = None,
    db_path: Path = DB,
) -> int:
    """Write one full engine run. Returns the inserted run id.

    `scored` is the DataFrame from engine.score_universe — indexed by ticker,
    with columns: final_score, n_signals, has_rex_filing, has_rex_launch,
    plus signal-level and pillar-level columns. We persist raw signal values
    + derived pillar scores as JSON so we can recompute later.
    """
    ensure_schema(db_path)

    today = date.today().isoformat()
    now = datetime.now().isoformat(timespec="seconds")

    n_tickers = len(scored)
    n_sent = int(scored["mentions_24h"].notna().sum()) if "mentions_24h" in scored.columns else 0
    n_oc = int(scored["oc_volume_1w"].notna().sum()) if "oc_volume_1w" in scored.columns else 0

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO li_engine_runs
               (run_date, run_timestamp, weights_version, pipeline_run_id,
                n_tickers, n_with_sentiment, n_with_oc_equity, skip_sentiment, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (today, now, weights_version, pipeline_run_id,
             n_tickers, n_sent, n_oc, int(skip_sentiment), notes),
        )
        run_row_id = cur.lastrowid

        pillar_cols = [c for c in scored.columns if c.endswith("_score") and c != "final_score"]
        signal_cols = [c for c in scored.columns
                       if not c.endswith("__z") and not c.endswith("_score")
                       and c not in {"final_raw", "final_score", "n_signals",
                                     "top_pillar", "has_rex_filing", "has_rex_launch",
                                     "rex_filing_count"}]

        rows = []
        for ticker, r in scored.iterrows():
            pillar_json = json.dumps(
                {p: (None if pd.isna(r[p]) else float(r[p])) for p in pillar_cols}
            )
            sig_json = json.dumps(
                {c: (None if pd.isna(r.get(c)) else float(r[c]))
                 for c in signal_cols if c in scored.columns and not isinstance(r.get(c), str)}
            )
            rows.append((
                today, now, ticker,
                float(r["final_score"]) if "final_score" in scored.columns and not pd.isna(r["final_score"]) else None,
                pillar_json, sig_json,
                int(r["n_signals"]) if "n_signals" in scored.columns and not pd.isna(r.get("n_signals")) else None,
                int(bool(r.get("has_rex_filing"))) if "has_rex_filing" in scored.columns else 0,
                int(bool(r.get("has_rex_launch"))) if "has_rex_launch" in scored.columns else 0,
                weights_version,
                pipeline_run_id,
            ))

        cur.executemany(
            """INSERT OR REPLACE INTO li_engine_daily
               (run_date, run_timestamp, ticker, final_score,
                pillar_scores_json, signal_values_json, n_signals,
                has_rex_filing, has_rex_launch, weights_version, pipeline_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        log.info("Wrote %d ticker rows for run %s (weights=%s)", len(rows), today, weights_version)
        return run_row_id
    finally:
        conn.close()


def read_ticker_history(ticker: str, days: int = 90, db_path: Path = DB) -> pd.DataFrame:
    """Return a ticker's score trajectory over the last N days."""
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """SELECT run_date, final_score, n_signals,
                      has_rex_filing, has_rex_launch,
                      pillar_scores_json, signal_values_json
               FROM li_engine_daily
               WHERE ticker = ?
               AND run_date >= date('now', ? || ' days')
               ORDER BY run_date""",
            conn,
            params=(ticker, f"-{days}"),
        )
    finally:
        conn.close()
    return df


def list_runs(db_path: Path = DB, limit: int = 30) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM li_engine_runs ORDER BY run_timestamp DESC LIMIT {limit}",
            conn,
        )
    finally:
        conn.close()
    return df


def top_movers(days: int = 7, n: int = 20, db_path: Path = DB) -> pd.DataFrame:
    """Tickers whose score rose most over the window. Requires 2+ runs in window."""
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """SELECT ticker, run_date, final_score
               FROM li_engine_daily
               WHERE run_date >= date('now', ? || ' days')
               ORDER BY ticker, run_date""",
            conn, params=(f"-{days}",),
        )
    finally:
        conn.close()
    if df.empty:
        return df
    first = df.groupby("ticker").first().rename(columns={"final_score": "score_start"})
    last = df.groupby("ticker").last().rename(columns={"final_score": "score_end"})
    joined = first.join(last[["score_end"]], how="inner")
    joined["delta"] = joined["score_end"] - joined["score_start"]
    return joined.sort_values("delta", ascending=False).head(n)


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    p = argparse.ArgumentParser(description="L&I engine persistence utilities")
    p.add_argument("--init", action="store_true", help="Create tables if missing")
    p.add_argument("--list-runs", action="store_true")
    p.add_argument("--ticker-history", type=str, help="Show score history for a ticker")
    p.add_argument("--top-movers", action="store_true", help="Top movers in last 7 days")
    args = p.parse_args()

    if args.init:
        ensure_schema()
        print("Schema initialized.")

    if args.list_runs:
        print(list_runs().to_string())

    if args.ticker_history:
        print(read_ticker_history(args.ticker_history).to_string())

    if args.top_movers:
        print(top_movers().to_string())


if __name__ == "__main__":
    main()
