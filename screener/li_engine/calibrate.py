"""Weight calibration — contemporaneous rank-IC.

Produces a new versioned weights JSON by:
    1. For every underlier with an existing 2x/3x/4x product, aggregating the
       product(s) trailing flow-to-AUM ratio as the target.
    2. Joining per-underlier signals from the most recent bbg snapshot.
    3. Computing Spearman rank-IC per signal across the cross-section.
    4. Deriving weights ∝ |IC|, floored at 5%, capped at 35% per pillar.

LIMITATIONS
-----------
- Only covers underliers that already have a leveraged product. That's the
  set for which we can observe demand. For file-candidates (no product yet),
  we can't measure IC directly — we assume the same weights generalize.
- Target is *contemporaneous*, not predictive. As dated snapshot history
  grows past 120 days, switch to forward windows. See
  docs/LI_ENGINE_METHODOLOGY.md §Target variable.

USAGE
-----
    python -m screener.li_engine.calibrate --write
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from screener.li_engine.scorer import PILLAR_SIGNALS
from screener.li_engine.signals import (
    _DB_PATH,
    _clean_ticker,
    load_bbg_stock_signals,
    load_competitive_whitespace,
    load_oc_equity_signals,
    load_sentiment_signals,
)

log = logging.getLogger(__name__)

_WEIGHTS_DIR = Path(__file__).resolve().parent

PILLAR_FLOOR = 0.05
PILLAR_CAP = 0.35


def load_underlier_flow_target(db_path: Path = _DB_PATH) -> pd.Series:
    """target = sum of trailing 3m net flow across all products on this underlier,
    divided by sum of starting AUM (approximated as current AUM - trailing 3m flow).

    Returns Series indexed by cleaned underlier ticker.
    """
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT map_li_underlier AS underlier,
                   aum,
                   fund_flow_3month
            FROM mkt_master_data
            WHERE map_li_underlier IS NOT NULL
              AND map_li_underlier != ''
              AND primary_category = 'LI'
              AND aum IS NOT NULL
              AND fund_flow_3month IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return pd.Series(dtype=float, name="flow_to_aum")
    df["ticker"] = df["underlier"].astype(str).map(_clean_ticker)
    df = df[df["ticker"] != ""]

    agg = df.groupby("ticker").agg(aum=("aum", "sum"), flow_3m=("fund_flow_3month", "sum"))
    # starting_aum = end_aum - flow_3m (so flow/starting)
    agg["starting_aum"] = agg["aum"] - agg["flow_3m"]
    # Drop cases where starting_aum is non-positive (new products, flow larger than final)
    agg = agg[agg["starting_aum"] > 0]
    agg["flow_to_aum"] = agg["flow_3m"] / agg["starting_aum"]
    # Winsorize 1% / 99%
    lo, hi = agg["flow_to_aum"].quantile([0.01, 0.99])
    agg["flow_to_aum"] = agg["flow_to_aum"].clip(lo, hi)
    log.info("Target loaded: %d underliers, target range [%.3f, %.3f], mean=%.3f",
             len(agg), agg["flow_to_aum"].min(), agg["flow_to_aum"].max(), agg["flow_to_aum"].mean())
    return agg["flow_to_aum"]


def build_signal_panel(skip_sentiment: bool = False) -> pd.DataFrame:
    """Same signals the engine uses — but we emit the raw panel for IC math."""
    bbg = load_bbg_stock_signals()
    ws = load_competitive_whitespace()
    oc = load_oc_equity_signals()
    sentiment = pd.DataFrame() if skip_sentiment else load_sentiment_signals(max_pages=5)

    panel = bbg.copy()
    if not ws.empty:
        panel = panel.join(ws.rename("density_score"), how="outer")
    if not oc.empty:
        panel = panel.join(oc, how="outer")
    if not sentiment.empty:
        panel = panel.join(sentiment, how="outer")
    return panel


def compute_signal_ic(panel: pd.DataFrame, target: pd.Series) -> dict[str, dict]:
    """Spearman rank-IC per signal against the target. Returns
    {signal: {ic, p_value, n}}."""
    common_idx = panel.index.intersection(target.index)
    log.info("IC computation: %d overlap (signals=%d, target=%d)",
             len(common_idx), len(panel), len(target))
    aligned = panel.loc[common_idx]
    y = target.loc[common_idx]

    out: dict[str, dict] = {}
    for pillar, sigs in PILLAR_SIGNALS.items():
        for sig in sigs:
            if sig not in aligned.columns:
                out[sig] = {"ic": None, "p_value": None, "n": 0, "pillar": pillar}
                continue
            x = aligned[sig]
            mask = x.notna() & y.notna()
            if mask.sum() < 10:
                out[sig] = {"ic": None, "p_value": None, "n": int(mask.sum()), "pillar": pillar}
                continue
            rho, pval = spearmanr(x[mask], y[mask])
            out[sig] = {
                "ic": float(rho) if pd.notna(rho) else None,
                "p_value": float(pval) if pd.notna(pval) else None,
                "n": int(mask.sum()),
                "pillar": pillar,
            }
    return out


def _normalize(values: dict[str, float], floor: float, cap: float) -> dict[str, float]:
    """Normalize to sum=1 with per-key floor and cap."""
    if not values:
        return {}
    raw = {k: abs(v) if v is not None else 0.0 for k, v in values.items()}
    total = sum(raw.values())
    if total == 0:
        equal = 1.0 / len(raw)
        return {k: equal for k in raw}
    w = {k: v / total for k, v in raw.items()}
    # Apply floor
    w = {k: max(floor, v) for k, v in w.items()}
    # Renormalize
    total = sum(w.values())
    w = {k: v / total for k, v in w.items()}
    # Apply cap
    w = {k: min(cap, v) for k, v in w.items()}
    # Final renormalize
    total = sum(w.values())
    w = {k: v / total for k, v in w.items()}
    return w


def derive_weights(ic_results: dict[str, dict]) -> tuple[dict, dict]:
    """Convert IC results into pillar + within-pillar weights."""
    pillar_to_sigs: dict[str, dict[str, float]] = {}
    pillar_mean_abs_ic: dict[str, float] = {}

    for sig, info in ic_results.items():
        pillar = info["pillar"]
        ic = info["ic"]
        pillar_to_sigs.setdefault(pillar, {})[sig] = ic if ic is not None else 0.0

    signal_weights = {}
    for pillar, sigs in pillar_to_sigs.items():
        signal_weights[pillar] = _normalize(sigs, floor=0.05, cap=0.80)
        abs_ics = [abs(v) for v in sigs.values() if v]
        pillar_mean_abs_ic[pillar] = (sum(abs_ics) / len(abs_ics)) if abs_ics else 0.0

    pillar_weights = _normalize(pillar_mean_abs_ic, floor=PILLAR_FLOOR, cap=PILLAR_CAP)

    # Special-case: sentiment has no historical data; override to prior 10% per methodology.
    if "social_sentiment" in pillar_weights:
        prior = 0.10
        other = {k: v for k, v in pillar_weights.items() if k != "social_sentiment"}
        other_total = sum(other.values())
        if other_total > 0:
            scale = (1.0 - prior) / other_total
            pillar_weights = {k: v * scale for k, v in other.items()}
            pillar_weights["social_sentiment"] = prior

    return pillar_weights, signal_weights


def write_weights_file(
    pillar_weights: dict,
    signal_weights: dict,
    ic_results: dict,
    sample_size: int,
    data_window: str,
) -> Path:
    """Write a new versioned weights JSON next to the existing ones."""
    existing = sorted(_WEIGHTS_DIR.glob("weights_v*.json"))
    next_idx = len(existing) + 1
    # Parse highest version suffix if we can
    max_v = 0.0
    for p in existing:
        m = p.stem.replace("weights_v", "")
        try:
            max_v = max(max_v, float(m))
        except ValueError:
            pass
    new_version = f"v{max_v + 0.1:.1f}" if max_v >= 0.1 else "v0.2"
    out_path = _WEIGHTS_DIR / f"weights_{new_version}.json"

    payload = {
        "version": new_version,
        "created": datetime.now().strftime("%Y-%m-%d"),
        "calibration": "contemporaneous-ic",
        "notes": (
            "Contemporaneous Spearman rank-IC of signals vs. trailing 3m "
            "flow-to-starting-AUM. Underliers with existing leveraged products only. "
            "Sentiment pillar forced to 10% prior (no historical data)."
        ),
        "data_window": data_window,
        "sample_size": sample_size,
        "pillar_weights": pillar_weights,
        "signal_weights_within_pillar": signal_weights,
        "ic_per_signal": {k: (v["ic"] if v["ic"] is not None else None) for k, v in ic_results.items()},
        "n_per_signal": {k: v["n"] for k, v in ic_results.items()},
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    log.info("Wrote weights file: %s", out_path)
    return out_path


def run_calibration(write: bool = False, skip_sentiment: bool = True) -> dict:
    target = load_underlier_flow_target()
    panel = build_signal_panel(skip_sentiment=skip_sentiment)
    ic = compute_signal_ic(panel, target)

    print("\n=== Spearman rank-IC per signal ===")
    for sig, info in sorted(ic.items(), key=lambda kv: (kv[1]["pillar"], kv[0])):
        ic_val = info["ic"]
        print(f"  [{info['pillar']:24s}] {sig:22s}  ic={ic_val:+.3f}" if ic_val is not None
              else f"  [{info['pillar']:24s}] {sig:22s}  ic=  n/a   (n={info['n']})")

    pillar_w, signal_w = derive_weights(ic)

    print("\n=== Pillar weights ===")
    for pillar, w in pillar_w.items():
        print(f"  {pillar:26s} {w:.1%}")

    if write:
        n_target = len(target)
        window = f"snapshot-{datetime.now().strftime('%Y-%m-%d')}"
        write_weights_file(pillar_w, signal_w, ic, sample_size=n_target, data_window=window)

    return {"ic": ic, "pillar_weights": pillar_w, "signal_weights": signal_w}


def _parse_args():
    p = argparse.ArgumentParser(description="L&I engine weight calibration")
    p.add_argument("--write", action="store_true", help="Write a new weights_vN.json file")
    p.add_argument("--with-sentiment", action="store_true",
                   help="Include ApeWisdom in the signal panel (slow, rate-limited)")
    return p.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()
    run_calibration(write=args.write, skip_sentiment=not args.with_sentiment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
