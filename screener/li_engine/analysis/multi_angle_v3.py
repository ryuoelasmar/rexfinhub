"""Multi-angle analysis v3 — clean signals, fixed target, cross-sector IC.

What's different from v2:
    - Dropped target-leakage signal (underlier_aum_12m_growth)
    - Dropped collinear duplicates (rvol_30d, turnover_30d, raw short_interest)
    - Added full bbg-stock field set: ret_1m/3m/6m/1y, pct_of_52w_high,
      range_position, inst_own_pct, insider_pct, news_sentiment_bbg
    - Fixed forward-flow target (observed 30-day window, as-of 60 days ago)
    - Cross-sector IC stratified by GICS sector
    - OI variants tested separately (total / call / put / ratio / skew)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from screener.li_engine.analysis.multi_angle import (
    angle_bootstrap_ic, angle_mutual_info, angle_pearson,
    angle_quintile_spread, angle_rf_importance, angle_size_controlled,
    angle_spearman, angle_lasso_survival,
)

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PANEL_PATH = _ROOT / "data" / "analysis" / "expanded_signal_panel_v2.parquet"

SIGNALS_V3 = [
    # size (one only)
    "market_cap",
    # liquidity
    "adv_30d", "turnover",
    # options (all variants tested separately)
    "total_oi", "call_oi", "put_oi", "call_put_ratio", "put_call_skew",
    # volatility (pick one — 90d as more stable)
    "rvol_90d",
    # short interest (ratio only)
    "si_ratio",
    # momentum — underlier returns
    "ret_1m", "ret_3m", "ret_6m", "ret_1y",
    # position in range
    "pct_of_52w_high", "range_position",
    # ownership
    "inst_own_pct", "insider_pct",
    # sentiment
    "news_sentiment_bbg", "mentions_24h",
]


@dataclass
class SignalReport:
    signal: str
    n_total: int = 0
    per_target: dict[str, Any] = field(default_factory=dict)
    per_sector: dict[str, float] = field(default_factory=dict)
    consensus_positive: int = 0
    consensus_negative: int = 0
    consensus_unclear: int = 0
    corr_with_others: dict[str, float] = field(default_factory=dict)

    def verdict(self) -> str:
        total = self.consensus_positive + self.consensus_negative + self.consensus_unclear
        if total == 0:
            return "insufficient-data"
        if self.consensus_positive >= 0.6 * total:
            return "keep (positive)"
        if self.consensus_negative >= 0.6 * total:
            return "flip-sign (negative)"
        return "ambiguous"


def build_targets(panel: pd.DataFrame) -> dict[str, pd.Series]:
    t = {}
    if "forward_flow_30d" in panel.columns:
        ff = panel["forward_flow_30d"].dropna()
        t["forward_flow_30d"] = ff
        t["rank_forward_flow"] = ff.rank(pct=True)
        t["log_forward_flow"] = np.sign(ff) * np.log1p(ff.abs())
    if "aum_as_of" in panel.columns and "forward_flow_30d" in panel.columns:
        # flow-to-starting-AUM (documented bias, kept for comparison)
        joined = panel.dropna(subset=["forward_flow_30d", "aum_as_of"])
        joined = joined[joined["aum_as_of"] > 0]
        t["flow_to_aum"] = (joined["forward_flow_30d"] / joined["aum_as_of"]).clip(-10, 10)
    return t


def compute_signal_correlations(panel: pd.DataFrame, signals: list[str]) -> pd.DataFrame:
    """Spearman corr matrix across signals. Flags redundancy."""
    avail = [s for s in signals if s in panel.columns]
    return panel[avail].corr("spearman").round(2)


def run_cross_sector(panel: pd.DataFrame, signal: str, target: pd.Series,
                     min_n: int = 10) -> dict[str, float]:
    """Per-sector Spearman IC. Only returns sectors with enough observations."""
    if "gics_sector" not in panel.columns:
        return {}
    idx = target.index.intersection(panel.index)
    sub = panel.loc[idx].copy()
    sub["_target"] = target.loc[idx]
    sub = sub.dropna(subset=[signal, "_target", "gics_sector"])

    out = {}
    for sector, grp in sub.groupby("gics_sector"):
        if len(grp) < min_n:
            continue
        try:
            rho, _ = spearmanr(grp[signal], grp["_target"])
            if not np.isnan(rho):
                out[sector] = float(rho)
        except Exception:
            continue
    return out


def run_all_v3(panel: pd.DataFrame, signals: list[str] | None = None) -> tuple[dict[str, SignalReport], pd.DataFrame]:
    signals = signals or SIGNALS_V3
    targets = build_targets(panel)
    reports: dict[str, SignalReport] = {s: SignalReport(signal=s) for s in signals}

    corr_matrix = compute_signal_correlations(panel, signals)

    for sig in signals:
        if sig not in panel.columns:
            continue
        for other in signals:
            if other != sig and other in corr_matrix.columns and sig in corr_matrix.index:
                reports[sig].corr_with_others[other] = float(corr_matrix.loc[sig, other])

    for tgt_name, tgt in targets.items():
        log.info("=== target: %s (n=%d) ===", tgt_name, len(tgt))

        rf_imp = angle_rf_importance(panel, tgt, [s for s in signals if s in panel.columns])
        lasso_coef = angle_lasso_survival(panel, tgt, [s for s in signals if s in panel.columns])

        for sig in signals:
            if sig not in panel.columns:
                continue
            r = reports[sig]
            x = panel[sig]

            sp_rho, sp_n = angle_spearman(x, tgt)
            pe_r, pe_n = angle_pearson(x, tgt)
            q_spread, q_n = angle_quintile_spread(x, tgt)
            size_col = panel["market_cap"] if "market_cap" in panel.columns else None
            sc_coef = float("nan")
            sc_n = 0
            if size_col is not None and sig != "market_cap":
                sc_coef, sc_n = angle_size_controlled(x, tgt, size_col)
            mi_val, mi_n = angle_mutual_info(x, tgt)
            boot_med, boot_ci, boot_n = angle_bootstrap_ic(x, tgt)

            r.per_target[tgt_name] = {
                "spearman": sp_rho, "n_sp": sp_n,
                "pearson": pe_r,
                "quintile_spread": q_spread,
                "size_controlled": sc_coef,
                "mutual_info": mi_val,
                "bootstrap_median": boot_med, "bootstrap_ci": boot_ci,
                "rf_importance": rf_imp.get(sig, float("nan")),
                "lasso_coef": lasso_coef.get(sig, float("nan")),
            }
            r.n_total = max(r.n_total, boot_n)

            sign_votes = []
            for k, v in r.per_target[tgt_name].items():
                if k in ("mutual_info", "rf_importance", "quintile_spread", "bootstrap_ci"):
                    continue
                if k.startswith("n_"):
                    continue
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    continue
                sign_votes.append(np.sign(v))

            if len(sign_votes) >= 3:
                pos = sum(1 for s in sign_votes if s > 0)
                neg = sum(1 for s in sign_votes if s < 0)
                if pos > neg and pos / len(sign_votes) >= 0.6:
                    r.consensus_positive += 1
                elif neg > pos and neg / len(sign_votes) >= 0.6:
                    r.consensus_negative += 1
                else:
                    r.consensus_unclear += 1

        # Cross-sector IC: primary forward target only (keeps output tractable)
        if tgt_name == "forward_flow_30d":
            for sig in signals:
                if sig not in panel.columns:
                    continue
                sect_ic = run_cross_sector(panel, sig, tgt)
                reports[sig].per_sector = sect_ic

    return reports, corr_matrix


def derive_weights_v3(reports: dict[str, SignalReport]) -> dict[str, float]:
    """Consensus-driven weights with explicit correlation-aware shrinkage."""
    mags: dict[str, float] = {}
    for sig, r in reports.items():
        if "positive" not in r.verdict():
            mags[sig] = 0.0
            continue
        ics = [abs(tv.get("spearman", float("nan"))) for tv in r.per_target.values()
               if tv.get("spearman") is not None and not np.isnan(tv["spearman"])]
        mags[sig] = float(np.median(ics)) if ics else 0.0

    # Correlation-aware shrinkage: if signal A correlates > 0.7 with a higher-magnitude
    # signal B, shrink A's weight by a factor of (1 - |corr|) to avoid double-counting.
    shrunk = dict(mags)
    for sig, m in mags.items():
        if m == 0:
            continue
        rep = reports[sig]
        for other, corr in rep.corr_with_others.items():
            if abs(corr) > 0.7 and mags.get(other, 0) > m:
                shrunk[sig] = shrunk[sig] * (1 - abs(corr))
                break

    total = sum(shrunk.values())
    if total == 0:
        return {s: 0.0 for s in shrunk}
    raw = {s: v / total for s, v in shrunk.items()}
    floored = {s: (max(0.03, raw[s]) if raw[s] > 0 else 0.0) for s in raw}
    tot2 = sum(floored.values())
    normed = {s: v / tot2 for s, v in floored.items()}
    capped = {s: min(0.25, v) for s, v in normed.items()}
    tot3 = sum(capped.values())
    return {s: v / tot3 for s, v in capped.items()}


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    panel = pd.read_parquet(PANEL_PATH)
    reports, corr = run_all_v3(panel)
    weights = derive_weights_v3(reports)

    print("\n=== Signal Verdicts ===")
    for sig, r in reports.items():
        if sig not in panel.columns:
            print(f"  {sig:24s} SIGNAL NOT IN PANEL")
            continue
        ics = [tv.get("spearman") for tv in r.per_target.values()
               if tv.get("spearman") is not None and not np.isnan(tv.get("spearman"))]
        med = np.median([abs(x) for x in ics]) if ics else float("nan")
        print(f"  {sig:24s} {r.verdict():22s} pos={r.consensus_positive} neg={r.consensus_negative} amb={r.consensus_unclear} n={r.n_total} med|IC|={med:.3f}")

    print("\n=== Weights ===")
    for s, w in sorted(weights.items(), key=lambda x: -x[1]):
        print(f"  {s:24s} {w:.1%}")

    print("\n=== Cross-sector IC on forward_flow_30d (top keepers) ===")
    for sig, r in reports.items():
        if "positive" not in r.verdict() or not r.per_sector:
            continue
        print(f"\n{sig}:")
        for sector, ic in sorted(r.per_sector.items(), key=lambda x: -abs(x[1])):
            print(f"  {sector:30s} IC={ic:+.3f}")

    return reports, weights, corr


if __name__ == "__main__":
    main()
