"""Multi-angle analysis v2 — expanded signal set + forward-flow target.

Signals (16):
    bbg stock:   market_cap, adv_30d, turnover_30d, total_oi, put_call_skew,
                 realized_vol_30d, realized_vol_90d, short_interest
    w5 momentum: ret_5d, ret_1m, ret_3m, ret_6m, ret_ytd, ret_1y
    aum trend:   underlier_aum_12m_growth
    sentiment:   mentions_24h, mentions_delta_24h (when available)

Targets (6):
    forward_30d_flow  — NEW, from bbg daily-flow time series (predictive IC)
    raw_3m_flow       — contemporaneous
    rank_3m_flow      — size-invariant rank
    log_3m_flow       — log-transformed
    aum_growth        — contaminated target kept for contrast
    flow_adj_perf     — flow minus estimated market P&L

10 angles carried over from v1: Spearman, Pearson, Quintile spread,
Size-controlled regression, Mutual Info, RF importance, Lasso, Bootstrap CI.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from screener.li_engine.analysis.multi_angle import (
    angle_bootstrap_ic, angle_mutual_info, angle_pearson,
    angle_quintile_spread, angle_rf_importance, angle_size_controlled,
    angle_spearman, angle_lasso_survival,
)

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PANEL_PATH = _ROOT / "data" / "analysis" / "expanded_signal_panel.parquet"


SIGNALS_V2 = [
    "market_cap", "adv_30d", "turnover_30d", "total_oi", "put_call_skew",
    "realized_vol_30d", "realized_vol_90d", "short_interest",
    "ret_5d", "ret_1m", "ret_3m", "ret_6m", "ret_ytd", "ret_1y",
    "underlier_aum_12m_growth",
    "mentions_24h", "mentions_delta_24h",
]


def build_targets_v2(panel: pd.DataFrame) -> dict[str, pd.Series]:
    t = {}
    if "forward_30d_flow" in panel:
        t["forward_30d_flow"] = panel["forward_30d_flow"].dropna()
    if "flow_3m" in panel:
        t["raw_3m_flow"] = panel["flow_3m"].dropna()
        t["rank_3m_flow"] = panel["flow_3m"].rank(pct=True).dropna()
        t["log_3m_flow"] = (np.sign(panel["flow_3m"]) * np.log1p(panel["flow_3m"].abs())).dropna()
    if "flow_3m" in panel and "total_aum" in panel:
        start_aum = (panel["total_aum"] - panel["flow_3m"]).clip(lower=1e-6)
        t["aum_growth"] = (panel["flow_3m"] / start_aum).clip(-5, 5).dropna()
    if "flow_3m" in panel and "total_aum" in panel and "prod_return_3m" in panel:
        market_pnl = panel["total_aum"] * panel["prod_return_3m"].fillna(0) / 100.0 * 2.0
        t["flow_adj_perf"] = (panel["flow_3m"] - market_pnl).dropna()
    return t


@dataclass
class SignalReport:
    signal: str
    n_total: int = 0
    per_angle: dict[str, Any] = field(default_factory=dict)
    consensus_positive: int = 0
    consensus_negative: int = 0
    consensus_unclear: int = 0

    def verdict(self) -> str:
        total = self.consensus_positive + self.consensus_negative + self.consensus_unclear
        if total == 0:
            return "insufficient-data"
        if self.consensus_positive >= 0.6 * total:
            return "keep (positive)"
        if self.consensus_negative >= 0.6 * total:
            return "flip-sign (negative)"
        return "ambiguous"


def run_all_v2(panel: pd.DataFrame, signals: list[str] | None = None) -> dict[str, SignalReport]:
    signals = signals or SIGNALS_V2
    targets = build_targets_v2(panel)
    reports: dict[str, SignalReport] = {s: SignalReport(signal=s) for s in signals}

    for tgt_name, tgt in targets.items():
        log.info("=== target: %s (n=%d) ===", tgt_name, len(tgt))

        rf_imp = angle_rf_importance(panel, tgt, signals)
        lasso_coef = angle_lasso_survival(panel, tgt, signals)

        for sig in signals:
            if sig not in panel.columns:
                continue
            r = reports[sig]
            x = panel[sig]

            sp_rho, sp_n = angle_spearman(x, tgt)
            pe_r, pe_n = angle_pearson(x, tgt)
            q_spread, q_n = angle_quintile_spread(x, tgt)
            size_col = panel["market_cap"] if "market_cap" in panel else None
            sc_coef = float("nan")
            sc_n = 0
            if size_col is not None and sig != "market_cap":
                sc_coef, sc_n = angle_size_controlled(x, tgt, size_col)
            mi_val, mi_n = angle_mutual_info(x, tgt)
            boot_med, boot_ci, boot_n = angle_bootstrap_ic(x, tgt)

            r.per_angle[tgt_name] = {
                "spearman": sp_rho, "n_sp": sp_n,
                "pearson": pe_r, "n_pe": pe_n,
                "quintile_spread": q_spread,
                "size_controlled": sc_coef,
                "mutual_info": mi_val,
                "bootstrap_median": boot_med,
                "bootstrap_ci": boot_ci,
                "rf_importance": rf_imp.get(sig, float("nan")),
                "lasso_coef": lasso_coef.get(sig, float("nan")),
            }
            r.n_total = max(r.n_total, boot_n)

            sign_votes = []
            for k, v in r.per_angle[tgt_name].items():
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

    return reports


def derive_weights_v2(reports: dict[str, SignalReport]) -> dict[str, float]:
    """Consensus weighting, cluster-aware for correlated vol/momentum signals."""
    magnitudes: dict[str, float] = {}
    for sig, r in reports.items():
        if "positive" not in r.verdict():
            magnitudes[sig] = 0.0
            continue
        ics = [abs(tv["spearman"]) for tv in r.per_angle.values()
               if tv.get("spearman") is not None and not np.isnan(tv["spearman"])]
        magnitudes[sig] = float(np.median(ics)) if ics else 0.0

    clusters = {
        "vol": {"realized_vol_30d", "realized_vol_90d"},
        "momentum_short": {"ret_5d", "ret_1m"},
        "momentum_mid": {"ret_3m", "ret_6m"},
        "momentum_long": {"ret_ytd", "ret_1y"},
        "sentiment": {"mentions_24h", "mentions_delta_24h"},
    }
    scaled: dict[str, float] = dict(magnitudes)
    for cluster_name, members in clusters.items():
        in_cluster = [m for m in members if scaled.get(m, 0) > 0]
        if len(in_cluster) > 1:
            for m in in_cluster:
                scaled[m] = scaled[m] / len(in_cluster)

    total = sum(scaled.values())
    if total == 0:
        return {s: 0.0 for s in scaled}
    raw = {s: v / total for s, v in scaled.items()}
    floored = {s: (max(0.03, raw[s]) if raw[s] > 0 else 0.0) for s in raw}
    tot2 = sum(floored.values())
    normed = {s: v / tot2 for s, v in floored.items()}
    capped = {s: min(0.25, v) for s, v in normed.items()}
    tot3 = sum(capped.values())
    return {s: v / tot3 for s, v in capped.items()}


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    panel = pd.read_parquet(PANEL_PATH)
    log.info("Panel: %d rows x %d cols", *panel.shape)

    reports = run_all_v2(panel)
    weights = derive_weights_v2(reports)

    print("\n=== Signal Verdicts ===")
    for sig, r in reports.items():
        print(f"  {sig:26s} {r.verdict():25s} pos={r.consensus_positive} neg={r.consensus_negative} amb={r.consensus_unclear} n={r.n_total}")

    print("\n=== Weights ===")
    for s, w in sorted(weights.items(), key=lambda x: -x[1]):
        print(f"  {s:26s} {w:.1%}")

    return reports, weights


if __name__ == "__main__":
    main()
