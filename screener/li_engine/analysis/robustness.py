"""Robustness checks on suspect v3 findings.

Ryu flagged: why is insider_pct weighted 14.5% when total_oi (which has
301-product historical backing at r_log=0.646) is only 2.7%? Also: why 1y
return keeps but no short-window burst / breakout signal?

This module runs targeted diagnostics to answer those questions directly.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PANEL = _ROOT / "data" / "analysis" / "expanded_signal_panel_v2.parquet"


def _zscore(s: pd.Series) -> pd.Series:
    mu, sd = s.mean(), s.std()
    if sd == 0 or np.isnan(sd):
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd


# ---------------------------------------------------------------------------
# Robustness: insider_pct
# ---------------------------------------------------------------------------

def insider_pct_robustness(panel: pd.DataFrame) -> dict:
    """Is insider_pct a real signal or are a few small-cap outliers driving it?"""
    sub = panel.dropna(subset=["insider_pct", "forward_flow_30d", "market_cap"]).copy()
    out = {"n_base": len(sub)}

    x, y, mc = sub["insider_pct"], sub["forward_flow_30d"], sub["market_cap"]

    # Base
    rho, _ = spearmanr(x, y)
    out["base_ic"] = float(rho)

    # Winsorize insider_pct to [5%, 95%]
    xw = x.clip(x.quantile(0.05), x.quantile(0.95))
    rho_w, _ = spearmanr(xw, y)
    out["winsorized_5_95_ic"] = float(rho_w)

    # Remove extreme outliers (top/bottom 5%)
    idx = (x > x.quantile(0.05)) & (x < x.quantile(0.95))
    rho_out, _ = spearmanr(x[idx], y[idx])
    out["no_extremes_ic"] = float(rho_out)
    out["n_no_extremes"] = int(idx.sum())

    # By market cap tercile
    by_tercile = {}
    qcut = pd.qcut(mc, 3, labels=["small", "mid", "large"])
    for tercile in ["small", "mid", "large"]:
        mask = (qcut == tercile)
        if mask.sum() < 10:
            by_tercile[tercile] = None
            continue
        r, _ = spearmanr(x[mask], y[mask])
        by_tercile[tercile] = {"ic": float(r), "n": int(mask.sum())}
    out["by_mktcap_tercile"] = by_tercile

    # Size-partialled: regress insider_pct on log(market_cap), take residual, correlate with flow
    import statsmodels.api as sm
    X = sm.add_constant(np.log1p(mc))
    try:
        res = sm.OLS(x, X).fit()
        insider_resid = x - res.fittedvalues
        r_partial, _ = spearmanr(insider_resid, y)
        out["size_partialled_ic"] = float(r_partial)
    except Exception as e:
        out["size_partialled_ic"] = None
        out["partial_error"] = str(e)

    # Bootstrap CI
    rng = np.random.default_rng(42)
    boots = []
    n = len(x)
    for _ in range(2000):
        idx_b = rng.integers(0, n, n)
        r, _ = spearmanr(x.iloc[idx_b], y.iloc[idx_b])
        if not np.isnan(r):
            boots.append(r)
    boots = np.array(boots)
    out["bootstrap_median"] = float(np.median(boots))
    out["bootstrap_95ci"] = (float(np.percentile(boots, 2.5)),
                              float(np.percentile(boots, 97.5)))
    out["bootstrap_p_pos"] = float((boots > 0).mean())

    # Top-10 insider_pct tickers — who are they?
    top10 = sub.nlargest(10, "insider_pct")[["insider_pct", "market_cap", "forward_flow_30d"]]
    out["top10_insider_pct"] = top10.to_dict(orient="index")

    return out


# ---------------------------------------------------------------------------
# Breakout / co-movement signal
# ---------------------------------------------------------------------------

def breakout_signal_tests(panel: pd.DataFrame) -> dict:
    """Does a combined ret × mentions signal outperform either alone?"""
    sub = panel.dropna(subset=["forward_flow_30d"]).copy()
    out = {"n_base": len(sub)}

    tgt = sub["forward_flow_30d"]

    # Variants
    def _pair_ic(x, y):
        m = x.notna() & y.notna()
        if m.sum() < 15:
            return None, int(m.sum())
        r, _ = spearmanr(x[m], y[m])
        return float(r), int(m.sum())

    variants = {}

    # Solo baselines
    if "ret_1m" in sub:
        variants["ret_1m_solo"] = _pair_ic(sub["ret_1m"], tgt)
    if "mentions_24h" in sub:
        variants["mentions_24h_solo"] = _pair_ic(sub["mentions_24h"], tgt)

    # Multiplicative (z-score × z-score) — captures "both up together"
    if "ret_1m" in sub and "mentions_24h" in sub:
        z_ret = _zscore(sub["ret_1m"])
        z_ment = _zscore(sub["mentions_24h"].fillna(0))
        variants["breakout_ret1m_x_mentions"] = _pair_ic(z_ret * z_ment, tgt)

    # AND condition: both above median
    if "ret_1m" in sub and "mentions_24h" in sub:
        both_hot = ((sub["ret_1m"] > sub["ret_1m"].median()) &
                    (sub["mentions_24h"].fillna(0) > sub["mentions_24h"].fillna(0).median())).astype(int)
        variants["breakout_both_above_median"] = _pair_ic(both_hot, tgt)

    # Vol spike × price move
    if "ret_1m" in sub and "rvol_90d" in sub:
        z_ret = _zscore(sub["ret_1m"])
        z_vol = _zscore(sub["rvol_90d"])
        variants["breakout_ret1m_x_vol"] = _pair_ic(z_ret * z_vol, tgt)

    # Short-horizon composite: top-decile in BOTH ret_1m and mentions
    if "ret_1m" in sub and "mentions_24h" in sub:
        q_ret = sub["ret_1m"].rank(pct=True)
        q_ment = sub["mentions_24h"].fillna(0).rank(pct=True)
        composite = q_ret + q_ment
        variants["sum_of_ranks_ret1m_mentions"] = _pair_ic(composite, tgt)

    # Breakout against 52w high: near-high AND high vol AND positive mentions change
    # (we don't have mentions_delta here cleanly, so use mentions_24h as proxy)
    if all(c in sub for c in ["pct_of_52w_high", "rvol_90d", "mentions_24h"]):
        z_p52 = _zscore(sub["pct_of_52w_high"])
        z_v = _zscore(sub["rvol_90d"])
        z_m = _zscore(sub["mentions_24h"].fillna(0))
        # Multiplicative breakout: near 52w high AND high vol AND attention
        breakout = z_p52 + z_v + z_m
        variants["breakout_52w_vol_mentions"] = _pair_ic(breakout, tgt)

    out["variants"] = {k: {"ic": v[0], "n": v[1]} if v[0] is not None else None for k, v in variants.items()}

    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    panel = pd.read_parquet(PANEL)
    log.info("Panel: %d rows x %d cols", *panel.shape)

    print("=" * 70)
    print("INSIDER_PCT ROBUSTNESS")
    print("=" * 70)
    r = insider_pct_robustness(panel)
    for k, v in r.items():
        if k in ("top10_insider_pct",):
            continue
        print(f"  {k:30s} {v}")

    print("\n  Top-10 insider_pct holdings (highest 10):")
    for tk, vals in r["top10_insider_pct"].items():
        print(f"    {tk:8s} insider={vals['insider_pct']:6.1f}% "
              f"mktcap={vals['market_cap']:12,.0f} fwd_flow={vals['forward_flow_30d']:8.2f}")

    print("\n" + "=" * 70)
    print("BREAKOUT / CO-MOVEMENT SIGNAL TESTS")
    print("=" * 70)
    b = breakout_signal_tests(panel)
    print(f"  sample: {b['n_base']}")
    for name, stats in b["variants"].items():
        if stats is None:
            print(f"  {name:40s} insufficient data")
            continue
        print(f"  {name:40s} IC={stats['ic']:+.3f} n={stats['n']}")


if __name__ == "__main__":
    main()
