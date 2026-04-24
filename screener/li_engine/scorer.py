"""Scoring logic: z-score, clip, aggregate, rank.

Convention:
    - Higher raw value = better (positive signal) for: turnover, adv, market_cap,
      total_oi, put_call_skew, realized_vol_*, density_score, oc_*, mentions_*.
    - No inverted signals in v0.1 (short interest lives in the existing
      analysis_3x.py pipeline; consider folding in later).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from screener.li_engine.weights import Weights

log = logging.getLogger(__name__)

PILLAR_SIGNALS: dict[str, list[str]] = {
    "liquidity_demand": ["turnover_30d", "adv_30d", "market_cap"],
    "options_demand": ["total_oi", "put_call_skew"],
    "volatility": ["realized_vol_30d", "realized_vol_90d"],
    "competitive_whitespace": ["density_score"],
    "korean_overnight": ["oc_volume_1w", "oc_wow_delta", "oc_1w_3m_ratio"],
    "social_sentiment": ["mentions_24h", "mentions_delta_24h"],
}


def _zscore(s: pd.Series, clip: float = 3.0, log_transform: bool = False) -> pd.Series:
    """Cross-sectional z-score with optional log1p transform for skewed data."""
    if s.empty:
        return s
    x = s.copy()
    if log_transform:
        x = np.log1p(x.clip(lower=0))
    mu = x.mean(skipna=True)
    sigma = x.std(skipna=True)
    if not sigma or np.isnan(sigma):
        return pd.Series(0.0, index=s.index)
    z = (x - mu) / sigma
    return z.clip(-clip, clip)


# Signals that are heavily right-skewed and benefit from log-transform before z-scoring
_LOG_TRANSFORM_SIGNALS = {
    "turnover_30d", "adv_30d", "market_cap", "total_oi",
    "oc_volume_1w", "oc_wow_delta", "mentions_24h", "mentions_delta_24h",
}


def build_panel(signal_frames: dict[str, pd.DataFrame | pd.Series]) -> pd.DataFrame:
    """Outer-join all signal frames into one panel indexed by ticker."""
    frames = []
    for name, f in signal_frames.items():
        if isinstance(f, pd.Series):
            f = f.to_frame(name=f.name or name)
        if f is None or len(f) == 0:
            continue
        if not f.index.is_unique:
            log.warning("build_panel: %s has %d duplicate indices, keeping first",
                        name, f.index.duplicated().sum())
            f = f[~f.index.duplicated(keep="first")]
        frames.append(f)
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, axis=1)
    panel = panel.loc[:, ~panel.columns.duplicated()]
    return panel


def score_panel(panel: pd.DataFrame, weights: Weights) -> pd.DataFrame:
    """Produce per-ticker scores.

    Output columns:
        - <signal>__z    : clipped z-score per signal
        - <pillar>_score : weighted within-pillar z-score
        - final_score    : weighted sum across pillars, rescaled to 0-100 percentile
        - top_pillar     : strongest contributing pillar
        - n_signals      : count of non-null raw signals per ticker
    """
    if panel.empty:
        return panel

    out = panel.copy()

    for pillar, sigs in PILLAR_SIGNALS.items():
        for sig in sigs:
            if sig in out.columns:
                out[f"{sig}__z"] = _zscore(out[sig], log_transform=(sig in _LOG_TRANSFORM_SIGNALS))
            else:
                out[f"{sig}__z"] = np.nan

    pillar_score_cols = []
    for pillar, sigs in PILLAR_SIGNALS.items():
        sig_w = weights.signal_weights_within_pillar.get(pillar, {})
        z_cols = [f"{s}__z" for s in sigs]
        available = out[z_cols].copy()
        weight_vec = pd.Series({f"{s}__z": sig_w.get(s, 0.0) for s in sigs})
        present_mask = available.notna()
        weighted = available.multiply(weight_vec, axis=1)
        weight_sum = present_mask.multiply(weight_vec, axis=1).sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            pillar_score = weighted.sum(axis=1) / weight_sum.replace(0, np.nan)
        col = f"{pillar}_score"
        out[col] = pillar_score
        pillar_score_cols.append(col)

    final = pd.Series(0.0, index=out.index)
    pillar_weight_sum = pd.Series(0.0, index=out.index)
    for pillar in PILLAR_SIGNALS.keys():
        col = f"{pillar}_score"
        w = weights.pillar_weights.get(pillar, 0.0)
        present = out[col].notna()
        final = final.add(out[col].fillna(0.0) * w * present.astype(float), fill_value=0.0)
        pillar_weight_sum = pillar_weight_sum.add(w * present.astype(float), fill_value=0.0)

    raw_final = final / pillar_weight_sum.replace(0, np.nan)
    out["final_raw"] = raw_final
    ranked = raw_final.rank(pct=True, na_option="keep") * 100.0
    out["final_score"] = ranked

    contribs = out[pillar_score_cols].copy()
    out["top_pillar"] = contribs.idxmax(axis=1).where(contribs.notna().any(axis=1))
    out["n_signals"] = panel[[c for c in panel.columns if c in {s for sigs in PILLAR_SIGNALS.values() for s in sigs}]].notna().sum(axis=1)

    return out
