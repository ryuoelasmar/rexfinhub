"""L&I Recommender Engine — orchestrator.

Pulls signals from every pillar, joins them into a panel, scores, and returns
a ranked recommendation table. CLI entry via `python -m screener.li_engine`.
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

import pandas as pd

from screener.li_engine.signals import (
    load_bbg_stock_signals,
    load_competitive_whitespace,
    load_etf_ticker_set,
    load_oc_equity_signals,
    load_rex_filing_status,
    load_sentiment_signals,
)
from screener.li_engine.scorer import build_panel, score_panel
from screener.li_engine.weights import Weights, load_weights

log = logging.getLogger(__name__)


def score_universe(
    pipeline_run_id: int | None = None,
    weights: Weights | None = None,
    skip_sentiment: bool = False,
    underliers_only: bool = False,
) -> pd.DataFrame:
    """Produce a ranked scoring table for every ticker in the universe.

    Args:
        pipeline_run_id: Specific bbg snapshot to score. Default = latest completed run.
        weights: Loaded Weights object; falls back to latest versioned file.
        skip_sentiment: If True, skip ApeWisdom fetch (useful for offline / rate-limited runs).
        underliers_only: If True, strip rows that are existing ETFs/ETPs — filing
            recommendations should target underliers, not products.
    """
    if weights is None:
        weights = load_weights()

    bbg = load_bbg_stock_signals(pipeline_run_id=pipeline_run_id)
    whitespace = load_competitive_whitespace()
    oc = load_oc_equity_signals()
    sentiment = pd.DataFrame() if skip_sentiment else load_sentiment_signals()
    filing_status = load_rex_filing_status()

    panel = build_panel({
        "bbg": bbg,
        "whitespace": whitespace.to_frame("density_score") if isinstance(whitespace, pd.Series) else whitespace,
        "oc": oc,
        "sentiment": sentiment,
    })

    # Density default: if a ticker is absent from the competitive whitespace query
    # (no existing leveraged products), it occupies MAX whitespace, not unknown.
    if "density_score" in panel.columns:
        panel["density_score"] = panel["density_score"].fillna(1.0)

    scored = score_panel(panel, weights)

    if not filing_status.empty:
        scored = scored.join(filing_status, how="left")
        for col in ("has_rex_filing", "has_rex_launch"):
            if col in scored.columns:
                scored[col] = scored[col].fillna(False)
        if "rex_filing_count" in scored.columns:
            scored["rex_filing_count"] = scored["rex_filing_count"].fillna(0).astype(int)

    if underliers_only:
        etfs = load_etf_ticker_set()
        before = len(scored)
        scored = scored[~scored.index.isin(etfs)]
        log.info("Filtered %d ETFs out, %d underlier rows remain", before - len(scored), len(scored))

    scored = scored.sort_values("final_score", ascending=False)
    scored.index.name = "ticker"
    return scored


def _format_summary(scored: pd.DataFrame, top_n: int = 25) -> str:
    lines = []
    pillar_cols = [c for c in scored.columns if c.endswith("_score") and c != "final_score"]
    view_cols = ["final_score", "n_signals", "top_pillar", "has_rex_filing", "has_rex_launch"] + pillar_cols
    view_cols = [c for c in view_cols if c in scored.columns]
    lines.append(scored[view_cols].head(top_n).round(2).to_string())
    return "\n".join(lines)


def run_engine(
    pipeline_run_id: int | None = None,
    weights_path: Path | None = None,
    skip_sentiment: bool = False,
    underliers_only: bool = False,
    output_csv: Path | None = None,
    top_n: int = 25,
    persist: bool = True,
) -> pd.DataFrame:
    """CLI-facing entry point. Scores, optionally writes CSV, returns the frame."""
    weights = load_weights(weights_path) if weights_path else load_weights()
    scored = score_universe(
        pipeline_run_id=pipeline_run_id,
        weights=weights,
        skip_sentiment=skip_sentiment,
        underliers_only=underliers_only,
    )
    if output_csv:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        scored.to_csv(output_csv)
        log.info("Wrote %d rows to %s", len(scored), output_csv)
    if persist:
        try:
            from screener.li_engine.persistence import write_run
            write_run(
                scored,
                weights_version=weights.version,
                pipeline_run_id=pipeline_run_id,
                skip_sentiment=skip_sentiment,
            )
        except Exception as e:
            log.warning("persist: %s", e)
    print(_format_summary(scored, top_n=top_n))
    return scored


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="L&I Recommender Engine")
    p.add_argument("--run-id", type=int, default=None, help="mkt_pipeline_runs.id to score (default=latest)")
    p.add_argument("--weights", type=Path, default=None, help="Path to weights JSON (default=latest versioned)")
    p.add_argument("--skip-sentiment", action="store_true", help="Skip ApeWisdom fetch")
    p.add_argument("--underliers-only", action="store_true", help="Filter out existing ETFs/ETPs; score underliers only")
    p.add_argument("--out", type=Path, default=None, help="Write full scored CSV to this path")
    p.add_argument("--top", type=int, default=25, help="Rows to print to stdout")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_engine(
        pipeline_run_id=args.run_id,
        weights_path=args.weights,
        skip_sentiment=args.skip_sentiment,
        underliers_only=args.underliers_only,
        output_csv=args.out,
        top_n=args.top,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
