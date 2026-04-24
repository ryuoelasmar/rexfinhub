"""L&I Recommender Engine.

Unified scoring engine for leveraged/inverse ETF candidate recommendation.
See docs/LI_ENGINE_METHODOLOGY.md for the weighting methodology.
"""
from screener.li_engine.engine import score_universe, run_engine
from screener.li_engine.weights import load_weights, DEFAULT_WEIGHTS

__all__ = ["score_universe", "run_engine", "load_weights", "DEFAULT_WEIGHTS"]
