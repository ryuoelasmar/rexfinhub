"""Weight loader for the L&I engine.

Weights live in versioned JSON files alongside this module. The active version
is resolved by symlink / filename convention (latest numbered file). Each
recalibration run writes a new versioned file; old files are retained for
reproducibility.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_WEIGHTS_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Weights:
    version: str
    pillar_weights: dict[str, float]
    signal_weights_within_pillar: dict[str, dict[str, float]]
    calibration: str
    created: str
    ic_per_signal: dict[str, float] | None
    sample_size: int | None

    def validate(self) -> None:
        pillar_sum = sum(self.pillar_weights.values())
        if abs(pillar_sum - 1.0) > 0.01:
            raise ValueError(f"pillar weights sum to {pillar_sum:.3f}, expected 1.0")
        for pillar, sigs in self.signal_weights_within_pillar.items():
            s = sum(sigs.values())
            if abs(s - 1.0) > 0.01:
                raise ValueError(f"{pillar} signal weights sum to {s:.3f}, expected 1.0")


DEFAULT_WEIGHTS = Weights(
    version="v0.1-inline",
    pillar_weights={
        "liquidity_demand": 0.25,
        "options_demand": 0.20,
        "volatility": 0.10,
        "competitive_whitespace": 0.20,
        "korean_overnight": 0.10,
        "social_sentiment": 0.15,
    },
    signal_weights_within_pillar={
        "liquidity_demand": {"turnover_30d": 0.50, "adv_30d": 0.30, "market_cap": 0.20},
        "options_demand": {"total_oi": 0.70, "put_call_skew": 0.30},
        "volatility": {"realized_vol_30d": 0.60, "realized_vol_90d": 0.40},
        "competitive_whitespace": {"density_score": 1.00},
        "korean_overnight": {"oc_volume_1w": 0.50, "oc_wow_delta": 0.30, "oc_1w_3m_ratio": 0.20},
        "social_sentiment": {"mentions_24h": 0.50, "mentions_delta_24h": 0.50},
    },
    calibration="priors",
    created="2026-04-22",
    ic_per_signal=None,
    sample_size=None,
)


def _latest_weights_file() -> Path | None:
    candidates = sorted(_WEIGHTS_DIR.glob("weights_v*.json"))
    return candidates[-1] if candidates else None


def load_weights(path: Path | None = None) -> Weights:
    if path is None:
        path = _latest_weights_file()
    if path is None or not path.exists():
        log.warning("No weights file found, using inline DEFAULT_WEIGHTS")
        DEFAULT_WEIGHTS.validate()
        return DEFAULT_WEIGHTS

    with path.open("r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)

    w = Weights(
        version=data["version"],
        pillar_weights=data["pillar_weights"],
        signal_weights_within_pillar=data["signal_weights_within_pillar"],
        calibration=data.get("calibration", "unknown"),
        created=data.get("created", "unknown"),
        ic_per_signal=data.get("ic_per_signal"),
        sample_size=data.get("sample_size"),
    )
    w.validate()
    log.info("Loaded weights %s (calibration=%s, created=%s)", w.version, w.calibration, w.created)
    return w
