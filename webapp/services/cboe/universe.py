"""Ticker universe generation + daily-tier selection.

The universe is every 1-4 letter uppercase alpha combo (475,254 total).
The daily tier is the subset we re-check every night: all 1-3 letter combos
(18,278), every symbol currently known-available (to catch claims fast),
and a rolling 50k-ticker slice of the 4-letter space (picks up where the
last run left off, wraps around at the end).
"""
from __future__ import annotations

from itertools import product
from string import ascii_uppercase
from typing import Iterator

from sqlalchemy.orm import Session

from webapp.models import CboeScanRun, CboeSymbol

ROLLING_BATCH_SIZE = 50_000

_RESUMABLE_TIERS = ("daily", "4-letter-batch", "full", "4-letter")


def combos_of_length(n: int) -> Iterator[str]:
    for combo in product(ascii_uppercase, repeat=n):
        yield "".join(combo)


def all_1_to_3_letter() -> list[str]:
    out: list[str] = []
    for n in (1, 2, 3):
        out.extend(combos_of_length(n))
    return out


def all_4_letter() -> list[str]:
    return list(combos_of_length(4))


def previously_available(db: Session) -> list[str]:
    rows = db.query(CboeSymbol.ticker).filter(CboeSymbol.available.is_(True)).all()
    return [r[0] for r in rows]


def _last_4_letter_checkpoint(db: Session) -> str | None:
    row = (
        db.query(CboeScanRun.last_ticker_scanned)
        .filter(CboeScanRun.tier.in_(_RESUMABLE_TIERS))
        .filter(CboeScanRun.last_ticker_scanned.isnot(None))
        .order_by(CboeScanRun.started_at.desc())
        .first()
    )
    if row is None:
        return None
    candidate = row[0]
    return candidate if candidate and len(candidate) == 4 else None


def rolling_4_letter_batch(db: Session, batch_size: int = ROLLING_BATCH_SIZE) -> list[str]:
    """Slice of the 4-letter space starting after the last checkpoint; wraps."""
    space = all_4_letter()
    checkpoint = _last_4_letter_checkpoint(db)
    start = 0
    if checkpoint is not None:
        try:
            start = space.index(checkpoint) + 1
        except ValueError:
            start = 0
    if start >= len(space):
        start = 0
    end = start + batch_size
    if end <= len(space):
        return space[start:end]
    # wrap
    return space[start:] + space[: end - len(space)]


def daily_tier(db: Session, rolling_batch_size: int = ROLLING_BATCH_SIZE) -> list[str]:
    """1-3 letter + previously-available + rolling 4-letter batch. De-duplicated."""
    seen: set[str] = set()
    out: list[str] = []

    def _add(t: str) -> None:
        if t not in seen:
            seen.add(t)
            out.append(t)

    for t in all_1_to_3_letter():
        _add(t)
    for t in previously_available(db):
        _add(t)
    for t in rolling_4_letter_batch(db, rolling_batch_size):
        _add(t)
    return out


def tier_by_name(db: Session, tier: str) -> list[str]:
    if tier == "1-letter":
        return list(combos_of_length(1))
    if tier == "2-letter":
        return list(combos_of_length(2))
    if tier == "3-letter":
        return list(combos_of_length(3))
    if tier == "4-letter":
        return all_4_letter()
    if tier == "daily":
        return daily_tier(db)
    if tier == "full":
        return all_1_to_3_letter() + all_4_letter()
    raise ValueError(f"Unknown tier: {tier!r}")
