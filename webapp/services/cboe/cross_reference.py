"""Join cboe_symbols against mkt_master_data for the /filings/symbols UI.

A taken (available=False) ticker is "active" if mkt_master_data knows the
fund, "reserved" if it doesn't — that gap is the competitor-pipeline signal.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from webapp.models import CboeScanRun, CboeStateChange, CboeSymbol, MktMasterData

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000


def _mkt_label_subquery(db: Session):
    """One representative row per ticker from mkt_master_data (first-by-min label)."""
    return (
        db.query(
            MktMasterData.ticker.label("ticker"),
            func.min(MktMasterData.fund_name).label("fund_name"),
            func.min(MktMasterData.issuer).label("issuer"),
            func.min(MktMasterData.listed_exchange).label("listed_exchange"),
            func.min(MktMasterData.etp_category).label("etp_category"),
        )
        .group_by(MktMasterData.ticker)
        .subquery()
    )


def enriched_rows(
    db: Session,
    *,
    length: int | None = None,
    state: str | None = None,
    search: str | None = None,
    sort: str = "last_checked_desc",
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Return (rows, total_matching) for the table. state is one of:
    'available', 'reserved', 'active', 'unknown', or None (all)."""
    limit = max(1, min(limit, MAX_LIMIT))
    offset = max(0, offset)

    mkt = _mkt_label_subquery(db)

    q = (
        db.query(
            CboeSymbol.ticker,
            CboeSymbol.length,
            CboeSymbol.available,
            CboeSymbol.last_checked_at,
            mkt.c.fund_name,
            mkt.c.issuer,
            mkt.c.listed_exchange,
            mkt.c.etp_category,
        )
        .outerjoin(mkt, CboeSymbol.ticker == mkt.c.ticker)
    )

    if length is not None:
        q = q.filter(CboeSymbol.length == length)
    if state == "available":
        q = q.filter(CboeSymbol.available.is_(True))
    elif state == "active":
        q = q.filter(CboeSymbol.available.is_(False)).filter(mkt.c.ticker.isnot(None))
    elif state == "reserved":
        q = q.filter(CboeSymbol.available.is_(False)).filter(mkt.c.ticker.is_(None))
    elif state == "unknown":
        q = q.filter(CboeSymbol.available.is_(None))
    if search:
        s_upper = search.upper().strip()
        q = q.filter(
            (CboeSymbol.ticker.startswith(s_upper))
            | (mkt.c.fund_name.ilike(f"%{search}%"))
        )

    total = q.count()

    if sort == "ticker":
        q = q.order_by(CboeSymbol.ticker)
    elif sort == "length":
        q = q.order_by(CboeSymbol.length, CboeSymbol.ticker)
    elif sort == "state":
        q = q.order_by(CboeSymbol.available, CboeSymbol.ticker)
    else:
        q = q.order_by(CboeSymbol.last_checked_at.desc().nullslast())

    results = q.offset(offset).limit(limit).all()

    rows: list[dict[str, Any]] = []
    for r in results:
        if r.available is True:
            state_str = "available"
        elif r.available is False:
            state_str = "active" if r.fund_name or r.issuer else "reserved"
        else:
            state_str = "unknown"
        rows.append(
            {
                "ticker": r.ticker,
                "length": r.length,
                "available": r.available,
                "state": state_str,
                "fund_name": r.fund_name,
                "issuer": r.issuer,
                "exchange": r.listed_exchange,
                "category": r.etp_category,
                "last_checked_at": r.last_checked_at,
            }
        )
    return rows, total


def summary_counts(db: Session) -> dict[str, int]:
    """KPI strip: available / reserved / active / recently flipped."""
    available = (
        db.query(func.count(CboeSymbol.ticker))
        .filter(CboeSymbol.available.is_(True))
        .scalar()
        or 0
    )
    known_tickers = {
        t[0] for t in db.query(MktMasterData.ticker).distinct()
    }
    taken = db.query(CboeSymbol.ticker).filter(CboeSymbol.available.is_(False)).all()
    active = sum(1 for (t,) in taken if t in known_tickers)
    reserved = sum(1 for (t,) in taken if t not in known_tickers)
    since = datetime.utcnow() - timedelta(hours=24)
    flipped = (
        db.query(func.count(CboeStateChange.id))
        .filter(CboeStateChange.detected_at >= since)
        .scalar()
        or 0
    )
    return {
        "available": available,
        "reserved": reserved,
        "active": active,
        "recently_flipped_24h": flipped,
    }


def last_scan(db: Session) -> dict[str, Any] | None:
    row = (
        db.query(CboeScanRun)
        .order_by(CboeScanRun.started_at.desc())
        .first()
    )
    if row is None:
        return None
    return {
        "id": row.id,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "status": row.status,
        "tier": row.tier,
        "tickers_checked": row.tickers_checked,
        "state_changes_detected": row.state_changes_detected,
    }
