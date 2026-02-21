"""
Advanced Market Intelligence routes.

Routes:
  GET /market/timeline   -> Fund Lifecycle Timeline (per-trust filing history)
  GET /market/calendar   -> Compliance Calendar (upcoming extensions, recent effectivities)
  GET /market/compare    -> Fund Comparison (side-by-side ticker comparison)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Query, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from webapp.dependencies import get_db

log = logging.getLogger(__name__)
router = APIRouter(prefix="/market", tags=["market-advanced"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/timeline")
def timeline_view(
    request: Request,
    trust_id: int = Query(default=None),
    db: Session = Depends(get_db),
):
    """Fund Lifecycle Timeline - shows filing history for a selected trust."""
    from webapp.models import Trust, Filing, FundExtraction

    trusts = db.execute(select(Trust).order_by(Trust.name)).scalars().all()

    timeline_items = []
    selected_trust = None

    if trust_id:
        selected_trust = db.get(Trust, trust_id)
        if selected_trust:
            filings = db.execute(
                select(Filing)
                .where(Filing.trust_id == trust_id)
                .where(Filing.form.in_(["485BPOS", "485BXT", "485APOS", "N-14"]))
                .order_by(desc(Filing.filing_date))
                .limit(200)
            ).scalars().all()

            for filing in filings:
                extractions = db.execute(
                    select(FundExtraction)
                    .where(FundExtraction.filing_id == filing.id)
                    .limit(10)
                ).scalars().all()

                # Get effective date from first extraction if available
                eff_date = None
                for ext in extractions:
                    if ext.effective_date:
                        eff_date = ext.effective_date
                        break

                timeline_items.append({
                    "filing": filing,
                    "extractions": extractions,
                    "fund_count": len(extractions),
                    "effective_date": eff_date,
                })

    return templates.TemplateResponse("market/timeline.html", {
        "request": request,
        "active_tab": "timeline",
        "available": True,
        "trusts": trusts,
        "selected_trust": selected_trust,
        "trust_id": trust_id,
        "timeline_items": timeline_items,
    })


@router.get("/calendar")
def calendar_view(
    request: Request,
    db: Session = Depends(get_db),
):
    """Compliance Calendar - upcoming 485BXT extensions and recent 485BPOS effectivities."""
    from webapp.models import Trust, Filing, FundExtraction

    today = date.today()

    # Upcoming 485BXT extensions: filings with future effective dates
    upcoming_rows = db.execute(
        select(FundExtraction, Filing, Trust)
        .join(Filing, FundExtraction.filing_id == Filing.id)
        .join(Trust, Filing.trust_id == Trust.id)
        .where(Filing.form == "485BXT")
        .where(FundExtraction.effective_date >= today)
        .order_by(FundExtraction.effective_date.asc())
        .limit(100)
    ).all()

    # Recently effective 485BPOS (last 30 days)
    recent_cutoff = today - timedelta(days=30)
    recently_rows = db.execute(
        select(FundExtraction, Filing, Trust)
        .join(Filing, FundExtraction.filing_id == Filing.id)
        .join(Trust, Filing.trust_id == Trust.id)
        .where(Filing.form == "485BPOS")
        .where(FundExtraction.effective_date >= recent_cutoff)
        .order_by(FundExtraction.effective_date.desc())
        .limit(50)
    ).all()

    # Deduplicate by accession_number (multiple extractions per filing)
    seen_upcoming = set()
    upcoming_classified = []
    for extraction, filing, trust in upcoming_rows:
        if filing.accession_number in seen_upcoming:
            continue
        seen_upcoming.add(filing.accession_number)
        days_until = (extraction.effective_date - today).days if extraction.effective_date else None
        urgency = "green"
        if days_until is not None:
            if days_until < 30:
                urgency = "red"
            elif days_until < 60:
                urgency = "amber"
        upcoming_classified.append({
            "filing": filing,
            "trust": trust,
            "effective_date": extraction.effective_date,
            "days_until": days_until,
            "urgency": urgency,
        })

    seen_recent = set()
    recently_effective = []
    for extraction, filing, trust in recently_rows:
        if filing.accession_number in seen_recent:
            continue
        seen_recent.add(filing.accession_number)
        recently_effective.append({
            "filing": filing,
            "trust": trust,
            "effective_date": extraction.effective_date,
        })

    return templates.TemplateResponse("market/calendar.html", {
        "request": request,
        "active_tab": "calendar",
        "available": True,
        "today": today,
        "upcoming": upcoming_classified,
        "recently_effective": recently_effective,
    })


@router.get("/compare")
def compare_view(
    request: Request,
    tickers: str = Query(default=""),
):
    """Fund Comparison - side-by-side comparison of up to 4 tickers."""
    from webapp.services.market_data import get_master_data, data_available

    available = data_available()
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()][:4]

    fund_data = []
    if available and ticker_list:
        try:
            master = get_master_data()
            ticker_col = next((c for c in master.columns if c.lower() == "ticker"), None)
            if ticker_col:
                for ticker in ticker_list:
                    row = master[master[ticker_col].str.upper() == ticker]
                    if not row.empty:
                        r = row.iloc[0]
                        fund_data.append({
                            "ticker": ticker,
                            "row": r.to_dict(),
                        })
        except Exception:
            log.exception("Error loading compare data")

    return templates.TemplateResponse("market/compare.html", {
        "request": request,
        "active_tab": "compare",
        "available": available,
        "tickers": tickers,
        "ticker_list": ticker_list,
        "fund_data": fund_data,
    })
