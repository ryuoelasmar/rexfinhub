"""
Trust router - Trust detail page with all funds.
"""
from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db
from webapp.models import Trust, FundStatus, Filing

router = APIRouter()
templates = Jinja2Templates(directory="webapp/templates")

_FORM_TOOLTIPS = {
    "485BPOS": "Post-effective amendment - fund is actively trading",
    "485BXT": "Extension filing - extends time for fund to go effective",
    "485APOS": "Initial filing - fund has 75 days to become effective",
    "497": "Supplement to existing prospectus",
    "497K": "Summary prospectus supplement",
}


def _days_since(d: date | None) -> int | None:
    if not d:
        return None
    return (date.today() - d).days


def _expected_effective(form: str | None, filing_date: date | None, eff_date: date | None) -> dict | None:
    """Compute expected effective date and days remaining."""
    if eff_date:
        days_left = (eff_date - date.today()).days
        return {"date": eff_date, "days_left": days_left}
    if form and form.upper().startswith("485A") and filing_date:
        expected = filing_date + timedelta(days=75)
        days_left = (expected - date.today()).days
        return {"date": expected, "days_left": days_left}
    return None


@router.get("/{slug}")
def trust_detail(slug: str, request: Request, db: Session = Depends(get_db)):
    trust = db.execute(select(Trust).where(Trust.slug == slug)).scalar_one_or_none()
    if not trust:
        raise HTTPException(status_code=404, detail="Trust not found")

    funds = db.execute(
        select(FundStatus).where(FundStatus.trust_id == trust.id)
    ).scalars().all()

    # Sort: PENDING first, then DELAYED, then EFFECTIVE
    status_order = {"PENDING": 0, "DELAYED": 1, "EFFECTIVE": 2, "UNKNOWN": 3}
    funds = sorted(funds, key=lambda f: (
        status_order.get(f.status, 3),
        -(f.latest_filing_date.toordinal() if f.latest_filing_date else 0),
    ))

    # Enrich with computed fields
    enriched = []
    for f in funds:
        enriched.append({
            "fund": f,
            "days_since": _days_since(f.latest_filing_date),
            "expected": _expected_effective(f.latest_form, f.latest_filing_date, f.effective_date),
            "form_tooltip": _FORM_TOOLTIPS.get((f.latest_form or "").upper(), ""),
        })

    # Status counts
    eff_count = sum(1 for f in funds if f.status == "EFFECTIVE")
    pend_count = sum(1 for f in funds if f.status == "PENDING")
    delay_count = sum(1 for f in funds if f.status == "DELAYED")

    # Recent filings for this trust
    recent_filings = db.execute(
        select(Filing)
        .where(Filing.trust_id == trust.id)
        .order_by(Filing.filing_date.desc())
        .limit(20)
    ).scalars().all()

    return templates.TemplateResponse("trust_detail.html", {
        "request": request,
        "trust": trust,
        "funds": enriched,
        "total": len(funds),
        "effective": eff_count,
        "pending": pend_count,
        "delayed": delay_count,
        "recent_filings": recent_filings,
        "today": date.today(),
    })
