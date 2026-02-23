"""
Filings router - Filing Analysis page with full search and filtering.

Standalone page for browsing all SEC filings across all trusts.
Focus is on filings (not funds) with fund name search as a secondary filter.
"""
from __future__ import annotations

import math
import urllib.parse
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db
from webapp.models import Trust, Filing, FundExtraction

router = APIRouter()
templates = Jinja2Templates(directory="webapp/templates")

PROSPECTUS_FORMS = ["485APOS", "485BPOS", "485BXT", "497", "497K"]
_DATE_RANGE_MAP = {"7": 7, "30": 30, "90": 90, "365": 365}


@router.get("/")
def filing_list(
    request: Request,
    q: str = "",
    form_type: str = "",
    trust_id: int = 0,
    date_range: str = "all",
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    """Filing search page — browse all filings with filters."""
    query = (
        select(
            Filing,
            Trust.name.label("trust_name"),
            Trust.slug.label("trust_slug"),
            Trust.is_rex.label("is_rex"),
            func.group_concat(FundExtraction.series_name.distinct()).label("fund_names"),
        )
        .join(Trust, Trust.id == Filing.trust_id)
        .outerjoin(FundExtraction, FundExtraction.filing_id == Filing.id)
        .group_by(Filing.id)
    )

    # Text search: trust name, accession, form type, AND fund names
    if q:
        # Use a subquery to find filing IDs matching fund name search
        fund_match = (
            select(FundExtraction.filing_id)
            .where(FundExtraction.series_name.ilike(f"%{q}%"))
        )
        query = query.where(or_(
            Filing.accession_number.ilike(f"%{q}%"),
            Trust.name.ilike(f"%{q}%"),
            Filing.form.ilike(f"%{q}%"),
            Filing.id.in_(fund_match),
        ))

    # Form type filter (dropdown)
    if form_type:
        query = query.where(Filing.form.ilike(f"{form_type}%"))

    # Date range filter
    days = _DATE_RANGE_MAP.get(date_range)
    if days:
        cutoff = date.today() - timedelta(days=days)
        query = query.where(Filing.filing_date >= cutoff)

    # Trust filter
    if trust_id:
        query = query.where(Filing.trust_id == trust_id)

    query = query.order_by(Filing.filing_date.desc())

    # Count total before pagination
    total_filings = db.execute(
        select(func.count()).select_from(query.subquery())
    ).scalar() or 0
    total_pages = max(1, math.ceil(total_filings / per_page))
    page = min(page, total_pages)

    # Paginated results
    results = db.execute(
        query.offset((page - 1) * per_page).limit(per_page)
    ).all()

    # Form type counts (for summary bar) — respect current filters except form_type
    count_query = select(Filing.form, func.count(Filing.id).label("cnt"))
    if q:
        fund_match_count = (
            select(FundExtraction.filing_id)
            .where(FundExtraction.series_name.ilike(f"%{q}%"))
        )
        count_query = (
            count_query
            .join(Trust, Trust.id == Filing.trust_id)
            .where(or_(
                Filing.accession_number.ilike(f"%{q}%"),
                Trust.name.ilike(f"%{q}%"),
                Filing.form.ilike(f"%{q}%"),
                Filing.id.in_(fund_match_count),
            ))
        )
    if days:
        count_query = count_query.where(Filing.filing_date >= cutoff)
    if trust_id:
        count_query = count_query.where(Filing.trust_id == trust_id)
    count_query = count_query.group_by(Filing.form)
    raw_counts = db.execute(count_query).all()

    form_counts = {}
    for form_name, cnt in raw_counts:
        key = form_name.upper().strip() if form_name else "OTHER"
        # Normalize: group 497J under 497, etc.
        if key.startswith("485B") and "BXT" not in key:
            form_counts["485BPOS"] = form_counts.get("485BPOS", 0) + cnt
        elif "BXT" in key:
            form_counts["485BXT"] = form_counts.get("485BXT", 0) + cnt
        elif key.startswith("485A"):
            form_counts["485APOS"] = form_counts.get("485APOS", 0) + cnt
        elif key.startswith("497"):
            form_counts["497"] = form_counts.get("497", 0) + cnt
        else:
            form_counts["OTHER"] = form_counts.get("OTHER", 0) + cnt

    # Total filings in DB (unfiltered) for header
    total_all = db.execute(select(func.count()).select_from(Filing)).scalar() or 0

    # Active trusts for filter dropdown
    trusts = db.execute(
        select(Trust).where(Trust.is_active == True).order_by(Trust.name)
    ).scalars().all()

    # Build query string for pagination links (preserve all filters, exclude page)
    qs_params = {}
    if q:
        qs_params["q"] = q
    if form_type:
        qs_params["form_type"] = form_type
    if trust_id:
        qs_params["trust_id"] = trust_id
    if date_range != "all":
        qs_params["date_range"] = date_range
    if per_page != 50:
        qs_params["per_page"] = per_page
    base_qs = urllib.parse.urlencode(qs_params)

    return templates.TemplateResponse("filing_list.html", {
        "request": request,
        "filings": results,
        "trusts": trusts,
        "q": q,
        "form_type": form_type,
        "trust_id": trust_id,
        "date_range": date_range,
        "page": page,
        "per_page": per_page,
        "total_filings": total_filings,
        "total_pages": total_pages,
        "total_all": total_all,
        "form_counts": form_counts,
        "base_qs": base_qs,
        "trust_count": len(trusts),
    })
