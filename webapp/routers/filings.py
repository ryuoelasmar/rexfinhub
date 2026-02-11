"""
Filings router - Filing list page.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db
from webapp.models import Trust, Filing, FundExtraction

router = APIRouter()
templates = Jinja2Templates(directory="webapp/templates")

PROSPECTUS_FORMS = ["485APOS", "485BPOS", "485BXT", "497", "497K"]


@router.get("/")
def filing_list(
    request: Request,
    form: str = "",
    trust_id: int = 0,
    show_all: bool = False,
    db: Session = Depends(get_db),
):
    query = (
        select(
            Filing,
            Trust.name.label("trust_name"),
            Trust.slug.label("trust_slug"),
            func.group_concat(FundExtraction.series_name.distinct()).label("fund_names"),
        )
        .join(Trust, Trust.id == Filing.trust_id)
        .outerjoin(FundExtraction, FundExtraction.filing_id == Filing.id)
        .group_by(Filing.id)
    )

    if form:
        query = query.where(Filing.form.ilike(f"%{form}%"))
    elif not show_all:
        query = query.where(Filing.form.in_(PROSPECTUS_FORMS))

    if trust_id:
        query = query.where(Filing.trust_id == trust_id)

    query = query.order_by(Filing.filing_date.desc()).limit(200)
    results = db.execute(query).all()

    trusts = db.execute(
        select(Trust).where(Trust.is_active == True).order_by(Trust.name)
    ).scalars().all()

    return templates.TemplateResponse("filing_list.html", {
        "request": request,
        "filings": results,
        "trusts": trusts,
        "form": form,
        "trust_id": trust_id,
        "show_all": show_all,
        "total": len(results),
    })
