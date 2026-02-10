"""
Filings router - Filing list page.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db
from webapp.models import Trust, Filing

router = APIRouter()
templates = Jinja2Templates(directory="webapp/templates")


@router.get("/")
def filing_list(
    request: Request,
    form: str = "",
    trust_id: int = 0,
    db: Session = Depends(get_db),
):
    query = (
        select(Filing, Trust.name.label("trust_name"), Trust.slug.label("trust_slug"))
        .join(Trust, Trust.id == Filing.trust_id)
    )

    if form:
        query = query.where(Filing.form.ilike(f"%{form}%"))
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
        "total": len(results),
    })
