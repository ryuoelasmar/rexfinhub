"""Product Pipeline admin routes.

Dedicated admin section for managing the REX product pipeline (rex_products table).
Separate from admin.py to avoid file contention.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/products", tags=["admin-products"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

ADMIN_PASSWORD = "ryu123"
VALID_STATUSES = ["Research", "Target List", "Filed", "Awaiting Effective", "Listed", "Delisted"]
VALID_SUITES = ["T-REX", "Premium Income", "Growth & Income", "IncomeMax", "Crypto", "Thematic", "Autocallable", "T-Bill"]


def _check_auth(request: Request) -> bool:
    return request.cookies.get("admin_auth") == ADMIN_PASSWORD


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def products_page(
    request: Request,
    status: str | None = None,
    suite: str | None = None,
    q: str | None = None,
    db: Session = Depends(get_db),
):
    """Product pipeline management page."""
    if not _check_auth(request):
        return RedirectResponse(url="/admin/", status_code=302)

    from webapp.models import RexProduct

    # Build query
    query = db.query(RexProduct)
    if status:
        query = query.filter(RexProduct.status == status)
    if suite:
        query = query.filter(RexProduct.product_suite == suite)
    if q:
        like = f"%{q}%"
        query = query.filter(
            (RexProduct.name.ilike(like))
            | (RexProduct.ticker.ilike(like))
            | (RexProduct.underlier.ilike(like))
        )

    products = query.order_by(RexProduct.product_suite, RexProduct.status, RexProduct.name).all()

    # Summary stats
    total = db.query(RexProduct).count()
    status_counts = dict(db.query(RexProduct.status, func.count(RexProduct.id)).group_by(RexProduct.status).all())
    suite_counts = dict(db.query(RexProduct.product_suite, func.count(RexProduct.id)).group_by(RexProduct.product_suite).all())

    msg = request.query_params.get("msg", "")

    return templates.TemplateResponse("admin_products.html", {
        "request": request,
        "products": products,
        "total": total,
        "filtered_count": len(products),
        "status_counts": status_counts,
        "suite_counts": suite_counts,
        "valid_statuses": VALID_STATUSES,
        "valid_suites": VALID_SUITES,
        "filter_status": status or "",
        "filter_suite": suite or "",
        "filter_q": q or "",
        "msg": msg,
    })


@router.post("/update/{product_id}")
def update_product(
    product_id: int,
    request: Request,
    name: str = Form(...),
    status: str = Form(...),
    product_suite: str = Form(...),
    ticker: str = Form(""),
    underlier: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    """Update a product record."""
    if not _check_auth(request):
        return RedirectResponse(url="/admin/", status_code=302)

    from webapp.models import RexProduct

    if status not in VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Valid: {VALID_STATUSES}")
    if product_suite not in VALID_SUITES:
        raise HTTPException(400, f"Invalid suite. Valid: {VALID_SUITES}")

    p = db.query(RexProduct).filter(RexProduct.id == product_id).first()
    if not p:
        raise HTTPException(404, "Product not found")

    p.name = name
    p.status = status
    p.product_suite = product_suite
    p.ticker = ticker or None
    p.underlier = underlier or None
    p.notes = notes or None
    p.updated_at = datetime.utcnow()
    db.commit()

    return RedirectResponse(url="/admin/products/?msg=updated", status_code=302)


@router.post("/add")
def add_product(
    request: Request,
    name: str = Form(...),
    product_suite: str = Form(...),
    status: str = Form("Research"),
    ticker: str = Form(""),
    underlier: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    """Add a new product."""
    if not _check_auth(request):
        return RedirectResponse(url="/admin/", status_code=302)

    from webapp.models import RexProduct

    if status not in VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Valid: {VALID_STATUSES}")
    if product_suite not in VALID_SUITES:
        raise HTTPException(400, f"Invalid suite. Valid: {VALID_SUITES}")

    p = RexProduct(
        name=name,
        product_suite=product_suite,
        status=status,
        ticker=ticker or None,
        underlier=underlier or None,
        notes=notes or None,
    )
    db.add(p)
    db.commit()

    return RedirectResponse(url="/admin/products/?msg=added", status_code=302)


@router.post("/sync-from-sec")
def sync_from_sec(request: Request, db: Session = Depends(get_db)):
    """Sync product status from SEC filings (match by series_id)."""
    if not _check_auth(request):
        return RedirectResponse(url="/admin/", status_code=302)

    from webapp.models import RexProduct, FundStatus

    updated = 0
    products = db.query(RexProduct).filter(RexProduct.series_id.isnot(None)).all()
    for p in products:
        fs = db.query(FundStatus).filter(FundStatus.series_id == p.series_id).first()
        if not fs:
            continue
        # Map SEC FundStatus to RexProduct status
        new_status = None
        if fs.status == "EFFECTIVE":
            new_status = "Listed" if p.official_listed_date else "Awaiting Effective"
        elif fs.status == "PENDING":
            new_status = "Filed"
        elif fs.status == "DELAYED":
            new_status = "Filed"

        changed = False
        if new_status and new_status != p.status:
            p.status = new_status
            changed = True
        if fs.latest_form and fs.latest_form != p.latest_form:
            p.latest_form = fs.latest_form
            changed = True
        if fs.prospectus_link and fs.prospectus_link != p.latest_prospectus_link:
            p.latest_prospectus_link = fs.prospectus_link
            changed = True
        if fs.effective_date and fs.effective_date != p.estimated_effective_date:
            p.estimated_effective_date = fs.effective_date
            changed = True

        if changed:
            p.updated_at = datetime.utcnow()
            updated += 1

    db.commit()
    return RedirectResponse(url=f"/admin/products/?msg=synced_{updated}", status_code=302)
