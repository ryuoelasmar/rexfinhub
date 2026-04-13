"""Public-facing REX Product Pipeline Calendar.

Calendar view of the product pipeline — click a month to see what's launching.
Data source: rex_products table (estimated_effective_date).
"""
from __future__ import annotations

import calendar as cal_mod
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from webapp.dependencies import get_db

router = APIRouter(prefix="/pipeline", tags=["pipeline"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


SUITE_COLORS = {
    "T-REX": "#0f172a",
    "Premium Income": "#2563eb",
    "Growth & Income": "#059669",
    "IncomeMax": "#d97706",
    "Crypto": "#8b5cf6",
    "Thematic": "#0891b2",
    "Autocallable": "#dc2626",
    "T-Bill": "#64748b",
    "MicroSectors ETN": "#0f766e",
}


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def pipeline_home(request: Request, db: Session = Depends(get_db)):
    """Pipeline calendar home — current month."""
    today = date.today()
    return _render_month(request, db, today.year, today.month)


@router.get("/{year}/{month}", response_class=HTMLResponse)
def pipeline_month(year: int, month: int, request: Request, db: Session = Depends(get_db)):
    """Pipeline calendar for a specific month."""
    if not (1 <= month <= 12) or not (2020 <= year <= 2030):
        return _render_month(request, db, date.today().year, date.today().month)
    return _render_month(request, db, year, month)


def _render_month(request: Request, db: Session, year: int, month: int) -> HTMLResponse:
    from webapp.models import RexProduct

    # Top-level KPIs
    total = db.query(RexProduct).count()
    listed = db.query(RexProduct).filter(RexProduct.status == "Listed").count()
    filed = db.query(RexProduct).filter(RexProduct.status.in_(["Filed", "Awaiting Effective"])).count()

    # All products with estimated effective date in this month
    first_day = date(year, month, 1)
    last_day_num = cal_mod.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)

    products_this_month = (
        db.query(RexProduct)
        .filter(RexProduct.estimated_effective_date.isnot(None))
        .filter(RexProduct.estimated_effective_date >= first_day)
        .filter(RexProduct.estimated_effective_date <= last_day)
        .order_by(RexProduct.estimated_effective_date, RexProduct.product_suite)
        .all()
    )

    # Group by day
    by_day = defaultdict(list)
    for p in products_this_month:
        by_day[p.estimated_effective_date].append(p)

    # Group by filing within day (trust + suite + date)
    by_day_grouped = {}
    for day, products in by_day.items():
        groups = defaultdict(lambda: {"trust": "", "suite": "", "funds": [], "funds_count": 0, "colors": set()})
        for p in products:
            key = (p.trust or "", p.product_suite or "")
            g = groups[key]
            g["trust"] = p.trust or ""
            g["suite"] = p.product_suite or ""
            g["funds"].append({"name": p.name, "ticker": p.ticker or ""})
            g["colors"].add(SUITE_COLORS.get(p.product_suite or "", "#64748b"))
        for g in groups.values():
            g["funds_count"] = len(g["funds"])
            g["colors"] = list(g["colors"])
        by_day_grouped[day] = sorted(groups.values(), key=lambda g: -g["funds_count"])

    # Build calendar grid (list of weeks, each week = list of days)
    cal = cal_mod.Calendar(firstweekday=6)  # Sunday start
    weeks_raw = cal.monthdatescalendar(year, month)
    weeks = []
    for week in weeks_raw:
        days = []
        for d in week:
            in_month = d.month == month
            filings = by_day_grouped.get(d, [])
            total_funds = sum(g["funds_count"] for g in filings)
            days.append({
                "date": d,
                "in_month": in_month,
                "day": d.day,
                "filings": filings,
                "fund_count": total_funds,
                "filing_count": len(filings),
                "is_today": d == date.today(),
            })
        weeks.append(days)

    # Previous / next month
    prev_month = (month - 1) if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = (month + 1) if month < 12 else 1
    next_year = year if month < 12 else year + 1

    month_name = cal_mod.month_name[month]

    return templates.TemplateResponse("pipeline_calendar.html", {
        "request": request,
        "year": year,
        "month": month,
        "month_name": month_name,
        "weeks": weeks,
        "prev_year": prev_year,
        "prev_month": prev_month,
        "next_year": next_year,
        "next_month": next_month,
        "total": total,
        "listed": listed,
        "filed": filed,
        "this_month_count": len(products_this_month),
        "this_month_filings": sum(len(by_day_grouped.get(d, [])) for d in by_day_grouped),
        "suite_colors": SUITE_COLORS,
        "today": date.today(),
    })
