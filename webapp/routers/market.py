"""
Market Intelligence router.

Routes:
  GET /market/            -> redirect to /market/rex
  GET /market/rex         -> REX View (suite-by-suite performance)
  GET /market/category    -> Category View (competitive landscape)
  GET /market/api/rex-summary       -> JSON for REX View charts
  GET /market/api/category-summary  -> JSON for Category View (with filters)
  GET /market/api/time-series       -> JSON for line charts
  GET /market/api/slicers/{cat}     -> JSON slicer options for a category
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

log = logging.getLogger(__name__)
router = APIRouter(prefix="/market", tags=["market"])
templates = Jinja2Templates(directory="webapp/templates")


def _svc():
    from webapp.services import market_data
    return market_data


#  Pages 

@router.get("/")
def market_index():
    return RedirectResponse("/market/rex", status_code=302)


@router.get("/rex")
def rex_view(request: Request):
    """REX View - executive dashboard by suite."""
    svc = _svc()
    if not svc.data_available():
        return templates.TemplateResponse("market/rex.html", {
            "request": request,
            "data_available": False,
        })
    try:
        summary = svc.get_rex_summary()
        ts_rex = svc.get_time_series(is_rex=True)
        return templates.TemplateResponse("market/rex.html", {
            "request": request,
            "data_available": True,
            "summary": summary,
            "ts_labels": ts_rex["labels"],
            "ts_values": ts_rex["values"],
        })
    except Exception as e:
        log.error("REX view error: %s", e, exc_info=True)
        return templates.TemplateResponse("market/rex.html", {
            "request": request,
            "data_available": False,
            "error": str(e),
        })


@router.get("/category")
def category_view(
    request: Request,
    cat: str = Query(default="All"),
    filters: str = Query(default=None),
):
    """Category View - competitive landscape with dynamic filters."""
    svc = _svc()
    if not svc.data_available():
        return templates.TemplateResponse("market/category.html", {
            "request": request,
            "data_available": False,
            "categories": svc.ALL_CATEGORIES,
            "selected_cat": cat,
        })
    try:
        filter_dict = json.loads(filters) if filters else {}
        summary = svc.get_category_summary(cat if cat != "All" else None, filter_dict)
        slicers = svc.get_slicer_options(cat) if cat and cat != "All" else []
        ts_cat = svc.get_time_series(category=cat if cat != "All" else None)
        ts_rex = svc.get_time_series(category=cat if cat != "All" else None, is_rex=True)
        return templates.TemplateResponse("market/category.html", {
            "request": request,
            "data_available": True,
            "categories": svc.ALL_CATEGORIES,
            "selected_cat": cat,
            "summary": summary,
            "slicers": slicers,
            "active_filters": filter_dict,
            "ts_cat_labels": ts_cat["labels"],
            "ts_cat_values": ts_cat["values"],
            "ts_rex_labels": ts_rex["labels"],
            "ts_rex_values": ts_rex["values"],
        })
    except Exception as e:
        log.error("Category view error: %s", e, exc_info=True)
        return templates.TemplateResponse("market/category.html", {
            "request": request,
            "data_available": False,
            "categories": svc.ALL_CATEGORIES,
            "selected_cat": cat,
            "error": str(e),
        })


#  API endpoints (AJAX) 

@router.get("/api/rex-summary")
def api_rex_summary():
    try:
        svc = _svc()
        return JSONResponse(svc.get_rex_summary())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/category-summary")
def api_category_summary(
    category: str = Query(default="All"),
    filters: str = Query(default=None),
):
    try:
        svc = _svc()
        filter_dict = json.loads(filters) if filters else {}
        cat = category if category != "All" else None
        data = svc.get_category_summary(cat, filter_dict)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/time-series")
def api_time_series(
    category: str = Query(default="All"),
    is_rex: str = Query(default="both"),
):
    try:
        svc = _svc()
        cat = category if category != "All" else None
        if is_rex == "true":
            data = svc.get_time_series(category=cat, is_rex=True)
        elif is_rex == "false":
            data = svc.get_time_series(category=cat, is_rex=False)
        else:
            # Return both
            all_ts = svc.get_time_series(category=cat)
            rex_ts = svc.get_time_series(category=cat, is_rex=True)
            data = {
                "labels": all_ts["labels"],
                "values_all": all_ts["values"],
                "values_rex": rex_ts["values"],
            }
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/slicers/{category:path}")
def api_slicers(category: str):
    try:
        svc = _svc()
        return JSONResponse(svc.get_slicer_options(category))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/invalidate-cache")
def api_invalidate_cache():
    """Clear the market data cache (admin utility)."""
    try:
        _svc().invalidate_cache()
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
