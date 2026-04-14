"""Admin reports preview page — serves pre-baked static HTML.

The heavy lifting (SQL queries, Bloomberg data loading, template rendering)
happens on the VPS via scripts/prebake_reports.py. Files are uploaded to
Render via POST /api/v1/reports/upload/{report_key} and stored at
data/prebaked_reports/{key}.html. This page just reads the static file.

Result: instant page load on Render, no per-view compute cost.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from webapp.dependencies import get_db

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/reports", tags=["admin-reports"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

ADMIN_PASSWORD = "ryu123"
PREBAKED_DIR = Path("data/prebaked_reports")


def _check_auth(request: Request) -> bool:
    return (
        request.cookies.get("admin_auth") == ADMIN_PASSWORD
        or request.session.get("is_admin") is True
    )


# Full report catalog — every report that send_email.py knows about
REPORT_CATALOG = {
    "daily_filing": {
        "name": "Daily Filing Report",
        "description": "Daily SEC filings digest with market snapshot",
        "cadence": "Daily",
        "list_type": "daily",
    },
    "weekly_report": {
        "name": "Weekly ETP Report",
        "description": "Weekly roll-up of filings, market activity, REX performance",
        "cadence": "Weekly",
        "list_type": "weekly",
    },
    "li_report": {
        "name": "Leverage & Inverse Report",
        "description": "L&I market landscape — Index and Single Stock segments",
        "cadence": "Weekly",
        "list_type": "li",
    },
    "income_report": {
        "name": "Income Report",
        "description": "Covered-call and income ETF landscape",
        "cadence": "Weekly",
        "list_type": "income",
    },
    "flow_report": {
        "name": "Flow Report",
        "description": "Fund flows by category and direction",
        "cadence": "Weekly",
        "list_type": "flow",
    },
    "autocall_report": {
        "name": "Autocallable Report",
        "description": "Autocallable ETF weekly update",
        "cadence": "Weekly",
        "list_type": "autocall",
    },
    "intelligence_brief": {
        "name": "Filing Intelligence Brief",
        "description": "Executive-first daily — action required, competitive races, effectives",
        "cadence": "Daily",
        "list_type": "intelligence",
    },
    "filing_screener": {
        "name": "Filing Candidates",
        "description": "Top 5 filing picks from foundation_scorer",
        "cadence": "Weekly",
        "list_type": "screener",
    },
    "product_status": {
        "name": "Product Pipeline",
        "description": "REX product lifecycle: Listed / Awaiting / Filed / Research",
        "cadence": "Monday",
        "list_type": "pipeline",
    },
}


def _load_metadata(report_key: str) -> dict:
    """Load the .meta.json sidecar for a pre-baked report."""
    meta_path = PREBAKED_DIR / f"{report_key}.meta.json"
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except Exception:
        return {}


def _report_status(report_key: str) -> dict:
    """Return status for a report: exists, baked_at, size, etc."""
    html_path = PREBAKED_DIR / f"{report_key}.html"
    if not html_path.exists():
        return {"exists": False, "baked_at": None, "size_bytes": 0}

    meta = _load_metadata(report_key)
    return {
        "exists": True,
        "baked_at": meta.get("baked_at"),
        "size_bytes": html_path.stat().st_size,
    }


@router.get("/preview", response_class=HTMLResponse)
def preview_landing(request: Request, db: Session = Depends(get_db)):
    """Admin landing page listing all pre-baked reports."""
    if not _check_auth(request):
        return RedirectResponse("/admin/", status_code=302)

    # Enrich each report with its current file status
    enriched = {}
    for key, meta in REPORT_CATALOG.items():
        enriched[key] = {**meta, **_report_status(key)}

    return templates.TemplateResponse("admin_reports_preview.html", {
        "request": request,
        "reports": enriched,
        "now": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "prebaked_dir": str(PREBAKED_DIR),
    })


@router.get("/preview/{report_key}/raw", response_class=HTMLResponse)
def preview_raw(report_key: str, request: Request):
    """Serve the pre-baked HTML for a report. Instant — no rendering."""
    if not _check_auth(request):
        return HTMLResponse("<h2>Unauthorized</h2>", status_code=401)

    if report_key not in REPORT_CATALOG:
        return HTMLResponse("<h2>Unknown report</h2>", status_code=404)

    html_path = PREBAKED_DIR / f"{report_key}.html"
    if not html_path.exists():
        return HTMLResponse(
            f"""
            <html><body style='font-family:sans-serif; padding:40px; background:#f8fafc;'>
            <div style='max-width:600px; margin:0 auto; background:white; padding:24px; border-radius:6px; border-left:3px solid #d97706;'>
            <h2 style='margin:0 0 8px; color:#0f172a;'>Not baked yet</h2>
            <p style='color:#374151;'>This report hasn't been baked yet. Run <code>python scripts/prebake_reports.py</code> on the VPS.</p>
            </div></body></html>
            """,
            status_code=404,
        )

    return HTMLResponse(html_path.read_text(encoding="utf-8"))
