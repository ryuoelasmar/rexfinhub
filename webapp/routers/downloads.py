"""
Downloads router - File downloads and CSV exports.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from webapp.dependencies import get_db
from webapp.models import Trust, FundStatus

router = APIRouter(prefix="/downloads")
templates = Jinja2Templates(directory="webapp/templates")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"


def _safe_path(requested: str) -> Path:
    """Resolve a requested path and ensure it's within OUTPUTS_DIR."""
    resolved = (OUTPUTS_DIR / requested).resolve()
    if not str(resolved).startswith(str(OUTPUTS_DIR.resolve())):
        raise HTTPException(status_code=403, detail="Access denied")
    if not resolved.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return resolved


@router.get("/")
def downloads_page(request: Request):
    """List available downloads."""
    summary_files = []
    trust_files = []
    digest_files = []

    if OUTPUTS_DIR.exists():
        # Summary Excel files
        for f in sorted(OUTPUTS_DIR.glob("*.xlsx")):
            summary_files.append({
                "name": f.name,
                "path": f.name,
                "size": f"{f.stat().st_size / 1024:.0f} KB",
            })

        # Daily digest
        digest_path = OUTPUTS_DIR / "daily_digest.html"
        if digest_path.exists():
            digest_files.append({
                "name": "daily_digest.html",
                "path": "daily_digest.html",
                "size": f"{digest_path.stat().st_size / 1024:.0f} KB",
            })

        # Per-trust CSV files
        for folder in sorted(OUTPUTS_DIR.iterdir()):
            if not folder.is_dir():
                continue
            csvs = []
            for csv_file in sorted(folder.glob("*.csv")):
                csvs.append({
                    "name": csv_file.name,
                    "path": f"{folder.name}/{csv_file.name}",
                    "size": f"{csv_file.stat().st_size / 1024:.0f} KB",
                })
            if csvs:
                trust_files.append({
                    "trust_name": folder.name,
                    "files": csvs,
                })

    return templates.TemplateResponse("downloads.html", {
        "request": request,
        "summary_files": summary_files,
        "trust_files": trust_files,
        "digest_files": digest_files,
    })


@router.get("/file")
def download_file(path: str):
    """Serve a file from the outputs directory."""
    resolved = _safe_path(path)
    return FileResponse(
        resolved,
        filename=resolved.name,
        media_type="application/octet-stream",
    )


@router.get("/export/funds")
def export_funds_csv(db: Session = Depends(get_db)):
    """Live CSV export of all fund statuses."""
    results = db.execute(
        select(FundStatus, Trust.name.label("trust_name"))
        .join(Trust, Trust.id == FundStatus.trust_id)
        .order_by(Trust.name, FundStatus.fund_name)
    ).all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Trust", "Fund Name", "Ticker", "Series ID",
        "Status", "Effective Date", "Latest Form",
        "Latest Filing Date", "Status Reason",
    ])
    for row in results:
        f = row.FundStatus
        writer.writerow([
            row.trust_name,
            f.fund_name,
            f.ticker or "",
            f.series_id or "",
            f.status or "",
            f.effective_date or "",
            f.latest_form or "",
            f.latest_filing_date or "",
            f.status_reason or "",
        ])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=funds_export.csv"},
    )
