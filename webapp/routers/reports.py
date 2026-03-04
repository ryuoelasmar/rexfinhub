"""Bloomberg weekly report pages -- HIDDEN pending redesign.

All routes redirect to home page. Report data is still served via
email previews in the admin panel.
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/")
@router.get("/li")
@router.get("/cc")
@router.get("/ss")
def reports_redirect():
    """Reports pages hidden -- redirect to home."""
    return RedirectResponse("/", status_code=302)
