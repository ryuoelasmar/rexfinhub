"""
FastAPI application factory for the ETP Filing Tracker web platform.

Run locally:
    uvicorn webapp.main:app --reload --port 8000
"""
from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from webapp.auth import SESSION_SECRET
from webapp.database import init_db

log = logging.getLogger(__name__)
WEBAPP_DIR = Path(__file__).resolve().parent


def _get_schedule_hour() -> int | None:
    """Read PIPELINE_SCHEDULE_HOUR from .env or environment."""
    env_file = WEBAPP_DIR.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("PIPELINE_SCHEDULE_HOUR="):
                val = line.split("=", 1)[1].strip()
                if val.isdigit():
                    return int(val)
    val = os.environ.get("PIPELINE_SCHEDULE_HOUR", "")
    return int(val) if val.isdigit() else None


def _scheduler_loop(schedule_hour: int):
    """Background thread: runs pipeline once daily at schedule_hour UTC."""
    last_run_date = None
    log.info("Scheduler started: pipeline will run daily at %02d:00 UTC", schedule_hour)

    while True:
        now = datetime.now(timezone.utc)
        today = now.date()

        if now.hour == schedule_hour and last_run_date != today:
            last_run_date = today
            log.info("Scheduler triggering daily pipeline run")
            try:
                from webapp.services.pipeline_service import run_pipeline_background, is_pipeline_running
                if not is_pipeline_running():
                    run_pipeline_background(triggered_by="scheduler")
                else:
                    log.info("Pipeline already running, skipping scheduled run")
            except Exception as e:
                log.error("Scheduler pipeline error: %s", e)

        time.sleep(300)  # Check every 5 minutes


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialize database + start scheduler. Shutdown: cleanup."""
    init_db()
    log.info("Database initialized.")

    # Start daily scheduler if configured
    schedule_hour = _get_schedule_hour()
    if schedule_hour is not None:
        t = threading.Thread(target=_scheduler_loop, args=(schedule_hour,), daemon=True)
        t.start()

    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="ETP Filing Tracker",
        version="2.0.0",
        lifespan=lifespan,
    )

    # Session middleware (required for Azure AD SSO)
    app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)

    # Static files (CSS, JS)
    static_dir = WEBAPP_DIR / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Routers
    from webapp.routers import auth_routes, dashboard, trusts, funds, filings, search, analysis, digest, api
    app.include_router(auth_routes.router)
    app.include_router(dashboard.router)
    app.include_router(trusts.router, prefix="/trusts")
    app.include_router(funds.router, prefix="/funds")
    app.include_router(filings.router, prefix="/filings")
    app.include_router(search.router)
    app.include_router(analysis.router)
    app.include_router(digest.router)
    app.include_router(api.router)

    # Health check
    @app.get("/health")
    def health():
        return {"status": "ok", "version": "2.0.0"}

    return app


app = create_app()
