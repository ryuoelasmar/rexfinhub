"""
FastAPI application factory for the ETP Filing Tracker web platform.

Run locally:
    uvicorn webapp.main:app --reload --port 8000
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from webapp.database import init_db

WEBAPP_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialize database. Shutdown: cleanup."""
    init_db()
    print("Database initialized.")
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="ETP Filing Tracker",
        version="2.0.0",
        lifespan=lifespan,
    )

    # Static files (CSS, JS)
    static_dir = WEBAPP_DIR / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Health check
    @app.get("/health")
    def health():
        return {"status": "ok", "version": "2.0.0"}

    return app


app = create_app()
