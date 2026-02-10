"""
FastAPI dependencies for the ETP Filing Tracker.
"""
from __future__ import annotations

from webapp.database import SessionLocal


def get_db():
    """Yields a DB session, auto-closes on completion."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
