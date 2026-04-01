"""Centralized Bloomberg daily file resolution.

Every data module must import get_bloomberg_file() from here.
No other module should hardcode OneDrive or local fallback paths.
"""
from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

_ONEDRIVE_PATH = Path(
    r"C:\Users\RyuEl-Asmar\REX Financial LLC"
    r"\REX Financial LLC - MasterFiles"
    r"\MASTER Data\bloomberg_daily_file.xlsm"
)
_LOCAL_FALLBACK = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "DASHBOARD"
    / "bloomberg_daily_file.xlsm"
)


def get_bloomberg_file() -> Path:
    """Return the ONE Bloomberg daily file path.

    Resolution order:
        1. OneDrive MASTER Data folder (primary, synced by Ryu)
        2. Local data/DASHBOARD/ fallback (for Render or offline work)

    Logs which source was chosen and its modification time.
    Raises FileNotFoundError if neither location has the file.
    """
    if _ONEDRIVE_PATH.exists():
        try:
            with open(_ONEDRIVE_PATH, "rb") as f:
                f.read(4)
            from datetime import datetime

            mtime = datetime.fromtimestamp(
                _ONEDRIVE_PATH.stat().st_mtime
            ).strftime("%Y-%m-%d %H:%M")
            log.info("Bloomberg file: OneDrive (modified %s)", mtime)
            return _ONEDRIVE_PATH
        except PermissionError:
            log.warning("Bloomberg file: OneDrive exists but not readable")

    if _LOCAL_FALLBACK.exists():
        try:
            with open(_LOCAL_FALLBACK, "rb") as f:
                f.read(4)
            import time

            age_hours = (time.time() - _LOCAL_FALLBACK.stat().st_mtime) / 3600
            log.warning(
                "Bloomberg file: LOCAL FALLBACK (%.1fh old) - OneDrive not available",
                age_hours,
            )
            return _LOCAL_FALLBACK
        except PermissionError:
            log.warning("Bloomberg file: local fallback exists but not readable")

    raise FileNotFoundError(
        f"Bloomberg file not found at {_ONEDRIVE_PATH} or {_LOCAL_FALLBACK}"
    )
