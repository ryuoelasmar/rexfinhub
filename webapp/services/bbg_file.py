"""Centralized Bloomberg daily file resolution.

Every data module must import get_bloomberg_file() from here.
No other module should hardcode OneDrive or local paths.

Resolution:
  1. Graph API (SharePoint) — pull fresh if newer than local cache
  2. Local cache (data/DASHBOARD/) — last successful Graph download
  No OneDrive dependency. No silent fallback to stale data.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

_LOCAL_CACHE = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "DASHBOARD"
    / "bloomberg_daily_file.xlsm"
)

_STALENESS_HOURS = 24  # Error if local cache older than this and Graph fails


def _file_age_hours(path: Path) -> float:
    """Return file age in hours, or 999 if file doesn't exist."""
    if not path.exists():
        return 999
    return (time.time() - path.stat().st_mtime) / 3600


def get_bloomberg_file() -> Path:
    """Return the Bloomberg daily file path.

    Resolution:
        1. Check SharePoint via Graph API. If newer than local cache, download.
        2. Use local cache (from last successful Graph download).
        3. If local cache is >24h old and Graph failed, log ERROR.

    Raises FileNotFoundError if no file available.
    """
    # --- Try Graph API: download if newer than local cache ---
    try:
        from webapp.services.graph_files import (
            is_sharepoint_newer_than_local,
            download_bloomberg_from_sharepoint,
        )

        if not _LOCAL_CACHE.exists() or is_sharepoint_newer_than_local(_LOCAL_CACHE):
            downloaded = download_bloomberg_from_sharepoint()
            if downloaded and downloaded.exists() and downloaded.stat().st_size > 1_000_000:
                mtime = datetime.fromtimestamp(downloaded.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                log.info("Bloomberg file: Graph API (modified %s)", mtime)
                return downloaded
            else:
                log.warning("Bloomberg file: Graph API download failed")
        else:
            age = _file_age_hours(_LOCAL_CACHE)
            log.info("Bloomberg file: local cache is current (%.1fh old)", age)
    except ImportError:
        log.warning("Bloomberg file: graph_files module not available")
    except Exception as e:
        log.warning("Bloomberg file: Graph API error: %s", e)

    # --- Use local cache (last successful download) ---
    if _LOCAL_CACHE.exists():
        age = _file_age_hours(_LOCAL_CACHE)
        if age > _STALENESS_HOURS:
            log.error(
                "Bloomberg file: local cache is %.0fh old and Graph API failed. "
                "Data is STALE. Check SharePoint connectivity.", age
            )
        else:
            log.info("Bloomberg file: using local cache (%.1fh old)", age)
        return _LOCAL_CACHE

    raise FileNotFoundError(
        f"Bloomberg file not available. Graph API failed and no local cache at {_LOCAL_CACHE}"
    )
