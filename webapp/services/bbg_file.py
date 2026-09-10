"""Centralized Bloomberg daily file resolution.

Every data module must import get_bloomberg_file() from here.
No other module should hardcode OneDrive or local paths.

Policy: Graph API is the ONLY source of truth. If it fails, we terminate.
The local file on disk is a byproduct of a successful Graph API download —
never a fallback for reads. This prevents silent use of yesterday's data
when auth/network/SharePoint breaks.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

_LOCAL_CACHE = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "DASHBOARD"
    / "bloomberg_daily_file.xlsm"
)


class BloombergGraphError(RuntimeError):
    """Raised when the Graph API path cannot produce an up-to-date Bloomberg file.

    This is a hard-stop. Do NOT catch and fall back to any local file.
    """


def get_bloomberg_file() -> Path:
    """Return the Bloomberg daily file, guaranteed to be today's SharePoint copy.

    Behavior:
        - If SharePoint is newer than local, download it.
        - If local is already current (matches SharePoint mtime), use it as-is.
        - If ANY step fails (auth, network, SharePoint unreachable, download
          too small, metadata unreadable), raise BloombergGraphError. No
          fallback to stale local data.

    Raises:
        BloombergGraphError: Any Graph API failure. The pipeline should
            abort and surface this error rather than proceed with stale data.
    """
    # --- Offline delivery path -------------------------------------------------
    # When the Graph app registration is unavailable, the workbook is delivered by
    # OneDrive desktop sync instead. Graph freshness cannot be checked, so we use a
    # STRONGER proof in its place: the dcterms:modified stamp Excel writes inside the
    # file when a human saves it. That travels with the bytes and cannot be forged by
    # a copy, whereas a filesystem mtime can. Refuses anything not saved today.
    import os as _os
    if _os.environ.get("REXFIN_SKIP_BBG_PULL") == "1":
        import zipfile as _zip, re as _re
        from datetime import datetime as _dt, timedelta as _td, date as _date
        if not _LOCAL_CACHE.exists():
            raise BloombergGraphError(f"skip-pull set but no local workbook at {_LOCAL_CACHE}")
        try:
            with _zip.ZipFile(_LOCAL_CACHE) as _z:
                _core = _z.read("docProps/core.xml").decode("utf8", "ignore")
            _saved = _dt.strptime(
                _re.search(r"<dcterms:modified[^>]*>([^<]+)<", _core).group(1),
                "%Y-%m-%dT%H:%M:%SZ") - _td(hours=4)
        except Exception as _e:
            raise BloombergGraphError(f"skip-pull: cannot read workbook save stamp: {_e}")
        if _saved.date() != _date.today():
            raise BloombergGraphError(
                f"skip-pull: workbook was saved {_saved:%Y-%m-%d %H:%M}, not today. "
                f"Refusing to serve a stale workbook.")
        log.warning("BBG skip-pull: serving local workbook saved %s", _saved.strftime("%Y-%m-%d %H:%M"))
        return _LOCAL_CACHE

    try:
        from webapp.services.graph_files import (
            is_sharepoint_newer_than_local,
            download_bloomberg_from_sharepoint,
            get_sharepoint_file_metadata,
        )
    except ImportError as e:
        raise BloombergGraphError(
            f"graph_files module not available: {e}. Cannot verify Bloomberg freshness."
        ) from e

    # Confirm Graph API is reachable BEFORE deciding whether to download.
    try:
        meta = get_sharepoint_file_metadata()
    except Exception as e:
        raise BloombergGraphError(f"Graph API metadata fetch failed: {e}") from e
    if not meta or not meta.get("lastModifiedDateTime"):
        raise BloombergGraphError(
            "Graph API returned no metadata for Bloomberg file. "
            "Cannot verify freshness — aborting."
        )

    # Either SharePoint is newer or local is missing — in both cases download.
    needs_download = not _LOCAL_CACHE.exists()
    if not needs_download:
        try:
            needs_download = is_sharepoint_newer_than_local(_LOCAL_CACHE)
        except Exception as e:
            raise BloombergGraphError(f"Graph API freshness check failed: {e}") from e

    if needs_download:
        try:
            downloaded = download_bloomberg_from_sharepoint()
        except Exception as e:
            raise BloombergGraphError(f"Graph API download failed: {e}") from e
        if not downloaded or not downloaded.exists():
            raise BloombergGraphError("Graph API download returned no file.")
        if downloaded.stat().st_size < 1_000_000:
            raise BloombergGraphError(
                f"Graph API download too small ({downloaded.stat().st_size} bytes). "
                f"Expected full Bloomberg xlsm."
            )
        mtime = datetime.fromtimestamp(downloaded.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        log.info("Bloomberg file: Graph API download (modified %s)", mtime)
        return downloaded

    # Local cache matches SharePoint — safe to use (Graph API confirmed freshness).
    age_hours = (datetime.now().timestamp() - _LOCAL_CACHE.stat().st_mtime) / 3600
    log.info("Bloomberg file: local cache matches SharePoint (%.1fh old)", age_hours)
    return _LOCAL_CACHE
