"""Download files from SharePoint via Microsoft Graph API.

Reuses the same Azure AD credentials as graph_email.py.
Primary use: pull bloomberg_daily_file.xlsm from SharePoint
to bypass unreliable OneDrive desktop sync.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# SharePoint site and file path
# Local OneDrive maps to: C:\Users\RyuEl-Asmar\REX Financial LLC\REX Financial LLC - Rex Financial LLC\
#   Product Development\MasterFiles\MASTER Data\bloomberg_daily_file.xlsm
# SharePoint: https://rexfin.sharepoint.com
_SHAREPOINT_HOSTNAME = "rexfin.sharepoint.com"
_SHAREPOINT_SITE_PATH = ""  # Will be resolved dynamically
_FILE_PATH = "/Product Development/MasterFiles/MASTER Data/bloomberg_daily_file.xlsm"

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Local download destination (same as bbg_file.py fallback)
_LOCAL_DEST = Path(__file__).resolve().parent.parent.parent / "data" / "DASHBOARD" / "bloomberg_daily_file.xlsm"

# Cache resolved IDs to avoid re-discovering on every call (1-hour TTL)
_cached_site_id: str | None = None
_cached_drive_id: str | None = None
_cache_time: float = 0
_CACHE_TTL = 3600  # 1 hour


def _get_auth() -> tuple[dict, str | None]:
    """Load Azure credentials and acquire access token.

    Returns (config_dict, token_string). Token is None on failure.
    """
    from webapp.services.graph_email import _load_env, _get_access_token

    cfg = _load_env()
    if not all([cfg["tenant_id"], cfg["client_id"], cfg["client_secret"]]):
        log.warning("Graph Files: Azure credentials not configured")
        return cfg, None

    token = _get_access_token(cfg["tenant_id"], cfg["client_id"], cfg["client_secret"])
    if not token:
        log.warning("Graph Files: failed to acquire access token")
    return cfg, token


def _get_site_and_drive(token: str) -> tuple[str | None, str | None]:
    """Resolve SharePoint site ID and drive ID for the Bloomberg file.

    Tries multiple approaches since SharePoint path structure varies:
    1. Search for the site by name
    2. List drives to find the document library containing MasterFiles

    Returns (site_id, drive_id) or (None, None) on failure.
    """
    global _cached_site_id, _cached_drive_id, _cache_time
    if _cached_site_id and _cached_drive_id and (time.time() - _cache_time) < _CACHE_TTL:
        log.info("Graph Files: using cached site/drive IDs")
        return _cached_site_id, _cached_drive_id

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Search for the REX Financial site
    search_url = f"{_GRAPH_BASE}/sites?search=REX Financial"
    try:
        resp = requests.get(search_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            log.error("Graph Files: site search failed [%d]: %s", resp.status_code, resp.text[:200])
            return None, None

        sites = resp.json().get("value", [])
        if not sites:
            log.error("Graph Files: no sites found matching 'REX Financial'")
            return None, None

        # Pick the first match
        site_id = sites[0].get("id")
        site_name = sites[0].get("displayName", "?")
        log.info("Graph Files: found site '%s' (id: %s)", site_name, site_id)

    except Exception as e:
        log.error("Graph Files: site search error: %s", e)
        return None, None

    # Step 2: List drives on the site to find the one with MasterFiles
    drives_url = f"{_GRAPH_BASE}/sites/{site_id}/drives"
    try:
        resp = requests.get(drives_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            log.error("Graph Files: drives list failed [%d]: %s", resp.status_code, resp.text[:200])
            return site_id, None

        drives = resp.json().get("value", [])
        for drive in drives:
            drive_name = drive.get("name", "")
            drive_id = drive.get("id")
            log.info("Graph Files: found drive '%s' (id: %s)", drive_name, drive_id)
            # Try to find the file in each drive
            file_url = f"{_GRAPH_BASE}/drives/{drive_id}/root:{_FILE_PATH}"
            file_resp = requests.get(file_url, headers=headers, timeout=15)
            if file_resp.status_code == 200:
                log.info("Graph Files: Bloomberg file found in drive '%s'", drive_name)
                _cached_site_id = site_id
                _cached_drive_id = drive_id
                _cache_time = time.time()
                return site_id, drive_id

        log.error("Graph Files: Bloomberg file not found in any drive (tried %d drives)", len(drives))
        return site_id, None

    except Exception as e:
        log.error("Graph Files: drive search error: %s", e)
        return site_id, None


def get_sharepoint_file_metadata(token: str = None) -> dict | None:
    """Get Bloomberg file metadata from SharePoint without downloading.

    Returns dict with 'lastModifiedDateTime', 'size', 'name', 'drive_id' or None on failure.
    """
    if token is None:
        _, token = _get_auth()
    if not token:
        return None

    site_id, drive_id = _get_site_and_drive(token)
    if not drive_id:
        return None

    url = f"{_GRAPH_BASE}/drives/{drive_id}/root:{_FILE_PATH}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            meta = {
                "name": data.get("name"),
                "size": data.get("size"),
                "lastModifiedDateTime": data.get("lastModifiedDateTime"),
                "webUrl": data.get("webUrl"),
                "drive_id": drive_id,
            }
            log.info("Graph Files: metadata OK — %s, %s bytes, modified %s",
                     meta["name"], meta["size"], meta["lastModifiedDateTime"])
            return meta
        else:
            log.error("Graph Files: metadata failed [%d]: %s", resp.status_code, resp.text[:200])
            return None
    except Exception as e:
        log.error("Graph Files: metadata error: %s", e)
        return None


def download_bloomberg_from_sharepoint(dest: Path = None) -> Path | None:
    """Download bloomberg_daily_file.xlsm from SharePoint to local disk.

    Args:
        dest: Local destination path. Defaults to data/DASHBOARD/bloomberg_daily_file.xlsm

    Returns:
        Path to downloaded file on success, None on failure.
    """
    if dest is None:
        dest = _LOCAL_DEST

    _, token = _get_auth()
    if not token:
        return None

    site_id, drive_id = _get_site_and_drive(token)
    if not drive_id:
        return None

    # Download file content
    url = f"{_GRAPH_BASE}/drives/{drive_id}/root:{_FILE_PATH}:/content"
    headers = {"Authorization": f"Bearer {token}"}

    log.info("Graph Files: downloading %s ...", _FILE_PATH)
    start = time.time()
    try:
        resp = requests.get(url, headers=headers, timeout=120, stream=True)
        if resp.status_code == 200:
            dest.parent.mkdir(parents=True, exist_ok=True)
            expected_size = int(resp.headers.get("content-length", 0))
            # Write to temp file first (atomic)
            tmp_path = dest.with_suffix(".tmp")
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            downloaded_size = tmp_path.stat().st_size
            # Validate size
            if downloaded_size < 1_000_000:  # Bloomberg file should be >1MB
                log.error("Graph Files: downloaded file too small (%.0f bytes), discarding", downloaded_size)
                tmp_path.unlink(missing_ok=True)
                return None
            if expected_size > 0 and abs(downloaded_size - expected_size) > 1024:
                log.error("Graph Files: size mismatch (expected %d, got %d), discarding",
                          expected_size, downloaded_size)
                tmp_path.unlink(missing_ok=True)
                return None
            # Atomic rename
            tmp_path.replace(dest)
            elapsed = time.time() - start
            size_mb = dest.stat().st_size / (1024 * 1024)
            log.info("Graph Files: downloaded %.1f MB in %.1fs -> %s", size_mb, elapsed, dest)

            # Archive daily snapshot (keep 30 days locally)
            _archive_snapshot(dest)

            return dest
        else:
            log.error("Graph Files: download failed [%d]: %s", resp.status_code, resp.text[:200])
            return None
    except Exception as e:
        log.error("Graph Files: download error: %s", e)
        return None


def is_sharepoint_newer_than_local(local_path: Path) -> bool:
    """Check if the SharePoint file is newer than the local copy.

    Returns True if SharePoint is newer or local doesn't exist.
    Returns False if local is up-to-date or SharePoint is unreachable.
    """
    if not local_path.exists():
        return True

    meta = get_sharepoint_file_metadata()
    if not meta or not meta.get("lastModifiedDateTime"):
        return False  # Can't determine — don't download

    try:
        from dateutil.parser import parse as parse_dt
        sp_mtime = parse_dt(meta["lastModifiedDateTime"]).timestamp()
        local_mtime = local_path.stat().st_mtime
        is_newer = sp_mtime > local_mtime + 60  # 60s buffer for clock skew
        if is_newer:
            sp_dt = datetime.fromtimestamp(sp_mtime)
            local_dt = datetime.fromtimestamp(local_mtime)
            log.info("Graph Files: SharePoint is newer (%s vs local %s)",
                     sp_dt.strftime("%m/%d %H:%M"), local_dt.strftime("%m/%d %H:%M"))
        return is_newer
    except Exception as e:
        log.warning("Graph Files: date comparison failed: %s", e)
        return False


def _archive_snapshot(src: Path):
    """Archive Bloomberg file with today's date. Keep 30 days locally."""
    import shutil
    from datetime import datetime as _dt, timedelta

    history_dir = src.parent / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    today = _dt.now().strftime("%Y-%m-%d")
    dest = history_dir / f"bloomberg_daily_file_{today}.xlsm"

    if not dest.exists():
        shutil.copy2(src, dest)
        log.info("Graph Files: archived snapshot -> %s", dest.name)

    # Mirror to D: drive if available
    d_archive = Path("D:/sec-data/archives/bloomberg")
    if d_archive.parent.exists():
        try:
            d_archive.mkdir(parents=True, exist_ok=True)
            d_dest = d_archive / dest.name
            if not d_dest.exists():
                shutil.copy2(src, d_dest)
                log.info("Graph Files: mirrored to D: -> %s", d_dest.name)
        except Exception as e:
            log.warning("Graph Files: D: mirror failed (non-fatal): %s", e)

    # Clean up local snapshots older than 30 days (D: keeps everything)
    cutoff = _dt.now() - timedelta(days=30)
    for old in history_dir.glob("bloomberg_daily_file_*.xlsm"):
        try:
            date_str = old.stem.split("_")[-1]
            file_date = _dt.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff:
                old.unlink()
                log.info("Graph Files: deleted old local snapshot %s", old.name)
        except (ValueError, OSError):
            pass
