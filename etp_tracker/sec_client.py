from __future__ import annotations
import time, json, hashlib
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from .config import USER_AGENT_DEFAULT, SEC_SUBMISSIONS_URL
except Exception:
    USER_AGENT_DEFAULT = "REX-ETP-FilingTracker/1.0 (contact: set USER_AGENT)"
    SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{CIK_PADDED}.json"

import os

_DEFAULT_CACHE_DIR = os.environ.get("SEC_CACHE_DIR", str(Path(__file__).resolve().parent.parent / "cache" / "sec"))
# Hard cap on cache/web total size (in MB). Above this, the oldest files
# (by mtime) are evicted on next SECClient init. 5 GB is comfortable for
# the watcher workload and leaves plenty of room on a 38 GB VPS disk.
# Overridable via env for targeted tuning.
_CACHE_MAX_MB = int(os.environ.get("SEC_CACHE_MAX_MB", "5000"))
# Check interval — only actually walk the tree every N seconds of wall
# clock. Prevents multiple workers from all pruning at once.
_CACHE_PRUNE_INTERVAL_SEC = int(os.environ.get("SEC_CACHE_PRUNE_INTERVAL", "3600"))


def _prune_web_cache(cache_dir: Path, max_mb: int) -> dict:
    """LRU-prune cache_dir/web to stay under max_mb.

    Walks the bucketed cache, totals size, and if above the cap deletes
    oldest files (by mtime) until we're back under. Safe to call on startup
    of every worker — races are benign because os.unlink of a missing file
    is tolerated.

    Returns a summary dict for logging: {initial_mb, evicted_files, final_mb}.
    """
    web = cache_dir / "web"
    if not web.exists():
        return {"initial_mb": 0, "evicted_files": 0, "final_mb": 0}

    marker = cache_dir / ".last_prune"
    # Skip if we pruned recently (avoids storm on parallel worker startup)
    if marker.exists():
        age = time.time() - marker.stat().st_mtime
        if age < _CACHE_PRUNE_INTERVAL_SEC:
            return {"initial_mb": 0, "evicted_files": 0, "final_mb": 0, "skipped": True}

    files = []
    total = 0
    for p in web.rglob("*"):
        if p.is_file():
            try:
                stat = p.stat()
                files.append((stat.st_mtime, stat.st_size, p))
                total += stat.st_size
            except OSError:
                continue

    initial_mb = total // (1024 * 1024)
    cap_bytes = max_mb * 1024 * 1024
    evicted = 0

    if total > cap_bytes:
        files.sort(key=lambda x: x[0])  # oldest first
        for mtime, size, path in files:
            if total <= cap_bytes:
                break
            try:
                path.unlink()
                total -= size
                evicted += 1
            except OSError:
                continue

    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
    except OSError:
        pass

    return {
        "initial_mb": initial_mb,
        "evicted_files": evicted,
        "final_mb": total // (1024 * 1024),
    }


class SECClient:
    def __init__(self, user_agent: str = USER_AGENT_DEFAULT, request_timeout: int = 30, pause: float = 0.25, cache_dir: Path | str = _DEFAULT_CACHE_DIR):
        self.user_agent = user_agent or USER_AGENT_DEFAULT
        # Use (connect, read) tuple so both phases are bounded.
        # Scalar timeout in requests only covers the initial connect;
        # a stalled body-read will hang forever and deadlock the ThreadPoolExecutor.
        self.timeout = (float(request_timeout), float(request_timeout))
        self.pause = float(pause)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        retry = Retry(total=5, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["HEAD","GET","OPTIONS"]))
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        (self.cache_dir / "submissions").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "web").mkdir(parents=True, exist_ok=True)

        # LRU-prune cache/web at init so we don't silently balloon to 20 GB
        # like we did on 2026-04-14. Cheap when already under cap (hits the
        # skipped branch via the .last_prune marker).
        try:
            import logging as _logging
            summary = _prune_web_cache(self.cache_dir, _CACHE_MAX_MB)
            if summary.get("evicted_files"):
                _logging.getLogger(__name__).info(
                    "SEC cache pruned: %d MB -> %d MB (evicted %d files)",
                    summary["initial_mb"], summary["final_mb"], summary["evicted_files"],
                )
        except Exception:
            pass  # prune is best-effort, never block client init

    def _hash_url(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _web_path(self, url: str, ext: str) -> Path:
        """Bucketed cache path: web/ab/abcdef...{ext} — keeps each folder small."""
        h = self._hash_url(url)
        bucket = self.cache_dir / "web" / h[:2]
        bucket.mkdir(parents=True, exist_ok=True)
        return bucket / (h + ext)

    def _find_cached(self, url: str, ext: str) -> Path | None:
        """Find a cached file — checks new bucketed path first, then old flat path."""
        h = self._hash_url(url)
        # New layout: web/ab/abcdef...ext
        bucketed = self.cache_dir / "web" / h[:2] / (h + ext)
        if bucketed.exists():
            return bucketed
        # Old layout: web/abcdef...ext (from C: drive copy)
        flat = self.cache_dir / "web" / (h + ext)
        if flat.exists():
            return flat
        return None

    def fetch_header_text(self, url: str, use_cache: bool = True) -> str:
        """Read only the SEC-HEADER portion (~2KB) from a cached .txt file.
        Falls back to full fetch if file is not cached yet."""
        if not url:
            return ""
        if use_cache:
            cached = self._find_cached(url, ".txt")
            if cached:
                try:
                    lines = []
                    with open(cached, encoding="utf-8", errors="ignore") as f:
                        for line in f:
                            lines.append(line)
                            if "</SEC-HEADER>" in line:
                                break
                    return "".join(lines)
                except Exception:
                    pass
        # Not cached or read error - fetch full file
        return self.fetch_text(url, use_cache=use_cache)

    def fetch_text(self, url: str, use_cache: bool = True) -> str:
        if not url: return ""
        if use_cache:
            cached = self._find_cached(url, ".txt")
            if cached:
                try: return cached.read_text(encoding="utf-8", errors="ignore")
                except Exception: pass
        time.sleep(self.pause)
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        text = r.text
        try:
            dest = self._web_path(url, ".txt")
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".tmp")
            tmp.write_text(text, encoding="utf-8", errors="ignore")
            tmp.replace(dest)  # atomic rename
        except Exception: pass
        return text

    def fetch_bytes(self, url: str, use_cache: bool = True) -> bytes:
        if not url: return b""
        if use_cache:
            cached = self._find_cached(url, ".bin")
            if cached:
                try: return cached.read_bytes()
                except Exception: pass
        time.sleep(self.pause)
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        data = r.content
        try:
            dest = self._web_path(url, ".bin")
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".tmp")
            tmp.write_bytes(data)
            tmp.replace(dest)  # atomic rename
        except Exception: pass
        return data

    def load_submissions_json(self, cik: str, refresh_submissions: bool = True, refresh_max_age_hours: int = 6, refresh_force_now: bool = False) -> dict:
        cik_int = int(str(cik))
        cik_padded = f"{cik_int:010d}"
        url = SEC_SUBMISSIONS_URL.replace("{CIK_PADDED}", cik_padded)
        cache_path = self.cache_dir / "submissions" / f"{cik_padded}.json"
        should_refresh = refresh_force_now
        if refresh_submissions and not should_refresh:
            if not cache_path.exists(): should_refresh = True
            else:
                try:
                    import time as _t
                    age = (_t.time() - cache_path.stat().st_mtime) / 3600.0
                    if age >= float(refresh_max_age_hours): should_refresh = True
                except Exception:
                    should_refresh = True
        if should_refresh:
            time.sleep(0.11)  # Faster for submissions checks (~9 req/s, within SEC 10 req/s limit)
            # Use If-Modified-Since to skip unchanged submissions (304 = no change)
            headers = {}
            if cache_path.exists():
                try:
                    import email.utils, os
                    mtime = os.path.getmtime(cache_path)
                    headers["If-Modified-Since"] = email.utils.formatdate(mtime, usegmt=True)
                except Exception:
                    pass
            r = self.session.get(url, timeout=self.timeout, headers=headers)
            if r.status_code == 304 and cache_path.exists():
                # Not modified — use cache, update mtime so we don't re-check for 6 hours
                try: os.utime(cache_path)
                except Exception: pass
                return json.loads(cache_path.read_text(encoding="utf-8"))
            r.raise_for_status()
            data = r.json()
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data), encoding="utf-8")
                tmp.replace(cache_path)
            except Exception: pass
            return data
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            time.sleep(self.pause)
            r = self.session.get(url, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(data), encoding="utf-8")
                tmp.replace(cache_path)
            except Exception: pass
            return data

    def fetch_json(self, url: str, use_cache: bool = True) -> dict:
        """Fetch a JSON resource from SEC with caching.

        Used for paginated filings files (filings.files[] in submissions JSON).
        """
        if not url:
            return {}
        cache_path = self.cache_dir / "submissions" / (self._hash_url(url) + ".json")
        if use_cache and cache_path.exists():
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        time.sleep(self.pause)
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(cache_path)
        except Exception:
            pass
        return data

    def get_entity_tickers(self, cik: str) -> list[str]:
        """Read tickers from cached submissions JSON (no network call).

        Returns list of ticker strings, or empty list on cache miss / parse error.
        """
        try:
            cik_padded = f"{int(str(cik)):010d}"
            cache_path = self.cache_dir / "submissions" / f"{cik_padded}.json"
            if not cache_path.exists():
                return []
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            tickers = data.get("tickers", [])
            return [t for t in tickers if isinstance(t, str) and t.strip()]
        except Exception:
            return []
