"""Ramping burst test to find CBOE's effective rate-limit ceiling.

CBOE doesn't publish a rate limit for the issuer symbol_status endpoint.
We ramp concurrency through a series of rungs, watch for 429s / connection
errors / timeouts, and report the last clean rung plus a safety-factor'd
suggested concurrency.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

from webapp.services.cboe.universe import combos_of_length

log = logging.getLogger(__name__)

CBOE_ENDPOINT = "https://account.cboe.com/account/listings/symbol_reservations/symbol_status/"
USER_AGENT = "rexfinhub-ticker-radar/0.1 (symbol-availability monitor)"

PROBE_RUNGS = (5, 10, 20, 35, 50, 75, 100)
REQUESTS_PER_RUNG = 100
COOLDOWN_BETWEEN_RUNGS = 5
SAFETY_FACTOR = 0.85
FAIL_RATE_THRESHOLD = 0.02


async def _fetch(
    session: aiohttp.ClientSession, symbol: str, cookie: str
) -> tuple[int, bool]:
    headers = {
        "User-Agent": USER_AGENT,
        "Cookie": cookie,
        "Accept": "application/json",
    }
    try:
        async with session.get(
            CBOE_ENDPOINT,
            params={"symbol": symbol},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return resp.status, False
            data = await resp.json(content_type=None)
            return 200, "available" in data
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return 0, False


async def _probe_rung(
    rung: int, total: int, cookie: str, symbols: list[str]
) -> dict[str, Any]:
    sem = asyncio.Semaphore(rung)
    symbol_cycle = (symbols * ((total // len(symbols)) + 1))[:total]
    statuses: list[int] = []
    oks = 0

    connector = aiohttp.TCPConnector(limit=rung * 2)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def _one(sym: str) -> None:
            nonlocal oks
            async with sem:
                code, ok = await _fetch(session, sym, cookie)
                statuses.append(code)
                if ok:
                    oks += 1

        started = time.monotonic()
        await asyncio.gather(*(_one(s) for s in symbol_cycle))
        elapsed = time.monotonic() - started

    return {
        "target_concurrency": rung,
        "requests": len(statuses),
        "ok": oks,
        "fail_rate": round(1 - (oks / len(statuses)) if statuses else 1.0, 3),
        "any_429": any(s == 429 for s in statuses),
        "any_auth_rejection": any(s in (401, 403) for s in statuses),
        "status_counts": {c: statuses.count(c) for c in set(statuses)},
        "effective_rps": round(len(statuses) / elapsed, 1) if elapsed > 0 else 0,
        "elapsed_seconds": round(elapsed, 1),
    }


async def probe_rate_limit(
    cookie: str, rungs: tuple[int, ...] = PROBE_RUNGS
) -> dict[str, Any]:
    if not cookie:
        raise ValueError("CBOE_SESSION_COOKIE required")

    symbols = list(combos_of_length(3))[:200]
    history: list[dict[str, Any]] = []
    last_clean: int | None = None

    for rung in rungs:
        log.info("Probing concurrency=%d (%d requests)...", rung, REQUESTS_PER_RUNG)
        stats = await _probe_rung(rung, REQUESTS_PER_RUNG, cookie, symbols)
        history.append(stats)
        log.info(
            "  -> fail_rate=%.3f any_429=%s rps=%s",
            stats["fail_rate"], stats["any_429"], stats["effective_rps"],
        )

        if stats["any_auth_rejection"]:
            log.error("Auth rejection (401/403) — session cookie is invalid. Aborting probe.")
            break

        clean = (stats["fail_rate"] < FAIL_RATE_THRESHOLD) and not stats["any_429"]
        if clean:
            last_clean = rung
        else:
            log.warning("Rung %d unhealthy; stopping ramp.", rung)
            break

        await asyncio.sleep(COOLDOWN_BETWEEN_RUNGS)

    suggested = max(1, int(last_clean * SAFETY_FACTOR)) if last_clean else 5

    return {
        "history": history,
        "last_clean_rung": last_clean,
        "safety_factor": SAFETY_FACTOR,
        "suggested_concurrency": suggested,
    }
