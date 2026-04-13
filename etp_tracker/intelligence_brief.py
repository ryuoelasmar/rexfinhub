"""Daily Filing Intelligence Brief — executive-first.

One question: what does Scott need to act on today?

Three sections only:
  1. Action Required — file/monitor/alert items with reasons
  2. Competitive Races — underliers with multiple filers, REX gaps first
  3. Effectives This Week — next 7 days, grouped by filing

No strategy watch, no pipeline summary, no raw filings dump.
Filings are grouped by accession number so a 485APOS with 10 funds shows as 1 row.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

DASHBOARD_URL = "https://rex-etp-tracker.onrender.com"

# Unified design tokens
_MAX_WIDTH = "680px"
_NAVY = "#0f172a"
_RED = "#dc2626"
_AMBER = "#d97706"
_GREEN = "#059669"
_BLUE = "#2563eb"
_GRAY = "#64748b"
_LIGHT = "#f8fafc"
_BORDER = "#e5e7eb"
_WHITE = "#ffffff"


def build_intelligence_brief(db: Session, lookback_days: int = 1) -> str:
    """Build the daily intelligence brief HTML."""
    today = date.today()
    since = today - timedelta(days=lookback_days)

    actions = _gather_actions(db, today)
    races = _gather_races(db, today)
    effectives = _gather_effectives_grouped(db, today, days_ahead=7)

    return _render(actions=actions, races=races, effectives=effectives, since=since, today=today)


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def _extract_underlier(fund_name: str) -> str | None:
    """Extract underlier ticker from a 2X fund name."""
    if not fund_name:
        return None
    name = fund_name.upper()
    m = re.search(r'2X\s+(?:LONG|INVERSE|SHORT)\s+([A-Z]{1,5})\s', name)
    if m:
        return m.group(1)
    m = re.search(r'DAILY\s+TARGET[^A-Z]*([A-Z]{2,5})\s', name)
    if m:
        return m.group(1)
    return None


def _gather_actions(db: Session, today: date) -> list[dict]:
    """Generate action items from the data.

    Actions driven by real signals:
      - FILE: An underlier has a competitor filing with effective date in <60 days, REX has not filed
      - MONITOR: REX has filed but a competitor will go effective earlier
      - ALERT: A REX fund has a new delaying amendment or extension
    """
    from webapp.models import FundStatus, Trust, Filing

    actions = []

    # Pull pending 2X funds across all issuers
    rows = db.execute(
        select(
            FundStatus.fund_name,
            FundStatus.status,
            FundStatus.effective_date,
            FundStatus.latest_form,
            FundStatus.latest_filing_date,
            Trust.name.label("trust_name"),
            Trust.is_rex,
        )
        .join(Trust, Trust.id == FundStatus.trust_id)
        .where(FundStatus.fund_name.ilike("%2X%"))
        .where(FundStatus.status.in_(["PENDING", "EFFECTIVE"]))
    ).all()

    # Group by underlier
    by_ul = defaultdict(list)
    for r in rows:
        ul = _extract_underlier(r.fund_name)
        if not ul:
            continue
        by_ul[ul].append(r)

    cutoff_60 = today + timedelta(days=60)

    for ul, entries in by_ul.items():
        has_rex = any(e.is_rex for e in entries)
        # Earliest competitor effective date
        competitors = [e for e in entries if not e.is_rex and e.effective_date]
        rex_funds = [e for e in entries if e.is_rex]

        if not competitors:
            continue

        earliest_comp = min(competitors, key=lambda e: e.effective_date)

        # FILE: No REX fund on this underlier, competitor going effective soon
        if not has_rex and earliest_comp.effective_date <= cutoff_60:
            days = (earliest_comp.effective_date - today).days
            actions.append({
                "type": "FILE",
                "underlier": ul,
                "title": f"FILE {ul}",
                "detail": f"{earliest_comp.trust_name} effective {earliest_comp.effective_date.strftime('%b %d')} ({days}d). REX not filed.",
                "severity": "high",
                "urgency": days,
            })
            continue

        # MONITOR: REX filed but is behind a competitor
        if has_rex:
            rex_earliest = [e for e in rex_funds if e.effective_date]
            if rex_earliest:
                rex_min = min(rex_earliest, key=lambda e: e.effective_date)
                if earliest_comp.effective_date < rex_min.effective_date:
                    comp_days = (earliest_comp.effective_date - today).days
                    gap = (rex_min.effective_date - earliest_comp.effective_date).days
                    actions.append({
                        "type": "MONITOR",
                        "underlier": ul,
                        "title": f"MONITOR {ul}",
                        "detail": f"{earliest_comp.trust_name} effective {earliest_comp.effective_date.strftime('%b %d')} ({comp_days}d). REX is {gap}d behind.",
                        "severity": "medium",
                        "urgency": comp_days,
                    })

    # ALERT: REX funds with delaying amendments filed in last 3 days
    recent = db.execute(
        select(
            Filing.form,
            Filing.filing_date,
            FundStatus.fund_name,
            Trust.name.label("trust_name"),
        )
        .join(Trust, Trust.id == Filing.trust_id)
        .outerjoin(FundStatus, FundStatus.trust_id == Trust.id)
        .where(Trust.is_rex == True)
        .where(Filing.filing_date >= today - timedelta(days=3))
        .where(Filing.form.in_(["485BXT", "485APOS"]))
    ).all()

    for r in recent:
        if r.form == "485BXT":
            ul = _extract_underlier(r.fund_name or "")
            title = f"ALERT: REX 485BXT"
            if ul:
                title = f"ALERT: REX {ul} extension"
            actions.append({
                "type": "ALERT",
                "underlier": ul or "",
                "title": title,
                "detail": f"Extension filed {r.filing_date.strftime('%b %d')}. Effective date may shift.",
                "severity": "medium",
                "urgency": 999,  # deprioritize vs file/monitor
            })

    # Sort: FILE first (by urgency), then MONITOR, then ALERT
    type_order = {"FILE": 0, "MONITOR": 1, "ALERT": 2}
    actions.sort(key=lambda a: (type_order.get(a["type"], 9), a["urgency"]))

    return actions[:5]


def _gather_races(db: Session, today: date) -> list[dict]:
    """Underliers with 2+ issuers filing. REX gaps sorted first."""
    from webapp.models import FundStatus, Trust

    rows = db.execute(
        select(
            FundStatus.fund_name,
            FundStatus.ticker,
            FundStatus.status,
            FundStatus.effective_date,
            Trust.name.label("trust_name"),
            Trust.is_rex,
        )
        .join(Trust, Trust.id == FundStatus.trust_id)
        .where(FundStatus.status.in_(["PENDING", "EFFECTIVE"]))
        .where(FundStatus.fund_name.ilike("%2X%"))
    ).all()

    by_ul = defaultdict(list)
    for r in rows:
        ul = _extract_underlier(r.fund_name)
        if not ul:
            continue
        by_ul[ul].append({
            "trust": r.trust_name or "",
            "status": r.status,
            "effective_date": r.effective_date,
            "ticker": r.ticker or "",
            "is_rex": r.is_rex,
        })

    races = []
    for ul, entries in by_ul.items():
        trusts = {e["trust"] for e in entries}
        if len(trusts) < 2:
            continue

        # Earliest effective date
        dated = [e for e in entries if e["effective_date"]]
        earliest = min(e["effective_date"] for e in dated) if dated else None
        has_rex = any(e["is_rex"] for e in entries)

        # Dedupe by trust+status, keep earliest effective
        by_trust = {}
        for e in sorted(entries, key=lambda x: (x["effective_date"] or date(2099, 1, 1))):
            key = e["trust"]
            if key not in by_trust:
                by_trust[key] = e

        races.append({
            "underlier": ul,
            "issuers": list(by_trust.values()),
            "issuer_count": len(by_trust),
            "earliest": earliest,
            "has_rex": has_rex,
        })

    # Sort: REX gaps first (competition exists, REX missing), then by urgency
    def sort_key(r):
        gap_priority = 0 if not r["has_rex"] else 1
        urgency = (r["earliest"] - today).days if r["earliest"] else 9999
        return (gap_priority, urgency)

    races.sort(key=sort_key)
    return races[:10]


def _gather_effectives_grouped(db: Session, today: date, days_ahead: int = 7) -> list[dict]:
    """Funds going effective in next N days, grouped by filing.

    If 10 funds share one accession number, they appear as ONE row with fund_count=10.
    """
    from webapp.models import FundStatus, Trust, Filing, FundExtraction

    cutoff = today + timedelta(days=days_ahead)

    # Pull pending funds with effective date in window
    rows = db.execute(
        select(
            FundStatus.fund_name,
            FundStatus.ticker,
            FundStatus.effective_date,
            FundStatus.latest_form,
            FundStatus.trust_id,
            Trust.name.label("trust_name"),
            Trust.is_rex,
            FundExtraction.filing_id,
            Filing.accession_number,
        )
        .join(Trust, Trust.id == FundStatus.trust_id)
        .outerjoin(FundExtraction, FundExtraction.series_id == FundStatus.series_id)
        .outerjoin(Filing, Filing.id == FundExtraction.filing_id)
        .where(FundStatus.status == "PENDING")
        .where(FundStatus.effective_date.isnot(None))
        .where(FundStatus.effective_date >= today)
        .where(FundStatus.effective_date <= cutoff)
    ).all()

    # Group by (trust, effective_date, form) — closest we can get to filing-level
    grouped = defaultdict(lambda: {
        "trust": "", "is_rex": False, "effective_date": None,
        "form": "", "funds": [], "fund_count": 0,
    })
    for r in rows:
        key = (r.trust_name, str(r.effective_date), r.latest_form or "")
        g = grouped[key]
        g["trust"] = r.trust_name or ""
        g["is_rex"] = r.is_rex
        g["effective_date"] = r.effective_date
        g["form"] = r.latest_form or ""
        if r.fund_name:
            g["funds"].append({"name": r.fund_name, "ticker": r.ticker or ""})

    result = []
    seen = set()
    for g in grouped.values():
        # Dedupe funds within a group
        unique_funds = []
        for f in g["funds"]:
            if f["name"] not in seen:
                unique_funds.append(f)
                seen.add(f["name"])
        g["funds"] = unique_funds
        g["fund_count"] = len(unique_funds)
        if g["fund_count"] > 0:
            result.append(g)
        seen.clear()  # reset per group for deduping within group only

    result.sort(key=lambda x: x["effective_date"])
    return result


# ---------------------------------------------------------------------------
# Rendering — clean, fixed-width, no emojis, no slop
# ---------------------------------------------------------------------------

def _render(*, actions, races, effectives, since, today) -> str:
    header = _render_header(today)
    actions_section = _render_actions(actions)
    races_section = _render_races(races, today)
    effectives_section = _render_effectives(effectives, today)

    body = "\n".join([header, actions_section, races_section, effectives_section])

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Filing Intelligence Brief — {today.strftime('%b %d')}</title></head>
<body style="margin:0; padding:20px; background:#f1f5f9; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:{_MAX_WIDTH}; margin:0 auto; background:{_WHITE}; border-radius:6px; border:1px solid {_BORDER}; overflow:hidden;">
{body}
</div>
</body></html>"""


def _render_header(today: date) -> str:
    return f"""
<div style="padding:20px 24px 16px; border-bottom:1px solid {_BORDER};">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:600;">REX Financial</div>
  <div style="font-size:22px; font-weight:700; color:{_NAVY}; margin-top:4px;">Filing Intelligence Brief</div>
  <div style="font-size:13px; color:{_GRAY}; margin-top:2px;">{today.strftime('%A, %B %d, %Y')}</div>
</div>"""


def _render_actions(actions: list[dict]) -> str:
    if not actions:
        return f"""
<div style="padding:18px 24px; border-bottom:1px solid {_BORDER};">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:8px;">Action Required</div>
  <div style="font-size:13px; color:{_GRAY}; font-style:italic;">No action required today.</div>
</div>"""

    items = []
    for a in actions:
        color = _RED if a["severity"] == "high" else _AMBER
        bg = "#fef2f2" if a["severity"] == "high" else "#fffbeb"
        items.append(f"""
  <div style="border-left:3px solid {color}; background:{bg}; padding:10px 14px; margin-bottom:8px; border-radius:0 4px 4px 0;">
    <div style="font-size:13px; font-weight:700; color:{_NAVY};">{a['title']}</div>
    <div style="font-size:12px; color:#374151; margin-top:2px;">{a['detail']}</div>
  </div>""")

    return f"""
<div style="padding:18px 24px; border-bottom:1px solid {_BORDER};">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:10px;">Action Required</div>
  {''.join(items)}
</div>"""


def _render_races(races: list[dict], today: date) -> str:
    if not races:
        return f"""
<div style="padding:18px 24px; border-bottom:1px solid {_BORDER};">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:8px;">Competitive Races</div>
  <div style="font-size:13px; color:{_GRAY}; font-style:italic;">No active races.</div>
</div>"""

    items = []
    for race in races:
        # Position label
        if not race["has_rex"]:
            status_label = f'<span style="color:{_RED}; font-weight:700; font-size:11px; text-transform:uppercase;">REX Gap</span>'
        else:
            status_label = f'<span style="color:{_GREEN}; font-weight:700; font-size:11px; text-transform:uppercase;">REX In</span>'

        issuer_rows = []
        for e in race["issuers"]:
            is_rex = e["is_rex"]
            rex_tag = f'<span style="color:{_GREEN}; font-weight:700; font-size:10px; margin-left:4px;">REX</span>' if is_rex else ""
            eff = e["effective_date"].strftime('%b %d') if e["effective_date"] else "TBD"
            trust_short = e["trust"][:30]
            issuer_rows.append(f"""
      <tr>
        <td style="padding:4px 0; font-size:12px; color:#374151;">{trust_short}{rex_tag}</td>
        <td style="padding:4px 0; font-size:12px; color:{_GRAY}; text-align:right;">{eff}</td>
      </tr>""")

        items.append(f"""
  <div style="border:1px solid {_BORDER}; border-radius:4px; padding:10px 14px; margin-bottom:8px;">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <div style="font-size:14px; font-weight:700; color:{_NAVY}; font-family:monospace;">{race['underlier']}</div>
      {status_label}
    </div>
    <table style="width:100%; border-collapse:collapse; margin-top:6px;">
      {''.join(issuer_rows)}
    </table>
  </div>""")

    return f"""
<div style="padding:18px 24px; border-bottom:1px solid {_BORDER};">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:10px;">Competitive Races</div>
  {''.join(items)}
</div>"""


def _render_effectives(effectives: list[dict], today: date) -> str:
    if not effectives:
        return f"""
<div style="padding:18px 24px;">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:8px;">Effectives This Week</div>
  <div style="font-size:13px; color:{_GRAY}; font-style:italic;">No funds going effective in the next 7 days.</div>
</div>"""

    rows = []
    for e in effectives:
        is_rex = e["is_rex"]
        days_left = (e["effective_date"] - today).days
        trust_short = e["trust"][:30]
        rex_tag = f'<span style="color:{_GREEN}; font-weight:700; font-size:10px; margin-left:4px;">REX</span>' if is_rex else ""

        # Fund count display
        if e["fund_count"] == 1:
            fund_display = e["funds"][0]["name"][:45]
        else:
            first_fund = e["funds"][0]["name"][:30]
            fund_display = f'{first_fund} <span style="color:{_GRAY};">+{e["fund_count"]-1} more</span>'

        row_bg = "#fef2f2" if days_left <= 2 else ""

        rows.append(f"""
      <tr style="background:{row_bg};">
        <td style="padding:8px 0; font-size:12px; color:{_NAVY}; font-weight:600; white-space:nowrap;">{e['effective_date'].strftime('%b %d')}</td>
        <td style="padding:8px 10px; font-size:12px; color:{_GRAY}; white-space:nowrap;">{days_left}d</td>
        <td style="padding:8px 10px; font-size:12px; color:#374151;">{trust_short}{rex_tag}</td>
        <td style="padding:8px 0; font-size:12px; color:#374151;">{fund_display}</td>
      </tr>""")

    return f"""
<div style="padding:18px 24px;">
  <div style="font-size:11px; text-transform:uppercase; letter-spacing:0.08em; color:{_GRAY}; font-weight:700; margin-bottom:10px;">Effectives This Week</div>
  <table style="width:100%; border-collapse:collapse;">
    <thead>
      <tr style="border-bottom:1px solid {_BORDER};">
        <th style="padding:6px 0; text-align:left; font-size:10px; color:{_GRAY}; text-transform:uppercase; font-weight:600;">Date</th>
        <th style="padding:6px 10px; text-align:left; font-size:10px; color:{_GRAY}; text-transform:uppercase; font-weight:600;">In</th>
        <th style="padding:6px 10px; text-align:left; font-size:10px; color:{_GRAY}; text-transform:uppercase; font-weight:600;">Issuer</th>
        <th style="padding:6px 0; text-align:left; font-size:10px; color:{_GRAY}; text-transform:uppercase; font-weight:600;">Filing</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</div>"""
