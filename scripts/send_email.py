"""
Send or preview REX email reports.

Commands (via bash aliases):
    send daily       REX Daily ETP Report
    send weekly      Weekly Report + L&I + Income + Flow
    preview daily    Open daily report in browser
    preview weekly   Open all weekly reports in browser
"""
from __future__ import annotations

import os
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

DASHBOARD_URL = "https://rex-etp-tracker.onrender.com"
PREVIEW_DIR = PROJECT_ROOT / "outputs" / "previews"


def _save_and_open(html: str, name: str):
    """Write HTML to file and open in browser."""
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    path = PREVIEW_DIR / f"{name}.html"
    path.write_text(html, encoding="utf-8")
    print(f"  {name}: {len(html):,} chars -> {path.name}")
    webbrowser.open(str(path.resolve()))


def _get_db():
    from webapp.database import SessionLocal
    return SessionLocal()


def _send_via_smtp(html: str, subject: str):
    """Send HTML email to all configured recipients."""
    from etp_tracker.email_alerts import _load_recipients, _load_private_recipients, _send_html_digest
    recipients = _load_recipients()
    private = _load_private_recipients()
    if not recipients and not private:
        print(f"  SKIP {subject} (no recipients)")
        return False
    ok = True
    if recipients:
        ok = _send_html_digest(html, recipients, subject_override=subject)
    if private:
        _send_html_digest(html, private, subject_override=subject)
    return ok


# ---------------------------------------------------------------------------
# Daily bundle: Filing Report + L&I + Income + Flow
# ---------------------------------------------------------------------------

def _build_daily_filing(db) -> str:
    from etp_tracker.email_alerts import build_digest_html_from_db
    return build_digest_html_from_db(db, DASHBOARD_URL, edition="daily")


def _build_li(db) -> str:
    from webapp.services.report_emails import build_li_email
    html, _ = build_li_email(DASHBOARD_URL, db)
    return html


def _build_income(db) -> str:
    from webapp.services.report_emails import build_cc_email
    html, _ = build_cc_email(DASHBOARD_URL, db)
    return html


def _build_flow(db) -> str:
    from webapp.services.report_emails import build_flow_email
    html, _ = build_flow_email(DASHBOARD_URL, db)
    return html


def _data_date(db) -> str:
    """Get data date (MM/DD/YYYY) from report cache for email subjects."""
    from webapp.services.report_data import get_li_report
    data = get_li_report(db)
    return data.get("data_as_of_short", datetime.now().strftime("%m/%d/%Y"))


DAILY_REPORTS = [
    ("REX Daily ETP Report", "daily_filing", _build_daily_filing),
]

WEEKLY_REPORTS = [
    ("REX Weekly ETP Report", "weekly_report", None),  # special handler
    ("REX ETP Leverage & Inverse Report", "li_report", _build_li),
    ("REX ETP Income Report", "income_report", _build_income),
    ("REX ETP Flow Report", "flow_report", _build_flow),
]


def do_daily(preview: bool):
    db = _get_db()
    try:
        date = _data_date(db)
        for base_title, filename, builder in DAILY_REPORTS:
            subject = f"{base_title}: {date}"
            print(f"\n  Building {subject}...")
            html = builder(db)
            if preview:
                _save_and_open(html, filename)
            else:
                ok = _send_via_smtp(html, subject)
                print(f"  {'Sent' if ok else 'FAILED'}: {subject}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Weekly bundle: Weekly Report + L&I + Income + Flow
# ---------------------------------------------------------------------------

def do_weekly(preview: bool):
    from etp_tracker.weekly_digest import build_weekly_digest_html, send_weekly_digest

    db = _get_db()
    try:
        date = _data_date(db)

        # Weekly report
        weekly_subject = f"REX Weekly ETP Report: {date}"
        print(f"\n  Building {weekly_subject}...")
        if preview:
            html = build_weekly_digest_html(db, DASHBOARD_URL)
            _save_and_open(html, "weekly_report")
        else:
            ok = send_weekly_digest(db, DASHBOARD_URL)
            print(f"  {'Sent' if ok else 'FAILED'}: {weekly_subject}")

        # Market reports (L&I, Income, Flow)
        for base_title, filename, builder in WEEKLY_REPORTS:
            if builder is None:
                continue  # weekly_report handled above
            subject = f"{base_title}: {date}"
            print(f"\n  Building {subject}...")
            html = builder(db)
            if preview:
                _save_and_open(html, filename)
            else:
                ok = _send_via_smtp(html, subject)
                print(f"  {'Sent' if ok else 'FAILED'}: {subject}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def do_market_share(preview: bool):
    """Generate market share analysis (4 categories x 2 charts + summary table)."""
    from scripts.generate_market_share_charts import main as gen_charts

    print("\n  Generating market share charts...")
    gen_charts()

    report_dir = PROJECT_ROOT / "reports"
    html_file = sorted(report_dir.glob("rex_market_share_analysis_*.html"), reverse=True)
    if not html_file:
        print("  ERROR: no HTML file generated")
        return

    html_path = html_file[0]
    html = html_path.read_text(encoding="utf-8")

    if preview:
        # Copy to previews dir + open
        dest = PREVIEW_DIR / "market_share.html"
        PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
        dest.write_text(html, encoding="utf-8")
        print(f"  market_share: {len(html):,} chars -> {dest.name}")
        webbrowser.open(str(dest.resolve()))
    else:
        subject = f"REX Market Share Analysis: {datetime.now().strftime('%m/%d/%Y')}"
        ok = _send_via_smtp(html, subject)
        print(f"  {'Sent' if ok else 'FAILED'}: {subject}")


VALID_BUNDLES = ("daily", "weekly", "market_share", "all")


def main():
    args = [a.lower() for a in sys.argv[1:]]

    if len(args) < 2 or args[0] not in ("send", "preview") or args[1] not in VALID_BUNDLES:
        print("Usage:")
        print("  send daily          REX Daily ETP Report")
        print("  send weekly         Weekly Report + L&I + Income + Flow")
        print("  send market_share   Market Share Analysis (CEO charts)")
        print("  preview daily       Open daily report in browser")
        print("  preview weekly      Open all weekly reports in browser")
        print("  preview market_share  Open market share analysis in browser")
        print("  preview all         Open daily + weekly reports in browser")
        sys.exit(0)

    action = args[0]
    bundle = args[1]
    preview = action == "preview"

    label = "PREVIEW" if preview else "SEND"
    print(f"=== [{label}] {bundle.upper()} ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ===")

    if bundle == "daily":
        do_daily(preview)
    elif bundle == "weekly":
        do_weekly(preview)
    elif bundle == "market_share":
        do_market_share(preview)
    else:  # "all"
        do_daily(preview)
        do_weekly(preview)

    print("\nDone.")


if __name__ == "__main__":
    main()
