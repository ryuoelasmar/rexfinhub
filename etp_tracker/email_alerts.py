"""
Email Alerts - Daily Digest

Email-client-compatible HTML digest (inline styles, table layout, no JS).
Works in Outlook, Gmail, Apple Mail, etc.

Sections:
- KPI summary bar
- What changed today
- REX trusts detail
- New prospectus filings
- Per-trust summary (counts + latest filing)
- Link to dashboard for full details
"""
from __future__ import annotations
import smtplib
import os
import html as html_mod
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

_REX_TRUSTS = {"REX ETF Trust", "ETF Opportunities Trust"}

# Email-safe colors (no CSS variables)
_NAVY = "#1a1a2e"
_GREEN = "#27ae60"
_ORANGE = "#e67e22"
_RED = "#e74c3c"
_BLUE = "#0984e3"
_GRAY = "#636e72"
_LIGHT = "#f8f9fa"
_BORDER = "#dee2e6"
_WHITE = "#ffffff"


def _load_recipients(project_root: Path | None = None) -> list[str]:
    if project_root is None:
        project_root = Path(__file__).parent.parent
    recipients_file = project_root / "email_recipients.txt"
    if recipients_file.exists():
        lines = recipients_file.read_text().strip().splitlines()
        return [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    env_to = os.environ.get("SMTP_TO", "")
    return [e.strip() for e in env_to.split(",") if e.strip()]


def _get_smtp_config() -> dict:
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"
    env_vars = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env_vars[key.strip()] = val.strip().strip('"').strip("'")
    return {
        "host": env_vars.get("SMTP_HOST", os.environ.get("SMTP_HOST", "smtp.gmail.com")),
        "port": int(env_vars.get("SMTP_PORT", os.environ.get("SMTP_PORT", "587"))),
        "user": env_vars.get("SMTP_USER", os.environ.get("SMTP_USER", "")),
        "password": env_vars.get("SMTP_PASSWORD", os.environ.get("SMTP_PASSWORD", "")),
        "from_addr": env_vars.get("SMTP_FROM", os.environ.get("SMTP_FROM", "")),
        "to_addrs": _load_recipients(project_root),
    }


def _clean_ticker(val) -> str:
    s = str(val).strip() if val is not None else ""
    if s.upper() in ("NAN", "SYMBOL", "N/A", "NA", "NONE", "TBD", ""):
        return ""
    if len(s) < 2:
        return ""
    return s


def _days_since(date_str: str, today: datetime) -> str:
    try:
        dt = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(dt):
            return ""
        delta = (today - dt).days
        return str(delta)
    except Exception:
        return ""


def _expected_effective(form: str, filing_date: str, eff_date: str) -> str:
    if eff_date and str(eff_date).strip() and str(eff_date) != "nan":
        return str(eff_date).strip()
    form_upper = str(form).upper()
    if form_upper.startswith("485A"):
        try:
            dt = pd.to_datetime(filing_date, errors="coerce")
            if not pd.isna(dt):
                return (dt + timedelta(days=75)).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""


def _esc(val) -> str:
    return html_mod.escape(str(val)) if val is not None else ""


def _status_color(status: str) -> str:
    return {
        "EFFECTIVE": _GREEN,
        "PENDING": _ORANGE,
        "DELAYED": _RED,
    }.get(status.upper(), _GRAY)


def _status_badge(status: str) -> str:
    color = _status_color(status)
    return (
        f'<span style="display:inline-block;padding:2px 10px;border-radius:12px;'
        f'font-size:12px;font-weight:600;color:{_WHITE};background:{color};">'
        f'{_esc(status)}</span>'
    )


def _rex_badge() -> str:
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;'
        f'font-size:11px;font-weight:600;color:{_WHITE};background:{_BLUE};'
        f'margin-left:6px;">REX</span>'
    )


def build_digest_html(
    output_dir: Path,
    dashboard_url: str = "",
    since_date: str | None = None,
) -> str:
    today = datetime.now()
    if not since_date:
        since_date = today.strftime("%Y-%m-%d")

    # --- Load data ---
    all_status = []
    all_names = []
    for folder in sorted(output_dir.iterdir()):
        if not folder.is_dir():
            continue
        for f4 in folder.glob("*_4_Fund_Status.csv"):
            all_status.append(pd.read_csv(f4, dtype=str))
        for f5 in folder.glob("*_5_Name_History.csv"):
            all_names.append(pd.read_csv(f5, dtype=str))

    df_all = pd.concat(all_status, ignore_index=True) if all_status else pd.DataFrame()
    df_names = pd.concat(all_names, ignore_index=True) if all_names else pd.DataFrame()

    if not df_all.empty and "Ticker" in df_all.columns:
        df_all["Ticker"] = df_all["Ticker"].apply(_clean_ticker)

    # Derived fields
    if not df_all.empty:
        df_all["Days Since Filing"] = df_all["Latest Filing Date"].apply(lambda x: _days_since(x, today))
        df_all["Expected Effective"] = df_all.apply(
            lambda r: _expected_effective(r.get("Latest Form", ""), r.get("Latest Filing Date", ""), r.get("Effective Date", "")),
            axis=1,
        )

    # Compute sections
    new_filings = pd.DataFrame()
    if not df_all.empty and "Latest Filing Date" in df_all.columns:
        date_mask = df_all["Latest Filing Date"].fillna("") >= since_date
        form_mask = df_all["Latest Form"].fillna("").str.upper().str.startswith("485")
        new_filings = df_all[date_mask & form_mask]

    newly_effective = pd.DataFrame()
    if not df_all.empty:
        eff_mask = (
            (df_all["Status"] == "EFFECTIVE")
            & (df_all["Effective Date"].fillna("") >= since_date)
        )
        newly_effective = df_all[eff_mask]

    name_changes = pd.DataFrame()
    changed_count = 0
    if not df_names.empty:
        multi = df_names.groupby("Series ID").size()
        changed_sids = multi[multi > 1].index
        name_changes = df_names[df_names["Series ID"].isin(changed_sids)]
        changed_count = name_changes["Series ID"].nunique()

    total = len(df_all) if not df_all.empty else 0
    eff_count = len(df_all[df_all["Status"] == "EFFECTIVE"]) if not df_all.empty else 0
    pend_count = len(df_all[df_all["Status"] == "PENDING"]) if not df_all.empty else 0
    delay_count = len(df_all[df_all["Status"] == "DELAYED"]) if not df_all.empty else 0
    trusts = sorted(df_all["Trust"].unique()) if not df_all.empty else []

    # Dashboard link
    dash_link = dashboard_url or ""

    # --- Build email-safe HTML ---
    h = []
    h.append(f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ETP Filing Tracker - {today.strftime('%Y-%m-%d')}</title>
</head>
<body style="margin:0;padding:0;background:{_LIGHT};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:{_NAVY};line-height:1.5;">

<!-- Wrapper table for email clients -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{_LIGHT};">
<tr><td align="center" style="padding:20px 10px;">

<!-- Main content table -->
<table width="680" cellpadding="0" cellspacing="0" border="0" style="background:{_WHITE};border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08);">

<!-- Header -->
<tr>
<td style="background:{_NAVY};padding:24px 30px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr>
    <td style="color:{_WHITE};font-size:22px;font-weight:700;">ETP Filing Tracker</td>
    <td align="right" style="color:rgba(255,255,255,0.7);font-size:13px;">{today.strftime('%A, %B %d, %Y')}</td>
  </tr>
  </table>
</td>
</tr>
""")

    # KPI Row
    h.append(f"""
<tr>
<td style="padding:20px 30px 10px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr>
    <td width="20%" align="center" style="padding:12px 8px;background:{_LIGHT};border-radius:8px;">
      <div style="font-size:28px;font-weight:700;color:{_NAVY};">{len(trusts)}</div>
      <div style="font-size:10px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.5px;">Trusts</div>
    </td>
    <td width="5%"></td>
    <td width="20%" align="center" style="padding:12px 8px;background:{_LIGHT};border-radius:8px;">
      <div style="font-size:28px;font-weight:700;color:{_NAVY};">{total}</div>
      <div style="font-size:10px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.5px;">Total Funds</div>
    </td>
    <td width="5%"></td>
    <td width="15%" align="center" style="padding:12px 8px;background:{_LIGHT};border-radius:8px;">
      <div style="font-size:28px;font-weight:700;color:{_GREEN};">{eff_count}</div>
      <div style="font-size:10px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.5px;">Effective</div>
    </td>
    <td width="5%"></td>
    <td width="15%" align="center" style="padding:12px 8px;background:{_LIGHT};border-radius:8px;">
      <div style="font-size:28px;font-weight:700;color:{_ORANGE};">{pend_count}</div>
      <div style="font-size:10px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.5px;">Pending</div>
    </td>
    <td width="5%"></td>
    <td width="15%" align="center" style="padding:12px 8px;background:{_LIGHT};border-radius:8px;">
      <div style="font-size:28px;font-weight:700;color:{_RED};">{delay_count}</div>
      <div style="font-size:10px;color:{_GRAY};text-transform:uppercase;letter-spacing:0.5px;">Delayed</div>
    </td>
  </tr>
  </table>
</td>
</tr>
""")

    # What Changed Today
    h.append(f"""
<tr>
<td style="padding:15px 30px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#e3f2fd;border-left:4px solid {_BLUE};border-radius:0 8px 8px 0;">
  <tr>
  <td style="padding:15px 20px;">
    <div style="font-size:16px;font-weight:700;color:{_NAVY};margin-bottom:6px;">What Changed ({since_date})</div>
    <table cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td style="padding-right:24px;font-size:14px;"><span style="font-size:20px;font-weight:700;color:{_BLUE};">{len(new_filings)}</span> new filings</td>
      <td style="padding-right:24px;font-size:14px;"><span style="font-size:20px;font-weight:700;color:{_GREEN};">{len(newly_effective)}</span> newly effective</td>
      <td style="padding-right:24px;font-size:14px;"><span style="font-size:20px;font-weight:700;color:{_ORANGE};">{changed_count}</span> name changes</td>
    </tr>
    </table>
  </td>
  </tr>
  </table>
</td>
</tr>
""")

    # === REX TRUSTS SECTION ===
    rex_data = []
    for rex_trust in sorted(_REX_TRUSTS):
        if df_all.empty:
            continue
        rex_df = df_all[df_all["Trust"] == rex_trust]
        if not rex_df.empty:
            rex_data.append((rex_trust, rex_df))

    if rex_data:
        h.append(f"""
<tr>
<td style="padding:10px 30px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:2px solid {_ORANGE};border-radius:8px;overflow:hidden;">
  <tr><td style="background:{_ORANGE};padding:10px 16px;color:{_WHITE};font-size:16px;font-weight:700;">
    REX Trusts
  </td></tr>
""")
        for rex_trust, rex_df in rex_data:
            rex_eff = len(rex_df[rex_df["Status"] == "EFFECTIVE"])
            rex_pend = len(rex_df[rex_df["Status"] == "PENDING"])
            rex_delay = len(rex_df[rex_df["Status"] == "DELAYED"])

            h.append(f"""
  <tr><td style="padding:12px 16px;border-bottom:1px solid {_BORDER};">
    <div style="font-size:15px;font-weight:700;color:{_NAVY};margin-bottom:4px;">{_esc(rex_trust)}</div>
    <div style="font-size:13px;color:{_GRAY};margin-bottom:8px;">
      {len(rex_df)} funds:
      <span style="color:{_GREEN};font-weight:600;">{rex_eff} effective</span> |
      <span style="color:{_ORANGE};font-weight:600;">{rex_pend} pending</span> |
      <span style="color:{_RED};font-weight:600;">{rex_delay} delayed</span>
    </div>
""")
            # Show pending/delayed funds for REX (most actionable)
            rex_action = rex_df[rex_df["Status"].isin(["PENDING", "DELAYED"])]
            if not rex_action.empty:
                h.append(_build_email_table(rex_action, today, show_trust=False))
            # Show last 5 effective too
            rex_eff_df = rex_df[rex_df["Status"] == "EFFECTIVE"].head(5)
            if not rex_eff_df.empty:
                h.append(f'<div style="font-size:12px;color:{_GRAY};margin:8px 0 4px;font-weight:600;">Recent Effective:</div>')
                h.append(_build_email_table(rex_eff_df, today, show_trust=False, compact=True))

            h.append("  </td></tr>")

        h.append("  </table>\n</td>\n</tr>")

    # === NEW PROSPECTUS FILINGS ===
    if not new_filings.empty:
        h.append(f"""
<tr>
<td style="padding:15px 30px 5px;">
  <div style="font-size:18px;font-weight:700;color:{_NAVY};border-bottom:2px solid {_NAVY};padding-bottom:6px;margin-bottom:10px;">
    New Prospectus Filings
    <span style="display:inline-block;background:{_NAVY};color:{_WHITE};padding:2px 10px;border-radius:12px;font-size:12px;margin-left:8px;">{len(new_filings)}</span>
  </div>
  <div style="font-size:11px;color:{_GRAY};margin-bottom:8px;">485APOS / 485BPOS / 485BXT filings since {since_date}</div>
</td>
</tr>
<tr>
<td style="padding:0 30px 15px;">
{_build_email_table(new_filings.head(30), today, show_trust=True)}
</td>
</tr>
""")

    # === NEWLY EFFECTIVE ===
    if not newly_effective.empty:
        h.append(f"""
<tr>
<td style="padding:15px 30px 5px;">
  <div style="font-size:18px;font-weight:700;color:{_NAVY};border-bottom:2px solid {_NAVY};padding-bottom:6px;margin-bottom:10px;">
    Newly Effective
    <span style="display:inline-block;background:{_GREEN};color:{_WHITE};padding:2px 10px;border-radius:12px;font-size:12px;margin-left:8px;">{len(newly_effective)}</span>
  </div>
</td>
</tr>
<tr>
<td style="padding:0 30px 15px;">
{_build_email_table(newly_effective.head(30), today, show_trust=True)}
</td>
</tr>
""")

    # === NAME CHANGES ===
    if changed_count > 0:
        h.append(f"""
<tr>
<td style="padding:15px 30px 5px;">
  <div style="font-size:18px;font-weight:700;color:{_NAVY};border-bottom:2px solid {_NAVY};padding-bottom:6px;margin-bottom:10px;">
    Name Changes
    <span style="display:inline-block;background:{_ORANGE};color:{_WHITE};padding:2px 10px;border-radius:12px;font-size:12px;margin-left:8px;">{changed_count}</span>
  </div>
</td>
</tr>
<tr>
<td style="padding:0 30px 15px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:13px;">
  <tr>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">Series ID</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">Old Name</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">New Name</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">Date</th>
  </tr>
""")
        for sid in list(name_changes["Series ID"].unique())[:20]:
            rows = name_changes[name_changes["Series ID"] == sid].sort_values("First Seen Date")
            if len(rows) >= 2:
                h.append(f"""  <tr>
    <td style="padding:6px 10px;border-bottom:1px solid {_BORDER};font-size:12px;color:{_GRAY};">{_esc(sid)}</td>
    <td style="padding:6px 10px;border-bottom:1px solid {_BORDER};font-size:12px;">{_esc(rows.iloc[0]["Name"])}</td>
    <td style="padding:6px 10px;border-bottom:1px solid {_BORDER};font-size:12px;font-weight:600;">{_esc(rows.iloc[-1]["Name"])}</td>
    <td style="padding:6px 10px;border-bottom:1px solid {_BORDER};font-size:12px;">{_esc(rows.iloc[-1]["First Seen Date"])}</td>
  </tr>""")

        h.append("  </table>\n</td>\n</tr>")

    # === PER-TRUST SUMMARY ===
    h.append(f"""
<tr>
<td style="padding:15px 30px 5px;">
  <div style="font-size:18px;font-weight:700;color:{_NAVY};border-bottom:2px solid {_NAVY};padding-bottom:6px;margin-bottom:10px;">
    Trust Overview
    <span style="display:inline-block;background:{_NAVY};color:{_WHITE};padding:2px 10px;border-radius:12px;font-size:12px;margin-left:8px;">{len(trusts)} trusts</span>
  </div>
</td>
</tr>
<tr>
<td style="padding:0 30px 15px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:13px;">
  <tr>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">Trust</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:center;font-size:11px;text-transform:uppercase;">Funds</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:center;font-size:11px;text-transform:uppercase;">Effective</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:center;font-size:11px;text-transform:uppercase;">Pending</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:center;font-size:11px;text-transform:uppercase;">Delayed</th>
    <th style="background:{_NAVY};color:{_WHITE};padding:8px 10px;text-align:left;font-size:11px;text-transform:uppercase;">Latest Filing</th>
  </tr>
""")
    for trust_name in trusts:
        t_df = df_all[df_all["Trust"] == trust_name]
        t_eff = len(t_df[t_df["Status"] == "EFFECTIVE"])
        t_pend = len(t_df[t_df["Status"] == "PENDING"])
        t_delay = len(t_df[t_df["Status"] == "DELAYED"])
        is_rex = trust_name in _REX_TRUSTS

        # Latest filing info
        latest_date = ""
        latest_form = ""
        if "Latest Filing Date" in t_df.columns:
            dates = pd.to_datetime(t_df["Latest Filing Date"], errors="coerce")
            valid = dates.dropna()
            if not valid.empty:
                idx = valid.idxmax()
                latest_date = str(t_df.loc[idx, "Latest Filing Date"])
                latest_form = str(t_df.loc[idx, "Latest Form"])

        rex_html = _rex_badge() if is_rex else ""
        bg = "#fff8e1" if is_rex else _WHITE

        h.append(f"""  <tr style="background:{bg};">
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};font-weight:600;">{_esc(trust_name)}{rex_html}</td>
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};text-align:center;">{len(t_df)}</td>
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};text-align:center;color:{_GREEN};font-weight:600;">{t_eff}</td>
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};text-align:center;color:{_ORANGE};font-weight:600;">{t_pend}</td>
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};text-align:center;color:{_RED};font-weight:600;">{t_delay}</td>
    <td style="padding:8px 10px;border-bottom:1px solid {_BORDER};font-size:12px;color:{_GRAY};">{_esc(latest_form)} ({_esc(latest_date)})</td>
  </tr>""")

    h.append("  </table>\n</td>\n</tr>")

    # === DASHBOARD CTA ===
    if dash_link:
        h.append(f"""
<tr>
<td style="padding:20px 30px;" align="center">
  <table cellpadding="0" cellspacing="0" border="0">
  <tr>
    <td style="background:{_BLUE};border-radius:8px;padding:14px 32px;">
      <a href="{_esc(dash_link)}" style="color:{_WHITE};text-decoration:none;font-size:15px;font-weight:600;">Open Dashboard</a>
    </td>
  </tr>
  </table>
  <div style="font-size:12px;color:{_GRAY};margin-top:8px;">Full fund details, AI analysis, and search available on the dashboard</div>
</td>
</tr>
""")

    # Footer
    h.append(f"""
<tr>
<td style="padding:20px 30px;border-top:1px solid {_BORDER};">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr>
    <td style="font-size:11px;color:{_GRAY};">
      Generated by ETP Filing Tracker | {today.strftime('%Y-%m-%d %H:%M')}
    </td>
    <td align="right" style="font-size:11px;color:{_GRAY};">
      {len(trusts)} trusts | {total} funds
    </td>
  </tr>
  </table>
</td>
</tr>

</table><!-- /main content -->
</td></tr>
</table><!-- /wrapper -->

</body></html>""")

    return "\n".join(h)


def _build_email_table(df: pd.DataFrame, today: datetime, show_trust: bool = False, compact: bool = False) -> str:
    """Build an email-safe table for a set of funds."""
    # Sort: pending first, then by filing date descending
    status_order = {"PENDING": 0, "DELAYED": 1, "EFFECTIVE": 2, "UNKNOWN": 3}
    sort_df = df.copy()
    sort_df["_status_sort"] = sort_df["Status"].map(status_order).fillna(3)
    sort_df["_fdt_sort"] = pd.to_datetime(sort_df["Latest Filing Date"], errors="coerce")
    sort_df = sort_df.sort_values(["_status_sort", "_fdt_sort"], ascending=[True, False])

    h = []
    font_size = "11px" if compact else "13px"
    th_pad = "6px 8px" if compact else "8px 10px"
    td_pad = "4px 8px" if compact else "6px 10px"

    h.append(f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:{font_size};">')
    h.append("<tr>")
    if show_trust:
        h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Trust</th>')
    h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Fund Name</th>')
    h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Ticker</th>')
    h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:center;font-size:10px;text-transform:uppercase;">Status</th>')
    h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Form</th>')
    h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Filed</th>')
    if not compact:
        h.append(f'<th style="background:{_NAVY};color:{_WHITE};padding:{th_pad};text-align:left;font-size:10px;text-transform:uppercase;">Exp. Effective</th>')
    h.append("</tr>")

    for _, r in sort_df.iterrows():
        name = _esc(r.get("Fund Name", ""))
        ticker = _clean_ticker(r.get("Ticker", ""))
        status = str(r.get("Status", ""))
        form = str(r.get("Latest Form", ""))
        filing_date = str(r.get("Latest Filing Date", ""))
        link = str(r.get("Prospectus Link", ""))
        expected = str(r.get("Expected Effective", "")) if not compact else ""
        trust_name = str(r.get("Trust", "")) if show_trust else ""

        # Form link
        if link and link != "nan":
            form_html = f'<a href="{_esc(link)}" style="color:{_BLUE};text-decoration:none;">{_esc(form)}</a>'
        else:
            form_html = _esc(form)

        # Expected effective with color
        if expected and expected != "nan":
            try:
                exp_dt = pd.to_datetime(expected, errors="coerce")
                if not pd.isna(exp_dt):
                    days_until = (exp_dt - today).days
                    if days_until <= 0:
                        exp_html = f'<span style="color:{_GREEN};font-weight:600;">{_esc(expected)}</span>'
                    elif days_until <= 14:
                        exp_html = f'<span style="color:{_ORANGE};font-weight:600;">{_esc(expected)} ({days_until}d)</span>'
                    else:
                        exp_html = f'{_esc(expected)} ({days_until}d)'
                else:
                    exp_html = _esc(expected)
            except Exception:
                exp_html = _esc(expected)
        else:
            exp_html = ""

        h.append("<tr>")
        if show_trust:
            h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};font-size:11px;color:{_GRAY};">{_esc(trust_name)}</td>')
        # Truncate long fund names in email
        display_name = name if len(name) <= 50 else name[:47] + "..."
        h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};">{display_name}</td>')
        h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};font-family:monospace;font-weight:600;">{_esc(ticker)}</td>')
        h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};text-align:center;">{_status_badge(status)}</td>')
        h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};">{form_html}</td>')
        h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};white-space:nowrap;">{_esc(filing_date)}</td>')
        if not compact:
            h.append(f'<td style="padding:{td_pad};border-bottom:1px solid {_BORDER};">{exp_html}</td>')
        h.append("</tr>")

    h.append("</table>")
    return "\n".join(h)


def send_digest_email(
    output_dir: Path,
    dashboard_url: str = "",
    since_date: str | None = None,
) -> bool:
    """
    Build and send the daily digest email.
    Tries Azure Graph API first, falls back to SMTP.
    Returns True if sent successfully.
    """
    recipients = _load_recipients()
    if not recipients:
        print("No recipients configured. Add emails to email_recipients.txt.")
        return False

    html_body = build_digest_html(output_dir, dashboard_url, since_date)
    subject = f"ETP Filing Tracker - Daily Digest ({datetime.now().strftime('%Y-%m-%d')})"

    # --- Try Azure Graph API first ---
    try:
        from webapp.services.graph_email import is_configured, send_email
        if is_configured():
            print("  Sending via Azure Graph API...")
            if send_email(subject=subject, html_body=html_body, recipients=recipients):
                print(f"  Digest sent via Azure to {', '.join(recipients)}")
                return True
            else:
                print("  Azure Graph API failed. Trying SMTP fallback...")
    except ImportError:
        pass  # webapp not installed, skip Azure

    # --- Fall back to SMTP ---
    config = _get_smtp_config()
    if not config["user"] or not config["password"] or not config["from_addr"]:
        print("Neither Azure nor SMTP configured.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config["from_addr"]
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(config["host"], config["port"]) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(config["user"], config["password"])
            server.sendmail(config["from_addr"], recipients, msg.as_string())
        print(f"  Digest sent via SMTP to {', '.join(recipients)}")
        return True
    except Exception as e:
        print(f"  Failed to send email: {e}")
        return False
