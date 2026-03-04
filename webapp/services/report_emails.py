"""
Executive-ready HTML email builders for Bloomberg weekly reports (L&I, CC/Income, SS).

Each builder returns (html_str, images_list) where:
- html_str: Complete HTML email body
- images_list: List of (content_id, png_bytes, filename) for CID inline images

Sections per email:
  1. Header + Date (via _wrap_email)
  2. KPI Banner (5-6 colored metric boxes)
  3. AUM Breakdown Chart (matplotlib donut via CID)
  4. REX Spotlight (top 8 flagship products, transposed)
  5. Flow Analysis (stacked bar + diverging bar)
  6. Provider / Category Breakdown Table
  7. Top 10 Inflows + Top 10 Outflows (with returns)
  8. Notable Mentions (narrative callouts)
  9. CTA + Footer (via _wrap_email)

Charts: matplotlib PNG via CID for donut/pie, table-based HTML for bars.
"""
from __future__ import annotations

import io
import logging
import math
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Email-safe colors
# ---------------------------------------------------------------------------
_NAVY = "#1a1a2e"
_TEAL = "#00897B"
_GREEN = "#27ae60"
_RED = "#e74c3c"
_BLUE = "#0984e3"
_GRAY = "#636e72"
_LIGHT = "#f8f9fa"
_BORDER = "#dee2e6"
_WHITE = "#ffffff"
_ORANGE = "#e67e22"
_REX_ROW_BG = "#e8f5e9"

_CHART_PALETTE = [
    "#1E40AF", "#059669", "#7C3AED", "#D97706", "#0891B2",
    "#E11D48", "#65A30D", "#9333EA", "#DC2626", "#0D9488",
]

_CHART_PALETTE_LIGHT = [
    "#DBEAFE", "#D1FAE5", "#EDE9FE", "#FEF3C7", "#CFFAFE",
    "#FCE7F3", "#ECFCCB", "#F3E8FF", "#FEE2E2", "#CCFBF1",
]


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _flow_color(val: float) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return _GRAY
    return _GREEN if val >= 0 else _RED


def _fmt_currency(val: float) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "$0"
    if abs(val) >= 1_000:
        return f"${val / 1_000:,.1f}B"
    if abs(val) >= 1:
        return f"${val:,.1f}M"
    return f"${val:.2f}M"


def _fmt_flow(val: float) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "$0"
    sign = "+" if val >= 0 else ""
    return f"{sign}{_fmt_currency(val)}"


def _fmt_pct(val: float) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "0.0%"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


def _fmt_pct_nosign(val: float) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "0.0%"
    return f"{val:.1f}%"


def _is_valid_date(date_str: str) -> bool:
    if not date_str:
        return False
    try:
        for fmt in ("%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.year >= 2020
            except ValueError:
                continue
    except Exception:
        pass
    return False


def _yesterday() -> datetime:
    return datetime.now() - timedelta(days=1)


def _data_date_str(data: dict, fmt: str = "%B %d, %Y") -> str:
    full = data.get("data_as_of", "")
    if _is_valid_date(full):
        return full
    short = data.get("data_as_of_short", "")
    if _is_valid_date(short):
        try:
            return datetime.strptime(short, "%m/%d/%Y").strftime(fmt)
        except ValueError:
            pass
    return _yesterday().strftime(fmt)


def _data_date_short(data: dict) -> str:
    short = data.get("data_as_of_short", "")
    if _is_valid_date(short):
        return short
    full = data.get("data_as_of", "")
    if _is_valid_date(full):
        try:
            return datetime.strptime(full, "%B %d, %Y").strftime("%m/%d/%Y")
        except ValueError:
            pass
    return _yesterday().strftime("%m/%d/%Y")


# ---------------------------------------------------------------------------
# Matplotlib donut chart generator
# ---------------------------------------------------------------------------
def _make_donut_chart(
    labels: list[str],
    values: list[float],
    title: str = "",
    colors: list[str] | None = None,
    width: int = 520,
    height: int = 280,
) -> bytes | None:
    """Generate a donut chart as PNG bytes using matplotlib.

    Returns None if matplotlib is unavailable or data is empty.
    """
    if not labels or not values or sum(values) <= 0:
        return None

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyBboxPatch
    except ImportError:
        log.warning("matplotlib not available, skipping donut chart")
        return None

    dpi = 96
    fig_w = width / dpi
    fig_h = height / dpi

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_facecolor("#ffffff")

    if colors is None:
        colors = _CHART_PALETTE[:len(labels)]
    if len(colors) < len(labels):
        colors = colors + _CHART_PALETTE[:len(labels) - len(colors)]

    # Sort by value descending for visual clarity
    paired = sorted(zip(values, labels, colors), reverse=True)
    values_s = [p[0] for p in paired]
    labels_s = [p[1] for p in paired]
    colors_s = [p[2] for p in paired]

    total = sum(values_s)

    wedges, texts = ax.pie(
        values_s,
        labels=None,
        colors=colors_s,
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.42, "edgecolor": "white", "linewidth": 1.5},
        pctdistance=0.78,
    )

    # Center text: total AUM
    center_text = _fmt_currency(total)
    ax.text(0, 0.06, center_text, ha="center", va="center",
            fontsize=16, fontweight="bold", color=_NAVY)
    ax.text(0, -0.12, "T O T A L", ha="center", va="center",
            fontsize=6.5, color=_GRAY, fontweight="600")

    # Legend on the right
    legend_items = []
    for i, (val, label, color) in enumerate(zip(values_s, labels_s, colors_s)):
        pct = val / total * 100 if total > 0 else 0
        legend_items.append(f"{label}  {_fmt_currency(val)}  ({pct:.0f}%)")

    legend = ax.legend(
        wedges, legend_items,
        loc="center left",
        bbox_to_anchor=(1.0, 0.5),
        fontsize=7.5,
        frameon=False,
        handlelength=1.0,
        handleheight=1.0,
        labelspacing=0.5,
    )
    for text in legend.get_texts():
        text.set_color(_NAVY)

    if title:
        ax.set_title(title, fontsize=11, fontweight="bold", color=_NAVY,
                      pad=8, loc="left")

    ax.set_aspect("equal")
    plt.tight_layout(pad=0.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor="#ffffff",
                edgecolor="none", dpi=dpi)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Email envelope
# ---------------------------------------------------------------------------
def _wrap_email(title: str, accent: str, body: str,
                dashboard_url: str = "", date_str: str = "") -> str:
    if not date_str:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%B %d, %Y")
    dash_link = _esc(dashboard_url) if dashboard_url else ""

    cta = ""
    if dash_link:
        cta = f"""
<tr><td style="padding:20px 30px;text-align:center;">
  <a href="{dash_link}/reports/" style="display:inline-block;padding:12px 28px;
    background:{accent};color:{_WHITE};text-decoration:none;border-radius:6px;
    font-weight:600;font-size:14px;">View Interactive Report</a>
</td></tr>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{_esc(title)}</title></head>
<body style="margin:0;padding:0;background:{_LIGHT};
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  color:{_NAVY};line-height:1.5;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{_LIGHT};">
<tr><td align="center" style="padding:20px 10px;">
<table width="660" cellpadding="0" cellspacing="0" border="0"
  style="background:{_WHITE};border-radius:12px;overflow:hidden;
  box-shadow:0 2px 8px rgba(0,0,0,0.08);">

<!-- Header -->
<tr><td style="background:{accent};padding:24px 30px;">
  <div style="font-size:22px;font-weight:700;color:{_WHITE};letter-spacing:-0.5px;">{_esc(title)}</div>
  <div style="font-size:12px;color:rgba(255,255,255,0.8);margin-top:4px;">{_esc(date_str)}</div>
</td></tr>

{body}
{cta}

<!-- Footer -->
<tr><td style="padding:16px 30px;border-top:1px solid {_BORDER};text-align:center;">
  <div style="font-size:11px;color:{_GRAY};">
    REX Financial Intelligence Hub &middot; Data sourced from Bloomberg L.P. and REX Shares, LLC
  </div>
</td></tr>

</table></td></tr></table></body></html>"""


# ---------------------------------------------------------------------------
# Shared section renderers
# ---------------------------------------------------------------------------
def _kpi_row(kpis: list[tuple[str, str, str]]) -> str:
    n = len(kpis)
    width = int(100 / n) if n else 25
    cells = []
    for label, value, color in kpis:
        cells.append(
            f'<td width="{width}%" style="padding:12px 6px;background:{_LIGHT};'
            f'border-radius:8px;text-align:center;">'
            f'<div style="font-size:22px;font-weight:700;color:{color};">{_esc(value)}</div>'
            f'<div style="font-size:9px;color:{_GRAY};text-transform:uppercase;'
            f'letter-spacing:0.5px;margin-top:2px;">{_esc(label)}</div></td>'
        )
    return (
        '<tr><td style="padding:15px 30px 5px;">'
        '<table width="100%" cellpadding="0" cellspacing="6" border="0">'
        f'<tr>{"".join(cells)}</tr>'
        '</table></td></tr>'
    )


def _section_title(title: str, accent: str = _TEAL) -> str:
    return (
        f'<tr><td style="padding:18px 30px 5px;">'
        f'<div style="font-size:16px;font-weight:700;color:{_NAVY};margin:0 0 8px 0;'
        f'padding-bottom:6px;border-bottom:2px solid {accent};">{_esc(title)}</div>'
        f'</td></tr>'
    )


def _sub_heading(title: str) -> str:
    return (
        f'<tr><td style="padding:10px 30px 2px;">'
        f'<div style="font-size:13px;font-weight:700;color:{_NAVY};">{_esc(title)}</div>'
        f'</td></tr>'
    )


def _table(headers: list[str], rows: list[list[str]], align: list[str] | None = None,
           highlight_col: int | None = None, bold_last_row: bool = False,
           rex_rows: set[int] | None = None) -> str:
    if not rows:
        return '<tr><td style="padding:10px 30px;color:#636e72;font-size:13px;">No data available.</td></tr>'

    n = len(headers)
    if align is None:
        align = ["left"] * n

    _th = (f"padding:8px 10px;background:{_LIGHT};font-size:10px;color:{_GRAY};"
           f"text-transform:uppercase;letter-spacing:0.5px;font-weight:600;"
           f"border-bottom:2px solid {_BORDER};")
    _td = f"padding:6px 10px;font-size:12px;color:{_NAVY};border-bottom:1px solid {_BORDER};"

    header_cells = "".join(
        f'<th style="{_th}text-align:{align[i]};">{_esc(h)}</th>'
        for i, h in enumerate(headers)
    )

    body_rows = []
    for ri, row in enumerate(rows):
        is_bold = bold_last_row and ri == len(rows) - 1
        is_rex = rex_rows and ri in rex_rows
        cells = []
        for i, val in enumerate(row):
            style = _td + f"text-align:{align[i]};"
            if is_bold:
                style += "font-weight:700;"
            if is_rex:
                style += f"background:{_REX_ROW_BG};"
            if highlight_col is not None and i == highlight_col:
                try:
                    fval = float(str(val).replace("$", "").replace(",", "")
                                 .replace("+", "").replace("B", "e3").replace("M", ""))
                except (ValueError, AttributeError):
                    fval = 0
                if fval > 0:
                    style += f"color:{_GREEN};"
                elif fval < 0:
                    style += f"color:{_RED};"
            cells.append(f'<td style="{style}">{_esc(str(val))}</td>')
        body_rows.append(f'<tr>{"".join(cells)}</tr>')

    return (
        '<tr><td style="padding:5px 30px 10px;">'
        '<table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
        f'<thead><tr>{header_cells}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></td></tr>'
    )


def _rex_spotlight(rex_funds: list[dict], accent: str = _GREEN,
                   label: str = "REX Spotlight") -> str:
    """Transposed REX product table -- top 8 flagship products only."""
    if not rex_funds:
        return ""
    # Top 8 by AUM
    sorted_rex = sorted(rex_funds, key=lambda f: f.get("aum", 0), reverse=True)[:8]
    headers = ["Metric"] + [f["ticker"] for f in sorted_rex]
    aligns = ["left"] + ["right"] * len(sorted_rex)
    aum_row = ["AUM"] + [f["aum_fmt"] for f in sorted_rex]
    flow_1w_row = ["1W Flow"] + [f.get("flow_1w_fmt", "--") for f in sorted_rex]
    flow_1m_row = ["1M Flow"] + [f.get("flow_1m_fmt", "--") for f in sorted_rex]
    rows = [aum_row, flow_1w_row, flow_1m_row]
    if any(f.get("yield_fmt") and f.get("yield_val", 0) for f in sorted_rex):
        yield_row = ["Yield"] + [f.get("yield_fmt", "--") for f in sorted_rex]
        rows.append(yield_row)
    return _section_title(label, accent) + _table(headers, rows, aligns)


# ---------------------------------------------------------------------------
# Email-safe chart helpers (table-based HTML -- universal cross-client)
# ---------------------------------------------------------------------------
def _render_stacked_bar(segments: list[tuple[str, float, str]], total_label: str = "") -> str:
    """Horizontal stacked bar with legend. segments = [(name, value, color), ...]."""
    total = sum(v for _, v, _ in segments) or 1
    bar_cells = []
    for name, val, color in segments:
        pct = val / total * 100
        if pct < 0.5:
            continue
        bar_cells.append(
            f'<td style="background:{color};height:18px;width:{pct:.1f}%;'
            f'font-size:1px;line-height:18px;">&nbsp;</td>'
        )
    legend_rows = []
    for name, val, color in segments:
        pct = val / total * 100
        if pct < 0.5:
            continue
        legend_rows.append(
            f'<tr>'
            f'<td style="padding:3px 6px;width:14px;">'
            f'<div style="width:10px;height:10px;background:{color};border-radius:2px;"></div></td>'
            f'<td style="padding:3px 6px;font-size:11px;font-weight:600;">{_esc(name)}</td>'
            f'<td style="padding:3px 6px;font-size:11px;text-align:right;">{_fmt_currency(val)}</td>'
            f'<td style="padding:3px 6px;font-size:10px;text-align:right;color:{_GRAY};">{pct:.0f}%</td>'
            f'</tr>'
        )
    if not bar_cells:
        return ""
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="border-radius:6px;overflow:hidden;border-collapse:collapse;">'
        f'<tr>{"".join(bar_cells)}</tr></table>'
        f'<div style="font-size:15px;font-weight:700;color:{_NAVY};text-align:center;'
        f'margin-top:6px;">{_esc(total_label)}'
        f'<span style="font-size:9px;color:{_GRAY};font-weight:400;margin-left:4px;">'
        f'TOTAL AUM</span></div>'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:6px;">'
        f'{"".join(legend_rows)}</table>'
    )


def _render_bar_chart(title: str, items: list[tuple[str, float]], subtitle: str = "") -> str:
    """Horizontal bar chart. items = [(label, value), ...]."""
    if not items:
        return ""
    max_abs = max(abs(v) for _, v in items) if items else 1
    if max_abs == 0:
        max_abs = 1
    sub_html = (f'<div style="font-size:12px;color:{_GRAY};margin-bottom:8px;">'
                f'{_esc(subtitle)}</div>') if subtitle else ""
    rows = []
    for i, (label, val) in enumerate(items):
        color = _CHART_PALETTE[i % len(_CHART_PALETTE)]
        bar_width = max(abs(val) / max_abs * 100, 2)
        val_fmt = _fmt_currency(val)
        rows.append(
            f'<tr>'
            f'<td style="padding:4px 8px;font-size:12px;font-weight:600;width:130px;'
            f'white-space:nowrap;">{_esc(label)}</td>'
            f'<td style="padding:4px 8px;">'
            f'<div style="background:{_LIGHT};border-radius:4px;overflow:hidden;">'
            f'<div style="background:{color};height:18px;width:{bar_width:.1f}%;'
            f'border-radius:4px;min-width:4px;"></div>'
            f'</div></td>'
            f'<td style="padding:4px 8px;font-size:12px;text-align:right;width:80px;'
            f'font-weight:600;">{val_fmt}</td>'
            f'</tr>'
        )
    return (
        f'<div style="font-size:14px;font-weight:700;color:{_NAVY};margin-bottom:6px;">{_esc(title)}</div>'
        f'{sub_html}'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{"".join(rows)}</table>'
    )


def _render_diverging_bar_chart(title: str, items: list[tuple[str, float]],
                                subtitle: str = "") -> str:
    """Diverging horizontal bar chart: bars grow left/right from center."""
    if not items:
        return ""
    max_abs = max(abs(v) for _, v in items) if items else 1
    if max_abs == 0:
        max_abs = 1
    sub_html = (
        f'<div style="font-size:12px;color:{_GRAY};margin-bottom:8px;">{_esc(subtitle)}</div>'
        if subtitle else ""
    )
    rows = []
    for label, val in items:
        bar_pct = abs(val) / max_abs * 50
        val_fmt = _fmt_flow(val)
        val_color = _flow_color(val)
        bar_color = _BLUE if val >= 0 else _RED
        _bar_h = "height:16px;font-size:1px;line-height:16px;"
        if val < 0:
            left_empty = 50 - bar_pct
            bar_html = (
                f'<td style="width:{left_empty:.1f}%;{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:{bar_pct:.1f}%;background:{bar_color};'
                f'{_bar_h}border-radius:3px 0 0 3px;padding:0;">&nbsp;</td>'
                f'<td style="width:2px;background:{_BORDER};{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:50%;{_bar_h}padding:0;">&nbsp;</td>'
            )
        elif val > 0:
            right_empty = 50 - bar_pct
            bar_html = (
                f'<td style="width:50%;{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:2px;background:{_BORDER};{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:{bar_pct:.1f}%;background:{bar_color};'
                f'{_bar_h}border-radius:0 3px 3px 0;padding:0;">&nbsp;</td>'
                f'<td style="width:{right_empty:.1f}%;{_bar_h}padding:0;">&nbsp;</td>'
            )
        else:
            bar_html = (
                f'<td style="width:50%;{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:2px;background:{_BORDER};{_bar_h}padding:0;">&nbsp;</td>'
                f'<td style="width:50%;{_bar_h}padding:0;">&nbsp;</td>'
            )
        rows.append(
            f'<tr>'
            f'<td style="padding:4px 6px;font-size:12px;font-weight:600;width:100px;'
            f'white-space:nowrap;">{_esc(label)}</td>'
            f'<td style="padding:4px 0;">'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'<tr>{bar_html}</tr></table></td>'
            f'<td style="padding:4px 6px;font-size:12px;text-align:right;width:80px;'
            f'font-weight:600;color:{val_color};white-space:nowrap;">{val_fmt}</td>'
            f'</tr>'
        )
    return (
        f'<div style="font-size:14px;font-weight:700;color:{_NAVY};margin-bottom:6px;">{_esc(title)}</div>'
        f'{sub_html}'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{"".join(rows)}</table>'
    )


def _chart_section(*charts_html: str) -> str:
    """Wrap 1-2 charts in a padded section."""
    inner = "".join(c for c in charts_html if c)
    if not inner:
        return ""
    return f'<tr><td style="padding:15px 30px;">{inner}</td></tr>'


def _cid_image(cid: str, width: int = 520, alt: str = "Chart") -> str:
    """Generate an img tag referencing a CID inline image."""
    return (f'<img src="cid:{cid}" width="{width}" alt="{_esc(alt)}" '
            f'style="display:block;max-width:100%;height:auto;margin:0 auto;'
            f'border-radius:8px;" />')


# ---------------------------------------------------------------------------
# Notable Mentions generator
# ---------------------------------------------------------------------------
def _notable_mentions(data: dict, report_type: str) -> str:
    """Generate narrative callouts from report data."""
    bullets = []

    if report_type == "li":
        providers = data.get("providers", [])
        top10 = data.get("top10", [])
        bottom10 = data.get("bottom10", [])
        kpis = data.get("kpis", {})

        # Biggest weekly inflow fund
        if top10:
            top = top10[0]
            bullets.append(
                f'<b>Top Weekly Inflow:</b> {_esc(top["ticker"])} '
                f'({_esc(top["fund_name"][:40])}) attracted '
                f'<span style="color:{_GREEN};font-weight:700;">{_esc(top["flow_1w_fmt"])}</span> this week'
            )

        # Biggest weekly outflow fund
        if bottom10:
            bot = bottom10[0]
            bullets.append(
                f'<b>Top Weekly Outflow:</b> {_esc(bot["ticker"])} '
                f'({_esc(bot["fund_name"][:40])}) saw '
                f'<span style="color:{_RED};font-weight:700;">{_esc(bot["flow_1w_fmt"])}</span> this week'
            )

        # Top provider by flow
        if providers:
            top_provider = max(providers, key=lambda p: p["flow_1w"])
            if top_provider["flow_1w"] > 0:
                bullets.append(
                    f'<b>Leading Provider:</b> {_esc(top_provider["issuer"])} led with '
                    f'{_esc(top_provider["flow_1w_fmt"])} in weekly net flows '
                    f'({_fmt_pct_nosign(top_provider["market_share"])} market share)'
                )

        # REX positioning
        rex_funds = data.get("rex_funds", [])
        if rex_funds:
            rex_aum = sum(f["aum"] for f in rex_funds)
            kpi_aum = kpis.get("total_aum", "$0")
            bullets.append(
                f'<b>REX Position:</b> {len(rex_funds)} REX L&I products with '
                f'{_fmt_currency(rex_aum)} combined AUM'
            )

    elif report_type == "cc":
        issuers = data.get("issuers", [])
        top_flow = data.get("top_flow_segments", {}).get("All", [])
        top_yield = data.get("top_yield_segments", {}).get("All", [])
        kpis = data.get("kpis", {})

        # Top inflow fund
        if top_flow:
            top = top_flow[0]
            bullets.append(
                f'<b>Top Monthly Inflow:</b> {_esc(top["ticker"])} '
                f'({_esc(top["issuer"][:25])}) attracted '
                f'<span style="color:{_GREEN};font-weight:700;">{_esc(top["flow_1m_fmt"])}</span> over 1M'
            )

        # Highest yield fund
        if top_yield:
            top = top_yield[0]
            bullets.append(
                f'<b>Highest Yield:</b> {_esc(top["ticker"])} at '
                f'<span style="color:{_TEAL};font-weight:700;">{_esc(top["yield_fmt"])}</span> '
                f'({_esc(top["issuer"][:25])})'
            )

        # Top issuer by AUM
        if issuers:
            top_iss = issuers[0]
            bullets.append(
                f'<b>Market Leader:</b> {_esc(top_iss["issuer"])} holds '
                f'{_fmt_pct_nosign(top_iss["market_share"])} market share '
                f'({_esc(top_iss["aum_fmt"])} AUM, {top_iss["count"]} funds)'
            )

        # REX positioning
        rex_funds = data.get("rex_funds", [])
        if rex_funds:
            rex_aum = sum(f["aum"] for f in rex_funds)
            bullets.append(
                f'<b>REX Position:</b> {len(rex_funds)} REX income products with '
                f'{_fmt_currency(rex_aum)} combined AUM'
            )

    elif report_type == "ss":
        underlier_summary = data.get("underlier_summary", [])
        top10 = data.get("top10", [])
        bottom10 = data.get("bottom10", [])
        kpis = data.get("kpis", {})

        # Largest underlier
        if underlier_summary:
            top_u = underlier_summary[0]
            bullets.append(
                f'<b>Top Underlier:</b> {_esc(top_u["underlier"])} with '
                f'{top_u["count"]} ETFs and {_esc(top_u["aum_fmt"])} in AUM '
                f'({_fmt_pct_nosign(top_u["market_share"])} of SS market)'
            )

        # Biggest weekly inflow fund
        if top10:
            top = top10[0]
            bullets.append(
                f'<b>Top Weekly Inflow:</b> {_esc(top["ticker"])} '
                f'({_esc(top.get("product_type", ""))}) attracted '
                f'<span style="color:{_GREEN};font-weight:700;">{_esc(top["flow_1w_fmt"])}</span>'
            )

        # Category split insight
        num_lev = kpis.get("num_leveraged", 0)
        num_cc = kpis.get("num_cc", 0)
        if num_lev and num_cc:
            bullets.append(
                f'<b>Category Split:</b> {num_lev} leveraged + {num_cc} covered call '
                f'single-stock ETFs ({kpis.get("aum_leveraged", "$0")} lev / '
                f'{kpis.get("aum_cc", "$0")} CC AUM)'
            )

        # REX positioning
        rex_funds = data.get("rex_funds", [])
        if rex_funds:
            rex_aum = sum(f["aum"] for f in rex_funds)
            bullets.append(
                f'<b>REX Position:</b> {len(rex_funds)} REX single-stock products with '
                f'{_fmt_currency(rex_aum)} combined AUM'
            )

    if not bullets:
        return ""

    bullet_html = "".join(
        f'<tr><td style="padding:5px 8px;font-size:12px;color:{_NAVY};line-height:1.6;">'
        f'<span style="color:{_TEAL};font-weight:700;margin-right:6px;">&#9679;</span>'
        f'{b}</td></tr>'
        for b in bullets
    )

    return (
        _section_title("Notable Mentions", _TEAL) +
        f'<tr><td style="padding:5px 30px 15px;">'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="background:{_LIGHT};border-radius:8px;padding:12px;">'
        f'{bullet_html}</table></td></tr>'
    )


# ---------------------------------------------------------------------------
# L&I Report Email
# ---------------------------------------------------------------------------
def build_li_email(dashboard_url: str = "", db=None) -> tuple[str, list]:
    """Build executive-ready email for U.S. Leveraged & Inverse ETP Report.

    Returns (html, images) where images = [(cid, png_bytes, filename), ...].
    """
    from webapp.services.report_data import get_li_report
    data = get_li_report(db)

    date_str = _data_date_str(data)
    date_short = _data_date_short(data)
    title = f"U.S. Leveraged & Inverse ETF Report: {date_short}"
    images: list[tuple[str, bytes, str]] = []

    if not data.get("available") or not data.get("kpis"):
        html = _wrap_email(title, _TEAL,
                           '<tr><td style="padding:20px 30px;">Bloomberg data not available.</td></tr>',
                           dashboard_url, date_str)
        return html, images

    kpis = data["kpis"]
    body = ""

    # --- 1. KPI Banner ---
    kpi_items = [
        ("Total ETPs", str(kpis.get("count", 0)), _NAVY),
        ("Total AUM", kpis.get("total_aum", "$0"), _NAVY),
        ("1W Net Flow", kpis.get("flow_1w", "$0"), _GREEN if kpis.get("flow_1w_positive", True) else _RED),
        ("YTD Net Flow", kpis.get("flow_ytd", "$0"), _GREEN if kpis.get("flow_ytd_positive", True) else _RED),
    ]
    wow = kpis.get("aum_change_1w", "")
    if wow:
        kpi_items.insert(2, ("AUM WoW", wow, _GREEN if kpis.get("aum_change_positive", True) else _RED))
    body += _kpi_row(kpi_items)

    # --- 2. AUM Breakdown Chart (donut via CID) ---
    providers = data.get("providers", [])
    top_providers = providers[:8]
    other_aum = sum(p["aum"] for p in providers[8:]) if len(providers) > 8 else 0

    donut_labels = [p["issuer"][:20] for p in top_providers]
    donut_values = [p["aum"] for p in top_providers]
    if other_aum > 0:
        donut_labels.append("Other")
        donut_values.append(other_aum)

    donut_bytes = _make_donut_chart(donut_labels, donut_values, "AUM by Provider")
    if donut_bytes:
        images.append(("li_aum_donut", donut_bytes, "li_aum_donut.png"))
        body += f'<tr><td style="padding:15px 30px 5px;text-align:center;">{_cid_image("li_aum_donut")}</td></tr>'
    else:
        # Fallback: HTML stacked bar
        segments = [(p["issuer"][:20], p["aum"], _CHART_PALETTE[i % len(_CHART_PALETTE)])
                    for i, p in enumerate(top_providers)]
        if other_aum > 0:
            segments.append(("Other", other_aum, _GRAY))
        total_aum_val = sum(p["aum"] for p in providers)
        body += _chart_section(_render_stacked_bar(segments, _fmt_currency(total_aum_val)))

    # --- 3. REX Spotlight ---
    body += _rex_spotlight(data.get("rex_funds", []), _GREEN, "REX Leveraged & Inverse Spotlight")

    # --- 4. Flow Analysis Charts ---
    # Diverging bar: 1W Flow by provider (top 10 by absolute flow)
    flow_items = sorted(providers, key=lambda p: abs(p["flow_1w"]), reverse=True)[:10]
    diverging_html = _render_diverging_bar_chart(
        "1W Net Flow by Provider",
        [(p["issuer"][:22], p["flow_1w"]) for p in flow_items],
    )
    body += _chart_section(diverging_html)

    # --- 5. Provider Summary Table ---
    body += _section_title("Provider Summary")
    total_row = data.get("total_row")
    has_split = total_row and "num_leveraged" in total_row

    if has_split:
        headers = ["Provider", "# Lev", "# Inv", "# Total",
                   "AUM Lev", "AUM Inv", "AUM Total",
                   "1W Flow", "Share"]
        aligns = ["left", "right", "right", "right",
                  "right", "right", "right", "right", "right"]
        rows = []
        rex_idxs = set()
        for p in providers[:20]:
            ri = len(rows)
            if p.get("is_rex"):
                rex_idxs.add(ri)
            rows.append([
                p["issuer"],
                str(p.get("num_leveraged", "")),
                str(p.get("num_inverse", "")),
                str(p["count"]),
                p.get("aum_leveraged_fmt", ""),
                p.get("aum_inverse_fmt", ""),
                p["aum_fmt"],
                p["flow_1w_fmt"],
                f'{p["market_share"]:.1f}%',
            ])
        if total_row:
            rows.append([
                "TOTAL",
                str(total_row.get("num_leveraged", "")),
                str(total_row.get("num_inverse", "")),
                str(total_row["count"]),
                total_row.get("aum_leveraged_fmt", ""),
                total_row.get("aum_inverse_fmt", ""),
                total_row["aum_fmt"],
                total_row["flow_1w_fmt"],
                "100.0%",
            ])
        body += _table(headers, rows, aligns, highlight_col=7,
                       bold_last_row=True, rex_rows=rex_idxs)
    else:
        headers = ["Provider", "# ETPs", "AUM", "1W Flow", "1M Flow", "YTD Flow", "Share"]
        aligns = ["left", "right", "right", "right", "right", "right", "right"]
        rows = []
        rex_idxs = set()
        for p in providers[:20]:
            ri = len(rows)
            if p.get("is_rex"):
                rex_idxs.add(ri)
            rows.append([
                p["issuer"], str(p["count"]), p["aum_fmt"],
                p["flow_1w_fmt"], p["flow_1m_fmt"], p["flow_ytd_fmt"],
                f'{p["market_share"]:.1f}%',
            ])
        if total_row:
            rows.append([
                "TOTAL", str(total_row["count"]), total_row["aum_fmt"],
                total_row["flow_1w_fmt"], total_row["flow_1m_fmt"], total_row["flow_ytd_fmt"],
                "100.0%",
            ])
        body += _table(headers, rows, aligns, highlight_col=3,
                       bold_last_row=True, rex_rows=rex_idxs)

    # --- 6. Top Movers ---
    body += _section_title("Top 10 Weekly Inflows", _GREEN)
    headers = ["Ticker", "Fund Name", "Type", "AUM", "1W Flow", "1W Ret"]
    aligns = ["left", "left", "left", "right", "right", "right"]
    rows = []
    for f in data.get("top10", []):
        rows.append([
            f["ticker"],
            f["fund_name"][:32],
            f.get("product_type", ""),
            f["aum_fmt"],
            f["flow_1w_fmt"],
            f.get("return_1w_fmt", ""),
        ])
    body += _table(headers, rows, aligns, highlight_col=4)

    body += _section_title("Top 10 Weekly Outflows", _RED)
    rows = []
    for f in data.get("bottom10", []):
        rows.append([
            f["ticker"],
            f["fund_name"][:32],
            f.get("product_type", ""),
            f["aum_fmt"],
            f["flow_1w_fmt"],
            f.get("return_1w_fmt", ""),
        ])
    body += _table(headers, rows, aligns, highlight_col=4)

    # --- 7. Notable Mentions ---
    body += _notable_mentions(data, "li")

    return _wrap_email(title, _TEAL, body, dashboard_url, date_str), images


# ---------------------------------------------------------------------------
# Income (Covered Call) Report Email
# ---------------------------------------------------------------------------
def build_cc_email(dashboard_url: str = "", db=None) -> tuple[str, list]:
    """Build executive-ready email for Income (Covered Call) ETFs report.

    Returns (html, images) where images = [(cid, png_bytes, filename), ...].
    """
    from webapp.services.report_data import get_cc_report
    data = get_cc_report(db)

    date_str = _data_date_str(data)
    date_short = _data_date_short(data)
    title = f"Income ETF Report: {date_short}"
    images: list[tuple[str, bytes, str]] = []

    if not data.get("available") or not data.get("kpis"):
        html = _wrap_email(title, _BLUE,
                           '<tr><td style="padding:20px 30px;">Bloomberg data not available.</td></tr>',
                           dashboard_url, date_str)
        return html, images

    kpis = data["kpis"]
    body = ""

    # --- 1. KPI Banner ---
    kpi_items = [
        ("Total Funds", str(kpis.get("count", 0)), _NAVY),
        ("Total AUM", kpis.get("total_aum", "$0"), _NAVY),
        ("1W Net Flow", kpis.get("flow_1w", "$0"), _GREEN if kpis.get("flow_1w_positive", True) else _RED),
        ("Avg Yield", kpis.get("avg_yield", "0.0%"), _TEAL),
    ]
    wow = kpis.get("aum_change_1w", "")
    if wow:
        kpi_items.insert(2, ("AUM WoW", wow, _GREEN if kpis.get("aum_change_positive", True) else _RED))
    body += _kpi_row(kpi_items)

    # --- 2. AUM Breakdown Chart (donut: by category) ---
    aum_by_cat = data.get("aum_by_category", [])
    if aum_by_cat:
        cat_labels = [c["category"] for c in aum_by_cat]
        cat_values = [c["aum"] for c in aum_by_cat]
        donut_bytes = _make_donut_chart(cat_labels, cat_values, "AUM by Category")
        if donut_bytes:
            images.append(("cc_aum_donut", donut_bytes, "cc_aum_donut.png"))
            body += f'<tr><td style="padding:15px 30px 5px;text-align:center;">{_cid_image("cc_aum_donut")}</td></tr>'
        else:
            # Fallback: stacked bar
            cat_segments = [(c["category"], c["aum"], _CHART_PALETTE[i % len(_CHART_PALETTE)])
                            for i, c in enumerate(aum_by_cat)]
            total_aum_val = sum(c["aum"] for c in aum_by_cat) if aum_by_cat else 0
            body += _chart_section(_render_stacked_bar(cat_segments, _fmt_currency(total_aum_val)))

    # --- 3. REX Spotlight ---
    body += _rex_spotlight(data.get("rex_funds", []), _GREEN, "REX Income Spotlight")

    # --- 4. Flow & Issuer Analysis ---
    issuers = data.get("issuers", [])

    # Bar chart: Top 10 issuers by AUM
    bar_items = [(iss["issuer"][:22], iss["aum"]) for iss in issuers[:10]]
    bar_html = _render_bar_chart("Top 10 Issuers by AUM", bar_items)

    # Diverging bar: Top 10 issuers by 1W flow
    flow_items = sorted(issuers, key=lambda i: abs(i["flow_1w"]), reverse=True)[:10]
    diverging_html = _render_diverging_bar_chart(
        "1W Net Flow by Issuer",
        [(i["issuer"][:22], i["flow_1w"]) for i in flow_items],
    )
    body += _chart_section(bar_html, diverging_html)

    # --- 5. AUM by Category Table ---
    if aum_by_cat:
        body += _section_title("AUM by Category")
        headers = ["Category", "# Funds", "AUM", "1W Flow", "1M Flow", "Share"]
        aligns = ["left", "right", "right", "right", "right", "right"]
        rows = []
        for c in aum_by_cat:
            rows.append([
                c["category"], str(c["count"]), c["aum_fmt"],
                c["flow_1w_fmt"], c["flow_1m_fmt"],
                f'{c["market_share"]:.1f}%',
            ])
        body += _table(headers, rows, aligns, highlight_col=3)

    # --- 6. Issuer Ranking ---
    if issuers:
        body += _section_title("Issuer Ranking")
        headers = ["Rank", "Issuer", "# Funds", "AUM", "1W Flow", "1M Flow", "Share"]
        aligns = ["right", "left", "right", "right", "right", "right", "right"]
        rows = []
        rex_idxs = set()
        for i, iss in enumerate(issuers[:20]):
            if iss.get("market_share", 0) < 0.05 and i > 12:
                break
            ri = len(rows)
            if "REX" in iss["issuer"].upper() or "rex" in iss["issuer"].lower():
                rex_idxs.add(ri)
            rows.append([
                str(i + 1),
                iss["issuer"][:28],
                str(iss["count"]),
                iss["aum_fmt"],
                iss["flow_1w_fmt"],
                iss["flow_1m_fmt"],
                f'{iss["market_share"]:.1f}%',
            ])
        body += _table(headers, rows, aligns, highlight_col=4, rex_rows=rex_idxs)

    # --- 7. Top Movers ---
    top_flow = data.get("top_flow_segments", {})
    all_flow = top_flow.get("All", [])
    if all_flow:
        body += _section_title("Top 10 by Monthly Inflows", _GREEN)
        headers = ["Ticker", "Fund Name", "Issuer", "AUM", "1W Flow", "1M Flow"]
        aligns = ["left", "left", "left", "right", "right", "right"]
        rows = []
        for f in all_flow[:10]:
            rows.append([f["ticker"], f["fund_name"][:28], f["issuer"][:22],
                         f["aum_fmt"], f.get("flow_1w_fmt", "--"), f["flow_1m_fmt"]])
        body += _table(headers, rows, aligns, highlight_col=5)

    top_yield = data.get("top_yield_segments", {})
    all_yield = top_yield.get("All", [])
    if all_yield:
        body += _section_title("Top 10 by Distribution Rate", _ORANGE)
        headers = ["Ticker", "Fund Name", "Issuer", "AUM", "Yield", "1W Ret"]
        aligns = ["left", "left", "left", "right", "right", "right"]
        rows = []
        for f in all_yield[:10]:
            rows.append([f["ticker"], f["fund_name"][:28], f["issuer"][:22],
                         f["aum_fmt"], f["yield_fmt"], f.get("return_1w_fmt", "")])
        body += _table(headers, rows, aligns)

    # --- 8. Notable Mentions ---
    body += _notable_mentions(data, "cc")

    return _wrap_email(title, _BLUE, body, dashboard_url, date_str), images


# ---------------------------------------------------------------------------
# Single-Stock Report Email
# ---------------------------------------------------------------------------
def build_ss_email(dashboard_url: str = "", db=None) -> tuple[str, list]:
    """Build executive-ready email for Single-Stock ETF Report.

    Returns (html, images) where images = [(cid, png_bytes, filename), ...].
    """
    from webapp.services.report_data import get_ss_report
    data = get_ss_report(db)

    date_str = _data_date_str(data)
    date_short = _data_date_short(data)
    title = f"Single-Stock ETF Report: {date_short}"
    images: list[tuple[str, bytes, str]] = []

    if not data.get("available") or not data.get("kpis"):
        html = _wrap_email(title, _NAVY,
                           '<tr><td style="padding:20px 30px;">Bloomberg data not available.</td></tr>',
                           dashboard_url, date_str)
        return html, images

    kpis = data["kpis"]
    body = ""

    # --- 1. KPI Banner ---
    kpi_items = [
        ("SS ETFs", str(kpis.get("count", 0)), _NAVY),
        ("Total AUM", kpis.get("total_aum", "$0"), _NAVY),
        ("Leveraged", f'{kpis.get("num_leveraged", 0)} / {kpis.get("aum_leveraged", "$0")}', _TEAL),
        ("Covered Call", f'{kpis.get("num_cc", 0)} / {kpis.get("aum_cc", "$0")}', _BLUE),
    ]
    wow = kpis.get("aum_change_1w", "")
    if wow:
        kpi_items.insert(2, ("AUM WoW", wow, _GREEN if kpis.get("aum_change_positive", True) else _RED))
    body += _kpi_row(kpi_items)

    # --- 2. AUM Breakdown Chart (donut: by provider) ---
    providers = data.get("providers", [])
    top_providers = providers[:8]
    other_aum = sum(p["aum"] for p in providers[8:]) if len(providers) > 8 else 0

    donut_labels = [p["issuer"][:20] for p in top_providers]
    donut_values = [p["aum"] for p in top_providers]
    if other_aum > 0:
        donut_labels.append("Other")
        donut_values.append(other_aum)

    donut_bytes = _make_donut_chart(donut_labels, donut_values, "AUM by Provider")
    if donut_bytes:
        images.append(("ss_aum_donut", donut_bytes, "ss_aum_donut.png"))
        body += f'<tr><td style="padding:15px 30px 5px;text-align:center;">{_cid_image("ss_aum_donut")}</td></tr>'
    else:
        # Fallback: stacked bar for Leveraged vs CC split
        aum_lev = 0
        aum_cc = 0
        try:
            for s, target in [(kpis.get("aum_leveraged", "$0"), "lev"),
                              (kpis.get("aum_cc", "$0"), "cc")]:
                v = s.replace("$", "").replace(",", "").replace("+", "")
                if "B" in v:
                    v = float(v.replace("B", "")) * 1000
                elif "M" in v:
                    v = float(v.replace("M", ""))
                else:
                    v = float(v) if v else 0
                if target == "lev":
                    aum_lev = v
                else:
                    aum_cc = v
        except (ValueError, AttributeError):
            pass
        ss_segments = []
        if aum_lev > 0:
            ss_segments.append(("Leveraged", aum_lev, _TEAL))
        if aum_cc > 0:
            ss_segments.append(("Covered Call", aum_cc, _BLUE))
        if ss_segments:
            body += _chart_section(
                _render_stacked_bar(ss_segments, kpis.get("total_aum", "$0"))
            )

    # --- 3. REX Spotlight ---
    body += _rex_spotlight(data.get("rex_funds", []), _GREEN, "REX Single-Stock Spotlight")

    # --- 4. Flow Analysis ---
    underlier_summary = data.get("underlier_summary", [])

    # Diverging bar: 1W Flow by underlier (top 10)
    flow_items = sorted(underlier_summary, key=lambda u: abs(u["flow_1w"]), reverse=True)[:10]
    diverging_html = _render_diverging_bar_chart(
        "1W Net Flow by Underlier",
        [(u["underlier"][:15], u["flow_1w"]) for u in flow_items],
    ) if flow_items else ""

    # Bar chart: Top 10 underliers by AUM
    bar_items = [(u["underlier"][:15], u["aum"]) for u in underlier_summary[:10]]
    bar_html = _render_bar_chart("Top Underliers by AUM", bar_items) if bar_items else ""

    body += _chart_section(bar_html, diverging_html)

    # --- 5. Underlier Summary ---
    if underlier_summary:
        body += _section_title("Underlier Summary")
        headers = ["Underlier", "# ETFs", "AUM", "1W Flow", "Share"]
        aligns = ["left", "right", "right", "right", "right"]
        rows = []
        for u in underlier_summary:
            rows.append([
                u["underlier"], str(u["count"]), u["aum_fmt"],
                u["flow_1w_fmt"], f'{u["market_share"]:.1f}%',
            ])
        body += _table(headers, rows, aligns, highlight_col=3)

    # --- 6. Provider Summary ---
    if providers:
        body += _section_title("Provider Summary")
        headers = ["Provider", "# ETFs", "AUM", "1W Flow", "1M Flow", "Share"]
        aligns = ["left", "right", "right", "right", "right", "right"]
        rows = []
        rex_idxs = set()
        for p in providers[:15]:
            ri = len(rows)
            if p.get("is_rex"):
                rex_idxs.add(ri)
            rows.append([
                p["issuer"], str(p["count"]), p["aum_fmt"],
                p["flow_1w_fmt"], p["flow_1m_fmt"],
                f'{p["market_share"]:.1f}%',
            ])
        body += _table(headers, rows, aligns, highlight_col=3, rex_rows=rex_idxs)

    # --- 7. Top Movers ---
    if data.get("top10"):
        body += _section_title("Top 10 Weekly Inflows", _GREEN)
        headers = ["Ticker", "Fund Name", "Type", "AUM", "1W Flow", "1W Ret"]
        aligns = ["left", "left", "left", "right", "right", "right"]
        rows = []
        for f in data["top10"]:
            rows.append([f["ticker"], f["fund_name"][:30],
                         f.get("product_type", ""),
                         f["aum_fmt"], f["flow_1w_fmt"],
                         f.get("return_1w_fmt", "")])
        body += _table(headers, rows, aligns, highlight_col=4)

    if data.get("bottom10"):
        body += _section_title("Top 10 Weekly Outflows", _RED)
        headers = ["Ticker", "Fund Name", "Type", "AUM", "1W Flow", "1W Ret"]
        aligns = ["left", "left", "left", "right", "right", "right"]
        rows = []
        for f in data["bottom10"]:
            rows.append([f["ticker"], f["fund_name"][:30],
                         f.get("product_type", ""),
                         f["aum_fmt"], f["flow_1w_fmt"],
                         f.get("return_1w_fmt", "")])
        body += _table(headers, rows, aligns, highlight_col=4)

    # --- 8. Notable Mentions ---
    body += _notable_mentions(data, "ss")

    return _wrap_email(title, _NAVY, body, dashboard_url, date_str), images


# ---------------------------------------------------------------------------
# Preview helper: convert CID refs to data URIs for browser rendering
# ---------------------------------------------------------------------------
def cid_to_data_uri(html: str, images: list[tuple[str, bytes, str]]) -> str:
    """Replace cid: references with data: URIs for browser preview."""
    import base64
    for cid, png_bytes, _ in images:
        b64 = base64.b64encode(png_bytes).decode()
        html = html.replace(f"cid:{cid}", f"data:image/png;base64,{b64}")
    return html
