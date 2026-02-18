"""PDF generator for Pre-IPO Leveraged ETF Filing Landscape Report.

Sections:
  Cover Page
  1. Executive Summary (KPIs, key findings)
  2. Competitor Pre-IPO Filings (every filing on private companies)
  3. Top 2026-2027 IPO Candidates (ranked by valuation)
  4. Recently IPO'd - Active Leveraged Products (CoreWeave, Reddit, ARM, etc.)
"""
from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colors (brand consistency with 3x report)
# ---------------------------------------------------------------------------
NAVY = colors.HexColor("#1a1a2e")
BLUE = colors.HexColor("#0984e3")
GREEN = colors.HexColor("#27ae60")
ORANGE = colors.HexColor("#e67e22")
RED = colors.HexColor("#e74c3c")
LIGHT_BG = colors.HexColor("#f5f7fa")
LIGHT_GREEN = colors.HexColor("#e8f5e9")
LIGHT_ORANGE = colors.HexColor("#fff3e0")
LIGHT_RED = colors.HexColor("#ffebee")
LIGHT_BLUE = colors.HexColor("#e3f2fd")
BORDER = colors.HexColor("#cccccc")
PURPLE = colors.HexColor("#8e44ad")
GOLD = colors.HexColor("#f39c12")

TW = 518  # usable table width


def _build_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("CoverTitle", parent=styles["Title"],
        fontSize=28, textColor=NAVY, spaceAfter=4, alignment=1))
    styles.add(ParagraphStyle("CoverSub", parent=styles["Normal"],
        fontSize=14, textColor=BLUE, alignment=1, spaceAfter=4))
    styles.add(ParagraphStyle("SectionHead", parent=styles["Heading2"],
        fontSize=14, textColor=NAVY, spaceBefore=16, spaceAfter=8,
        borderWidth=1, borderColor=NAVY, borderPadding=4))
    styles.add(ParagraphStyle("SubHead", parent=styles["Heading3"],
        fontSize=11, textColor=BLUE, spaceBefore=10, spaceAfter=4))
    styles.add(ParagraphStyle("Body", parent=styles["Normal"],
        fontSize=9, leading=12, spaceAfter=4))
    styles.add(ParagraphStyle("SmallNote", parent=styles["Normal"],
        fontSize=7, textColor=colors.grey, leading=9))
    styles.add(ParagraphStyle("CellWrap", parent=styles["Normal"],
        fontSize=7, leading=9, wordWrap="CJK"))
    styles.add(ParagraphStyle("KPI", parent=styles["Normal"],
        fontSize=18, textColor=NAVY, alignment=1, leading=22))
    styles.add(ParagraphStyle("KPILabel", parent=styles["Normal"],
        fontSize=7, textColor=colors.grey, alignment=1))
    styles.add(ParagraphStyle("BulletBody", parent=styles["Normal"],
        fontSize=9, leading=13, spaceAfter=2, leftIndent=12,
        bulletIndent=0, bulletFontSize=9))
    return styles


def _table_style(header_color=NAVY):
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, 0), 7),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ])


def _paginated_table(story, styles, rows, header, col_widths, row_fn,
                     rows_per_page=28, color_fn=None, header_color=NAVY):
    for page_start in range(0, len(rows), rows_per_page):
        chunk = rows[page_start:page_start + rows_per_page]
        data = [header]
        for i, r in enumerate(chunk):
            data.append(row_fn(page_start + i, r))
        t = Table(data, colWidths=col_widths)
        ts = _table_style(header_color)
        if color_fn:
            for i, r in enumerate(chunk):
                color_fn(i, r, ts)
        t.setStyle(ts)
        story.append(t)
        if page_start + rows_per_page < len(rows):
            story.append(PageBreak())
            story.append(Paragraph("(continued)", styles["SubHead"]))
            story.append(Spacer(1, 4))
    story.append(Spacer(1, 6))


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

# IPO target definitions
IPO_TARGETS = {
    "spacex": {"company": "SpaceX + xAI", "sector": "Aerospace / AI", "valuation": "$1.25T+", "timing": "H2 2026", "ipo_status": "Confirmed", "private": True, "val_sort": 1250},
    "openai": {"company": "OpenAI", "sector": "AI / Software", "valuation": "$800B-$1T", "timing": "Q4 2026", "ipo_status": "Preparing", "private": True, "val_sort": 900},
    "bytedance": {"company": "ByteDance", "sector": "Social Media", "valuation": "~$500B", "timing": "2027+", "ipo_status": "Unlikely near-term", "private": True, "val_sort": 500},
    "anthropic": {"company": "Anthropic", "sector": "AI / Software", "valuation": "$380B", "timing": "Late 2026", "ipo_status": "Exploring", "private": True, "val_sort": 380},
    "databricks": {"company": "Databricks", "sector": "Data / AI / Cloud", "valuation": "~$134B", "timing": "H2 2026", "ipo_status": "Expected", "private": True, "val_sort": 134},
    "stripe": {"company": "Stripe", "sector": "Fintech", "valuation": "~$120B", "timing": "2027+", "ipo_status": "No rush", "private": True, "val_sort": 120},
    "revolut": {"company": "Revolut", "sector": "Fintech", "valuation": "$75-90B", "timing": "2027-2028", "ipo_status": "Drifting out", "private": True, "val_sort": 82},
    "shein": {"company": "Shein", "sector": "E-commerce", "valuation": "~$66B", "timing": "2026 (HK)", "ipo_status": "Filed (HK)", "private": True, "val_sort": 66},
    "epic": {"company": "Epic Games", "sector": "Gaming", "valuation": "~$57B", "timing": "2026 possible", "ipo_status": "Unconfirmed", "private": True, "val_sort": 57},
    "canva": {"company": "Canva", "sector": "Design / SaaS", "valuation": "~$42B", "timing": "2026", "ipo_status": "Likely", "private": True, "val_sort": 42},
    "anduril": {"company": "Anduril", "sector": "Defense Tech", "valuation": "~$30.5B", "timing": "2026-2027", "ipo_status": "No S-1", "private": True, "val_sort": 30},
    "fanatics": {"company": "Fanatics", "sector": "Sports Commerce", "valuation": "~$21B", "timing": "2026", "ipo_status": "S-1 Filed", "private": True, "val_sort": 21},
    "discord": {"company": "Discord", "sector": "Social / Gaming", "valuation": "$7-15B", "timing": "March 2026", "ipo_status": "S-1 Filed", "private": True, "val_sort": 11},
    "cerebras": {"company": "Cerebras", "sector": "AI Chips", "valuation": "$8-22B", "timing": "Q2 2026", "ipo_status": "Refiling S-1", "private": True, "val_sort": 15},
    "plaid": {"company": "Plaid", "sector": "Fintech", "valuation": "$6-10B", "timing": "Mid-Late 2026", "ipo_status": "No S-1", "private": True, "val_sort": 8},
    # Recently IPO'd
    "coreweave": {"company": "CoreWeave", "sector": "AI / Cloud GPU", "valuation": "$23B IPO", "timing": "Mar 28, 2025", "ipo_status": "PUBLIC", "private": False, "ticker": "CRWV", "val_sort": 23},
    "reddit": {"company": "Reddit", "sector": "Social Media", "valuation": "$6.5B IPO", "timing": "Mar 21, 2024", "ipo_status": "PUBLIC", "private": False, "ticker": "RDDT", "val_sort": 6.5},
    "rivian": {"company": "Rivian", "sector": "EV / Auto", "valuation": "$77B IPO", "timing": "Nov 10, 2021", "ipo_status": "PUBLIC", "private": False, "ticker": "RIVN", "val_sort": 77},
}

MATCH_ALIASES = {"crwv": "coreweave", "rddt": "reddit", "rivn": "rivian", "arm hold": "arm"}


def _collect_filings(output_root: Path = Path("outputs")) -> dict:
    """Scan pipeline outputs for filings matching IPO target names."""
    search_keys = list(IPO_TARGETS.keys()) + list(MATCH_ALIASES.keys())
    results = []

    for trust_dir in output_root.iterdir():
        if not trust_dir.is_dir():
            continue
        for csv_file in list(trust_dir.glob("*_3_*.csv")) + list(trust_dir.glob("*_4_*.csv")):
            try:
                with open(csv_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        sn = row.get("Series Name", "") or row.get("series_name", "") or row.get("fund_name", "") or ""
                        pn = row.get("Prospectus Name", "") or ""
                        combined = (sn + " " + pn).lower()
                        for key in search_keys:
                            if key in combined:
                                canonical = MATCH_ALIASES.get(key, key)
                                results.append({
                                    "trust": trust_dir.name,
                                    "fund_name": sn,
                                    "form": row.get("Form", row.get("form", "")),
                                    "filing_date": row.get("Filing Date", row.get("filing_date", "")),
                                    "status": row.get("status", ""),
                                    "ticker": row.get("Class Symbol", row.get("ticker", "")),
                                    "match": canonical,
                                })
                                break
            except Exception:
                pass

    # Deduplicate
    seen = set()
    unique = []
    for r in results:
        key = (r["trust"], r["fund_name"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    by_company = defaultdict(list)
    for r in unique:
        by_company[r["match"]].append(r)

    return dict(by_company)


# ---------------------------------------------------------------------------
# PDF sections
# ---------------------------------------------------------------------------

def _build_cover(story, styles, report_date):
    story.append(Spacer(1, 2.5 * inch))
    story.append(Paragraph("Pre-IPO Leveraged ETF", styles["CoverTitle"]))
    story.append(Paragraph("Filing Landscape", styles["CoverTitle"]))
    story.append(Spacer(1, 0.4 * inch))
    story.append(Paragraph("REX Financial", styles["CoverSub"]))
    story.append(Paragraph(report_date, styles["CoverSub"]))
    story.append(Spacer(1, 0.5 * inch))
    divider = Table([[""]], colWidths=[3 * inch])
    divider.setStyle(TableStyle([
        ("LINEBELOW", (0, 0), (-1, -1), 2, NAVY),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    outer = Table([[divider]], colWidths=[7 * inch])
    outer.setStyle(TableStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")]))
    story.append(outer)
    story.append(Spacer(1, 0.8 * inch))
    story.append(Paragraph(
        "Competitive intelligence on leveraged ETF filings targeting pre-IPO and "
        "recently-IPO'd companies. Includes every SEC filing identified across 20 "
        "monitored trusts, plus the top anticipated IPOs of 2026-2027 ranked by valuation.",
        ParagraphStyle("CoverDesc", parent=styles["Body"], fontSize=10,
                       alignment=1, textColor=colors.grey, leading=14)))


def _build_exec_summary(story, styles, by_company, report_date):
    story.append(Paragraph("1. Executive Summary", styles["SectionHead"]))

    # Separate private vs public
    private_cos = {k: v for k, v in by_company.items() if k in IPO_TARGETS and IPO_TARGETS[k]["private"]}
    public_cos = {k: v for k, v in by_company.items() if k in IPO_TARGETS and not IPO_TARGETS[k]["private"]}

    total_private_filings = sum(len(v) for v in private_cos.values())
    total_public_filings = sum(len(v) for v in public_cos.values())
    private_issuers = set()
    for filings in private_cos.values():
        for f in filings:
            private_issuers.add(f["trust"])
    rex_private = sum(1 for k, v in private_cos.items()
                      if any("rex" in f["trust"].lower() or "opportunities" in f["trust"].lower() for f in v))

    # KPI Row
    kpi_data = [
        [Paragraph(f"<b>{len(private_cos)}</b>", styles["KPI"]),
         Paragraph(f"<b>{total_private_filings}</b>", styles["KPI"]),
         Paragraph(f"<b>{len(private_issuers)}</b>", styles["KPI"]),
         Paragraph(f"<b>{rex_private}</b>", styles["KPI"])],
        [Paragraph("Private Companies<br/>With Filings", styles["KPILabel"]),
         Paragraph("Total Pre-IPO<br/>Leveraged Filings", styles["KPILabel"]),
         Paragraph("Competitor Trusts<br/>Filing Pre-IPO", styles["KPILabel"]),
         Paragraph("REX Has Filed<br/>(of those)", styles["KPILabel"])],
    ]
    kpi = Table(kpi_data, colWidths=[TW / 4] * 4)
    kpi.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (-1, -1), 1, NAVY),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, BORDER),
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(kpi)
    story.append(Spacer(1, 12))

    # Key findings
    story.append(Paragraph("Key Findings", styles["SubHead"]))

    bullets = [
        f"<b>ProShares is most aggressive</b> -- filed \"Ultra\" 2x products on {len([k for k in private_cos if any(f['trust']=='ProShares Trust' for f in private_cos[k])])} private companies (Anthropic, ByteDance, Databricks, Discord, OpenAI, Shein, SpaceX, Stripe).",
        f"<b>Themes ETF Trust (Leverage Shares)</b> filed 2x Long AND 2x Short pairs on 6 private companies.",
        f"<b>Discord is most contested</b> -- 4 issuers have filed (incl. T-REX). S-1 filed Jan 2026, IPO expected March 2026.",
        f"<b>Databricks has 3 issuers filing</b> -- Tradr is furthest along (485BXT extensions). IPO expected H2 2026.",
        f"<b>Recently IPO'd names are already crowded</b> -- CoreWeave has {len(by_company.get('coreweave', []))} leveraged products across {len(set(f['trust'] for f in by_company.get('coreweave', [])))} trusts, including 3x and 5x products.",
        f"<b>REX has filed on Discord only</b> (T-REX 2X Long Discord). No filings on the other 7 pre-IPO targets.",
    ]
    for b in bullets:
        story.append(Paragraph(f"<bullet>&bull;</bullet> {b}", styles["BulletBody"]))

    story.append(Spacer(1, 10))
    story.append(Paragraph(f"Data sourced from SEC EDGAR pipeline across 20 monitored trusts. Report date: {report_date}.",
                           styles["SmallNote"]))


def _build_preipo_filings(story, styles, by_company):
    story.append(Paragraph("2. Competitor Pre-IPO Leveraged Filings", styles["SectionHead"]))
    story.append(Paragraph(
        "Every leveraged ETF filing identified on companies that are still private. "
        "All filings are 485APOS (initial) or 485BXT (extension) -- none are effective yet.",
        styles["Body"]))

    # Summary table first
    story.append(Paragraph("Pre-IPO Filing Summary", styles["SubHead"]))
    private_keys = sorted(
        [k for k in by_company if k in IPO_TARGETS and IPO_TARGETS[k]["private"]],
        key=lambda k: len(by_company[k]), reverse=True)

    summary_header = ["Company", "Sector", "Est. Valuation", "IPO Timing", "Issuers Filing", "Total", "REX"]
    summary_data = [summary_header]
    for key in private_keys:
        info = IPO_TARGETS[key]
        filings = by_company[key]
        trusts = sorted(set(f["trust"] for f in filings))
        issuer_names = []
        for t in trusts:
            if "ProShares" in t: issuer_names.append("ProShares")
            elif "Themes" in t: issuer_names.append("Themes")
            elif "Opportunities" in t: issuer_names.append("T-REX")
            elif "Tidal" in t: issuer_names.append("Defiance")
            elif "Investment Managers" in t: issuer_names.append("Tradr")
            else: issuer_names.append(t.split()[0])
        has_rex = any("rex" in f["trust"].lower() or "opportunities" in f["trust"].lower() for f in filings)
        summary_data.append([
            info["company"], info["sector"], info["valuation"], info["timing"],
            Paragraph(", ".join(issuer_names), styles["CellWrap"]),
            str(len(filings)),
            "Yes" if has_rex else "No",
        ])

    t = Table(summary_data, colWidths=[70, 80, 65, 60, 140, 33, 30])
    ts = _table_style()
    for i, key in enumerate(private_keys):
        has_rex = any("rex" in f["trust"].lower() or "opportunities" in f["trust"].lower() for f in by_company[key])
        color = GREEN if has_rex else RED
        ts.add("TEXTCOLOR", (6, i + 1), (6, i + 1), color)
        ts.add("FONTNAME", (6, i + 1), (6, i + 1), "Helvetica-Bold")
    t.setStyle(ts)
    story.append(t)
    story.append(Spacer(1, 14))

    # Detailed filing list per company
    story.append(Paragraph("Detailed Filing List", styles["SubHead"]))
    all_filings = []
    for key in private_keys:
        info = IPO_TARGETS[key]
        for f in by_company[key]:
            all_filings.append({
                "company": info["company"],
                "trust": f["trust"],
                "fund_name": f["fund_name"],
                "form": f["form"],
                "filing_date": f["filing_date"],
            })

    _paginated_table(
        story, styles, all_filings,
        header=["#", "Company", "Issuer Trust", "Fund Name", "Form", "Filed"],
        col_widths=[22, 62, 120, 190, 50, 58],
        row_fn=lambda i, r: [
            str(i + 1), r["company"],
            Paragraph(r["trust"], styles["CellWrap"]),
            Paragraph(r["fund_name"], styles["CellWrap"]),
            r["form"], r["filing_date"],
        ],
        rows_per_page=28,
        header_color=RED,
    )


def _build_ipo_candidates(story, styles, by_company):
    story.append(Paragraph("3. Top 2026-2027 IPO Candidates", styles["SectionHead"]))
    story.append(Paragraph(
        "The largest private companies expected to IPO, ranked by estimated valuation. "
        "Companies with existing leveraged ETF filings are highlighted.",
        styles["Body"]))

    candidates = sorted(
        [v for v in IPO_TARGETS.values() if v["private"]],
        key=lambda x: x["val_sort"], reverse=True)

    header = ["#", "Company", "Sector", "Est. Valuation", "IPO Timeline", "IPO Status", "Lev. Filings"]
    data = [header]
    filing_counts = {}
    for key, info in IPO_TARGETS.items():
        if info["private"]:
            filing_counts[info["company"]] = len(by_company.get(key, []))

    for i, c in enumerate(candidates):
        fc = filing_counts.get(c["company"], 0)
        data.append([
            str(i + 1), c["company"], c["sector"], c["valuation"],
            c["timing"], c["ipo_status"],
            f"{fc} filings" if fc > 0 else "None",
        ])

    t = Table(data, colWidths=[22, 75, 85, 75, 72, 90, 60])
    ts = _table_style(BLUE)
    for i, c in enumerate(candidates):
        fc = filing_counts.get(c["company"], 0)
        # Color the filing count column
        if fc > 0:
            ts.add("TEXTCOLOR", (6, i + 1), (6, i + 1), RED)
            ts.add("FONTNAME", (6, i + 1), (6, i + 1), "Helvetica-Bold")
            ts.add("BACKGROUND", (6, i + 1), (6, i + 1), LIGHT_RED)
        else:
            ts.add("TEXTCOLOR", (6, i + 1), (6, i + 1), GREEN)
            ts.add("FONTNAME", (6, i + 1), (6, i + 1), "Helvetica-Bold")
            ts.add("BACKGROUND", (6, i + 1), (6, i + 1), LIGHT_GREEN)
        # Highlight near-term IPOs
        if c["timing"] in ("March 2026", "Q2 2026", "H1 2026"):
            ts.add("FONTNAME", (4, i + 1), (4, i + 1), "Helvetica-Bold")
            ts.add("TEXTCOLOR", (4, i + 1), (4, i + 1), RED)
    t.setStyle(ts)
    story.append(t)
    story.append(Spacer(1, 10))

    story.append(Paragraph(
        "Red filings column = competitors have already filed. Green = no leveraged filings yet (open opportunity). "
        "Bold red timeline = IPO expected within 6 months.",
        styles["SmallNote"]))


def _build_recent_ipos(story, styles, by_company):
    story.append(Paragraph("4. Recently IPO'd -- Active Leveraged Products", styles["SectionHead"]))
    story.append(Paragraph(
        "Companies that have already IPO'd and now have active leveraged ETF products trading. "
        "This shows how quickly the market moves after an IPO -- CoreWeave had products "
        "within weeks of listing.",
        styles["Body"]))

    public_keys = [k for k in by_company if k in IPO_TARGETS and not IPO_TARGETS[k]["private"]]
    public_keys.sort(key=lambda k: len(by_company[k]), reverse=True)

    for key in public_keys:
        info = IPO_TARGETS[key]
        filings = by_company[key]
        trusts = sorted(set(f["trust"] for f in filings))
        ticker = info.get("ticker", "")

        story.append(Paragraph(
            f"{info['company']} ({ticker}) -- {info['valuation']}, IPO'd {info['timing']}",
            styles["SubHead"]))

        _paginated_table(
            story, styles, sorted(filings, key=lambda f: f["filing_date"], reverse=True),
            header=["#", "Issuer Trust", "Fund Name", "Form", "Filed"],
            col_widths=[22, 135, 230, 55, 60],
            row_fn=lambda i, r: [
                str(i + 1),
                Paragraph(r["trust"], styles["CellWrap"]),
                Paragraph(r["fund_name"], styles["CellWrap"]),
                r["form"], r["filing_date"],
            ],
            rows_per_page=28,
            header_color=GREEN,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_ipo_report(output_root: Path = Path("outputs")) -> bytes:
    """Generate the Pre-IPO Filing Landscape PDF. Returns PDF bytes."""
    by_company = _collect_filings(output_root)
    report_date = datetime.now().strftime("%B %d, %Y")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
        topMargin=0.5 * inch, bottomMargin=0.5 * inch,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch)
    styles = _build_styles()
    story = []

    _build_cover(story, styles, report_date)
    story.append(PageBreak())

    _build_exec_summary(story, styles, by_company, report_date)
    story.append(PageBreak())

    _build_preipo_filings(story, styles, by_company)
    story.append(PageBreak())

    _build_ipo_candidates(story, styles, by_company)
    story.append(PageBreak())

    _build_recent_ipos(story, styles, by_company)

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()

    log.info("IPO report generated: %d bytes, %d companies, %d total filings",
             len(pdf_bytes), len(by_company),
             sum(len(v) for v in by_company.values()))
    return pdf_bytes


def run_ipo_report(output_root: Path = Path("outputs")) -> Path:
    """Generate and save the IPO report PDF."""
    pdf_bytes = generate_ipo_report(output_root)
    ts = datetime.now().strftime("%Y%m%d")
    out_path = Path("reports") / f"Pre-IPO_Filing_Landscape_{ts}.pdf"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(pdf_bytes)
    log.info("Saved: %s (%d bytes)", out_path, len(pdf_bytes))
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    path = run_ipo_report()
    print(f"Report saved: {path}")
