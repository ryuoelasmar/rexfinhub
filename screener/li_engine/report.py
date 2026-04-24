"""Weekly L&I Recommender Report — PDF generator.

Consumes:
    - scored DataFrame from engine.score_universe(underliers_only=True)
    - IPO rows from ipo_scrape.fetch_ipos()
    - themes YAML for trend rollup

Produces a PDF at reports/li_weekly_YYYY-MM-DD.pdf.
Local only; never routed through the webapp per standing order.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from screener.li_engine.ipo_scrape import IPORow, fetch_ipos
from screener.li_engine.engine import score_universe
from screener.li_engine.weights import load_weights

log = logging.getLogger(__name__)

NAVY = colors.HexColor("#1a1a2e")
BLUE = colors.HexColor("#0984e3")
GREEN = colors.HexColor("#27ae60")
RED = colors.HexColor("#e74c3c")
LIGHT_BG = colors.HexColor("#f5f7fa")
BORDER = colors.HexColor("#cccccc")
TW = 518  # usable table width, matches existing reports

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_THEMES_PATH = Path(__file__).resolve().parent / "themes.yaml"


def _styles():
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle("H1", parent=ss["Title"], fontSize=22, textColor=NAVY, spaceAfter=6))
    ss.add(ParagraphStyle("H2", parent=ss["Heading2"], fontSize=14, textColor=NAVY, spaceBefore=16, spaceAfter=8))
    ss.add(ParagraphStyle("H3", parent=ss["Heading3"], fontSize=11, textColor=NAVY, spaceBefore=10, spaceAfter=4))
    ss.add(ParagraphStyle("Body", parent=ss["Normal"], fontSize=9, leading=12))
    ss.add(ParagraphStyle("Muted", parent=ss["Normal"], fontSize=8, leading=10, textColor=colors.grey))
    return ss


def _table(data, col_widths=None, header_bg=NAVY, zebra=True):
    t = Table(data, colWidths=col_widths, repeatRows=1)
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.25, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
    ]
    if zebra:
        for i in range(1, len(data)):
            if i % 2 == 0:
                cmds.append(("BACKGROUND", (0, i), (-1, i), LIGHT_BG))
    t.setStyle(TableStyle(cmds))
    return t


def _fmt_score(v):
    if pd.isna(v):
        return "—"
    return f"{v:.1f}"


def _fmt_sig(v):
    if pd.isna(v):
        return "—"
    return f"{v:+.2f}"


def _top_pillar_label(col: str | float) -> str:
    if pd.isna(col) or not isinstance(col, str):
        return "—"
    return col.replace("_score", "").replace("_", " ").title()


def _section_top_candidates(scored: pd.DataFrame, filter_label: str,
                            filter_mask, n: int, styles) -> list:
    """Shared builder for 'launch candidates' and 'file candidates' tables."""
    sub = scored[filter_mask].head(n)
    if sub.empty:
        return [Paragraph(f"No candidates matched the {filter_label} filter.", styles["Body"])]

    header = ["Rank", "Ticker", "Score", "Top Pillar", "Liq", "Opt", "Vol", "WS", "KR", "Sent", "Signals"]
    rows = [header]
    for i, (ticker, r) in enumerate(sub.iterrows(), start=1):
        rows.append([
            str(i),
            ticker,
            _fmt_score(r.get("final_score")),
            _top_pillar_label(r.get("top_pillar")),
            _fmt_sig(r.get("liquidity_demand_score")),
            _fmt_sig(r.get("options_demand_score")),
            _fmt_sig(r.get("volatility_score")),
            _fmt_sig(r.get("competitive_whitespace_score")),
            _fmt_sig(r.get("korean_overnight_score")),
            _fmt_sig(r.get("social_sentiment_score")),
            f"{int(r.get('n_signals', 0))}",
        ])
    widths = [30, 45, 45, 90, 38, 38, 38, 38, 38, 45, 50]
    return [_table(rows, col_widths=widths)]


def _section_ipos(ipos: list[IPORow], styles, max_rows: int = 25) -> list:
    if not ipos:
        return [Paragraph("No IPO data this week — scraper returned empty. "
                          "Check stockanalysis.com or re-run the scraper.", styles["Body"])]
    header = ["Ticker", "Company", "Exchange", "Expected / Priced", "Price Range", "Shares", "Section"]
    rows = [header]
    for r in ipos[:max_rows]:
        rows.append([
            r.ticker,
            (r.company or "")[:38],
            r.exchange or "—",
            r.expected_date or "—",
            r.price_range or "—",
            r.shares_offered or "—",
            r.source_section,
        ])
    widths = [45, 165, 50, 75, 75, 58, 50]
    return [_table(rows, col_widths=widths)]


def _section_themes(scored: pd.DataFrame, themes: dict, styles,
                    top_per_theme: int = 5) -> list:
    out = []
    theme_summaries = []
    for theme, tickers in themes.items():
        present = scored.reindex([t.upper() for t in tickers]).dropna(subset=["final_score"])
        if present.empty:
            continue
        mean_score = present["final_score"].mean()
        theme_summaries.append((theme, mean_score, present))
    theme_summaries.sort(key=lambda x: -x[1])

    if not theme_summaries:
        return [Paragraph("No theme rollups available.", styles["Body"])]

    summary_header = ["Theme", "Avg Score", "# Tickers", "Top 3"]
    summary_rows = [summary_header]
    for theme, mean_score, present in theme_summaries:
        top3 = ", ".join(present.sort_values("final_score", ascending=False).head(3).index.tolist())
        summary_rows.append([
            theme.replace("_", " ").title(),
            f"{mean_score:.1f}",
            str(len(present)),
            top3,
        ])
    out.append(_table(summary_rows, col_widths=[130, 70, 60, 258]))
    out.append(Spacer(1, 12))

    # Top per theme
    for theme, mean_score, present in theme_summaries[:6]:
        out.append(Paragraph(
            f"<b>{theme.replace('_', ' ').title()}</b> — avg {mean_score:.1f}, "
            f"{len(present)} tickers",
            styles["H3"],
        ))
        sub = present.sort_values("final_score", ascending=False).head(top_per_theme)
        sub_header = ["Ticker", "Score", "Top Pillar", "Signals", "Has REX Filing"]
        sub_rows = [sub_header]
        for ticker, r in sub.iterrows():
            sub_rows.append([
                ticker,
                _fmt_score(r.get("final_score")),
                _top_pillar_label(r.get("top_pillar")),
                f"{int(r.get('n_signals', 0))}",
                "Yes" if r.get("has_rex_filing") else "No",
            ])
        out.append(_table(sub_rows, col_widths=[50, 50, 120, 60, 80]))
        out.append(Spacer(1, 8))
    return out


def generate_weekly_report(
    scored: pd.DataFrame | None = None,
    ipos: list[IPORow] | None = None,
    themes: dict | None = None,
    output_dir: Path | None = None,
    report_date: date | None = None,
) -> Path:
    """Build the weekly PDF and return the output path."""
    if output_dir is None:
        output_dir = _PROJECT_ROOT / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    if report_date is None:
        report_date = date.today()

    if scored is None:
        scored = score_universe(underliers_only=True)

    if ipos is None:
        try:
            ipos = fetch_ipos()
        except Exception as e:
            log.warning("IPO fetch failed: %s", e)
            ipos = []

    if themes is None:
        if _THEMES_PATH.exists():
            with _THEMES_PATH.open("r", encoding="utf-8") as f:
                themes = yaml.safe_load(f).get("themes", {})
        else:
            themes = {}

    weights = load_weights()
    styles = _styles()

    out_path = output_dir / f"li_weekly_{report_date.isoformat()}.pdf"
    doc = SimpleDocTemplate(
        str(out_path), pagesize=letter,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.7 * inch, bottomMargin=0.7 * inch,
        title=f"L&I Weekly Recommender — {report_date.isoformat()}",
        author="REX Financial",
    )

    story = []

    # Cover
    story.append(Spacer(1, 60))
    story.append(Paragraph("L&amp;I Recommender", styles["H1"]))
    story.append(Paragraph("Weekly Report", styles["H2"]))
    story.append(Paragraph(report_date.strftime("%B %d, %Y"), styles["Body"]))
    story.append(Spacer(1, 20))
    story.append(Paragraph(
        f"Engine weights v{weights.version} ({weights.calibration}, created {weights.created}). "
        f"Universe: {len(scored):,} underliers. Methodology: "
        f"docs/LI_ENGINE_METHODOLOGY.md.",
        styles["Muted"],
    ))

    # Exec summary
    story.append(PageBreak())
    story.append(Paragraph("Executive Summary", styles["H2"]))
    has_filing = scored[scored["has_rex_filing"].astype(bool)] if "has_rex_filing" in scored.columns else pd.DataFrame()
    no_filing = scored[~scored["has_rex_filing"].astype(bool)] if "has_rex_filing" in scored.columns else scored
    summary_header = ["Category", "Top 3"]
    summary_rows = [
        summary_header,
        ["Launch candidates (have REX filing, not launched)", ", ".join(has_filing[~has_filing.get("has_rex_launch", pd.Series(False, index=has_filing.index)).astype(bool)].head(3).index.tolist()) or "—"],
        ["File candidates (no REX filing)", ", ".join(no_filing.head(3).index.tolist()) or "—"],
        ["IPOs this window", ", ".join(r.ticker for r in ipos[:3]) or "—"],
    ]
    story.append(_table(summary_rows, col_widths=[300, 218]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "<b>Caveat:</b> v0.1 uses contemporaneous-correlation IC as a stand-in "
        "for predictive IC pending 120+ days of dated history. Rankings describe "
        "what is currently associated with inflows, not what will cause them.",
        styles["Muted"],
    ))

    # Launch candidates
    story.append(Paragraph("Launch Candidates (existing REX filings)", styles["H2"]))
    story.append(Paragraph(
        "Underliers where we have filed but not launched. Ranked by engine score.",
        styles["Body"],
    ))
    if "has_rex_filing" in scored.columns and "has_rex_launch" in scored.columns:
        mask = scored["has_rex_filing"].astype(bool) & ~scored["has_rex_launch"].astype(bool)
    else:
        mask = pd.Series(False, index=scored.index)
    story.extend(_section_top_candidates(scored, "Launch Candidates", mask, 25, styles))

    # File candidates
    story.append(PageBreak())
    story.append(Paragraph("File Candidates (no REX filing yet)", styles["H2"]))
    story.append(Paragraph(
        "Underliers without an existing REX filing, ranked by engine score. "
        "Market cap floor / additional filters may be applied downstream.",
        styles["Body"],
    ))
    if "has_rex_filing" in scored.columns:
        mask = ~scored["has_rex_filing"].astype(bool)
    else:
        mask = pd.Series(True, index=scored.index)
    story.extend(_section_top_candidates(scored, "File Candidates", mask, 30, styles))

    # IPOs
    story.append(PageBreak())
    story.append(Paragraph("IPO Pipeline", styles["H2"]))
    story.append(Paragraph(
        "Upcoming + recent IPOs from stockanalysis.com. Not yet scored — "
        "post-IPO underliers will enter the main engine once bbg data is available.",
        styles["Body"],
    ))
    story.extend(_section_ipos(ipos, styles))

    # Trending themes
    story.append(PageBreak())
    story.append(Paragraph("Trending Themes", styles["H2"]))
    story.append(Paragraph(
        "Theme rollup = average engine score across tickers in each theme. "
        "Themes defined in screener/li_engine/themes.yaml.",
        styles["Body"],
    ))
    story.extend(_section_themes(scored, themes, styles))

    # Methodology snippet
    story.append(PageBreak())
    story.append(Paragraph("Methodology Snapshot", styles["H2"]))
    w_header = ["Pillar", "Weight"]
    w_rows = [w_header]
    for pillar, w in weights.pillar_weights.items():
        w_rows.append([pillar.replace("_", " ").title(), f"{w:.0%}"])
    story.append(_table(w_rows, col_widths=[250, 100]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Target variable: forward 90-day net flows / starting AUM, winsorized. "
        "AUM was rejected because it is contaminated by underlier market P&amp;L "
        "(BMNU case). Weights are re-estimated monthly from Spearman rank-IC "
        "of each signal against the target, with 5% floor and 35% cap per pillar. "
        "See docs/LI_ENGINE_METHODOLOGY.md for full detail.",
        styles["Body"],
    ))

    doc.build(story)
    log.info("Weekly report written to %s", out_path)
    return out_path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    path = generate_weekly_report()
    print(f"Report: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
