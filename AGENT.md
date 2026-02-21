# AGENT: Home-Design
**Task**: TASK-A — Home Page + Global CSS Design Overhaul
**Branch**: feature/home-design
**Status**: DONE

## Progress Reporting
Write timestamped progress to: `.agents/progress/Home-Design.md`
Format: `## [HH:MM] Task description` then bullet details.
Update every major step.

## Your Mission
Elevate the home page and global CSS to Bloomberg Terminal / etf.com quality.
Three files you own completely:
- `webapp/templates/home.html`
- `webapp/static/css/style.css`
- `webapp/templates/base.html`

## Step 1: Read First
Read ALL three files before touching anything:
- `webapp/static/css/style.css` (understand existing variables, classes, what exists)
- `webapp/templates/home.html` (current naked layout)
- `webapp/templates/base.html` (nav structure)

## Step 2: style.css — Design System Overhaul

Add to `:root` (do NOT remove any existing variables):
```css
/* Extended palette */
--teal:        #0D9488;
--teal-light:  #CCFBF1;
--slate:       #475569;
--slate-light: #F8FAFC;
--indigo:      #4338CA;
--amber:       #D97706;
--emerald:     #059669;
--rose:        #E11D48;

/* Market category colors */
--cat-li:      #1E40AF;
--cat-income:  #059669;
--cat-crypto:  #7C3AED;
--cat-defined: #D97706;
--cat-thematic:#0891B2;

/* Surface hierarchy */
--surface-0:   #F8FAFC;
--surface-1:   #FFFFFF;
--surface-2:   #F1F5F9;
--surface-3:   #E2E8F0;

/* Typography */
--text-primary:   #0F172A;
--text-secondary: #475569;
--text-muted:     #94A3B8;
--text-accent:    #1E40AF;
```

Add global improvements:
- `.data-table th` → `position: sticky; top: 0; z-index: 2; background: var(--surface-1);`
- `.data-table tr:hover` → `background: var(--surface-2);`
- `.kpi` cards → left border accent, stronger shadow on hover

Add utility classes:
```css
.badge-positive { background: #D1FAE5; color: #065F46; }
.badge-negative { background: #FEE2E2; color: #991B1B; }
.badge-neutral  { background: #F1F5F9; color: #475569; }
.flow-positive  { color: #059669; font-weight: 600; }
.flow-negative  { color: #DC2626; font-weight: 600; }
.text-mono      { font-family: var(--font-mono); }
.truncate-cell  { max-width: 220px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.sticky-col     { position: sticky; left: 0; background: var(--surface-1); z-index: 1; }
```

Add complete Home page CSS section (hero, sections, cards, contact, pagination):
```css
/* ── Home ─────────────────────────────────────────────────── */
.home-hero {
  background: linear-gradient(135deg, #0f1923 0%, #1a3a5c 60%, #0D9488 100%);
  color: white; padding: 56px 40px; border-radius: 12px;
  margin-bottom: 40px; position: relative; overflow: hidden;
}
.home-hero::after {
  content: ''; position: absolute; right: -60px; top: -60px;
  width: 300px; height: 300px; border-radius: 50%;
  background: rgba(13,148,136,0.15); pointer-events: none;
}
.home-title { font-size: 2.6rem; font-weight: 800; letter-spacing: -0.04em; margin: 0 0 12px; }
.home-tagline { font-size: 1.05rem; opacity: 0.82; max-width: 640px; line-height: 1.65; margin: 0; }
.home-section { margin-bottom: 36px; }
.home-section-title {
  font-size: 0.7rem; font-weight: 700; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--text-muted);
  margin-bottom: 16px; padding-bottom: 10px;
  border-bottom: 2px solid var(--surface-3);
  display: flex; align-items: center; gap: 8px;
}
.home-section-title::before {
  content: ''; display: inline-block; width: 3px; height: 14px;
  border-radius: 2px; background: var(--teal);
}
.home-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(210px, 1fr)); gap: 14px; }
.home-card {
  display: block; text-decoration: none; color: inherit;
  border: 1px solid var(--surface-3); border-radius: 10px;
  padding: 18px 20px; background: var(--surface-1);
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  transition: box-shadow 0.18s, transform 0.18s, border-color 0.18s;
  border-top: 3px solid transparent;
}
.home-card:hover { box-shadow: 0 4px 16px rgba(0,0,0,0.10); transform: translateY(-2px); text-decoration: none; color: inherit; }
.home-card--market  { border-top-color: var(--cat-li); }
.home-card--filing  { border-top-color: var(--emerald); }
.home-card--product { border-top-color: var(--amber); }
.home-card--ops     { border-top-color: var(--indigo); }
.home-card-tag { font-size: 0.62rem; font-weight: 700; letter-spacing: 0.14em; text-transform: uppercase; margin-bottom: 8px; }
.home-card--market  .home-card-tag { color: var(--cat-li); }
.home-card--filing  .home-card-tag { color: var(--emerald); }
.home-card--product .home-card-tag { color: var(--amber); }
.home-card--ops     .home-card-tag { color: var(--indigo); }
.home-card-title { font-size: 0.98rem; font-weight: 700; margin-bottom: 6px; color: var(--text-primary); }
.home-card-desc { font-size: 0.8rem; color: var(--text-secondary); line-height: 1.5; }
.home-contact {
  text-align: center; color: var(--text-muted); font-size: 0.85rem;
  margin-top: 40px; padding-top: 20px; border-top: 1px solid var(--surface-3);
}
/* Pagination */
.pagination-bar { display: flex; align-items: center; gap: 12px; margin-top: 20px; flex-wrap: wrap; }
.pagination-info { color: var(--text-muted); font-size: 0.83rem; }
.pagination-controls { display: flex; gap: 4px; }
.pagination-controls .btn { min-width: 34px; text-align: center; }
.select-sm { padding: 4px 8px; font-size: 0.82rem; border: 1px solid var(--surface-3); border-radius: 6px; background: var(--surface-1); color: var(--text-primary); }
```

## Step 3: home.html — Executive Hub Rewrite

4 sections, professional tone:

**Hero**: "REX Financial Intelligence Hub" with tagline:
> "The central intelligence platform for all REX teams. Real-time access to market positioning, SEC filing activity, product analytics, and operational data across the structured and leveraged ETP universe."

**Section 1 — MARKET INTELLIGENCE** (`.home-card--market`, 6 cards):
- REX View `/market/rex` — "REX branded suite performance, AUM by suite, and competitive positioning."
- Category View `/market/category` — "Deep-dive into AUM and flow data across 8 ETP categories."
- Product Treemap `/market/treemap` — "Visual AUM distribution across the entire product universe."
- Issuer Analysis `/market/issuer` — "Rank and compare all ETP issuers by AUM, flows, and market share."
- Market Share `/market/share` — "Track category-level market share shifts over time."
- Underlier Deep-Dive `/market/underlier` — "Analyze the full ETP landscape by underlying asset."

**Section 2 — FILINGS & COMPLIANCE** (`.home-card--filing`, 3 cards):
- Filing Tracker `/dashboard` — "Monitor SEC filings across 122 trusts. Track 485BPOS, 485BXT, and 497 forms."
- Fund Search `/funds/` — "Search 7,000+ ETF funds by ticker, name, or trust. Filter by status."
- Filing Analysis `/analysis` — "Analyze filing trends, effective date patterns, and amendment history."

**Section 3 — PRODUCT DEVELOPMENT** (`.home-card--product`, 2 cards):
- Launch Screener `/screener/` — "Score ETP launch candidates across 4 pillars: AUM, flows, options, spread."
- Candidate Evaluator `/screener/evaluate` — "Side-by-side scoring comparison for specific tickers."

**Section 4 — OPERATIONS** (`.home-card--ops`, 2 cards):
- Data Downloads `/downloads/` — "Export fund status, filing history, and pipeline CSVs for any trust."
- Email Digest `/digest` — "Send filing digest emails to subscribed REX team members."

Footer: `relasmar@rexfin.com`

Use this structure:
```html
{% extends "base.html" %}
{% block title %}REX Financial Intelligence Hub{% endblock %}
{% block content %}
<div class="home-hero">
  <h1 class="home-title">REX Financial Intelligence Hub</h1>
  <p class="home-tagline">The central intelligence platform for all REX teams...</p>
</div>

<div class="home-section">
  <div class="home-section-title">Market Intelligence</div>
  <div class="home-grid">
    <a href="/market/rex" class="home-card home-card--market">
      <div class="home-card-tag">Market</div>
      <div class="home-card-title">REX View</div>
      <div class="home-card-desc">...</div>
    </a>
    ... (5 more cards)
  </div>
</div>
... (3 more sections)

<div class="home-contact">
  <p>Questions or feedback? Contact <a href="mailto:relasmar@rexfin.com">relasmar@rexfin.com</a></p>
</div>
{% endblock %}
```

## Step 4: base.html — Nav Improvements

Read base.html carefully first, then:
- Add `<meta name="theme-color" content="#0f1923">` in `<head>`
- Ensure nav has consistent dark background
- Add subtle bottom border on nav: `border-bottom: 1px solid rgba(255,255,255,0.1)`
- Keep ALL existing links and structure

## Commit Convention
```
git add webapp/templates/home.html webapp/static/css/style.css webapp/templates/base.html
git commit -m "feat: Home page executive hub redesign + global CSS design system"
```

## Done Criteria
- [ ] home.html has 4 color-coded sections with proper CSS classes
- [ ] style.css has all new CSS variables in :root
- [ ] style.css has all home-card, home-section, home-hero CSS
- [ ] style.css has pagination-bar, .flow-positive/.flow-negative utilities
- [ ] base.html nav is consistent
- [ ] No existing CSS broken (only additions/improvements)
