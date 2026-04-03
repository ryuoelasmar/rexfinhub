"""
Foundation scorer for 2x Long single-stock ETF candidates.

Three components:
  A. Demand Floor — pass/fail thresholds (eliminate unviable stocks)
  B. Demand Rank — single percentile score from turnover (the least-redundant demand metric)
  C. Context Adjustments — competitive position, volume surge, short interest

Honest about limitations:
  - Stock metrics explain ~1-2% of product success
  - Early flow (first 30 days) is the real predictor, but we can't know it before launch
  - This scorer identifies which stocks have the FLOOR of demand needed
  - The real decision factors (issuer execution, timing, luck) are outside this model

Design principle: learn and iterate. Track predictions, review quarterly.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)


@dataclass
class CandidateScore:
    """Score for a single stock candidate."""
    ticker: str
    company_name: str
    sector: str

    # Demand floor
    floor_pass: bool
    floor_turnover: float  # raw value
    floor_oi: float
    floor_volume: float

    # Demand rank (percentile)
    demand_rank: float  # 0-100

    # Context adjustments
    adj_competition: float  # -10 to +10
    adj_volume_surge: float  # 0 to +5
    adj_short_interest: float  # -5 to 0

    # Final
    composite_score: float  # demand_rank + adjustments
    recommendation: str  # RECOMMEND / CONSIDER / PASS
    reasoning: str

    # Flags (informational, not scored)
    competition_count: int = 0
    competition_aum: float = 0
    competition_flow_ytd: float = 0
    rex_position: int = 0  # 0 = no REX product, N = Nth entrant
    filing_status: str = ""
    expense_ratio_benchmark: float = 0


def load_stock_universe(db_path: str = "data/etp_tracker.db") -> pd.DataFrame:
    """Load all stock data from the database."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT ticker, data_json FROM mkt_stock_data").fetchall()
    conn.close()

    records = []
    for ticker, data_json in rows:
        try:
            d = json.loads(data_json)[0]
        except (json.JSONDecodeError, IndexError):
            continue

        def s(k):
            v = d.get(k)
            try:
                return float(v) if v and str(v) not in ('#ERROR', 'N/A', 'nan', '') else 0
            except (ValueError, TypeError):
                return 0

        records.append({
            'ticker': ticker,
            'name': d.get('Ticker', ticker),
            'sector': d.get('GICS Sector', ''),
            'mktcap': s('Mkt Cap'),
            'volume_30d': s('Avg Volume 30D'),
            'volume_5d': s('Avg Volume 5D'),
            'volume_3m': s('Avg Volume 3M'),
            'turnover': s('Turnover / Traded Value'),
            'total_oi': s('Total OI'),
            'call_oi': s('Total Call OI'),
            'put_oi': s('Total Put OI'),
            'volatility_30d': s('Volatility 30D'),
            'short_interest_ratio': s('Short Interest Ratio'),
            'inst_ownership': s('Institutional Owner % Shares Outstanding'),
            'last_price': s('Last Price'),
            'high_52w': s('52W High'),
            'low_52w': s('52W Low'),
            'return_1m': s('1M Total Return'),
            'return_1y': s('1Y Total Return'),
            'sentiment': s('News Sentiment Daily Avg'),
        })

    df = pd.DataFrame(records)
    # Volume surge: recent vs historical
    df['volume_surge'] = np.where(df['volume_3m'] > 0, df['volume_5d'] / df['volume_3m'], 1.0)
    return df


def load_competition(db_path: str = "data/etp_tracker.db") -> pd.DataFrame:
    """Load existing L&I single stock products for competitive landscape."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT ticker, fund_name, issuer_display, aum, is_singlestock,
               expense_ratio, fund_flow_1month, fund_flow_ytd,
               map_li_direction, inception_date
        FROM mkt_master_data
        WHERE category_display LIKE '%Leverage%Single Stock%'
        AND market_status = 'ACTV' AND map_li_direction = 'Long'
    """).fetchall()
    conn.close()

    cols = ['prod_ticker', 'prod_name', 'issuer', 'aum', 'underlier',
            'exp_ratio', 'flow_1m', 'flow_ytd', 'direction', 'inception']
    df = pd.DataFrame(rows, columns=cols)
    for c in ['aum', 'exp_ratio', 'flow_1m', 'flow_ytd']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df


def compute_thresholds(stocks: pd.DataFrame, competition: pd.DataFrame) -> dict:
    """Compute demand floor thresholds from stocks that have successful products."""
    # Find underliers of products with AUM > $25M
    successful_ul = set(competition[competition['aum'] >= 25]['underlier'].unique())
    successful_stocks = stocks[stocks['ticker'].isin(successful_ul)]

    if len(successful_stocks) < 10:
        successful_stocks = stocks  # fallback to full universe

    # Use 5th percentile of successful underliers as absolute minimum
    # This is a LOW bar — we're just eliminating penny stocks and illiquids
    return {
        'turnover_floor': successful_stocks['turnover'].quantile(0.05),
        'oi_floor': successful_stocks['total_oi'].quantile(0.05),
        'volume_floor': successful_stocks['volume_30d'].quantile(0.05),
    }


def score_candidate(
    ticker: str,
    stocks: pd.DataFrame,
    competition: pd.DataFrame,
    thresholds: dict,
) -> CandidateScore | None:
    """Score a single stock candidate."""
    # Find the stock
    clean_ticker = ticker.upper().strip()
    if not clean_ticker.endswith(' US'):
        clean_ticker += ' US'

    stock_row = stocks[stocks['ticker'] == clean_ticker]
    if stock_row.empty:
        return None
    s = stock_row.iloc[0]

    # A. DEMAND FLOOR
    pass_turnover = s['turnover'] >= thresholds['turnover_floor']
    pass_oi = s['total_oi'] >= thresholds['oi_floor']
    pass_volume = s['volume_30d'] >= thresholds['volume_floor']
    floor_pass = pass_turnover or pass_oi or pass_volume  # pass if ANY threshold met

    # B. DEMAND RANK (turnover percentile within full universe)
    demand_rank = (stocks['turnover'] < s['turnover']).mean() * 100

    # C. CONTEXT ADJUSTMENTS
    # Competition
    comp_for_ul = competition[competition['underlier'] == clean_ticker]
    comp_count = len(comp_for_ul)
    comp_aum = comp_for_ul['aum'].sum()
    comp_flow_ytd = comp_for_ul['flow_ytd'].sum()
    rex_products = comp_for_ul[comp_for_ul['issuer'] == 'REX']
    rex_position = len(rex_products)

    adj_competition = 0
    if comp_count == 0:
        adj_competition = 0  # Whitespace — neutral (unvalidated)
    elif comp_count <= 4 and comp_flow_ytd > 0:
        adj_competition = +5  # Validated demand, not overcrowded
    elif comp_count <= 4 and comp_flow_ytd <= 0:
        adj_competition = -3  # Existing products losing money
    elif comp_count >= 5 and comp_flow_ytd > 0:
        adj_competition = +2  # Crowded but still flowing
    elif comp_count >= 5 and comp_flow_ytd <= 0:
        adj_competition = -8  # Crowded AND declining

    # Volume surge
    adj_surge = 0
    if s['volume_surge'] >= 2.0:
        adj_surge = +5  # Major attention event
    elif s['volume_surge'] >= 1.5:
        adj_surge = +3  # Elevated attention

    # Short interest
    adj_short = 0
    if s['short_interest_ratio'] > 5:
        adj_short = -5  # Very high short interest = risky
    elif s['short_interest_ratio'] > 3:
        adj_short = -2

    # COMPOSITE
    composite = demand_rank + adj_competition + adj_surge + adj_short

    # RECOMMENDATION
    if not floor_pass and demand_rank < 70:
        recommendation = "PASS"
        reasoning = "Below minimum demand thresholds and low demand rank"
    elif not floor_pass and demand_rank >= 70:
        recommendation = "CONSIDER"
        reasoning = "High demand rank but below liquidity thresholds for leveraged products"
    elif composite >= 80:
        recommendation = "RECOMMEND"
        reasoning = "Strong demand signal"
        if comp_count > 0 and comp_flow_ytd > 0:
            reasoning += " + validated by existing product flows"
        if adj_surge > 0:
            reasoning += " + elevated recent attention"
    elif composite >= 50:
        recommendation = "CONSIDER"
        reasoning = "Moderate demand signal"
        if comp_count == 0:
            reasoning += " — no existing products (unvalidated demand)"
        if adj_short < 0:
            reasoning += " — elevated short interest"
    else:
        recommendation = "PASS"
        reasoning = "Low demand signal"
        if comp_count >= 5 and comp_flow_ytd <= 0:
            reasoning += " — crowded space with declining flows"

    # Expense ratio benchmark
    exp_benchmark = comp_for_ul['exp_ratio'].median() if len(comp_for_ul) > 0 else 1.05

    return CandidateScore(
        ticker=clean_ticker,
        company_name=s.get('name', clean_ticker),
        sector=s.get('sector', ''),
        floor_pass=floor_pass,
        floor_turnover=s['turnover'],
        floor_oi=s['total_oi'],
        floor_volume=s['volume_30d'],
        demand_rank=round(demand_rank, 1),
        adj_competition=adj_competition,
        adj_volume_surge=adj_surge,
        adj_short_interest=adj_short,
        composite_score=round(composite, 1),
        recommendation=recommendation,
        reasoning=reasoning,
        competition_count=comp_count,
        competition_aum=round(comp_aum, 1),
        competition_flow_ytd=round(comp_flow_ytd, 1),
        rex_position=rex_position,
        expense_ratio_benchmark=round(exp_benchmark, 2),
    )


def score_candidates(tickers: list[str], db_path: str = "data/etp_tracker.db") -> list[CandidateScore]:
    """Score a list of stock candidates."""
    stocks = load_stock_universe(db_path)
    competition = load_competition(db_path)
    thresholds = compute_thresholds(stocks, competition)

    results = []
    for ticker in tickers:
        score = score_candidate(ticker, stocks, competition, thresholds)
        if score:
            results.append(score)
        else:
            log.warning("No data for ticker: %s", ticker)

    results.sort(key=lambda x: -x.composite_score)
    return results


def score_full_universe(db_path: str = "data/etp_tracker.db", top_n: int = 100) -> list[CandidateScore]:
    """Score the entire stock universe and return top N candidates."""
    stocks = load_stock_universe(db_path)
    competition = load_competition(db_path)
    thresholds = compute_thresholds(stocks, competition)

    results = []
    for _, s in stocks.iterrows():
        ticker = s['ticker']
        score = score_candidate(ticker, stocks, competition, thresholds)
        if score and score.floor_pass:
            results.append(score)

    results.sort(key=lambda x: -x.composite_score)
    return results[:top_n]
