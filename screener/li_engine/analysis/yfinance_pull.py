"""yfinance underlier signal pull.

Narrow scope by design: the L&I underlier universe (~235 tickers from
`mkt_fund_classification`) plus any extra tickers we need. NOT the full
6,500-ticker stock universe — that rate-limits yfinance within minutes.

Produces per-ticker signals: momentum, realized vol, drawdown, RSI,
%-of-52w-high, and SPY-relative capture.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    raise RuntimeError("yfinance required: pip install yfinance")

log = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DB = _ROOT / "data" / "etp_tracker.db"
OUT_DIR = _ROOT / "data" / "analysis"
PARQUET = OUT_DIR / "yfinance_signal_panel.parquet"
CACHE = OUT_DIR / "yfinance_ohlcv_cache.parquet"


def _clean(t: str) -> str:
    if not isinstance(t, str):
        return ""
    return t.split()[0].upper().strip()


def get_universe(db_path: Path = DB) -> list[str]:
    """L&I underliers + SPY benchmark."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT DISTINCT map_li_underlier FROM mkt_master_data "
            "WHERE primary_category='LI' AND map_li_underlier IS NOT NULL "
            "AND map_li_underlier != ''"
        ).fetchall()
    finally:
        conn.close()

    tickers = {_clean(r[0]) for r in rows if r[0]}
    tickers.discard("")
    # Drop obvious non-equity tickers (currency codes, Bloomberg-internal)
    tickers = {t for t in tickers if not any(x in t for x in ("CURNCY", "CMDTY", "INDEX", "SOL"))}
    tickers.add("SPY")  # benchmark
    return sorted(tickers)


def fetch_ohlcv(tickers: list[str], period: str = "2y",
                batch_size: int = 25, sleep_between: float = 3.0,
                max_retries: int = 3) -> pd.DataFrame:
    """Download in small batches with backoff. yfinance rate-limits hard
    even on ~200-ticker bulk calls, so we slow-drip."""
    log.info("Pulling %d tickers in batches of %d (sleep=%ss)",
             len(tickers), batch_size, sleep_between)
    all_frames = []
    success_count = 0
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        attempt = 0
        while attempt < max_retries:
            try:
                d = yf.download(
                    tickers=" ".join(batch),
                    period=period,
                    group_by="ticker",
                    auto_adjust=True,
                    threads=False,
                    progress=False,
                )
                if d is not None and not d.empty:
                    all_frames.append(d)
                    # count how many tickers actually returned data
                    if len(batch) == 1:
                        if not d.empty:
                            success_count += 1
                    else:
                        returned = {t for t in batch if (t, "Close") in d.columns}
                        success_count += len(returned)
                break
            except Exception as e:
                attempt += 1
                backoff = sleep_between * (2 ** attempt)
                log.warning("batch %d attempt %d failed (%s), backing off %ss",
                            i // batch_size, attempt, e, backoff)
                time.sleep(backoff)
        log.info("batch %d/%d done (success_total=%d)",
                 i // batch_size + 1, (len(tickers) + batch_size - 1) // batch_size,
                 success_count)
        time.sleep(sleep_between)

    if not all_frames:
        return pd.DataFrame()
    combined = pd.concat(all_frames, axis=1)
    return combined


def _pct_return(close: pd.Series, days: int) -> float:
    if len(close) < days + 1 or close.iloc[-1] != close.iloc[-1] or close.iloc[-days - 1] != close.iloc[-days - 1]:
        return float("nan")
    return float(close.iloc[-1] / close.iloc[-days - 1] - 1.0)


def _realized_vol(close: pd.Series, window: int) -> float:
    if len(close) < window + 1:
        return float("nan")
    logret = np.log(close).diff().dropna().iloc[-window:]
    if len(logret) < window // 2:
        return float("nan")
    return float(logret.std() * np.sqrt(252) * 100.0)


def _max_drawdown(close: pd.Series, window: int = 90) -> float:
    w = close.iloc[-window:] if len(close) >= window else close
    if len(w) < 2:
        return float("nan")
    peak = w.cummax()
    dd = (w / peak - 1.0).min()
    return float(dd)


def _pct_of_52w_high(close: pd.Series) -> float:
    w = close.iloc[-252:] if len(close) >= 252 else close
    if len(w) < 2:
        return float("nan")
    return float(close.iloc[-1] / w.max())


def _streak_up(close: pd.Series, window: int = 30) -> int:
    w = close.iloc[-window:] if len(close) >= window else close
    if len(w) < 2:
        return 0
    diffs = w.diff().dropna()
    longest = cur = 0
    for d in diffs:
        if d > 0:
            cur += 1
            longest = max(longest, cur)
        else:
            cur = 0
    return int(longest)


def _rsi_14(close: pd.Series) -> float:
    if len(close) < 15:
        return float("nan")
    delta = close.diff().dropna().iloc[-14:]
    gain = delta.where(delta > 0, 0.0).sum()
    loss = -delta.where(delta < 0, 0.0).sum()
    if loss == 0:
        return 100.0
    rs = gain / loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _capture(t_close: pd.Series, spy_close: pd.Series, window: int = 30) -> tuple[float, float]:
    t_ret = t_close.pct_change().dropna().iloc[-window:]
    s_ret = spy_close.pct_change().dropna().iloc[-window:]
    common = t_ret.index.intersection(s_ret.index)
    if len(common) < 10:
        return float("nan"), float("nan")
    t_ret, s_ret = t_ret.loc[common], s_ret.loc[common]
    up = s_ret > 0
    down = s_ret < 0
    up_cap = (t_ret[up].mean() / s_ret[up].mean()) if up.sum() >= 3 and s_ret[up].mean() != 0 else float("nan")
    dn_cap = (t_ret[down].mean() / s_ret[down].mean()) if down.sum() >= 3 and s_ret[down].mean() != 0 else float("nan")
    return float(up_cap), float(dn_cap)


def compute_signals(data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Given a multi-ticker yfinance DataFrame, compute per-ticker signals."""
    # Extract SPY separately for capture calculations
    spy_close = None
    if "SPY" in tickers and ("SPY", "Close") in data.columns:
        spy_close = data[("SPY", "Close")].dropna()

    rows = []
    for t in tickers:
        try:
            if (t, "Close") not in data.columns:
                continue
            close = data[(t, "Close")].dropna()
            if len(close) < 30:
                continue

            vol = data.get((t, "Volume"), pd.Series(dtype=float)).dropna()

            up_cap, dn_cap = (float("nan"), float("nan"))
            if spy_close is not None and t != "SPY":
                up_cap, dn_cap = _capture(close, spy_close)

            rows.append({
                "ticker": t,
                "last_price": float(close.iloc[-1]),
                "last_date": close.index[-1].date() if hasattr(close.index[-1], "date") else None,
                "n_days": int(len(close)),
                "ret_1w": _pct_return(close, 5),
                "ret_1m": _pct_return(close, 21),
                "ret_3m": _pct_return(close, 63),
                "ret_6m": _pct_return(close, 126),
                "rvol_30d": _realized_vol(close, 30),
                "rvol_60d": _realized_vol(close, 60),
                "rvol_90d": _realized_vol(close, 90),
                "max_drawdown_90d": _max_drawdown(close, 90),
                "pct_of_52w_high": _pct_of_52w_high(close),
                "avg_vol_30d": float(vol.iloc[-30:].mean()) if len(vol) >= 30 else float("nan"),
                "streak_up": _streak_up(close),
                "rsi_14": _rsi_14(close),
                "upside_capture_30d": up_cap,
                "downside_capture_30d": dn_cap,
            })
        except Exception as e:
            log.warning("signal compute failed for %s: %s", t, e)
    return pd.DataFrame(rows).set_index("ticker")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    tickers = get_universe()
    log.info("Universe: %d tickers", len(tickers))

    start = time.time()
    data = fetch_ohlcv(tickers, period="2y")
    log.info("OHLCV pull: %.1fs", time.time() - start)

    if data is None or data.empty:
        log.error("No OHLCV data returned — yfinance blocked or all tickers invalid")
        return
    signals = compute_signals(data, tickers)
    log.info("Signals: %d tickers computed", len(signals))

    if len(signals) == 0:
        log.error("0 signals computed")
        return
    signals.to_parquet(PARQUET, compression="snappy")
    log.info("Saved to %s (%.1f KB)", PARQUET, PARQUET.stat().st_size / 1024)

    print(f"Pulled: {len(signals)} / {len(tickers)}")
    print(f"Output: {PARQUET}")
    print(signals.describe().round(3).to_string())


if __name__ == "__main__":
    main()
