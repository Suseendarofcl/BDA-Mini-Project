"""
download_stocks.py - Bronze-layer downloader for stocks, company debt and the two indices

Downloads into data/bronze/stocks/ :
  constituents.csv      the 50 Nifty 50 companies with their industry (official NSE list)
  prices/<SYMBOL>.csv   11 years of daily prices per company (Yahoo Finance, adjusted for splits and dividends)
  indices/*.csv         11 years of daily values of the Nifty 50 and the Sensex
  fundamentals.csv      yearly debt, equity, EBIT and interest expense per company
                        (Yahoo Finance publishes about the last four financial years)

Usage
  python download_stocks.py                 everything
  python download_stocks.py --years 10      years to analyse (default 10; one extra year is downloaded)

Files already downloaded today are skipped, so the script can be re-run if it stops.
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

OUT = Path("data") / "bronze" / "stocks"
NSE_LIST_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"}
INDICES = {"NIFTY 50": "^NSEI", "SENSEX": "^BSESN"}

# Nifty 50 members and industries from the NSE list on 3 October 2026.
# Used only if the live list cannot be downloaded.
FALLBACK = {
    "ADANIENT": "Metals & Mining", "ADANIPORTS": "Services", "APOLLOHOSP": "Healthcare",
    "ASIANPAINT": "Consumer Durables", "AXISBANK": "Financial Services", "BSE": "Financial Services",
    "BAJAJ-AUTO": "Automobile and Auto Components", "BAJFINANCE": "Financial Services",
    "BAJAJFINSV": "Financial Services", "BEL": "Capital Goods", "BHARTIARTL": "Telecommunication",
    "CIPLA": "Healthcare", "COALINDIA": "Oil Gas & Consumable Fuels", "DRREDDY": "Healthcare",
    "EICHERMOT": "Automobile and Auto Components", "ETERNAL": "Consumer Services",
    "GRASIM": "Construction Materials", "HCLTECH": "Information Technology",
    "HDFCBANK": "Financial Services", "HDFCLIFE": "Financial Services", "HINDALCO": "Metals & Mining",
    "HINDUNILVR": "Fast Moving Consumer Goods", "ICICIBANK": "Financial Services",
    "ITC": "Fast Moving Consumer Goods", "INFY": "Information Technology", "INDIGO": "Services",
    "JSWSTEEL": "Metals & Mining", "JIOFIN": "Financial Services", "KOTAKBANK": "Financial Services",
    "LT": "Construction", "M&M": "Automobile and Auto Components", "MARUTI": "Automobile and Auto Components",
    "MAXHEALTH": "Healthcare", "NTPC": "Power", "NESTLEIND": "Fast Moving Consumer Goods",
    "ONGC": "Oil Gas & Consumable Fuels", "POWERGRID": "Power", "RELIANCE": "Oil Gas & Consumable Fuels",
    "SBILIFE": "Financial Services", "SHRIRAMFIN": "Financial Services", "SBIN": "Financial Services",
    "SUNPHARMA": "Healthcare", "TCS": "Information Technology", "TATACONSUM": "Fast Moving Consumer Goods",
    "TMPV": "Automobile and Auto Components", "TATASTEEL": "Metals & Mining",
    "TECHM": "Information Technology", "TITAN": "Consumer Durables", "TRENT": "Consumer Services",
    "ULTRACEMCO": "Construction Materials",
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def safe_name(symbol: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in symbol)


def fresh(path: Path) -> bool:
    """True if the file exists and was written today."""
    return path.exists() and path.stat().st_size > 0 and date.fromtimestamp(path.stat().st_mtime) == date.today()


# ----------------------------------------------------------------------------
# 1. Company list
# ----------------------------------------------------------------------------
def get_constituents() -> pd.DataFrame:
    try:
        r = requests.get(NSE_LIST_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={"Company Name": "company", "Industry": "industry", "Symbol": "symbol"})
        df = df[["symbol", "company", "industry"]].dropna()
        if len(df) < 40:
            raise ValueError(f"only {len(df)} rows")
        log(f"Company list: {len(df)} Nifty 50 companies from NSE")
        return df
    except Exception as exc:
        log(f"Could not download the NSE list ({exc}); using the built-in list of 3 October 2026")
        return pd.DataFrame([{"symbol": s, "company": s, "industry": i} for s, i in FALLBACK.items()])


# ----------------------------------------------------------------------------
# 2. Prices
# ----------------------------------------------------------------------------
def tidy_history(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Yahoo history frame -> symbol,date,open,high,low,close,volume."""
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):                # some yfinance versions add a ticker level
        df.columns = [c[0] for c in df.columns]
    idx = pd.DatetimeIndex(df.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    out = pd.DataFrame({
        "symbol": symbol, "date": idx.normalize().strftime("%Y-%m-%d"),
        "open": df["Open"].to_numpy(), "high": df["High"].to_numpy(), "low": df["Low"].to_numpy(),
        "close": df["Close"].to_numpy(), "volume": df["Volume"].to_numpy() if "Volume" in df else 0,
    })
    out = out.dropna(subset=["close"])
    return out[out["close"] > 0].drop_duplicates("date")


def fetch_history(yf, ticker: str, years: int) -> pd.DataFrame:
    last = None
    for attempt in range(3):
        try:
            # one extra year, so a price exists on or before the 10-year-ago date
            start = date.today().replace(day=1, year=date.today().year - years - 1)
            raw = yf.Ticker(ticker).history(start=start.isoformat(), interval="1d", auto_adjust=True)
            if raw is not None and not raw.empty:
                return raw
            last = "no rows returned"
        except Exception as exc:                              # network error or rate limit
            last = str(exc)
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(last or "download failed")


# ----------------------------------------------------------------------------
# 3. Fundamentals (debt)
# ----------------------------------------------------------------------------
def pick(stmt: pd.DataFrame, names: list[str]) -> pd.Series:
    """First matching row of a Yahoo statement, by label with spaces removed."""
    if stmt is None or stmt.empty:
        return pd.Series(dtype="float64")
    labels = {str(i).replace(" ", "").lower(): i for i in stmt.index}
    for n in names:
        if n.lower() in labels:
            return pd.to_numeric(stmt.loc[labels[n.lower()]], errors="coerce")
    return pd.Series(dtype="float64")


def tidy_fundamentals(bs: pd.DataFrame, inc: pd.DataFrame, symbol: str) -> list[dict]:
    series = {
        "total_debt": pick(bs, ["TotalDebt"]),
        "equity": pick(bs, ["StockholdersEquity", "CommonStockEquity", "TotalEquityGrossMinorityInterest"]),
        "ebit": pick(inc, ["EBIT", "OperatingIncome"]),
        "interest_expense": pick(inc, ["InterestExpense", "InterestExpenseNonOperating"]),
        "revenue": pick(inc, ["TotalRevenue", "OperatingRevenue"]),
        "net_income": pick(inc, ["NetIncome", "NetIncomeCommonStockholders"]),
    }
    periods = sorted({p for s in series.values() for p in s.index})
    rows = []
    for p in periods:
        row = {"symbol": symbol, "fiscal_year_end": pd.Timestamp(p).strftime("%Y-%m-%d")}
        for k, s in series.items():
            v = s.get(p)
            row[k] = None if v is None or pd.isna(v) else float(v)
        if row["total_debt"] is not None or row["equity"] is not None:
            rows.append(row)
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description="Download stock, index and company-debt data into data/bronze/stocks/")
    p.add_argument("--years", type=int, default=10)
    args = p.parse_args()
    try:
        import yfinance as yf
    except ImportError:
        print("yfinance is not installed. Run:  pip install yfinance")
        return 1

    (OUT / "prices").mkdir(parents=True, exist_ok=True)
    (OUT / "indices").mkdir(parents=True, exist_ok=True)
    cons = get_constituents()
    cons.to_csv(OUT / "constituents.csv", index=False)

    # ---- indices
    for name, ticker in INDICES.items():
        target = OUT / "indices" / f"{safe_name(name)}.csv"
        if fresh(target):
            log(f"  {name}: already downloaded today")
            continue
        try:
            df = tidy_history(fetch_history(yf, ticker, args.years), name)
            df.to_csv(target, index=False)
            log(f"  {name}: {len(df):,} days, {df['date'].min()} to {df['date'].max()}")
        except Exception as exc:
            log(f"  {name}: FAILED ({exc})")
        time.sleep(0.5)

    # ---- company prices and fundamentals
    ok, failed, fund_rows = 0, [], []
    for i, row in enumerate(cons.itertuples(index=False), 1):
        sym, ticker = row.symbol, f"{row.symbol}.NS"
        target = OUT / "prices" / f"{safe_name(sym)}.csv"
        try:
            if fresh(target):
                n = sum(1 for _ in target.open()) - 1
            else:
                df = tidy_history(fetch_history(yf, ticker, args.years), sym)
                if df.empty:
                    raise RuntimeError("no price rows")
                df.to_csv(target, index=False)
                n = len(df)
            ok += 1
            note = ""
            try:
                t = yf.Ticker(ticker)
                rows = tidy_fundamentals(t.balance_sheet, t.income_stmt, sym)
                fund_rows += rows
                note = f", {len(rows)} years of debt data"
            except Exception as exc:
                note = f", no debt data ({str(exc)[:60]})"
            log(f"  {i:>2}/{len(cons)} {sym:<12} {n:>5,} days{note}")
        except Exception as exc:
            failed.append(sym)
            log(f"  {i:>2}/{len(cons)} {sym:<12} FAILED ({str(exc)[:80]})")
        time.sleep(0.6)                                       # stay polite to Yahoo

    cols = ["symbol", "fiscal_year_end", "total_debt", "equity", "ebit", "interest_expense", "revenue", "net_income"]
    with (OUT / "fundamentals.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(fund_rows)

    have_idx = [n for n in INDICES if (OUT / "indices" / f"{safe_name(n)}.csv").exists()]
    print("=" * 64)
    print(f"Companies with prices : {ok} of {len(cons)}" + (f"   failed: {', '.join(failed)}" if failed else ""))
    print(f"Companies with debt   : {len({r['symbol'] for r in fund_rows})}   ({len(fund_rows)} yearly rows)")
    print(f"Indices               : {', '.join(have_idx) if have_idx else 'none'}")
    print(f"Files are in          : {OUT.resolve()}")
    print("=" * 64)
    if ok < 10 or "NIFTY 50" not in have_idx:
        print("Too little data came through (Yahoo may be blocking or slow). Wait a few minutes and run again.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
