"""
download_data.py - Bronze-layer data downloader
Big Data Architecture project: Stock Market & Mutual Fund Risk Analytics

Downloads raw data from free public sources into data/bronze/ :

  amfi    Full AMFI mutual-fund NAV archive (all schemes, 2006 onwards)
          + today's AMFI scheme master (fund house, category)
          + a Parquet copy of the NAV table, partitioned by year, for Spark
  nse     NSE daily equity bhavcopy (all stocks) and daily index values
          (Nifty 50, Nifty Next 50, Nifty Midcap 150, ...) for a date range
  mfapi   Latest NAV history as JSON for the curated fund list (mfapi.in)
  sensex  Sensex and Nifty 50 daily history from Yahoo Finance

Usage
  pip install requests zstandard pandas pyarrow yfinance
  python download_data.py all
  python download_data.py amfi
  python download_data.py nse --start 2016-10-01 --end 2026-09-30
  python download_data.py mfapi
  python download_data.py sensex

The script is resumable: files already on disk are skipped, so if it stops
(network drop, laptop sleep) just run the same command again.
Every download is logged to data/bronze/_manifest.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
BRONZE = Path("data") / "bronze"

AMFI_ARCHIVE_URL = (
    "https://github.com/captn3m0/historical-mf-data/releases/latest/download/funds.db.zst"
)
AMFI_NAVALL_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
MFAPI_BASE = "https://api.mfapi.in/mf"
NSE_BASES = ["https://nsearchives.nseindia.com", "https://archives.nseindia.com"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Starter fund universe: well-known Direct-plan Growth schemes that each have
# NAV history from 2013 to today in the AMFI archive (checked 2 Oct 2026).
# This is a starting list, not a performance ranking. Rank the final
# "top performing" set from the data once the return metrics are computed.
CURATED_FUNDS = {
    # Large cap
    120586: ("Large cap", "ICICI Prudential Large Cap Fund - Direct - Growth"),
    120465: ("Large cap", "Axis Large Cap Fund - Direct - Growth"),
    118825: ("Large cap", "Mirae Asset Large Cap Fund - Direct - Growth"),
    118632: ("Large cap", "Nippon India Large Cap Fund - Direct - Growth"),
    119598: ("Large cap", "SBI Large Cap Fund - Direct - Growth"),
    118269: ("Large cap", "Canara Robeco Large Cap Fund - Direct - Growth"),
    119250: ("Large cap", "DSP Large Cap Fund - Direct - Growth"),
    # Flexi cap
    122639: ("Flexi cap", "Parag Parikh Flexi Cap Fund - Direct - Growth"),
    119718: ("Flexi cap", "SBI Flexicap Fund - Direct - Growth"),
    118275: ("Flexi cap", "Canara Robeco Flexicap Fund - Direct - Growth"),
    119076: ("Flexi cap", "DSP Flexi Cap Fund - Direct - Growth"),
    118535: ("Flexi cap", "Franklin India Flexi Cap Fund - Direct - Growth"),
    129046: ("Flexi cap", "Motilal Oswal Flexi Cap Fund - Direct - Growth"),
    120046: ("Flexi cap", "HSBC Flexi Cap Fund - Direct - Growth"),
    118424: ("Flexi cap", "Bandhan Flexi Cap Fund - Direct - Growth"),
    # Mid cap
    120505: ("Mid cap", "Axis Midcap Fund - Direct - Growth"),
    119071: ("Mid cap", "DSP Midcap Fund - Direct - Growth"),
    120381: ("Mid cap", "ICICI Prudential MidCap Fund - Direct - Growth"),
    119775: ("Mid cap", "Kotak Midcap Fund - Direct - Growth"),
    119716: ("Mid cap", "SBI Midcap Fund - Direct - Growth"),
    118668: ("Mid cap", "Nippon India Growth Mid Cap Fund - Direct - Growth"),
    127042: ("Mid cap", "Motilal Oswal Midcap Fund - Direct - Growth"),
    120403: ("Mid cap", "Invesco India Midcap Fund - Direct - Growth"),
    119178: ("Mid cap", "Tata Mid Cap Fund - Direct - Growth"),
    118533: ("Mid cap", "Franklin India Mid Cap Fund - Direct - Growth"),
    # Index funds
    120716: ("Index", "UTI Nifty 50 Index Fund - Direct - Growth"),
    119827: ("Index", "SBI Nifty Index Fund - Direct - Growth"),
    118741: ("Index", "Nippon India Index Fund Nifty 50 Plan - Direct - Growth"),
    118482: ("Index", "Bandhan Nifty 50 Index Fund - Direct - Growth"),
    120684: ("Index", "ICICI Prudential Nifty Next 50 Index Fund - Direct - Growth"),
}

# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------
_manifest_lock = threading.Lock()


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def manifest(source: str, item: str, status: str, path: Path | None, url: str, note: str = "") -> None:
    """Append one line to data/bronze/_manifest.csv (lineage record)."""
    BRONZE.mkdir(parents=True, exist_ok=True)
    mf = BRONZE / "_manifest.csv"
    with _manifest_lock:
        new = not mf.exists()
        with mf.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["downloaded_at", "source", "item", "status", "bytes", "path", "url", "note"])
            size = path.stat().st_size if path and path.exists() else 0
            w.writerow([datetime.now().isoformat(timespec="seconds"), source, item, status, size,
                        str(path) if path else "", url, note])


def get(session: requests.Session, url: str, *, stream: bool = False, timeout: int = 60,
        retries: int = 4) -> requests.Response | None:
    """GET with retries. Returns None on 404, raises after repeated failures."""
    last = ""
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=timeout, stream=stream)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(2 * (2 ** attempt))  # 2s, 4s, 8s, 16s
    raise RuntimeError(f"{url} failed after {retries} tries ({last})")


def save_stream(resp: requests.Response, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    done, t0 = 0, time.time()
    with tmp.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            fh.write(chunk)
            done += len(chunk)
            if time.time() - t0 > 5:
                log(f"  ... {done / 1e6:,.0f} MB")
                t0 = time.time()
    tmp.replace(target)


# ----------------------------------------------------------------------------
# 1. AMFI: full NAV archive + scheme master
# ----------------------------------------------------------------------------
def step_amfi(args) -> None:
    out = BRONZE / "amfi"
    out.mkdir(parents=True, exist_ok=True)
    s = requests.Session()

    # 1a. Compressed SQLite archive of every AMFI NAV since 2006 (~380 MB)
    zst = out / "funds.db.zst"
    if zst.exists() and not args.refresh:
        log(f"AMFI archive already downloaded ({zst.stat().st_size / 1e6:,.0f} MB). Use --refresh to re-download.")
    else:
        log("Downloading AMFI NAV archive (about 380 MB)...")
        r = get(s, AMFI_ARCHIVE_URL, stream=True, timeout=120)
        if r is None:
            raise RuntimeError("AMFI archive not found at " + AMFI_ARCHIVE_URL)
        save_stream(r, zst)
        manifest("amfi", "funds.db.zst", "downloaded", zst, AMFI_ARCHIVE_URL)

    # 1b. Decompress to SQLite (~1.2 GB)
    db = out / "funds.db"
    if not db.exists() or db.stat().st_mtime < zst.stat().st_mtime:
        import zstandard
        log("Decompressing archive to funds.db (about 1.2 GB)...")
        tmp = db.with_name("funds.db.part")
        with zst.open("rb") as src, tmp.open("wb") as dst:
            zstandard.ZstdDecompressor().copy_stream(src, dst)
        tmp.replace(db)

    con = sqlite3.connect(db)
    n_schemes = con.execute("SELECT COUNT(*) FROM schemes").fetchone()[0]
    n_rows, d_min, d_max = con.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM nav").fetchone()
    log(f"Archive: {n_schemes:,} schemes, {n_rows:,} NAV rows, {d_min} to {d_max}")

    # 1c. Export to Parquet partitioned by year, so Spark can read it directly
    pq_dir = out / "nav_parquet"
    marker = pq_dir / "_SUCCESS"
    if marker.exists() and marker.read_text().strip() == d_max and not args.refresh:
        log("Parquet copy is already up to date.")
    else:
        import shutil
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        if pq_dir.exists():
            shutil.rmtree(pq_dir)
        log("Writing Parquet copy partitioned by year (takes a few minutes)...")
        cur = con.execute("SELECT scheme_code, date, nav FROM nav")
        written = 0
        while True:
            rows = cur.fetchmany(2_000_000)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=["scheme_code", "date", "nav"])
            df["year"] = df["date"].str.slice(0, 4).astype("int16")
            df["date"] = pd.to_datetime(df["date"]).dt.date
            pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False),
                                root_path=str(pq_dir), partition_cols=["year"])
            written += len(df)
            log(f"  ... {written:,} / {n_rows:,} rows")
        marker.write_text(d_max)

    with (out / "schemes.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["scheme_code", "scheme_name"])
        w.writerows(con.execute("SELECT scheme_code, scheme_name FROM schemes ORDER BY scheme_code"))
    con.close()

    # 1d. Today's scheme master from AMFI: adds fund house and category
    try:
        r = get(s, AMFI_NAVALL_URL, timeout=60)
        if r is None:
            raise RuntimeError("NAVAll.txt not found")
        raw = out / f"NAVAll_{date.today():%Y%m%d}.txt"
        raw.write_bytes(r.content)
        manifest("amfi", raw.name, "downloaded", raw, AMFI_NAVALL_URL)
        n = parse_navall(raw, out / "scheme_master.csv")
        log(f"Scheme master: {n:,} active schemes with fund house and category")
    except Exception as exc:  # AMFI portal is sometimes down; the archive is still usable
        log(f"WARNING: could not fetch AMFI scheme master ({exc}). Run 'amfi' again later.")
        manifest("amfi", "NAVAll.txt", "error", None, AMFI_NAVALL_URL, str(exc))

    # 1e. Curated universe file
    with (out / "curated_universe.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["scheme_code", "category", "scheme_name"])
        for code, (cat, name) in CURATED_FUNDS.items():
            w.writerow([code, cat, name])
    log(f"Curated universe: {len(CURATED_FUNDS)} funds written to {out / 'curated_universe.csv'}")


def parse_navall(raw: Path, out_csv: Path) -> int:
    """Turn AMFI's NAVAll.txt (sections for category and fund house) into a flat CSV."""
    category = fund_house = ""
    header: list[str] | None = None
    n = 0
    with raw.open(encoding="utf-8", errors="replace") as fh, \
            out_csv.open("w", newline="", encoding="utf-8") as oh:
        w = csv.writer(oh)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if ";" in line:
                parts = [p.strip() for p in line.split(";")]
                if header is None:
                    header = ["_".join(p.lower().replace("/", " ").split()) for p in parts]
                    w.writerow(["scheme_type", "category", "fund_house"] + header)
                    continue
                if not parts[0].isdigit():
                    continue
                stype, _, cat = category.partition("(")
                w.writerow([stype.strip(), cat.rstrip(")").strip(), fund_house] + parts)
                n += 1
            elif "Schemes" in line and "(" in line:
                category = line
            else:
                fund_house = line
    return n


# ----------------------------------------------------------------------------
# 2. NSE: daily equity bhavcopy + daily index values
# ----------------------------------------------------------------------------
def nse_candidates(kind: str, d: date) -> list[tuple[str, str]]:
    """(relative URL, file name) options for one day, newest format first."""
    mon = d.strftime("%b").upper()
    if kind == "index":
        name = f"ind_close_all_{d:%d%m%Y}.csv"
        return [(f"/content/indices/{name}", name)]
    full = f"sec_bhavdata_full_{d:%d%m%Y}.csv"            # 2020 onwards
    old = f"cm{d:%d}{mon}{d.year}bhav.csv.zip"            # up to mid-2024
    return [(f"/products/content/{full}", full),
            (f"/content/historical/EQUITIES/{d.year}/{mon}/{old}", old)]


def looks_valid(name: str, content: bytes) -> bool:
    if len(content) < 200:
        return False
    if name.endswith(".zip"):
        return content[:2] == b"PK"
    head = content[:300].decode("utf-8", errors="replace").lower()
    return "<html" not in head and ("symbol" in head or "index name" in head)


def fetch_nse_day(session: requests.Session, kind: str, d: date, out_root: Path, skip: set[str]) -> tuple[str, str]:
    key = d.isoformat()
    for _rel, name in nse_candidates(kind, d):
        if (out_root / str(d.year) / name).exists():
            return key, "cached"
    if key in skip:
        return key, "no_data"

    errors = []
    for rel, name in nse_candidates(kind, d):
        for base in NSE_BASES:
            url = base + rel
            try:
                r = get(session, url, timeout=45, retries=3)
            except RuntimeError as exc:
                errors.append(str(exc))
                continue
            if r is None:
                break                      # 404: try the next file format
            if not looks_valid(name, r.content):
                errors.append(f"{url} returned unexpected content")
                continue
            target = out_root / str(d.year) / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(r.content)
            manifest(f"nse_{kind}", key, "downloaded", target, url)
            time.sleep(0.25)               # be polite to NSE
            return key, "downloaded"
    if errors:
        manifest(f"nse_{kind}", key, "error", None, "", errors[-1][:200])
        return key, "error"
    return key, "no_data"                  # weekend-like holiday: nothing published


def step_nse(args) -> None:
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = [start + timedelta(n) for n in range((end - start).days + 1)]
    days = [d for d in days if d.weekday() < 5]          # Mon-Fri only
    log(f"NSE: {len(days):,} weekdays from {start} to {end}")

    for kind in ("index", "equity"):
        out_root = BRONZE / f"nse_{kind}"
        out_root.mkdir(parents=True, exist_ok=True)
        skip_file = out_root / "_no_data_days.json"
        skip = set(json.loads(skip_file.read_text())) if skip_file.exists() else set()
        # re-check the last 10 days in case files were published late
        recent = {(date.today() - timedelta(n)).isoformat() for n in range(10)}
        skip -= recent

        counts = {"downloaded": 0, "cached": 0, "no_data": 0, "error": 0}
        session = requests.Session()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(fetch_nse_day, session, kind, d, out_root, skip) for d in days]
            for i, fut in enumerate(as_completed(futures), 1):
                key, status = fut.result()
                counts[status] += 1
                if status == "no_data":
                    skip.add(key)
                if i % 200 == 0 or i == len(days):
                    log(f"  {kind}: {i:,}/{len(days):,}  {counts}")
                    skip_file.write_text(json.dumps(sorted(skip)))
        skip_file.write_text(json.dumps(sorted(skip)))
        have = counts["downloaded"] + counts["cached"]
        log(f"NSE {kind}: {have:,} trading-day files on disk, {counts['no_data']} holidays, {counts['error']} errors")
        if counts["error"]:
            log("  Some days failed. Run the same command again to retry only those days.")


# ----------------------------------------------------------------------------
# 3. mfapi.in: JSON NAV history for the curated funds (handy for daily refresh)
# ----------------------------------------------------------------------------
def step_mfapi(args) -> None:
    out = BRONZE / "mfapi"
    out.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    ok = 0
    for code, (cat, name) in CURATED_FUNDS.items():
        url = f"{MFAPI_BASE}/{code}"
        try:
            r = get(s, url, timeout=60)
            if r is None:
                raise RuntimeError("scheme not found")
            data = r.json()
            target = out / f"{code}.json"
            target.write_text(json.dumps(data), encoding="utf-8")
            rows = data.get("data", [])
            span = f"{rows[-1]['date']} to {rows[0]['date']}" if rows else "no rows"
            log(f"  {code} {name[:45]:45s} {len(rows):>5} rows  {span}")
            manifest("mfapi", str(code), "downloaded", target, url)
            ok += 1
        except Exception as exc:
            log(f"  {code} FAILED: {exc}")
            manifest("mfapi", str(code), "error", None, url, str(exc)[:200])
        time.sleep(0.3)
    log(f"mfapi: {ok}/{len(CURATED_FUNDS)} funds saved to {out}")


# ----------------------------------------------------------------------------
# 4. Yahoo Finance: Sensex (BSE index, not in NSE files) and Nifty 50
# ----------------------------------------------------------------------------
def step_sensex(args) -> None:
    try:
        import yfinance as yf
    except ImportError:
        log("yfinance is not installed. Run: pip install yfinance")
        return
    out = BRONZE / "yahoo"
    out.mkdir(parents=True, exist_ok=True)
    end = (date.fromisoformat(args.end) + timedelta(days=1)).isoformat()
    for ticker, fname in (("^BSESN", "sensex.csv"), ("^NSEI", "nifty50.csv")):
        try:
            df = yf.Ticker(ticker).history(start=args.start, end=end, auto_adjust=False)
            if df.empty:
                raise RuntimeError("no rows returned")
            df.index = df.index.date
            df.index.name = "date"
            target = out / fname
            df.to_csv(target)
            log(f"  {ticker}: {len(df):,} rows, {df.index.min()} to {df.index.max()}")
            manifest("yahoo", ticker, "downloaded", target, f"yfinance:{ticker}")
        except Exception as exc:
            log(f"  {ticker} FAILED: {exc}")
            manifest("yahoo", ticker, "error", None, f"yfinance:{ticker}", str(exc)[:200])


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------
def main() -> int:
    today = date.today()
    ten_years_ago = today.replace(year=today.year - 10)
    p = argparse.ArgumentParser(description="Download raw data into data/bronze/")
    p.add_argument("step", choices=["all", "amfi", "nse", "mfapi", "sensex"])
    p.add_argument("--start", default=ten_years_ago.isoformat(), help="first date, YYYY-MM-DD (default: 10 years ago)")
    p.add_argument("--end", default=today.isoformat(), help="last date, YYYY-MM-DD (default: today)")
    p.add_argument("--workers", type=int, default=4, help="parallel NSE downloads (keep this small)")
    p.add_argument("--refresh", action="store_true", help="re-download the AMFI archive even if present")
    args = p.parse_args()

    steps = {"amfi": step_amfi, "nse": step_nse, "mfapi": step_mfapi, "sensex": step_sensex}
    todo = list(steps) if args.step == "all" else [args.step]
    failed = []
    for name in todo:
        log(f"===== {name.upper()} =====")
        try:
            steps[name](args)
        except Exception as exc:
            log(f"ERROR in {name}: {exc}")
            failed.append(name)
    log("Done." + (f" Steps with errors: {', '.join(failed)}" if failed else " All steps finished."))
    log(f"Files are in {BRONZE.resolve()}  (log: {BRONZE / '_manifest.csv'})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
