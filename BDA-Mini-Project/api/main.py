"""
api/main.py - serves the Gold tables to Power BI, protected by an API key.

Start it (from the project folder):
    export API_KEY=your-secret-key
    uvicorn api.main:app --host 0.0.0.0 --port 8000

Every request except /health must send the header   X-API-Key: your-secret-key
Website:               http://localhost:8000/
Interactive test page: http://localhost:8000/docs
"""
import datetime
import json
import os
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles

GOLD_DIR = Path(os.environ.get("GOLD_DIR", "gold_export"))
API_KEY = os.environ.get("API_KEY", "")
if not API_KEY:
    raise SystemExit("API_KEY is not set. Run:  export API_KEY=your-secret-key   and start again.")

TABLES = ["fund_metrics", "risk_scores", "holding_period_stats", "nav_monthly"]
missing = [t for t in TABLES if not (GOLD_DIR / t).exists()]
if missing:
    raise SystemExit(f"Gold tables not found in '{GOLD_DIR}': {missing}. "
                     "Copy them first:  hdfs dfs -get /project/gold gold_export")

# Tables written by jobs/models.py. They are optional, so the API still starts without them.
MODEL_TABLES = ["model_comparison", "model_by_year", "model_predictions",
                "risk_model_comparison", "risk_prediction"]

# Gold tables are small, so load them once at start-up.
DATA = {t: pd.read_parquet(GOLD_DIR / t) for t in TABLES}
DATA.update({t: pd.read_parquet(GOLD_DIR / t) for t in MODEL_TABLES if (GOLD_DIR / t).exists()})

# Tables written by jobs/stocks.py (the stocks and company-debt add-on). Also optional.
STOCK_TABLES = ["stock_metrics", "stock_debt_history", "index_metrics", "index_monthly"]
DATA.update({t: pd.read_parquet(GOLD_DIR / t) for t in STOCK_TABLES if (GOLD_DIR / t).exists()})

app = FastAPI(title="Mutual Fund Risk Analytics API", version="1.0")
key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def check_key(key: str = Security(key_header)) -> None:
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Missing or wrong X-API-Key header")


def records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> JSON-safe list (NaN becomes null, dates become YYYY-MM-DD)."""
    return json.loads(df.to_json(orient="records", date_format="iso", date_unit="s", double_precision=6))


def model_table(name: str) -> pd.DataFrame:
    if name not in DATA:
        job = "run_stocks.sh" if name in STOCK_TABLES else "run_pipeline.sh"
        raise HTTPException(status_code=404, detail=f"'{name}' is not available yet. Run {job}, then restart the API.")
    return DATA[name]


def fmt_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Turn every date column into YYYY-MM-DD text."""
    df = df.copy()
    for c in df.columns:
        first = df[c].dropna().iloc[0] if df[c].notna().any() else None
        if isinstance(first, (datetime.date, pd.Timestamp)):
            df[c] = pd.to_datetime(df[c]).dt.strftime("%Y-%m-%d")
    return df


# ---- website: http://localhost:8000/  (the page asks for the API key, then calls the endpoints below)
WEB_DIR = Path(__file__).resolve().parent.parent / "web"
if (WEB_DIR / "index.html").exists():
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def website():
        return FileResponse(WEB_DIR / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "tables": {t: len(d) for t, d in DATA.items()}}


@app.get("/funds", dependencies=[Depends(check_key)])
def funds(curated_only: bool = True):
    """Return and risk metrics. curated_only=false returns every fund in the analysis."""
    df = DATA["fund_metrics"]
    if curated_only:
        df = df[df["is_curated"]]
    return records(fmt_dates(df))


@app.get("/funds/{scheme_code}/metrics", dependencies=[Depends(check_key)])
def fund_metrics(scheme_code: int):
    df = DATA["fund_metrics"]
    df = df[df["scheme_code"] == scheme_code]
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Scheme {scheme_code} is not in the analysis")
    return records(fmt_dates(df))[0]


@app.get("/top-funds", dependencies=[Depends(check_key)])
def top_funds(by: str = "cagr_10y", n: int = 10):
    """Top curated funds, ranked by cagr_10y, cagr_5y, cagr_3y, return_1y or sharpe."""
    allowed = {"cagr_10y", "cagr_5y", "cagr_3y", "return_1y", "sharpe"}
    if by not in allowed:
        raise HTTPException(status_code=400, detail=f"'by' must be one of {sorted(allowed)}")
    df = DATA["fund_metrics"]
    df = df[df["is_curated"]].sort_values(by, ascending=False).head(max(1, min(n, 50)))
    return records(fmt_dates(df))


@app.get("/risk-scores", dependencies=[Depends(check_key)])
def risk_scores(curated_only: bool = True):
    df = DATA["risk_scores"]
    if curated_only:
        df = df[df["is_curated"]]
    return records(fmt_dates(df))


@app.get("/holding-period", dependencies=[Depends(check_key)])
def holding_period():
    return records(DATA["holding_period_stats"].sort_values(["scheme_code", "holding_years"]))


@app.get("/nav-monthly", dependencies=[Depends(check_key)])
def nav_monthly():
    return records(fmt_dates(DATA["nav_monthly"].sort_values(["scheme_code", "date"])))


@app.get("/models/comparison", dependencies=[Depends(check_key)])
def models_comparison():
    """Which forecasting model performs better: RMSE, MAE and directional accuracy per model."""
    return records(model_table("model_comparison").sort_values("rank"))


@app.get("/models/by-year", dependencies=[Depends(check_key)])
def models_by_year():
    """Error of each forecasting model in each walk-forward test year."""
    return records(model_table("model_by_year").sort_values(["target_year", "model"]))


@app.get("/models/predictions", dependencies=[Depends(check_key)])
def models_predictions():
    """Actual versus predicted next-month return, per model, fund and month."""
    return records(fmt_dates(model_table("model_predictions").sort_values(["model", "scheme_code", "date"])))


@app.get("/risk-model/comparison", dependencies=[Depends(check_key)])
def risk_model_comparison():
    """Accuracy of the models that predict next quarter's risk band."""
    return records(model_table("risk_model_comparison").sort_values("rank"))


@app.get("/risk-prediction", dependencies=[Depends(check_key)])
def risk_prediction():
    """Predicted risk band of each curated fund for the coming quarter."""
    return records(fmt_dates(model_table("risk_prediction").sort_values("prob_high", ascending=False)))


@app.get("/stocks", dependencies=[Depends(check_key)])
def stocks():
    """Return, risk, debt and risk score of each Nifty 50 company."""
    return records(fmt_dates(model_table("stock_metrics").sort_values("cagr_5y", ascending=False)))


@app.get("/stocks/debt-history", dependencies=[Depends(check_key)])
def stocks_debt_history():
    """Debt-to-equity and interest cover per company and financial year."""
    return records(fmt_dates(model_table("stock_debt_history").sort_values(["symbol", "fiscal_year"])))


@app.get("/indices", dependencies=[Depends(check_key)])
def indices():
    """Return and risk of the Nifty 50 and the Sensex."""
    return records(fmt_dates(model_table("index_metrics")))


@app.get("/indices/monthly", dependencies=[Depends(check_key)])
def indices_monthly():
    """Month-end index values and growth of Rs 1 lakh over ten years."""
    return records(fmt_dates(model_table("index_monthly").sort_values(["index_name", "date"])))
