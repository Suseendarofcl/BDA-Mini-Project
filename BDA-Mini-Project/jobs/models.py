"""
models.py - Silver -> Gold (which model performs better + risk prediction)

Part 1  Return forecasting: predict NEXT MONTH's return of each curated fund.
        Models: Historical mean (baseline), ARIMA(1,0,1), Linear Regression,
                Random Forest, Gradient-Boosted Trees (the last three in Spark MLlib).
        Tested with walk-forward validation: train on all years before the test
        year, test on that year, then roll forward one year. Never shuffled.

Part 2  Risk prediction: predict NEXT QUARTER's risk band of each fund. The band
        is the fund's volatility rank among its peers that quarter (lowest third
        = Low, middle = Medium, highest = High), like the risk score.
        Models: Persistence (baseline: "same as this quarter"),
                Logistic Regression, Random Forest (Spark MLlib).

Reads   <base>/silver/nav_daily, <base>/silver/scheme_dim
Writes  <base>/gold/model_comparison        one row per forecasting model
        <base>/gold/model_by_year           error of each model in each test year
        <base>/gold/model_predictions       actual vs predicted, per fund and month
        <base>/gold/risk_model_comparison   one row per risk-band model
        <base>/gold/risk_prediction         predicted risk band for next quarter, per fund

Run on Hadoop:
    spark-submit --driver-memory 2g jobs/models.py
Run on local files (no Hadoop):
    spark-submit --driver-memory 2g jobs/models.py --base file://$PWD/data
"""
import argparse
import warnings

import numpy as np
import pandas as pd
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession, Window, functions as F

p = argparse.ArgumentParser()
p.add_argument("--base", default="hdfs://localhost:9000/project")
p.add_argument("--first-test-year", type=int, default=2019)
p.add_argument("--benchmark", type=int, default=120716,
               help="scheme used as the market: UTI Nifty 50 Index Fund - Direct - Growth")
args = p.parse_args()
BASE = args.base.rstrip("/")

spark = (SparkSession.builder.appName("models")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "Asia/Kolkata")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

nav = spark.read.parquet(f"{BASE}/silver/nav_daily")
dim = spark.read.parquet(f"{BASE}/silver/scheme_dim")
curated = F.broadcast(dim.where("is_curated").select("scheme_code", "short_name", "category"))
daily = nav.join(curated, "scheme_code").cache()
print(f"Daily rows for the curated funds: {daily.count():,}")


def to_pd(df) -> pd.DataFrame:
    """Spark -> pandas through plain Python rows (works with any pandas / pyarrow version)."""
    return pd.DataFrame([r.asDict() for r in df.collect()], columns=df.columns)


def write_gold(pdf: pd.DataFrame, name: str) -> None:
    """pandas -> Spark through plain Python values, then write the Gold table."""
    cols = list(pdf.columns)
    rows = list(zip(*[pdf[c].tolist() for c in cols]))
    spark.createDataFrame(rows, schema=cols).coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/{name}")


# =============================================================================
# PART 1 - RETURN FORECASTING
# =============================================================================
# ---- monthly table: one row per fund per month ------------------------------
monthly = (daily.withColumn("month", F.trunc("date", "month"))
           .groupBy("scheme_code", "short_name", "category", "month")
           .agg(F.max_by("nav", "date").alias("nav"),
                F.count("*").alias("n_days"),
                (F.stddev_samp("daily_return") * F.sqrt(F.lit(252.0))).alias("vol_1m"))
           .where("n_days >= 15"))                                  # drop incomplete months

w = Window.partitionBy("scheme_code").orderBy("month")
monthly = monthly.withColumn("ret", F.col("nav") / F.lag("nav").over(w) - 1)

# market return of the same month (Nifty 50 index fund)
mkt = monthly.where(F.col("scheme_code") == args.benchmark).select("month", F.col("ret").alias("mkt_ret"))
monthly = monthly.join(F.broadcast(mkt), "month", "left")

# ---- features known at the end of a month, target = NEXT month's return -----
feat = (monthly
        .withColumn("ret_lag1", F.lag("ret", 1).over(w))
        .withColumn("ret_lag2", F.lag("ret", 2).over(w))
        .withColumn("mom_3m", F.col("nav") / F.lag("nav", 3).over(w) - 1)
        .withColumn("mom_6m", F.col("nav") / F.lag("nav", 6).over(w) - 1)
        .withColumn("mom_12m", F.col("nav") / F.lag("nav", 12).over(w) - 1)
        .withColumn("vol_3m", F.avg("vol_1m").over(w.rowsBetween(-2, 0)))
        .withColumn("target", F.lead("ret", 1).over(w))
        .withColumn("target_month", F.lead("month", 1).over(w))
        .withColumn("target_year", F.year("target_month")))
FEATURES = ["ret", "ret_lag1", "ret_lag2", "mom_3m", "mom_6m", "mom_12m", "vol_1m", "vol_3m", "mkt_ret"]
feat = feat.dropna(subset=FEATURES + ["target"])
feat = VectorAssembler(inputCols=FEATURES, outputCol="features").transform(feat).coalesce(2).cache()

last_year = feat.agg(F.max("target_year")).first()[0]
TEST_YEARS = list(range(args.first_test_year, last_year + 1))
print(f"Forecasting rows: {feat.count():,}   test years: {TEST_YEARS[0]}-{TEST_YEARS[-1]}")

KEYS = ["scheme_code", "short_name", "category", "target_month", "target_year", "target"]
ML_MODELS = {
    "Linear Regression": lambda: LinearRegression(featuresCol="features", labelCol="target", regParam=0.01),
    "Random Forest": lambda: RandomForestRegressor(featuresCol="features", labelCol="target",
                                                   numTrees=100, maxDepth=4, seed=42),
    "Gradient-Boosted Trees": lambda: GBTRegressor(featuresCol="features", labelCol="target",
                                                   maxIter=40, maxDepth=3, stepSize=0.05, seed=42),
}

preds = []
for year in TEST_YEARS:
    train = feat.where(F.col("target_year") < year)
    test = feat.where(F.col("target_year") == year)
    for name, make in ML_MODELS.items():
        out = to_pd(make().fit(train).transform(test).select(*KEYS, F.col("prediction").alias("predicted")))
        out["model"] = name
        preds.append(out)
    print(f"  walk-forward fold {year}: Spark MLlib models done")

# ---- baseline and ARIMA work on each fund's own monthly return series -------
series = to_pd(monthly.where("ret is not null").select("scheme_code", "month", "ret"))
series["month"] = pd.to_datetime(series["month"])
all_truth = pd.concat([p_[KEYS] for p_ in preds if p_["model"].iloc[0] == "Linear Regression"])
all_truth["target_month"] = pd.to_datetime(all_truth["target_month"])

from statsmodels.tsa.arima.model import ARIMA  # noqa: E402

warnings.filterwarnings("ignore")
rows_naive, rows_arima = [], []
for code, g in series.groupby("scheme_code"):
    g = g.sort_values("month").reset_index(drop=True)
    for year in TEST_YEARS:
        tr = g[g["month"].dt.year < year]
        te = g[g["month"].dt.year == year]
        if len(tr) < 24 or te.empty:
            continue
        # Baseline: predict the fund's average past monthly return
        rows_naive += [(code, m, tr["ret"].mean()) for m in te["month"]]
        # ARIMA(1,0,1): fit on the past, then one-step-ahead forecasts through the test year
        try:
            fit = ARIMA(tr["ret"].to_numpy(), order=(1, 0, 1)).fit()
            upto = g[g["month"].dt.year <= year]["ret"].to_numpy()
            fc = fit.apply(upto).predict(start=len(tr), end=len(upto) - 1)
            rows_arima += list(zip([code] * len(te), te["month"], fc))
        except Exception:
            rows_arima += [(code, m, tr["ret"].mean()) for m in te["month"]]
print("  baseline and ARIMA done")

for name, rows in (("Historical mean (baseline)", rows_naive), ("ARIMA(1,0,1)", rows_arima)):
    d = pd.DataFrame(rows, columns=["scheme_code", "target_month", "predicted"])
    d = all_truth.merge(d, on=["scheme_code", "target_month"], how="inner")
    d["model"] = name
    preds.append(d)

P = pd.concat(preds, ignore_index=True)
P["target_month"] = pd.to_datetime(P["target_month"])
P["error"] = P["predicted"] - P["target"]


def reg_metrics(g: pd.DataFrame) -> pd.Series:
    return pd.Series({
        "rmse": float(np.sqrt((g["error"] ** 2).mean())),
        "mae": float(g["error"].abs().mean()),
        "directional_accuracy": float((np.sign(g["predicted"]) == np.sign(g["target"])).mean() * 100),
        "n_predictions": int(len(g)),
    })


comparison = P.groupby("model").apply(reg_metrics).reset_index().sort_values("rmse")
base_rmse = comparison.loc[comparison["model"].str.contains("baseline"), "rmse"].iloc[0]
comparison["rmse_vs_baseline_pct"] = (comparison["rmse"] / base_rmse - 1) * 100
comparison["rank"] = range(1, len(comparison) + 1)
comparison["n_predictions"] = comparison["n_predictions"].astype(int)

by_year = P.groupby(["model", "target_year"]).apply(reg_metrics).reset_index()
by_year["n_predictions"] = by_year["n_predictions"].astype(int)

out_pred = P.rename(columns={"target": "actual", "target_month": "date", "target_year": "year"})[
    ["model", "scheme_code", "short_name", "category", "date", "year", "actual", "predicted"]]
out_pred["date"] = out_pred["date"].dt.date

write_gold(comparison, "model_comparison")
write_gold(by_year, "model_by_year")
write_gold(out_pred, "model_predictions")

# =============================================================================
# PART 2 - RISK-BAND PREDICTION
# =============================================================================
# Band = where a fund's volatility ranks among the curated funds in that quarter:
# lowest third = Low, middle third = Medium, highest third = High. This is the same
# peer-relative idea as the risk score, so no cut-off is carried across years.
wq = Window.partitionBy("scheme_code", "quarter").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
q = (daily.withColumn("quarter", F.trunc("date", "quarter"))
     .withColumn("dd", F.col("nav") / F.max("nav").over(wq) - 1)
     .groupBy("scheme_code", "short_name", "category", "quarter")
     .agg((F.stddev_samp("daily_return") * F.sqrt(F.lit(252.0))).alias("vol_q"),
          (F.exp(F.sum(F.log1p("daily_return"))) - 1).alias("ret_q"),
          F.min("dd").alias("dd_q"),
          F.count("*").alias("n_days"))
     .where("n_days >= 50"))                                         # complete quarters only

wx = lambda c: Window.partitionBy("quarter").orderBy(c)              # noqa: E731  rank within a quarter
ws = Window.partitionBy("scheme_code").orderBy("quarter")
band = lambda c: F.when(F.col(c) <= 1 / 3, 0.0).when(F.col(c) <= 2 / 3, 1.0).otherwise(2.0)  # noqa: E731
q = (q.withColumn("vol_rank", F.percent_rank().over(wx("vol_q")))
      .withColumn("ret_rank", F.percent_rank().over(wx("ret_q")))
      .withColumn("dd_rank", F.percent_rank().over(wx(-F.col("dd_q"))))
      .withColumn("current", band("vol_rank"))
      .withColumn("rank_lag1", F.lag("vol_rank", 1).over(ws))
      .withColumn("rank_lag2", F.lag("vol_rank", 2).over(ws))
      .withColumn("rank_lag3", F.lag("vol_rank", 3).over(ws))
      .withColumn("rank_avg4", F.avg("vol_rank").over(ws.rowsBetween(-3, 0)))
      .withColumn("label", F.lead("current", 1).over(ws))
      .withColumn("next_quarter", F.lead("quarter", 1).over(ws)))
Q_FEATURES = ["vol_rank", "rank_lag1", "rank_lag2", "rank_lag3", "rank_avg4", "ret_rank", "dd_rank"]
qf = VectorAssembler(inputCols=Q_FEATURES, outputCol="features").transform(q.dropna(subset=Q_FEATURES)).coalesce(2).cache()
labelled = qf.where("label is not null").withColumn("next_year", F.year("next_quarter"))
BANDS = ["Low", "Medium", "High"]

CLF = {
    "Logistic Regression": lambda: LogisticRegression(featuresCol="features", labelCol="label", maxIter=100),
    "Random Forest": lambda: RandomForestClassifier(featuresCol="features", labelCol="label",
                                                    numTrees=100, maxDepth=4, seed=42),
}
cls = []
for year in TEST_YEARS:
    train = labelled.where(F.col("next_year") < year)
    test = labelled.where(F.col("next_year") == year)
    if test.limit(1).count() == 0:
        continue
    base = to_pd(test.select("scheme_code", "next_quarter", "label", F.col("current").alias("prediction")))
    base["model"] = "Persistence (baseline)"
    cls.append(base)
    for name, make in CLF.items():
        o = to_pd(make().fit(train).transform(test).select("scheme_code", "next_quarter", "label", "prediction"))
        o["model"] = name
        cls.append(o)
    print(f"  risk-band fold {year} done")
C = pd.concat(cls, ignore_index=True)


def clf_metrics(g: pd.DataFrame) -> pd.Series:
    f1s, rec_high = [], np.nan
    for k in (0.0, 1.0, 2.0):
        tp = ((g["prediction"] == k) & (g["label"] == k)).sum()
        fp = ((g["prediction"] == k) & (g["label"] != k)).sum()
        fn = ((g["prediction"] != k) & (g["label"] == k)).sum()
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1s.append(2 * prec * rec / (prec + rec) if prec + rec else 0.0)
        if k == 2.0:
            rec_high = rec
    return pd.Series({"accuracy": float((g["prediction"] == g["label"]).mean() * 100),
                      "macro_f1": float(np.mean(f1s)),
                      "recall_high_band": float(rec_high * 100),
                      "n_predictions": int(len(g))})


risk_cmp = C.groupby("model").apply(clf_metrics).reset_index().sort_values("accuracy", ascending=False)
risk_cmp["n_predictions"] = risk_cmp["n_predictions"].astype(int)
risk_cmp["rank"] = range(1, len(risk_cmp) + 1)
write_gold(risk_cmp, "risk_model_comparison")

# ---- forecast for the coming quarter: train the best ML model on everything,
#      then predict from the latest complete quarter
best_name = risk_cmp[~risk_cmp["model"].str.contains("baseline")]["model"].iloc[0]
final = CLF[best_name]().fit(labelled)
latest_q = qf.agg(F.max("quarter")).first()[0]
fp = to_pd(final.transform(qf.where(F.col("quarter") == F.lit(latest_q)))
           .select("scheme_code", "short_name", "category", "quarter", "vol_q",
                   "current", "prediction", "probability"))
nxt = (pd.Timestamp(latest_q) + pd.offsets.QuarterBegin(1, startingMonth=1)).date()
risk_pred = pd.DataFrame({
    "scheme_code": fp["scheme_code"], "short_name": fp["short_name"], "category": fp["category"],
    "based_on_quarter": pd.to_datetime(fp["quarter"]).dt.date,
    "predicted_for_quarter": nxt,
    "model": best_name,
    "current_volatility": fp["vol_q"],
    "current_band": fp["current"].map(lambda v: BANDS[int(v)]),
    "predicted_band": fp["prediction"].map(lambda v: BANDS[int(v)]),
    "prob_low": fp["probability"].map(lambda v: float(v[0])),
    "prob_medium": fp["probability"].map(lambda v: float(v[1])),
    "prob_high": fp["probability"].map(lambda v: float(v[2])),
})
write_gold(risk_pred, "risk_prediction")

# =============================================================================
# REPORT
# =============================================================================
pd.set_option("display.width", 200)
print("=" * 78)
print("RETURN FORECASTING - next month's return, walk-forward", f"{TEST_YEARS[0]}-{TEST_YEARS[-1]}")
print(comparison[["rank", "model", "rmse", "mae", "directional_accuracy", "rmse_vs_baseline_pct",
                  "n_predictions"]].round(4).to_string(index=False))
print("\nRISK-BAND PREDICTION - next quarter's band (Low / Medium / High)")
print(risk_cmp.round(3).to_string(index=False))
print(f"\nPredicted band for the quarter starting {nxt}, using {best_name}:")
print(risk_pred["predicted_band"].value_counts().to_string())
print(f"\nOutput: {BASE}/gold/")
print("=" * 78)
spark.stop()
