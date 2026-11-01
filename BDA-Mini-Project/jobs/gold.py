"""
gold.py - Silver -> Gold (return metrics, risk metrics, risk score, holding-period analysis)

Reads   <base>/silver/nav_daily, <base>/silver/scheme_dim
Writes  <base>/gold/fund_metrics          returns + risk metrics, one row per fund
        <base>/gold/risk_scores           0-100 risk score and Low/Medium/High band
        <base>/gold/holding_period_stats  chance of gain for 1/3/5/7/10-year holds
        <base>/gold/nav_monthly           month-end NAV and growth of Rs 1 lakh (curated funds)

Run on Hadoop:
    spark-submit --driver-memory 3g jobs/gold.py
Run on local files (no Hadoop):
    spark-submit --driver-memory 3g jobs/gold.py --base file://$PWD/data
"""
import argparse
from datetime import timedelta

from pyspark.sql import SparkSession, Window, functions as F

p = argparse.ArgumentParser()
p.add_argument("--base", default="hdfs://localhost:9000/project")
p.add_argument("--risk-years", type=int, default=10, help="window used for the risk metrics")
p.add_argument("--rf", type=float, default=0.065,
               help="annual risk-free rate (assumption: roughly the 91-day T-bill yield)")
p.add_argument("--benchmark", type=int, default=120716,
               help="scheme used as the market: UTI Nifty 50 Index Fund - Direct - Growth")
args = p.parse_args()
BASE = args.base.rstrip("/")
RF_DAILY = args.rf / 252

spark = (SparkSession.builder.appName("gold")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.session.timeZone", "Asia/Kolkata")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

nav = spark.read.parquet(f"{BASE}/silver/nav_daily")
dim = spark.read.parquet(f"{BASE}/silver/scheme_dim")


def years_before(d, n):
    try:
        return d.replace(year=d.year - n)
    except ValueError:               # 29 Feb
        return d.replace(year=d.year - n, day=28)


# "As of" date = latest NAV date of the curated funds
AS_OF = dim.where("is_curated").agg(F.max("last_date")).first()[0]
WIN_START = years_before(AS_OF, args.risk_years)
print(f"As-of date {AS_OF}, risk window starts {WIN_START}")

# ------------------------------------------------------------------ universe
# Growth options only: IDCW/dividend plans drop in NAV on every payout, which
# would look like a loss. The fund must cover the whole risk window and still be active.
name = F.lower(F.col("scheme_name"))
universe = (dim
            .withColumn("is_growth", name.contains("growth")
                        & ~name.rlike("idcw|dividend|bonus|payout|reinvest"))
            .where(F.col("is_growth") | F.col("is_curated"))
            .where(F.col("first_date") <= F.lit(WIN_START + timedelta(days=30)))
            .where(F.col("last_date") >= F.lit(AS_OF - timedelta(days=15)))
            .select("scheme_code", "scheme_name", "short_name", "category", "is_curated"))
print(f"Funds in the analysis universe: {universe.count():,}")
universe = F.broadcast(universe)

u_nav = nav.join(universe.select("scheme_code"), "scheme_code")      # all history of those funds
w_nav = u_nav.where(F.col("date") >= F.lit(WIN_START))               # risk window only

# ------------------------------------------------------------------ returns
def nav_on_or_before(target):
    """NAV on the last trading day on or before `target` (null if more than 10 days stale)."""
    t = f"date'{target}'"
    return F.expr(f"""CASE WHEN datediff({t}, max(date) FILTER (WHERE date <= {t})) <= 10
                           THEN max_by(nav, date) FILTER (WHERE date <= {t}) END""")


targets = {n: years_before(AS_OF, n) for n in (1, 3, 5, 10)}
ret = u_nav.groupBy("scheme_code").agg(
    F.max("date").alias("last_date"),
    F.max_by("nav", "date").alias("nav_last"),
    *[nav_on_or_before(t).alias(f"nav_{n}y_ago") for n, t in targets.items()])
ret = ret.withColumn("return_1y", F.col("nav_last") / F.col("nav_1y_ago") - 1)
for n in (3, 5, 10):
    ret = ret.withColumn(f"cagr_{n}y", F.pow(F.col("nav_last") / F.col(f"nav_{n}y_ago"), 1.0 / n) - 1)
ret = ret.withColumn("absolute_return_10y", F.col("nav_last") / F.col("nav_10y_ago") - 1)
ret = ret.withColumn("value_of_1_lakh_10y", F.round(F.lit(100000) * F.col("nav_last") / F.col("nav_10y_ago"), 0))

# ------------------------------------------------------------------ risk
r = F.col("daily_return")
risk = w_nav.where(r.isNotNull()).groupBy("scheme_code").agg(
    F.count("*").alias("n_returns"),
    F.avg(r).alias("mean_daily"),
    (F.stddev_samp(r) * F.sqrt(F.lit(252.0))).alias("volatility"),
    (F.sqrt(F.avg(F.pow(F.least(r - F.lit(RF_DAILY), F.lit(0.0)), 2))) * F.sqrt(F.lit(252.0))).alias("downside_dev"),
    F.percentile_approx(r, 0.05, 10000).alias("var_95"))
risk = (risk
        .withColumn("annual_return", F.col("mean_daily") * 252)
        .withColumn("sharpe", (F.col("annual_return") - F.lit(args.rf)) / F.col("volatility"))
        .withColumn("sortino", (F.col("annual_return") - F.lit(args.rf)) / F.col("downside_dev")))

# CVaR = average loss on the days worse than VaR
cvar = (w_nav.join(F.broadcast(risk.select("scheme_code", "var_95")), "scheme_code")
        .where(r <= F.col("var_95"))
        .groupBy("scheme_code").agg(F.avg(r).alias("cvar_95")))

# Maximum drawdown = biggest fall from a previous peak
w_run = Window.partitionBy("scheme_code").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
mdd = (w_nav.withColumn("peak", F.max("nav").over(w_run))
       .groupBy("scheme_code").agg(F.min(F.col("nav") / F.col("peak") - 1).alias("max_drawdown")))

# Beta and alpha against the Nifty 50 index fund
bench = F.broadcast(w_nav.where(F.col("scheme_code") == args.benchmark)
                    .select("date", F.col("daily_return").alias("rb")).where("rb is not null"))
ba = (w_nav.where(r.isNotNull()).join(bench, "date")
      .groupBy("scheme_code").agg(
          (F.covar_samp(r, F.col("rb")) / F.var_samp("rb")).alias("beta"),
          F.avg(r).alias("m_r"), F.avg("rb").alias("m_rb")))
ba = ba.withColumn("alpha", ((F.col("m_r") - RF_DAILY) - F.col("beta") * (F.col("m_rb") - RF_DAILY)) * 252) \
       .select("scheme_code", "beta", "alpha")

metrics = (universe.join(ret, "scheme_code").join(risk, "scheme_code")
           .join(cvar, "scheme_code", "left").join(mdd, "scheme_code", "left").join(ba, "scheme_code", "left")
           .where(F.col("n_returns") >= 200 * args.risk_years)        # enough trading days in the window
           .where(F.col("volatility").isNotNull())
           .withColumn("as_of_date", F.lit(AS_OF))
           .drop("mean_daily", "nav_1y_ago", "nav_3y_ago", "nav_5y_ago", "nav_10y_ago"))
metrics = metrics.cache()
n_metrics = metrics.count()
metrics.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/fund_metrics")

# ------------------------------------------------------------------ risk score (0-100)
# Weights from the design: volatility 30, drawdown 25, VaR 15, beta 15, debt-to-equity 15.
# Funds have no debt-to-equity, so its 15 is spread over the other four.
W = {"volatility": 30 / 85, "drawdown": 25 / 85, "var": 15 / 85, "beta": 15 / 85}


def score(df, suffix, partition):
    wp = lambda c: Window.partitionBy(*partition).orderBy(c)          # noqa: E731
    df = (df.withColumn("rank_volatility", F.percent_rank().over(wp(F.col("volatility"))) * 100)
            .withColumn("rank_drawdown", F.percent_rank().over(wp(-F.col("max_drawdown"))) * 100)
            .withColumn("rank_var", F.percent_rank().over(wp(-F.col("var_95"))) * 100)
            .withColumn("rank_beta", F.percent_rank().over(wp(F.col("beta"))) * 100))
    s = (F.col("rank_volatility") * W["volatility"] + F.col("rank_drawdown") * W["drawdown"]
         + F.col("rank_var") * W["var"] + F.col("rank_beta") * W["beta"])
    df = df.withColumn(f"risk_score{suffix}", F.round(s, 1))
    return df.withColumn(f"risk_band{suffix}",
                         F.when(F.col(f"risk_score{suffix}") <= 33, "Low")
                          .when(F.col(f"risk_score{suffix}") <= 66, "Medium").otherwise("High"))


base_cols = ["scheme_code", "scheme_name", "short_name", "category", "is_curated",
             "volatility", "max_drawdown", "var_95", "beta"]
rs_all = score(metrics.select(*base_cols).withColumn("_p", F.lit(1)), "", ["_p"]).drop("_p")
rs_cur = (score(metrics.where("is_curated").select(*base_cols).withColumn("_p", F.lit(1)), "_curated", ["_p"])
          .select("scheme_code", "risk_score_curated", "risk_band_curated"))
risk_scores = rs_all.join(rs_cur, "scheme_code", "left").withColumn("as_of_date", F.lit(AS_OF))
risk_scores.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/risk_scores")

# ------------------------------------------------------------------ holding-period analysis
# Month-end NAVs of the curated funds, then every possible N-year window (one per start month).
cur_codes = F.broadcast(dim.where("is_curated").select("scheme_code", "short_name", "category"))
monthly = (nav.join(cur_codes, "scheme_code")
           .withColumn("month", F.trunc("date", "month"))
           .groupBy("scheme_code", "short_name", "category", "month")
           .agg(F.max_by("nav", "date").alias("nav"), F.max("date").alias("date")))
wm = Window.partitionBy("scheme_code").orderBy("month")
hp = None
for n in (1, 3, 5, 7, 10):
    x = (monthly.withColumn("end_nav", F.lead("nav", 12 * n).over(wm))
         .where(F.col("end_nav").isNotNull())
         .withColumn("cagr", F.pow(F.col("end_nav") / F.col("nav"), 1.0 / n) - 1)
         .groupBy("scheme_code", "short_name", "category")
         .agg(F.count("*").alias("n_windows"),
              F.round(F.avg(F.when(F.col("cagr") > 0, 1.0).otherwise(0.0)) * 100, 1).alias("pct_windows_positive"),
              F.percentile_approx("cagr", 0.5, 10000).alias("median_cagr"),
              F.percentile_approx("cagr", 0.1, 10000).alias("cagr_p10"),
              F.percentile_approx("cagr", 0.9, 10000).alias("cagr_p90"),
              F.min("cagr").alias("worst_cagr"), F.max("cagr").alias("best_cagr"))
         .withColumn("holding_years", F.lit(n)))
    hp = x if hp is None else hp.unionByName(x)
hp.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/holding_period_stats")

# ------------------------------------------------------------------ monthly NAV for charts
wfirst = Window.partitionBy("scheme_code").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
nav_monthly = (monthly.where(F.col("month") >= F.trunc(F.lit(WIN_START), "month"))
               .withColumn("growth_of_1_lakh", F.round(F.lit(100000) * F.col("nav") / F.first("nav").over(wfirst), 0))
               .select("scheme_code", "short_name", "category", "date", "nav", "growth_of_1_lakh"))
nav_monthly.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/nav_monthly")

# ------------------------------------------------------------------ report
print("=" * 72)
print(f"fund_metrics         : {n_metrics:,} funds")
print(f"risk_scores          : {spark.read.parquet(f'{BASE}/gold/risk_scores').count():,} funds")
print(f"holding_period_stats : {spark.read.parquet(f'{BASE}/gold/holding_period_stats').count():,} rows")
print(f"nav_monthly          : {spark.read.parquet(f'{BASE}/gold/nav_monthly').count():,} rows")
print("\nTop 5 curated funds by 10-year CAGR:")
(metrics.where("is_curated").orderBy(F.desc("cagr_10y"))
 .select("short_name", F.round(F.col("cagr_10y") * 100, 2).alias("cagr_10y_%"),
         F.round(F.col("volatility") * 100, 2).alias("volatility_%"),
         F.round(F.col("max_drawdown") * 100, 2).alias("max_drawdown_%"),
         F.round("sharpe", 2).alias("sharpe"))
 .show(5, truncate=48))
print(f"Output: {BASE}/gold/")
print("=" * 72)
spark.stop()
