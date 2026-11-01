"""
stocks.py - Bronze -> Silver -> Gold for stocks, company debt and the two indices

Reads   <base>/bronze/stocks/prices/        daily prices of the Nifty 50 companies
        <base>/bronze/stocks/indices/       daily values of the Nifty 50 and the Sensex
        <base>/bronze/stocks/constituents.csv, fundamentals.csv
Writes  <base>/silver/stock_daily           clean daily prices + daily return
        <base>/gold/stock_metrics           return, risk, debt and risk score per company
        <base>/gold/stock_debt_history      debt-to-equity and interest cover per company and year
        <base>/gold/index_metrics           return and risk of the Nifty 50 and the Sensex
        <base>/gold/index_monthly           month-end index values and growth of Rs 1 lakh

Run on Hadoop:
    spark-submit --driver-memory 2g jobs/stocks.py
Run on local files (no Hadoop):
    spark-submit --driver-memory 2g jobs/stocks.py --base file://$PWD/data
"""
import argparse
from datetime import timedelta

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

p = argparse.ArgumentParser()
p.add_argument("--base", default="hdfs://localhost:9000/project")
p.add_argument("--risk-years", type=int, default=5, help="window used for the risk metrics")
p.add_argument("--rf", type=float, default=0.065, help="annual risk-free rate (assumption)")
p.add_argument("--benchmark", default="NIFTY 50")
args = p.parse_args()
BASE = args.base.rstrip("/")
RF_DAILY = args.rf / 252

spark = (SparkSession.builder.appName("stocks")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Kolkata")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

PRICE = StructType([StructField("symbol", StringType()), StructField("date", DateType())] +
                   [StructField(c, DoubleType()) for c in ("open", "high", "low", "close", "volume")])
FUND = StructType([StructField("symbol", StringType()), StructField("fiscal_year_end", DateType())] +
                  [StructField(c, DoubleType()) for c in ("total_debt", "equity", "ebit", "interest_expense", "revenue", "net_income")])

stocks_raw = spark.read.csv(f"{BASE}/bronze/stocks/prices", header=True, schema=PRICE).withColumn("is_index", F.lit(False))
index_raw = spark.read.csv(f"{BASE}/bronze/stocks/indices", header=True, schema=PRICE).withColumn("is_index", F.lit(True))
cons = spark.read.csv(f"{BASE}/bronze/stocks/constituents.csv", header=True).select("symbol", "company", "industry")
fund = spark.read.csv(f"{BASE}/bronze/stocks/fundamentals.csv", header=True, schema=FUND)

# ------------------------------------------------------------------ silver
raw = stocks_raw.unionByName(index_raw)
n_raw = raw.count()
w = Window.partitionBy("symbol").orderBy("date")
daily = (raw.where(F.col("close").isNotNull() & (F.col("close") > 0) & F.col("date").isNotNull())
         .dropDuplicates(["symbol", "date"])
         .withColumn("prev", F.lag("close").over(w))
         .withColumn("gap", F.datediff("date", F.lag("date").over(w)))
         .withColumn("daily_return", F.col("close") / F.col("prev") - 1)
         .withColumn("daily_return", F.when((F.abs("daily_return") > 0.5) | (F.col("gap") > 10), None)
                     .otherwise(F.col("daily_return")))
         .select("symbol", "is_index", "date", "close", "volume", "daily_return"))
daily.coalesce(4).write.mode("overwrite").parquet(f"{BASE}/silver/stock_daily")
daily = spark.read.parquet(f"{BASE}/silver/stock_daily").cache()
n_silver = daily.count()


def years_before(d, n):
    try:
        return d.replace(year=d.year - n)
    except ValueError:
        return d.replace(year=d.year - n, day=28)


AS_OF = daily.where(F.col("symbol") == args.benchmark).agg(F.max("date")).first()[0] or daily.agg(F.max("date")).first()[0]
WIN_START = years_before(AS_OF, args.risk_years)
print(f"As-of date {AS_OF}, risk window starts {WIN_START}")

# ------------------------------------------------------------------ returns (stocks and indices together)
def close_on_or_before(target):
    t = f"date'{target}'"
    return F.expr(f"""CASE WHEN datediff({t}, max(date) FILTER (WHERE date <= {t})) <= 10
                           THEN max_by(close, date) FILTER (WHERE date <= {t}) END""")


targets = {n: years_before(AS_OF, n) for n in (1, 3, 5, 10)}
ret = daily.groupBy("symbol", "is_index").agg(
    F.min("date").alias("first_date"), F.max("date").alias("last_date"),
    F.max_by("close", "date").alias("last_close"),
    *[close_on_or_before(t).alias(f"c_{n}y") for n, t in targets.items()])
ret = ret.withColumn("return_1y", F.col("last_close") / F.col("c_1y") - 1)
for n in (3, 5, 10):
    ret = ret.withColumn(f"cagr_{n}y", F.pow(F.col("last_close") / F.col(f"c_{n}y"), 1.0 / n) - 1)
ret = ret.withColumn("value_of_1_lakh_10y", F.round(F.lit(100000) * F.col("last_close") / F.col("c_10y"), 0)) \
         .drop("c_1y", "c_3y", "c_5y", "c_10y")

# ------------------------------------------------------------------ risk (needs the full risk window)
full = F.broadcast(ret.where(F.col("first_date") <= F.lit(WIN_START + timedelta(days=30))).select("symbol"))
wd = daily.where(F.col("date") >= F.lit(WIN_START)).join(full, "symbol")
r = F.col("daily_return")
risk = wd.where(r.isNotNull()).groupBy("symbol").agg(
    (F.stddev_samp(r) * F.sqrt(F.lit(252.0))).alias("volatility"),
    F.avg(r).alias("mean_daily"),
    F.percentile_approx(r, 0.05, 10000).alias("var_95"))
risk = risk.withColumn("sharpe", (F.col("mean_daily") * 252 - F.lit(args.rf)) / F.col("volatility")).drop("mean_daily")
w_run = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
mdd = (wd.withColumn("peak", F.max("close").over(w_run))
       .groupBy("symbol").agg(F.min(F.col("close") / F.col("peak") - 1).alias("max_drawdown")))
bench = F.broadcast(wd.where(F.col("symbol") == args.benchmark).select("date", r.alias("rb")).where("rb is not null"))
ba = (wd.where(r.isNotNull()).join(bench, "date").groupBy("symbol").agg(
    (F.covar_samp(r, F.col("rb")) / F.var_samp("rb")).alias("beta"), F.avg(r).alias("m_r"), F.avg("rb").alias("m_rb")))
ba = ba.withColumn("alpha", ((F.col("m_r") - RF_DAILY) - F.col("beta") * (F.col("m_rb") - RF_DAILY)) * 252).select("symbol", "beta", "alpha")

metrics = (ret.join(risk, "symbol", "left").join(mdd, "symbol", "left").join(ba, "symbol", "left")
           .withColumn("as_of_date", F.lit(AS_OF)))

# ------------------------------------------------------------------ company debt
debt = (fund.withColumn("fiscal_year", F.year("fiscal_year_end"))
        .withColumn("debt_to_equity", F.when(F.col("equity") > 0, F.col("total_debt") / F.col("equity")))
        .withColumn("interest_coverage", F.when(F.abs("interest_expense") > 0, F.col("ebit") / F.abs("interest_expense")))
        .withColumn("total_debt_cr", F.round(F.col("total_debt") / 1e7, 0))       # rupees -> crore
        .withColumn("equity_cr", F.round(F.col("equity") / 1e7, 0)))
debt_hist = (debt.join(F.broadcast(cons), "symbol", "left")
             .select("symbol", "company", "industry", "fiscal_year", "fiscal_year_end", "total_debt_cr", "equity_cr",
                     "debt_to_equity", "interest_coverage"))
debt_hist.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/stock_debt_history")

with_de = debt.where(F.col("debt_to_equity").isNotNull())
latest = with_de.groupBy("symbol").agg(
    F.max_by("debt_to_equity", "fiscal_year_end").alias("debt_to_equity"),
    F.max_by("interest_coverage", "fiscal_year_end").alias("interest_coverage"),
    F.max_by("total_debt_cr", "fiscal_year_end").alias("total_debt_cr"),
    F.max("fiscal_year").alias("debt_year"),
    F.min_by("debt_to_equity", "fiscal_year_end").alias("de_first"),
    F.count("*").alias("debt_years"))
latest = latest.withColumn("de_change", F.col("debt_to_equity") - F.col("de_first")).drop("de_first")

# ------------------------------------------------------------------ stock table with debt flags and risk score
st = (metrics.where(~F.col("is_index")).drop("is_index").join(F.broadcast(cons), "symbol", "left").join(latest, "symbol", "left")
      .withColumn("company", F.coalesce("company", "symbol"))
      .withColumn("is_financial", F.coalesce(F.col("industry") == "Financial Services", F.lit(False)))
      # Banks and lenders borrow as their business, so their debt ratio is not judged like other companies'.
      .withColumn("debt_level",
                  F.when(F.col("is_financial"), "Financial company")
                   .when(F.col("debt_to_equity").isNull(), "No data")
                   .when(F.col("debt_to_equity") <= 0.5, "Low")
                   .when(F.col("debt_to_equity") <= 1.0, "Moderate").otherwise("High"))
      .withColumn("weak_interest_cover",
                  F.when(F.col("is_financial") | F.col("interest_coverage").isNull(), None)
                   .otherwise(F.col("interest_coverage") < 3)))

# Risk score: volatility 30, drawdown 25, VaR 15, beta 15, debt-to-equity 15.
# Financial companies and companies without debt data are scored on the first four, scaled to 100.
scored = st.where(F.col("volatility").isNotNull() & F.col("beta").isNotNull())
one = Window.partitionBy(F.lit(1))
rk = lambda c: F.percent_rank().over(one.orderBy(c)) * 100                          # noqa: E731
scored = (scored.withColumn("rank_volatility", rk(F.col("volatility")))
          .withColumn("rank_drawdown", rk(-F.col("max_drawdown")))
          .withColumn("rank_var", rk(-F.col("var_95")))
          .withColumn("rank_beta", rk(F.col("beta"))))
de_rank = (scored.where(~F.col("is_financial") & F.col("debt_to_equity").isNotNull())
           .withColumn("rank_debt", rk(F.col("debt_to_equity"))).select("symbol", "rank_debt"))
scored = scored.join(de_rank, "symbol", "left")
four = (F.col("rank_volatility") * 30 + F.col("rank_drawdown") * 25 + F.col("rank_var") * 15 + F.col("rank_beta") * 15)
scored = (scored.withColumn("risk_score", F.round(F.when(F.col("rank_debt").isNotNull(), (four + F.col("rank_debt") * 15) / 100)
                                                   .otherwise(four / 85), 1))
          .withColumn("risk_band", F.when(F.col("risk_score") <= 33, "Low").when(F.col("risk_score") <= 66, "Medium").otherwise("High"))
          .select("symbol", "risk_score", "risk_band"))
stock_metrics = st.join(scored, "symbol", "left").cache()
n_stocks = stock_metrics.count()
stock_metrics.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/stock_metrics")

# ------------------------------------------------------------------ indices
index_metrics = metrics.where(F.col("is_index")).drop("is_index", "beta", "alpha")
index_metrics.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/index_metrics")
ten = years_before(AS_OF, 10)
im = (daily.where(F.col("is_index") & (F.col("date") >= F.lit(ten)))
      .withColumn("month", F.trunc("date", "month"))
      .groupBy("symbol", "month").agg(F.max_by("close", "date").alias("close"), F.max("date").alias("date")))
# growth starts from the same price as value_of_1_lakh_10y, so the last point matches that column
base10 = daily.where(F.col("is_index")).groupBy("symbol").agg(close_on_or_before(ten).alias("base"))
im = im.join(F.broadcast(base10), "symbol") \
       .withColumn("base", F.coalesce("base", F.first("close").over(
           Window.partitionBy("symbol").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))) \
       .withColumn("growth_of_1_lakh", F.round(F.lit(100000) * F.col("close") / F.col("base"), 0)) \
       .select(F.col("symbol").alias("index_name"), "date", "close", "growth_of_1_lakh")
im.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/gold/index_monthly")

# ------------------------------------------------------------------ report
pc = lambda c: F.round(F.col(c) * 100, 2)                                         # noqa: E731
print("=" * 78)
print(f"Rows read {n_raw:,} -> clean rows {n_silver:,}")
print(f"stock_metrics: {n_stocks} companies   with a risk score: {stock_metrics.where('risk_score is not null').count()}"
      f"   with debt data: {stock_metrics.where('debt_to_equity is not null').count()}")
print("\nIndices:")
index_metrics.select("symbol", pc("return_1y").alias("return_1y_%"), pc("cagr_5y").alias("cagr_5y_%"),
                     pc("cagr_10y").alias("cagr_10y_%"), pc("volatility").alias("volatility_%"),
                     pc("max_drawdown").alias("max_drawdown_%")).show(truncate=False)
print("Top 5 companies by 5-year CAGR:")
(stock_metrics.orderBy(F.desc_nulls_last("cagr_5y"))
 .select("symbol", "industry", pc("cagr_5y").alias("cagr_5y_%"), pc("volatility").alias("volatility_%"),
         F.round("debt_to_equity", 2).alias("debt_to_equity"), "debt_level", "risk_band").show(5, truncate=30))
print("Highest debt-to-equity (non-financial companies):")
(stock_metrics.where("not is_financial and debt_to_equity is not null").orderBy(F.desc("debt_to_equity"))
 .select("symbol", "industry", F.round("debt_to_equity", 2).alias("debt_to_equity"),
         F.round("interest_coverage", 1).alias("interest_cover"), "debt_year", "debt_level").show(5, truncate=30))
print(f"Output: {BASE}/gold/")
print("=" * 78)
spark.stop()
