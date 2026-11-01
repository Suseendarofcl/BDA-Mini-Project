"""
silver.py - Bronze -> Silver (clean the raw mutual-fund NAV data)

Reads   <base>/bronze/amfi/nav_parquet        every NAV of every scheme
        <base>/bronze/amfi/schemes.csv        scheme code -> name
        <base>/bronze/amfi/curated_universe.csv   the 30 funds we study in depth
Writes  <base>/silver/nav_daily               clean daily NAV + daily return
        <base>/silver/scheme_dim              one row per scheme

Run on Hadoop:
    spark-submit --driver-memory 3g jobs/silver.py
Run on local files (no Hadoop):
    spark-submit --driver-memory 3g jobs/silver.py --base file://$PWD/data
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F

p = argparse.ArgumentParser()
p.add_argument("--base", default="hdfs://localhost:9000/project")
p.add_argument("--start", default="2013-01-01", help="keep NAVs from this date (direct plans began Jan 2013)")
args = p.parse_args()
BASE = args.base.rstrip("/")

spark = (SparkSession.builder.appName("silver")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.session.timeZone", "Asia/Kolkata")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------- read bronze
nav = spark.read.parquet(f"{BASE}/bronze/amfi/nav_parquet")
schemes = spark.read.csv(f"{BASE}/bronze/amfi/schemes.csv", header=True, inferSchema=True)
curated = spark.read.csv(f"{BASE}/bronze/amfi/curated_universe.csv", header=True, inferSchema=True)

raw_rows = nav.count()
print(f"Bronze rows read: {raw_rows:,}")

# ---------------------------------------------------------------- clean
clean = (nav
         .select(F.col("scheme_code").cast("int").alias("scheme_code"),
                 F.col("date").cast("date").alias("date"),
                 F.col("nav").cast("double").alias("nav"))
         .where(F.col("date") >= F.lit(args.start))
         .where(F.col("nav").isNotNull() & (F.col("nav") > 0))      # bad or missing NAVs
         .dropDuplicates(["scheme_code", "date"]))                  # repeated rows

# daily return = today's NAV / previous NAV - 1, computed per scheme
w = Window.partitionBy("scheme_code").orderBy("date")
clean = (clean
         .withColumn("prev_nav", F.lag("nav").over(w))
         .withColumn("gap_days", F.datediff("date", F.lag("date").over(w)))
         .withColumn("daily_return", F.col("nav") / F.col("prev_nav") - 1)
         # A jump of more than 50% in a few days is a data glitch (unit change,
         # re-based NAV), not a real return. Keep the NAV, blank the return.
         .withColumn("daily_return",
                     F.when((F.abs("daily_return") > 0.5) | (F.col("gap_days") > 10), None)
                      .otherwise(F.col("daily_return")))
         .withColumn("year", F.year("date"))
         .drop("prev_nav", "gap_days"))

(clean.repartition("year")
      .write.mode("overwrite").partitionBy("year")
      .parquet(f"{BASE}/silver/nav_daily"))

# ---------------------------------------------------------------- scheme dimension
silver_nav = spark.read.parquet(f"{BASE}/silver/nav_daily")
span = silver_nav.groupBy("scheme_code").agg(
    F.min("date").alias("first_date"), F.max("date").alias("last_date"), F.count("*").alias("n_days"))

dim = (span.join(schemes.select(F.col("scheme_code").cast("int").alias("scheme_code"), "scheme_name"),
                 "scheme_code", "left")
           .join(curated.select(F.col("scheme_code").cast("int").alias("scheme_code"),
                                F.col("category"), F.col("scheme_name").alias("short_name")),
                 "scheme_code", "left")
           .withColumn("is_curated", F.col("category").isNotNull()))

dim.coalesce(1).write.mode("overwrite").parquet(f"{BASE}/silver/scheme_dim")

# ---------------------------------------------------------------- report
silver_rows = silver_nav.count()
print("=" * 60)
print(f"Silver rows written : {silver_rows:,}  (removed {raw_rows - silver_rows:,} old, bad or duplicate rows)")
print(f"Schemes             : {dim.count():,}   curated: {dim.where('is_curated').count()}")
r = silver_nav.agg(F.min("date"), F.max("date")).first()
print(f"Date range          : {r[0]} to {r[1]}")
print(f"Output              : {BASE}/silver/")
print("=" * 60)
spark.stop()
