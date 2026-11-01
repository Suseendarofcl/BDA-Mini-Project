#!/usr/bin/env bash
# Runs the whole pipeline: download -> Bronze -> Silver -> Gold -> models -> export for the API.
#
#   bash run_pipeline.sh            run on Hadoop (HDFS must be running: bash start_hadoop.sh)
#   bash run_pipeline.sh --local    run on plain folders, without Hadoop
set -euo pipefail
cd "$(dirname "$0")"
MODE=hadoop
[[ "${1:-}" == "--local" ]] && MODE=local
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
step() { echo; echo "=================================================================="; echo "  $1"; echo "=================================================================="; }
run_job() {   # run one Spark job; keep Spark's log in logs/ and show it only on failure
  mkdir -p logs
  if ! spark-submit --driver-memory 2g "jobs/$1.py" --base "$BASE" 2> "logs/$1.log"; then
    echo; echo "FAILED: jobs/$1.py. Last lines of logs/$1.log:"; tail -25 "logs/$1.log"; exit 1
  fi
}
START=$(date +%s)

step "0/6  Python environment"
[[ -d venv ]] || python3 -m venv venv
source venv/bin/activate
python -c "import pyspark, statsmodels, fastapi, zstandard, pyarrow, setuptools" 2>/dev/null || pip install -q -r requirements.txt
echo "Python environment ready."

step "1/6  Download raw data (skipped if already downloaded)"
if [[ -f data/bronze/amfi/nav_parquet/_SUCCESS ]]; then
  echo "Raw data already in data/bronze/amfi."
else
  python download_data.py amfi
fi

if [[ "$MODE" == "hadoop" ]]; then
  BASE="hdfs://localhost:9000/project"
  step "2/6  Load Bronze into Hadoop (skipped if already there)"
  if ! hdfs dfs -ls / >/dev/null 2>&1; then
    echo "Hadoop is not running. Start it first:  bash start_hadoop.sh"; exit 1
  fi
  if hdfs dfs -test -e /project/bronze/amfi/nav_parquet/_SUCCESS; then
    echo "Bronze is already in HDFS at /project/bronze/amfi."
  else
    hdfs dfs -mkdir -p /project/bronze/amfi
    hdfs dfs -put -f data/bronze/amfi/nav_parquet data/bronze/amfi/schemes.csv \
        data/bronze/amfi/curated_universe.csv data/bronze/amfi/funds.db.zst /project/bronze/amfi/
    echo "Bronze copied to HDFS."
  fi
else
  BASE="file://$PWD/data"
  step "2/6  Local mode: using the folder data/ instead of Hadoop"
fi

step "3/6  Spark job: Bronze -> Silver (clean)"
run_job silver

step "4/6  Spark job: Silver -> Gold (returns, risk, risk score, holding period)"
run_job gold

step "5/6  Spark job: models (forecast comparison and risk prediction)"
run_job models

step "6/6  Export Gold tables for the API"
rm -rf gold_export
if [[ "$MODE" == "hadoop" ]]; then hdfs dfs -get /project/gold gold_export; else cp -r data/gold gold_export; fi
ls gold_export

echo
echo "Pipeline finished in $(( ($(date +%s) - START) / 60 )) min $(( ($(date +%s) - START) % 60 )) s."
echo "Next:  bash start_api.sh    then refresh Power BI."
