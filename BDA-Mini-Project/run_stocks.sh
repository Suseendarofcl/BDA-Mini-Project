#!/usr/bin/env bash
# Stocks and company-debt add-on: download -> Bronze -> Spark job -> export for the API.
# It only adds new tables; the mutual fund pipeline is not touched.
#
#   bash run_stocks.sh            run on Hadoop (HDFS must be running: bash start_hadoop.sh)
#   bash run_stocks.sh --local    run on plain folders, without Hadoop
set -euo pipefail
cd "$(dirname "$0")"
MODE=hadoop
[[ "${1:-}" == "--local" ]] && MODE=local
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
step() { echo; echo "=================================================================="; echo "  $1"; echo "=================================================================="; }
source venv/bin/activate
python -c "import yfinance" 2>/dev/null || pip install -q yfinance

step "1/4  Download stock prices, index values and company debt (Yahoo Finance)"
python download_stocks.py

if [[ "$MODE" == "hadoop" ]]; then
  BASE="hdfs://localhost:9000/project"
  step "2/4  Load into Hadoop"
  if ! hdfs dfs -ls / >/dev/null 2>&1; then echo "Hadoop is not running. Start it first:  bash start_hadoop.sh"; exit 1; fi
  hdfs dfs -mkdir -p /project/bronze
  hdfs dfs -rm -r -f -skipTrash /project/bronze/stocks >/dev/null 2>&1 || true
  hdfs dfs -put data/bronze/stocks /project/bronze/
  hdfs dfs -ls /project/bronze/stocks
else
  BASE="file://$PWD/data"
  step "2/4  Local mode: using the folder data/ instead of Hadoop"
fi

step "3/4  Spark job: stocks, company debt and indices"
mkdir -p logs
if ! spark-submit --driver-memory 2g jobs/stocks.py --base "$BASE" 2> logs/stocks.log; then
  echo; echo "FAILED: jobs/stocks.py. Last lines of logs/stocks.log:"; tail -25 logs/stocks.log; exit 1
fi

step "4/4  Export Gold tables for the API"
mkdir -p gold_export
for t in stock_metrics stock_debt_history index_metrics index_monthly; do
  rm -rf "gold_export/$t"
  if [[ "$MODE" == "hadoop" ]]; then hdfs dfs -get "/project/gold/$t" "gold_export/$t"; else cp -r "data/gold/$t" "gold_export/$t"; fi
done
ls gold_export
echo
echo "Done. Restart the API (Ctrl + C, then: bash start_api.sh) and reload the website."
