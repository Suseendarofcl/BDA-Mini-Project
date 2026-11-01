# Stock Market & Mutual Fund Risk Analytics Platform

Big Data Architecture project. Team: Suseendar Hari Krishnan, Lakshan, Viswesh.

A working big-data pipeline for Indian mutual funds and stocks. It downloads every NAV that AMFI
has published since 2006 (38,279 schemes, 37 million rows), stores it in Hadoop (HDFS), cleans
and analyses it with Apache Spark, and serves the results to a website and to Power BI through
an API that checks an API key. An add-on does the same for the Nifty 50 companies (share prices
and company debt) and for the Nifty 50 and Sensex indices.

```
AMFI NAV archive -> Python downloader -> HDFS Bronze -> Spark -> HDFS Silver -> Spark -> HDFS Gold
                                                                                           |
                                           Website, Power BI <- FastAPI (X-API-Key) <-----+
```

## What it produces

| Output                | Detail                                                                         |
|-----------------------|--------------------------------------------------------------------------------|
| Return metrics        | 1-year return, 3/5/10-year CAGR, value of Rs 1 lakh invested 10 years ago       |
| Risk metrics          | Volatility, maximum drawdown, beta, alpha, Sharpe, Sortino, VaR and CVaR (95%) |
| Risk score            | 0-100 score and Low/Medium/High band for 1,137 funds                           |
| Holding-period table  | Chance of gain and return range for 1, 3, 5, 7 and 10-year holds                |
| Model comparison      | Five forecasting models tested with walk-forward validation, 2019-2026         |
| Risk prediction       | Predicted risk band of each curated fund for the next quarter                  |
| Stocks and debt       | Return, risk, debt-to-equity, interest cover and risk score of the Nifty 50 companies |
| Indices               | Return and risk of the Nifty 50 and the Sensex, growth of Rs 1 lakh             |
| Website               | http://localhost:8000/ with top funds, risk, holding period, models, stocks and debt |
| Dashboard             | Power BI pages: Home, Risk, Holding Period, Models (and Stocks, optional)      |

## Folder contents

| File                 | Purpose                                                                  |
|----------------------|--------------------------------------------------------------------------|
| download_data.py     | Downloads the raw data into data/bronze/ and logs a manifest             |
| jobs/silver.py       | Spark job: cleans the NAVs and computes daily returns                    |
| jobs/gold.py         | Spark job: returns, risk metrics, risk score, holding-period analysis    |
| jobs/models.py       | Spark job: model comparison and risk prediction (Spark MLlib, ARIMA)     |
| download_stocks.py   | Downloads Nifty 50 share prices, index values and company debt           |
| jobs/stocks.py       | Spark job: stock returns and risk, company debt, index metrics           |
| run_stocks.sh        | Runs the stocks add-on in order with one command                         |
| api/main.py          | FastAPI service that serves the Gold tables and checks the API key       |
| web/index.html       | The website; it asks for the API key and reads the same API              |
| web/chart.umd.js     | Chart.js library, bundled so the site works without internet             |
| web/fonts/           | Chakra Petch typeface (SIL Open Font License), bundled for the same reason |
| run_pipeline.sh      | Runs everything in order with one command                                |
| start_hadoop.sh      | Starts SSH and HDFS                                                      |
| start_api.sh         | Creates the API key if needed and starts the API                         |
| powerbi/queries.md   | The Power BI queries and the layout of each dashboard page               |
| requirements.txt     | Python libraries                                                         |

## Requirements

- Windows with WSL (Ubuntu), or any Linux machine
- Java 11, Python 3.10 or newer
- Hadoop 3.4.3 in single-node mode (fs.defaultFS = hdfs://localhost:9000)
- Power BI Desktop
- About 4 GB of free disk space and 8 GB of RAM

## One-time setup

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk openssh-server python3-venv unzip wget

# passwordless SSH to this machine (Hadoop needs it)
sudo service ssh start
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh -o StrictHostKeyChecking=no localhost exit

# Hadoop
cd ~ && wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz
tar xzf hadoop-3.4.3.tar.gz && mv hadoop-3.4.3 hadoop
cat >> ~/.bashrc <<'EOT'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
EOT
source ~/.bashrc
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/hadoop/etc/hadoop/hadoop-env.sh
cat > ~/hadoop/etc/hadoop/core-site.xml <<EOT
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HOME/hadoop_data</value></property>
</configuration>
EOT
cat > ~/hadoop/etc/hadoop/hdfs-site.xml <<EOT
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
EOT
hdfs namenode -format      # only once, ever
```

## Run

```bash
bash start_hadoop.sh       # 1. start HDFS (web page: http://localhost:9870)
bash run_pipeline.sh       # 2. download, load into HDFS, run the three Spark jobs, export Gold
bash start_api.sh          # 3. start the API (leave this terminal open)
```

Then open the website at http://localhost:8000/ and paste the key from the `.api_key` file
(print it with `cat .api_key`). For Power BI, open the dashboard file and click Home -> Refresh.

`run_pipeline.sh` creates the Python environment on the first run and skips the download and
the HDFS upload when they are already done. A full run takes about 5 to 10 minutes on a laptop.

To run without Hadoop, on plain folders: `bash run_pipeline.sh --local`

## Stocks and company debt (add-on)

Run this after the main pipeline. It needs internet and takes about 3 to 5 minutes.

```bash
bash run_stocks.sh         # download, load into HDFS, run the Spark job, export
bash start_api.sh          # restart the API so it picks up the new tables
```

It downloads the official list of Nifty 50 companies from NSE, then ten years of daily prices
for each company and for the two indices, and the yearly balance sheets, from Yahoo Finance.
The Spark job writes four Gold tables: stock_metrics, stock_debt_history, index_metrics and
index_monthly. The website then shows a "Stocks and debt" section; without these tables the
section stays hidden and everything else works as before.

How debt is judged:

- Debt-to-equity = total debt / shareholders' equity. Up to 0.5 is Low, up to 1 is Moderate,
  above 1 is High.
- Interest cover = operating profit (EBIT) / interest expense. Below 3 is flagged as weak.
- Banks, insurers and lenders (NSE industry "Financial Services") are labelled "Financial
  company". Borrowing is their business, so their ratio is shown but not judged.
- Stock risk score: volatility 30, maximum drawdown 25, VaR 15, beta 15, debt-to-equity 15.
  Financial companies and companies without debt data are scored on the first four, scaled to 100.
  Risk metrics use the last five years, so a company listed for less than five years has no score.

To run without Hadoop: `bash run_stocks.sh --local`

## Power BI inside the website (optional)

1. Publish the report from Power BI Desktop (Home -> Publish -> My workspace).
2. In app.powerbi.com open the report -> File -> Embed report -> Publish to web (public), or
   Website or portal if Publish to web is turned off. Copy the link.
3. Save the link in the file web/powerbi_link.txt:
   `echo "PASTE-THE-LINK-HERE" > web/powerbi_link.txt`
4. Reload the website. A "Power BI" section appears at the end of the page and in the top menu.

If your organisation blocks embed codes ("Contact your admin to enable embed code creation"),
export the report as a PDF instead (app.powerbi.com: File -> Export -> PDF, or Power BI Desktop:
File -> Export -> Export to PDF) and save it as web/powerbi_report.pdf. The website then shows
the PDF in the same section. Without a link file or a PDF, the section stays hidden.

## Expected output

The pipeline prints these checkpoints (figures from the run of 2 October 2026; they change
slightly as new NAVs are published):

- Silver rows written: 31,187,540. Schemes: 29,097, curated: 30
- fund_metrics: 1,137 funds. Top fund by 10-year CAGR: Invesco India Midcap, 18.96%
- Forecasting: Random Forest RMSE 0.0487, baseline 0.0518 (5.9% better), others worse than baseline
- Risk-band prediction accuracy: Logistic Regression 61.2%, Random Forest 60.1%, baseline 56.7%

## API

Every endpoint except /health needs the header `X-API-Key: <contents of .api_key>`.
A missing or wrong key returns 401. Interactive test page: http://localhost:8000/docs

| Endpoint                     | Returns                                              |
|------------------------------|------------------------------------------------------|
| GET /                        | The website (the page itself needs no key; its data does) |
| GET /health                  | Status and row counts (no key needed)                |
| GET /funds                   | Return and risk metrics of the curated funds         |
| GET /funds/{code}/metrics    | Metrics of one fund                                  |
| GET /top-funds               | Curated funds ranked by a chosen return measure      |
| GET /risk-scores             | Risk score and band                                  |
| GET /holding-period          | Holding-period statistics                            |
| GET /nav-monthly             | Month-end NAV and growth of Rs 1 lakh                |
| GET /models/comparison       | Forecasting model results                            |
| GET /models/by-year          | Forecasting error per test year                      |
| GET /models/predictions      | Actual versus predicted monthly returns              |
| GET /risk-model/comparison   | Risk-band model results                              |
| GET /risk-prediction         | Predicted risk band for the next quarter             |
| GET /stocks                  | Return, risk, debt and risk score per Nifty 50 company (add-on) |
| GET /stocks/debt-history     | Debt-to-equity and interest cover per company and year (add-on) |
| GET /indices                 | Return and risk of the Nifty 50 and the Sensex (add-on) |
| GET /indices/monthly         | Month-end index values and growth of Rs 1 lakh (add-on) |

## Demo order

1. http://localhost:9870 -> Utilities -> Browse the file system -> /project (Bronze, Silver, Gold in HDFS)
2. `bash run_pipeline.sh` in the terminal (Spark jobs and their printed results)
3. http://localhost:8000/docs (a request without the key fails with 401)
4. http://localhost:8000/ (the website: enter a wrong key first to show it is rejected;
   end on the Stocks and debt section and click a company to show its debt by year)
5. Power BI: Home, Risk, Holding Period, Models

## Design choices and limits

- Scope: mutual funds in depth, plus the Nifty 50 companies and the two indices.
- Company debt covers about the last four financial years, which is what Yahoo Finance publishes.
  Yahoo Finance is an unofficial source; for a production system the figures would come from
  company filings.
- The stock list is today's Nifty 50, so companies that left the index earlier are not included
  (survivorship bias). Index churn, the history of companies entering and leaving the index,
  is not built, because no reliable dated list of the changes is available as open data.
- Benchmark for funds: the UTI Nifty 50 Index Fund stands in for the Nifty 50 index.
  Benchmark for stocks: the Nifty 50 index itself.
- Risk-free rate: assumed 6.5% a year (change with `--rf` on gold.py).
- Analysis universe: Growth options with a full 10-year history that are still active.
- Risk band: a fund's volatility rank among its peers (lowest, middle or highest third).
- Models are tested with walk-forward validation only; data is never shuffled.
- Hadoop runs on a single node and the API runs locally.

## Troubleshooting

| Problem                                         | Fix                                                       |
|-------------------------------------------------|-----------------------------------------------------------|
| "Hadoop is not running"                         | `bash start_hadoop.sh`                                    |
| `ModuleNotFoundError: No module named distutils`| `pip install setuptools` (Python 3.12)                    |
| A Spark job fails                               | Read the end of logs/<job>.log                            |
| run_stocks.sh: "Too little data came through"   | Yahoo is rate-limiting. Wait 10 minutes and run it again; finished files are kept |
| Website has no "Stocks and debt" section        | Run `bash run_stocks.sh`, restart the API, reload with Ctrl + Shift + R |
| Power BI: "Unable to connect"                   | The API is not running: `bash start_api.sh`               |
| Power BI: 401 Unauthorized                      | The key in the query does not match the file .api_key     |
| `sudo` password unknown in WSL                  | In PowerShell: `wsl -u root passwd <your-linux-username>` |

Data sources: AMFI NAV data through the open archive github.com/captn3m0/historical-mf-data;
Nifty 50 company list from NSE; share prices, index values and balance sheets from Yahoo Finance.
