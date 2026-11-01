# Power BI queries

Power BI reads eight tables from the API. Each one is a Blank Query
(Home -> Get data -> Blank query -> Advanced Editor) with this code.
Only the `RelativePath` line changes. Replace YOUR-API-KEY with the text in the `.api_key` file.

```
let
    Source = Json.Document(Web.Contents("http://localhost:8000", [
        RelativePath = "funds",
        Headers = [#"X-API-Key" = "YOUR-API-KEY"]
    ])),
    Result = Table.FromRecords(Source)
in
    Result
```

After each query: select all columns (Ctrl + A), then Transform -> Detect Data Type.
If asked how to connect, choose Anonymous.

| Table name in Power BI   | RelativePath              | Rows  |
|--------------------------|---------------------------|-------|
| funds                    | "funds"                   | 30    |
| risk_scores              | "risk-scores"             | 30    |
| holding_period           | "holding-period"          | 150   |
| nav_monthly              | "nav-monthly"             | 3,630 |
| model_comparison         | "models/comparison"       | 5     |
| model_by_year            | "models/by-year"          | 40    |
| risk_model_comparison    | "risk-model/comparison"   | 3     |
| risk_prediction          | "risk-prediction"         | 30    |

Relationships (Model view): funds[scheme_code] -> nav_monthly[scheme_code] and
funds[scheme_code] -> holding_period[scheme_code].

## Dashboard pages

1. Home: bar chart of cagr_10y by short_name; card with Maximum of value_of_1_lakh_10y;
   line chart of growth_of_1_lakh by date with short_name as legend; slicer on category.
2. Risk: scatter of volatility (X) against cagr_10y (Y) with short_name as values and category
   as legend; bar chart of risk_score_curated by short_name with risk_band_curated as legend;
   table of short_name, volatility, max_drawdown, beta, sharpe.
3. Holding Period: column chart of Average pct_windows_positive by holding_years; line chart of
   Average cagr_p10, median_cagr and cagr_p90 by holding_years; slicer on short_name.
4. Models: bar charts of rmse and directional_accuracy by model; line chart of Average rmse by
   target_year with model as legend; tables from risk_model_comparison and risk_prediction.

## Stocks and debt (optional fifth page)

After `bash run_stocks.sh`, add these four queries the same way.

| Table name in Power BI   | RelativePath              | Rows (about) |
|--------------------------|---------------------------|--------------|
| stocks                   | "stocks"                  | 50           |
| stock_debt_history       | "stocks/debt-history"     | 200          |
| indices                  | "indices"                 | 2            |
| index_monthly            | "indices/monthly"         | 242          |

Page "Stocks": bar chart of cagr_5y by company; scatter of debt_to_equity (X) against cagr_5y (Y)
with company as values and debt_level as legend; table of company, industry, debt_to_equity,
interest_coverage, debt_level, risk_band; column chart of Average debt_to_equity by fiscal_year
from stock_debt_history with a slicer on company; line chart of growth_of_1_lakh by date with
index_name as legend from index_monthly.

To refresh after re-running the pipeline: restart the API, then Home -> Refresh.
