# Retail Demand Forecasting and Optimization

A retail analytics project with code to turn daily sales, calendar, and price
data into weekly demand forecasts, product classifications, inventory policy
scenarios, and Power BI reporting. The Python pipeline uses a
Bronze/Silver/Gold data architecture and DuckDB/Parquet for data processing.

I built this project end to end, covering data engineering, forecasting
models, ABC/XYZ product analysis, inventory scenarios, and the Power BI
dashboard. The forecast metrics reported here are holdout evaluation results,
not measured business savings or stockout reductions.

![Executive Overview dashboard](docs/screenshots/executive-overview.png)

## Business problem and objectives

Retail teams need to understand sales patterns at product and store level,
compare demand forecasts, identify products with different revenue and
variability profiles, and examine how lead time and service-level assumptions
change reorder policies.

The project aims to:

- Preserve source data in a queryable Bronze layer and prepare reusable Silver
  and Gold datasets.
- Compare simple forecasting baselines with LightGBM using a chronological
  28-week holdout.
- Classify products by estimated revenue contribution (ABC) and weekly demand
  variability (XYZ).
- Explore reorder-point and safety-stock scenarios across selected lead times
  and target service levels.
- Present sales, forecast, and product views in Power BI.

## Architecture

```mermaid
flowchart LR
    A["Raw CSV files<br/>M5-style sales, calendar, prices"] --> B["Bronze<br/>DuckDB tables"]
    B --> C["Silver<br/>daily sales, calendar, prices"]
    C --> D["Gold<br/>weekly sales and analytics"]
    D --> E["Forecasting<br/>baselines and LightGBM"]
    D --> F["ABC/XYZ<br/>product classes"]
    D --> G["Inventory scenarios<br/>lead time × service level"]
    D --> H["Power BI CSV exports"]
    E --> G
    H --> I["Power BI report"]
```

## Pipeline and methods

### Bronze: ingest and validate

- [`src/ingestion/build_bronze.py`](src/ingestion/build_bronze.py) loads the
  calendar, sales validation/evaluation, sample submission, and sell-price CSVs
  into DuckDB tables in `data/bronze/retail_bronze.duckdb`.
- [`src/ingestion/validate_bronze.py`](src/ingestion/validate_bronze.py) checks
  calendar and sales keys, duplicate price keys, price quality, and submission
  IDs.

The ingestion script reads five CSVs from the directory configured in its
`DATASET` constant. That constant currently contains a machine-specific
absolute Windows path; update it to the directory containing the source CSVs
before running the pipeline on another machine. Raw competition data is not
included in this repository.

### Silver: daily sales and dimensions

- [`src/transformation/build_silver_sales.py`](src/transformation/build_silver_sales.py)
  unpivots the wide `bronze_sales_evaluation` table into one row per
  product/store/date and joins the day key to the Bronze calendar.
- The result is
  `data/processed/silver/sales_daily.parquet`, a daily long-format dataset with
  product, department, category, store, state, date, day key, and sales units.
- [`src/transformation/build_silver_dimensions.py`](src/transformation/build_silver_dimensions.py)
  writes typed calendar and sell-price Parquet files.
- [`src/transformation/build_silver_enriched.py`](src/transformation/build_silver_enriched.py)
  joins daily sales to calendar attributes and weekly sell prices.

The sales transformation uses the evaluation sales table. The validation sales
table is ingested and checked in Bronze, but is not used by this transformation.

### Gold: forecasting base and analytics

- [`src/forecasting/build_weekly_forecasting_base.py`](src/forecasting/build_weekly_forecasting_base.py)
  aggregates Silver daily units to product/store/week in
  `data/processed/gold/weekly_forecasting_base.parquet`.
- [`src/analytics/build_gold_sales.py`](src/analytics/build_gold_sales.py)
  uses enriched Silver data to create weekly item/store sales and revenue,
  monthly store/category performance, and store summaries.
- [`src/analytics/build_abc_xyz.py`](src/analytics/build_abc_xyz.py) calculates
  product-level estimated revenue and demand variability from weekly
  item/store data.
- [`src/analytics/build_monthly_demand_trends.py`](src/analytics/build_monthly_demand_trends.py),
  `build_monthly_seasonality.py`, and `build_top_products.py` create additional
  Gold analysis tables.

#### ABC/XYZ classes

ABC classes rank products by estimated revenue contribution: A covers the
leading 80% of cumulative revenue, B the next 15% (through 95%), and C the
remainder. The implementation uses cumulative revenue before each item when
assigning those bands.

XYZ classes use the coefficient of variation of weekly units
(`population standard deviation / mean weekly units`):

- X: CV at or below 0.50
- Y: CV above 0.50 and at or below 1.00
- Z: CV above 1.00
- N: mean weekly demand is not positive

Revenue is estimated from sales units multiplied by sell price; it is not a
profit or margin measure.

### Forecasting

The forecasting scripts are:

- [`src/forecasting/build_baseline_forecasts.py`](src/forecasting/build_baseline_forecasts.py):
  naive and 52-week seasonal-naive forecasts.
- [`src/forecasting/build_moving_average_forecasts.py`](src/forecasting/build_moving_average_forecasts.py):
  4-week and 13-week moving-average forecasts.
- [`src/forecasting/build_lgbm_features.py`](src/forecasting/build_lgbm_features.py):
  lag, rolling, and calendar features.
- [`src/forecasting/train_lightgbm.py`](src/forecasting/train_lightgbm.py):
  LightGBM training, holdout forecasts, and metrics.
- [`src/forecasting/build_model_comparison.py`](src/forecasting/build_model_comparison.py):
  overall and category-level model comparison.

The feature set includes lags of 1, 2, 3, 4, 8, 13, 26, and 52 weeks; 4- and
13-week rolling means; a 4-week rolling standard deviation; calendar fields;
and product/store hierarchy categories. Rolling windows stop before the target
week.

The last 28 weeks are held out chronologically. LightGBM is trained on rows
earlier than that window with a deterministic hash-selected training sample and
non-null 52-week lag. This is a rolling one-week-ahead evaluation: features for
a forecast week use preceding observed weeks, including earlier weeks in the
holdout once they have elapsed. It is not a single forecast issued once at the
start of the 28-week window.

### Inventory scenario analysis

[`src/inventory/build_inventory_scenarios.py`](src/inventory/build_inventory_scenarios.py)
creates nine combinations of lead time (1, 2, or 4 weeks) and target service
level (90%, 95%, or 99%) for each product/store series. It combines the latest
LightGBM weekly forecast with historical demand variability before the holdout
start to calculate lead-time demand, safety stock, and a rounded-up reorder
point. [`src/inventory/build_inventory_sensitivity.py`](src/inventory/build_inventory_sensitivity.py)
summarizes the policies and compares the 1-week/90% case with the 4-week/99%
case.

This is **inventory scenario analysis**, not a full inventory optimization
engine. It does not model inventory on hand, replenishment decisions, supplier
constraints, ordering costs, or realized stockout outcomes.

## Forecast results

WAPE is `100 × sum(abs(actual - forecast)) / sum(actual)` and is reported as a
percentage; lower is better. MAE and RMSE are in weekly units per evaluated
product/store forecast. Each model was evaluated on the same 853,720
product/store/week forecasts in the 28-week holdout.

| Rank | Model | WAPE | MAE | RMSE |
|---:|---|---:|---:|---:|
| 1 | LightGBM | 36.04% | 3.33 | 7.80 |
| 2 | Moving average (4-week) | 37.63% | 3.48 | 8.66 |
| 3 | Naive | 41.62% | 3.85 | 8.92 |
| 4 | Moving average (13-week) | 42.05% | 3.89 | 9.85 |
| 5 | Seasonal naive (52-week) | 64.24% | 5.94 | 14.36 |

The metric outputs are
`data/outputs/forecasting/lightgbm_model_metrics.parquet` and
`data/outputs/forecasting/model_comparison.parquet`. Those generated files are
ignored by Git, so the values above describe a local run and are not
recreated merely by cloning the repository. These metrics alone do not establish
business savings, improved availability, or stockout reduction.

## Power BI report

The PBIX file contains three report pages.

### 1. Executive Overview

Total units sold (66.9M), estimated revenue (191.6M), and average daily units
(34.3K), with the monthly sales trend from 2011 to 2016, units and revenue by
category, and category and date-range slicers.

![Executive Overview](docs/screenshots/executive-overview.png)

### 2. Forecast Performance

Weekly LightGBM actual-versus-forecast units across the holdout, WAPE by model,
and KPI cards for average weekly error (10.68K units), forecast accuracy
(100 − WAPE = 63.96%), and forecast bias (−1.91%), plus a week-level table.

![Forecast Performance](docs/screenshots/forecast-performance.png)

### 3. Product Intelligence

Top 10 products by estimated revenue and by units sold, estimated revenue by
ABC class split by XYZ demand variability, and the most volatile products by
demand coefficient of variation.

![Product Intelligence](docs/screenshots/product-intelligence.png)

The screenshots are exported from the PBIX and stored in `docs/screenshots/`.

### Opening the report

Open the PBIX in Power BI Desktop:

- `dashboard/retail-demand-optimization.pbix.pbix`

The editable PBIP project is also present:

- `dashboard/retail-demand-optimization.pbip`
- `dashboard/retail-demand-optimization.Report/`
- `dashboard/retail-demand-optimization.SemanticModel/`

**PBIP/PBIX consistency needs attention:** the PBIX contains the three pages
above, but the PBIP report definition currently lists only one generic
`Page 1`. Its semantic model currently contains two imported business tables,
`monthly_demand_trends` and `category_sales`, plus Power BI date tables. The
PBIP's Power Query definitions point to absolute local CSV paths
(`C:\Users\vivek\...`).

The PBIP report data is read from:

- `data/outputs/powerbi/monthly_demand_trends.csv`
- `data/outputs/powerbi/category_sales.csv`

These are generated by `src/analytics/export_powerbi_monthly_trends.py` and
`src/analytics/export_powerbi_category_sales.py`, respectively. Recreate them
after their Gold inputs are ready, then update the two Power Query file paths
in Power BI Desktop if the project is in a different directory. The
`data/outputs/powerbi/dashboard_data/` folder also contains generated CSVs, but
the checked PBIP semantic model does not currently reference those files.

## Technology stack

The declared Python dependencies in [`requirements.txt`](requirements.txt) are
DuckDB, PyArrow, pandas, NumPy, SQLAlchemy, psycopg, scikit-learn, matplotlib,
seaborn, JupyterLab, openpyxl, LightGBM, Streamlit, and Plotly. The pipeline
scripts directly use DuckDB, pandas, NumPy, and LightGBM; the full requirements
file also includes additional analysis and application packages.

## Repository layout and outputs

```text
.
├── dashboard/                  # PBIP report/model and PBIX report
├── data/
│   ├── raw/                    # source_inventory.csv; raw source CSVs are external
│   ├── bronze/                 # generated DuckDB database
│   ├── processed/
│   │   ├── silver/             # daily sales, calendar, prices, enriched sales
│   │   └── gold/               # weekly base, classifications, scenarios, trends
│   └── outputs/
│       ├── forecasting/        # forecasts and evaluation Parquet
│       └── powerbi/            # report-source and other generated CSVs
├── docs/
│   └── screenshots/            # Power BI dashboard page images
├── src/
│   ├── ingestion/
│   ├── transformation/
│   ├── analytics/
│   ├── forecasting/
│   └── inventory/
├── requirements.txt
└── README.md
```

Important generated outputs include:

- `data/processed/silver/sales_daily.parquet`
- `data/processed/gold/weekly_forecasting_base.parquet`
- `data/processed/gold/weekly_model_features.parquet`
- `data/processed/gold/product_abc_xyz.parquet`
- `data/processed/gold/inventory_scenario_summary.parquet`
- `data/processed/gold/inventory_sensitivity.parquet`
- `data/outputs/forecasting/lightgbm_model_metrics.parquet`
- `data/outputs/forecasting/model_comparison.parquet`
- `data/outputs/powerbi/monthly_demand_trends.csv`
- `data/outputs/powerbi/category_sales.csv`

Raw source data, the DuckDB database, generated Parquet, and generated output
files are excluded by `.gitignore`. The M5-style input CSVs are expected to be
available locally; they are not vendored here.

## Windows setup

Run these commands in PowerShell from the repository root:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation under the current policy, use the environment's
Python executable directly instead:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Before ingestion, edit the `DATASET` path in
`src/ingestion/build_bronze.py` to point to the directory containing
`calendar.csv`, `sales_train_validation.csv`, `sales_train_evaluation.csv`,
`sample_submission.csv`, and `sell_prices.csv`. Keep those raw files outside
Git.

## Run order

Run commands from the repository root after setup and after configuring the
source CSV path. Each stage consumes files generated by earlier stages.

```powershell
# Bronze
python src\ingestion\build_bronze.py
python src\ingestion\validate_bronze.py

# Silver
python src\transformation\build_silver_sales.py
python src\transformation\build_silver_dimensions.py
python src\transformation\validate_silver_sales.py
python src\transformation\validate_silver_dimensions.py
python src\transformation\build_silver_enriched.py
python src\transformation\validate_silver_enriched.py

# Gold sales and analytics
python src\analytics\build_gold_sales.py
python src\analytics\build_monthly_demand_trends.py
python src\analytics\build_abc_xyz.py
python src\analytics\build_monthly_seasonality.py
python src\analytics\build_top_products.py

# Forecasting
python src\forecasting\build_weekly_forecasting_base.py
python src\forecasting\build_baseline_forecasts.py
python src\forecasting\build_moving_average_forecasts.py
python src\forecasting\build_lgbm_features.py
python src\forecasting\train_lightgbm.py
python src\forecasting\build_model_comparison.py

# Inventory scenarios (uses the LightGBM forecast)
python src\inventory\build_inventory_scenarios.py
python src\inventory\build_inventory_sensitivity.py

# CSVs used by the current PBIP semantic model
python src\analytics\export_powerbi_monthly_trends.py
python src\analytics\export_powerbi_category_sales.py
```

The repository also includes standalone `validate_*.py` scripts under
`src/ingestion/`, `src/transformation/`, `src/analytics/`,
`src/forecasting/`, and `src/inventory/`. Run the validator for a stage after
its outputs have been generated.

## Validation and limitations

- There is no automated pytest or unittest suite yet.
- Validation is currently provided by the standalone scripts named
  `validate_*.py`. All 16 validation scripts passed against the current
  generated outputs; a fresh end-to-end rebuild has not been run.
- Source discovery is not portable until the hardcoded `DATASET` path in
  `build_bronze.py` is updated for the local machine.
- PBIP data queries also contain absolute paths and need to be repointed after
  moving the project. The PBIP page/model definitions do not currently match
  the three-page PBIX file.
- The reported holdout metrics are forecast-evaluation measures, not measured
  operational outcomes.
- Inventory outputs compare assumed policies; they are not a cost-optimized
  replenishment plan.

Possible next steps include parameterizing local data paths, aligning the PBIP
report/model with the three-page PBIX, adding automated tests for core metric
and transformation logic, and extending inventory analysis with actual stock,
cost, and replenishment constraints.

## Author

**Vivek Hegde** · [GitHub @vivekhegde-IS](https://github.com/vivekhegde-IS)

Designed and built the full project: data pipeline, forecasting models,
product classification, inventory scenario analysis, and Power BI dashboard.
