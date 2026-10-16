from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

BASE = GOLD / "weekly_forecasting_base.parquet"
FORECASTS = GOLD / "baseline_forecasts.parquet"
METRICS = GOLD / "baseline_model_metrics.parquet"

TEMP.mkdir(parents=True, exist_ok=True)

def sql_path(path):
    return path.as_posix().replace("'", "''")

con = duckdb.connect()

try:
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{sql_path(TEMP)}'")
    con.execute("SET max_temp_directory_size = '50GB'")

    print("Preparing chronological holdout and historical lags...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE history AS
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY week_start DESC
            ) AS weeks_from_end,
            LAG(weekly_units_sold, 1) OVER (
                PARTITION BY id ORDER BY week_start
            ) AS naive_forecast,
            LAG(weekly_units_sold, 52) OVER (
                PARTITION BY id ORDER BY week_start
            ) AS seasonal_naive_forecast
        FROM read_parquet('{sql_path(BASE)}')
    """)

    print("Generating both baseline forecasts...")

    con.execute(f"""
        COPY (
            SELECT
                id, item_id, dept_id, cat_id, store_id, state_id,
                week_start,
                weekly_units_sold AS actual_units,
                'naive' AS model_name,
                naive_forecast AS forecast_units
            FROM history
            WHERE weeks_from_end <= 28

            UNION ALL

            SELECT
                id, item_id, dept_id, cat_id, store_id, state_id,
                week_start,
                weekly_units_sold AS actual_units,
                'seasonal_naive_52' AS model_name,
                seasonal_naive_forecast AS forecast_units
            FROM history
            WHERE weeks_from_end <= 28
        )
        TO '{sql_path(FORECASTS)}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("Calculating evaluation metrics...")

    con.execute(f"""
        COPY (
            SELECT
                model_name,
                COUNT(*) AS evaluated_forecasts,
                AVG(ABS(actual_units - forecast_units)) AS mae,
                100.0 * SUM(ABS(actual_units - forecast_units))
                    / NULLIF(SUM(actual_units), 0) AS wape_pct,
                SQRT(AVG(POWER(actual_units - forecast_units, 2)))
                    AS rmse,
                SUM(actual_units) AS total_actual_units,
                SUM(forecast_units) AS total_forecast_units
            FROM read_parquet('{sql_path(FORECASTS)}')
            WHERE forecast_units IS NOT NULL
            GROUP BY model_name
        )
        TO '{sql_path(METRICS)}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("\nBaseline model metrics:")
    for row in con.execute(f"""
        SELECT
            model_name,
            evaluated_forecasts,
            ROUND(mae, 4),
            ROUND(wape_pct, 2),
            ROUND(rmse, 4),
            ROUND(total_actual_units, 0),
            ROUND(total_forecast_units, 0)
        FROM read_parquet('{sql_path(METRICS)}')
        ORDER BY wape_pct
    """).fetchall():
        print(row)

    print(f"\nForecast output: {FORECASTS}")
    print(f"Metrics output: {METRICS}")

finally:
    con.close()
