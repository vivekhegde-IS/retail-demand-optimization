from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data" / "processed" / "gold" / "weekly_forecasting_base.parquet"
OUT_DIR = ROOT / "data" / "outputs" / "forecasting"
TEMP_DIR = ROOT / "data" / "temp" / "duckdb_spill"

OUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

FORECASTS_OUT = OUT_DIR / "moving_average_forecasts.parquet"
METRICS_OUT = OUT_DIR / "moving_average_model_metrics.parquet"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP_DIR.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    base_path = BASE.as_posix().replace("'", "''")
    forecasts_path = FORECASTS_OUT.as_posix().replace("'", "''")
    metrics_path = METRICS_OUT.as_posix().replace("'", "''")

    print("Building moving-average history...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE moving_average_history AS
        SELECT
            id,
            item_id,
            dept_id,
            cat_id,
            store_id,
            state_id,
            week_start,
            weekly_units_sold,
            ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY week_start DESC
            ) AS weeks_from_end,
            AVG(weekly_units_sold) OVER (
                PARTITION BY id
                ORDER BY week_start
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) AS ma_4,
            AVG(weekly_units_sold) OVER (
                PARTITION BY id
                ORDER BY week_start
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS ma_13
        FROM read_parquet('{base_path}')
    """)

    print("Creating forecasts for the final 28 weeks...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE moving_average_forecasts AS
        SELECT
            id, item_id, dept_id, cat_id, store_id, state_id,
            week_start,
            weekly_units_sold AS actual_units,
            'moving_average_4' AS model,
            ma_4 AS forecast_units
        FROM moving_average_history
        WHERE weeks_from_end <= 28

        UNION ALL

        SELECT
            id, item_id, dept_id, cat_id, store_id, state_id,
            week_start,
            weekly_units_sold AS actual_units,
            'moving_average_13' AS model,
            ma_13 AS forecast_units
        FROM moving_average_history
        WHERE weeks_from_end <= 28
    """)

    con.execute(f"""
        COPY moving_average_forecasts
        TO '{forecasts_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("Calculating evaluation metrics...")

    con.execute(f"""
        COPY (
            SELECT
                model,
                COUNT(*) AS evaluated_forecasts,
                AVG(ABS(actual_units - forecast_units)) AS mae,
                100.0 * SUM(ABS(actual_units - forecast_units))
                    / NULLIF(SUM(actual_units), 0) AS wape_pct,
                SQRT(AVG(POWER(actual_units - forecast_units, 2))) AS rmse,
                SUM(actual_units) AS total_actual_units,
                SUM(forecast_units) AS total_forecast_units
            FROM moving_average_forecasts
            GROUP BY model
            ORDER BY model
        )
        TO '{metrics_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("\nMOVING-AVERAGE RESULTS")
    print(con.execute("""
        SELECT * FROM read_parquet(?)
        ORDER BY model
    """, [metrics_path]).df().to_string(index=False))

    print("\nForecast output:", FORECASTS_OUT)
    print("Metrics output:", METRICS_OUT)

finally:
    con.close()
