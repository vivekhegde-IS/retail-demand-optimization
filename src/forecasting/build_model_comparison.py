from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
PROCESSED = ROOT / "data" / "processed" / "gold"
OUT = ROOT / "data" / "outputs" / "forecasting"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

BASELINE = PROCESSED / "baseline_forecasts.parquet"
WEEKLY_BASE = PROCESSED / "weekly_forecasting_base.parquet"
MOVING = OUT / "moving_average_forecasts.parquet"
LIGHTGBM = OUT / "lightgbm_forecasts.parquet"

COMPARISON = OUT / "model_comparison.parquet"
CATEGORY = OUT / "category_model_comparison.parquet"

TEMP.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    def sql_path(path):
        return path.as_posix().replace("'", "''")

    baseline_path = sql_path(BASELINE)
    moving_path = sql_path(MOVING)
    lgbm_path = sql_path(LIGHTGBM)
    weekly_path = sql_path(WEEKLY_BASE)
    comparison_path = sql_path(COMPARISON)
    category_path = sql_path(CATEGORY)

    print("Combining all five models...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE all_forecasts AS

        SELECT
            id, week_start, actual_units,
            model_name AS model, forecast_units
        FROM read_parquet('{baseline_path}')

        UNION ALL

        SELECT
            id, week_start, actual_units,
            model, forecast_units
        FROM read_parquet('{moving_path}')

        UNION ALL

        SELECT
            id, week_start, actual_units,
            model, forecast_units
        FROM read_parquet('{lgbm_path}')
    """)

    print("Calculating overall metrics...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE overall_metrics AS
        SELECT
            model,
            COUNT(*) AS evaluated_forecasts,
            AVG(ABS(actual_units - forecast_units)) AS mae,
            100.0 * SUM(ABS(actual_units - forecast_units))
                / NULLIF(SUM(actual_units), 0) AS wape_pct,
            SQRT(AVG(POWER(actual_units - forecast_units, 2))) AS rmse,
            SUM(actual_units) AS total_actual_units,
            SUM(forecast_units) AS total_forecast_units
        FROM all_forecasts
        GROUP BY model
    """)

    con.execute(f"""
        COPY (
            SELECT *,
                RANK() OVER (ORDER BY wape_pct) AS wape_rank
            FROM overall_metrics
            ORDER BY wape_rank, model
        )
        TO '{comparison_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("Calculating category-level metrics...")

    con.execute(f"""
        COPY (
            SELECT
                f.model,
                b.cat_id,
                COUNT(*) AS evaluated_forecasts,
                AVG(ABS(f.actual_units - f.forecast_units)) AS mae,
                100.0 * SUM(ABS(f.actual_units - f.forecast_units))
                    / NULLIF(SUM(f.actual_units), 0) AS wape_pct,
                SQRT(AVG(POWER(f.actual_units - f.forecast_units, 2))) AS rmse
            FROM all_forecasts f
            JOIN read_parquet('{weekly_path}') b
              ON f.id = b.id
             AND f.week_start = b.week_start
            GROUP BY f.model, b.cat_id
        )
        TO '{category_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print("\nOVERALL MODEL RANKING — LOWER IS BETTER")
    print(con.execute(f"""
        SELECT model, evaluated_forecasts,
               ROUND(mae, 4) AS mae,
               ROUND(wape_pct, 2) AS wape_pct,
               ROUND(rmse, 4) AS rmse,
               wape_rank
        FROM read_parquet('{comparison_path}')
        ORDER BY wape_rank, model
    """).df().to_string(index=False))

    print("\nCATEGORY-LEVEL WAPE — LOWER IS BETTER")
    print(con.execute(f"""
        SELECT model, cat_id, evaluated_forecasts,
               ROUND(mae, 4) AS mae,
               ROUND(wape_pct, 2) AS wape_pct,
               ROUND(rmse, 4) AS rmse
        FROM read_parquet('{category_path}')
        ORDER BY cat_id, wape_pct
    """).df().to_string(index=False))

    print("\nSaved overall comparison:", COMPARISON)
    print("Saved category comparison:", CATEGORY)

finally:
    con.close()
