from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "data" / "processed" / "gold" / "weekly_forecasting_base.parquet"
OUT = ROOT / "data" / "outputs" / "forecasting"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

FORECASTS = OUT / "moving_average_forecasts.parquet"
METRICS = OUT / "moving_average_model_metrics.parquet"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"[PASS] {name}" + (f" — {detail}" if detail else ""))
    else:
        failed += 1
        print(f"[FAIL] {name}" + (f" — {detail}" if detail else ""))

try:
    base_path = BASE.as_posix().replace("'", "''")
    forecasts_path = FORECASTS.as_posix().replace("'", "''")
    metrics_path = METRICS.as_posix().replace("'", "''")

    print("Checking saved forecast structure...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE saved_forecasts AS
        SELECT * FROM read_parquet('{forecasts_path}')
    """)

    models = [
        "moving_average_4",
        "moving_average_13",
    ]
    expected_count = 30490 * 28

    found_models = [
        row[0] for row in con.execute("""
            SELECT DISTINCT model
            FROM saved_forecasts
            ORDER BY model
        """).fetchall()
    ]
    check("Expected models present", found_models == sorted(models),
          str(found_models))

    for model in models:
        row = con.execute("""
            SELECT
                COUNT(*),
                COUNT(DISTINCT id || '|' || CAST(week_start AS VARCHAR)),
                COUNT(*) FILTER (
                    WHERE forecast_units IS NULL
                       OR actual_units IS NULL
                       OR forecast_units < 0
                       OR actual_units < 0
                ),
                COUNT(DISTINCT week_start)
            FROM saved_forecasts
            WHERE model = ?
        """, [model]).fetchone()

        check(f"{model}: forecast count", row[0] == expected_count,
              f"{row[0]:,} rows")
        check(f"{model}: unique series-week keys", row[1] == row[0],
              f"{row[1]:,} unique keys")
        check(f"{model}: valid values", row[2] == 0,
              f"{row[2]} invalid rows")
        check(f"{model}: 28 holdout dates", row[3] == 28,
              f"{row[3]} dates")

    print("\nRecomputing moving averages from historical demand...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE expected_forecasts AS
        SELECT
            id,
            week_start,
            weekly_units_sold AS actual_units,
            AVG(weekly_units_sold) OVER (
                PARTITION BY id
                ORDER BY week_start
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) AS ma_4,
            AVG(weekly_units_sold) OVER (
                PARTITION BY id
                ORDER BY week_start
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS ma_13,
            ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY week_start DESC
            ) AS weeks_from_end
        FROM read_parquet('{base_path}')
    """)

    for model, column in [
        ("moving_average_4", "ma_4"),
        ("moving_average_13", "ma_13"),
    ]:
        mismatches = con.execute(f"""
            SELECT COUNT(*)
            FROM saved_forecasts f
            LEFT JOIN expected_forecasts e
              ON f.id = e.id AND f.week_start = e.week_start
            WHERE f.model = ?
              AND (
                    e.id IS NULL
                 OR e.weeks_from_end > 28
                 OR ABS(f.actual_units - e.actual_units) > 0.000001
                 OR ABS(f.forecast_units - e.{column}) > 0.000001
              )
        """, [model]).fetchone()[0]

        check(f"{model}: forecasts use prior weeks only",
              mismatches == 0, f"{mismatches} mismatches")

    key_check = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT id, week_start
            FROM saved_forecasts
            GROUP BY id, week_start
            HAVING COUNT(*) != 2 OR COUNT(DISTINCT model) != 2
        )
    """).fetchone()[0]

    check("Both models use identical evaluation keys",
          key_check == 0, f"{key_check} mismatched keys")

    print("\nRecalculating evaluation metrics...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE recalculated_metrics AS
        SELECT
            model,
            COUNT(*) AS evaluated_forecasts,
            AVG(ABS(actual_units - forecast_units)) AS mae,
            100.0 * SUM(ABS(actual_units - forecast_units))
                / NULLIF(SUM(actual_units), 0) AS wape_pct,
            SQRT(AVG(POWER(actual_units - forecast_units, 2))) AS rmse,
            SUM(actual_units) AS total_actual_units,
            SUM(forecast_units) AS total_forecast_units
        FROM saved_forecasts
        GROUP BY model
    """)

    metric_mismatches = con.execute(f"""
        SELECT COUNT(*)
        FROM recalculated_metrics r
        LEFT JOIN read_parquet('{metrics_path}') m
          ON r.model = m.model
        WHERE m.model IS NULL
           OR r.evaluated_forecasts != m.evaluated_forecasts
           OR ABS(r.mae - m.mae) > 0.0001
           OR ABS(r.wape_pct - m.wape_pct) > 0.0001
           OR ABS(r.rmse - m.rmse) > 0.0001
           OR r.total_actual_units != m.total_actual_units
           OR ABS(r.total_forecast_units - m.total_forecast_units) > 0.001
    """).fetchone()[0]

    check("Saved metrics match recalculated metrics",
          metric_mismatches == 0, f"{metric_mismatches} mismatches")

    print("\nRESULT:", f"{passed}/{passed + failed} checks passed")
    if failed:
        raise SystemExit(1)

finally:
    con.close()
