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
checks = []

def check(name, passed, details=""):
    checks.append(bool(passed))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{sql_path(TEMP)}'")
    con.execute("SET max_temp_directory_size = '50GB'")

    base = sql_path(BASE)
    forecasts = sql_path(FORECASTS)
    metrics = sql_path(METRICS)

    print("Checking forecast counts and uniqueness...")

    model_stats = con.execute(f"""
        SELECT
            model_name,
            COUNT(*) AS rows,
            COUNT(DISTINCT id || '|' || CAST(week_start AS VARCHAR))
                AS unique_series_weeks,
            COUNT(*) FILTER (WHERE forecast_units IS NULL)
                AS null_forecasts,
            COUNT(*) FILTER (WHERE forecast_units < 0)
                AS negative_forecasts,
            COUNT(DISTINCT week_start) AS dates
        FROM read_parquet('{forecasts}')
        GROUP BY model_name
        ORDER BY model_name
    """).fetchall()

    expected_models = {"naive", "seasonal_naive_52"}
    actual_models = {row[0] for row in model_stats}

    check("Both expected models present",
          actual_models == expected_models,
          str(sorted(actual_models)))

    for row in model_stats:
        name, count, unique_keys, nulls, negatives, dates = row

        check(f"{name}: expected forecast count",
              count == 853720, f"{count:,} rows")

        check(f"{name}: unique series-week keys",
              count == unique_keys, f"{unique_keys:,} unique keys")

        check(f"{name}: no null or negative forecasts",
              nulls == 0 and negatives == 0,
              f"{nulls} null; {negatives} negative")

        check(f"{name}: 28 holdout dates",
              dates == 28, f"{dates} dates")

    print("\nRecomputing lag values from weekly history...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE expected_lags AS
        SELECT
            id,
            week_start,
            ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY week_start DESC
            ) AS weeks_from_end,
            LAG(weekly_units_sold, 1) OVER (
                PARTITION BY id ORDER BY week_start
            ) AS expected_naive,
            LAG(weekly_units_sold, 52) OVER (
                PARTITION BY id ORDER BY week_start
            ) AS expected_seasonal
        FROM read_parquet('{base}')
    """)

    lag_mismatches = con.execute(f"""
        SELECT
            COUNT(*) FILTER (
                WHERE f.model_name = 'naive'
                  AND ABS(f.forecast_units - e.expected_naive) > 0.000001
            ) AS naive_mismatches,
            COUNT(*) FILTER (
                WHERE f.model_name = 'seasonal_naive_52'
                  AND ABS(f.forecast_units - e.expected_seasonal) > 0.000001
            ) AS seasonal_mismatches
        FROM read_parquet('{forecasts}') f
        JOIN expected_lags e
          ON f.id = e.id
         AND f.week_start = e.week_start
        WHERE e.weeks_from_end <= 28
    """).fetchone()

    check("Naïve forecasts match previous-week actuals",
          lag_mismatches[0] == 0,
          f"{lag_mismatches[0]} mismatches")

    check("Seasonal forecasts match 52-week lag",
          lag_mismatches[1] == 0,
          f"{lag_mismatches[1]} mismatches")

    # Verify the two models use the same series and evaluation dates.
    coverage = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT id, week_start
            FROM read_parquet('{forecasts}')
            GROUP BY id, week_start
            HAVING COUNT(DISTINCT model_name) != 2
                OR COUNT(*) != 2
        )
    """).fetchone()[0]

    check("Both models share identical evaluation keys",
          coverage == 0, f"{coverage} mismatched keys")

    print("\nRecomputing saved evaluation metrics...")

    metric_mismatches = con.execute(f"""
        WITH calculated AS (
            SELECT
                model_name,
                COUNT(*) AS forecast_count,
                AVG(ABS(actual_units - forecast_units)) AS mae,
                100.0 * SUM(ABS(actual_units - forecast_units))
                    / NULLIF(SUM(actual_units), 0) AS wape_pct,
                SQRT(AVG(POWER(actual_units - forecast_units, 2)))
                    AS rmse
            FROM read_parquet('{forecasts}')
            GROUP BY model_name
        )
        SELECT COUNT(*)
        FROM calculated c
        FULL OUTER JOIN read_parquet('{metrics}') m
          USING (model_name)
        WHERE c.model_name IS NULL
           OR m.model_name IS NULL
           OR c.forecast_count != m.evaluated_forecasts
           OR ABS(c.mae - m.mae) > 0.0001
           OR ABS(c.wape_pct - m.wape_pct) > 0.0001
           OR ABS(c.rmse - m.rmse) > 0.0001
    """).fetchone()[0]

    check("Saved metrics match forecast-level calculations",
          metric_mismatches == 0,
          f"{metric_mismatches} mismatches")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review failed checks.")

finally:
    con.close()
