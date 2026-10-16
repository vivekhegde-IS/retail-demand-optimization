from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "outputs" / "forecasting"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

FORECASTS = OUT / "lightgbm_forecasts.parquet"
METRICS = OUT / "lightgbm_model_metrics.parquet"
BASELINE = ROOT / "data" / "processed" / "gold" / "baseline_forecasts.parquet"

TEMP.mkdir(parents=True, exist_ok=True)

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
    forecasts_path = FORECASTS.as_posix().replace("'", "''")
    metrics_path = METRICS.as_posix().replace("'", "''")
    baseline_path = BASELINE.as_posix().replace("'", "''")

    print("Checking forecast structure...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lgbm AS
        SELECT * FROM read_parquet('{forecasts_path}')
    """)

    row = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT id || '|' || CAST(week_start AS VARCHAR)),
            COUNT(*) FILTER (
                WHERE actual_units IS NULL
                   OR forecast_units IS NULL
                   OR actual_units < 0
                   OR forecast_units < 0
            ),
            COUNT(DISTINCT week_start),
            COUNT(DISTINCT model)
        FROM lgbm
    """).fetchone()

    expected = 30490 * 28
    check("Expected forecast count", row[0] == expected,
          f"{row[0]:,} rows")
    check("Unique series-week keys", row[1] == row[0],
          f"{row[1]:,} unique keys")
    check("No null or negative values", row[2] == 0,
          f"{row[2]} invalid rows")
    check("Exactly 28 holdout dates", row[3] == 28,
          f"{row[3]} dates")
    check("Only LightGBM predictions", row[4] == 1,
          f"{row[4]} model label(s)")

    print("\nChecking evaluation keys and actual values...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE baseline_keys AS
        SELECT id, week_start, actual_units
        FROM read_parquet('{baseline_path}')
        WHERE model_name = 'naive'
    """)

    mismatch = con.execute("""
        SELECT COUNT(*)
        FROM lgbm l
        FULL OUTER JOIN baseline_keys b
          ON l.id = b.id AND l.week_start = b.week_start
        WHERE l.id IS NULL
           OR b.id IS NULL
           OR ABS(l.actual_units - b.actual_units) > 0.000001
    """).fetchone()[0]

    check("Holdout keys and actuals match baseline",
          mismatch == 0, f"{mismatch} mismatches")

    print("\nRecalculating saved evaluation metrics...")

    calculated = con.execute("""
        SELECT
            COUNT(*) AS n,
            AVG(ABS(actual_units - forecast_units)) AS mae,
            100.0 * SUM(ABS(actual_units - forecast_units))
                / NULLIF(SUM(actual_units), 0) AS wape_pct,
            SQRT(AVG(POWER(actual_units - forecast_units, 2))) AS rmse,
            SUM(actual_units) AS total_actual_units,
            SUM(forecast_units) AS total_forecast_units
        FROM lgbm
    """).fetchone()

    saved = con.execute(f"""
        SELECT evaluated_forecasts, mae, wape_pct, rmse,
               total_actual_units, total_forecast_units
        FROM read_parquet('{metrics_path}')
        WHERE model = 'lightgbm'
    """).fetchall()

    metric_ok = len(saved) == 1
    if metric_ok:
        s = saved[0]
        metric_ok = (
            calculated[0] == s[0]
            and abs(calculated[1] - s[1]) <= 0.0001
            and abs(calculated[2] - s[2]) <= 0.0001
            and abs(calculated[3] - s[3]) <= 0.0001
            and calculated[4] == s[4]
            and abs(calculated[5] - s[5]) <= 0.01
        )

    check("Saved metrics match recalculated metrics", metric_ok,
          "metrics independently recalculated")

    print("\nRESULT:", f"{passed}/{passed + failed} checks passed")
    if failed:
        raise SystemExit(1)

finally:
    con.close()
