from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"

SEASONAL = GOLD / "monthly_seasonality.parquet"
MONTHLY = GOLD / "monthly_demand_trends.parquet"

con = duckdb.connect()
checks = []

def check(name, passed, details=""):
    checks.append(bool(passed))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    seasonal = SEASONAL.as_posix()
    monthly = MONTHLY.as_posix()

    stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT calendar_month),
            MIN(calendar_month),
            MAX(calendar_month),
            MIN(observations),
            MAX(observations)
        FROM read_parquet(?)
    """, [seasonal]).fetchone()

    rows, months, first_month, last_month, min_obs, max_obs = stats

    check("Expected seasonal row count", rows == 360,
          f"{rows:,} rows")

    check("All 12 calendar months present",
          months == 12 and first_month == 1 and last_month == 12,
          f"{months} months; range {first_month}–{last_month}")

    duplicate_keys = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT state_id, store_id, cat_id, calendar_month
            FROM read_parquet(?)
            GROUP BY state_id, store_id, cat_id, calendar_month
            HAVING COUNT(*) > 1
        )
    """, [seasonal]).fetchone()[0]

    check("Unique store-category-month keys",
          duplicate_keys == 0,
          f"{duplicate_keys} duplicate groups")

    invalid_rows = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE calendar_month NOT BETWEEN 1 AND 12
           OR observations NOT BETWEEN 5 AND 6
           OR avg_monthly_units IS NULL
           OR avg_monthly_units < 0
           OR avg_monthly_estimated_revenue IS NULL
           OR avg_monthly_estimated_revenue < 0
           OR demand_seasonality_index IS NULL
           OR demand_seasonality_index < 0
           OR revenue_seasonality_index IS NULL
           OR revenue_seasonality_index < 0
    """, [seasonal]).fetchone()[0]

    check("Valid seasonal metrics and observation counts",
          invalid_rows == 0,
          f"{invalid_rows} invalid rows")

    index_errors = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                state_id,
                store_id,
                cat_id,
                AVG(demand_seasonality_index) AS mean_index,
                AVG(revenue_seasonality_index) AS mean_revenue_index
            FROM read_parquet(?)
            GROUP BY state_id, store_id, cat_id
        )
        WHERE ABS(mean_index - 1.0) > 0.000001
           OR ABS(mean_revenue_index - 1.0) > 0.000001
    """, [seasonal]).fetchone()[0]

    check("Seasonal indices average to 1",
          index_errors == 0,
          f"{index_errors} store-category groups outside tolerance")

    # Recompute the monthly means directly from the source.
    source_mismatches = con.execute("""
        WITH expected AS (
            SELECT
                state_id,
                store_id,
                cat_id,
                EXTRACT(MONTH FROM month)::INTEGER AS calendar_month,
                COUNT(*) AS observations,
                AVG(units_sold) AS avg_monthly_units,
                AVG(estimated_revenue) AS avg_monthly_estimated_revenue
            FROM read_parquet(?)
            GROUP BY
                state_id, store_id, cat_id,
                EXTRACT(MONTH FROM month)::INTEGER
        )
        SELECT COUNT(*)
        FROM read_parquet(?) actual
        FULL OUTER JOIN expected e
          ON actual.state_id = e.state_id
         AND actual.store_id = e.store_id
         AND actual.cat_id = e.cat_id
         AND actual.calendar_month = e.calendar_month
        WHERE actual.store_id IS NULL
           OR e.store_id IS NULL
           OR actual.observations != e.observations
           OR ABS(actual.avg_monthly_units - e.avg_monthly_units) > 0.01
           OR ABS(
               actual.avg_monthly_estimated_revenue
               - e.avg_monthly_estimated_revenue
           ) > 0.01
    """, [monthly, seasonal]).fetchone()[0]

    check("Monthly averages match source Gold",
          source_mismatches == 0,
          f"{source_mismatches} mismatches")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review the failed checks.")

finally:
    con.close()
