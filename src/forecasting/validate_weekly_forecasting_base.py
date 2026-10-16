from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
BASE = GOLD / "weekly_forecasting_base.parquet"
WEEKLY = GOLD / "weekly_item_store.parquet"

con = duckdb.connect()
checks = []

def check(name, passed, details=""):
    checks.append(bool(passed))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    base = BASE.as_posix()
    weekly = WEEKLY.as_posix()

    stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT id),
            COUNT(DISTINCT week_start),
            MIN(week_start),
            MAX(week_start),
            SUM(weekly_units_sold)
        FROM read_parquet(?)
    """, [base]).fetchone()

    rows, series, weeks, first_date, last_date, units = stats

    check("Expected product-store series", series == 30490,
          f"{series:,} series")

    check("Expected weekly record count", rows == 8476220,
          f"{rows:,} rows")

    check("Expected date boundaries",
          str(first_date) == "2011-01-24"
          and str(last_date) == "2016-05-16",
          f"{first_date} to {last_date}")

    duplicates = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT id, week_start
            FROM read_parquet(?)
            GROUP BY id, week_start
            HAVING COUNT(*) > 1
        )
    """, [base]).fetchone()[0]

    check("Unique series-week keys", duplicates == 0,
          f"{duplicates} duplicate groups")

    invalid = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE id IS NULL
           OR week_start IS NULL
           OR weekly_units_sold IS NULL
           OR weekly_units_sold < 0
    """, [base]).fetchone()[0]

    check("Valid weekly demand values", invalid == 0,
          f"{invalid} invalid rows")

    source_units = con.execute("""
        SELECT SUM(weekly_units_sold)
        FROM read_parquet(?)
    """, [weekly]).fetchone()[0]

    check("Units reconcile with existing weekly Gold",
          abs(units - source_units) < 0.001,
          f"base={units:,.0f}; source={source_units:,.0f}")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review failed checks.")

finally:
    con.close()
