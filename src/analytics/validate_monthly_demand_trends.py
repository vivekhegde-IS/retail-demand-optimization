from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
OUTPUT = GOLD / "monthly_demand_trends.parquet"
WEEKLY = GOLD / "weekly_item_store.parquet"

con = duckdb.connect()
checks = []

def check(name, passed, details=""):
    checks.append(bool(passed))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    monthly = OUTPUT.as_posix()
    weekly = WEEKLY.as_posix()

    stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT month),
            MIN(month),
            MAX(month),
            SUM(units_sold),
            SUM(estimated_revenue)
        FROM read_parquet(?)
    """, [monthly]).fetchone()

    rows, months, first_month, last_month, units, revenue = stats

    check("Expected monthly record count", rows == 1950,
          f"{rows:,} rows")

    check("Complete month coverage", months == 65,
          f"{months} distinct months")

    check("Correct date boundaries",
          str(first_month) == "2011-01-01"
          and str(last_month) == "2016-05-01",
          f"{first_month} to {last_month}")

    duplicate_keys = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT month, store_id, cat_id
            FROM read_parquet(?)
            GROUP BY month, store_id, cat_id
            HAVING COUNT(*) > 1
        )
    """, [monthly]).fetchone()[0]

    check("Unique month-store-category keys",
          duplicate_keys == 0,
          f"{duplicate_keys} duplicate groups")

    invalid_metrics = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE month IS NULL
           OR store_id IS NULL
           OR cat_id IS NULL
           OR units_sold IS NULL
           OR units_sold < 0
           OR estimated_revenue IS NULL
           OR estimated_revenue < 0
    """, [monthly]).fetchone()[0]

    check("Valid monthly metrics", invalid_metrics == 0,
          f"{invalid_metrics} invalid rows")

    source_units, source_revenue = con.execute("""
        SELECT SUM(weekly_units_sold), SUM(weekly_revenue)
        FROM read_parquet(?)
    """, [weekly]).fetchone()

    check("Units reconcile with weekly Gold",
          abs(units - source_units) < 0.001,
          f"monthly={units:,.0f}; weekly={source_units:,.0f}")

    check("Revenue reconciles with weekly Gold",
          abs(revenue - source_revenue) < 0.10,
          f"monthly=${revenue:,.2f}; weekly=${source_revenue:,.2f}")

    # Compare the stored year-over-year values with a direct
    # join to the same store/category twelve months earlier.
    yoy = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?) current
        JOIN read_parquet(?) previous
          ON current.store_id = previous.store_id
         AND current.cat_id = previous.cat_id
         AND current.month =
             (previous.month + INTERVAL '12 months')::DATE
        WHERE previous.units_sold > 0
          AND ABS(
              current.units_yoy_change_pct
              - (current.units_sold - previous.units_sold)
                / previous.units_sold
          ) > 0.0000001
    """, [monthly, monthly]).fetchone()[0]

    check("Year-over-year demand calculations", yoy == 0,
          f"{yoy} mismatches")

    print("\nMonthly trend dataset summary:")
    print(f"  Rows: {rows:,}")
    print(f"  Months: {months}")
    print(f"  Units sold: {units:,.0f}")
    print(f"  Estimated revenue: ${revenue:,.2f}")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review the failed checks.")

finally:
    con.close()
