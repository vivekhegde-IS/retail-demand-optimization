from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"

WEEKLY = GOLD / "weekly_item_store.parquet"
BY_UNITS = GOLD / "top_products_by_units.parquet"
BY_REVENUE = GOLD / "top_products_by_revenue.parquet"

con = duckdb.connect()
checks = []

def check(name, passed, details=""):
    checks.append(bool(passed))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    weekly = WEEKLY.as_posix()
    units_path = BY_UNITS.as_posix()
    revenue_path = BY_REVENUE.as_posix()

    # Recalculate product metrics from the source Gold dataset.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE expected_products AS
        SELECT
            item_id,
            SUM(weekly_units_sold) AS total_units_sold,
            SUM(weekly_revenue) AS total_estimated_revenue
        FROM read_parquet(?)
        GROUP BY item_id
    """, [weekly])

    con.execute("""
        CREATE OR REPLACE TEMP TABLE expected_units AS
        SELECT
            item_id,
            ROW_NUMBER() OVER (
                ORDER BY total_units_sold DESC, item_id
            ) AS expected_rank,
            total_units_sold
        FROM expected_products
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE expected_revenue AS
        SELECT
            item_id,
            ROW_NUMBER() OVER (
                ORDER BY total_estimated_revenue DESC, item_id
            ) AS expected_rank,
            total_estimated_revenue
        FROM expected_products
    """)

    for label, path, rank_col, metric_col, expected_table in [
        ("Units", units_path, "units_rank",
         "total_units_sold", "expected_units"),
        ("Revenue", revenue_path, "revenue_rank",
         "total_estimated_revenue", "expected_revenue"),
    ]:
        print(f"\nValidating ranking by {label.lower()}...")

        stats = con.execute(f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT item_id),
                MIN({rank_col}),
                MAX({rank_col})
            FROM read_parquet(?)
        """, [path]).fetchone()

        count, unique_items, min_rank, max_rank = stats

        check(f"{label}: exactly 100 products", count == 100,
              f"{count} rows")

        check(f"{label}: unique product IDs",
              count == unique_items,
              f"{unique_items} unique IDs")

        check(f"{label}: ranks 1 through 100",
              min_rank == 1 and max_rank == 100,
              f"range {min_rank}–{max_rank}")

        invalid_metrics = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE {metric_col} IS NULL OR {metric_col} < 0
        """, [path]).fetchone()[0]

        check(f"{label}: valid ranking metric",
              invalid_metrics == 0,
              f"{invalid_metrics} invalid rows")

        # Check IDs, ranks, and metric values against a fresh
        # ranking derived directly from weekly Gold.
        mismatch_count = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet(?) actual
            FULL OUTER JOIN (SELECT * FROM {expected_table} WHERE expected_rank <= 100) expected
              ON actual.{rank_col} = expected.expected_rank
             AND actual.item_id = expected.item_id
            WHERE actual.item_id IS NULL
               OR expected.item_id IS NULL
               OR ABS(actual.{metric_col} - expected.{metric_col}) > 0.01
        """, [path]).fetchone()[0]

        check(f"{label}: ranking matches weekly Gold",
              mismatch_count == 0,
              f"{mismatch_count} mismatches")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review failed checks.")

finally:
    con.close()
