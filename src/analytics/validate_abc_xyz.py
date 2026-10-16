from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"

WEEKLY = GOLD / "weekly_item_store.parquet"
PRODUCTS = GOLD / "product_abc_xyz.parquet"
SUMMARY = GOLD / "category_abc_xyz_summary.parquet"

con = duckdb.connect(":memory:")

checks = []

def check(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    checks.append(passed)
    print(f"[{status}] {name}" + (f" — {details}" if details else ""))

try:
    product_count = con.execute("""
        SELECT COUNT(*) FROM read_parquet(?)
    """, [str(PRODUCTS)]).fetchone()[0]

    unique_items = con.execute("""
        SELECT COUNT(DISTINCT item_id)
        FROM read_parquet(?)
    """, [str(PRODUCTS)]).fetchone()[0]

    check(
        "One row per product",
        product_count == unique_items,
        f"{product_count:,} rows; {unique_items:,} unique items"
    )

    invalid_abc = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE abc_class IS NULL
           OR abc_class NOT IN ('A', 'B', 'C')
    """, [str(PRODUCTS)]).fetchone()[0]

    invalid_xyz = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE xyz_class IS NULL
           OR xyz_class NOT IN ('X', 'Y', 'Z')
    """, [str(PRODUCTS)]).fetchone()[0]

    invalid_segments = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE abc_xyz_segment != abc_class || xyz_class
           OR abc_xyz_segment IS NULL
    """, [str(PRODUCTS)]).fetchone()[0]

    check("Valid ABC classes", invalid_abc == 0, f"{invalid_abc} invalid")
    check("Valid XYZ classes", invalid_xyz == 0, f"{invalid_xyz} invalid")
    check("Segment labels match classes", invalid_segments == 0,
          f"{invalid_segments} mismatches")

    invalid_metrics = con.execute("""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE total_units_sold IS NULL
           OR total_units_sold < 0
           OR total_estimated_revenue IS NULL
           OR total_estimated_revenue < 0
           OR demand_cv IS NULL
           OR demand_cv < 0
    """, [str(PRODUCTS)]).fetchone()[0]

    check("Valid product metrics", invalid_metrics == 0,
          f"{invalid_metrics} invalid rows")

    source_totals = con.execute("""
        SELECT
            SUM(weekly_units_sold),
            SUM(weekly_revenue)
        FROM read_parquet(?)
    """, [str(WEEKLY)]).fetchone()

    product_totals = con.execute("""
        SELECT
            SUM(total_units_sold),
            SUM(total_estimated_revenue)
        FROM read_parquet(?)
    """, [str(PRODUCTS)]).fetchone()

    source_units, source_revenue = source_totals
    product_units, product_revenue = product_totals

    check(
        "Units reconcile with weekly Gold",
        abs(source_units - product_units) < 0.001,
        f"weekly={source_units:,.0f}; products={product_units:,.0f}"
    )

    check(
        "Estimated revenue reconciles with weekly Gold",
        abs(source_revenue - product_revenue) < 0.10,
        f"weekly=${source_revenue:,.2f}; products=${product_revenue:,.2f}"
    )

    summary_rows = con.execute("""
        SELECT COUNT(*) FROM read_parquet(?)
    """, [str(SUMMARY)]).fetchone()[0]

    check("Category summary is populated", summary_rows > 0,
          f"{summary_rows:,} summary rows")

    print("\nProduct counts by ABC class:")
    for abc, count in con.execute("""
        SELECT abc_class, COUNT(*)
        FROM read_parquet(?)
        GROUP BY abc_class
        ORDER BY abc_class
    """, [str(PRODUCTS)]).fetchall():
        print(f"  {abc}: {count:,}")

    print("\nProduct counts by XYZ class:")
    for xyz, count in con.execute("""
        SELECT xyz_class, COUNT(*)
        FROM read_parquet(?)
        GROUP BY xyz_class
        ORDER BY xyz_class
    """, [str(PRODUCTS)]).fetchall():
        print(f"  {xyz}: {count:,}")

    print("\nCategory summary schema:")
    for column in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(SUMMARY)]
    ).fetchall():
        print(f"  {column[0]}: {column[1]}")

    print(f"\nRESULT: {sum(checks)}/{len(checks)} checks passed.")

    if not all(checks):
        raise SystemExit("Validation failed. Review the failed checks.")

finally:
    con.close()
