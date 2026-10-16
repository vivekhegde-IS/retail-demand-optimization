from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

TEMP.mkdir(parents=True, exist_ok=True)

weekly = (GOLD / "weekly_item_store.parquet").as_posix()
products = (GOLD / "product_abc_xyz.parquet").as_posix()

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    print("Aggregating product-level performance...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE product_performance AS
        SELECT
            item_id,
            cat_id,
            dept_id,
            SUM(weekly_units_sold) AS total_units_sold,
            SUM(weekly_revenue) AS total_estimated_revenue,
            AVG(average_sell_price) AS mean_weekly_average_price,
            COUNT(DISTINCT wm_yr_wk) AS observed_weeks
        FROM read_parquet(?)
        GROUP BY item_id, cat_id, dept_id
    """, [weekly])

    # Include ABC/XYZ labels so the ranked outputs are
    # useful for business prioritization.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE ranked_products AS
        SELECT
            p.*,
            a.abc_class,
            a.xyz_class,
            a.abc_xyz_segment,
            ROW_NUMBER() OVER (
                ORDER BY p.total_units_sold DESC, p.item_id
            ) AS units_rank,
            ROW_NUMBER() OVER (
                ORDER BY p.total_estimated_revenue DESC, p.item_id
            ) AS revenue_rank
        FROM product_performance p
        LEFT JOIN read_parquet(?) a USING (item_id)
    """, [products])

    units_path = (GOLD / "top_products_by_units.parquet").as_posix()
    revenue_path = (GOLD / "top_products_by_revenue.parquet").as_posix()

    con.execute("""
        COPY (
            SELECT *
            FROM ranked_products
            WHERE units_rank <= 100
            ORDER BY units_rank
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """, [units_path])

    con.execute("""
        COPY (
            SELECT *
            FROM ranked_products
            WHERE revenue_rank <= 100
            ORDER BY revenue_rank
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """, [revenue_path])

    for label, path, rank in [
        ("Units", units_path, "units_rank"),
        ("Revenue", revenue_path, "revenue_rank"),
    ]:
        result = con.execute(f"""
            SELECT COUNT(*), MIN({rank}), MAX({rank})
            FROM read_parquet(?)
        """, [path]).fetchone()

        print(f"\nTop products by {label.lower()}:")
        print(f"  Output: {path}")
        print(f"  Rows: {result[0]}")
        print(f"  Rank range: {result[1]} to {result[2]}")

        query = f"""
            SELECT item_id, cat_id, total_units_sold,
                   total_estimated_revenue, abc_xyz_segment
            FROM read_parquet(?)
            ORDER BY {rank}
            LIMIT 5
        """
        print("  Top 5:")
        for row in con.execute(query, [path]).fetchall():
            print(f"    {row}")

    print("\nTop-product datasets created successfully.")

finally:
    con.close()
