from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
GOLD = PROJECT / "data" / "processed" / "gold"
SPILL = PROJECT / "data" / "temp" / "duckdb_spill"

SOURCE = GOLD / "weekly_item_store.parquet"
OUTPUT = GOLD / "product_abc_xyz.parquet"
CATEGORY_OUTPUT = GOLD / "category_abc_xyz_summary.parquet"


def sql_path(path):
    return path.resolve().as_posix().replace("'", "''")


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(f"Missing Gold input: {SOURCE}")

    GOLD.mkdir(parents=True, exist_ok=True)
    SPILL.mkdir(parents=True, exist_ok=True)

    source = sql_path(SOURCE)
    output = sql_path(OUTPUT)
    category_output = sql_path(CATEGORY_OUTPUT)
    spill = sql_path(SPILL)

    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")
        con.execute("SET preserve_insertion_order = false")
        con.execute(f"SET temp_directory = '{spill}'")
        con.execute("SET max_temp_directory_size = '50GB'")

        print("Building weekly demand by product...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE item_weekly AS
            SELECT
                item_id,
                cat_id,
                dept_id,
                wm_yr_wk,
                SUM(weekly_units_sold) AS weekly_units,
                SUM(weekly_revenue) AS weekly_revenue
            FROM read_parquet('{source}')
            GROUP BY item_id, cat_id, dept_id, wm_yr_wk
        """)

        print("Calculating product revenue and demand variability...")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE item_metrics AS
            SELECT
                item_id,
                cat_id,
                dept_id,
                SUM(weekly_units) AS total_units_sold,
                SUM(weekly_revenue) AS total_estimated_revenue,
                AVG(weekly_units) AS mean_weekly_units,
                STDDEV_POP(weekly_units) AS stddev_weekly_units,
                COUNT(*) AS observed_weeks,
                COUNT(*) FILTER (WHERE weekly_units > 0) AS selling_weeks
            FROM item_weekly
            GROUP BY item_id, cat_id, dept_id
        """)

        print("Assigning ABC and XYZ classes...")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE classified AS
            WITH cv AS (
                SELECT
                    *,
                    CASE
                        WHEN mean_weekly_units > 0
                        THEN stddev_weekly_units / mean_weekly_units
                        ELSE NULL
                    END AS demand_cv
                FROM item_metrics
            ),
            ranked AS (
                SELECT
                    *,
                    SUM(total_estimated_revenue) OVER (
                        ORDER BY total_estimated_revenue DESC, item_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) AS revenue_before_item,
                    SUM(total_estimated_revenue) OVER () AS all_revenue
                FROM cv
            )
            SELECT
                *,
                CASE
                    WHEN COALESCE(revenue_before_item, 0) / NULLIF(all_revenue, 0) < 0.80
                        THEN 'A'
                    WHEN COALESCE(revenue_before_item, 0) / NULLIF(all_revenue, 0) < 0.95
                        THEN 'B'
                    ELSE 'C'
                END AS abc_class,
                CASE
                    WHEN mean_weekly_units <= 0 THEN 'N'
                    WHEN demand_cv <= 0.50 THEN 'X'
                    WHEN demand_cv <= 1.00 THEN 'Y'
                    ELSE 'Z'
                END AS xyz_class
            FROM ranked
        """)

        con.execute(f"""
            COPY (
                SELECT
                    *,
                    abc_class || xyz_class AS abc_xyz_segment
                FROM classified
            )
            TO '{output}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("Building category-level classification summary...")
        con.execute(f"""
            COPY (
                SELECT
                    cat_id,
                    abc_class,
                    xyz_class,
                    COUNT(*) AS product_count,
                    SUM(total_units_sold) AS total_units_sold,
                    SUM(total_estimated_revenue) AS total_estimated_revenue
                FROM read_parquet('{output}')
                GROUP BY cat_id, abc_class, xyz_class
                ORDER BY cat_id, abc_class, xyz_class
            )
            TO '{category_output}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\nABC/XYZ outputs created:")
        print(f"Product classifications: {OUTPUT}")
        print(f"Category summary: {CATEGORY_OUTPUT}")

        result = con.execute(f"""
            SELECT
                COUNT(*) AS products,
                COUNT(DISTINCT item_id) AS unique_items,
                COUNT(*) FILTER (WHERE demand_cv IS NULL) AS undefined_cv,
                COUNT(*) FILTER (WHERE total_estimated_revenue < 0) AS negative_revenue
            FROM read_parquet('{output}')
        """).fetchone()

        print(f"\nProducts: {result[0]:,}")
        print(f"Unique item IDs: {result[1]:,}")
        print(f"Products with undefined CV: {result[2]:,}")
        print(f"Products with negative estimated revenue: {result[3]:,}")

        print("\nProduct counts by ABC/XYZ segment:")
        segments = con.execute(f"""
            SELECT abc_xyz_segment, COUNT(*)
            FROM read_parquet('{output}')
            GROUP BY abc_xyz_segment
            ORDER BY abc_xyz_segment
        """).fetchall()

        for segment, count in segments:
            print(f"{segment}: {count:,}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
