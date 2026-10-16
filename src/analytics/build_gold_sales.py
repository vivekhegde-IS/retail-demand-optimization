from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
SILVER = PROJECT / "data" / "processed" / "silver"
GOLD = PROJECT / "data" / "processed" / "gold"
SPILL = PROJECT / "data" / "temp" / "duckdb_spill"

SOURCE = SILVER / "sales_daily_enriched.parquet"


def sql_path(path):
    return path.resolve().as_posix().replace("'", "''")


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(f"Silver input missing: {SOURCE}")

    GOLD.mkdir(parents=True, exist_ok=True)
    SPILL.mkdir(parents=True, exist_ok=True)

    source = sql_path(SOURCE)
    spill = sql_path(SPILL)

    outputs = {
        "weekly": GOLD / "weekly_item_store.parquet",
        "monthly": GOLD / "monthly_store_category.parquet",
        "store": GOLD / "store_performance.parquet",
    }

    con = duckdb.connect()

    try:
        # Limit RAM and allow large operations to spill to disk.
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")
        con.execute("SET preserve_insertion_order = false")
        con.execute(f"SET temp_directory = '{spill}'")
        con.execute("SET max_temp_directory_size = '50GB'")

        print("DuckDB memory limit: 4 GB")
        print("DuckDB threads: 2")
        print(f"Temporary spill directory: {SPILL}")

        # Remove outputs from an earlier incomplete build.
        # These are generated Gold files, not source data.
        for path in outputs.values():
            if path.exists():
                path.unlink()

        print("\n1/3 Building weekly item-store performance...")
        weekly_out = sql_path(outputs["weekly"])

        con.execute(f"""
            COPY (
                SELECT
                    id,
                    item_id,
                    dept_id,
                    cat_id,
                    store_id,
                    state_id,
                    wm_yr_wk,
                    SUM(sales) AS weekly_units_sold,
                    SUM(
                        CASE
                            WHEN sell_price IS NOT NULL
                            THEN sales * sell_price
                            ELSE 0
                        END
                    ) AS weekly_revenue,
                    AVG(sell_price) AS average_sell_price,
                    COUNT(DISTINCT date) AS days_in_week
                FROM read_parquet('{source}')
                GROUP BY
                    id, item_id, dept_id, cat_id,
                    store_id, state_id, wm_yr_wk
            )
            TO '{weekly_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\n2/3 Building monthly store-category performance...")
        monthly_out = sql_path(outputs["monthly"])

        con.execute(f"""
            COPY (
                SELECT
                    DATE_TRUNC('month', date)::DATE AS month_start,
                    state_id,
                    store_id,
                    cat_id,
                    dept_id,
                    SUM(sales) AS units_sold,
                    SUM(
                        CASE
                            WHEN sell_price IS NOT NULL
                            THEN sales * sell_price
                            ELSE 0
                        END
                    ) AS revenue,
                    COUNT(DISTINCT item_id) AS distinct_items,
                    COUNT(DISTINCT item_id)
                        FILTER (WHERE sales > 0) AS items_sold
                FROM read_parquet('{source}')
                GROUP BY
                    DATE_TRUNC('month', date)::DATE,
                    state_id, store_id, cat_id, dept_id
            )
            TO '{monthly_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\n3/3 Building store performance summary...")
        store_out = sql_path(outputs["store"])

        con.execute(f"""
            COPY (
                SELECT
                    state_id,
                    store_id,
                    SUM(units_sold) AS total_units_sold,
                    SUM(revenue) AS total_revenue,
                    SUM(items_sold) AS monthly_item_store_activity,
                    COUNT(*) AS store_category_month_records
                FROM read_parquet('{monthly_out}')
                GROUP BY state_id, store_id
            )
            TO '{store_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\nGold datasets created. Checking output files...")

        for name, path in outputs.items():
            count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{sql_path(path)}')"
            ).fetchone()[0]
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"{name}: {count:,} rows, {size_mb:.1f} MB")

        print(f"\nGold output directory: {GOLD}")
        print("Gold build completed successfully.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
