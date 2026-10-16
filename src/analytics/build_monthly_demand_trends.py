from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data" / "processed" / "silver"
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

GOLD.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

output = GOLD / "monthly_demand_trends.parquet"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    sales_path = (SILVER / "sales_daily_enriched.parquet").as_posix()
    calendar_path = (SILVER / "calendar.parquet").as_posix()

    print("Building monthly demand trends from daily Silver data...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE monthly_base AS
        SELECT
            DATE_TRUNC('month', CAST(s.date AS DATE))::DATE AS month,
            s.state_id,
            s.store_id,
            s.cat_id,
            SUM(s.sales) AS units_sold,
            SUM(
                CASE
                    WHEN s.sell_price IS NOT NULL
                    THEN s.sales * s.sell_price
                    ELSE 0
                END
            ) AS estimated_revenue,
            COUNT(DISTINCT s.item_id) AS products_in_catalog,
            COUNT(DISTINCT CASE
                WHEN s.sales > 0 THEN s.item_id
            END) AS products_sold,
            SUM(CASE WHEN s.sales > 0 THEN 1 ELSE 0 END)
                AS product_store_days_with_sales
        FROM read_parquet(?) s
        GROUP BY
            DATE_TRUNC('month', CAST(s.date AS DATE))::DATE,
            s.state_id,
            s.store_id,
            s.cat_id
    """, [sales_path])

    print("Calculating year-over-year changes...")

    con.execute("""
        COPY (
            WITH trend AS (
                SELECT
                    *,
                    LAG(units_sold, 12) OVER (
                        PARTITION BY state_id, store_id, cat_id
                        ORDER BY month
                    ) AS units_sold_previous_year,
                    LAG(estimated_revenue, 12) OVER (
                        PARTITION BY state_id, store_id, cat_id
                        ORDER BY month
                    ) AS estimated_revenue_previous_year
                FROM monthly_base
            )
            SELECT
                *,
                CASE
                    WHEN units_sold_previous_year > 0
                    THEN
                        (units_sold - units_sold_previous_year)
                        / units_sold_previous_year
                    ELSE NULL
                END AS units_yoy_change_pct,
                CASE
                    WHEN estimated_revenue_previous_year > 0
                    THEN
                        (estimated_revenue - estimated_revenue_previous_year)
                        / estimated_revenue_previous_year
                    ELSE NULL
                END AS revenue_yoy_change_pct
            FROM trend
        )
        TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """, [str(output)])

    result = con.execute("""
        SELECT
            COUNT(*) AS rows,
            MIN(month) AS first_month,
            MAX(month) AS last_month,
            SUM(units_sold) AS total_units,
            SUM(estimated_revenue) AS total_estimated_revenue
        FROM read_parquet(?)
    """, [str(output)]).fetchone()

    print("\nMonthly demand trends created:")
    print(f"Output: {output}")
    print(f"Rows: {result[0]:,}")
    print(f"First month: {result[1]}")
    print(f"Last month: {result[2]}")
    print(f"Total units: {result[3]:,.0f}")
    print(f"Estimated revenue: ${result[4]:,.2f}")

finally:
    con.close()
