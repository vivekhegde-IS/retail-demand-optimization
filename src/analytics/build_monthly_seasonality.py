from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

TEMP.mkdir(parents=True, exist_ok=True)

source = (GOLD / "monthly_demand_trends.parquet").as_posix()
output = GOLD / "monthly_seasonality.parquet"

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET threads = 2")
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{TEMP.as_posix()}'")
con.execute("SET max_temp_directory_size = '50GB'")

try:
    print("Calculating historical month-of-year demand patterns...")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE seasonal_base AS
        SELECT
            state_id,
            store_id,
            cat_id,
            EXTRACT(MONTH FROM month)::INTEGER AS calendar_month,
            COUNT(*) AS observations,
            AVG(units_sold) AS avg_monthly_units,
            AVG(estimated_revenue) AS avg_monthly_estimated_revenue,
            MIN(units_sold) AS min_monthly_units,
            MAX(units_sold) AS max_monthly_units
        FROM read_parquet(?)
        GROUP BY
            state_id,
            store_id,
            cat_id,
            EXTRACT(MONTH FROM month)::INTEGER
    """, [source])

    print("Calculating seasonal indices...")

    con.execute("""
        COPY (
            WITH indexed AS (
                SELECT
                    *,
                    AVG(avg_monthly_units) OVER (
                        PARTITION BY state_id, store_id, cat_id
                    ) AS overall_avg_monthly_units,
                    AVG(avg_monthly_estimated_revenue) OVER (
                        PARTITION BY state_id, store_id, cat_id
                    ) AS overall_avg_monthly_revenue
                FROM seasonal_base
            )
            SELECT
                *,
                CASE
                    WHEN overall_avg_monthly_units > 0
                    THEN avg_monthly_units / overall_avg_monthly_units
                    ELSE NULL
                END AS demand_seasonality_index,
                CASE
                    WHEN overall_avg_monthly_revenue > 0
                    THEN avg_monthly_estimated_revenue
                         / overall_avg_monthly_revenue
                    ELSE NULL
                END AS revenue_seasonality_index
            FROM indexed
            ORDER BY state_id, store_id, cat_id, calendar_month
        )
        TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """, [str(output)])

    summary = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT calendar_month),
            MIN(calendar_month),
            MAX(calendar_month),
            MIN(observations),
            MAX(observations)
        FROM read_parquet(?)
    """, [str(output)]).fetchone()

    print("\nSeasonality dataset created:")
    print(f"Output: {output}")
    print(f"Rows: {summary[0]:,}")
    print(f"Distinct calendar months: {summary[1]}")
    print(f"Calendar month range: {summary[2]}–{summary[3]}")
    print(f"Observations per seasonal group: {summary[4]}–{summary[5]}")

    print("\nExample: seasonal demand indices")
    for row in con.execute("""
        SELECT
            store_id,
            cat_id,
            calendar_month,
            observations,
            ROUND(avg_monthly_units, 2),
            ROUND(demand_seasonality_index, 3)
        FROM read_parquet(?)
        WHERE store_id = (
            SELECT MIN(store_id) FROM read_parquet(?)
        )
          AND cat_id = (
            SELECT MIN(cat_id) FROM read_parquet(?)
        )
        ORDER BY calendar_month
    """, [str(output), source, source]).fetchall():
        print(row)

finally:
    con.close()
