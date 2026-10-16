from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data" / "processed" / "silver"
GOLD = ROOT / "data" / "processed" / "gold"
TEMP = ROOT / "data" / "temp" / "duckdb_spill"

TEMP.mkdir(parents=True, exist_ok=True)
GOLD.mkdir(parents=True, exist_ok=True)

sales_file = SILVER / "sales_daily.parquet"
output_file = GOLD / "weekly_forecasting_base.parquet"

if not sales_file.exists():
    raise FileNotFoundError(f"Silver input not found: {sales_file}")

sales_sql = sales_file.as_posix().replace("'", "''")
output_sql = output_file.as_posix().replace("'", "''")
temp_sql = TEMP.as_posix().replace("'", "''")

con = duckdb.connect()

try:
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{temp_sql}'")
    con.execute("SET max_temp_directory_size = '50GB'")

    print(f"Input:  {sales_file}")
    print(f"Output: {output_file}")
    print("\nBuilding weekly forecasting base...")

    con.execute(f"""
        COPY (
            SELECT
                id,
                item_id,
                dept_id,
                cat_id,
                store_id,
                state_id,
                CAST(DATE_TRUNC('week', date) AS DATE) AS week_start,
                SUM(sales) AS weekly_units_sold
            FROM read_parquet('{sales_sql}')
            GROUP BY
                id,
                item_id,
                dept_id,
                cat_id,
                store_id,
                state_id,
                CAST(DATE_TRUNC('week', date) AS DATE)
        )
        TO '{output_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT id),
            COUNT(DISTINCT week_start),
            MIN(week_start),
            MAX(week_start),
            SUM(weekly_units_sold)
        FROM read_parquet(?)
    """, [str(output_file)]).fetchone()

    print("\nWeekly forecasting base created:")
    print(f"Rows: {stats[0]:,}")
    print(f"Series: {stats[1]:,}")
    print(f"Distinct weeks: {stats[2]:,}")
    print(f"First week: {stats[3]}")
    print(f"Last week: {stats[4]}")
    print(f"Total units: {stats[5]:,.0f}")

finally:
    con.close()
