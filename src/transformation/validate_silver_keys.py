from pathlib import Path
import duckdb

project = Path(__file__).resolve().parents[2]
parquet = (project / "data" / "processed" / "silver" / "sales_daily.parquet")
parquet_sql = parquet.as_posix().replace("'", "''")

con = duckdb.connect()

try:
    print("Checking product-store/date key uniqueness...")

    result = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT (id, date)) AS unique_keys
        FROM read_parquet('{parquet_sql}')
    """).fetchone()

    total_rows, unique_keys = result
    duplicate_rows = total_rows - unique_keys

    print(f"Total rows: {total_rows:,}")
    print(f"Unique id-date keys: {unique_keys:,}")
    print(f"Duplicate key rows: {duplicate_rows:,}")

    if duplicate_rows != 0:
        raise SystemExit("FAIL: Duplicate id-date keys found.")

    print("PASS: Every id-date key is unique.")

finally:
    con.close()
