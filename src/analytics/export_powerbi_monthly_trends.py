from pathlib import Path
import duckdb

root = Path(__file__).resolve().parents[2]
source = root / "data" / "processed" / "gold" / "monthly_demand_trends.parquet"
destination = root / "data" / "outputs" / "powerbi" / "monthly_demand_trends.csv"

destination.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
try:
    source_sql = source.as_posix().replace("'", "''")
    destination_sql = destination.as_posix().replace("'", "''")

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{source_sql}')
        )
        TO '{destination_sql}'
        (FORMAT CSV, HEADER, DELIMITER ',')
    """)

    count = con.execute(f"""
        SELECT COUNT(*)
        FROM read_csv_auto('{destination_sql}')
    """).fetchone()[0]

    print(f"Export successful: {destination}")
    print(f"Verified rows: {count:,}")

    if count != 1950:
        raise ValueError(f"Expected 1950 rows, got {count}")

finally:
    con.close()
