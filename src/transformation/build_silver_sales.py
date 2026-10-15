from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT / "data" / "bronze" / "retail_bronze.duckdb"
OUTPUT_PATH = PROJECT / "data" / "processed" / "silver" / "sales_daily.parquet"

def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        output = OUTPUT_PATH.as_posix().replace("'", "''")
        print("Starting wide-to-long sales transformation...")

        con.execute("""
            CREATE OR REPLACE TEMP TABLE sales_long AS
            UNPIVOT bronze_sales_evaluation
            ON COLUMNS(* EXCLUDE (
                id, item_id, dept_id, cat_id, store_id, state_id
            ))
            INTO NAME day VALUE sales
        """)

        print("Sales columns unpivoted. Joining calendar...")

        con.execute(f"""
            COPY (
                SELECT
                    s.id,
                    s.item_id,
                    s.dept_id,
                    s.cat_id,
                    s.store_id,
                    s.state_id,
                    c.date,
                    s.day,
                    CAST(s.sales AS INTEGER) AS sales
                FROM sales_long AS s
                JOIN bronze_calendar AS c
                  ON c.d = s.day
            )
            TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        result = con.execute(f"""
            SELECT COUNT(*), MIN(date), MAX(date)
            FROM read_parquet('{output}')
        """).fetchone()

        print(f"Rows written: {result[0]:,}")
        print(f"First date: {result[1]}")
        print(f"Last date: {result[2]}")
        print(f"Output: {OUTPUT_PATH}")

    finally:
        con.close()

if __name__ == "__main__":
    main()
