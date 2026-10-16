from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT / "data" / "processed" / "silver"

SALES_PATH = SILVER_DIR / "sales_daily.parquet"
CALENDAR_PATH = SILVER_DIR / "calendar.parquet"
PRICES_PATH = SILVER_DIR / "sell_prices.parquet"
OUTPUT_PATH = SILVER_DIR / "sales_daily_enriched.parquet"


def sql_path(path):
    return path.as_posix().replace("'", "''")


def main():
    for path in [SALES_PATH, CALENDAR_PATH, PRICES_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Required input missing: {path}")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    sales = sql_path(SALES_PATH)
    calendar = sql_path(CALENDAR_PATH)
    prices = sql_path(PRICES_PATH)
    output = sql_path(OUTPUT_PATH)

    con = duckdb.connect()

    try:
        print("Starting Silver sales enrichment...")
        print("Joining daily sales to calendar and weekly prices.")
        print("This may take several minutes.")

        con.execute(f"""
            COPY (
                SELECT
                    s.id,
                    s.item_id,
                    s.dept_id,
                    s.cat_id,
                    s.store_id,
                    s.state_id,
                    s.date,
                    s.day,
                    s.sales,
                    c.wm_yr_wk,
                    c.weekday,
                    c.wday,
                    c.month,
                    c.year,
                    c.event_name_1,
                    c.event_type_1,
                    c.event_name_2,
                    c.event_type_2,
                    c.snap_CA,
                    c.snap_TX,
                    c.snap_WI,
                    p.sell_price
                FROM read_parquet('{sales}') AS s
                INNER JOIN read_parquet('{calendar}') AS c
                    ON s.date = c.date
                LEFT JOIN read_parquet('{prices}') AS p
                    ON s.store_id = p.store_id
                   AND s.item_id = p.item_id
                   AND c.wm_yr_wk = p.wm_yr_wk
            )
            TO '{output}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        print("\nEnrichment output created.")
        print(f"Output: {OUTPUT_PATH}")

        summary = con.execute(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT (id, date)) AS unique_keys,
                COUNT(*) FILTER (WHERE sell_price IS NULL) AS missing_prices,
                COUNT(*) FILTER (WHERE sell_price IS NOT NULL) AS matched_prices,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM read_parquet('{output}')
        """).fetchone()

        labels = [
            "Total rows",
            "Unique id-date keys",
            "Rows with missing prices",
            "Rows with matched prices",
            "First date",
            "Last date",
        ]

        print("\nPreliminary output summary:")
        for label, value in zip(labels, summary):
            print(
                f"{label}: {value:,}"
                if isinstance(value, int)
                else f"{label}: {value}"
            )

    finally:
        con.close()


if __name__ == "__main__":
    main()
