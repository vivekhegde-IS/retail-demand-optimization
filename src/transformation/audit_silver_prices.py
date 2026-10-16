from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
DATA_PATH = (
    PROJECT / "data" / "processed" / "silver"
    / "sales_daily_enriched.parquet"
)

def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(DATA_PATH)

    path = DATA_PATH.as_posix().replace("'", "''")
    con = duckdb.connect()

    try:
        con.execute(f"""
            CREATE VIEW enriched AS
            SELECT * FROM read_parquet('{path}')
        """)

        print("=== PRICE COVERAGE AUDIT ===")

        overall = con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (
                    WHERE sell_price IS NULL
                ) AS missing_price_rows,
                COUNT(*) FILTER (
                    WHERE sell_price IS NULL AND sales = 0
                ) AS missing_price_zero_sales,
                COUNT(*) FILTER (
                    WHERE sell_price IS NULL AND sales > 0
                ) AS missing_price_positive_sales,
                COALESCE(SUM(sales) FILTER (
                    WHERE sell_price IS NULL
                ), 0) AS units_sold_without_price,
                COUNT(*) FILTER (
                    WHERE sell_price IS NOT NULL AND sell_price <= 0
                ) AS invalid_matched_prices
            FROM enriched
        """).fetchone()

        labels = [
            "Total rows",
            "Rows missing price",
            "Missing price and zero sales",
            "Missing price and positive sales",
            "Units sold on rows missing price",
            "Invalid matched prices",
        ]

        for label, value in zip(labels, overall):
            print(f"{label}: {value:,}")

        print("\n=== MISSING PRICE BY YEAR ===")

        yearly = con.execute("""
            SELECT
                year,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (
                    WHERE sell_price IS NULL
                ) AS missing_price_rows,
                COUNT(*) FILTER (
                    WHERE sell_price IS NULL AND sales > 0
                ) AS missing_price_positive_sales,
                COALESCE(SUM(sales) FILTER (
                    WHERE sell_price IS NULL
                ), 0) AS units_sold_without_price
            FROM enriched
            GROUP BY year
            ORDER BY year
        """).fetchall()

        print(
            "Year | Total rows | Missing prices | "
            "Missing price + sales | Units sold without price"
        )

        for row in yearly:
            print(
                f"{row[0]} | {row[1]:,} | {row[2]:,} | "
                f"{row[3]:,} | {row[4]:,}"
            )

        print("\n=== STORES WITH MOST POSITIVE SALES WITHOUT PRICE ===")

        stores = con.execute("""
            SELECT
                store_id,
                COUNT(*) AS affected_rows,
                SUM(sales) AS units_sold_without_price
            FROM enriched
            WHERE sell_price IS NULL AND sales > 0
            GROUP BY store_id
            ORDER BY units_sold_without_price DESC
            LIMIT 10
        """).fetchall()

        if stores:
            for store, rows, units in stores:
                print(
                    f"{store}: {rows:,} affected rows, "
                    f"{units:,} units sold without price"
                )
        else:
            print("No positive-sales rows with missing prices.")

    finally:
        con.close()

if __name__ == "__main__":
    main()
