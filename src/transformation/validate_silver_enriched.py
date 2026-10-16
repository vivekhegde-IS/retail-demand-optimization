from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
SILVER = PROJECT / "data" / "processed" / "silver"

SOURCE = (SILVER / "sales_daily.parquet").as_posix().replace("'", "''")
ENRICHED = (SILVER / "sales_daily_enriched.parquet").as_posix().replace("'", "''")

def main():
    con = duckdb.connect()

    try:
        print("Running final Silver integrity checks...")

        result = con.execute(f"""
            WITH source_stats AS (
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT (id, date)) AS unique_keys,
                    SUM(sales) AS total_sales,
                    MIN(date) AS first_date,
                    MAX(date) AS last_date
                FROM read_parquet('{SOURCE}')
            ),
            enriched_stats AS (
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT (id, date)) AS unique_keys,
                    SUM(sales) AS total_sales,
                    MIN(date) AS first_date,
                    MAX(date) AS last_date,
                    COUNT(*) FILTER (WHERE wm_yr_wk IS NULL) AS missing_calendar,
                    COUNT(*) FILTER (
                        WHERE sell_price IS NULL AND sales > 0
                    ) AS missing_price_with_sales,
                    COUNT(*) FILTER (
                        WHERE sell_price IS NOT NULL AND sell_price <= 0
                    ) AS invalid_prices
                FROM read_parquet('{ENRICHED}')
            )
            SELECT *
            FROM source_stats CROSS JOIN enriched_stats
        """).fetchone()

        (
            source_rows, source_keys, source_sales, source_min, source_max,
            enriched_rows, enriched_keys, enriched_sales,
            enriched_min, enriched_max, missing_calendar,
            missing_price_with_sales, invalid_prices
        ) = result

        checks = {
            "Row count preserved": source_rows == enriched_rows,
            "Unique keys preserved": source_keys == enriched_keys,
            "Sales total preserved": source_sales == enriched_sales,
            "First date preserved": source_min == enriched_min,
            "Last date preserved": source_max == enriched_max,
            "No missing calendar matches": missing_calendar == 0,
            "No missing prices on positive-sales rows": missing_price_with_sales == 0,
            "No invalid matched prices": invalid_prices == 0,
        }

        print(f"\nSource rows: {source_rows:,}")
        print(f"Enriched rows: {enriched_rows:,}")
        print(f"Source total sales: {source_sales:,}")
        print(f"Enriched total sales: {enriched_sales:,}")
        print(f"Missing calendar matches: {missing_calendar:,}")
        print(f"Missing prices with positive sales: {missing_price_with_sales:,}")
        print(f"Invalid matched prices: {invalid_prices:,}")

        print("\nFINAL SILVER CHECKS")
        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}: {name}")

        if not all(checks.values()):
            raise SystemExit("Final Silver validation failed.")

        print("\nAll final Silver integrity checks passed.")

    finally:
        con.close()

if __name__ == "__main__":
    main()
