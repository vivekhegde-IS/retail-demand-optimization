from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
SILVER = PROJECT / "data" / "processed" / "silver"

def main():
    con = duckdb.connect()
    calendar = (SILVER / "calendar.parquet").as_posix().replace("'", "''")
    prices = (SILVER / "sell_prices.parquet").as_posix().replace("'", "''")

    try:
        checks = [
            (
                "Calendar rows",
                f"SELECT COUNT(*) FROM read_parquet('{calendar}')",
                1969,
            ),
            (
                "Unique calendar dates",
                f"SELECT COUNT(DISTINCT date) FROM read_parquet('{calendar}')",
                1969,
            ),
            (
                "Calendar null dates",
                f"SELECT COUNT(*) FROM read_parquet('{calendar}') WHERE date IS NULL",
                0,
            ),
            (
                "Sell-price rows",
                f"SELECT COUNT(*) FROM read_parquet('{prices}')",
                6841121,
            ),
            (
                "Duplicate price keys",
                f"""
                SELECT COUNT(*) FROM (
                    SELECT store_id, item_id, wm_yr_wk
                    FROM read_parquet('{prices}')
                    GROUP BY store_id, item_id, wm_yr_wk
                    HAVING COUNT(*) > 1
                )
                """,
                0,
            ),
            (
                "Invalid prices",
                f"""
                SELECT COUNT(*) FROM read_parquet('{prices}')
                WHERE sell_price IS NULL OR sell_price <= 0
                """,
                0,
            ),
        ]

        failed = False
        for name, query, expected in checks:
            actual = con.execute(query).fetchone()[0]
            passed = actual == expected
            print(f"{'PASS' if passed else 'FAIL'}: {name}: {actual:,}")
            if not passed:
                failed = True

        if failed:
            raise SystemExit("Silver dimension validation failed.")

        print("\nAll Silver dimension checks passed.")

    finally:
        con.close()

if __name__ == "__main__":
    main()
