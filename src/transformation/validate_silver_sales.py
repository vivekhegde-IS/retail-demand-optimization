from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
PARQUET_PATH = PROJECT / "data" / "processed" / "silver" / "sales_daily.parquet"

def main():
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"Missing file: {PARQUET_PATH}")

    parquet = PARQUET_PATH.as_posix().replace("'", "''")

    with duckdb.connect() as con:
        print("Validating Silver sales data...")

        result = con.execute(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT id) AS unique_series,
                COUNT(DISTINCT date) AS unique_dates,
                COUNT(*) FILTER (
                    WHERE id IS NULL OR item_id IS NULL
                       OR store_id IS NULL OR date IS NULL
                       OR sales IS NULL
                ) AS null_required_values,
                COUNT(*) FILTER (WHERE sales < 0) AS negative_sales,
                MIN(date) AS first_date,
                MAX(date) AS last_date,
                SUM(sales) AS total_units_sold
            FROM read_parquet('{parquet}')
        """).fetchone()

        labels = [
            "Total rows", "Unique product-store series", "Unique dates",
            "Rows with null required values", "Rows with negative sales",
            "First date", "Last date", "Total units sold"
        ]

        for label, value in zip(labels, result):
            print(f"{label}: {value:,}" if isinstance(value, int)
                  else f"{label}: {value}")

        checks = {
            "Expected row count": result[0] == 59_181_090,
            "Expected series count": result[1] == 30_490,
            "Expected date count": result[2] == 1_941,
            "No null required values": result[3] == 0,
            "No negative sales": result[4] == 0,
            "Correct first date": str(result[5]) == "2011-01-29",
            "Correct last date": str(result[6]) == "2016-05-22",
        }

        print("\nVALIDATION RESULTS")
        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}: {name}")

        if not all(checks.values()):
            raise SystemExit("Validation failed. Do not continue yet.")

        print("\nAll initial Silver validation checks passed.")

if __name__ == "__main__":
    main()
