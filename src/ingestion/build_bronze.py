from pathlib import Path
import duckdb

PROJECT = Path(__file__).resolve().parents[2]
DATASET = Path(
    r"C:\Users\vivek\Downloads\data analytics\retail forecasting\m5-forecasting-accuracy"
)
DB_PATH = PROJECT / "data" / "bronze" / "retail_bronze.duckdb"

FILES = {
    "bronze_calendar": "calendar.csv",
    "bronze_sales_validation": "sales_train_validation.csv",
    "bronze_sales_evaluation": "sales_train_evaluation.csv",
    "bronze_sample_submission": "sample_submission.csv",
    "bronze_sell_prices": "sell_prices.csv",
}

def sql_path(path):
    return str(path.resolve()).replace("\\", "/").replace("'", "''")

def main():
    missing = [name for name in FILES.values()
               if not (DATASET / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing source files: {missing}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    try:
        for table, filename in FILES.items():
            source = sql_path(DATASET / filename)
            print(f"Loading {filename} into {table}...")

            con.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                f"SELECT * FROM read_csv_auto('{source}')"
            )

            count = con.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            print(f"  Loaded {count:,} rows.")

        print("\nBronze ingestion completed.")
        print(f"Database: {DB_PATH}")

    finally:
        con.close()

if __name__ == "__main__":
    main()
