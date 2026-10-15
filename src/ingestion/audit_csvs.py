from pathlib import Path
import duckdb

DATASET = Path(
    r"C:\Users\vivek\Downloads\data analytics\retail forecasting\m5-forecasting-accuracy"
)

FILES = [
    "calendar.csv",
    "sales_train_validation.csv",
    "sales_train_evaluation.csv",
    "sample_submission.csv",
    "sell_prices.csv",
]

def sql_path(path):
    return str(path.resolve()).replace("\\", "/").replace("'", "''")

con = duckdb.connect()

try:
    for filename in FILES:
        path = DATASET / filename
        print(f"\n{'=' * 65}")
        print(f"FILE: {filename}")

        if not path.is_file():
            print("ERROR: File not found")
            continue

        source = sql_path(path)
        relation = f"read_csv_auto('{source}', sample_size=1000)"

        try:
            schema = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {relation}"
            ).fetchone()[0]

            columns = [column[0] for column in schema]

            print(f"Rows: {row_count:,}")
            print(f"Columns: {len(columns):,}")
            print(f"First columns: {columns[:8]}")
            print(f"Last columns: {columns[-5:]}")

        except Exception as exc:
            print(f"ERROR while inspecting file: {exc}")

finally:
    con.close()
